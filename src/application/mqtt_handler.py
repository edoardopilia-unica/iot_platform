import logging
from datetime import datetime, timezone
from flask import current_app

logger = logging.getLogger(__name__)

class MQTTHandler:
    def __init__(self, app=None, mqtt_client=None):
        self.app = app
        self.mqtt_client = mqtt_client
    
    def handle_message(self, client, userdata, msg):
        """
        Handle ESP8266 messages
        Topic:
            - devices/<mac>/sensor/temp   (Payload: "25.50")
            - devices/<mac>/sensor/smoke  (Payload: "300")
            - devices/<mac>/sensor/flame  (Payload: "0" = FIRE, "1" = OK)
            - devices/<mac>/discovery     (Payload: "online")
        """

        with self.app.app_context():
            try:
                topic = msg.topic
                payload = msg.payload.decode()

                parts = topic.split('/')

                if len(parts) < 3:
                    logger.warning(f"Invalid topic format: {topic}.")
                    return
                
                mac_address = parts[1]
                msg_type = parts[-1]

                if msg_type == 'discovery':
                    logger.info(f"Device {mac_address} is online.")
                    payload_str = msg.payload.decode('utf-8')
                    if payload_str == "online":
                        self._handle_discovery(mac_address)
                    return
                
                if msg_type == 'sensor':
                    sensor_msg_type = parts[3]
                    payload_str = msg.payload.decode('utf-8')
                    self._process_sensor_data(mac_address, sensor_msg_type, payload_str)

            except Exception as e:
                logger.error(f"Error handling MQTT message: {e}")
        
    def _handle_discovery(self, mac_address):
        db_service = current_app.config['DB_SERVICE']
        dr_factory = current_app.config['DR_FACTORY']

        nodes = db_service.query_drs('node', {'profile.mac_address': mac_address})

        if nodes:
            node = nodes[0]
            logger.info(f"Node {mac_address} came online.")
            node['entity']['data']['last_seen'] = datetime.now(timezone.utc)
            if node['profile'].get('zone_id'):
                node['entity']['data']['status'] = 'Active'
            db_service.update_dr('node', node['_id'], node)
        else:
            logger.info(f"New node discovered with MAC address {mac_address}.")
            
            node_data = {
                'mac_address': mac_address,
                'zone_id': None,
                'created_at': datetime.now(timezone.utc),
                'updated_at': datetime.now(timezone.utc),
            }

            try:
                new_node = dr_factory.create_dr('node', node_data)
                new_node['entity']['data']['status'] = 'Provisioning'
                new_node['entity']['data']['last_seen'] = datetime.now(timezone.utc)

                node_id = db_service.insert_dr('node', new_node)

                logger.info(f"Node created with ID {node_id} for MAC address {mac_address}.")
            except Exception as e:
                logger.error(f"Failed to create node for MAC address {mac_address}: {e}")

    def _process_sensor_data(self, mac_address, sensor_type, value_str):
        db_service = current_app.config['DB_SERVICE']

        nodes = db_service.query_drs('node', {'profile.mac_address': mac_address})

        if not nodes:
            logger.warning(f"No node found for MAC address {mac_address}.")
            return
        
        node = nodes[0]
        node_id = node['_id']
        entity_data = node['entity']['data']

        try:
            if sensor_type == 'temp':
                entity_data['temp_level'] = float(value_str)
            elif sensor_type == 'smoke':
                entity_data['smoke_level'] = float(value_str)
            elif sensor_type == 'flame':
                entity_data['flame_level'] = (value_str == "0")  # True if FIRE, False if OK
            
            entity_data['last_seen'] = datetime.now(timezone.utc)
            db_service.update_dr('node', node_id, node)

            if entity_data.get('status') == 'Active' and node['profile'].get('zone_id'):
                self._check_safety_thresholds(node)
        except ValueError:
            logger.error(f"Invalid sensor value '{value_str}' for sensor type '{sensor_type}' on node {mac_address}.")

    def _check_safety_thresholds(self, node):
        zone_id = node['profile'].get('zone_id')
        db_service = current_app.config['DB_SERVICE']
        dr_factory = current_app.config['DR_FACTORY']

        zone = db_service.get_dr('zone', zone_id)

        if not zone:
            logger.warning(f"No zone found for zone ID {zone_id}.")
            return
        
        data = node['entity']['data']
        temp_level = data.get('temp_level', 0)
        smoke_level = data.get('smoke_level', 0)
        is_flame = data.get('flame_level', False)

        temp_threshold = zone['entity']['data'].get('temp_threshold', 40.0)
        smoke_threshold = zone['entity']['data'].get('smoke_threshold', 800.0)

        detected_cause = None

        if (is_flame): detected_cause = 'flame'
        elif (temp_level >= temp_threshold): detected_cause = 'temp'
        elif (smoke_level >= smoke_threshold): detected_cause = 'smoke'
        

        current_zone_status = zone['entity']['data']['status']
        now = datetime.now(timezone.utc)

        if detected_cause:
            if current_zone_status != "Active" or current_zone_status == detected_cause: return
            
            alarm_data = {
                "zone_id": zone_id,
                "trigger_cause": detected_cause,
                "start_time": now,
                "end_time": None,
                "created_at": now,
                "updated_at": now
            }

            alarm_dr = dr_factory.create_dr('alarm', alarm_data)
            alarm_id = db_service.insert_dr('alarm', alarm_dr)

            zone['entity']['data']['status'] = detected_cause
            zone['metadata']['updated_at'] = now

            db_service.update_dr('zone', zone_id, zone)

            self.send_command(node['profile']['mac_address'], 'actuate_alarms')
            logger.info(f"Alarm {alarm_id} triggered for zone {zone_id} due to {detected_cause}.")

    def send_command(self, mac_address, command):
        """
        Send command to ESP8266 device
        """

        if self.mqtt_client:
            topic = f"devices/{mac_address}/command"
            self.mqtt_client.publish(topic, command)
            logger.info(f"Sent command '{command}' to device {mac_address} on topic '{topic}'.")
