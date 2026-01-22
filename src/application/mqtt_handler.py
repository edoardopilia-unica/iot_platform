import logging
import json
import paho.mqtt.client as mqtt
from datetime import datetime, timezone
from flask import current_app

logger = logging.getLogger(__name__)
seen_list = []

class MQTTHandler:
    def __init__(self, app):
        self.app = app
        self.client = mqtt.Client()
        
        #Callback
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        
        
        self._isrunning = False

    def start(self):
        broker = self.app.config.get('MQTT_BROKER', 'mosquitto')
        port = self.app.config.get('MQTT_PORT', 1883)

        try:
            logger.info(f"Connecting to MQTT Broker at {broker}:{port}")
            self.client.connect(broker, port, 60)

            self.client.loop_start()
            self._isrunning = True
            logger.info("MQTT Handler started and listening for messages.")
        except Exception as e:
            logger.error(f"Error connecting to MQTT Broker: {e}")


    def stop(self):
        if self._isrunning:
            self.client.loop_stop()
            self.client.disconnect()
            self._isrunning = False
            logger.info("MQTT Handler stopped.")

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logger.info("Connected to MQTT Broker successfully.")
            # Subscribe to relevant topics
            self.client.subscribe("devices/+/+")
            self.client.subscribe("devices/+/sensor/+")

        else:
            logger.error(f"Failed to connect to MQTT Broker. Return code: {rc}")
        logger.info("Connected to MQTT Broker")
    

    def _on_message(self, client, userdata, msg):
        with self.app.app_context():
            try:
                topic = msg.topic
                try:
                    payload_str = msg.payload.decode('utf-8')
                except:
                    return
                logger.info(f"Received message on topic: {msg.topic}")
                
                parts = topic.split('/')

                if len(parts) < 3:
                    logger.warning(f"Invalid topic format: {topic}")
                    return
                
                mac_address = parts[1]
                category = parts[2]

                # Discovery
                if category == "discovery":
                    if payload_str == "online":
                        self._handle_discovery(mac_address)
                    return
                
                # Sensor Data
                if category == "sensor" and len(parts) > 3:
                    sensor_type = parts[3]
                    self._process_sensor_data(mac_address, sensor_type, payload_str)
                    return
                
            except Exception as e:
                logger.error(f"Error processing MQTT message: {e} - {parts}")
    

    def _process_sensor_data(self, mac_address, sensor_type, payload_str):
        db_service = current_app.config['DB_SERVICE']

        val_flame = False
        val_smoke = 0.0
        val_temp = 0.0

        try:
            if sensor_type == "flame":
                val_flame = (str(payload_str).strip() == "0")
            elif sensor_type == "smoke":
                val_smoke = float(payload_str)
            elif sensor_type == "temp":
                val_temp = float(payload_str)
        except ValueError:
            return
        
        nodes = db_service.query_drs('node', {'profile.mac_address': mac_address})
        if not nodes:
            self._handle_discovery(mac_address)
            return
        
        node = nodes[0]
        node_id = node['_id']
        zone_id = node['profile'].get('zone_id')

        entity_data = node.get('data')
        entity_data['last_seen'] = datetime.now(timezone.utc)
        entity_data['flame_detected'] = val_flame
        entity_data['smoke_level'] = val_smoke
        entity_data['temp_level'] = val_temp

        node['metadata']['updated_at'] = datetime.now(timezone.utc)
        db_service.update_dr('node', node_id, node)

        if zone_id and entity_data['status'] == "Active":
            self._check_zone_thresholds(zone_id, val_temp, val_smoke, val_flame)
        
    def _check_zone_thresholds(self, zone_id, val_temp, val_smoke, val_flame):
        db_service = current_app.config['DB_SERVICE']
        zone = db_service.get_dr('zone', zone_id)
        if not zone:
            return
        
        zone_data = zone['profile']
        temp_threshold = zone_data.get('temp_threshold', 50.0)
        smoke_threshold = zone_data.get('smoke_threshold', 500.0)

        alarm_type = ""
        
        if val_temp > temp_threshold:
            alarm_type = "Temperature"
        elif val_smoke > smoke_threshold:
            alarm_type = "Smoke"
        elif val_flame:
            alarm_type = "Flame"

        if alarm_type:
            self._trigger_alarm(zone_id, alarm_type)
        else:
            status = zone['data']['status']
            if status == "Active":
                existing = db_service.query_drs("alarm", {
                    "profile.zone_id": zone_id,
                    "profile.trigger_cause": "manual",
                    "data.end_time": None
                })
                if not(existing): self.send_command(zone['profile']['mac_address'], "stop_alarm")
            


    def _trigger_alarm(self, zone_id, alarm_type):
        db_service = current_app.config['DB_SERVICE']
        dr_factory = current_app.config['DR_FACTORY']['alarm']

        logger.warning(f"Triggering alarm for zone {zone_id} due to {alarm_type}")

        zone = db_service.get_dr('zone', zone_id)
        zone['data']['status'] = alarm_type
        zone['metadata']['updated_at'] = datetime.now(timezone.utc)
        db_service.update_dr('zone', zone_id, zone)

        existing = db_service.query_drs("alarm", {
            "profile.zone_id": zone_id,
            "data.end_time": None
        })

        if not existing:
            alarm_data = {
                "zone_id": zone_id,
                "trigger_cause": alarm_type,
                "start_time": datetime.now(timezone.utc),
                "end_time": None,
                "created_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc)
            }
            try:
                alarm_dr = dr_factory.create_dr('alarm', alarm_data)
                alarm_id = db_service.save_dr('alarm', alarm_dr)
            except Exception as e:
                logger.error(f"Error triggering alarm: {e}")
        
        nodes = db_service.query_drs('node', {'profile.zone_id': zone_id})
        for node in nodes:
            self.send_command(node['profile']['mac_address'], "actuate_alarm")
    
    def _handle_discovery(self, mac_address):
        db_service = current_app.config['DB_SERVICE']
        dr_factory = current_app.config['DR_FACTORY']['node']

        if db_service.query_drs('node', {'profile.mac_address': mac_address}):
            return
        
        logger.info(f"New node: {mac_address}")

        try:
            node_data = {
                'profile': {
                    'mac_address': mac_address,
                    'zone_id': "",
                },
                'data': {
                    'status': 'Provisioning',
                    'last_seen': datetime.now(timezone.utc),
                },
                'metadata': {
                'created_at': datetime.now(timezone.utc),
                'updated_at': datetime.now(timezone.utc),
            }}
            node_dr = dr_factory.create_dr('node', node_data)
            db_service.save_dr('node', node_dr)
        except Exception as e:
            logger.error(f"Error handling provisioning: {e}")
    
        
    def send_command(self, mac_address, command):
        if self.client.is_connected():
            topic = f"devices/{mac_address}/command"
            self.client.publish(topic, command)
            logger.info(f"Sent {command} to {mac_address}")
        else:
            logger.warning("MQTT client not connected. Command not sent.")

    
