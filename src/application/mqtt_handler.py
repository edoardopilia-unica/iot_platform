import logging
import json
import paho.mqtt.client as mqtt
from datetime import datetime, timezone
from flask import current_app

logger = logging.getLogger(__name__)

class MQTTHandler:
    def __init__(self, app):
        self.app = app
        self.client = mqtt.Client()
        
        #Callback
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.db_service = current_app.config['DB_SERVICE']
        
        self._isrunning = False

    def start(self):
        broker = self.app.config.get('MQTT_BROKER', 'mqtt')
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
            self.client.subscribe("devices/+/sensors/+")

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
                if category == "sensors" and len(parts) > 3:
                    sensor_type = parts[3]
                    self._process_sensor_data(mac_address, sensor_type, payload_str)
                    return
                
            except Exception as e:
                logger.error(f"Error processing MQTT message: {e}")
    

    def _process_sensor_data(self, mac_address, sensor_type, payload_str):
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
        
        nodes = self.db_service.query_drs('node', {'profile.mac_address': mac_address})
        if not nodes:
            self._handle_discovery(mac_address)
            return
        
        node = nodes[0]
        node_id = node['_id']
        zone_id = node['profile'].get('zone_id')

        entity_data = node['entity']['data']
        entity_data['last_seen'] = datetime.now(timezone.utc)
        if sensor_type == "flame": entity_data['flame_detected'] = val_flame
        if sensor_type == "smoke": entity_data['smoke_level'] = val_smoke
        if sensor_type == "temp": entity_data['temp_level'] = val_temp

        node['metadata']['updated_at'] = datetime.now(timezone.utc)
        self.db_service.update_dr('node', node_id, node)

        if zone_id and entity_data['status'] == "Active":
            self._check_zone_thresholds(zone_id, val_temp, val_smoke, val_flame, sensor_type)
        
    def _check_zone_thresholds(self, zone_id, val_temp, val_smoke, val_flame, sensor_type):
        zone = self.db_service.get_dr('zone', zone_id)
        if not zone:
            return
        
        zone_data = zone['entity']['data']
        temp_threshold = zone_data.get('temp_threshold', 50.0)
        smoke_threshold = zone_data.get('smoke_threshold', 500.0)

        alarm_triggered = False
        alarm_type = ""

        if sensor_type == "temp" and val_temp > temp_threshold:
            alarm_triggered = True
            alarm_type = "Temperature"
        elif sensor_type == "smoke" and val_smoke > smoke_threshold:
            alarm_triggered = True
            alarm_type = "Smoke"
        elif sensor_type == "flame" and val_flame:
            alarm_triggered = True
            alarm_type = "Flame"

        if alarm_triggered:
            alarm_data = {
                'zone_id': zone_id,
                'alarm_type': alarm_type,
                'triggered_at': datetime.now(timezone.utc),
                'status': 'Active'
            }
            alarm_dr = current_app.config['DT_FACTORY'].create_dr('alarm', alarm_data)
            self.db_service.insert_dr('alarm', alarm_dr)
            logger.warning(f"Alarm triggered in Zone {zone_id} due to {alarm_type} sensor.")