from flask import Blueprint, request, jsonify, current_app
from datetime import datetime

# Blueprint FCS APIs
fcs_api = Blueprint('fcs_api', __name__, url_prefix='/api/fcs')

# --- Zone Management APIs ---
@fcs_api.route('/zones', methods=['POST'])
def create_zone():
    """Create a new zone"""
    try:
        data = request.get_json()
        if 'name' not in data:
            return jsonify({'error': 'Zone name is required'}), 400
        
        dr_factory = current_app.config['DR_FACTORY']

        zone_data = {
            'name': data['name'],
            'description': data.get('description', ''),
            'created_at': datetime.now(datetime.timezone.utc),
            'updated_at': datetime.now(datetime.timezone.utc),
            'temp_threshold': data.get('temp_threshold', 50.0),
            'smoke_threshold': data.get('smoke_threshold', 500.0)
        }

        zone_dr = dr_factory.create_dr('zone', zone_data)
        zone_id = current_app.config['DB_SERVICE'].insert_dr('zone', zone_dr)
        return jsonify({'zone_id': zone_id, 'message': 'Zone created successfully'}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
@fcs_api.route('/zones/<zone_id>', methods=['GET'])
def get_zone(zone_id):
    """Get zone details"""
    try:
        zone = current_app.config['DB_SERVICE'].get_dr('zone', zone_id)
        if not zone:
            return jsonify({'error': 'Zone not found'}), 404
        return jsonify(zone), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@fcs_api.route('/zones/<zone_id>', methods=['DELETE'])
def delete_zone(zone_id):
    """Delete a zone -- All nodes assigned to this zone will be detached"""
    try:
        db_service = current_app.config['DB_SERVICE']
        
        if not db_service.get_dr('zone', zone_id):
            return jsonify({'error': 'Zone not found'}), 404
        
        nodes_in_zone = db_service.query_drs('node', {'profile.zone_id': zone_id})
        for node in nodes_in_zone:
            node['profile']['zone_id'] = None
            node['entity']['data']['status'] = 'Inactive'
            node['metadata']['updated_at'] = datetime.now(datetime.timezone.utc)
            db_service.update_dr("node", node['_id'], node)
        
        db_service.delete_dr("zone", zone_id)
        return jsonify({'message': f'Zone {zone_id} deleted successfully'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
@fcs_api.route('/zones/<zone_id>/resolve', methods=['PUT'])
def resolve_alarms(zone_id):
    try:
        db_service = current_app.config['DB_SERVICE']
        zone = db_service.get_dr('zone', zone_id)
        if not zone:
            return jsonify({'error': 'Zone not found'}), 404

        zone_status = zone['entity']['data']['status']
        if zone_status == 'Inactive' or zone_status == 'Active':
            return jsonify({'message': f'No active alarms to resolve for zone {zone_id}'}), 200

        active_alarms = db_service.query_drs('alarm', {
            'profile.zone_id': zone_id,
            'entity.data.end_time': None
        })

        now = datetime.now(datetime.timezone.utc)

        if not active_alarms:
            zone['entity']['data']['status'] = 'Active'
            zone['metadata']['updated_at'] = now
            db_service.update_dr('zone', zone_id, zone)
            return jsonify({'message': f'No active alarms to resolve for zone {zone_id}'}), 200
        
        for alarm in active_alarms:
            alarm['entity']['data']['end_time'] = now
            alarm['metadata']['updated_at'] = now
            db_service.update_dr('alarm', alarm['_id'], alarm)
        
        zone['entity']['data']['status'] = 'Active'
        zone['metadata']['updated_at'] = now
        db_service.update_dr('zone', zone_id, zone)

        return jsonify({'message': f'Alarms resolved for zone {zone_id} successfully'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@fcs_api.route('/zones', methods=['GET'])
def get_zones():
    """Get status of all zones"""
    try:
        db_service = current_app.config['DB_SERVICE']
        zones = db_service.query_drs('zone', {})

        result = []
        for zone in zones:
            result.append({
                'id': zone['_id'],
                'name': zone['name'],
                'description': zone['description'],
                'temp_threshold': zone['temp_threshold'],
                'smoke_threshold': zone['smoke_threshold'],
                'status': zone['entity']['data']['status']
            })
        return jsonify(zones), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    

# --- Node Management APIs ---
@fcs_api.route('/nodes', methods=['POST'])
def create_node():
    """Create a new node"""
    try:
        data = request.get_json()
        required_fields = ['mac_address']
        if not all(field in data for field in required_fields):
            return jsonify({'error': 'Missing MAC Address for node'}), 400
        
        dr_factory = current_app.config['DR_FACTORY']

        node_data = {
            'mac_address': data['mac_address'],
            'zone_id': data.get('zone_id', None),
            'created_at': datetime.now(datetime.timezone.utc),
            'updated_at': datetime.now(datetime.timezone.utc),
        }

        node_dr = dr_factory.create_dr('node', node_data)
        node_dr['entity']['data']['status'] = 'Provisioning'
        node_id = current_app.config['DB_SERVICE'].insert_dr('node', node_dr)
        return jsonify({'node_id': node_id, 'message': 'Node created successfully'}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
@fcs_api.route('/nodes/<node_id>/assign/<zone_id>', methods=['PUT'])
def assign_node(node_id, zone_id):
    """Assign a node to a zone"""
    try:
        data = request.get_json()

        zone = current_app.config['DB_SERVICE'].get_dr('zone', zone_id)
        if not zone:
            return jsonify({'error': 'Zone not found'}), 404
        
        node = current_app.config['DB_SERVICE'].get_dr('node', node_id)
        if not node:
            return jsonify({'error': 'Node not found'}), 404
        
        node['profile']['zone_id'] = zone_id
        node['entity']['data']['status'] = 'Active'
        node['metadata']['updated_at'] = datetime.now(datetime.timezone.utc)

        current_app.config['DB_SERVICE'].update_dr('node', node_id, node)
        return jsonify({'message': f'Node {node_id} assigned to zone {zone_id} successfully'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
@fcs_api.route('/nodes/<node_id>/detach', methods=['PUT'])
def detach_node(node_id):
    """Detach a node from a zone"""
    try:
        node = current_app.config['DB_SERVICE'].get_dr('node', node_id)
        if not node:
            return jsonify({'error': 'Node not found'}), 404
        
        zone = node['profile'].get('zone_id', None)
        node['profile']['zone_id'] = None
        node['entity']['data']['status'] = 'Inactive'
        node['metadata']['updated_at'] = datetime.now(datetime.timezone.utc)

        current_app.config['DB_SERVICE'].update_dr('node', node_id, node)
        return jsonify({'message': f'Node {node_id} detached from zone {zone} successfully'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
    
# --- Alarm Management APIs ---
@fcs_api.route('/alarms', methods=['GET'])
def get_alarms():
    """Get all active alarms, ?active=true/false to filter by active/inactive alarms"""
    
    try:
        active_filter = request.args.get('active', None)
        db_service = current_app.config['DB_SERVICE']

        query = {}
        if active_filter == 'true':
            query = {'entity.data.end_time': None}  # Filter for active alarms
        elif active_filter == 'false':
            query = {'entity.data.end_time': not None}  # Filter for inactive alarms
        
        alarms = db_service.query_drs('alarm', query)
        return jsonify(alarms), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@fcs_api.route('/alarms', methods=['POST'])
def trigger_alarm():
    """Trigger a specific alarm"""

    try:
        data = request.get_json()
        if 'zone_id' not in data or 'trigger_cause' not in data:
            return jsonify({'error': 'Missing zone_id or trigger_cause'}), 400
        
        zone_id = data['zone_id']
        trigger_cause = data['trigger_cause']

        valid_causes = ["manual", "temp", "smoke", "flame"]
        if trigger_cause not in valid_causes:
            return jsonify({'error': f'Invalid trigger cause. Must be one of {valid_causes}'}), 400
        
        db_service = current_app.config['DB_SERVICE']

        zone = db_service.get_dr('zone', zone_id)
        if not zone:
            return jsonify({'error': 'Zone not found'}), 404
        
        dr_factory = current_app.config['DR_FACTORY']

        alarm_data = {
            "zone_id": zone_id,
            "trigger_cause": trigger_cause,
            "start_time": datetime.now(datetime.timezone.utc),
            "end_time": None,
            "created_at": datetime.now(datetime.timezone.utc),
            "updated_at": datetime.now(datetime.timezone.utc)
        }
        
        alarm_dr = dr_factory.create_dr('alarm', alarm_data)
        alarm_id = db_service.insert_dr('alarm', alarm_dr)

        zone_status = zone['entity']['data']['status']
        if zone_status == 'Active':
            zone['entity']['data']['status'] = trigger_cause
            zone['metadata']['updated_at'] = datetime.now(datetime.timezone.utc)
            db_service.update_dr('zone', zone_id, zone)
        
        return jsonify({'alarm_id': alarm_id, 'message': 'Alarm triggered successfully'}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# --- Misc ---
@fcs_api.route('/personnel/sync', methods=['POST'])
def acs_sync():
    try:
        data = request.get_json()
        db_service = current_app.config['DB_SERVICE']
        
        zones = db_service.query_drs('zone', {})
        zone_map = {z['profile']['name']: str(z['_id']) for z in zones}
        
        count = 0
        personnel_coll = db_service.db['personnel']

        for p in data:
            z_name = p.get('zone_name')
        
            current_zone_id = None
            if p.get('is_inside') and z_name in zone_map:
                current_zone_id = zone_map[z_name]
            
            personnel_coll.update_one(
                {'badge_id': str(p['badge_id'])},
                {'$set': {
                    'full_name': p['full_name'],
                    'current_zone_id': current_zone_id,
                    'last_update': datetime.now(datetime.timezone.utc)
                }},
                upsert=True
            )
            count += 1
            
        return jsonify({'message': f'Synced {count} personnel records'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
@fcs_api.route('/personnel/<zone_id>', methods=['GET'])
def get_trapped(zone_id):
    try:
        db_service = current_app.config['DB_SERVICE']
        people = list(db_service.db['personnel'].find({
            'current_zone_id': zone_id}))
        result = []
        for p in people:
            result.append({
                'badge_id': p['badge_id'],
                'full_name': p.get('full_name', 'Unknown')
            })
        return jsonify(result), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


