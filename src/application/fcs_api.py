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