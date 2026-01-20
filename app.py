from flask import Flask
from flask_cors import CORS
from src.virtualization.digital_replica.schema_registry import SchemaRegistry
from src.services.database_service import DatabaseService
from src.digital_twin.dt_factory import DTFactory
from src.application.api import register_api_blueprints
from config.config_loader import ConfigLoader


class FlaskServer:
    def __init__(self):
        self.app = Flask(__name__)
        CORS(self.app)
        self._init_components()
        self._register_blueprints()

    def _init_components(self):
        """Initialize all required components and store them in app config"""
        
        # Initialize SchemaRegistry and load schemas
        schema_registry = SchemaRegistry()

        schema_registry.load_schema('zone', 'src/virtualization/templates/zone.yaml')
        schema_registry.load_schema('node', 'src/virtualization/templates/node.yaml')
        schema_registry.load_schema('alarm', 'src/virtualization/templates/alarm.yaml')    

        # Load database configuration
        db_config = ConfigLoader.load_database_config()
        connection_string = ConfigLoader.build_connection_string(db_config)

        # Initialize DatabaseService with populated schema_registry
        db_service = DatabaseService(
            connection_string=connection_string,
            db_name=db_config["settings"]["name"],
            schema_registry=schema_registry,
        )
        db_service.connect()

        # Initialize DTFactory
        dt_factory = DTFactory(db_service, schema_registry)

        # Store references
        self.app.config["SCHEMA_REGISTRY"] = schema_registry
        self.app.config["DB_SERVICE"] = db_service
        self.app.config["DT_FACTORY"] = dt_factory

    def _register_blueprints(self):
        """Register all API blueprints"""
        register_api_blueprints(self.app)

    def run(self, host="0.0.0.0", port=5000, debug=True):
        """Run the Flask server"""
        try:
            self.app.run(host=host, port=port, debug=debug)
        finally:
            # Cleanup on server shutdown
            if "DB_SERVICE" in self.app.config:
                self.app.config["DB_SERVICE"].disconnect()


if __name__ == "__main__":
    server = FlaskServer()
    server.run()
