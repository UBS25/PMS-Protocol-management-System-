import asyncio
import threading
from abc import abstractmethod, ABC
from pydantic import BaseModel

import aiocoap
from fastapi.logger import logger
from datetime import datetime
from operator import and_
from typing import Optional, List, Any

from opcua import ua,Server
from sqlalchemy import Column, Integer, String, DateTime, Boolean, Text, ForeignKey, JSON, Date, Float, Time, Index, \
    Numeric, TIMESTAMP
from sqlalchemy.sql import func
from sqlalchemy.orm import validates, relationship, Session
import re

from typing import Dict

from .database import Base, SessionLocal

class SensorValue(Base):
    __tablename__ = "sensor_values"

    id = Column(Integer, primary_key=True, index=True)
    machine_id = Column(Integer, nullable=False)
    sensor_type = Column(String, nullable=False)
    value = Column(Float, nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow)

class User(Base):
    __tablename__ = "users"
    __table_args__ = {'schema': 'protocols'}

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True, nullable=False)
    username = Column(String(50), unique=True, index=True, nullable=False)
    password = Column(String, nullable=False)
    contact_number = Column(String(20))
    full_name = Column(String(100))
    is_active = Column(Boolean, default=True)
    is_verified = Column(Boolean, default=False)
    last_login = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    reset_token = Column(String)
    reset_token_expires = Column(DateTime(timezone=True))
    preferences = Column(Text, default="{}")

    @validates('email')
    def validate_email(self, key, email):
        if not email:
            raise ValueError("Email is required")
        if not re.match(r"[^@]+@[^@]+\.[^@]+", email):
            raise ValueError("Invalid email format")
        return email

    @validates('contact_number')
    def validate_phone(self, key, number):
        if number and not re.match(r'^\+?1?\d{9,15}$', number):
            raise ValueError("Invalid phone number format")
        return number

    @validates('username')
    def validate_username(self, key, username):
        if not username:
            raise ValueError("Username is required")
        if len(username) < 3:
            raise ValueError("Username must be at least 3 characters long")
        if not re.match("^[a-zA-Z0-9_.-]+$", username):
            raise ValueError("Username can only contain letters, numbers, and _.-")
        return username
    
class Machine(Base):
    __tablename__ = "machine_details"
    __table_args__ = {"schema": "protocols", "extend_existing": True}

    id = Column(Integer, primary_key=True, index=True)
    machine_name = Column(String)
    machine_photo = Column(String)
    machine_protocol = Column(String)
    created_at = Column(TIMESTAMP(timezone=True))

class MachineDetails(Base):
    __tablename__ = "machine_details"
    __table_args__ = {"schema": "protocols", "extend_existing": True}

    id = Column(Integer, primary_key=True, autoincrement=True)
    machine_name = Column(String, nullable=False)
    machine_photo = Column(String, nullable=False)
    machine_protocol = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    register_values = relationship("RegisterValue", back_populates="machine")
    profinet_raw_data = relationship("ProfinetRawData", back_populates="machine")
    mqtt_configs = relationship("MqttConfiguration", back_populates="machine")
    node_values = relationship("NodeValue", back_populates="machine")
    coapp_configs = relationship("CoAPPConfigurations", back_populates="machine")
    amqp_configs = relationship("AmqpConfiguration", back_populates="machine")

class RS485Config(Base):
    __tablename__ = "rs485_configurations"
    __table_args__ = {'schema': 'protocols'}

    id = Column(Integer, primary_key=True, index=True)
    machine_id = Column(Integer, ForeignKey('protocols.machine_details.id'), nullable=False)
    com_port = Column(String, nullable=False)
    baud_rate = Column(Integer, nullable=False)
    data_bits = Column(Integer, nullable=False)
    stop_bits = Column(Integer, nullable=False)
    parity = Column(String, nullable=False)
    slave_id = Column(Integer, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class ReadingParameters(Base):
    __tablename__ = "reading_parameters"
    __table_args__ = {'schema': 'protocols'}

    id = Column(Integer, primary_key=True, index=True)
    machine_id = Column(Integer, ForeignKey('protocols.machine_details.id'), nullable=False)
    function_code = Column(Integer, nullable=False)
    start_address = Column(Integer)
    end_address = Column(Integer)
    specific_address = Column(Integer)
    combine_register = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class DataStorageConfig(Base):
    __tablename__ = "data_storage_config"
    __table_args__ = {'schema': 'protocols'}

    id = Column(Integer, primary_key=True, index=True)
    machine_id = Column(Integer, ForeignKey('protocols.machine_details.id'), nullable=False)
    store_in_database = Column(String, nullable=False)
    store_in_opcua_server = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class StatCard(Base):
    __tablename__ = "stat_cards"
    __table_args__ = {'schema': 'protocols'}

    id = Column(Integer, primary_key=True, index=True)
    machine_id = Column(Integer, ForeignKey('protocols.machine_details.id'))
    machine_photo = Column(String)
    config_data = Column(JSON)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

# Add this model to your models.py
class RegisterConfig(Base):
    __tablename__ = "register_configs"
    __table_args__ = {'schema': 'protocols'}

    id = Column(Integer, primary_key=True, index=True)
    machine_id = Column(Integer, ForeignKey("protocols.machine_details.id"))
    register_number = Column(Integer)
    is_raw = Column(Boolean)  # True = raw register, False = processed
    is_selected = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)


# '''''''below is the specific registere value storage class''''''''''
class RegisterValue(Base):
    __tablename__ = "register_values"
    __table_args__ = {'schema': 'protocols'}

    id = Column(Integer, primary_key=True, index=True)
    machine_id = Column(Integer, ForeignKey("protocols.machine_details.id"))
    register_number = Column(Integer)
    is_raw = Column(Boolean)
    value = Column(String)
    status = Column(String)
    timestamp = Column(DateTime, default=datetime.utcnow)

    # Establish relationship to MachineDetails model
    machine = relationship("MachineDetails", back_populates="register_values")

class OPCUAServerManager:
    _instances: Dict[int, "OPCUAServerManager"] = {}
    _lock = threading.Lock()

    def __new__(cls, machine_id: int):
        with cls._lock:
            if machine_id not in cls._instances:
                instance = super().__new__(cls)
                cls._instances[machine_id] = instance
                return instance
            return cls._instances[machine_id]

    def __init__(self, machine_id: int):
        if not hasattr(self, 'initialized'):
            self.machine_id = machine_id
            self.port = 4840 + machine_id
            self.endpoint = f"opc.tcp://localhost:{self.port}/freeopcua/server/"
            self.server: Optional[Server] = None
            self.namespace_idx = None
            self.machine_obj = None
            self.server_thread = None
            self.initialized = True
            logger.info(f"Initialized OPC UA Server Manager for machine {machine_id}")

    def start_server(self) -> None:
        """Start the OPC UA server if it's not already running."""
        if self.server is None:
            try:
                self.server = Server()
                self.server.set_endpoint(self.endpoint)
                self.namespace_idx = self.server.register_namespace("http://example.org")

                # Create the machine object
                root = self.server.nodes.objects
                machine_obj_name = f"Machine_{self.machine_id}"
                self.machine_obj = root.add_object(self.namespace_idx, machine_obj_name)

                # Start server in a separate thread
                self.server_thread = threading.Thread(
                    target=self._run_server,
                    daemon=True
                )
                self.server_thread.start()
                logger.info(f"Started OPC UA server for machine {self.machine_id}")
            except Exception as e:
                logger.error(f"Failed to start OPC UA server: {str(e)}")
                raise

    def _run_server(self) -> None:
        """Internal method to run the server."""
        try:
            self.server.start()
            logger.info(f"OPC UA Server running at {self.endpoint}")
        except Exception as e:
            logger.error(f"Server failed to start: {str(e)}")
            self.server = None
            raise

    def stop_server(self) -> None:
        """Stop the OPC UA server."""
        if self.server:
            try:
                self.server.stop()
                if self.server_thread:
                    self.server_thread.join(timeout=5)
                self.server = None
                self.server_thread = None
                logger.info(f"Stopped OPC UA server for machine {self.machine_id}")
            except Exception as e:
                logger.error(f"Error stopping server: {str(e)}")

    def get_function_code_for_address(self, db: Session, register_address: int) -> Optional[int]:
        """Get the function code for a specific register address from the reading_parameters table."""
        try:
            # Query the reading_parameters table for the matching register address
            parameter = db.query(ReadingParameters).filter(
                and_(
                    ReadingParameters.machine_id == self.machine_id,
                    ReadingParameters.start_address <= register_address,
                    ReadingParameters.end_address >= register_address
                )
            ).first()

            if parameter:
                return parameter.function_code

            # Check for specific address if not found in range
            parameter = db.query(ReadingParameters).filter(
                and_(
                    ReadingParameters.machine_id == self.machine_id,
                    ReadingParameters.specific_address == register_address
                )
            ).first()

            return parameter.function_code if parameter else None

        except Exception as e:
            logger.error(f"Error retrieving function code for address {register_address}: {str(e)}")
            return None

    # def update_nodes(self, db: Session, results: List[dict], data_type: str) -> None:
    #     """Update or create nodes based on the results."""
    #     if not self.server:
    #         self.start_server()
    #
    #     try:
    #         for result in results:
    #             if result.get("status") != "success":
    #                 continue
    #
    #             function_code = result["function_code"]
    #             register_addresses = result["register_addresses"]
    #
    #             # Handle both raw and actual data formats
    #             if data_type == "actual":
    #                 # For actual data, handle both single values and lists
    #                 data_values = result["data"] if isinstance(result["data"], list) else [result["data"]]
    #             else:
    #                 # For raw data
    #                 data_values = result["data"] if isinstance(result["data"], list) else [result["data"]]
    #
    #             # Create a folder for this function code if it doesn't exist
    #             folder_name = f"FC{function_code}_{data_type}"  # Separate folders for raw and actual data
    #             try:
    #                 fc_folder = self.machine_obj.get_child([f"{self.namespace_idx}:{folder_name}"])
    #             except ua.uaerrors.BadNoMatch:
    #                 fc_folder = self.machine_obj.add_object(self.namespace_idx, folder_name)
    #
    #             # Create/update nodes for each register
    #             for reg_addr, value in zip(register_addresses, data_values):
    #                 node_name = f"Register_{reg_addr}"
    #
    #                 try:
    #                     # Try to find existing node
    #                     existing_node = fc_folder.get_child([f"{self.namespace_idx}:{node_name}"])
    #                     existing_node.set_value(float(value) if isinstance(value, (int, float)) else value)
    #                     logger.debug(f"Updated existing node {node_name} with value {value}")
    #                 except ua.uaerrors.BadNoMatch:
    #                     # Create new node if it doesn't exist
    #                     new_node = fc_folder.add_variable(
    #                         self.namespace_idx,
    #                         node_name,
    #                         float(value) if isinstance(value, (int, float)) else value,
    #                         ua.VariantType.Float if isinstance(value, (int, float)) else ua.VariantType.String
    #                     )
    #                     new_node.set_writable()
    #
    #                     # Add properties to the node
    #                     new_node.add_property(
    #                         self.namespace_idx,
    #                         "RegisterAddress",
    #                         reg_addr
    #                     )
    #                     new_node.add_property(
    #                         self.namespace_idx,
    #                         "DataType",
    #                         data_type
    #                     )
    #                     new_node.add_property(
    #                         self.namespace_idx,
    #                         "FunctionCode",
    #                         function_code
    #                     )
    #                     logger.info(f"Created new node {node_name} with value {value}")
    #
    #     except Exception as e:
    #         logger.error(f"Error updating nodes: {str(e)}")
    #         raise


    def update_nodes(self, db: Session, results: List[dict], data_type: str) -> None:
        """Update or create nodes based on the results for both Modbus and Profinet protocols."""
        if not self.server:
            self.start_server()

        try:
            if data_type.lower() == "profinet":
                self._update_profinet_nodes(results)
            else:
                self._update_modbus_nodes(db, results, data_type)
        except Exception as e:
            logger.error(f"Error updating nodes: {str(e)}")
            raise

    def _update_profinet_nodes(self, results: List[dict]) -> None:
        """Handle Profinet-specific node creation and updates."""
        try:
            # Create a main folder for Profinet data if it doesn't exist
            try:
                profinet_folder = self.machine_obj.get_child([f"{self.namespace_idx}:Profinet"])
            except ua.uaerrors.BadNoMatch:
                profinet_folder = self.machine_obj.add_object(self.namespace_idx, "Profinet")

            for result in results:
                if "node_id" not in result or "value" not in result:
                    continue

                # Extract data source and offset from node_id
                data_source = result["node_id"].split('.')[0]

                # Create/get folder for data source
                try:
                    source_folder = profinet_folder.get_child([f"{self.namespace_idx}:{data_source}"])
                except ua.uaerrors.BadNoMatch:
                    source_folder = profinet_folder.add_object(self.namespace_idx, data_source)

                node_name = result["node_id"].replace(".", "_")
                value = result["value"]
                data_type = result.get("data_type", "String")

                try:
                    # Try to find and update existing node
                    existing_node = source_folder.get_child([f"{self.namespace_idx}:{node_name}"])
                    existing_node.set_value(self._convert_value(value, data_type))
                    logger.debug(f"Updated existing Profinet node {node_name} with value {value}")
                except ua.uaerrors.BadNoMatch:
                    # Create new node if it doesn't exist
                    new_node = source_folder.add_variable(
                        self.namespace_idx,
                        node_name,
                        self._convert_value(value, data_type),
                        self._get_variant_type(data_type)
                    )
                    new_node.set_writable()

                    # Add metadata properties
                    new_node.add_property(
                        self.namespace_idx,
                        "DataSource",
                        data_source
                    )
                    new_node.add_property(
                        self.namespace_idx,
                        "DataType",
                        data_type
                    )
                    new_node.add_property(
                        self.namespace_idx,
                        "NodeId",
                        result["node_id"]
                    )
                    logger.info(f"Created new Profinet node {node_name} with value {value}")

        except Exception as e:
            logger.error(f"Error updating Profinet nodes: {str(e)}")
            raise

    def _update_modbus_nodes(self, db: Session, results: List[dict], data_type: str) -> None:
        """Handle Modbus-specific node creation and updates (existing functionality)."""
        try:
            for result in results:
                if result.get("status") != "success":
                    continue

                function_code = result["function_code"]
                register_addresses = result["register_addresses"]
                data_values = result["data"] if isinstance(result["data"], list) else [result["data"]]

                # Create a folder for this function code if it doesn't exist
                folder_name = f"FC{function_code}_{data_type}"
                try:
                    fc_folder = self.machine_obj.get_child([f"{self.namespace_idx}:{folder_name}"])
                except ua.uaerrors.BadNoMatch:
                    fc_folder = self.machine_obj.add_object(self.namespace_idx, folder_name)

                # Create/update nodes for each register
                for reg_addr, value in zip(register_addresses, data_values):
                    node_name = f"Register_{reg_addr}"
                    self._create_or_update_modbus_node(fc_folder, node_name, value, reg_addr, data_type, function_code)

        except Exception as e:
            logger.error(f"Error updating Modbus nodes: {str(e)}")
            raise

    def _convert_value(self, value: Any, data_type: str) -> Any:
        """Convert value based on data type."""
        try:
            if data_type.lower() in ['float', 'real', 'double']:
                return float(value)
            elif data_type.lower() in ['int', 'integer', 'short', 'long']:
                return int(value)
            elif data_type.lower() in ['bool', 'boolean']:
                return bool(value)
            else:
                return str(value)
        except (ValueError, TypeError):
            return str(value)

    def _get_variant_type(self, data_type: str) -> ua.VariantType:
        """Get OPC UA variant type based on data type."""
        type_mapping = {
            'float': ua.VariantType.Float,
            'real': ua.VariantType.Float,
            'double': ua.VariantType.Double,
            'int': ua.VariantType.Int32,
            'integer': ua.VariantType.Int32,
            'short': ua.VariantType.Int16,
            'long': ua.VariantType.Int64,
            'bool': ua.VariantType.Boolean,
            'boolean': ua.VariantType.Boolean,
            'string': ua.VariantType.String
        }
        return type_mapping.get(data_type.lower(), ua.VariantType.String)

    def _create_or_update_modbus_node(self, folder, node_name: str, value: Any, register_address: int, data_type: str,
                                      function_code: int) -> None:
        """Create or update a Modbus node with its properties."""
        try:
            # Try to find existing node
            try:
                existing_node = folder.get_child([f"{self.namespace_idx}:{node_name}"])
                existing_node.set_value(float(value) if isinstance(value, (int, float)) else value)
                logger.debug(f"Updated existing node {node_name} with value {value}")
            except ua.uaerrors.BadNoMatch:
                # Create new node if it doesn't exist
                new_node = folder.add_variable(
                    self.namespace_idx,
                    node_name,
                    float(value) if isinstance(value, (int, float)) else value,
                    ua.VariantType.Float if isinstance(value, (int, float)) else ua.VariantType.String
                )
                new_node.set_writable()

                # Add properties to the node
                new_node.add_property(
                    self.namespace_idx,
                    "RegisterAddress",
                    register_address
                )
                new_node.add_property(
                    self.namespace_idx,
                    "DataType",
                    data_type
                )
                new_node.add_property(
                    self.namespace_idx,
                    "FunctionCode",
                    function_code
                )
                logger.info(f"Created new node {node_name} with value {value}")

        except Exception as e:
            logger.error(f"Error creating/updating Modbus node {node_name}: {str(e)}")
            raise


class ModbusRawData(Base):
    __tablename__ = "modbus_raw_data"
    __table_args__ = {
        'schema': 'protocols'
    }

    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    machine_id = Column(Integer, ForeignKey('protocols.machine_details.id'), nullable=False)
    function_code = Column(Integer, nullable=False)
    register_address = Column(Integer, nullable=False)
    raw_value = Column(JSON, nullable=True)  # Store raw register values as JSON
    status = Column(String, nullable=False)  # 'success' or 'error'
    error_message = Column(String, nullable=True)


class ModbusActualData(Base):
    __tablename__ = "modbus_actual_data"
    __table_args__ = {
        'schema': 'protocols'
    }

    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    machine_id = Column(Integer, ForeignKey('protocols.machine_details.id'), nullable=False)
    function_code = Column(Integer, nullable=False)
    register_address = Column(Integer, nullable=False)
    actual_value = Column(Float, nullable=True)  # Store converted float value
    status = Column(String, nullable=False)  # 'success' or 'error'
    error_message = Column(String, nullable=True)






# ''''''''''''''''opcus server class code '''''''''''''''''''''''
class OpcUaConnection(Base):
    __tablename__ = "opc_ua_connections"
    __table_args__ = {'schema': 'protocols'}

    connection_id = Column(String, primary_key=True)
    server_url = Column(String, nullable=False)
    machine_name = Column(String, nullable=False)

    last_connected = Column(DateTime(timezone=True), nullable=False)
    readings = relationship("OpcUaReading", back_populates="connection")

class OpcUaReading(Base):
    __tablename__ = "opc_ua_readings"
    __table_args__ = (
        Index('idx_opc_ua_readings_connection_id', 'connection_id'),
        Index('idx_opc_ua_readings_time', 'time'),
        {'schema': 'protocols'}
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    connection_id = Column(String, ForeignKey('protocols.opc_ua_connections.connection_id'), nullable=False)
    machine_name = Column(String, nullable=False)
    time = Column(DateTime(timezone=True), nullable=False)
    node_id = Column(String, nullable=False)
    node_description = Column(String)
    value_type = Column(String)
    value_numeric = Column(Float, nullable=True)
    value_text = Column(Text, nullable=True)
    value_boolean = Column(Boolean, nullable=True)
    connection = relationship("OpcUaConnection", back_populates="readings")

#
class OpcUaMachineMapping(Base):
    __tablename__ = "opc_ua_machine_mapping"
    __table_args__ = {'schema': 'protocols'}

    machine_id = Column(Integer, ForeignKey('protocols.machine_details.id'), primary_key=True)
    connection_id = Column(String, nullable=False)
    server_url = Column(String, nullable=False)
    last_connected = Column(DateTime(timezone=True), nullable=False)




# ''''''''''''Below CODE OF STARTING OF CLASS  for MODBUS TCP/IP PROTOCOL''''''''''''''''''''''''''

# Models
# Define Modbus TCP/IP configuration model
# class ModbusTCP(Base):
#     __tablename__ = "modbus_tcp_config"
#     __table_args__ = {"schema": "protocols"}
#
#     id = Column(Integer, primary_key=True, index=True)
#     machine_id = Column(Integer, ForeignKey("protocols.machine_details.id"), nullable=False)
#     ip_address = Column(String, nullable=False)
#     port = Column(Integer, nullable=False, default=502)
#     unit_id = Column(Integer, nullable=False, default=1)
#     # New reading parameters
#     read_address = Column(Integer, nullable=True)
#     read_count = Column(Integer, nullable=True)
#     function_code = Column(Integer, nullable=True)


class ModbusTCP(Base):
    __tablename__ = "modbus_tcp_config"
    __table_args__ = {"schema": "protocols"}

    id = Column(Integer, primary_key=True, index=True)
    machine_id = Column(Integer, ForeignKey("protocols.machine_details.id"), nullable=False)
    ip_address = Column(String, nullable=False)
    port = Column(Integer, nullable=False, default=502)
    unit_id = Column(Integer, nullable=False, default=1)
    read_address = Column(Integer, nullable=True)
    read_count = Column(Integer, nullable=True)
    function_code = Column(Integer, nullable=True)

    # Add relationship
    machine = relationship("MachineDetails", backref="modbus_config")

class RegisterSelection(Base):
    __tablename__ = "register_selections"
    __table_args__ = {"schema": "protocols"}

    id = Column(Integer, primary_key=True, index=True)
    machine_id = Column(String, index=True, nullable=False)
    register_number = Column(Integer, nullable=False)
    register_value = Column(String, nullable=False)
    is_selected = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    class Config:
        from_attributes = True  # Instead of orm_mode = True
# ''''''below is configuration details storing added for OPCUA model table class''''''''''''

class OPCUA(Base):
    __tablename__ = "opcua_config"
    __table_args__ = {"schema": "protocols"}
    id = Column(Integer, primary_key=True, index=True)
    machine_id = Column(Integer, ForeignKey("protocols.machine_details.id"), nullable=False)
    server_url = Column(String, nullable=False)
    security_policy = Column(String, nullable=True)
    security_mode = Column(String, nullable=True)
    username = Column(String, nullable=True)
    password = Column(String, nullable=True)
    # Add relationship
    machine = relationship("MachineDetails", backref="opcua_config")



class NodeValue(Base):
    __tablename__ = "node_values"
    __table_args__ = {'schema': 'protocols'}  # Add schema definition

    id = Column(Integer, primary_key=True, index=True)
    machine_id = Column(Integer, ForeignKey("protocols.machine_details.id"))  # Foreign key reference
    server_url = Column(String)  # Assuming you want to store the server URL
    node_id = Column(String)
    value = Column(Float)  # Adjust the type based on your expected value type
    timestamp = Column(DateTime(timezone=True), server_default=func.now())  # Add timezone support
    machine = relationship("MachineDetails", back_populates="node_values")


# '''''''''''below class code is for Profinet configuration details storing '''''''''''''''''''
#profinet endpoints
# '''''see this below tabels are storing configuration details based on th oem type in differnt tables
class ProfinetBaseConfig(Base):
    """Base class for PROFINET configurations"""
    __abstract__ = True
    id = Column(Integer, primary_key=True, index=True)
    machine_id = Column(Integer, ForeignKey('protocols.machine_details.id'), nullable=False)
    ip_address = Column(String, nullable=False)
    port = Column(Integer, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class SiemensProfinetConfig(ProfinetBaseConfig):
    __tablename__ = "siemens_profinet_config"
    __table_args__ = {'schema': 'protocols'}
    rack = Column(Integer, nullable=False)  # Required
    slot = Column(Integer, nullable=False)  # Required

class BeckhoffProfinetConfig(ProfinetBaseConfig):
    __tablename__ = "beckhoff_profinet_config"
    __table_args__ = {'schema': 'protocols'}
    ams_net_id = Column(String, nullable=False)
    target_ads_port = Column(Integer, nullable=False)

class PhoenixProfinetConfig(ProfinetBaseConfig):
    __tablename__ = "phoenix_profinet_config"
    __table_args__ = {'schema': 'protocols'}
    device_name = Column(String, nullable=False)
    vlan_id = Column(Integer, nullable=False)

class ABBProfinetConfig(ProfinetBaseConfig):
    __tablename__ = "abb_profinet_config"
    __table_args__ = {'schema': 'protocols'}
    device_id = Column(String, nullable=False)
    subnet_mask = Column(String, nullable=False)

class BRProfinetConfig(ProfinetBaseConfig):
    __tablename__ = "br_profinet_config"
    __table_args__ = {'schema': 'protocols'}
    node_number = Column(Integer, nullable=False)
    cycle_time = Column(Integer, nullable=False)


# '''see this below model class to store the reading data of profinet

class ProfinetRawData(Base):
    __tablename__ = "profinet_raw_data"
    __table_args__ = {'schema': 'protocols'}  # Add schema definition

    id = Column(Integer, primary_key=True, index=True)
    machine_id = Column(Integer, ForeignKey("protocols.machine_details.id"))  # Update foreign key reference
    oem_type = Column(String)
    data_source = Column(String)
    offset = Column(Integer)
    data_type = Column(String)
    value = Column(String)
    status = Column(String)
    error_message = Column(String, nullable=True)
    timestamp = Column(DateTime(timezone=True), server_default=func.now())  # Add timezone support
    machine = relationship("MachineDetails", back_populates="profinet_raw_data")




# ''''''''this below class is for MQTT '''''''''''''''''
class MqttConfiguration(Base):
    __tablename__ = "mqtt_configurations"
    __table_args__ = {'schema': 'protocols'}  # Add schema definition

    id = Column(Integer, primary_key=True, index=True)
    machine_id = Column(Integer, ForeignKey("protocols.machine_details.id"))
    broker = Column(String, nullable=False)
    port = Column(Integer, nullable=False)
    username = Column(String, nullable=True)
    password = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationship back to Machine Details
    machine = relationship("MachineDetails", back_populates="mqtt_configs")

# '''''''''''coapp'''''''''''''''''''
class CoAPPConfigurations(Base):
    __tablename__ = "coapp_configurations"
    __table_args__ = {'schema': 'protocols'}

    id = Column(Integer, primary_key=True, index=True)
    machine_id = Column(Integer, ForeignKey("protocols.machine_details.id"))
    ip_address = Column(String)  
    port = Column(Integer)
    created_at = Column(DateTime(timezone=True), default=func.now())

    machine = relationship("MachineDetails", back_populates="coapp_configs")

# '''''''''''amqp'''''''''''''''''''
class AmqpConfiguration(Base):
    __tablename__ = "amqp_configurations"
    __table_args__ = {'schema': 'protocols'}

    id = Column(Integer, primary_key=True, index=True)
    host = Column(String, nullable=False)
    port = Column(Integer, nullable=False)
    username = Column(String, nullable=False)
    password = Column(String, nullable=False)
    virtual_host = Column(String, nullable=False)
    exchange = Column(String, nullable=False)
    queue = Column(String, nullable=False)
    routing_key = Column(String, nullable=False)
    machine_id = Column(Integer, ForeignKey("protocols.machine_details.id"))
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Reverse relationship to MachineDetails
    machine = relationship("MachineDetails", back_populates="amqp_configs")

class AmqpConfigurationCreate(BaseModel):
    host: str
    port: int
    username: Optional[str]
    password: Optional[str]

class AmqpConfigurationUpdate(BaseModel):
    host: Optional[str]
    port: Optional[int]
    username: Optional[str]
    password: Optional[str]

class AmqpConfigurationResponse(AmqpConfigurationCreate):
    machine_id: int

class AmqpSubscription(BaseModel):
    machine_id: int
    queue: str
