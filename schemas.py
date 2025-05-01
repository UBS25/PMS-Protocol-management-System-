from pydantic import BaseModel, EmailStr, constr, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
from pydantic import ConfigDict  # ✅ Add this

from sqlalchemy import Column, Integer, String, TIMESTAMP
# app/schemas.py

class UserCreate(BaseModel):
    email: EmailStr
    username: str
    password: str
    contact_number: str


class UserLogin(BaseModel):
    username: str
    password: str


class PasswordUpdate(BaseModel):
    username: str
    new_password: str


class RS485ConfigCreate(BaseModel):
    com_port: str
    baud_rate: int
    data_bits: int
    stop_bits: int
    parity: str
    slave_id: int


class ReadingParametersCreate(BaseModel):
    function_code: int
    start_address: Optional[int] = None
    end_address: Optional[int] = None
    specific_address: Optional[int] = None
    combine_register: str


class CombinedConfigCreate(BaseModel):
    com_port: str
    baud_rate: int
    data_bits: int
    stop_bits: int
    parity: str
    slave_id: int
    function_code: int
    start_address: Optional[int] = None
    end_address: Optional[int] = None
    specific_address: Optional[int] = None
    combine_register: str


class CombinedConfigUpdate(BaseModel):
    com_port: Optional[str] = None
    baud_rate: Optional[int] = None
    data_bits: Optional[int] = None
    stop_bits: Optional[int] = None
    parity: Optional[str] = None
    slave_id: Optional[int] = None
    function_code: Optional[int] = None
    start_address: Optional[int] = None
    end_address: Optional[int] = None
    specific_address: Optional[int] = None
    combine_register: Optional[str] = None


class MachineConfigResponse(BaseModel):
    machine_id: int
    machine_name: str
    machine_protocol: str
    machine_photo: str
    rs485_config: Optional[dict]
    reading_parameters: Optional[dict]
    created_at: datetime

    class Config:
        from_attributes = True


class DataStorageConfigCreate(BaseModel):
    store_in_database: str
    store_in_opcua_server: str


class DataStorageConfigUpdate(BaseModel):
    store_in_database: Optional[bool] = None
    store_in_opcua_server: Optional[bool] = None

class ModbusConfig(BaseModel):
    port: str
    slave_address: int
    baudrate: int
    parity: str
    stopbits: int
    timeout: float


# Pydantic models for request validation
# Add these models to your schemas.py
class RegisterConfig(BaseModel):
    register_number: int
    is_selected: bool

class MachineRegisters(BaseModel):
    raw_registers: List[RegisterConfig]
    actual_registers: List[RegisterConfig]

# '''''below is the schema for specific register values storing class schemma'''''
class RegisterValueItem(BaseModel):
    register: str
    value: str
    status: str

class RegisterValueBatch(BaseModel):
    machine_id: int
    raw_values: List[RegisterValueItem] = []
    actual_values: List[RegisterValueItem] = []


# ''''opcua''''''''''''''''
class OPCUAConnectionParams(BaseModel):
    """Schema for OPC UA connection parameters"""
    url: str
    username: Optional[str] = None
    password: Optional[str] = None

class OPCUAConnectionBase(BaseModel):
    """Base schema for OPC UA connection"""
    machine_id: int
    server_url: str
    last_connected: datetime

class OPCUAConnectionCreate(OPCUAConnectionBase):
    """Schema for creating OPC UA connection"""
    pass

class OPCUAConnection(OPCUAConnectionBase):
    """Schema for OPC UA connection response"""
    id: int
    created_at: datetime

    class Config:
        from_attributes = True

class OPCUAReadingBase(BaseModel):
    """Base schema for OPC UA reading"""
    machine_id: int
    node_id: str
    node_description: str
    value_type: str
    value_numeric: Optional[float] = None
    value_text: Optional[str] = None
    value_boolean: Optional[bool] = None
    timestamp: datetime

class OPCUAReadingCreate(OPCUAReadingBase):
    """Schema for creating OPC UA reading"""
    pass

class OPCUAReading(OPCUAReadingBase):
    """Schema for OPC UA reading response"""
    id: int

    class Config:
        from_attributes = True

class OPCUANodeResponse(BaseModel):
    """Schema for OPC UA node read response"""
    status: str
    machine_id: int
    data: dict


class NodeDetail(BaseModel):
    node_id: str
    description: str
    type: str

class NodeRetrievalResponse(BaseModel):
    status: str
    folders: List[NodeDetail]
    parameters: List[NodeDetail]
    total_folders: int
    total_parameters: int
    execution_time_seconds: float


# Pydantic models for request/response
class NodeInfo(BaseModel):
    node_id: str
    description: str
    type: str
    has_children: Optional[bool] = False

class BrowseResponse(BaseModel):
    status: str
    folders: List[NodeInfo]
    parameters: List[NodeInfo]
    total_folders: int
    total_parameters: int
    execution_time_seconds: float


# '''''''opcua server code ''''''''''''

class OPCUAServerConnect(BaseModel):
    url: str
    username: Optional[str] = ""
    password: Optional[str] = ""
    machine_name: Optional[str] = ""

class OPCUAServerResponse(BaseModel):
    id: int
    url: str
    username: Optional[str]
    machine_name: Optional[str]
    last_connected: datetime
    created_at: datetime

    class Config:
        from_attributes = True



# ''''''''''''''newly opcua enpoints schema''''''''''

class ConnectionParams(BaseModel):
    url: str
    username: str = ""
    password: str = ""
    machine_name: str = ""

class NodeRequest(BaseModel):
    node_id: str

class NodeInfo(BaseModel):
    node_id: str
    description: str
    type: str

class FolderInfo(BaseModel):
    node_id: str
    description: str

class ParameterInfo(BaseModel):
    node_id: str
    description: str

class NodesResponse(BaseModel):
    status: str
    folders: List[FolderInfo]
    parameters: List[ParameterInfo]
    total_folders: int
    total_parameters: int
    execution_time_seconds: float

class ConnectionResponse(BaseModel):
    status: str
    message: str
    connection_id: str
    machine_name: str

class DisconnectionResponse(BaseModel):
    status: str
    message: str
    connection_id: str

class ReadNodeResponse(BaseModel):
    status: str
    node_id: str
    type: str
    description: str
    connection_id: str
    machine_name: str
    value: Optional[Any] = None



# '''''''''''''Below Code is the schema for MODBUS TCP/IP ''''''''''''''
# Request Models
# Pydantic model for request/response
class ModbusTCPConfigBase(BaseModel):
    ip_address: str
    port: int = 502
    unit_id: int = 1
    read_address: Optional[int] = None
    read_count: Optional[int] = None
    function_code: Optional[int] = None
    machine_protocol: Optional[str] = None


class ModbusTCPConfigCreate(ModbusTCPConfigBase):
    machine_id: int


class ModbusTCPConfigResponse(ModbusTCPConfigBase):
    id: int
    machine_id: int

    class Config:
        orm_mode = True


# Schema for the read data
class ModbusReadDataBase(BaseModel):
    machine_id: int
    function_code: int
    register_address: int
    value: float
    timestamp: datetime

    class Config:
        orm_mode = True

class RegisterSelectionCreate(BaseModel):
    machine_id: str
    register_selections: List[dict]
# '''''''''shcema for this configuration storing opcua configuration details class schema'''''''''''

# Pydantic models for request/response
class OPCUACreate(BaseModel):
    server_url: str
    security_policy: str
    security_mode: str
    username: str
    password: str
    store_in_opcua_server: Optional[bool] = False
    store_in_database: Optional[bool] = False

class OPCUAResponse(BaseModel):
    id: int
    machine_id: int
    machine_name: str
    machine_protocol: Optional[str] = None  # Make it optional
    server_url: str
    security_policy: Optional[str] = None
    security_mode: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    store_in_opcua_server: Optional[str] = "false"
    store_in_database: Optional[str] = "false"

    class Config:
        orm_mode = True



# ''''''''''beflow schema is for profinet''''''''''

class ProfinetConfigBase(BaseModel):
    oem_type: str
    ip_address: str
    port: Optional[int] = None  # Optional port field, defaults to None if not provided

class SiemensProfinetConfigSchema(ProfinetConfigBase):
    rack: int  # Required for Siemens
    slot: int  # Required for Siemens

class BeckhoffProfinetConfigSchema(ProfinetConfigBase):
    ams_net_id: str
    target_ads_port: int

class PhoenixProfinetConfigSchema(ProfinetConfigBase):
    device_name: str
    vlan_id: int

class ABBProfinetConfigSchema(ProfinetConfigBase):
    device_id: str
    subnet_mask: str

class BRProfinetConfigSchema(ProfinetConfigBase):
    node_number: int
    cycle_time: int

class ProfinetConfigResponse(BaseModel):
    machine_id: int
    machine_name: str
    machine_protocol: str
    machine_photo: Optional[str]
    created_at: datetime
    profinet_config: Optional[Dict[str, Any]] = None

    class Config:
        from_attributes = True

class ProfinetConfigUpdateBase(BaseModel):
    ip_address: Optional[str] = None
    port: Optional[int] = None

class SiemensProfinetConfigUpdate(ProfinetConfigUpdateBase):
    rack: Optional[int] = None
    slot: Optional[int] = None

class BeckhoffProfinetConfigUpdate(ProfinetConfigUpdateBase):
    ams_net_id: Optional[str] = None
    target_ads_port: Optional[int] = None

class PhoenixProfinetConfigUpdate(ProfinetConfigUpdateBase):
    device_name: Optional[str] = None
    vlan_id: Optional[int] = None

class ABBProfinetConfigUpdate(ProfinetConfigUpdateBase):
    device_id: Optional[str] = None
    subnet_mask: Optional[str] = None

class BRProfinetConfigUpdate(ProfinetConfigUpdateBase):
    node_number: Optional[int] = None
    cycle_time: Optional[int] = None

class ProfinetUpdateRequest(BaseModel):
    oem_type: str
    config: dict


class ProfinetReadField(BaseModel):
    data_source: str  # "db", "input", "output", "memory"
    db_number: Optional[int] = None
    offset: int
    data_type: str  # "int", "real", "bool", "string", "char"
    count: int = 1  # Default to reading one register

class ProfinetReadRequest(BaseModel):  # Renamed from ProfinetRawData
    machine_id: int
    fields: List[ProfinetReadField]

class ProfinetReadResponse(BaseModel):  # Add response model
    status: str
    message: str
    machine_id: int
    oem_type: str
    data: List[dict]
    raw_results: List[dict]
    opcua_url: Optional[str] = None




# ''''''''''''''''''MQTT schemas'''''''''''''''''


class MqttConfigurationBase(BaseModel):
    broker: str = Field(..., description="MQTT Broker Address")
    port: int = Field(..., description="MQTT Broker Port")
    username: Optional[str] = None
    password: Optional[str] = None

class MqttConfigurationCreate(MqttConfigurationBase):
    pass

class MqttConfigurationResponse(MqttConfigurationBase):
    id: int
    machine_id: int
    machine_name: str
    created_at: datetime

    class Config:
        orm_mode = True

class MqttConfigurationUpdate(BaseModel):
    broker: Optional[str] = None
    port: Optional[int] = None
    username: Optional[str] = None
    password: Optional[str] = None
# ''''''''''''''''''''
class MqttSubscription(BaseModel):
    machine_id: int = Field(..., description="Machine ID to connect to")
    topic: str = Field(..., description="MQTT topic to subscribe to")
    timeout: Optional[int] = 5  # Optional with default value of 5

# '''''''new schema of mqtt foe to get availabale topics'''''


# '''''''''this is retrive the data from the databse using start and end date for modbus rtu those schema below
# Pydantic models for response
class ModbusRawDataResponse(BaseModel):
    id: int
    timestamp: datetime
    machine_id: int
    function_code: int
    register_address: int
    raw_value: Optional[Dict[str, Any]]
    status: str
    error_message: Optional[str]

    class Config:
        orm_mode = True


class ModbusActualDataResponse(BaseModel):
    id: int
    timestamp: datetime
    machine_id: int
    function_code: int
    register_address: int
    actual_value: Optional[float]
    status: str
    error_message: Optional[str]

    class Config:
        orm_mode = True

# '''''coapp schema for configuration storing table
# Pydantic models for request and response

# class CoAPPConfigCreate(BaseModel):
#         ip_address: str
#         port: int
#
# class CoAPPConfigResponse(BaseModel):
#         id: int
#         machine_id: int
#         ip_address: str
#         port: int
#         created_at: datetime
#
#         class Config:
#             orm_mode = True
# Updated Pydantic models
class CoAPPConfigCreate(BaseModel):
    ip_address: str
    port: int

class CoAPPConfigResponse(BaseModel):
    id: int
    machine_id: int
    machine_name: str  # Keeps the machine_name field
    machine_protocol: str  # Add this line
    ip_address: str
    port: int
    created_at: datetime

    class Config:
        orm_mode = True

class SensorDataResponse(BaseModel):
    machine_id: int
    machine_name: str
    sensor_type: str
    value: float
    timestamp: datetime

#''''''''''''''''''''''amqp'''''''''''''''''
class AmqpConfigurationCreate(BaseModel):
    queue: str
    exchange: str
    routing_key: str
    host: str
    port: int
    username: str
    password: str
    virtual_host: Optional[str] = "/"

    class Config:
        from_attributes = True  # For Pydantic v2

# 🔧 Add this 👇 for the update endpoint:
class AmqpConfigurationUpdate(BaseModel):
    queue: Optional[str] = None
    exchange: Optional[str] = None
    routing_key: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    username: Optional[str] = None
    password: Optional[str] = None
    virtual_host: Optional[str] = "/"

    model_config = ConfigDict(from_attributes=True)

from typing import Optional
class AmqpConfigurationResponse(BaseModel):
    host: str
    port: int
    username: str
    password: str
    machine_id: str
    class Config:
        from_attributes = True  # for Pydantic v2

class AmqpSubscription(BaseModel):
    machine_id: int
    queue: str


