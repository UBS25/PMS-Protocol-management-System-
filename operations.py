import ipaddress
import json
from platform import machine
import threading
import uuid
from socket import socket

import aiocoap
import minimalmodbus
import serial
from datetime import timedelta, datetime
import struct

import snap7
from aiocoap import Context, Message, Code
from pymodbus.client import ModbusTcpClient
from sqlalchemy.orm import Session
from app import models, schemas
import base64
from urllib.parse import unquote
import socket
import json
import logging
import uuid
import asyncio
# '''''''''''''''''''
# Configure logging

import logging
import hashlib
import asyncio
import time
import re
from datetime import datetime
from typing import Dict, List, Set, Optional, Any
from weakref import WeakValueDictionary
from concurrent.futures import ThreadPoolExecutor
import asyncpg
from opcua import Client, ua
from fastapi import HTTPException, BackgroundTasks
from snap7.client import Client as S7Client

from app.schemas import ProfinetReadField

from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from app.models import Machine
from app.models import MachineDetails 
from app.models import SensorValue

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
MAX_CONCURRENT_REQUESTS = 20
BATCH_SIZE = 50
MAX_DEPTH = 10
TIMEOUT = 60

# Global variables
opc_clients: Dict[str, Client] = {}
url_to_connection_id: Dict[str, str] = {}
connection_details: Dict[str, dict] = {}
node_cache: Dict[str, dict] = {}
folder_cache: Set[str] = set()
machine_counter = 0
url_to_machine_name = {}
db_pool = None

# ''''''''''''''''''''''
async def get_machine_by_id(machine_id: int, db: Session):
    return db.query(MachineDetails).filter(MachineDetails.id == machine_id).first()

def combine_registers_and_convert_to_float(registers, endianess="big"):
    """Combine two 16-bit Modbus registers into a 32-bit float."""
    if len(registers) != 2:
        raise ValueError("Exactly two registers are required for conversion to a float.")

    if endianess == "little":
        int_value = (registers[0] << 16) + registers[1]
    else:
        int_value = (registers[1] << 16) + registers[0]

    float_value = struct.unpack('f', struct.pack('I', int_value))[0]
    return float_value

def get_register_values(instrument, function_code, start_address, end_address=None):
    """Request register values based on user input."""
    # Handle case where end_address is 0
    if end_address == 0:
        end_address = None

    result = read_modbus_register(instrument, function_code, start_address, end_address)

    # If the result is a list of registers, return the raw register values
    if isinstance(result, list):
        return {
            "status": "success",
            "message": f"Read raw registers from address {start_address} {f'to {end_address}' if end_address else ''} using function code {function_code}",
            "data": result  # Raw register values
        }

    # If result is already a dict with status
    if isinstance(result, dict) and "status" in result:
        return result

    # In case of an unexpected return type
    return {
        "status": "error",
        "message": "Unexpected result format"
    }

def calculate_quantity(start_address, end_address):
    """Calculate quantity of registers to read."""
    if end_address is None or end_address == 0:
        return 2

    if end_address < start_address:
        raise ValueError("End address must be greater than or equal to start address.")

    return end_address - start_address + 1

def read_modbus_register(instrument, function_code, start_address, end_address=None):
    """Read Modbus registers based on function code."""
    try:
        if end_address == 0:
            end_address = None

        if end_address is None:
            if function_code in [3, 4]:
                return instrument.read_registers(start_address, 2)
            else:
                return {"status": "error", "message": "Unsupported function code for single register read"}

        quantity = calculate_quantity(start_address, end_address)

        if function_code == 1:
            return instrument.read_bits(start_address, quantity)
        elif function_code == 2:
            return instrument.read_bits(start_address, quantity)
        elif function_code == 3:
            return instrument.read_registers(start_address, quantity)
        elif function_code == 4:
            return instrument.read_registers(start_address, quantity)
        else:
            return {"status": "error", "message": "Unsupported function code"}

    except minimalmodbus.ModbusException as e:
        return {"status": "error", "message": f"Modbus Error: {e}"}
    except Exception as e:
        return {"status": "error", "message": f"Failed to read registers: {e}"}

def write_modbus_register(instrument, function_code, start_address, values, end_address=None):
    """Write to Modbus registers."""
    try:
        if end_address is None:
            end_address = start_address

        if len(values) != (end_address - start_address + 1):
            return {"status": "error", "message": "Number of values does not match register range"}

        if function_code == 5:
            return instrument.write_bit(start_address, values[0])
        elif function_code == 6:
            return instrument.write_register(start_address, values[0])
        elif function_code == 15:
            return instrument.write_bits(start_address, values)
        elif function_code == 16:
            return instrument.write_registers(start_address, values)
        else:
            return {"status": "error", "message": "Unsupported function code for writing"}

    except minimalmodbus.ModbusException as e:
        return {"status": "error", "message": f"Modbus Error: {e}"}
    except Exception as e:
        return {"status": "error", "message": f"Failed to write registers: {e}"}

# ''''''?/below is the correct working class for device connection for connect-machine endpoint //'''''''''
# class DeviceConnection:
#     def __init__(self, port: str, slave_address: int, baudrate: int, parity: str, stopbits: int, timeout: float):
#         self.config = {
#             'port': port,
#             'slave_address': slave_address,
#             'baudrate': baudrate,
#             'parity': parity,
#             'stopbits': stopbits,
#             'timeout': timeout
#         }
#         self.instrument = None
#         self.last_check = None
#         self.is_connected = False
#         self.connection_timeout = timedelta(hours=24)  # Increased timeout to 24 hours
#
#     def connect(self):
#         """Establish connection to the device."""
#         parity_map = {
#             'even': serial.PARITY_EVEN,
#             'odd': serial.PARITY_ODD,
#             'none': serial.PARITY_NONE
#         }
#
#         try:
#             if self.is_connected and self.instrument and self.instrument.serial.is_open:
#                 return True
#
#             self.instrument = minimalmodbus.Instrument(
#                 self.config['port'],
#                 self.config['slave_address']
#             )
#             self.instrument.serial.baudrate = self.config['baudrate']
#             self.instrument.serial.bytesize = 8
#             self.instrument.serial.parity = parity_map.get(self.config['parity'].lower(), serial.PARITY_NONE)
#             self.instrument.serial.stopbits = self.config['stopbits']
#             self.instrument.serial.timeout = self.config['timeout']
#
#             # Test the connection with a simple read operation
#             try:
#                 self.instrument.read_registers(0, 1)
#             except minimalmodbus.ModbusException:
#                 # If read fails, the device might be busy - that's okay
#                 pass
#
#             self.is_connected = True
#             self.last_check = datetime.now()
#             return True
#         except Exception as e:
#             self.is_connected = False
#             raise e
#
#     def disconnect(self):
#         """Explicitly disconnect from the device."""
#         try:
#             if self.instrument and self.instrument.serial.is_open:
#                 self.instrument.serial.close()
#             self.is_connected = False
#             self.last_check = None
#             self.instrument = None
#             return True
#         except Exception as e:
#             raise e
#
#     def ensure_connection(self):
#         """Ensure the connection is active and reconnect if necessary."""
#         try:
#             if not self.is_connected or not self.instrument or not self.instrument.serial.is_open:
#                 return self.connect()
#
#             # Update last_check timestamp if connection is still good
#             if self.instrument.serial.is_open:
#                 self.last_check = datetime.now()
#                 return True
#
#             return self.connect()
#         except Exception:
#             return self.connect()
#
#     def is_connection_valid(self) -> bool:
#         """Check if connection is still valid with a more lenient timeout."""
#         if not self.is_connected or not self.last_check:
#             return False
#
#         # Check if we've exceeded the connection timeout
#         if datetime.now() - self.last_check > self.connection_timeout:
#             return False
#
#         # If the connection is marked as connected and within timeout, consider it valid
#         if self.instrument and self.instrument.serial.is_open:
#             return True
#
#         return False
#
#     def update_last_check(self):
#         """Update the last check timestamp."""
#         self.last_check = datetime.now()


# class DeviceConnectionManager:
#     def __init__(self):
#         self.connections: Dict[int, DeviceConnection] = {}
#
#     def add_connection(self, machine_id: int, connection: DeviceConnection) -> bool:
#         """Add a new device connection or update existing one."""
#         try:
#             if machine_id in self.connections:
#                 # If connection exists and is valid, don't recreate it
#                 if self.connections[machine_id].is_connection_valid():
#                     return True
#                 # If invalid, disconnect the old one
#                 self.connections[machine_id].disconnect()
#
#             connection.connect()
#             self.connections[machine_id] = connection
#             return True
#         except Exception as e:
#             raise e
#
#     def remove_connection(self, machine_id: int) -> bool:
#         """Explicitly remove a device connection."""
#         if machine_id in self.connections:
#             try:
#                 self.connections[machine_id].disconnect()
#                 del self.connections[machine_id]
#                 return True
#             except Exception as e:
#                 raise e
#         return False
#
#     def get_connection(self, machine_id: int) -> Optional[DeviceConnection]:
#         """Get a specific device connection and ensure it's active."""
#         connection = self.connections.get(machine_id)
#         if connection:
#             connection.ensure_connection()
#         return connection
#
#     def is_connected(self, machine_id: int) -> bool:
#         """Check if a specific device is connected."""
#         connection = self.get_connection(machine_id)
#         if connection:
#             return connection.is_connection_valid()
#         return False
#
#     def get_client(self, machine_id: int) -> Client:
#         """Get the OPC UA client for a specific machine.
#
#         Args:
#             machine_id (int): The ID of the machine
#
#         Returns:
#             Client: The OPC UA client instance
#
#         Raises:
#             Exception: If the machine is not connected
#         """
#         if machine_id not in self.clients:
#             raise Exception("Machine not connected")
#         return self.clients[machine_id]
#

# '''''this below classs code is added additional logic to connect for modbustcp also''''
#
class BaseDeviceConnection:
    """Base class for device connections"""

    def __init__(self):
        self.last_check = None
        self.is_connected = False
        self.connection_timeout = timedelta(hours=24)

    def connect(self) -> bool:
        raise NotImplementedError

    def disconnect(self) -> bool:
        raise NotImplementedError

    def ensure_connection(self) -> bool:
        raise NotImplementedError

    def is_connection_valid(self) -> bool:
        if not self.is_connected or not self.last_check:
            return False

        if datetime.now() - self.last_check > self.connection_timeout:
            return False

        return True


class ModbusRTUConnection(BaseDeviceConnection):
    def __init__(self, port: str, slave_address: int, baudrate: int, parity: str, stopbits: int, timeout: float):
        super().__init__()
        self.config = {
            'port': port,
            'slave_address': slave_address,
            'baudrate': baudrate,
            'parity': parity,
            'stopbits': stopbits,
            'timeout': timeout
        }
        self.instrument = None

    def connect(self) -> bool:
        parity_map = {
            'even': serial.PARITY_EVEN,
            'odd': serial.PARITY_ODD,
            'none': serial.PARITY_NONE
        }

        try:
            if self.is_connected and self.instrument and self.instrument.serial.is_open:
                return True

            self.instrument = minimalmodbus.Instrument(
                self.config['port'],
                self.config['slave_address']
            )
            self.instrument.serial.baudrate = self.config['baudrate']
            self.instrument.serial.bytesize = 8
            self.instrument.serial.parity = parity_map.get(self.config['parity'].lower(), serial.PARITY_NONE)
            self.instrument.serial.stopbits = self.config['stopbits']
            self.instrument.serial.timeout = self.config['timeout']

            # Test the connection
            try:
                self.instrument.read_registers(0, 1)
            except minimalmodbus.ModbusException:
                pass  # Device might be busy

            self.is_connected = True
            self.last_check = datetime.now()
            return True
        except Exception as e:
            self.is_connected = False
            raise e

    def disconnect(self) -> bool:
        try:
            if self.instrument and self.instrument.serial.is_open:
                self.instrument.serial.close()
            self.is_connected = False
            self.last_check = None
            self.instrument = None
            return True
        except Exception as e:
            raise e

    def ensure_connection(self) -> bool:
        try:
            if not self.is_connected or not self.instrument or not self.instrument.serial.is_open:
                return self.connect()

            if self.instrument.serial.is_open:
                self.last_check = datetime.now()
                return True

            return self.connect()
        except Exception:
            return self.connect()

    def is_connection_valid(self) -> bool:
        if not super().is_connection_valid():
            return False
        return self.instrument and self.instrument.serial.is_open


class ModbusTCPConnection(BaseDeviceConnection):
    def __init__(self, host: str, port: int, timeout: int = 10):
        super().__init__()
        self.host = host
        self.port = port
        self.timeout = timeout
        self.instrument = None

    def connect(self) -> bool:
        try:
            if self.is_connected and self.instrument and self.instrument.is_socket_open():
                return True

            self.instrument = ModbusTcpClient(
                host=self.host,
                port=self.port,
                timeout=self.timeout
            )

            if not self.instrument.connect():
                raise Exception("Failed to connect to Modbus TCP server")

            self.is_connected = True
            self.last_check = datetime.now()
            return True
        except Exception as e:
            self.is_connected = False
            logger.error(f"TCP Connection error: {str(e)}")
            raise e

    def disconnect(self) -> bool:
        try:
            if self.instrument:
                self.instrument.close()
            self.is_connected = False
            self.last_check = None
            self.instrument = None
            return True
        except Exception as e:
            raise e

    def ensure_connection(self) -> bool:
        try:
            if not self.is_connected or not self.instrument or not self.instrument.is_socket_open():
                return self.connect()
            self.last_check = datetime.now()
            return True
        except Exception:
            return self.connect()

    def is_connection_valid(self) -> bool:
        if not super().is_connection_valid():
            return False
        return self.instrument and self.instrument.is_socket_open()


# '''''''''''''''''''//this below class is for Profinet class//''''''''''''''''''''''
class ProfinetConnection(BaseDeviceConnection):
    def __init__(self, ip_address: str, oem_type: str, **kwargs):
        super().__init__()
        self.ip_address = ip_address
        self.oem_type = oem_type.lower()
        self.client = None
        self.config = kwargs

    def connect(self) -> bool:
        try:
            if self.is_connected and self.client:
                return True

            if self.oem_type == "siemens":
                self.client = S7Client()
                rack = self.config.get('rack', 0)
                slot = self.config.get('slot', 1)
                self.client.connect(self.ip_address, rack, slot)

            elif self.oem_type == "beckhoff":
                # self.client = AdsClient()
                ams_net_id = self.config.get('ams_net_id')
                port = self.config.get('target_ads_port')
                self.client.connect(ams_net_id, port)

            elif self.oem_type == "phoenix":
                # Phoenix Contact PLCnext Technology connection
                device_name = self.config.get('device_name')
                vlan_id = self.config.get('vlan_id')
                port = self.config.get('port', 1962)  # Default PLCnext port

                # self.client = PlcNextClient()
                self.client.connect(
                    ip_address=self.ip_address,
                    port=port,
                    device_name=device_name,
                    vlan_id=vlan_id
                )

            elif self.oem_type == "abb":
                # ABB PLC connection
                device_id = self.config.get('device_id')
                subnet_mask = self.config.get('subnet_mask')
                port = self.config.get('port', 502)  # Default Modbus TCP port for ABB

                # self.client = ABBClient()
                self.client.connect(
                    ip_address=self.ip_address,
                    port=port,
                    device_id=device_id,
                    subnet_mask=subnet_mask
                )

            elif self.oem_type == "br":
                # B&R Automation PLC connection
                node_number = self.config.get('node_number')
                cycle_time = self.config.get('cycle_time')
                port = self.config.get('port', 11159)  # Default B&R port

                # self.client = BRClient()
                self.client.connect(
                    ip_address=self.ip_address,
                    port=port,
                    node_number=node_number,
                    cycle_time=cycle_time
                )

            self.is_connected = True
            self.last_check = datetime.now()
            return True

        except Exception as e:
            self.is_connected = False
            logger.error(f"Profinet Connection error: {str(e)}")
            raise e

    def disconnect(self) -> bool:
        try:
            if self.client:
                if self.oem_type == "siemens":
                    self.client.disconnect()
                elif self.oem_type == "beckhoff":
                    self.client.close()
                elif self.oem_type == "phoenix":
                    self.client.disconnect()
                elif self.oem_type == "abb":
                    self.client.close_connection()
                elif self.oem_type == "br":
                    self.client.stop_communication()

            self.is_connected = False
            self.last_check = None
            self.client = None
            return True
        except Exception as e:
            raise e

    def ensure_connection(self) -> bool:
        try:
            if not self.is_connected or not self.client:
                return self.connect()

            # Test connection is still valid
            if self.oem_type == "siemens":
                self.client.get_cpu_state()
            elif self.oem_type == "beckhoff":
                self.client.read_state()
            elif self.oem_type == "phoenix":
                self.client.check_connection()
            elif self.oem_type == "abb":
                self.client.ping_device()
            elif self.oem_type == "br":
                self.client.verify_connection()

            self.last_check = datetime.now()
            return True
        except Exception:
            return self.connect()

    def read_data(self, address, size=1):
        """Generic method to read data from PLC"""
        try:
            if not self.ensure_connection():
                raise Exception("Connection failed")

            if self.oem_type == "siemens":
                return self.client.read_area(area=0x84, dbnumber=0, start=address, size=size)
            elif self.oem_type == "beckhoff":
                return self.client.read_by_name(address, size)
            elif self.oem_type == "phoenix":
                return self.client.read_variable(address)
            elif self.oem_type == "abb":
                return self.client.read_registers(address, size)
            elif self.oem_type == "br":
                return self.client.read_variable(address, size)

        except Exception as e:
            logger.error(f"Error reading data: {str(e)}")
            raise e

    def write_data(self, address, value):
        """Generic method to write data to PLC"""
        try:
            if not self.ensure_connection():
                raise Exception("Connection failed")

            if self.oem_type == "siemens":
                return self.client.write_area(area=0x84, dbnumber=0, start=address, data=value)
            elif self.oem_type == "beckhoff":
                return self.client.write_by_name(address, value)
            elif self.oem_type == "phoenix":
                return self.client.write_variable(address, value)
            elif self.oem_type == "abb":
                return self.client.write_registers(address, value)
            elif self.oem_type == "br":
                return self.client.write_variable(address, value)

        except Exception as e:
            logger.error(f"Error writing data: {str(e)}")
            raise e

# ''''''''''''''''''''End of Profinet Class'''''''''''''''''''''''''''''''''''''''''
class UnifiedConnectionManager:
    def __init__(self):
        self.connections: Dict[int, BaseDeviceConnection] = {}

    def add_connection(self, machine_id: int, connection: BaseDeviceConnection) -> bool:
        try:
            if machine_id in self.connections:
                if self.connections[machine_id].is_connection_valid():
                    return True
                self.connections[machine_id].disconnect()

            connection.connect()
            self.connections[machine_id] = connection
            logger.info(f"Successfully connected to machine {machine_id}")
            return True
        except Exception as e:
            logger.exception(f"Error adding connection for machine {machine_id}")
            raise

    def get_connection(self, machine_id: int) -> Optional[BaseDeviceConnection]:
        connection = self.connections.get(machine_id)
        if connection:
            if not connection.is_connection_valid():
                logger.warning(f"Connection to machine {machine_id} is invalid, removing.")
                self.remove_connection(machine_id)
                return None
            connection.ensure_connection()
        return connection

    def remove_connection(self, machine_id: int) -> bool:
        if machine_id in self.connections:
            try:
                connection = self.connections[machine_id]
                connection.disconnect()
                del self.connections[machine_id]
                logger.info(f"Successfully removed connection to machine {machine_id}")
                return True
            except Exception as e:
                logger.error(f"Error removing connection for machine {machine_id}: {str(e)}")
                del self.connections[machine_id]
                return False
        return False

    def is_connected(self, machine_id: int) -> bool:
        connection = self.get_connection(machine_id)
        return connection is not None and connection.is_connection_valid()

    def create_connection(self, machine_id: int, protocol: str, **kwargs) -> BaseDeviceConnection:
        protocol = protocol.lower()

        if protocol == "modbus tcp/ip":
            return ModbusTCPConnection(
                host=kwargs.get('host'),
                port=kwargs.get('port'),
                timeout=kwargs.get('timeout', 10)
            )
        elif protocol == "modbus rtu 485":
            return ModbusRTUConnection(
                port=kwargs.get('port'),
                slave_address=kwargs.get('slave_address'),
                baudrate=kwargs.get('baudrate'),
                parity=kwargs.get('parity'),
                stopbits=kwargs.get('stopbits'),
                timeout=kwargs.get('timeout', 5.0)
            )
        elif protocol == "profinet":
            ip_address = kwargs.pop('ip_address')
            oem_type = kwargs.pop('oem_type')
            connection_kwargs = {k: v for k, v in kwargs.items() if k not in ['ip_address', 'oem_type']}
            return ProfinetConnection(
                ip_address=ip_address,
                oem_type=oem_type,
                **connection_kwargs
            )
        elif protocol == "opcua":
            return OPCUAConnection(
                url=kwargs.get('url'),
                username=kwargs.get('username'),
                password=kwargs.get('password'),
                security_policy=kwargs.get('security_policy'),
                security_mode=kwargs.get('security_mode')
            )
        elif protocol == "mqtt":
            return MQTTConnection(
                broker=kwargs.get('broker'),
                port=kwargs.get('port'),
                username=kwargs.get('username'),
                password=kwargs.get('password'),
                client_id=kwargs.get('client_id')
            )
        elif protocol == "coapp":
            logger.info(f"Creating connection with CoAppConnection, ip_address={kwargs.get('ip_address')}")
            return CoAppConnection(
            ip_address=kwargs.get('ip_address'),
            port=kwargs.get('port', 5683),
            timeout=kwargs.get('timeout', 5.0)
            )
        else:
            raise ValueError(f"Unsupported protocol: {protocol}")

# '''''below is the supporting class for mqtt protocol connection ''''''
# class MQTTConnection(BaseDeviceConnection):
#     def __init__(self, broker: str, port: int, username: str = None, password: str = None, client_id: str = None):
#         import paho.mqtt.client as mqtt
#
#         self.broker = broker
#         self.port = int(port)
#         self.username = username
#         self.password = password
#         self.client_id = client_id or f"client_{uuid.uuid4().hex[:8]}"
#         self.connected = False
#         self.client = None
#
#         # Initialize connection
#         self._connect()
#
#     def _connect(self):
#         import paho.mqtt.client as mqtt
#
#         # Define callbacks
#         def on_connect(client, userdata, flags, rc):
#             if rc == 0:
#                 self.connected = True
#                 logger.info(f"Connected to MQTT broker {self.broker}:{self.port}")
#             else:
#                 logger.error(f"Failed to connect to MQTT broker, return code: {rc}")
#
#         def on_disconnect(client, userdata, rc):
#             self.connected = False
#             if rc != 0:
#                 logger.warning(f"Unexpected disconnection from MQTT broker: {rc}")
#
#         # Create client
#         self.client = mqtt.Client(client_id=self.client_id)
#
#         # Set callbacks
#         self.client.on_connect = on_connect
#         self.client.on_disconnect = on_disconnect
#
#         # Set authentication if provided
#         if self.username and self.password:
#             self.client.username_pw_set(self.username, self.password)
#
#         # Connect to broker
#         try:
#             self.client.connect(self.broker, self.port)
#             self.client.loop_start()  # Start background thread
#         except Exception as e:
#             logger.error(f"Failed to connect to MQTT broker: {str(e)}")
#             raise e
#
#     def is_connected(self) -> bool:
#         return self.connected
#
#     def disconnect(self):
#         if self.client and self.connected:
#             self.client.loop_stop()
#             self.client.disconnect()
#             self.connected = False
#
#     def publish(self, topic: str, payload: str, qos: int = 0, retain: bool = False) -> bool:
#         """Publish a message to an MQTT topic"""
#         if not self.is_connected():
#             raise ConnectionError("Not connected to MQTT broker")
#
#         result = self.client.publish(topic, payload, qos, retain)
#         return result.rc == 0
#
#     def subscribe(self, topic: str, qos: int = 0, callback=None):
#         """Subscribe to an MQTT topic"""
#         if not self.is_connected():
#             raise ConnectionError("Not connected to MQTT broker")
#
#         if callback:
#             def on_message(client, userdata, msg):
#                 callback(msg.topic, msg.payload)
#
#             self.client.on_message = on_message
#
#         result = self.client.subscribe(topic, qos)
#         return result[0] == 0


# ''''''below class is for the CoAPP connetcion manager''''''''''''

# ////this below code is connection is working but data reading i was getting null
# class CoAppConnection(BaseDeviceConnection):
#     def __init__(self, ip_address: str, port: int = 5683, timeout: float = 5.0):
#         """
#         Initialize a CoAP connection
#
#         :param ip_address: IP address of the CoAP server
#         :param port: Port of the CoAP server (default 5683)
#         :param timeout: Connection timeout in seconds
#         """
#         self.ip_address = ip_address
#         self.port = port
#         self.timeout = timeout
#         self._context = None
#         self._connection_lock = threading.Lock()
#         # Cache to store recent sensor readings
#         self._sensor_cache: Dict[str, float] = {}
#         self._loop = None
#
#     def _ensure_event_loop(self):
#         """
#         Ensure an event loop is available for async operations
#         """
#         try:
#             self._loop = asyncio.get_event_loop()
#         except RuntimeError:
#             # If no event loop exists, create a new one
#             self._loop = asyncio.new_event_loop()
#             asyncio.set_event_loop(self._loop)
#
#     def _create_async_context(self):
#         """
#         Create an asynchronous CoAP client context
#         """
#         try:
#             # Ensure we have an event loop
#             self._ensure_event_loop()
#
#             # Create context using the current event loop
#             self._context = self._loop.run_until_complete(aiocoap.Context.create_client_context())
#             return True
#         except Exception as e:
#             logger.error(f"Failed to create CoAP context: {e}")
#             return False
#
#     def connect(self):
#         """
#         Establish connection to the CoAP server
#         """
#         with self._connection_lock:
#             try:
#                 # Check socket connectivity
#                 self._check_socket_connection()
#
#                 # Create context if not exists
#                 if self._context is None:
#                     self._create_async_context()
#
#                 logger.info(f"Successfully connected to CoAP server at {self.ip_address}:{self.port}")
#                 return True
#             except Exception as e:
#                 logger.error(f"Connection to {self.ip_address}:{self.port} failed: {e}")
#                 raise ConnectionError(f"Could not connect to CoAP server: {e}")
#
#     def read_sensor_data(self, sensor_type: str) -> Optional[float]:
#         """
#         Read sensor data with explicit event loop management and path handling
#
#         :param sensor_type: Type of sensor to read (e.g., 'temperature', 'humidity')
#         :return: Sensor value or None if reading fails
#         """
#         try:
#             # Separate event loop creation
#             import asyncio
#             import aiocoap
#
#             # Create a new event loop for this operation
#             loop = asyncio.new_event_loop()
#             asyncio.set_event_loop(loop)
#
#             try:
#                 # Create client context
#                 context = loop.run_until_complete(aiocoap.Context.create_client_context())
#
#                 # Construct request URI with the exact path your server uses
#                 request_uri = f'coap://{self.ip_address}:{self.port}/{sensor_type}'
#
#                 # Create request
#                 request = aiocoap.Message(
#                     code=aiocoap.Code.GET,
#                     uri=request_uri
#                 )
#
#                 # Send request and get response
#                 response_future = context.request(request)
#                 response = loop.run_until_complete(response_future.response)
#
#                 # Check response
#                 if response.code == aiocoap.Code.CONTENT:
#                     payload = response.payload.decode('utf-8').strip()
#
#                     try:
#                         value = float(payload)
#                         logger.info(f"Successfully read {sensor_type}: {value}")
#                         return value
#                     except ValueError:
#                         logger.warning(f"Could not convert payload to float: {payload}")
#                         return None
#
#             except Exception as e:
#                 logger.error(f"Error reading {sensor_type} sensor: {e}", exc_info=True)
#                 return None
#
#             finally:
#                 # Always close the loop
#                 loop.close()
#
#         except Exception as comprehensive_error:
#             logger.error(
#                 f"Comprehensive sensor reading error for {sensor_type}: {comprehensive_error}",
#                 exc_info=True
#             )
#             return None
#     def disconnect(self):
#         """
#         Close the CoAP connection
#         """
#         with self._connection_lock:
#             if self._context:
#                 # Use the existing event loop to shutdown the context
#                 if self._loop and not self._loop.is_closed():
#                     try:
#                         self._loop.run_until_complete(self._context.shutdown())
#                     except Exception as e:
#                         logger.error(f"Error during disconnect: {e}")
#
#                 self._context = None
#                 self._loop = None
#
#             logger.info(f"Disconnected from CoAP server at {self.ip_address}:{self.port}")
#
#     def _check_socket_connection(self):
#         """
#         Check socket connection
#         """
#         try:
#             with socket.create_connection((self.ip_address, self.port), timeout=self.timeout):
#                 pass
#         except (socket.timeout, ConnectionRefusedError) as e:
#             logger.error(f"Socket connection failed: {e}")
#             raise
#
#     def is_connection_valid(self) -> bool:
#         """
#         Check if the connection is valid
#         """
#         try:
#             self._check_socket_connection()
#             return True
#         except Exception:
#             return False
#
#     def ensure_connection(self):
#         """
#         Ensure the connection is active, reconnect if needed
#         """
#         if not self.is_connection_valid():
#             self.connect()


class CoAppConnection(BaseDeviceConnection):
    def __init__(self, ip_address: str, port: int = 5683, timeout: float = 5.0):
        super().__init__()  # Initialize parent class
        self.ip_address = ip_address
        self.port = port
        self.timeout = timeout
        self._context = None
        self._connection_lock = threading.Lock()
        self._sensor_cache: Dict[str, float] = {}
        self._loop = None

    async def connect(self):
        """ Asynchronous method to establish connection to the CoAP server """
        async with self._connection_lock:
            try:
                # Ensure socket connectivity
                await self._check_socket_connection()

                # Create context if not exists
                if self._context is None:
                    await self._create_async_context()

                logger.info(f"Successfully connected to CoAP server at {self.ip_address}:{self.port}")
                return True
            except Exception as e:
                logger.error(f"Connection to {self.ip_address}:{self.port} failed: {e}")
                raise ConnectionError(f"Could not connect to CoAP server: {e}")

    async def _check_socket_connection(self):
        """ Asynchronously check socket connection """
        try:
            logger.info(f"Checking socket connection to {self.ip_address}:{self.port}")
            await asyncio.wait_for(asyncio.to_thread(socket.create_connection, (self.ip_address, self.port), timeout=self.timeout), timeout=self.timeout)
        except (socket.timeout, ConnectionRefusedError) as e:
            logger.error(f"Socket connection failed to {self.ip_address}:{self.port}: {e}")
            raise

    async def _create_async_context(self):
        """ Asynchronously create CoAP context """
        try:
            # Ensure event loop exists
            await self._ensure_event_loop()

            # Create CoAP context
            self._context = await aiocoap.Context.create_client_context()
            logger.info(f"CoAP context created for {self.ip_address}:{self.port}")
            return True
        except Exception as e:
            logger.error(f"Failed to create CoAP context: {e}")
            return False

    async def _ensure_event_loop(self):
        """ Ensure the event loop is available for async operations """
        if self._loop is None:
            self._loop = asyncio.get_event_loop()

    async def read_sensor_data_async(self, sensor_type: str) -> Optional[float]:
        try:
            # Create CoAP context and make the request
            context = await aiocoap.Context.create_client_context()
            request_uri = f'coap://{self.ip_address}:{self.port}/sensor/{sensor_type}'  # Ensure the URI path is correct
            request = aiocoap.Message(code=aiocoap.Code.GET, uri=request_uri)
            response = await context.request(request).response

            # Check if we get a valid response
            if response.code == aiocoap.Code.CONTENT:
                payload = response.payload.decode('utf-8').strip()

                try:
                    # Attempt to parse the response payload
                    if payload.startswith('{') and payload.endswith('}'):
                        parsed = json.loads(payload)
                        value = float(parsed.get("value"))
                    else:
                        value = float(payload)

                    logger.info(f"Successfully read {sensor_type}: {value}")
                    self._sensor_cache[sensor_type] = value
                    return value

                except ValueError:
                    logger.warning(f"Could not convert payload to float: {payload}")
                    return None
                except json.JSONDecodeError:
                    logger.error(f"Error parsing JSON payload for {sensor_type}: {payload}")
                    return None
                except Exception as e:
                    logger.error(f"Unexpected error when reading sensor data: {sensor_type}, {e}", exc_info=True)
                    return None
            else:
                logger.warning(f"Failed to retrieve valid response for {sensor_type}, response code: {response.code}")
                return None

        except Exception as e:
            logger.error(f"Error reading {sensor_type} sensor: {e}", exc_info=True)
            return None

    def read_sensor_data(self, sensor_type: str) -> Optional[float]:
        """ Read sensor data synchronously """
        try:
            if self._loop is None:
                self._ensure_event_loop()

            result = self._loop.run_until_complete(self.read_sensor_data_async(sensor_type))
            return result
        except Exception as e:
            logger.error(f"Comprehensive sensor reading error for {sensor_type}: {e}", exc_info=True)
            return None

    def read_multiple_sensors(self, sensor_types: List[str]) -> Dict[str, Optional[float]]:
        """ Read multiple sensors synchronously """
        results = {}
        for sensor_type in sensor_types:
            results[sensor_type] = self.read_sensor_data(sensor_type)
        return results

# '''''''''''''''''''END OF CoAPP'''''''''''''''''''''''''
class MQTTConnection(BaseDeviceConnection):
    def __init__(self, broker: str, port: int, username: str = None, password: str = None, client_id: str = None):


        self.broker = broker
        self.port = int(port)
        self.username = username
        self.password = password
        self.client_id = client_id or f"client_{uuid.uuid4().hex[:8]}"
        self.connected = False  # This is an attribute
        self.client = None
        self.last_check = None  # Add the last_check attribute

    def connect(self):
        """Implementation of the connect method required by BaseDeviceConnection"""
        self._connect()
        self.last_check = datetime.now()  # Update last_check timestamp

    def _connect(self):
        import paho.mqtt.client as mqtt

        # Create client if it doesn't exist
        if not self.client:
            # Define callbacks
            def on_connect(client, userdata, flags, rc):
                if rc == 0:
                    self.connected = True
                    logger.info(f"Connected to MQTT broker {self.broker}:{self.port}")
                else:
                    error_msg = f"Failed to connect to MQTT broker, return code: {rc}"
                    logger.error(error_msg)
                    raise ConnectionError(error_msg)

            def on_disconnect(client, userdata, rc):
                self.connected = False
                if rc != 0:
                    logger.warning(f"Unexpected disconnection from MQTT broker: {rc}")

            # Create client
            self.client = mqtt.Client(client_id=self.client_id)

            # Set callbacks
            self.client.on_connect = on_connect
            self.client.on_disconnect = on_disconnect

            # Set authentication if provided
            if self.username and self.password:
                self.client.username_pw_set(self.username, self.password)

        # Connect to broker
        try:
            self.client.connect(self.broker, self.port)
            self.client.loop_start()  # Start background thread
            self.last_check = datetime.now()
        except Exception as e:
            error_msg = f"Failed to connect to MQTT broker: {str(e)}"
            logger.error(error_msg)
            raise ConnectionError(error_msg)

    @property  # Make this a property to match the expected interface
    def is_connected(self) -> bool:
        """Property that returns connection status"""
        return self.connected

    def is_connection_valid(self) -> bool:
        """Check if the connection is valid and active"""
        # Check both connected status and last_check timestamp
        if not self.connected or not self.last_check:
            return False
        # You could add additional validity checks here
        return True

    def ensure_connection(self):
        """Ensure the connection is established"""
        if not self.connected:
            self.connect()
        self.last_check = datetime.now()

    def disconnect(self):
        """Safely disconnect the MQTT client"""
        if self.client:
            try:
                if self.connected:
                    self.client.loop_stop()
                    self.client.disconnect()
            except Exception as e:
                logger.error(f"Error disconnecting MQTT client: {str(e)}")
            finally:
                self.connected = False
                self.last_check = None
    # ''''''''''
    def get_mqtt_configuration(db: Session, machine_id: int):
        """
        Get the MQTT configuration for a specific machine.
        Returns the configuration with machine details.
        """
        # Get the latest MQTT configuration for the machine
        mqtt_config = db.query(models.MqttConfiguration).filter(
            models.MqttConfiguration.machine_id == machine_id
        ).order_by(models.MqttConfiguration.created_at.desc()).first()

        # Debug logging
        logger.info(f"MQTT Config DB Query Result: {mqtt_config}")

        if not mqtt_config:
            logger.warning(f"No MQTT configuration found for machine ID {machine_id}")
            return None

        # Get machine details
        machine = db.query(models.MachineDetails).filter(
            models.MachineDetails.id == machine_id
        ).first()

        if not machine:
            logger.warning(f"No machine details found for machine ID {machine_id}")
            return None

        try:
            # Create response schema
            response = schemas.MqttConfigurationResponse(
                id=mqtt_config.id,
                machine_id=mqtt_config.machine_id,
                machine_name=machine.name,
                broker=mqtt_config.broker,
                port=mqtt_config.port,
                username=mqtt_config.username,
                password=mqtt_config.password,
                created_at=mqtt_config.created_at
            )

            # Debug the created response
            logger.info(f"MQTT Config Response: {response.dict()}")

            return response
        except Exception as e:
            # If there's an error in creating the schema, log it and fall back to dict
            logger.error(f"Error creating response schema: {str(e)}", exc_info=True)

            # Fall back to returning a dictionary
            return {
                "id": mqtt_config.id,
                "machine_id": mqtt_config.machine_id,
                "machine_name": machine.name,
                "broker": mqtt_config.broker,
                "port": mqtt_config.port,
                "username": mqtt_config.username,
                "password": mqtt_config.password,
                "created_at": mqtt_config.created_at
            }

# ''''''''''''''''''end of mqqt connection class''''''''''''''''''''''''
class OPCUAConnection(BaseDeviceConnection):
    def __init__(self, url: str, username: Optional[str] = None, password: Optional[str] = None,
                 security_policy: Optional[str] = None, security_mode: Optional[str] = None):

        super().__init__()
        self.url = url
        self.username = username
        self.password = password
        self.security_policy = security_policy
        self.security_mode = security_mode
        self.client = None


    def connect(self) -> bool:
        try:
            if self.is_connected and self.client:
                return True

            self.client = Client(self.url)

            if self.security_policy and self.security_mode:
                security_string = f"Basic{self.security_policy}_{self.security_mode}"
                self.client.set_security_string(security_string)

            if self.username and self.password:
                self.client.set_user(self.username)
                self.client.set_password(self.password)

            self.client.connect()
            self.is_connected = True
            self.last_check = datetime.now()
            return True

        except Exception as e:
            self.is_connected = False
            logger.error(f"OPC UA Connection error: {str(e)}")
            raise e




    def disconnect(self) -> bool:
        try:
            if self.client:
                self.client.disconnect()
            self.is_connected = False
            self.last_check = None
            self.client = None
            return True
        except Exception as e:
            raise e

    def ensure_connection(self) -> bool:
        try:
            if not self.is_connected or not self.client:
                return self.connect()

            # Test if connection is still valid
            self.client.get_namespace_array()  # Simple operation to test connection
            self.last_check = datetime.now()
            return True
        except Exception:
            return self.connect()

    def is_connection_valid(self) -> bool:
        if not super().is_connection_valid():
            return False
        return self.client is not None and self.is_connected

    def discover_mqtt_topics(mqtt_connection: MQTTConnection) -> List[str]:
        """
        Discover available MQTT topics from a broker
        Uses # wildcard to discover all available topics

        Args:
            mqtt_connection: An established MQTTConnection object

        Returns:
            List of available topic strings
        """
        if not mqtt_connection.is_connection_valid():
            mqtt_connection.ensure_connection()

        discovered_topics = []

        # Create an event to signal when discovery is complete
        discovery_complete = threading.Event()

        # Keep track of the collected topics
        def on_message(client, userdata, message):
            nonlocal discovered_topics
            if message.topic not in discovered_topics:
                discovered_topics.append(message.topic)

        # Setup timeout for discovery process
        def on_timeout():
            discovery_complete.set()

        # Set message callback
        original_on_message = mqtt_connection.client.on_message
        mqtt_connection.client.on_message = on_message

        try:
            # Subscribe to wildcard topic to discover available topics
            mqtt_connection.client.subscribe("#", qos=0)

            # Set timeout for discovery (5 seconds)
            timer = threading.Timer(5.0, on_timeout)
            timer.start()

            # Wait for timeout
            discovery_complete.wait()

            return discovered_topics
        finally:
            # Clean up
            mqtt_connection.client.unsubscribe("#")
            mqtt_connection.client.on_message = original_on_message
            timer.cancel()

    def subscribe_and_get_mqtt_data(mqtt_connection: MQTTConnection, topic: str, qos: int = 0) -> Dict[str, Any]:
        """
        Subscribe to a specific MQTT topic and get the latest data

        Args:
            mqtt_connection: An established MQTTConnection object
            topic: The MQTT topic to subscribe to
            qos: Quality of Service level (0-2)

        Returns:
            Dictionary with topic, payload data, and timestamp
        """
        if not mqtt_connection.is_connection_valid():
            mqtt_connection.ensure_connection()

        # Create event for signaling when message is received
        message_received = threading.Event()

        # Variables to store the received message
        message_data = {
            "topic": topic,
            "payload": {},
            "timestamp": None
        }

        # Callback for message reception
        def on_message(client, userdata, message):
            nonlocal message_data
            if message.topic == topic:
                try:
                    # Try to decode as JSON
                    payload = json.loads(message.payload.decode('utf-8'))
                except:
                    # If not JSON, store as string
                    payload = {"data": message.payload.decode('utf-8')}

                message_data["payload"] = payload
                message_data["timestamp"] = datetime.now()
                message_received.set()

        # Set timeout for message reception
        def on_timeout():
            message_received.set()

        # Set message callback
        original_on_message = mqtt_connection.client.on_message
        mqtt_connection.client.on_message = on_message

        try:
            # Subscribe to the specific topic
            mqtt_connection.client.subscribe(topic, qos=qos)

            # Set timeout for waiting for a message (10 seconds)
            timer = threading.Timer(10.0, on_timeout)
            timer.start()

            # Wait for message or timeout
            message_received.wait()

            if message_data["timestamp"] is None:
                raise ValueError(f"No data received from topic {topic} within timeout period")

            return message_data
        finally:
            # Clean up
            mqtt_connection.client.unsubscribe(topic)
            mqtt_connection.client.on_message = original_on_message
            timer.cancel()
# ''''''''''''''newly opcua endpoints functions'''''''''''''

#..................amqp....................
def create_amqp_configuration(db: Session, config: schemas.AmqpConfigurationCreate):
    latest_machine = db.query(models.MachineDetails).order_by(models.MachineDetails.id.desc()).first()
    db_config = models.AmqpConfiguration(
        machine_id=latest_machine.id,
        host=config.host,
        port=config.port,
        username=config.username,
        password=config.password
    )
    db.add(db_config)
    db.commit()
    db.refresh(db_config)
    return db_config

def get_amqp_configuration(db: Session, machine_id: int):
    return db.query(models.AmqpConfiguration).filter(models.AmqpConfiguration.machine_id == machine_id).order_by(models.AmqpConfiguration.id.desc()).first()

def update_amqp_configuration(db: Session, machine_id: int, config: schemas.AmqpConfigurationUpdate):
    db_config = get_amqp_configuration(db, machine_id)
    if db_config is None:
        raise HTTPException(status_code=404, detail="AMQP configuration not found")

    for var, value in vars(config).items():
        if value is not None:
            setattr(db_config, var, value)

    db.commit()
    db.refresh(db_config)
    return db_config
#.....................end of amqp.................

def parse_db_url(url):
    # Expected format: postgresql://user:password@host:port/dbname
    pattern = r'postgresql://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)'
    match = re.match(pattern, url)
    if match:
        user, password, host, port, dbname = match.groups()
        return {
            'host': host,
            'database': dbname,
            'user': user,
            'password': password,
            'port': int(port)
        }
    else:
        raise ValueError(f"Could not parse database URL: {url}")

async def init_db_async(url, init_db_func):
    """Initialize database connection and create required tables"""
    global db_pool
    try:
        # Initialize SQLAlchemy tables using the provided init_db function
        init_db_func()
        logger.info("Database tables created successfully using SQLAlchemy ORM")

        # Parse the SQLAlchemy URL to get connection parameters for asyncpg
        db_config = parse_db_url(url)

        # Create asyncpg pool for async operations
        db_pool = await asyncpg.create_pool(**db_config)
        logger.info("Database connection pool initialized successfully")
    except Exception as e:
        logger.error(f"Database initialization error: {e}")
        raise

def generate_connection_id(url: str) -> str:
    """Generate a consistent connection ID based on the server URL"""
    return hashlib.md5(url.encode()).hexdigest()

async def get_or_create_machine_name(connection_id: str, server_url: str) -> str:
    try:
        if server_url in url_to_machine_name:
            return url_to_machine_name[server_url]

        async with db_pool.acquire() as conn:
            existing_machine = await conn.fetchval('''
                SELECT machine_name 
                FROM protocols.opc_ua_connections 
                WHERE connection_id = $1
            ''', connection_id)

            if existing_machine and existing_machine != 'Unknown Machine':
                url_to_machine_name[server_url] = existing_machine
                return existing_machine

            global machine_counter
            machine_counter += 1
            new_machine_name = f"Machine{machine_counter}"
            url_to_machine_name[server_url] = new_machine_name
            return new_machine_name

    except Exception as e:
        logger.error(f"Error managing machine name: {e}")
        return "Machine1"

async def store_reading(connection_id: str, node_id: str, node_description: str, value: Any, value_type: str):
    try:
        value_numeric = None
        value_text = None
        value_boolean = None

        if isinstance(value, (int, float)):
            value_numeric = float(value)
        elif isinstance(value, bool):
            value_boolean = value
        else:
            value_text = str(value)

        machine_name = connection_details[connection_id]['machine_name']

        async with db_pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO protocols.opc_ua_readings
                (connection_id, machine_name, time, node_id, node_description, value_type,
                 value_numeric, value_text, value_boolean)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ''', connection_id, machine_name, datetime.now(), node_id, node_description,
                               value_type, value_numeric, value_text, value_boolean)

    except Exception as e:
        logger.error(f"Error storing reading: {e}")
        raise



# Node handling functions
def check_if_folder(client: Client, node_id: str) -> bool:
    if node_id in folder_cache:
        return True
    try:
        node = client.get_node(node_id)
        is_folder = node.get_node_class() == ua.NodeClass.Object
        if is_folder:
            folder_cache.add(node_id)
        return is_folder
    except Exception as e:
        logger.error(f"Error checking folder for {node_id}: {e}")
        return False

async def process_node_batch_async(client: Client, nodes_batch, depth=0):
    results = []
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_REQUESTS) as executor:
        futures = []
        for node in nodes_batch:
            try:
                node_id = node.nodeid.to_string()
                if node_id in node_cache:
                    results.append(node_cache[node_id])
                    continue

                future = executor.submit(lambda n: n.get_display_name().Text, node)
                futures.append((node, node_id, future))
            except Exception as e:
                logger.error(f"Error queuing node: {e}")
                continue

        for node, node_id, future in futures:
            try:
                description = future.result(timeout=2)
                is_folder = check_if_folder(client, node_id)

                node_info = {
                    "node_id": node_id,  # Keep original node_id without encoding
                    "description": description,
                    "type": "folder" if is_folder else "parameter"
                }

                node_cache[node_id] = node_info
                results.append(node_info)

                if is_folder and depth < MAX_DEPTH:
                    try:
                        children = node.get_children()
                        child_results = await process_node_batch_async(client, children, depth + 1)
                        results.extend(child_results)
                    except Exception as e:
                        logger.error(f"Error processing children for {node_id}: {e}")

            except Exception as e:
                logger.error(f"Error processing future: {e}")
                continue

    return results

async def retrieve_all_nodes_by_machine(machine_id: int, db: Session):
    """Retrieve all nodes for a specific machine"""
    try:
        # Get machine mapping
        mapping = db.query(models.OpcUaMachineMapping).filter_by(machine_id=machine_id).first()
        if not mapping:
            raise HTTPException(status_code=400, detail="Machine not connected to OPC UA server")

        connection_id = mapping.connection_id
        if connection_id not in opc_clients:
            raise HTTPException(status_code=400, detail="Machine connection lost")

        client = opc_clients[connection_id]
        start_time = time.time()
        node_cache.clear()
        folder_cache.clear()

        root_node = client.get_node("i=84")
        initial_children = root_node.get_children()
        batches = [initial_children[i:i + BATCH_SIZE] for i in range(0, len(initial_children), BATCH_SIZE)]

        all_nodes = []
        tasks = []

        for batch in batches:
            if time.time() - start_time >= TIMEOUT:
                break
            task = asyncio.create_task(process_node_batch_async(client, batch))
            tasks.append(task)

        done, pending = await asyncio.wait(
            tasks,
            timeout=TIMEOUT - (time.time() - start_time),
            return_when=asyncio.ALL_COMPLETED
        )

        for task in pending:
            task.cancel()

        for task in done:
            try:
                results = await task
                all_nodes.extend(results)
            except Exception as e:
                logger.error(f"Error collecting task results: {e}")
                continue

        folders = []
        parameters = []
        for node in all_nodes:
            if node["type"] == "folder":
                folders.append({
                    "node_id": node["node_id"],  # Keep original node_id
                    "description": node["description"]
                })
            else:
                parameters.append({
                    "node_id": node["node_id"],  # Keep original node_id
                    "description": node["description"]
                })

        return {
            "status": "success",
            "machine_id": machine_id,
            "folders": folders,
            "parameters": parameters,
            "total_folders": len(folders),
            "total_parameters": len(parameters),
            "execution_time_seconds": time.time() - start_time
        }

    except Exception as e:
        logger.error(f"Error retrieving nodes for machine {machine_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ''' ''''''''below is the connect function even including machine id''' '''''''''''''''''''''''
async def connect_to_server_by_machine(machine_id: int, db: Session, params=None):
    """Connect to OPC UA server and associate with machine ID"""
    try:
        # Get machine details
        machine = db.query(models.MachineDetails).filter(models.MachineDetails.id == machine_id).first()
        if not machine:
            raise HTTPException(status_code=404, detail="Machine not found")

        # Generate connection ID based on machine ID
        connection_id = generate_connection_id(f"machine_{machine_id}")

        # Disconnect existing connection if any
        if connection_id in opc_clients:
            try:
                opc_clients[connection_id].disconnect()
            except:
                pass
            del opc_clients[connection_id]

        # Create new client
        client = Client(params.url if params else "opc.tcp://localhost:4840")
        if params:
            if params.username:
                client.set_user(params.username)
            if params.password:
                client.set_password(params.password)

        client.connect()
        opc_clients[connection_id] = client

        # Store connection details
        connection_details[connection_id] = {
            'url': params.url if params else "opc.tcp://localhost:4840",
            'machine_name': machine.machine_name
        }

        # Update database
        mapping = db.query(models.OpcUaMachineMapping).filter_by(machine_id=machine_id).first()
        if mapping:
            mapping.connection_id = connection_id
            mapping.server_url = params.url if params else "opc.tcp://localhost:4840"
            mapping.last_connected = datetime.now()
        else:
            mapping = models.OpcUaMachineMapping(
                machine_id=machine_id,
                connection_id=connection_id,
                server_url=params.url if params else "opc.tcp://localhost:4840",
                last_connected=datetime.now()
            )
            db.add(mapping)

        db.commit()

        return {
            "status": "connected",
            "message": f"Successfully connected machine {machine.machine_name}",
            "machine_id": machine_id,
            "connection_id": connection_id
        }

    except Exception as e:
        logger.error(f"Connection error for machine {machine_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

async def disconnect_from_server(connection_id: str):
    try:
        if connection_id in opc_clients:
            client = opc_clients[connection_id]
            client.disconnect()
            del opc_clients[connection_id]

            async with db_pool.acquire() as conn:
                await conn.execute('''
                    UPDATE protocols.opc_ua_connections 
                    SET last_connected = $1
                    WHERE connection_id = $2
                ''', datetime.now(), connection_id)

            return {
                "status": "success",
                "message": "Disconnected successfully",
                "connection_id": connection_id
            }
        return {"status": "error", "message": "Not connected"}
    except Exception as e:
        logger.error(f"Disconnect error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

async def continuous_monitor(machine_id: int, node_id: str, connection_id: str, client, db: Session):
    """Background task to continuously monitor node values"""
    try:
        while True:
            node = client.get_node(node_id)
            value = node.get_value()
            description = node.get_display_name().Text

            # Store the reading
            await store_reading(
                connection_id=connection_id,
                node_id=node_id,
                node_description=description,
                value=value,
                value_type=type(value).__name__
            )

            # Wait for 1 second before next read
            await asyncio.sleep(1)

    except Exception as e:
        logger.error(f"Error in continuous monitoring for node {node_id}: {e}")


async def read_node_by_machine(
        machine_id: int,
        node_id: str,
        db: Session,
        background_tasks: BackgroundTasks
):
    """Read a specific node value for a machine using machine_id and start continuous monitoring"""
    try:
        # Get machine mapping
        mapping = db.query(models.OpcUaMachineMapping).filter_by(machine_id=machine_id).first()
        if not mapping:
            raise HTTPException(status_code=400, detail="Machine not connected to OPC UA server")

        connection_id = mapping.connection_id
        if connection_id not in opc_clients:
            raise HTTPException(status_code=400, detail="Machine connection lost")

        # Verify connection exists in opc_ua_connections table
        connection = db.query(models.OpcUaConnection).filter_by(connection_id=connection_id).first()
        if not connection:
            # Insert connection record if it doesn't exist
            new_connection = models.OpcUaConnection(
                connection_id=connection_id,
                server_url=mapping.server_url,
                machine_name=connection_details[connection_id]['machine_name'],
                last_connected=datetime.now()
            )
            db.add(new_connection)
            db.commit()

        client = opc_clients[connection_id]
        node = client.get_node(node_id)
        node_class = node.get_node_class()

        if node_class == ua.NodeClass.Object:
            description = node.get_display_name().Text
            return {
                "status": "success",
                "machine_id": machine_id,
                "node_id": node_id,
                "type": "folder",
                "description": description,
                "connection_id": connection_id,
                "machine_name": connection_details[connection_id]['machine_name']
            }
        else:
            value = node.get_value()
            description = node.get_display_name().Text

            # Store initial reading
            await store_reading(
                connection_id=connection_id,
                node_id=node_id,
                node_description=description,
                value=value,
                value_type=type(value).__name__
            )

            # Start continuous monitoring
            background_tasks.add_task(
                continuous_monitor,
                machine_id,
                node_id,
                connection_id,
                client,
                db
            )

            return {
                "status": "success",
                "machine_id": machine_id,
                "node_id": node_id,
                "type": "parameter",
                "value": value,
                "description": description,
                "connection_id": connection_id,
                "machine_name": connection_details[connection_id]['machine_name']
            }

    except Exception as e:
        logger.error(f"Error reading node for machine {machine_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

async def get_connection_history():
    try:
        async with db_pool.acquire() as conn:
            history = await conn.fetch('''
                SELECT connection_id, server_url, machine_name, last_connected
                FROM protocols.opc_ua_connections
                ORDER BY last_connected DESC
            ''')
            return {
                "status": "success",
                "connections": [dict(record) for record in history]
            }
    except Exception as e:
        logger.error(f"Error retrieving connection history: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Add these helpers
def encode_node_id(node_id: str) -> str:
    """Encode node ID for URL-safe transmission"""
    return base64.b64encode(node_id.encode()).decode()

def decode_node_id(encoded_node_id: str) -> str:
    """Decode node ID from URL-safe format"""
    try:
        return base64.b64decode(encoded_node_id).decode()
    except:
        # If not base64 encoded, try URL decode
        return unquote(encoded_node_id)




# ''''''''''''reading modbus tcp data functions'''''''''''''''''''''
def combine_registers_and_convert_to_float(registers, endianess="big"):
    """Combine two 16-bit Modbus registers into a 32-bit float."""
    if len(registers) != 2:
        raise ValueError("Exactly two registers are required for conversion to a float.")

    if endianess == "little":
        int_value = (registers[0] << 16) + registers[1]
    else:
        int_value = (registers[1] << 16) + registers[0]

    float_value = struct.unpack('f', struct.pack('I', int_value))[0]
    return float_value


def get_register_values(instrument, function_code, start_address, end_address=None, count=None):
    """
    Request register values based on user input. Works with both Modbus RTU and TCP/IP.

    Args:
        instrument: Modbus instrument (RTU or TCP)
        function_code: Modbus function code (1-4 for reading)
        start_address: Starting register address
        end_address: Ending register address (optional)
        count: Number of registers to read (used for TCP, optional)
    """
    try:
        # Handle TCP case where count is provided instead of end_address
        if count is not None:
            end_address = start_address + count - 1

        # Calculate quantity based on end_address or default to 2 for float conversion
        quantity = calculate_quantity(start_address, end_address)

        result = read_modbus_register(instrument, function_code, start_address, quantity)



        # If the result is a list of registers, return success with data
        if isinstance(result, list):
            return {
                "status": "success",
                "message": f"Read registers from address {start_address} {f'to {end_address}' if end_address else ''} using function code {function_code}",
                "data": result
            }

        # If result is already a dict with status (error case)
        if isinstance(result, dict) and "status" in result:
            return result


        # In case of an unexpected return type
        return {
            "status": "error",
            "message": "Unexpected result format"
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to read registers: {str(e)}"
        }


def calculate_quantity(start_address, end_address):
    """Calculate quantity of registers to read."""
    if end_address is None:
        return 2  # Default for float conversion

    if end_address < start_address:
        raise ValueError("End address must be greater than or equal to start address.")

    return end_address - start_address + 1


def read_modbus_register(instrument, function_code, start_address, quantity):
    """
    Read Modbus registers based on function code.
    Works with both RTU and TCP instruments.
    """
    try:
        # Check if instrument is TCP or RTU based on instance type
        is_tcp = hasattr(instrument, 'read_holding_registers')  # TCP instruments typically have this method

        if is_tcp:
            if function_code in [1, 2]:
                return instrument.read_discrete_inputs(start_address,
                                                       quantity) if function_code == 2 else instrument.read_coils(
                    start_address, quantity)
            elif function_code == 3:
                response = instrument.read_holding_registers(start_address, quantity)
                return response.registers
            elif function_code == 4:
                response = instrument.read_input_registers(start_address, quantity)
                return response.registers
        else:
            # RTU reading using minimalmodbus
            if function_code == 1:
                return instrument.read_bits(start_address, quantity)
            elif function_code == 2:
                return instrument.read_bits(start_address, quantity)
            elif function_code in [3, 4]:
                return instrument.read_registers(start_address, quantity)

        return {"status": "error", "message": "Unsupported function code"}

    except (minimalmodbus.ModbusException, Exception) as e:
        return {"status": "error", "message": f"Modbus Error: {str(e)}"}


def write_modbus_register(instrument, function_code, start_address, values, end_address=None):
    """Write to Modbus registers for both RTU and TCP."""
    try:
        if end_address is None:
            end_address = start_address

        if len(values) != (end_address - start_address + 1):
            return {"status": "error", "message": "Number of values does not match register range"}

        # Check if instrument is TCP or RTU
        is_tcp = hasattr(instrument, 'write_register')

        if is_tcp:
            if function_code == 5:
                return instrument.write_single_coil(start_address, values[0])
            elif function_code == 6:
                return instrument.write_single_register(start_address, values[0])
            elif function_code == 15:
                return instrument.write_multiple_coils(start_address, values)
            elif function_code == 16:
                return instrument.write_multiple_registers(start_address, values)
        else:
            # RTU writing using minimalmodbus
            if function_code == 5:
                return instrument.write_bit(start_address, values[0])
            elif function_code == 6:
                return instrument.write_register(start_address, values[0])
            elif function_code == 15:
                return instrument.write_bits(start_address, values)
            elif function_code == 16:
                return instrument.write_registers(start_address, values)

        return {"status": "error", "message": "Unsupported function code for writing"}

    except (minimalmodbus.ModbusException, Exception) as e:
        return {"status": "error", "message": f"Failed to write registers: {str(e)}"}

# '''''below code function is for Profinet data reading logic
# def read_and_interpret_data(client, field: ProfinetReadField):
#     """
#     Read and interpret data from Siemens PLC using Profinet protocol.
#
#     Args:
#         client: Snap7 client instance
#         field: ProfinetReadField containing read configuration
#
#     Returns:
#         Interpreted value based on data type
#     """
#     try:
#         # Determine area based on data source
#         area_mapping = {
#             "db": snap7.types.Areas.DB,
#             "input": snap7.types.Areas.PE,
#             "output": snap7.types.Areas.PA,
#             "memory": snap7.types.Areas.MK
#         }
#
#         area = area_mapping.get(field.data_source.lower())
#         if area is None:
#             raise ValueError(f"Invalid data source: {field.data_source}")
#
#         # Set DB number for DB areas
#         db_number = field.db_number if field.data_source.lower() == "db" else 0
#
#         # Determine size based on data type
#         type_size_mapping = {
#             "bool": 1,
#             "byte": 1,
#             "word": 2,
#             "dword": 4,
#             "int": 2,
#             "dint": 4,
#             "real": 4,
#             "string": field.string_length + 2 if field.string_length else 256
#         }
#
#         size = type_size_mapping.get(field.data_type.lower(), 1)
#
#         # Read raw bytes
#         raw_data = client.read_area(
#             area=area,
#             dbnumber=db_number,
#             start=field.offset,
#             size=size
#         )
#
#         # Interpret data based on type
#         if field.data_type.lower() == "bool":
#             byte_index = field.offset // 8
#             bit_index = field.offset % 8
#             return bool(raw_data[byte_index] & (1 << bit_index))
#
#         elif field.data_type.lower() in ["byte", "word", "dword"]:
#             return int.from_bytes(raw_data, byteorder='big')
#
#         elif field.data_type.lower() == "int":
#             return int.from_bytes(raw_data, byteorder='big', signed=True)
#
#         elif field.data_type.lower() == "dint":
#             return int.from_bytes(raw_data, byteorder='big', signed=True)
#
#         elif field.data_type.lower() == "real":
#             return struct.unpack('>f', raw_data)[0]
#
#         elif field.data_type.lower() == "string":
#             # First byte is max length, second byte is actual length
#             actual_length = raw_data[1]
#             return raw_data[2:2+actual_length].decode('utf-8')
#
#         else:
#             raise ValueError(f"Unsupported data type: {field.data_type}")
#
#     except Exception as e:
#         logger.error(f"Error reading data from Siemens PLC: {str(e)}")
#         raise ValueError(f"Failed to read data: {str(e)}")

def read_and_interpret_data(client, field: ProfinetReadField):
    """
    Read and interpret data from Siemens PLC using Profinet protocol.

    Args:
        client: Snap7 client instance
        field: ProfinetReadField containing read configuration

    Returns:
        Interpreted value based on data type or a list of values if reading a range.
    """
    try:
        area_mapping = {
            "db": snap7.types.Areas.DB,
            "input": snap7.types.Areas.PE,
            "output": snap7.types.Areas.PA,
            "memory": snap7.types.Areas.MK
        }

        area = area_mapping.get(field.data_source.lower())
        if area is None:
            raise ValueError(f"Invalid data source: {field.data_source}")

        db_number = field.db_number if field.data_source.lower() == "db" else 0

        type_size_mapping = {
            "bool": 1,  # Boolean is treated as 1 bit
            "byte": 1,
            "word": 2,
            "dword": 4,
            "int": 2,
            "dint": 4,
            "real": 4,
            "char": 1,
            "string": 125  # Assuming max string length is 256
        }

        size = type_size_mapping.get(field.data_type.lower(), 1)

        raw_data = client.read_area(
            area=area,
            dbnumber=db_number,
            start=field.offset,
            size=size * (field.count if field.data_type.lower() != "string" else 1)  # Read full string if type is string
        )

        if field.data_type.lower() == "string":
            # Read the entire string from the offset
            string_data = raw_data[:256]  # Adjust based on your max string length
            return string_data.split(b'\x00', 1)[0].decode('utf-8', errors='ignore')

        if field.data_type.lower() == "bool":
            # Handle boolean data type
            bool_values = []
            for i in range(field.count):
                byte_index = (field.offset + i) // 8  # Calculate the byte index
                bit_index = (field.offset + i) % 8   # Calculate the bit index
                if byte_index < len(raw_data):
                    bool_value = (raw_data[byte_index] >> bit_index) & 0x01  # Extract the bit
                    bool_values.append(bool(bool_value))
                else:
                    raise ValueError(f"Offset {field.offset + i} is out of range for reading boolean data.")
            return bool_values if field.count > 1 else bool(bool_values[0])

        if field.count > 1:
            values = []
            for i in range(field.count):
                offset = i * size
                if field.data_type.lower() in ["byte", "word", "dword", "int", "dint", "real"]:
                    values.append(int.from_bytes(raw_data[offset:offset + size], byteorder='big'))
                elif field.data_type.lower() == "char":
                    values.append(chr(raw_data[offset]))
                else:
                    raise ValueError(f"Unsupported data type: {field.data_type}")
            return values

        else:
            if field.data_type.lower() in ["byte", "word", "dword", "int", "dint", "real"]:
                return int.from_bytes(raw_data, byteorder='big')
            elif field.data_type.lower() == "char":
                return chr(raw_data[0])
            else:
                raise ValueError(f"Unsupported data type: {field.data_type}")

    except Exception as e:
        logger.error(f"Error reading data from Siemens PLC: {str(e)}")
        raise ValueError(f"Failed to read data: {str(e)}")



# ''''''''Mqtt configuration function below''''''''''''
def create_mqtt_configuration(db: Session, mqtt_config: schemas.MqttConfigurationCreate):
    try:
        # Find the latest machine from machine_details
        latest_machine = db.query(models.MachineDetails).order_by(models.MachineDetails.id.desc()).first()

        if not latest_machine:
            raise HTTPException(status_code=404, detail="No machine found")

        # Create new MQTT configuration
        new_config = models.MqttConfiguration(
            machine_id=latest_machine.id,
            broker=mqtt_config.broker,
            port=mqtt_config.port,
            username=mqtt_config.username,
            password=mqtt_config.password
        )

        db.add(new_config)
        db.commit()
        db.refresh(new_config)

        return {
            **new_config.__dict__,
            'machine_name': latest_machine.machine_name,
        }
    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


def get_mqtt_configuration(db: Session, machine_id: int):
    try:
        # Get machine details
        machine = db.query(models.MachineDetails).filter(models.MachineDetails.id == machine_id).first()

        if not machine:
            raise HTTPException(status_code=404, detail="Machine not found")

        # Get MQTT configuration for this machine
        mqtt_config = db.query(models.MqttConfiguration).filter(
            models.MqttConfiguration.machine_id == machine_id
        ).order_by(models.MqttConfiguration.id.desc()).first()

        if not mqtt_config:
            raise HTTPException(status_code=404, detail="No MQTT configuration found for this machine")

        return {
            **mqtt_config.__dict__,
            'machine_name': machine.machine_name
        }
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


def update_mqtt_configuration(db: Session, machine_id: int, mqtt_config: schemas.MqttConfigurationUpdate):
    try:
        # Verify machine exists
        machine = db.query(models.MachineDetails).filter(models.MachineDetails.id == machine_id).first()

        if not machine:
            raise HTTPException(status_code=404, detail="Machine not found")

        # Find the latest MQTT configuration for this machine
        existing_config = db.query(models.MqttConfiguration).filter(
            models.MqttConfiguration.machine_id == machine_id
        ).order_by(models.MqttConfiguration.id.desc()).first()

        if not existing_config:
            raise HTTPException(status_code=404, detail="No MQTT configuration found for this machine")

        # Update fields that are provided
        update_data = mqtt_config.dict(exclude_unset=True)
        for key, value in update_data.items():
            setattr(existing_config, key, value)

        db.commit()
        db.refresh(existing_config)

        return {
            **existing_config.__dict__,
            'machine_name': machine.machine_name
        }
    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


def create_mqtt_connection(self, machine_id, broker, port, username=None, password=None, client_id=None):
    """Create an MQTT client connection"""
    import paho.mqtt.client as mqtt

    client = mqtt.Client(client_id=client_id)

    if username and password:
        client.username_pw_set(username, password)

    # You might want to add callbacks for on_connect, on_disconnect, etc.

    try:
        client.connect(broker, port)
        client.loop_start()  # Start the background thread
        return client
    except Exception as e:
        logger.error(f"Failed to connect to MQTT broker: {str(e)}")
        raise e