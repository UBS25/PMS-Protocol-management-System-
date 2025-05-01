import asyncio
from contextvars import Context
from pickle import GET
import platform
if platform.system() != 'Windows':
    import resource
import time
import uuid
from asyncio.log import logger
from datetime import datetime
import aio_pika
import asyncio
import contextlib  # Add this import
import async_timeout  # You'll need to pip install async-timeout
from adodbapi import IntegrityError
# Import required libraries necessary for the modbus tcp ip

# ''''''uvicorn app.main:app --reload
from fastapi import FastAPI,Request, HTTPException, Depends, File, UploadFile, Body, APIRouter, BackgroundTasks, Query
from fastapi.middleware.cors import CORSMiddleware
from opcua import ua, Client
from sqlalchemy import and_, desc
from sqlalchemy.orm import Session
from typing import List, Dict, Set, Optional, Union, Any
import os
from uuid import uuid4
from pydantic import BaseModel
from app.coap_server import generate_sensor_data
from coap_sensor_simulator import SensorResource
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from coap_sensor_simulator import SensorResource
from app.models import ( AmqpConfiguration, AmqpConfigurationCreate,AmqpConfigurationUpdate, AmqpConfigurationResponse, AmqpSubscription)
                                                                                                                                                                                                                                                                   
from passlib.context import CryptContext
from starlette import status

from app import database, operations, schemas, models
from app.database import get_db, engine, Base, init_db, SQLALCHEMY_DATABASE_URL
from app.models import (User, MachineDetails, RS485Config, ReadingParameters,
                        DataStorageConfig, StatCard, RegisterConfig, OPCUAServerManager, ModbusActualData,
                        ModbusRawData, RegisterValue, ModbusTCP, OPCUA, MqttConfiguration, RegisterSelection,
                        CoAPPConfigurations)

from app.schemas import (UserCreate, UserLogin, PasswordUpdate,
                         CombinedConfigCreate, MachineConfigResponse,
                         CombinedConfigUpdate, DataStorageConfigCreate, MachineRegisters, ConnectionResponse,
                         ConnectionParams, DisconnectionResponse, OPCUAConnectionParams, RegisterValueBatch,
                         ModbusTCPConfigCreate, ModbusTCPConfigResponse, ModbusTCPConfigBase,
                         OPCUAResponse, OPCUACreate, DataStorageConfigUpdate, ModbusRawDataResponse,
                         ModbusActualDataResponse, RegisterSelectionCreate, CoAPPConfigResponse, CoAPPConfigCreate,
                         SensorDataResponse)

# from app.operations import (DeviceConnection, DeviceConnectionManager, read_modbus_register,
#                             combine_registers_and_convert_to_float, get_register_values
#                             )
from app.operations import (read_modbus_register,
                            combine_registers_and_convert_to_float, get_register_values
                            )

import app.operations as ops
from app.schemas import ConnectionParams, NodeRequest, ConnectionResponse, DisconnectionResponse, ReadNodeResponse, NodesResponse

from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from app.coap_server import CoAPPConnectionManager
coap_manager = CoAPPConnectionManager()
from app.operations import get_machine_by_id
from app.models import CoAPPConfigurations

# Initialize FastAPI app
app = FastAPI()

from app.database import engine, Base

Base.metadata.create_all(bind=engine, checkfirst=True)

# Initialize database (create schema)
init_db()

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from jose import jwt, JWTError

SECRET_KEY = "mysecretkey"
ALGORITHM = "HS256"

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    token = websocket.query_params.get("token")

    if not token:
        await websocket.close(code=1008)
        return

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        if not username:
            await websocket.close(code=1008)
            return

        await websocket.accept()
        while True:
            data = await websocket.receive_text()
            await websocket.send_text(f"Hello {username}, you said: {data}")

    except JWTError:
        await websocket.close(code=1008)


# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global connection manager instance
# connection_manager = DeviceConnectionManager()
connection_manager = operations.UnifiedConnectionManager()

# Upload directory configuration
UPLOAD_DIR = "uploaded_images"
os.makedirs(UPLOAD_DIR, exist_ok=True)

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

from .database import SessionLocal # ✅ Make sure this import exists
from app.models import SensorValue      # ✅ This too

# Create new file services/user_service.py
async def register_user(user: UserCreate, db: Session):
    db_user = db.query(User).filter(User.username == user.username).first()
    if db_user:
        raise HTTPException(status_code=400, detail="Username already registered")

    db_user = db.query(User).filter(User.email == user.email).first()
    if db_user:
        raise HTTPException(status_code=400, detail="Email already registered")

    hashed_password = pwd_context.hash(user.password)
    db_user = User(
        email=user.email,
        username=user.username,
        password=hashed_password,
        contact_number=user.contact_number
    )
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return {"message": "User registered successfully"}



# Create new file services/machine_service.py
async def add_machine(machine_name: str, machine_protocol: str, machine_photo: UploadFile, db: Session):
    file_extension = machine_photo.filename.split(".")[-1]
    file_name = f"{uuid4()}.{file_extension}"
    file_path = os.path.join(UPLOAD_DIR, file_name)

    with open(file_path, "wb") as f:
        f.write(await machine_photo.read())

    new_machine = MachineDetails(
        machine_name=machine_name,
        machine_photo=file_name,
        machine_protocol=machine_protocol
    )
    db.add(new_machine)
    db.commit()
    db.refresh(new_machine)

    return {
        "message": "Machine details added successfully",
        "machine_id": new_machine.id,
        "machine_name": new_machine.machine_name,
        "machine_protocol": new_machine.machine_protocol
    }


# Create new file services/modbus_service.py
# async def connect_machine(machine_id: int, db: Session, connection_manager: DeviceConnectionManager):
#     machine = db.query(MachineDetails).filter(MachineDetails.id == machine_id).first()
#     if not machine:
#         raise HTTPException(status_code=404, detail="Machine not found")
#
#     if machine.machine_protocol.lower() != "modbus rtu 485":
#         raise HTTPException(status_code=400, detail="Only Modbus RTU 485 protocol is supported")
#
#     rs485_config = db.query(RS485Config).filter(RS485Config.machine_id == machine_id).first()
#     if not rs485_config:
#         raise HTTPException(status_code=404, detail="RS485 configuration not found")
#
#     try:
#         connection = DeviceConnection(
#             port=rs485_config.com_port,
#             slave_address=rs485_config.slave_id,
#             baudrate=rs485_config.baud_rate,
#             parity=rs485_config.parity,
#             stopbits=rs485_config.stop_bits,
#             timeout=5.0
#         )
#
#         connection_manager.add_connection(machine_id, connection)
#
#         return {
#             "status": "success",
#             "message": f"Successfully connected to machine {machine_id}"
#         }
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Connection failed: {str(e)}")

# Create new file services/storage_service.py
async def store_data(config: DataStorageConfigCreate, db: Session):
    latest_machine = db.query(MachineDetails).order_by(MachineDetails.id.desc()).first()
    if not latest_machine:
        raise HTTPException(status_code=404, detail="No machines found. Please add a machine first.")

    data_storage_config = DataStorageConfig(
        machine_id=latest_machine.id,
        store_in_database=config.store_in_database,
        store_in_opcua_server=config.store_in_opcua_server
    )
    db.add(data_storage_config)
    db.commit()
    db.refresh(data_storage_config)

    return {
        "message": "Data storage configuration saved successfully",
        "config": {
            "id": data_storage_config.id,
            "machine_id": data_storage_config.machine_id,
            "machine_name": latest_machine.machine_name,
            "store_in_database": data_storage_config.store_in_database,
            "store_in_opcua_server": data_storage_config.store_in_opcua_server,
            "created_at": data_storage_config.created_at
        }
    }

# Create new file services/stat_card_service.py
async def create_stat_card(machine_id: int, machine_photo: str, config_data: dict, db: Session):
    stat_card = StatCard(
        machine_id=machine_id,
        machine_photo=machine_photo,
        config_data=config_data
    )
    db.add(stat_card)
    db.commit()
    db.refresh(stat_card)

    return {
        "id": stat_card.id,
        "machine_id": stat_card.machine_id,
        "machine_photo": stat_card.machine_photo,
        "config_data": stat_card.config_data,
        "created_at": stat_card.created_at
    }
# User Management Endpoints
@app.post("/register")
def register_user(user: UserCreate, db: Session = Depends(get_db)):
    db_user = db.query(User).filter(User.username == user.username).first()
    if db_user:
        raise HTTPException(status_code=400, detail="Username already registered")

    db_user = db.query(User).filter(User.email == user.email).first()
    if db_user:
        raise HTTPException(status_code=400, detail="Email already registered")

    hashed_password = pwd_context.hash(user.password)
    db_user = User(
        email=user.email,
        username=user.username,
        password=hashed_password,
        contact_number=user.contact_number
    )
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return {"message": "User registered successfully"}

@app.post("/login")
def login_user(user: UserLogin, db: Session = Depends(get_db)):
    db_user = db.query(User).filter(User.username == user.username).first()
    if not db_user:
        raise HTTPException(status_code=400, detail="Incorrect username or password")

    if not pwd_context.verify(user.password, db_user.password):
        raise HTTPException(status_code=400, detail="Incorrect username or password")

    return {"message": "Login successful"}
@app.post("/update-password")
def update_password(password_update: PasswordUpdate, db: Session = Depends(get_db)):
    db_user = db.query(User).filter(User.username == password_update.username).first()
    if not db_user:
        raise HTTPException(status_code=404, detail="User not found")

    hashed_password = pwd_context.hash(password_update.new_password)
    db_user.password = hashed_password
    db.commit()

    return {"message": "Password updated successfully"}


# Machine Management Endpoints
@app.post("/add-machine")
async def add_machine(
        machine_name: str,
        machine_protocol: str,
        machine_photo: UploadFile = File(...),
        db: Session = Depends(get_db)
):
    file_extension = machine_photo.filename.split(".")[-1]
    file_name = f"{uuid4()}.{file_extension}"
    file_path = os.path.join(UPLOAD_DIR, file_name)

    with open(file_path, "wb") as f:
        f.write(await machine_photo.read())

    new_machine = MachineDetails(
        machine_name=machine_name,
        machine_photo=file_name,
        machine_protocol=machine_protocol
    )
    db.add(new_machine)
    db.commit()
    db.refresh(new_machine)

    return {
        "message": "Machine details added successfully",
        "machine_id": new_machine.id,
        "machine_name": new_machine.machine_name,
        "machine_protocol": new_machine.machine_protocol
    }

@app.post("/config/combined")
async def add_combined_config(
        config: CombinedConfigCreate,
        db: Session = Depends(get_db)
):
    latest_machine = db.query(MachineDetails).order_by(MachineDetails.id.desc()).first()
    if not latest_machine:
        raise HTTPException(status_code=404, detail="No machine found. Please add a machine first.")

    rs485_config = RS485Config(
        machine_id=latest_machine.id,
        com_port=config.com_port,
        baud_rate=config.baud_rate,
        data_bits=config.data_bits,
        stop_bits=config.stop_bits,
        parity=config.parity,
        slave_id=config.slave_id
    )

    reading_params = ReadingParameters(
        machine_id=latest_machine.id,
        function_code=config.function_code,
        start_address=config.start_address,
        end_address=config.end_address,
        specific_address=config.specific_address,
        # data_format=config.data_format,
        combine_register=config.combine_register
    )

    db.add(rs485_config)
    db.add(reading_params)
    db.commit()
    db.refresh(rs485_config)
    db.refresh(reading_params)

    return {
        "message": "Configurations saved successfully",
        "machine_id": latest_machine.id,
        "machine_name": latest_machine.machine_name,
        "rs485_config_id": rs485_config.id,
        "reading_params_id": reading_params.id
    }

@app.get("/machine-config/{machine_id}", response_model=MachineConfigResponse)
async def get_machine_config(machine_id: int, db: Session = Depends(get_db)):
    machine = db.query(MachineDetails).filter(MachineDetails.id == machine_id).first()
    if not machine:
        raise HTTPException(status_code=404, detail="Machine not found")

    rs485_config = db.query(RS485Config).filter(RS485Config.machine_id == machine_id).first()
    reading_params = db.query(ReadingParameters).filter(ReadingParameters.machine_id == machine_id).first()

    response = {
        "machine_id": machine.id,
        "machine_name": machine.machine_name,
        "machine_protocol": machine.machine_protocol,
        "machine_photo": machine.machine_photo,
        "created_at": machine.created_at,
        "rs485_config": None,
        "reading_parameters": None
    }

    if rs485_config:
        response["rs485_config"] = {
            "id": rs485_config.id,
            "com_port": rs485_config.com_port,
            "baud_rate": rs485_config.baud_rate,
            "data_bits": rs485_config.data_bits,
            "stop_bits": rs485_config.stop_bits,
            "parity": rs485_config.parity,
            "slave_id": rs485_config.slave_id,
            "created_at": rs485_config.created_at
        }

    if reading_params:
        response["reading_parameters"] = {
            "id": reading_params.id,
            "function_code": reading_params.function_code,
            "start_address": reading_params.start_address,
            "end_address": reading_params.end_address,
            "specific_address": reading_params.specific_address,
            # "data_format": reading_params.data_format,
            "combine_register": reading_params.combine_register,
            "created_at": reading_params.created_at
        }

    return response

@app.get("/machine-configs", response_model=List[MachineConfigResponse])
async def get_all_machine_configs(db: Session = Depends(get_db)):
    machines = db.query(MachineDetails).all()
    if not machines:
        raise HTTPException(status_code=404, detail="No machines found")

    machine_configs = []
    for machine in machines:
        rs485_config = db.query(RS485Config).filter(RS485Config.machine_id == machine.id).first()
        reading_params = db.query(ReadingParameters).filter(ReadingParameters.machine_id == machine.id).first()

        config = {
            "machine_id": machine.id,
            "machine_name": machine.machine_name,
            "machine_protocol": machine.machine_protocol,
            "machine_photo": machine.machine_photo,
            "created_at": machine.created_at,
            "rs485_config": None,
            "reading_parameters": None
        }

        if rs485_config:
            config["rs485_config"] = {
                "id": rs485_config.id,
                "com_port": rs485_config.com_port,
                "baud_rate": rs485_config.baud_rate,
                "data_bits": rs485_config.data_bits,
                "stop_bits": rs485_config.stop_bits,
                "parity": rs485_config.parity,
                "slave_id": rs485_config.slave_id,
                "created_at": rs485_config.created_at
            }

        if reading_params:
            config["reading_parameters"] = {
                "id": reading_params.id,
                "function_code": reading_params.function_code,
                "start_address": reading_params.start_address,
                "end_address": reading_params.end_address,
                "specific_address": reading_params.specific_address,
                # "data_format": reading_params.data_format,
                "combine_register": reading_params.combine_register,
                "created_at": reading_params.created_at
            }

        machine_configs.append(config)

    return machine_configs
@app.get("/machine/{machine_id}")
async def get_machine(machine_id: int, db: Session = Depends(get_db)):
    machine = db.query(MachineDetails).filter(MachineDetails.id == machine_id).first()
    if not machine:
        raise HTTPException(status_code=404, detail="Machine not found")
    return {
        "machine_id": machine.id,
        "machine_name": machine.machine_name,
        "machine_protocol": machine.machine_protocol,
        "machine_photo": machine.machine_photo,
        "created_at": machine.created_at
    }

@app.get("/machines")
async def get_all_machines(db: Session = Depends(get_db)):
    machines = db.query(MachineDetails).all()
    if not machines:
        raise HTTPException(status_code=404, detail="No machines found")
    return [
        {
            "machine_id": machine.id,
            "machine_name": machine.machine_name,
            "machine_protocol": machine.machine_protocol,
            "machine_photo": machine.machine_photo,
            "created_at": machine.created_at
        }
        for machine in machines
    ]


@app.delete("/machine/{machine_id}")
async def delete_machine(machine_id: int, db: Session = Depends(get_db)):
    # Delete associated configurations first
    db.query(RS485Config).filter(RS485Config.machine_id == machine_id).delete()
    db.query(ReadingParameters).filter(ReadingParameters.machine_id == machine_id).delete()

    # Delete the machine
    machine = db.query(MachineDetails).filter(MachineDetails.id == machine_id).first()
    if not machine:
        raise HTTPException(status_code=404, detail="Machine not found")

    # Delete the machine photo file
    if machine.machine_photo:
        file_path = os.path.join(UPLOAD_DIR, machine.machine_photo)
        if os.path.exists(file_path):
            os.remove(file_path)

    db.delete(machine)
    db.commit()
    return {"message": "Machine and associated configurations deleted successfully"}

@app.put("/config/update/{machine_id}")
async def update_combined_config(
    machine_id: int,
    config: CombinedConfigUpdate,
    db: Session = Depends(get_db)
):
    # Fetch the machine
    machine = db.query(MachineDetails).filter(MachineDetails.id == machine_id).first()
    if not machine:
        raise HTTPException(status_code=404, detail="Machine not found")

    # Update RS485 configuration
    rs485_config = db.query(RS485Config).filter(RS485Config.machine_id == machine_id).first()
    if rs485_config:
        if config.com_port is not None:
            rs485_config.com_port = config.com_port
        if config.baud_rate is not None:
            rs485_config.baud_rate = config.baud_rate
        if config.data_bits is not None:
            rs485_config.data_bits = config.data_bits
        if config.stop_bits is not None:
            rs485_config.stop_bits = config.stop_bits
        if config.parity is not None:
            rs485_config.parity = config.parity
        if config.slave_id is not None:
            rs485_config.slave_id = config.slave_id
        db.add(rs485_config)

    # Update reading parameters
    reading_params = db.query(ReadingParameters).filter(ReadingParameters.machine_id == machine_id).first()
    if reading_params:
        if config.function_code is not None:
            reading_params.function_code = config.function_code
        if config.start_address is not None:
            reading_params.start_address = config.start_address
        if config.end_address is not None:
            reading_params.end_address = config.end_address
        if config.specific_address is not None:
            reading_params.specific_address = config.specific_address
        # if config.data_format is not None:
        #     reading_params.data_format = config.data_format
        if config.combine_register is not None:
            reading_params.combine_register = config.combine_register
        db.add(reading_params)

    # Commit the updates
    db.commit()

    return {"message": "Configuration updated successfully"}

# Modbus Communication Endpoints
# @app.post("/connect-machine")
# async def connect_machine(machine_id: int = Body(...), db: Session = Depends(get_db)):
#     """Connect to a specific machine"""
#     # Retrieve machine details
#     machine = db.query(MachineDetails).filter(MachineDetails.id == machine_id).first()
#     if not machine:
#         raise HTTPException(status_code=404, detail="Machine not found")
#
#     if machine.machine_protocol.lower() != "modbus rtu 485":
#         raise HTTPException(status_code=400, detail="Only Modbus RTU 485 protocol is supported")
#
#     # Retrieve RS485 configuration
#     rs485_config = db.query(RS485Config).filter(RS485Config.machine_id == machine_id).first()
#     if not rs485_config:
#         raise HTTPException(status_code=404, detail="RS485 configuration not found")
#
#     try:
#         # Create new connection
#         connection = DeviceConnection(
#             port=rs485_config.com_port,
#             slave_address=rs485_config.slave_id,
#             baudrate=rs485_config.baud_rate,
#             parity=rs485_config.parity,
#             stopbits=rs485_config.stop_bits,
#             timeout=5.0
#         )
#
#         # Add to connection manager
#         connection_manager.add_connection(machine_id, connection)
#
#         return {
#             "status": "success",
#             "message": f"Successfully connected to machine {machine_id}"
#         }
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Connection failed: {str(e)}")
def get_connection_manager():
    return connection_manager

# @app.post("/connect-machine/{machine_id}")
# async def connect_machine(
#     machine_id: int,
#     db: Session = Depends(get_db),
#     connection_manager: operations.UnifiedConnectionManager = Depends(get_connection_manager)
# ):
#     # Get machine details
#     machine = db.query(MachineDetails).filter(MachineDetails.id == machine_id).first()
#     if not machine:
#         raise HTTPException(status_code=404, detail="Machine not found")
#
#     # Check if already connected
#     if connection_manager.is_connected(machine_id):
#         return {"status": "success", "message": f"Machine {machine_id} is already connected"}
#
#     try:
#         if machine.machine_protocol == "Modbus TCP/IP":
#             config = db.query(ModbusTCP).filter(ModbusTCP.machine_id == machine_id).first()
#             if not config:
#                 raise HTTPException(status_code=404, detail="Modbus TCP/IP configuration not found")
#
#             connection = connection_manager.create_connection(
#                 machine_id,
#                 protocol="Modbus TCP/IP",
#                 host=config.ip_address,
#                 port=config.port
#             )
#
#         elif machine.machine_protocol == "Modbus RTU 485":
#             config = db.query(RS485Config).filter(RS485Config.machine_id == machine_id).first()
#             if not config:
#                 raise HTTPException(status_code=404, detail="RS485 configuration not found")
#
#             connection = connection_manager.create_connection(
#                 machine_id,
#                 protocol="Modbus RTU 485",
#                 port=config.com_port,
#                 slave_address=config.slave_id,
#                 baudrate=config.baud_rate,
#                 parity=config.parity,
#                 stopbits=config.stop_bits
#             )
#         elif machine.machine_protocol == "OPC UA":
#             config = db.query(OPCUA).filter(OPCUA.machine_id == machine_id).first()
#             if not config:
#                 raise HTTPException(status_code=404, detail="OPC UA configuration not found")
#
#             connection = connection_manager.create_connection(
#                 machine_id,
#                 protocol="opcua",
#                 url=config.server_url,
#                 username=config.username if config.username != "None" else None,
#                 password=config.password if config.password != "None" else None,
#                 security_policy=config.security_policy if config.security_policy != "None" else None,
#                 security_mode=config.security_mode if config.security_mode != "None" else None
#             )
#
#         elif machine.machine_protocol == "Profinet":
#
#             # Handle different OEM types
#             siemens_config = db.query(models.SiemensProfinetConfig).filter(
#                 models.SiemensProfinetConfig.machine_id == machine_id).first()
#             beckhoff_config = db.query(models.BeckhoffProfinetConfig).filter(
#                 models.BeckhoffProfinetConfig.machine_id == machine_id).first()
#             phoenix_config = db.query(models.PhoenixProfinetConfig).filter(
#                 models.PhoenixProfinetConfig.machine_id == machine_id).first()
#             abb_config = db.query(models.ABBProfinetConfig).filter(
#                 models.ABBProfinetConfig.machine_id == machine_id).first()
#             br_config = db.query(models.BRProfinetConfig).filter(
#                 models.BRProfinetConfig.machine_id == machine_id).first()
#
#             config = None
#             connection_params = {}
#
#             if siemens_config:
#                 config = siemens_config
#                 connection_params = {
#                     'oem_type': 'siemens',
#                     'ip_address': config.ip_address,
#                     'rack': config.rack,
#                     'slot': config.slot,
#                     'port': config.port
#                 }
#             elif beckhoff_config:
#                 config = beckhoff_config
#                 connection_params = {
#                     'oem_type': 'beckhoff',
#                     'ip_address': config.ip_address,
#                     'ams_net_id': config.ams_net_id,
#                     'target_ads_port': config.target_ads_port,
#                     'port': config.port
#                 }
#             elif phoenix_config:
#                 config = phoenix_config
#                 connection_params = {
#                     'oem_type': 'phoenix',
#                     'ip_address': config.ip_address,
#                     'device_name': config.device_name,
#                     'vlan_id': config.vlan_id,
#                     'port': config.port
#                 }
#             elif abb_config:
#                 config = abb_config
#                 connection_params = {
#                     'oem_type': 'abb',
#                     'ip_address': config.ip_address,
#                     'device_id': config.device_id,
#                     'subnet_mask': config.subnet_mask,
#                     'port': config.port
#                 }
#             elif br_config:
#                 config = br_config
#                 connection_params = {
#                     'oem_type': 'br',
#                     'ip_address': config.ip_address,
#                     'node_number': config.node_number,
#                     'cycle_time': config.cycle_time,
#                     'port': config.port
#                 }
#
#             if not config:
#                 raise HTTPException(status_code=404, detail="Profinet configuration not found")
#
#             connection = connection_manager.create_connection(
#                 machine_id,
#                 protocol="profinet",
#                 **connection_params
#             )
#         else:
#             raise HTTPException(status_code=400, detail="Unsupported protocol type")
#
#         # Add connection to manager
#         success = connection_manager.add_connection(machine_id, connection)
#         if not success:
#             raise Exception("Failed to add connection to manager")
#
#         return {
#             "status": "success",
#             "message": f"Successfully connected to machine {machine_id} via {machine.machine_protocol}"
#         }
#
#     except Exception as e:
#         logger.error(f"Connection failed for machine {machine_id}: {str(e)}")
#         raise HTTPException(
#             status_code=500,
#             detail=f"Failed to establish connection: {str(e)}"
#         )

@app.post("/connect-machine/{machine_id}")
async def connect_machine(
        machine_id: int,
        db: Session = Depends(get_db),
        connection_manager: operations.UnifiedConnectionManager = Depends(get_connection_manager)
):
    # Get machine details
    machine = db.query(MachineDetails).filter(MachineDetails.id == machine_id).first()
    if not machine:
        raise HTTPException(status_code=404, detail="Machine not found")

    # Check if already connected
    if connection_manager.is_connected(machine_id):
        return {"status": "success", "message": f"Machine {machine_id} is already connected"}

    try:
        if machine.machine_protocol == "Modbus TCP/IP":
            config = db.query(ModbusTCP).filter(ModbusTCP.machine_id == machine_id).first()
            if not config:
                raise HTTPException(status_code=404, detail="Modbus TCP/IP configuration not found")

            connection = connection_manager.create_connection(
                machine_id,
                protocol="Modbus TCP/IP",
                host=config.ip_address,
                port=config.port
            )

        elif machine.machine_protocol == "Modbus RTU 485":
            config = db.query(RS485Config).filter(RS485Config.machine_id == machine_id).first()
            if not config:
                raise HTTPException(status_code=404, detail="RS485 configuration not found")

            connection = connection_manager.create_connection(
                machine_id,
                protocol="Modbus RTU 485",
                port=config.com_port,
                slave_address=config.slave_id,
                baudrate=config.baud_rate,
                parity=config.parity,
                stopbits=config.stop_bits
            )
        elif machine.machine_protocol == "OPC UA":
            config = db.query(OPCUA).filter(OPCUA.machine_id == machine_id).first()
            if not config:
                raise HTTPException(status_code=404, detail="OPC UA configuration not found")

            connection = connection_manager.create_connection(
                machine_id,
                protocol="opcua",
                url=config.server_url,
                username=config.username if config.username != "None" else None,
                password=config.password if config.password != "None" else None,
                security_policy=config.security_policy if config.security_policy != "None" else None,
                security_mode=config.security_mode if config.security_mode != "None" else None
            )

        elif machine.machine_protocol == "Profinet":
            # Handle different OEM types
            siemens_config = db.query(models.SiemensProfinetConfig).filter(
                models.SiemensProfinetConfig.machine_id == machine_id).first()
            beckhoff_config = db.query(models.BeckhoffProfinetConfig).filter(
                models.BeckhoffProfinetConfig.machine_id == machine_id).first()
            phoenix_config = db.query(models.PhoenixProfinetConfig).filter(
                models.PhoenixProfinetConfig.machine_id == machine_id).first()
            abb_config = db.query(models.ABBProfinetConfig).filter(
                models.ABBProfinetConfig.machine_id == machine_id).first()
            br_config = db.query(models.BRProfinetConfig).filter(
                models.BRProfinetConfig.machine_id == machine_id).first()

            config = None
            connection_params = {}

            if siemens_config:
                config = siemens_config
                connection_params = {
                    'oem_type': 'siemens',
                    'ip_address': config.ip_address,
                    'rack': config.rack,
                    'slot': config.slot,
                    'port': config.port
                }
            elif beckhoff_config:
                config = beckhoff_config
                connection_params = {
                    'oem_type': 'beckhoff',
                    'ip_address': config.ip_address,
                    'ams_net_id': config.ams_net_id,
                    'target_ads_port': config.target_ads_port,
                    'port': config.port
                }
            elif phoenix_config:
                config = phoenix_config
                connection_params = {
                    'oem_type': 'phoenix',
                    'ip_address': config.ip_address,
                    'device_name': config.device_name,
                    'vlan_id': config.vlan_id,
                    'port': config.port
                }
            elif abb_config:
                config = abb_config
                connection_params = {
                    'oem_type': 'abb',
                    'ip_address': config.ip_address,
                    'device_id': config.device_id,
                    'subnet_mask': config.subnet_mask,
                    'port': config.port
                }
            elif br_config:
                config = br_config
                connection_params = {
                    'oem_type': 'br',
                    'ip_address': config.ip_address,
                    'node_number': config.node_number,
                    'cycle_time': config.cycle_time,
                    'port': config.port
                }

            if not config:
                raise HTTPException(status_code=404, detail="Profinet configuration not found")

            connection = connection_manager.create_connection(
                machine_id,
                protocol="profinet",
                **connection_params
            )

        elif machine.machine_protocol == "MQTT":
            config = db.query(MqttConfiguration).filter(MqttConfiguration.machine_id == machine_id).first()
            if not config:
                raise HTTPException(status_code=404, detail="MQTT configuration not found")

            connection = connection_manager.create_connection(
                machine_id,
                protocol="mqtt",
                broker=config.broker,
                port=config.port,
                username=config.username if config.username and config.username != "None" else None,
                password=config.password if config.password and config.password != "None" else None,
                client_id=f"machine_{machine_id}_{uuid.uuid4().hex[:8]}"  # Generate unique client ID
            )
        # elif machine.machine_protocol == "CoAPP":
        #     config = db.query(CoAPPConfiguration).filter(CoAPPConfiguration.machine_id == machine_id).first()
        #     if not config:
        #         raise HTTPException(status_code=404, detail="CoApp configuration not found")
        #
        #     connection = connection_manager.create_connection(
        #         machine_id,
        #         protocol="coapp",
        #         ip_address=config.ip_address,
        #         port=config.port
        #     )
        elif machine.machine_protocol == "CoAPP":
            config = db.query(CoAPPConfigurations).filter(CoAPPConfigurations.machine_id == machine_id).first()
            if not config:
                raise HTTPException(status_code=404, detail="CoAP configuration not found")

            connection = connection_manager.create_connection(
                machine_id,
                protocol="coapp",
                ip_address=config.ip_address,
                port=config.port
            )

        elif machine.machine_protocol == "amqp":
            config = db.query(models.AmqpConfiguration).filter(models.AmqpConfiguration.machine_id == machine_id).first()
            if not config:
                raise HTTPException(status_code=404, detail="AMQP configuration not found")

            connection = connection_manager.create_connection(
                machine_id,
                protocol="amqp",
                host=config.host,
                port=config.port,
                virtual_host=config.virtual_host,
                username=config.username if config.username and config.username != "None" else None,
                password=config.password if config.password and config.password != "None" else None,
                exchange=config.exchange,
                routing_key=config.routing_key,
                queue=config.queue
            )

        else:
            raise HTTPException(status_code=400, detail="Unsupported protocol type")

        # Add connection to manager
        success = connection_manager.add_connection(machine_id, connection)
        if not success:
            raise Exception("Failed to add connection to manager")

        return {
            "status": "success",
            "message": f"Successfully connected to machine {machine_id} via {machine.machine_protocol}"
        }

    except Exception as e:
        logger.error(f"Connection failed for machine {machine_id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to establish connection: {str(e)}"
        )


@app.post("/disconnect-machine/{machine_id}")
async def disconnect_machine(machine_id: int, db: Session = Depends(get_db)):
    """Disconnect a specific machine"""
    if not connection_manager.is_connected(machine_id):
        raise HTTPException(status_code=400, detail="Machine is not connected")

    try:
        connection_manager.remove_connection(machine_id)
        return {
            "status": "success",
            "message": f"Successfully disconnected machine {machine_id}"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Disconnection failed: {str(e)}")


@app.get("/check-connection/{machine_id}")
async def check_connection(machine_id: int, db: Session = Depends(get_db)):
    """Check if a specific machine is currently connected"""
    try:
        # Check if machine exists
        machine = db.query(MachineDetails).filter(MachineDetails.id == machine_id).first()
        if not machine:
            raise HTTPException(status_code=404, detail="Machine not found")

        # Get connection status from connection manager
        is_connected = connection_manager.is_connected(machine_id)

        return {
            "status": "success",
            "isConnected": is_connected,
            "machine_id": machine_id,
            "machine_name": machine.machine_name
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error checking connection: {str(e)}")


@app.post("/read-raw-data")
async def read_raw_data(machine_id: int = Body(...), db: Session = Depends(get_db)):
    """Read raw data from Modbus registers and conditionally store based on configuration."""

    # Retrieve machine details
    machine = db.query(MachineDetails).filter(MachineDetails.id == machine_id).first()
    if not machine:
        raise HTTPException(status_code=404, detail="Machine not found")

    # Check if machine connection is valid
    connection = connection_manager.get_connection(machine_id)
    if not connection or not connection.is_connection_valid():
        raise HTTPException(status_code=500, detail="Connection to machine is not valid")

    # Retrieve all reading parameters for the given machine
    reading_parameters_list = db.query(ReadingParameters).filter(ReadingParameters.machine_id == machine_id).all()
    if not reading_parameters_list:
        raise HTTPException(status_code=404, detail="No reading parameters found for the machine")

    try:
        instrument = connection.instrument
        all_results = []
        formatted_results = []

        for reading_parameters in reading_parameters_list:
            function_code = reading_parameters.function_code
            start_address = reading_parameters.start_address
            end_address = reading_parameters.end_address
            specific_address = reading_parameters.specific_address

            # Determine register addresses
            if start_address > 0 and (end_address == 0 or end_address is None):
                end_address = start_address
            elif start_address == 0:
                start_address = specific_address
                if not start_address:
                    continue
                end_address = start_address

            register_addresses = list(range(start_address, end_address + 1))

            # Get the raw register values
            result = get_register_values(instrument, function_code, start_address, end_address)

            if isinstance(result, dict) and result.get("status") == "error":
                for reg in register_addresses:
                    formatted_results.append(f"register {reg} - error ({result['message']})")
                all_results.append({
                    "register_addresses": register_addresses,
                    "status": "error",
                    "message": result["message"]
                })
                continue

            # Append successful results
            data_values = result.get("data") if isinstance(result, dict) else result
            for reg, value in zip(register_addresses, data_values):
                formatted_results.append(f"register {reg} - {value}")

            all_results.append({
                "register_addresses": register_addresses,
                "data": data_values,
                "status": "success"
            })

        if all_results:
            # Get data storage configuration
            data_storage_config = db.query(DataStorageConfig).filter(DataStorageConfig.machine_id == machine_id).first()
            if not data_storage_config:
                raise HTTPException(status_code=404, detail="Data storage configuration not found.")

            # Store raw data only if store_in_database is true
            if data_storage_config.store_in_database.lower() == 'true':
                await store_raw_data(
                    machine_id=machine_id,
                    readings=[
                        {
                            "function_code": reading_parameters.function_code,
                            "start_address": result["register_addresses"][0] if "register_addresses" in result else None,
                            "data": result["data"] if "data" in result else [],
                            "status": result.get("status", "success"),
                            "message": result.get("message")
                        }
                        for result in all_results if "register_addresses" in result and result["register_addresses"]
                    ],
                    db=db
                )

            # Handle OPC UA server if enabled
            opcua_url = None
            if data_storage_config.store_in_opcua_server.lower() == 'true':
                try:
                    server_manager = OPCUAServerManager(machine_id)
                    server_manager.start_server()

                    opcua_data = []
                    for params, result in zip(reading_parameters_list, all_results):
                        if result.get("status") == "success" and "register_addresses" in result and "data" in result:
                            opcua_data.append({
                                "function_code": params.function_code,
                                "register_addresses": result["register_addresses"],
                                "data": result["data"],
                                "status": "success"
                            })

                    server_manager.update_nodes(db, opcua_data, data_type="raw")
                    opcua_url = server_manager.endpoint

                except Exception as e:
                    logger.error(f"Error updating OPC UA server: {str(e)}")
                    raise HTTPException(status_code=500, detail=f"Error updating OPC UA server: {str(e)}")

            return {
                "status": "success",
                "message": "Read registers successfully" +
                          (" and stored" if data_storage_config.store_in_database.lower() == 'true' else ""),
                "data": formatted_results,
                "opcua_url": opcua_url
            }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading registers: {str(e)}")

@app.post("/read-actual-data")
async def read_actual_data(machine_id: int = Body(...), db: Session = Depends(get_db)):
    """Read actual data and conditionally store based on configuration."""

    # Retrieve machine details
    machine = db.query(MachineDetails).filter(MachineDetails.id == machine_id).first()
    if not machine:
        raise HTTPException(status_code=404, detail="Machine not found")

    # Check if machine connection is valid
    connection = connection_manager.get_connection(machine_id)
    if not connection or not connection.is_connection_valid():
        raise HTTPException(status_code=500, detail="Connection to machine is not valid")

    # Retrieve all reading parameters
    reading_parameters_list = db.query(ReadingParameters).filter(ReadingParameters.machine_id == machine_id).all()
    if not reading_parameters_list:
        raise HTTPException(status_code=404, detail="No reading parameters found for the machine")

    try:
        instrument = connection.instrument
    except AttributeError as e:
        raise HTTPException(status_code=500, detail="Invalid connection configuration") from e

    actual_data_results = []
    formatted_results = []

    try:
        for reading_parameters in reading_parameters_list:
            function_code = reading_parameters.function_code
            start_address = reading_parameters.start_address
            end_address = reading_parameters.end_address

            if start_address > 0 and (end_address == 0 or end_address is None):
                end_address = None
            elif start_address == 0:
                start_address = reading_parameters.specific_address
                if not start_address:
                    continue
                end_address = None

            raw_data = get_register_values(instrument, function_code, start_address, end_address)

            if isinstance(raw_data, dict) and raw_data.get("status") == "error":
                formatted_results.append(f"register {start_address} - error ({raw_data['message']})")
                actual_data_results.append({
                    "register_addresses": [start_address],
                    "status": "error",
                    "message": raw_data["message"]
                })
                continue

            data = raw_data.get("data") if isinstance(raw_data, dict) else raw_data

            if not isinstance(data, list):
                formatted_results.append(f"register {start_address} - error (Invalid data format)")
                actual_data_results.append({
                    "register_addresses": [start_address],
                    "status": "error",
                    "message": "Invalid raw data format. Expected a list of integers."
                })
                continue

            if end_address is None:
                if len(data) >= 2:
                    float_value = combine_registers_and_convert_to_float(data[:2], endianess="big")
                    formatted_results.append(f"register {start_address} - {float_value}")
                    actual_data_results.append({
                        "register_addresses": [start_address],
                        "actual_data": float_value,
                        "status": "success"
                    })
                else:
                    formatted_results.append(f"register {start_address} - error (Insufficient data for float conversion)")
                    actual_data_results.append({
                        "register_addresses": [start_address],
                        "status": "error",
                        "message": "Insufficient data received for float conversion"
                    })
            else:
                range_data = []
                register_addresses = list(range(start_address, end_address + 1, 2))
                for i in range(0, len(data) - 1, 2):
                    try:
                        float_value = combine_registers_and_convert_to_float(data[i:i + 2], endianess="big")
                        range_data.append(float_value)
                        formatted_results.append(f"register {register_addresses[i//2]} - {float_value}")
                    except ValueError as e:
                        error_msg = f"Error: {str(e)} at index {i}"
                        range_data.append(error_msg)
                        formatted_results.append(f"register {register_addresses[i//2]} - error ({error_msg})")

                actual_data_results.append({
                    "register_addresses": register_addresses,
                    "actual_data": range_data,
                    "status": "success"
                })

        if actual_data_results:
            # Get data storage configuration
            data_storage_config = db.query(DataStorageConfig).filter(DataStorageConfig.machine_id == machine_id).first()
            if not data_storage_config:
                raise HTTPException(status_code=404, detail="Data storage configuration not found.")

            # Store actual data only if store_in_database is true
            if data_storage_config.store_in_database.lower() == 'true':
                storage_readings = []
                for params, result in zip(reading_parameters_list, actual_data_results):
                    if result.get("status") == "success":
                        if isinstance(result["actual_data"], list):
                            for addr, value in zip(result["register_addresses"], result["actual_data"]):
                                storage_readings.append({
                                    "function_code": params.function_code,
                                    "address": addr,
                                    "actual_data": value,
                                    "status": "success"
                                })
                        else:
                            storage_readings.append({
                                "function_code": params.function_code,
                                "address": result["register_addresses"][0],
                                "actual_data": result["actual_data"],
                                "status": "success"
                            })
                    else:
                        storage_readings.append({
                            "function_code": params.function_code,
                            "address": result["register_addresses"][0],
                            "status": "error",
                            "message": result.get("message", "Unknown error")
                        })

                await store_actual_data(
                    machine_id=machine_id,
                    readings=storage_readings,
                    db=db
                )

            # Handle OPC UA server if enabled
            opcua_url = None
            if data_storage_config.store_in_opcua_server.lower() == 'true':
                try:
                    server_manager = OPCUAServerManager(machine_id)
                    server_manager.start_server()

                    opcua_data = []
                    for params, result in zip(reading_parameters_list, actual_data_results):
                        if result.get("status") == "success":
                            opcua_data.append({
                                "function_code": params.function_code,
                                "register_addresses": result["register_addresses"],
                                "data": result["actual_data"] if isinstance(result["actual_data"], list)
                                else [result["actual_data"]],
                                "status": "success"
                            })

                    server_manager.update_nodes(db, opcua_data, data_type="actual")
                    opcua_url = server_manager.endpoint

                except Exception as e:
                    logger.error(f"Error updating OPC UA server: {str(e)}")
                    raise HTTPException(status_code=500, detail=f"Error updating OPC UA server: {str(e)}")

            return {
                "status": "success",
                "message": "Read and converted registers successfully" +
                          (" and stored" if data_storage_config.store_in_database.lower() == 'true' else ""),
                "data": formatted_results,
                "opcua_url": opcua_url
            }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error during data conversion: {str(e)}")

# ''''''''''''''''''''''''''''''''''''''//START//''''''''''''''''''''''''''''''''''''''''''''''

def create_opcua_server_with_nodes(machine_id: int, db: Session, results: List[dict], data_type: str) -> None:
    """Create or update OPC UA server and nodes for a machine."""
    try:
        server_manager = OPCUAServerManager(machine_id)

        # Start server if it's not already running
        if not server_manager.server:
            server_manager.start_server()

        # Update nodes with new data, passing the database session
        server_manager.update_nodes(db, results, data_type)

        logger.info(f"Successfully updated OPC UA server for machine {machine_id}")
        return server_manager.endpoint

    except Exception as e:
        logger.error(f"Error in create_opcua_server_with_nodes: {str(e)}")
        raise

@app.post("/store-raw-data")
async def store_raw_data(
        machine_id: int = Body(...),
        readings: List[dict] = Body(...),
        db: Session = Depends(get_db)
):
    """Store raw Modbus readings in TimescaleDB, correctly handling multiple addresses."""
    try:
        stored_readings = []
        current_timestamp = datetime.utcnow()

        for reading in readings:
            start_address = reading.get("start_address")
            raw_data = reading.get("data")

            if start_address is None or raw_data is None:
                continue  # Skip invalid readings

            if isinstance(raw_data, list):
                for idx, value in enumerate(raw_data):
                    register_address = start_address + idx
                    db_entry = ModbusRawData(
                        timestamp=current_timestamp,
                        machine_id=machine_id,
                        function_code=reading.get("function_code"),
                        register_address=register_address,
                        raw_value={"value": value},
                        status=reading.get("status", "success"),
                        error_message=reading.get("message")
                    )
                    db.add(db_entry)
                    stored_readings.append({
                        "register_address": register_address,
                        "raw_value": value
                    })
            else:
                db_entry = ModbusRawData(
                    timestamp=current_timestamp,
                    machine_id=machine_id,
                    function_code=reading.get("function_code"),
                    register_address=start_address,
                    raw_value={"value": raw_data},
                    status=reading.get("status", "success"),
                    error_message=reading.get("message")
                )
                db.add(db_entry)
                stored_readings.append({
                    "register_address": start_address,
                    "raw_value": raw_data
                })

        db.commit()
        return {
            "status": "success",
            "message": f"Stored {len(stored_readings)} raw readings",
            "stored_data": stored_readings
        }

    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error storing raw data: {str(e)}")

@app.post("/store-actual-data")
async def store_actual_data(
        machine_id: int = Body(...),
        readings: List[dict] = Body(...),
        db: Session = Depends(get_db)
):
    """Store actual (converted) Modbus readings in TimescaleDB."""
    try:
        stored_readings = []
        current_timestamp = datetime.utcnow()

        for reading in readings:
            if "address" in reading:
                # Single value
                db_entry = ModbusActualData(
                    timestamp=current_timestamp,
                    machine_id=machine_id,
                    function_code=reading.get("function_code"),
                    register_address=reading["address"],
                    actual_value=reading.get("actual_data"),
                    status=reading.get("status", "success"),
                    error_message=reading.get("message")
                )
                db.add(db_entry)
                stored_readings.append({
                    "register_address": reading["address"],
                    "actual_value": reading.get("actual_data")
                })
            elif "start_address" in reading and "actual_data" in reading:
                # Range of values
                actual_data = reading["actual_data"]
                if isinstance(actual_data, list):
                    for idx, value in enumerate(actual_data):
                        register_address = reading["start_address"] + (idx * 2)
                        db_entry = ModbusActualData(
                            timestamp=current_timestamp,
                            machine_id=machine_id,
                            function_code=reading.get("function_code"),
                            register_address=register_address,
                            actual_value=value,
                            status=reading.get("status", "success"),
                            error_message=reading.get("message")
                        )
                        db.add(db_entry)
                        stored_readings.append({
                            "register_address": register_address,
                            "actual_value": value
                        })

        db.commit()
        return {
            "status": "success",
            "message": f"Stored {len(stored_readings)} actual readings",
            "stored_data": stored_readings
        }

    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error storing actual data: {str(e)}")

# Storage Configuration Endpoints
@app.post("/store-data")
async def store_data(
        config: DataStorageConfigCreate,
        db: Session = Depends(get_db)
):
    # Get the latest machine from machine_details table
    latest_machine = db.query(MachineDetails).order_by(MachineDetails.id.desc()).first()
    if not latest_machine:
        raise HTTPException(status_code=404, detail="No machines found. Please add a machine first.")

    # Create new data storage config with automatically fetched machine_id
    data_storage_config = DataStorageConfig(
        machine_id=latest_machine.id,
        store_in_database=config.store_in_database,
        store_in_opcua_server=config.store_in_opcua_server
    )
    db.add(data_storage_config)
    db.commit()
    db.refresh(data_storage_config)

    return {
        "message": "Data storage configuration saved successfully",
        "config": {
            "id": data_storage_config.id,
            "machine_id": data_storage_config.machine_id,
            "machine_name": latest_machine.machine_name,  # Added to show which machine was configured
            "store_in_database": data_storage_config.store_in_database,
            "store_in_opcua_server": data_storage_config.store_in_opcua_server,
            "created_at": data_storage_config.created_at
        }
    }


@app.put("/update-data-storage/machine/{machine_id}")
async def update_data_storage_by_machine(
        machine_id: int,
        config: DataStorageConfigUpdate,
        db: Session = Depends(get_db)
):
    # Find the existing machine
    machine = db.query(MachineDetails).filter(MachineDetails.id == machine_id).first()
    if not machine:
        raise HTTPException(status_code=404, detail=f"Machine with ID {machine_id} not found")

    # Find the data storage config for this machine
    existing_config = db.query(DataStorageConfig).filter(DataStorageConfig.machine_id == machine_id).first()
    if not existing_config:
        raise HTTPException(status_code=404, detail=f"Data storage configuration for machine ID {machine_id} not found")

    # Update the configuration
    if config.store_in_database is not None:
        existing_config.store_in_database = config.store_in_database
    if config.store_in_opcua_server is not None:
        existing_config.store_in_opcua_server = config.store_in_opcua_server

    db.commit()
    db.refresh(existing_config)

    return {
        "message": "Data storage configuration updated successfully",
        "config": {
            "id": existing_config.id,
            "machine_id": existing_config.machine_id,
            "machine_name": machine.machine_name,
            "store_in_database": existing_config.store_in_database,
            "store_in_opcua_server": existing_config.store_in_opcua_server,
            "updated_at": datetime.now()  # Assuming you track update time
        }
    }
# Keep the GET endpoint for retrieving storage config
@app.get("/storage-config/{machine_id}")
async def get_storage_config(machine_id: int, db: Session = Depends(get_db)):
    config = db.query(DataStorageConfig).filter(DataStorageConfig.machine_id == machine_id).first()
    if not config:
        raise HTTPException(status_code=404, detail="Storage configuration not found for this machine")

    # Get machine details to include machine name in response
    machine = db.query(MachineDetails).filter(MachineDetails.id == machine_id).first()

    return {
        "id": config.id,
        "machine_id": config.machine_id,
        "machine_name": machine.machine_name if machine else None,
        "store_in_database": config.store_in_database,
        "store_in_opcua_server": config.store_in_opcua_server,
        "created_at": config.created_at
    }




# Stat Card Endpoints
@app.post("/stat-cards")
async def create_stat_card(
        machine_id: int = Body(...),
        machine_photo: str = Body(...),
        config_data: dict = Body(...),
        db: Session = Depends(get_db)
):
    stat_card = StatCard(
        machine_id=machine_id,
        machine_photo=machine_photo,
        config_data=config_data
    )
    db.add(stat_card)
    db.commit()
    db.refresh(stat_card)

    return {
        "id": stat_card.id,
        "machine_id": stat_card.machine_id,
        "machine_photo": stat_card.machine_photo,
        "config_data": stat_card.config_data,
        "created_at": stat_card.created_at
    }


@app.get("/stat-cards")
async def get_stat_cards(db: Session = Depends(get_db)):
    stat_cards = db.query(StatCard).all()
    return [
        {
            "id": card.id,
            "machine_id": card.machine_id,
            "machine_photo": card.machine_photo,
            "config_data": card.config_data,
            "created_at": card.created_at
        }
        for card in stat_cards
    ]


@app.delete("/stat-cards/{card_id}")
async def delete_stat_card(card_id: int, db: Session = Depends(get_db)):
    stat_card = db.query(StatCard).filter(StatCard.id == card_id).first()
    if not stat_card:
        raise HTTPException(status_code=404, detail="Stat card not found")

    db.delete(stat_card)
    db.commit()
    return {"message": "Stat card deleted successfully"}



# %Below code is the endpoints for Specific Register address fecthinga and storing the selected specific register address and its value also fetching both storing register address and fecthing values using same endpoint below
# configure-and-read-register endpoint

@app.post("/get-register-addresses")
async def get_register_addresses(machine_id: int = Body(...), db: Session = Depends(get_db)):
    """
    Retrieve raw and actual register addresses for the specified machine.
    """
    # Retrieve machine details
    machine = db.query(MachineDetails).filter(MachineDetails.id == machine_id).first()
    if not machine:
        raise HTTPException(status_code=404, detail="Machine not found")

    # Retrieve all reading parameters for the given machine
    reading_parameters_list = db.query(ReadingParameters).filter(ReadingParameters.machine_id == machine_id).all()
    if not reading_parameters_list:
        raise HTTPException(status_code=404, detail="No reading parameters found for the machine")

    # Initialize lists for raw and actual registers
    raw_registers = []
    actual_registers = []

    for reading_parameters in reading_parameters_list:
        start_address = reading_parameters.start_address
        end_address = reading_parameters.end_address
        specific_address = reading_parameters.specific_address

        # Handle single and range of addresses
        if start_address > 0 and (end_address == 0 or end_address is None):
            end_address = start_address  # Single register case
        elif start_address == 0:
            start_address = specific_address
            if not start_address:
                continue
            end_address = start_address

        # Add all raw registers in the range
        raw_registers.extend(range(start_address, end_address + 1))

        # Calculate actual registers based on the logic of combining raw registers
        for i in range(start_address, end_address + 1, 2):  # Combine every two raw registers
            actual_registers.append(i)

    # Format raw and actual registers for output
    raw_registers_formatted = [f"register {reg}" for reg in raw_registers]
    actual_registers_formatted = [f"register {reg}" for reg in actual_registers]

    return {
        "status": "success",
        "message": "Register addresses fetched successfully",
        "data": {
            "raw_registers": raw_registers_formatted,
            "actual_registers": actual_registers_formatted,
        }
    }

# '''''''''''''''''''''''''''//splitted endpoints below to save the selected register address and to fecth the selected register address value//'''''''''''''''''''''''''
# First endpoint: Fast configuration saving
@app.post("/save-register-config/{machine_id}")
async def save_register_config(
        machine_id: int,
        registers: MachineRegisters,
        db: Session = Depends(get_db)
):
    """Quickly save register configurations without reading values"""
    machine = db.query(MachineDetails).filter(MachineDetails.id == machine_id).first()
    if not machine:
        raise HTTPException(status_code=404, detail="Machine not found")

    try:
        # Delete existing configurations in bulk
        db.query(RegisterConfig).filter(RegisterConfig.machine_id == machine_id).delete()

        # Prepare all configurations at once
        configs = []

        # Add raw register configs
        configs.extend([
            RegisterConfig(
                machine_id=machine_id,
                register_number=reg.register_number,
                is_raw=True,
                is_selected=reg.is_selected
            ) for reg in registers.raw_registers
        ])

        # Add actual register configs
        configs.extend([
            RegisterConfig(
                machine_id=machine_id,
                register_number=reg.register_number,
                is_raw=False,
                is_selected=reg.is_selected
            ) for reg in registers.actual_registers
        ])

        # Bulk insert all configurations
        db.bulk_save_objects(configs)
        db.commit()

        return {
            "status": "success",
            "message": "Register configurations saved successfully",
            "machine_id": machine_id
        }

    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error saving configurations: {str(e)}")

@app.post("/save-register-values")
async def save_register_values(
    values: RegisterValueBatch,
    db: Session = Depends(get_db)
):
    """Save the fetched register values for both raw and actual registers"""
    machine = db.query(MachineDetails).filter(MachineDetails.id == values.machine_id).first()
    if not machine:
        raise HTTPException(status_code=404, detail="Machine not found")

    try:
        # Prepare all register values at once
        register_values = []
        # Fix datetime usage to match your model's import style
        current_time = datetime.utcnow()  # Not datetime.datetime.utcnow()

        # Add raw register values
        register_values.extend([
            RegisterValue(
                machine_id=values.machine_id,
                register_number=int(item.register),
                is_raw=True,
                value=item.value,
                status=item.status,
                timestamp=current_time
            ) for item in values.raw_values
        ])

        # Add actual register values
        register_values.extend([
            RegisterValue(
                machine_id=values.machine_id,
                register_number=int(item.register),
                is_raw=False,
                value=item.value,
                status=item.status,
                timestamp=current_time
            ) for item in values.actual_values
        ])

        # Bulk insert all values
        db.bulk_save_objects(register_values)
        db.commit()

        return {
            "status": "success",
            "message": "Register values saved successfully",
            "machine_id": values.machine_id,
            "timestamp": current_time.isoformat(),
            "raw_count": len(values.raw_values),
            "actual_count": len(values.actual_values)
        }

    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error saving register values: {str(e)}")


# '''''''''''''''below OPC UA COMPONENT endpoints connect and retriving all node using machine id ''''''''''''''
@app.post("/opcua/connect/{machine_id}")
async def connect_opc_machine(
    machine_id: int,
    params: Optional[OPCUAConnectionParams] = None,
    db: Session = Depends(get_db)
):
    """Connect a machine to OPC UA server"""
    return await ops.connect_to_server_by_machine(machine_id, db, params)

@app.post("/disconnect/{connection_id}", response_model=DisconnectionResponse)
async def disconnect_endpoint(connection_id: str):
    return await ops.disconnect_from_server(connection_id)

@app.get("/opcua/nodes/{machine_id}")
async def get_machine_nodes(
        machine_id: int,
        db: Session = Depends(get_db)
):
    """Get all nodes for a specific machine"""
    try:
        # Simply return the result without encoding node IDs
        result = await ops.retrieve_all_nodes_by_machine(machine_id, db)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/read_node/machine/{machine_id}", response_model=ReadNodeResponse)
async def read_node_by_machine_endpoint(
    machine_id: int,
    node_request: NodeRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    return await ops.read_node_by_machine(machine_id, node_request.node_id, db, background_tasks)

@app.get("/connection-history")
async def connection_history():
    return await ops.get_connection_history()

@app.on_event("startup")
async def startup_event():
    await ops.init_db_async(SQLALCHEMY_DATABASE_URL, init_db)


# //''''''''''''''''''''Below endpoint For MODBUS TCP/IP '''''''''''''''''''''''//

# POST endpoint to create Modbus TCP/IP configuration
# @app.post("/config/modbus-tcp")
# async def add_modbus_tcp_config(
#         config: ModbusTCPConfigBase,
#         db: Session = Depends(get_db)
# ):
#     # Get the latest machine with Modbus TCP/IP protocol
#     latest_machine = db.query(MachineDetails).filter(
#         MachineDetails.machine_protocol == "Modbus TCP/IP"
#     ).order_by(MachineDetails.id.desc()).first()
#     print(latest_machine)
#     if not latest_machine:
#         raise HTTPException(
#             status_code=404,
#             detail="No Modbus TCP/IP machine found. Please add a machine with Modbus TCP/IP protocol first."
#         )
#
#     # Check if configuration already exists for this machine
#     existing_config = db.query(ModbusTCP).filter(ModbusTCP.machine_id == latest_machine.id).first()
#
#     if existing_config:
#         # Update existing configuration
#         existing_config.ip_address = config.ip_address
#         existing_config.port = config.port
#         existing_config.unit_id = config.unit_id
#
#         # Update reading parameters if provided
#         if config.read_address is not None:
#             existing_config.read_address = config.read_address
#         if config.read_count is not None:
#             existing_config.read_count = config.read_count
#         if config.function_code is not None:
#             existing_config.function_code = config.function_code
#
#         db.commit()
#         db.refresh(existing_config)
#
#         return {
#             "message": "Modbus TCP/IP configuration updated successfully",
#             "machine_id": latest_machine.id,
#             "machine_name": latest_machine.machine_name,
#             "modbus_tcp_config_id": existing_config.id,
#             "operation": "updated"
#         }
#
#     # Create new configuration
#     modbus_tcp_config = ModbusTCP(
#         machine_id=latest_machine.id,
#         ip_address=config.ip_address,
#         port=config.port,
#         unit_id=config.unit_id,
#         read_address=config.read_address,
#         read_count=config.read_count,
#         function_code=config.function_code
#     )
#
#     db.add(modbus_tcp_config)
#     db.commit()
#     db.refresh(modbus_tcp_config)
#
#     return {
#         "message": "Modbus TCP/IP configuration saved successfully",
#         "machine_id": latest_machine.id,
#         "machine_name": latest_machine.machine_name,
#         "modbus_tcp_config_id": modbus_tcp_config.id,
#         "operation": "created"
#     }


@app.post("/config/modbus-tcp")
async def add_modbus_tcp_config(
        config: ModbusTCPConfigBase,
        db: Session = Depends(get_db)
):
    # Get the latest machine with Modbus TCP/IP protocol
    latest_machine = db.query(MachineDetails).filter(
        MachineDetails.machine_protocol == "Modbus TCP/IP"
    ).order_by(MachineDetails.id.desc()).first()

    print(latest_machine)
    if not latest_machine:
        raise HTTPException(
            status_code=404,
            detail="No Modbus TCP/IP machine found. Please add a machine with Modbus TCP/IP protocol first."
        )

    # Check if configuration already exists for this machine
    existing_config = db.query(ModbusTCP).filter(ModbusTCP.machine_id == latest_machine.id).first()

    if existing_config:
        # Update existing configuration
        for field in ['ip_address', 'port', 'unit_id', 'read_address', 'read_count', 'function_code']:
            if hasattr(config, field) and getattr(config, field) is not None:
                setattr(existing_config, field, getattr(config, field))

        db.commit()
        db.refresh(existing_config)

        return {
            "message": "Modbus TCP/IP configuration updated successfully",
            "machine_id": latest_machine.id,
            "machine_name": latest_machine.machine_name,
            "machine_protocol": latest_machine.machine_protocol,  # Include protocol in response
            "modbus_tcp_config_id": existing_config.id,
            "operation": "updated"
        }

    # Create new configuration
    modbus_tcp_config = ModbusTCP(
        machine_id=latest_machine.id,
        ip_address=config.ip_address,
        port=config.port,
        unit_id=config.unit_id,
        read_address=config.read_address,
        read_count=config.read_count,
        function_code=config.function_code
    )

    db.add(modbus_tcp_config)
    db.commit()
    db.refresh(modbus_tcp_config)

    return {
        "message": "Modbus TCP/IP configuration saved successfully",
        "machine_id": latest_machine.id,
        "machine_name": latest_machine.machine_name,
        "machine_protocol": latest_machine.machine_protocol,  # Include protocol in response
        "modbus_tcp_config_id": modbus_tcp_config.id,
        "operation": "created"
    }


# PUT endpoint to update existing Modbus TCP/IP configuration
@app.put("/config/update/modbus-tcp/{machine_id}")
async def update_modbus_tcp_config(
        machine_id: int,
        config: ModbusTCPConfigBase,
        db: Session = Depends(get_db)
):
    # Fetch the machine
    machine = db.query(MachineDetails).filter(MachineDetails.id == machine_id).first()
    if not machine:
        raise HTTPException(status_code=404, detail="Machine not found")

    # Check if the machine protocol is Modbus TCP/IP
    if machine.machine_protocol != "Modbus TCP/IP":
        raise HTTPException(
            status_code=400,
            detail="This machine does not use Modbus TCP/IP protocol"
        )

    # Update Modbus TCP/IP configuration
    modbus_config = db.query(ModbusTCP).filter(ModbusTCP.machine_id == machine_id).first()
    if not modbus_config:
        raise HTTPException(
            status_code=404,
            detail="Modbus TCP/IP configuration not found for this machine"
        )

    # Update fields if provided
    if config.ip_address is not None:
        modbus_config.ip_address = config.ip_address
    if config.port is not None:
        modbus_config.port = config.port
    if config.unit_id is not None:
        modbus_config.unit_id = config.unit_id

    # Update reading parameters if provided
    if config.read_address is not None:
        modbus_config.read_address = config.read_address
    if config.read_count is not None:
        modbus_config.read_count = config.read_count
    if config.function_code is not None:
        modbus_config.function_code = config.function_code

    # Commit the updates
    db.commit()
    db.refresh(modbus_config)

    return {
        "message": "Modbus TCP/IP configuration updated successfully",
        "machine_id": machine_id,
        "machine_name": machine.machine_name,
        "modbus_tcp_config": {
            "id": modbus_config.id,
            "ip_address": modbus_config.ip_address,
            "port": modbus_config.port,
            "unit_id": modbus_config.unit_id,
            "read_address": modbus_config.read_address,
            "read_count": modbus_config.read_count,
            "function_code": modbus_config.function_code
        }
    }


# # GET endpoint to retrieve a machine's configuration
# @app.get("/modbus-tcp/config/{machine_id}", response_model=ModbusTCPConfigResponse)
# def get_modbus_config(machine_id: int, db: Session = Depends(get_db)):
#     config = db.query(ModbusTCP).filter(ModbusTCP.machine_id == machine_id).first()
#     if not config:
#         raise HTTPException(status_code=404, detail=f"No configuration found for machine ID {machine_id}")
#     return config

@app.get("/modbus-tcp/config/{machine_id}", response_model=ModbusTCPConfigResponse)
def get_modbus_config(machine_id: int, db: Session = Depends(get_db)):
    # Query both ModbusTCP and MachineDetails tables
    config = (
        db.query(ModbusTCP, MachineDetails.machine_protocol)
        .join(
            MachineDetails,
            ModbusTCP.machine_id == MachineDetails.id
        )
        .filter(ModbusTCP.machine_id == machine_id)
        .first()
    )

    if not config:
        raise HTTPException(
            status_code=404,
            detail=f"No configuration found for machine ID {machine_id}"
        )

    # Combine the ModbusTCP config with the protocol information
    config_dict = config[0].__dict__
    config_dict["machine_protocol"] = config[1]

    # Remove SQLAlchemy instance state
    config_dict.pop('_sa_instance_state', None)

    return config_dict

# GET endpoint to retrieve all machine configurations
@app.get("/modbus-tcp/configs", response_model=list[ModbusTCPConfigResponse])
def get_all_modbus_configs(db: Session = Depends(get_db)):
    configs = db.query(ModbusTCP).all()
    return configs

@app.post("/read-raw-data-tcp")
async def read_raw_data(machine_id: int = Body(...), db: Session = Depends(get_db)):
    """Read raw data from Modbus registers, store it in the database (if enabled), and update OPC UA (if enabled)."""

    # Retrieve machine details
    machine = db.query(MachineDetails).filter(MachineDetails.id == machine_id).first()
    if not machine:
        raise HTTPException(status_code=404, detail="Machine not found")


    connection = connection_manager.get_connection(machine_id)
    if not connection:
        logger.error(f"Connection for machine {machine_id} not found.")
        raise HTTPException(status_code=500, detail="Connection to machine is not valid")

    if not connection.is_connection_valid():
        logger.error(f"Connection for machine {machine_id} is invalid.")
        raise HTTPException(status_code=500, detail="Connection to machine is not valid")

    logger.debug(f"Connection for machine {machine_id} is valid.")

    # Get Modbus TCP configuration
    modbus_tcp_config = db.query(ModbusTCP).filter(ModbusTCP.machine_id == machine_id).first()

    if not modbus_tcp_config:
        raise HTTPException(status_code=404, detail="Modbus TCP configuration not found")

    try:
        instrument = connection.instrument
        all_results = []
        formatted_results = []

        # Get read parameters from Modbus TCP configuration
        start_address = modbus_tcp_config.read_address  # Previously `start_address`
        read_count = modbus_tcp_config.read_count       # Previously `end_address`
        end_address = start_address + read_count - 1
        function_code = modbus_tcp_config.function_code# Compute actual end address

        register_addresses = list(range(start_address, end_address + 1))

        # Read registers from Modbus device

        result = get_register_values(instrument, function_code, start_address, count=read_count)

        if isinstance(result, dict) and result.get("status") == "error":
            for reg in register_addresses:
                formatted_results.append(f"register {reg} - error ({result['message']})")
            all_results.append({"register_addresses": register_addresses, "status": "error", "message": result["message"]})
        else:
            data_values = result.get("data") if isinstance(result, dict) else result
            for reg, value in zip(register_addresses, data_values):
                formatted_results.append(f"register {reg} - {value}")

            all_results.append({"register_addresses": register_addresses, "data": data_values, "status": "success"})

        # Check data storage configuration
        data_storage_config = db.query(DataStorageConfig).filter(DataStorageConfig.machine_id == machine_id).first()
        if not data_storage_config:
            raise HTTPException(status_code=404, detail="Data storage configuration not found.")

        # Store raw data in the database if enabled
        if data_storage_config.store_in_database.lower() == 'true':

            await store_raw_data(
                machine_id=machine_id,
                readings=[
                    {
                        "function_code": modbus_tcp_config.function_code,
                        "start_address": result["register_addresses"][0] if "register_addresses" in result else None,
                        "data": result["data"] if "data" in result else [],
                        "status": result.get("status", "success"),
                        "message": result.get("message")
                    }
                    for result in all_results if "register_addresses" in result and result["register_addresses"]
                ],
                db=db
            )

        # Update OPC UA server if enabled
        opcua_url = None
        if data_storage_config.store_in_opcua_server.lower() == 'true':
            print("hi")
            try:
                server_manager = OPCUAServerManager(machine_id)
                server_manager.start_server()

                opcua_data = []
                for result in all_results:
                    if result.get("status") == "success":
                        opcua_data.append({
                            "function_code": modbus_tcp_config.function_code,
                            "register_addresses": result["register_addresses"],
                            "data": result["data"],
                            "status": "success"
                        })

                server_manager.update_nodes(db, opcua_data, data_type="raw")
                opcua_url = server_manager.endpoint

            except Exception as e:
                logger.error(f"Error updating OPC UA server: {str(e)}")
                raise HTTPException(status_code=500, detail=f"Error updating OPC UA server: {str(e)}")

        return {
            "status": "success",
            "message": "Read registers successfully" +
                      (" and stored" if data_storage_config.store_in_database.lower() == 'true' else ""),
            "data": formatted_results,
            "opcua_url": opcua_url
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading registers: {str(e)}")


# @app.post("/save-register-selections", response_model=dict)
# def save_register_selections(
#         selection: RegisterSelectionCreate,
#         db: Session = Depends(get_db)
# ):
#     try:
#         # Get existing selections for this machine
#         existing_selections = db.query(RegisterSelection).filter(
#             RegisterSelection.machine_id == selection.machine_id
#         ).all()
#
#         # Create a set of existing (machine_id, register_number) combinations
#         existing_registers = {(s.machine_id, s.register_number) for s in existing_selections}
#
#         # Add only new selections that don't already exist
#         for reg in selection.register_selections:
#             # Check if this register selection already exists
#             if (selection.machine_id, reg["register_number"]) not in existing_registers:
#                 # This is a new selection, add it to the database
#                 db_selection = RegisterSelection(
#                     machine_id=selection.machine_id,
#                     register_number=reg["register_number"],
#                     register_value=reg["register_value"],
#                     is_selected=reg["is_selected"]
#                 )
#                 db.add(db_selection)
#
#         db.commit()
#         return {"message": "New register selections saved successfully"}
#     except Exception as e:
#         db.rollback()
#         raise HTTPException(status_code=500, detail=f"Error saving register selections: {str(e)}")

@app.post("/save-register-selections", response_model=dict)
def save_register_selections(
        selection: RegisterSelectionCreate,
        db: Session = Depends(get_db)
):
    try:
        # Simply add all selections without checking for duplicates
        for reg in selection.register_selections:
            db_selection = RegisterSelection(
                machine_id=selection.machine_id,
                register_number=reg["register_number"],
                register_value=reg["register_value"],
                is_selected=reg["is_selected"]
                # If you have a timestamp field, you can add it here
                # timestamp=datetime.now()
            )
            db.add(db_selection)

        db.commit()
        return {"message": "Register selections saved successfully"}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error saving register selections: {str(e)}")
# '''''''''''below is the configuration storing opcua endpoints putendpoint post endpoint'''''


# Create OPC UA configuration
@app.post("/opcua/config", response_model=OPCUAResponse, status_code=status.HTTP_201_CREATED)
def create_opcua_config(opcua: OPCUACreate, db: Session = Depends(get_db)):
    # Get the latest machine record from machine_details table
    latest_machine = db.query(MachineDetails).order_by(MachineDetails.id.desc()).first()

    if not latest_machine:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No machines found in the database"
        )

    # Create OPC UA config with the latest machine_id
    db_opcua = OPCUA(
        machine_id=latest_machine.id,  # Use the latest machine_id
        server_url=opcua.server_url,
        security_policy=opcua.security_policy,
        security_mode=opcua.security_mode,
        username=opcua.username,
        password=opcua.password
    )

    db.add(db_opcua)
    db.commit()
    db.refresh(db_opcua)

    # Create response with machine name included
    response = OPCUAResponse(
        id=db_opcua.id,
        machine_id=latest_machine.id,
        machine_name=latest_machine.machine_name,
        machine_protocol=latest_machine.machine_protocol,
        server_url=db_opcua.server_url,
        security_policy=db_opcua.security_policy,
        security_mode=db_opcua.security_mode,
        username=db_opcua.username,
        password=db_opcua.password
    )

    return response

# Get all OPC UA configurations
@app.get("/", response_model=List[OPCUAResponse])
def get_all_opcua_configs(db: Session = Depends(get_db)):
    opcua_configs = db.query(OPCUA).all()

    response = []
    for config in opcua_configs:
        machine = db.query(MachineDetails).filter(MachineDetails.id == config.machine_id).first()
        response.append(OPCUAResponse(
            id=config.id,
            machine_id=config.machine_id,
            machine_name=machine.name if machine else "Unknown",
            server_url=config.server_url,
            security_policy=config.security_policy,
            security_mode=config.security_mode,
            username=config.username,
            password=config.password
        ))

    return response


# Get OPC UA configuration by ID
@app.get("/opcconfig/{machine_id}", response_model=OPCUAResponse)
def get_opcua_config_by_machine(machine_id: int, db: Session = Depends(get_db)):
    # First check if machine exists
    machine = db.query(MachineDetails).filter(MachineDetails.id == machine_id).first()
    if not machine:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Machine with id {machine_id} not found"
        )

    # Get the OPC UA configuration for this machine
    opcua_config = db.query(OPCUA).filter(OPCUA.machine_id == machine_id).first()
    if not opcua_config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"OPC UA configuration for machine id {machine_id} not found"
        )

    return OPCUAResponse(
        id=opcua_config.id,
        machine_id=opcua_config.machine_id,
        machine_name=machine.machine_name,
        machine_protocol=machine.machine_protocol,
        server_url=opcua_config.server_url,
        security_policy=opcua_config.security_policy,
        security_mode=opcua_config.security_mode,
        username=opcua_config.username,
        password=opcua_config.password
    )


# Update OPC UA configuration
@app.put("/opc_update/machine/{machine_id}", response_model=OPCUAResponse)
def update_opcua_config(machine_id: int, opcua: OPCUACreate, db: Session = Depends(get_db)):
    # First find the OPC UA config for this machine
    opcua_config = db.query(OPCUA).filter(OPCUA.machine_id == machine_id).first()
    if not opcua_config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"OPC UA configuration for machine id {machine_id} not found"
        )

    # Check if machine exists
    machine = db.query(MachineDetails).filter(MachineDetails.id == machine_id).first()
    if not machine:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Machine with id {machine_id} not found"
        )

    # Update fields
    opcua_config.server_url = opcua.server_url
    opcua_config.security_policy = opcua.security_policy
    opcua_config.security_mode = opcua.security_mode
    opcua_config.username = opcua.username
    opcua_config.password = opcua.password

    db.commit()
    db.refresh(opcua_config)

    return OPCUAResponse(
        id=opcua_config.id,
        machine_id=machine_id,
        machine_name=machine.machine_name,
        server_url=opcua_config.server_url,
        security_policy=opcua_config.security_policy,
        security_mode=opcua_config.security_mode,
        username=opcua_config.username,
        password=opcua_config.password
    )


@app.get("/connect-machine/{machine_id}/nodes")
async def get_machine_nodes(
        machine_id: int,
        db: Session = Depends(get_db),
        connection_manager: operations.UnifiedConnectionManager = Depends(get_connection_manager)
):
    """Get all nodes for a specific machine"""
    try:
        # Get machine details and connection
        machine = db.query(MachineDetails).filter(MachineDetails.id == machine_id).first()
        if not machine:
            raise HTTPException(status_code=404, detail="Machine not found")

        if machine.machine_protocol != "OPC UA":
            raise HTTPException(status_code=400, detail="Machine is not an OPC UA device")

        connection = connection_manager.get_connection(machine_id)
        if not connection:
            raise HTTPException(status_code=400, detail="Machine not connected")

        if not isinstance(connection, operations.OPCUAConnection):
            raise HTTPException(status_code=400, detail="Invalid connection type")

        client = connection.client
        start_time = time.time()
        node_cache = set()
        folder_cache = set()
        BATCH_SIZE = 50
        TIMEOUT = 30

        async def process_node_recursive(node, depth=0, max_depth=10):
            if depth > max_depth:
                return []

            try:
                results = []
                node_id = node.nodeid.to_string()

                if node_id in node_cache:
                    return results

                node_cache.add(node_id)

                try:
                    node_class = node.get_node_class()
                    browse_name = node.get_browse_name()
                    description = browse_name.Name
                except Exception:
                    return results

                # Add current node
                if node_class == ua.NodeClass.Object:
                    if node_id not in folder_cache:
                        folder_cache.add(node_id)
                        results.append({
                            "type": "folder",
                            "node_id": node_id,
                            "description": description
                        })
                elif node_class == ua.NodeClass.Variable:
                    results.append({
                        "type": "parameter",
                        "node_id": node_id,
                        "description": description
                    })

                # Process children
                try:
                    children = node.get_children()
                    for child in children:
                        child_results = await process_node_recursive(child, depth + 1, max_depth)
                        results.extend(child_results)
                except Exception as e:
                    logger.warning(f"Error getting children for node {node_id}: {e}")

                return results

            except Exception as e:
                logger.warning(f"Error processing node: {e}")
                return []

        async def process_batch(nodes):
            all_results = []
            for node in nodes:
                results = await process_node_recursive(node)
                all_results.extend(results)
            return all_results

        # Start from root node
        root_node = client.get_node("i=84")  # Objects node
        initial_children = root_node.get_children()

        # Process in batches
        all_nodes = []
        batches = [initial_children[i:i + BATCH_SIZE] for i in range(0, len(initial_children), BATCH_SIZE)]

        for batch in batches:
            if time.time() - start_time >= TIMEOUT:
                break
            results = await process_batch(batch)
            all_nodes.extend(results)

        # Organize results
        folders = []
        parameters = []
        for node in all_nodes:
            if node["type"] == "folder":
                folders.append({
                    "node_id": node["node_id"],
                    "description": node["description"]
                })
            else:
                parameters.append({
                    "node_id": node["node_id"],
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
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve nodes: {str(e)}"
        )


# latest_values = {}  # Use a dictionary to store the latest values for multiple nodes
# @app.post("/connect-machine/{machine_id}/nodes/monitor")
# async def monitor_node_values(
#         machine_id: int,
#         node_ids: List[str],
#         background_tasks: BackgroundTasks,
#         db: Session = Depends(get_db),
#         connection_manager: operations.UnifiedConnectionManager = Depends(get_connection_manager)
# ):
#     """Start continuous monitoring of specific node values for a machine"""
#     try:
#         # Get OPC UA configuration details
#         opcua_config = db.query(OPCUA).filter(OPCUA.machine_id == machine_id).first()
#         if not opcua_config:
#             raise HTTPException(status_code=404, detail="OPC UA configuration not found for this machine")
#
#         server_url = opcua_config.server_url  # Retrieve server URL from opcua_config
#
#         # Get machine details
#         machine = db.query(MachineDetails).filter(MachineDetails.id == machine_id).first()
#         if not machine:
#             raise HTTPException(status_code=404, detail="Machine not found")
#
#         # Check if the machine protocol is OPC UA
#         if machine.machine_protocol != "OPC UA":
#             raise HTTPException(status_code=400, detail="Machine is not an OPC UA device")
#
#         connection = connection_manager.get_connection(machine_id)
#         if not connection:
#             raise HTTPException(status_code=400, detail="Machine not connected")
#
#         if not isinstance(connection, operations.OPCUAConnection):
#             raise HTTPException(status_code=400, detail="Invalid connection type")
#
#         client = connection.client
#
#         # Verify all nodes exist
#         descriptions = {}
#         for node_id in node_ids:
#             node = client.get_node(node_id)
#             node_class = node.get_node_class()
#             if node_class != ua.NodeClass.Variable:
#                 raise HTTPException(status_code=400, detail=f"Node {node_id} is not a variable")
#             descriptions[node_id] = node.get_display_name().Text
#
#         # Start continuous monitoring in the background for all nodes
#         background_tasks.add_task(
#             continuous_monitor_multiple,
#             machine_id,
#             node_ids,
#             client,
#             descriptions,
#             db,  # Pass the database session
#             server_url  # Pass the server URL from opcua_config
#         )
#
#         return {
#             "status": "success",
#             "machine_id": machine_id,
#             "node_ids": node_ids,
#             "descriptions": descriptions,
#             "message": "Monitoring started"
#         }
#
#     except Exception as e:
#         logger.error(f"Error starting monitoring for nodes {node_ids} on machine {machine_id}: {e}")
#         raise HTTPException(status_code=500, detail=str(e))
#
# async def continuous_monitor_multiple(
#         machine_id: int,
#         node_ids: List[str],
#         client,
#         descriptions: dict,
#         db: Session,
#         server_url: str,
#         interval: int = 1  # Interval in seconds
# ):
#     """Continuously read and update the values of multiple nodes at specified intervals"""
#     global latest_values  # Use the global variable
#     while True:
#         try:
#             # Create a temporary dictionary to hold the latest values for this iteration
#             current_values = {}
#             for node_id in node_ids:
#                 node = client.get_node(node_id)
#                 value = node.get_value()
#
#                 # Update the latest value for each node
#                 current_values[node_id] = {
#                     "machine_id": machine_id,
#                     "node_id": node_id,
#                     "description": descriptions[node_id],
#                     "value": value
#                 }
#
#                 # Display the value in the console
#                 print(f"Machine ID: {machine_id}, Node ID: {node_id}, Description: {descriptions[node_id]}, Value: {value}")
#
#                 # Store the value in the database
#                 new_value = models.NodeValue(
#                     machine_id=machine_id,
#                     server_url=server_url,
#                     node_id=node_id,
#                     value=value
#                 )
#                 db.add(new_value)
#
#             # Commit the session to save all new values
#             db.commit()
#
#             # Update the global latest values with the current iteration values
#             latest_values = current_values
#
#             # Wait for the specified interval before the next reading
#             await asyncio.sleep(interval)
#
#         except Exception as e:
#             logger.error(f"Error reading nodes {node_ids} for machine {machine_id}: {e}")
#             db.rollback()  # Rollback the session on error
#             break  # Exit the loop on error
#
# @app.get("/latest-values")
# async def get_latest_values():
#     """Get the latest monitored values"""
#     if not latest_values:
#         raise HTTPException(status_code=404, detail="No values available")
#     return latest_values


# @app.post("/connect-machine/{machine_id}/nodes/monitor")
# async def monitor_node_values(
#         machine_id: int,
#         node_ids: List[str],
#         db: Session = Depends(get_db),
#         connection_manager: operations.UnifiedConnectionManager = Depends(get_connection_manager)
# ):
#     """Fetch specific node values for a machine directly"""
#     try:
#         # Get OPC UA configuration details
#         opcua_config = db.query(OPCUA).filter(OPCUA.machine_id == machine_id).first()
#         if not opcua_config:
#             raise HTTPException(status_code=404, detail="OPC UA configuration not found for this machine")
#
#         server_url = opcua_config.server_url
#
#         # Get machine details
#         machine = db.query(MachineDetails).filter(MachineDetails.id == machine_id).first()
#         if not machine:
#             raise HTTPException(status_code=404, detail="Machine not found")
#
#         # Check if the machine protocol is OPC UA
#         if machine.machine_protocol != "OPC UA":
#             raise HTTPException(status_code=400, detail="Machine is not an OPC UA device")
#
#         connection = connection_manager.get_connection(machine_id)
#         if not connection:
#             raise HTTPException(status_code=400, detail="Machine not connected")
#
#         if not isinstance(connection, operations.OPCUAConnection):
#             raise HTTPException(status_code=400, detail="Invalid connection type")
#
#         client = connection.client
#
#         # Fetch current values for all requested nodes
#         values = {}
#         for node_id in node_ids:
#             try:
#                 node = client.get_node(node_id)
#                 node_class = node.get_node_class()
#
#                 if node_class != ua.NodeClass.Variable:
#                     raise HTTPException(status_code=400, detail=f"Node {node_id} is not a variable")
#
#                 description = node.get_display_name().Text
#
#                 # Get data type and value with proper conversion
#                 dv = node.get_data_value()
#                 value = dv.Value.Value
#
#                 # Handle string values properly
#                 if isinstance(value, bytes):
#                     try:
#                         value = value.decode('utf-8').strip('\x00')
#                     except UnicodeDecodeError:
#                         value = str(value)
#
#                 # Get timestamps
#                 source_timestamp = dv.SourceTimestamp.isoformat() if dv.SourceTimestamp else None
#                 server_timestamp = dv.ServerTimestamp.isoformat() if dv.ServerTimestamp else None
#
#                 # Get status code
#                 status_code = "Good" if dv.StatusCode.is_good() else str(dv.StatusCode)
#
#                 # Get data type
#                 try:
#                     data_type = node.get_data_type_as_variant_type().name
#                 except Exception:
#                     data_type = "Unknown"
#
#                 # Store the node value in the database
#                 new_value = models.NodeValue(
#                     machine_id=machine_id,
#                     server_url=server_url,
#                     node_id=node_id,
#                     value=str(value)
#                 )
#                 db.add(new_value)
#
#                 # Add to response values
#                 values[node_id] = {
#                     "machine_id": machine_id,
#                     "node_id": node_id,
#                     "description": description,
#                     "value": value,
#                     "datatype": data_type,
#                     "source_timestamp": source_timestamp,
#                     "server_timestamp": server_timestamp,
#                     "status_code": status_code
#                 }
#
#                 # Log the value
#                 logger.info(f"Machine ID: {machine_id}, Node ID: {node_id}, Description: {description}, Value: {value}")
#
#             except Exception as e:
#                 logger.error(f"Error reading node {node_id}: {e}")
#                 values[node_id] = {
#                     "machine_id": machine_id,
#                     "node_id": node_id,
#                     "error": str(e)
#                 }
#
#         # Commit all database changes
#         db.commit()
#
#         import datetime
#
#         return {
#             "status": "success",
#             "machine_id": machine_id,
#             "timestamp": datetime.datetime.now().isoformat(),
#             "values": values
#         }

#
#     except Exception as e:
#         logger.error(f"Error fetching values for nodes {node_ids} on machine {machine_id}: {e}")
#         db.rollback()  # Rollback on error
#         raise HTTPException(status_code=500, detail=str(e))

@app.post("/connect-machine/{machine_id}/nodes/monitor")
async def monitor_node_values(
        machine_id: int,
        node_ids: List[str],
        db: Session = Depends(get_db),
        connection_manager: operations.UnifiedConnectionManager = Depends(get_connection_manager)
):
    """Fetch specific node values for a machine directly"""
    try:
        # Get OPC UA configuration details
        opcua_config = db.query(OPCUA).filter(OPCUA.machine_id == machine_id).first()
        if not opcua_config:
            raise HTTPException(status_code=404, detail="OPC UA configuration not found for this machine")

        server_url = opcua_config.server_url

        # Get machine details
        machine = db.query(MachineDetails).filter(MachineDetails.id == machine_id).first()
        if not machine:
            raise HTTPException(status_code=404, detail="Machine not found")

        # Check if the machine protocol is OPC UA
        if machine.machine_protocol != "OPC UA":
            raise HTTPException(status_code=400, detail="Machine is not an OPC UA device")

        connection = connection_manager.get_connection(machine_id)
        if not connection:
            raise HTTPException(status_code=400, detail="Machine not connected")

        if not isinstance(connection, operations.OPCUAConnection):
            raise HTTPException(status_code=400, detail="Invalid connection type")

        client = connection.client

        # Fetch current values for all requested nodes
        values = {}
        for node_id in node_ids:
            try:
                node = client.get_node(node_id)
                node_class = node.get_node_class()

                if node_class != ua.NodeClass.Variable:
                    raise HTTPException(status_code=400, detail=f"Node {node_id} is not a variable")

                description = node.get_display_name().Text
                value = node.get_value()

                # Store the node value in the database
                new_value = models.NodeValue(
                    machine_id=machine_id,
                    server_url=server_url,
                    node_id=node_id,
                    value=value
                )
                db.add(new_value)

                # Add to response values
                values[node_id] = {
                    "machine_id": machine_id,
                    "node_id": node_id,
                    "description": description,
                    "value": value
                }

                # Log the value
                logger.info(f"Machine ID: {machine_id}, Node ID: {node_id}, Description: {description}, Value: {value}")

            except Exception as e:
                logger.error(f"Error reading node {node_id}: {e}")
                values[node_id] = {
                    "machine_id": machine_id,
                    "node_id": node_id,
                    "error": str(e)
                }

        # Commit all database changes
        db.commit()

        import datetime

        return {
            "status": "success",
            "machine_id": machine_id,
            "timestamp": datetime.datetime.now().isoformat(),
            "values": values
        }

    except Exception as e:
        logger.error(f"Error fetching values for nodes {node_ids} on machine {machine_id}: {e}")
        db.rollback()  # Rollback on error
        raise HTTPException(status_code=500, detail=str(e))

# Delete OPC UA configuration
@app.delete("/{opcua_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_opcua_config(opcua_id: int, db: Session = Depends(get_db)):
    opcua_config = db.query(OPCUA).filter(OPCUA.id == opcua_id).first()
    if not opcua_config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"OPC UA configuration with id {opcua_id} not found"
        )

    db.delete(opcua_config)
    db.commit()

    return None



# '''''''''//Profinet protocols//'''''''''''''''

# @app.post("/config/profinet")
# def add_profinet_config(config: Union[
#     schemas.SiemensProfinetConfigSchema,
#     schemas.BeckhoffProfinetConfigSchema,
#     schemas.PhoenixProfinetConfigSchema,
#     schemas.ABBProfinetConfigSchema,
#     schemas.BRProfinetConfigSchema
# ], db: Session = Depends(get_db)):
#     try:
#         # Fetch the latest machine_id from the MachineDetails table
#         latest_machine = db.query(models.MachineDetails).order_by(models.MachineDetails.id.desc()).first()
#
#         # If no machine is found, raise an error
#         if not latest_machine:
#             raise HTTPException(status_code=404, detail="No machine found. Please add a machine first.")
#
#         # Handle Siemens Profinet configuration
#         if config.oem_type.lower() == "siemens":
#             config.port = config.port or 102  # Default to port 102 if not provided
#             new_config = models.SiemensProfinetConfig(
#                 machine_id=latest_machine.id,
#                 rack=config.rack,
#                 slot=config.slot,
#                 ip_address=config.ip_address,
#                 port=config.port
#             )
#
#         # Handle Beckhoff Profinet configuration
#         elif config.oem_type.lower() == "beckhoff":
#             new_config = models.BeckhoffProfinetConfig(
#                 machine_id=latest_machine.id,
#                 ams_net_id=config.ams_net_id,
#                 target_ads_port=config.target_ads_port,
#                 ip_address=config.ip_address,
#                 port=config.port
#             )
#
#         # Handle Phoenix Profinet configuration
#         elif config.oem_type.lower() == "phoenix":
#             new_config = models.PhoenixProfinetConfig(
#                 machine_id=latest_machine.id,
#                 device_name=config.device_name,
#                 vlan_id=config.vlan_id,
#                 ip_address=config.ip_address,
#                 port=config.port
#             )
#
#         # Handle ABB Profinet configuration
#         elif config.oem_type.lower() == "abb":
#             new_config = models.ABBProfinetConfig(
#                 machine_id=latest_machine.id,
#                 device_id=config.device_id,
#                 subnet_mask=config.subnet_mask,
#                 ip_address=config.ip_address,
#                 port=config.port
#             )
#
#         # Handle BR Profinet configuration
#         elif config.oem_type.lower() == "br":
#             new_config = models.BRProfinetConfig(
#                 machine_id=latest_machine.id,
#                 node_number=config.node_number,
#                 cycle_time=config.cycle_time,
#                 ip_address=config.ip_address,
#                 port=config.port
#             )
#
#         else:
#             raise HTTPException(status_code=400, detail="Unsupported OEM type")
#
#         # Add the configuration to the database
#         db.add(new_config)
#         db.commit()
#         db.refresh(new_config)
#         return {"message": f"{config.oem_type} PROFINET config added successfully", "config_id": new_config.id}
#
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))



@app.get("/profinet-config/{machine_id}", response_model=schemas.ProfinetConfigResponse)
async def get_profinet_config(machine_id: int, db: Session = Depends(get_db)):
    # Get machine details
    machine = db.query(MachineDetails).filter(MachineDetails.id == machine_id).first()
    if not machine:
        raise HTTPException(status_code=404, detail="Machine not found")

    # Initialize response dictionary
    response = {
        "machine_id": machine.id,
        "machine_name": machine.machine_name,
        "machine_protocol": machine.machine_protocol,
        "machine_photo": machine.machine_photo,
        "created_at": machine.created_at,
        "profinet_config": None
    }

    # Check all possible Profinet configurations
    siemens_config = db.query(models.SiemensProfinetConfig).filter(
        models.SiemensProfinetConfig.machine_id == machine_id).first()
    beckhoff_config = db.query(models.BeckhoffProfinetConfig).filter(
        models.BeckhoffProfinetConfig.machine_id == machine_id).first()
    phoenix_config = db.query(models.PhoenixProfinetConfig).filter(
        models.PhoenixProfinetConfig.machine_id == machine_id).first()
    abb_config = db.query(models.ABBProfinetConfig).filter(
        models.ABBProfinetConfig.machine_id == machine_id).first()
    br_config = db.query(models.BRProfinetConfig).filter(
        models.BRProfinetConfig.machine_id == machine_id).first()

    # Add the found configuration to the response
    if siemens_config:
        response["profinet_config"] = {
            "oem_type": "siemens",
            "id": siemens_config.id,
            "ip_address": siemens_config.ip_address,
            "port": siemens_config.port,
            "rack": siemens_config.rack,
            "slot": siemens_config.slot,
            "created_at": siemens_config.created_at
        }
    elif beckhoff_config:
        response["profinet_config"] = {
            "oem_type": "beckhoff",
            "id": beckhoff_config.id,
            "ip_address": beckhoff_config.ip_address,
            "port": beckhoff_config.port,
            "ams_net_id": beckhoff_config.ams_net_id,
            "target_ads_port": beckhoff_config.target_ads_port,
            "created_at": beckhoff_config.created_at
        }
    elif phoenix_config:
        response["profinet_config"] = {
            "oem_type": "phoenix",
            "id": phoenix_config.id,
            "ip_address": phoenix_config.ip_address,
            "port": phoenix_config.port,
            "device_name": phoenix_config.device_name,
            "vlan_id": phoenix_config.vlan_id,
            "created_at": phoenix_config.created_at
        }
    elif abb_config:
        response["profinet_config"] = {
            "oem_type": "abb",
            "id": abb_config.id,
            "ip_address": abb_config.ip_address,
            "port": abb_config.port,
            "device_id": abb_config.device_id,
            "subnet_mask": abb_config.subnet_mask,
            "created_at": abb_config.created_at
        }
    elif br_config:
        response["profinet_config"] = {
            "oem_type": "br",
            "id": br_config.id,
            "ip_address": br_config.ip_address,
            "port": br_config.port,
            "node_number": br_config.node_number,
            "cycle_time": br_config.cycle_time,
            "created_at": br_config.created_at
        }

    return response


@app.put("/profinet-config/update/{machine_id}")
async def update_profinet_config(
    machine_id: int,
    update_data: schemas.ProfinetUpdateRequest,
    db: Session = Depends(get_db)
):
    try:
        # Fetch the machine
        machine = db.query(MachineDetails).filter(MachineDetails.id == machine_id).first()
        if not machine:
            raise HTTPException(status_code=404, detail="Machine not found")

        oem_type = update_data.oem_type.lower()
        config_data = update_data.config

        # Update Siemens configuration
        if oem_type == "siemens":
            config = db.query(models.SiemensProfinetConfig).filter(
                models.SiemensProfinetConfig.machine_id == machine_id).first()
            if not config:
                raise HTTPException(status_code=404, detail="Siemens Profinet configuration not found")

            update_model = schemas.SiemensProfinetConfigUpdate(**config_data)
            if update_model.ip_address:
                config.ip_address = update_model.ip_address
            if update_model.port:
                config.port = update_model.port
            if update_model.rack is not None:
                config.rack = update_model.rack
            if update_model.slot is not None:
                config.slot = update_model.slot

        # Update Beckhoff configuration
        elif oem_type == "beckhoff":
            config = db.query(models.BeckhoffProfinetConfig).filter(
                models.BeckhoffProfinetConfig.machine_id == machine_id).first()
            if not config:
                raise HTTPException(status_code=404, detail="Beckhoff Profinet configuration not found")

            update_model = schemas.BeckhoffProfinetConfigUpdate(**config_data)
            if update_model.ip_address:
                config.ip_address = update_model.ip_address
            if update_model.port:
                config.port = update_model.port
            if update_model.ams_net_id:
                config.ams_net_id = update_model.ams_net_id
            if update_model.target_ads_port:
                config.target_ads_port = update_model.target_ads_port

        # Update Phoenix configuration
        elif oem_type == "phoenix":
            config = db.query(models.PhoenixProfinetConfig).filter(
                models.PhoenixProfinetConfig.machine_id == machine_id).first()
            if not config:
                raise HTTPException(status_code=404, detail="Phoenix Profinet configuration not found")

            update_model = schemas.PhoenixProfinetConfigUpdate(**config_data)
            if update_model.ip_address:
                config.ip_address = update_model.ip_address
            if update_model.port:
                config.port = update_model.port
            if update_model.device_name:
                config.device_name = update_model.device_name
            if update_model.vlan_id:
                config.vlan_id = update_model.vlan_id

        # Update ABB configuration
        elif oem_type == "abb":
            config = db.query(models.ABBProfinetConfig).filter(
                models.ABBProfinetConfig.machine_id == machine_id).first()
            if not config:
                raise HTTPException(status_code=404, detail="ABB Profinet configuration not found")

            update_model = schemas.ABBProfinetConfigUpdate(**config_data)
            if update_model.ip_address:
                config.ip_address = update_model.ip_address
            if update_model.port:
                config.port = update_model.port
            if update_model.device_id:
                config.device_id = update_model.device_id
            if update_model.subnet_mask:
                config.subnet_mask = update_model.subnet_mask

        # Update BR configuration
        elif oem_type == "br":
            config = db.query(models.BRProfinetConfig).filter(
                models.BRProfinetConfig.machine_id == machine_id).first()
            if not config:
                raise HTTPException(status_code=404, detail="BR Profinet configuration not found")

            update_model = schemas.BRProfinetConfigUpdate(**config_data)
            if update_model.ip_address:
                config.ip_address = update_model.ip_address
            if update_model.port:
                config.port = update_model.port
            if update_model.node_number:
                config.node_number = update_model.node_number
            if update_model.cycle_time:
                config.cycle_time = update_model.cycle_time

        else:
            raise HTTPException(status_code=400, detail="Unsupported OEM type")

        # Commit the updates
        db.commit()

        return {"message": f"{oem_type.capitalize()} Profinet configuration updated successfully"}

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



@app.post("/config/profinet")
def add_profinet_config(config: Union[
    schemas.SiemensProfinetConfigSchema,
    schemas.BeckhoffProfinetConfigSchema,
    schemas.PhoenixProfinetConfigSchema,
    schemas.ABBProfinetConfigSchema,
    schemas.BRProfinetConfigSchema
], db: Session = Depends(get_db)):
    try:
        # Fetch the latest machine_id from the MachineDetails table
        latest_machine = db.query(models.MachineDetails).order_by(models.MachineDetails.id.desc()).first()

        # If no machine is found, raise an error
        if not latest_machine:
            raise HTTPException(status_code=404, detail="No machine found. Please add a machine first.")

        # Handle Siemens Profinet configuration
        if config.oem_type.lower() == "siemens":
            config.port = config.port or 102  # Default to port 102 if not provided
            new_config = models.SiemensProfinetConfig(
                machine_id=latest_machine.id,
                rack=config.rack,
                slot=config.slot,
                ip_address=config.ip_address,
                port=config.port
            )

        # Handle Beckhoff Profinet configuration
        elif config.oem_type.lower() == "beckhoff":
            new_config = models.BeckhoffProfinetConfig(
                machine_id=latest_machine.id,
                ams_net_id=config.ams_net_id,
                target_ads_port=config.target_ads_port,
                ip_address=config.ip_address,
                port=config.port
            )

        # Handle Phoenix Profinet configuration
        elif config.oem_type.lower() == "phoenix":
            new_config = models.PhoenixProfinetConfig(
                machine_id=latest_machine.id,
                device_name=config.device_name,
                vlan_id=config.vlan_id,
                ip_address=config.ip_address,
                port=config.port
            )

        # Handle ABB Profinet configuration
        elif config.oem_type.lower() == "abb":
            new_config = models.ABBProfinetConfig(
                machine_id=latest_machine.id,
                device_id=config.device_id,
                subnet_mask=config.subnet_mask,
                ip_address=config.ip_address,
                port=config.port
            )

        # Handle BR Profinet configuration
        elif config.oem_type.lower() == "br":
            new_config = models.BRProfinetConfig(
                machine_id=latest_machine.id,
                node_number=config.node_number,
                cycle_time=config.cycle_time,
                ip_address=config.ip_address,
                port=config.port
            )

        else:
            raise HTTPException(status_code=400, detail="Unsupported OEM type")

        # Add the configuration to the database
        db.add(new_config)
        db.commit()
        db.refresh(new_config)

        # Create response with all configuration details
        response_data = {
            "message": f"{config.oem_type} PROFINET config added successfully",
            "config_id": new_config.id,
            "machine_id": latest_machine.id,
            "config_details": {
                "ip_address": new_config.ip_address,
                "port": new_config.port
            }
        }

        # Add OEM-specific details to the response
        if config.oem_type.lower() == "siemens":
            response_data["config_details"].update({
                "rack": new_config.rack,
                "slot": new_config.slot
            })
        elif config.oem_type.lower() == "beckhoff":
            response_data["config_details"].update({
                "ams_net_id": new_config.ams_net_id,
                "target_ads_port": new_config.target_ads_port
            })
        elif config.oem_type.lower() == "phoenix":
            response_data["config_details"].update({
                "device_name": new_config.device_name,
                "vlan_id": new_config.vlan_id
            })
        elif config.oem_type.lower() == "abb":
            response_data["config_details"].update({
                "device_id": new_config.device_id,
                "subnet_mask": new_config.subnet_mask
            })
        elif config.oem_type.lower() == "br":
            response_data["config_details"].update({
                "node_number": new_config.node_number,
                "cycle_time": new_config.cycle_time
            })

        return response_data

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/read-profinet-data")
async def read_profinet_data(
        config: schemas.ProfinetReadRequest,
        db: Session = Depends(get_db),
        connection_manager: operations.UnifiedConnectionManager = Depends(get_connection_manager)
):
    """Read raw data from Profinet PLC and conditionally store based on configuration."""

    # Retrieve machine details
    machine = db.query(MachineDetails).filter(MachineDetails.id == config.machine_id).first()
    if not machine:
        raise HTTPException(status_code=404, detail="Machine not found")

    if machine.machine_protocol.lower() != "profinet":
        raise HTTPException(status_code=400, detail="Machine is not configured for Profinet")

    # Check if machine connection is valid
    connection = connection_manager.get_connection(config.machine_id)
    if not connection or not connection.is_connection_valid():
        raise HTTPException(status_code=500, detail="Connection to machine is not valid")

    try:
        # Get OEM-specific configuration
        configs = {
            "siemens": (models.SiemensProfinetConfig, "siemens"),
            "beckhoff": (models.BeckhoffProfinetConfig, "beckhoff"),
            "phoenix": (models.PhoenixProfinetConfig, "phoenix"),
            "abb": (models.ABBProfinetConfig, "abb"),
            "br": (models.BRProfinetConfig, "br")
        }

        plc_config = None
        oem_type = None

        for oem, (model_class, oem_name) in configs.items():
            config_instance = db.query(model_class).filter(
                model_class.machine_id == config.machine_id
            ).first()
            if config_instance:
                plc_config = config_instance
                oem_type = oem_name
                break

        if not plc_config:
            raise HTTPException(status_code=404, detail="Profinet configuration not found")

        all_results = []
        formatted_results = []

        # Read data for each field
        for field in config.fields:
            try:
                result = None
                if oem_type == "siemens":
                    result = operations.read_and_interpret_data(connection.client, field)
                elif oem_type == "beckhoff":
                    result = connection.client.read_by_name(
                        field.data_source,
                        field.offset,
                        field.data_type
                    )
                elif oem_type == "phoenix":
                    result = connection.client.read_variable(
                        field.data_source,
                        field.offset,
                        field.data_type
                    )
                elif oem_type == "abb":
                    result = connection.client.read_tag(
                        field.data_source,
                        field.offset,
                        field.data_type
                    )
                elif oem_type == "br":
                    result = connection.client.read_variable(
                        field.data_source,
                        field.offset,
                        field.data_type
                    )

                formatted_results.append({
                    "source": field.data_source,
                    "offset": field.offset,
                    "type": field.data_type,
                    "value": result
                })

                all_results.append({
                    "field": field.dict(),
                    "value": result,
                    "status": "success"
                })

            except Exception as e:
                logger.error(f"Error reading field {field.dict()}: {str(e)}")
                all_results.append({
                    "field": field.dict(),
                    "status": "error",
                    "message": str(e)
                })

        # Get data storage configuration
        data_storage_config = db.query(DataStorageConfig).filter(
            DataStorageConfig.machine_id == config.machine_id
        ).first()

        if not data_storage_config:
            raise HTTPException(status_code=404, detail="Data storage configuration not found")

        # Store data if configured
        if data_storage_config.store_in_database.lower() == 'true':
            await store_profinet_data(
                machine_id=config.machine_id,
                oem_type=oem_type,
                readings=all_results,
                db=db
            )

        # Handle OPC UA server if enabled
        opcua_url = None
        if data_storage_config.store_in_opcua_server.lower() == 'true':
            try:
                server_manager = OPCUAServerManager(config.machine_id)
                server_manager.start_server()

                # Format data for OPC UA
                opcua_data = [
                    {
                        "node_id": f"{result['field']['data_source']}.{result['field']['offset']}",
                        "value": result["value"],
                        "data_type": result["field"]["data_type"]
                    }
                    for result in all_results
                    if result["status"] == "success"
                ]

                server_manager.update_nodes(db, opcua_data, data_type="profinet")
                opcua_url = server_manager.endpoint

            except Exception as e:
                logger.error(f"Error updating OPC UA server: {str(e)}")

        return schemas.ProfinetReadResponse(
            status="success",
            message="Read data successfully" +
                    (" and stored" if data_storage_config.store_in_database.lower() == 'true' else ""),
            machine_id=config.machine_id,
            oem_type=oem_type,
            data=formatted_results,
            raw_results=all_results,
            opcua_url=opcua_url
        )

    except Exception as e:
        logger.error(f"Error reading Profinet data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error reading Profinet data: {str(e)}")


async def store_profinet_data(machine_id: int, oem_type: str, readings: List[dict], db: Session):
    """Store Profinet readings in the database"""
    try:
        timestamp = datetime.now()

        for reading in readings:
            db_reading = models.ProfinetRawData(
                machine_id=machine_id,
                oem_type=oem_type,
                data_source=reading["field"]["data_source"],
                offset=reading["field"]["offset"],
                data_type=reading["field"]["data_type"],
                value=str(reading.get("value")),
                status=reading["status"],
                error_message=reading.get("message"),
                timestamp=timestamp
            )
            db.add(db_reading)

        db.commit()
        return True
    except Exception as e:
        db.rollback()
        logger.error(f"Error storing Profinet data: {str(e)}")
        raise




# ''''''''''below endpoint is for MQTT configurations''''''''''''
@app.post("/mqtt-config/", response_model=schemas.MqttConfigurationResponse)
def create_mqtt_config(
    mqtt_config: schemas.MqttConfigurationCreate,
    db: Session = Depends(get_db)
):
    """
    Create MQTT Configuration for the latest machine
    - Automatically uses the latest machine ID from machine_details
    - Returns configuration with machine details
    """
    return operations.create_mqtt_configuration(db, mqtt_config)

@app.get("/mqtt-config/{machine_id}", response_model=schemas.MqttConfigurationResponse)
def get_mqtt_config(
    machine_id: int,
    db: Session = Depends(get_db)
):
    """
    Get MQTT Configuration for a specific machine
    - Requires machine ID
    - Returns latest configuration for that machine
    """
    return operations.get_mqtt_configuration(db, machine_id)

@app.put("/mqtt-config/{machine_id}", response_model=schemas.MqttConfigurationResponse)
def update_mqtt_config(
    machine_id: int,
    mqtt_config: schemas.MqttConfigurationUpdate,
    db: Session = Depends(get_db)
):
    """
    Update MQTT Configuration for a specific machine
    - Requires machine ID
    - Allows partial updates to configuration
    """
    return operations.update_mqtt_configuration(db, machine_id, mqtt_config)




# '''''''


@app.get("/mqtt-topics/{machine_id}")
def get_mqtt_topics(
        machine_id: int,
        db: Session = Depends(get_db)
):
    """
    Get all available topics from the MQTT broker for a specific machine
    - Requires machine ID
    - Returns list of available topics from the broker
    """
    # Get the MQTT configuration for the machine
    mqtt_config_response = operations.get_mqtt_configuration(db, machine_id)
    if not mqtt_config_response:
        raise HTTPException(status_code=404, detail="MQTT configuration not found for this machine")

    # Debug logging
    logger.info(f"MQTT Config Response Type: {type(mqtt_config_response)}")
    logger.info(f"MQTT Config Response Content: {mqtt_config_response}")

    # Connect to the MQTT broker
    try:
        # Check if it's a dictionary and access accordingly
        if isinstance(mqtt_config_response, dict):
            broker = mqtt_config_response.get("broker")
            port = mqtt_config_response.get("port")
            username = mqtt_config_response.get("username")
            password = mqtt_config_response.get("password")
        else:
            # Try accessing as object with attributes
            broker = mqtt_config_response.broker
            port = mqtt_config_response.port
            username = mqtt_config_response.username
            password = mqtt_config_response.password

        connection = ops.MQTTConnection(
            broker=broker,
            port=port,
            username=username,
            password=password
        )
        connection.connect()

        # Use a function to discover topics
        topics = _discover_mqtt_topics(connection)

        # Clean up
        connection.disconnect()

        return {"machine_id": machine_id, "topics": topics}
    except ConnectionError as e:
        raise HTTPException(status_code=503, detail=f"Failed to connect to MQTT broker: {str(e)}")
    except Exception as e:
        logger.error(f"Error fetching MQTT topics: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error fetching MQTT topics: {str(e)}")


@app.post("/mqtt-subscribe/")
def subscribe_mqtt_topic(
        subscription: schemas.MqttSubscription,
        db: Session = Depends(get_db)
):
    """
    Subscribe to an MQTT topic and get the latest data
    - Requires machine ID and topic name
    - Returns the latest data from the subscribed topic
    """
    # Get the MQTT configuration for the machine
    mqtt_config = operations.get_mqtt_configuration(db, subscription.machine_id)
    if not mqtt_config:
        raise HTTPException(status_code=404, detail="MQTT configuration not found for this machine")

    try:
        # Access configuration regardless of whether it's a dict or object
        if isinstance(mqtt_config, dict):
            broker = mqtt_config["broker"]
            port = mqtt_config["port"]
            username = mqtt_config.get("username")
            password = mqtt_config.get("password")
        else:
            # If it's an object with attributes (ORM model or Pydantic model)
            broker = mqtt_config.broker
            port = mqtt_config.port
            username = getattr(mqtt_config, "username", None)
            password = getattr(mqtt_config, "password", None)

        # Get data from the subscribed topic
        data = _get_mqtt_topic_data(
            broker=broker,
            port=port,
            username=username,
            password=password,
            topic=subscription.topic,
            timeout=subscription.timeout if hasattr(subscription, 'timeout') else 5
        )

        return {"machine_id": subscription.machine_id, "topic": subscription.topic, "data": data}
    except ConnectionError as e:
        raise HTTPException(status_code=503, detail=f"Failed to connect to MQTT broker: {str(e)}")
    except TimeoutError:
        raise HTTPException(status_code=504, detail="Timeout waiting for MQTT message")
    except Exception as e:
        logger.error(f"Error reading from MQTT topic: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error reading from MQTT topic: {str(e)}")

# Helper functions for MQTT operations
def _discover_mqtt_topics(connection: ops.MQTTConnection) -> list:
    """
    Discover available topics from an MQTT broker.
    This implementation uses a common technique of subscribing to '#' wildcard
    and collecting topics for a short period.
    """
    import paho.mqtt.client as mqtt
    import time

    discovered_topics = set()

    def on_message(client, userdata, msg):
        discovered_topics.add(msg.topic)

    # Configure the client for topic discovery
    connection.client.on_message = on_message
    connection.client.subscribe('#')  # Subscribe to all topics

    # Wait for some time to collect topics
    timeout = 3  # seconds
    start_time = time.time()
    while time.time() - start_time < timeout:
        time.sleep(0.1)

    # Unsubscribe from wildcard
    connection.client.unsubscribe('#')

    return list(discovered_topics)


def _get_mqtt_topic_data(broker: str, port: int, username: str, password: str, topic: str, timeout: int = 5):
    """
    Get the latest data from a specific MQTT topic.
    Subscribes to the topic and waits for a message until timeout.
    """
    import paho.mqtt.client as mqtt
    import threading
    import time

    result = {"message": None, "received": False}

    # Create a client
    client_id = f"reader_{uuid.uuid4().hex[:8]}"
    client = mqtt.Client(client_id=client_id)

    # Set authentication if provided
    if username and password:
        client.username_pw_set(username, password)

    # Define callbacks
    def on_message(client, userdata, msg):
        try:
            # Try to parse JSON
            import json
            result["message"] = json.loads(msg.payload.decode())
        except:
            # If not JSON, store as string
            result["message"] = msg.payload.decode()
        result["received"] = True

    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            client.subscribe(topic)
        else:
            raise ConnectionError(f"Failed to connect to MQTT broker, return code: {rc}")

    # Set callbacks
    client.on_message = on_message
    client.on_connect = on_connect

    # Connect and start loop
    client.connect(broker, port)
    client.loop_start()

    # Wait for message or timeout
    start_time = time.time()
    while not result["received"] and time.time() - start_time < timeout:
        time.sleep(0.1)

    # Clean up
    client.loop_stop()
    client.disconnect()

    if not result["received"]:
        raise TimeoutError(f"No message received on topic {topic} within {timeout} seconds")

    return result["message"]
# '''''''''this below is tor retrive the modbus rtu data from the database'''''''''''

@app.get("/modbus/raw-data/{machine_id}", response_model=List[ModbusRawDataResponse])
def get_modbus_raw_data_by_machine(
        machine_id: int,
        db: Session = Depends(get_db)
):
    """
    Fetch all ModbusRawData entries for a specific machine ID.
    """
    raw_data = db.query(ModbusRawData).filter(
        ModbusRawData.machine_id == machine_id
    ).all()

    if not raw_data:
        raise HTTPException(status_code=404, detail=f"No raw data found for machine ID: {machine_id}")

    return raw_data


@app.get("/modbus-data/{machine_id}", response_model=List[ModbusActualDataResponse])
def get_modbus_data_by_machine(
    machine_id: int,
    db: Session = Depends(get_db)
):
    """
    Fetch all modbus actual data for a specific machine by machine_id.
    """
    # Query all data for the specified machine
    data = db.query(ModbusActualData).filter(
        ModbusActualData.machine_id == machine_id
    ).order_by(desc(ModbusActualData.timestamp)).all()

    if not data:
        raise HTTPException(status_code=404, detail=f"No data found for machine ID: {machine_id}")

    return data

# '''''''''''''''coapp endpoints''''''''''''''''''''''
async def main():
    root = resource.Site()
    root.add_resource(('.well-known', 'core'), resource.WKCResource(root.get_resources_as_linkheader))

    # Register each individual sensor endpoint
    root.add_resource(('sensor', 'temperature'), SensorResource('temperature'))
    root.add_resource(('sensor', 'humidity'), SensorResource('humidity'))
    root.add_resource(('sensor', 'pressure'), SensorResource('pressure'))


    asyncio.create_task(generate_sensor_data())

    print("🚀 CoAP Server Running on 192.168.56.1:5683...")
    await Context.create_server_context(root, bind=("192.168.56.1", 5683))

    await asyncio.sleep(1000000)

@app.post("/coap-configuration", response_model=CoAPPConfigResponse, status_code=status.HTTP_201_CREATED)
def create_coapp_config(config: CoAPPConfigCreate, db: Session = Depends(get_db)):
    # Get the latest machine from machine_details
    latest_machine = db.query(MachineDetails).order_by(desc(MachineDetails.id)).first()

    if not latest_machine:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No machines found in the database. Please create a machine first."
        )

    # Create new CoAPP configuration using the latest machine's ID
    db_config = CoAPPConfigurations(
        machine_id=latest_machine.id,
        ip_address=config.ip_address,
        port=config.port
    )

    db.add(db_config)
    db.commit()
    db.refresh(db_config)

    # Create a response object that includes machine name
    response = CoAPPConfigResponse(
        id=db_config.id,
        machine_id=db_config.machine_id,
        machine_name=latest_machine.machine_name,  # Using machine_name as you specified
        machine_protocol=latest_machine.machine_protocol,
        ip_address=db_config.ip_address,
        port=db_config.port,
        created_at=db_config.created_at
    )

    return response

@app.get("/configuration/{machine_id}", response_model=CoAPPConfigResponse)
def get_coapp_config(machine_id: int, db: Session = Depends(get_db)):
    config = db.query(CoAPPConfigurations).filter(CoAPPConfigurations.machine_id == machine_id).first()

    if not config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"CoAPP configuration for machine ID {machine_id} not found"
        )

    # Access machine name through the relationship
    return {
        **config.__dict__,
        'machine_name': config.machine.machine_name,  # Assuming the MachineDetails model has a 'name' column
        'machine_protocol': config.machine.machine_protocol  # Add machine protocol
    }

@app.put("/configuration/machine/{machine_id}", response_model=CoAPPConfigResponse)
def update_coapp_config_by_machine(
        machine_id: str,
        config_update: CoAPPConfigCreate,
        db: Session = Depends(get_db)
):
    # Find the CoAPP configuration for the given machine ID
    db_config = db.query(CoAPPConfigurations).filter(CoAPPConfigurations.machine_id == machine_id).first()

    if not db_config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"CoAPP configuration for machine ID {machine_id} not found"
        )

    # Find the corresponding machine details to get the machine name
    machine = db.query(MachineDetails).filter(MachineDetails.id == machine_id).first()

    if not machine:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Machine with ID {machine_id} not found"
        )

    # Update IP address and port
    db_config.ip_address = config_update.ip_address
    db_config.port = config_update.port

    db.commit()
    db.refresh(db_config)

    # Create a response object that includes machine name
    response = CoAPPConfigResponse(
        id=db_config.id,
        machine_id=db_config.machine_id,
        machine_name=machine.machine_name,  # Get machine name from MachineDetails
        machine_protocol=machine.machine_protocol,  # Add machine protocol
        ip_address=db_config.ip_address,
        port=db_config.port,
        created_at=db_config.created_at
    )

    return response

# Delete CoAPP configuration (keep this as is)
@app.delete("/configuration/{config_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_coapp_config(config_id: int, db: Session = Depends(get_db)):
    db_config = db.query(CoAPPConfigurations).filter(CoAPPConfigurations.id == config_id).first()
    if not db_config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"CoAPP configuration with ID {config_id} not found"
        )

    db.delete(db_config)
    db.commit()
    return None

# @app.get("/machine/{machine_id}/sensor/{sensor_type}")
# async def read_sensor_data(
#         machine_id: int,
#         sensor_type: str,
#         db: Session = Depends(get_db),
#         connection_manager: operations.UnifiedConnectionManager = Depends(get_connection_manager)
# ):
#     # Validate machine exists
#     machine = db.query(MachineDetails).filter(MachineDetails.id == machine_id).first()
#     if not machine:
#         raise HTTPException(status_code=404, detail="Machine not found")
#
#     # Check if machine protocol is CoAP
#     if machine.machine_protocol != "CoAP":
#         raise HTTPException(status_code=400, detail="Machine is not a CoAP device")
#
#     # Get connection
#     connection = connection_manager.get_connection(machine_id)
#     if not connection:
#         raise HTTPException(status_code=503, detail="Could not establish connection")
#
#     # Read sensor data
#     try:
#         # Use asyncio to run the async method
#         data = await connection.read_sensor_data(sensor_type)
#         if data is None:
#             raise HTTPException(status_code=500, detail=f"Failed to read {sensor_type} sensor")
#
#         return {
#             "machine_id": machine_id,
#             "sensor_type": sensor_type,
#             "value": data,
#             "timestamp": datetime.utcnow()
#         }
#     except Exception as e:
#         logger.error(f"Error reading sensor data: {e}")
#         raise HTTPException(status_code=500, detail=f"Error reading {sensor_type} sensor")

class CoAPConnectionManager:
    def __init__(self):
        self.connections = {}

    async def read_sensor_data(self, ip: str, port: int, sensor_type: str) -> str:
        protocol = await Context.create_client_context()
        await asyncio.sleep(0.1)  # Small delay to avoid race conditions
        uri = f"coap://{ip}:{port}/sensor/{sensor_type}"
        logger.info(f"🔍 Requesting sensor data from: {uri}")
        try:
            request = aio_pika.Message(code=GET, uri=uri)
            response = await protocol.request(request).response
            return response.payload.decode('utf-8')
        except Exception as e:
            logger.error(f"❌ Failed to fetch sensor data from {uri}: {e}")
            raise HTTPException(status_code=500, detail=str(e))
        
@app.get("/machine/{machine_id}/sensors")
async def read_all_sensor_data(machine_id: int, db: Session = Depends(get_db)):
    machine = await get_machine_by_id(machine_id, db)
    coap_query = db.query(CoAPPConfigurations).filter(CoAPPConfigurations.machine_id == machine_id)
    coap_config = coap_query.first()


    if not coap_config:
        raise HTTPException(status_code=400, detail="CoAP configuration not found for this machine")
        ip_address = coap_config["192.168.56.1"]
        port = coap_config["5683"]

    ip = coap_config.ip_address
    port = coap_config.port
    sensor_types = ["temperature", "humidity", "pressure"]
    sensor_data = {}

    for sensor_type in sensor_types:
        try:
            value = await coap_manager.read_sensor_data(ip, port, sensor_type)
            sensor_data[sensor_type] = value
        except Exception as e:
            logger.error(f"Failed to read {sensor_type}: {e}")
            sensor_data[sensor_type] = "error"

    return {
        "machine_id": machine_id,
        "protocol": "CoAPP",
        "ip": ip,
        "port": port,
        "sensor_data": sensor_data,
        "message": "Sensor data retrieved successfully"
    }


# @app.post("/read-coapp/{machine_id}/sensors/monitor")
# async def monitor_coap_sensors(
#         machine_id: int,
#         sensor_types: List[str] = Body(...),
#         db: Session = Depends(get_db),
#         connection_manager: operations.UnifiedConnectionManager = Depends(get_connection_manager)
# ):
#     """
#     Fetch sensor values for a machine from a CoAP server
#
#     :param machine_id: Unique identifier for the machine
#     :param sensor_types: List of sensor types to fetch (e.g., ['temperature', 'humidity', 'pressure'])
#     :return: Dictionary of sensor readings
#     """
#     try:
#         # Get machine details from database
#         machine = db.query(MachineDetails).filter(MachineDetails.id == machine_id).first()
#         if not machine:
#             raise HTTPException(status_code=404, detail="Machine not found")
#
#         # Log machine protocol for debugging
#         logger.info(f"Machine Protocol: {machine.machine_protocol}")
#
#         # Check if the machine protocol is CoAP (case-insensitive)
#         if machine.machine_protocol.lower() not in ["coapp", "coap"]:
#             raise HTTPException(
#                 status_code=400,
#                 detail=f"Machine is not a CoAP device. Current protocol: {machine.machine_protocol}"
#             )
#
#         # Get CoAP configuration
#         coap_config = (
#             db.query(CoAPPConfiguration)
#             .filter(CoAPPConfiguration.machine_id == machine_id)
#             .first()
#         )
#         if not coap_config:
#             raise HTTPException(
#                 status_code=404,
#                 detail=f"CoAP configuration not found for machine {machine_id}"
#             )
#
#         # Detailed logging of CoAP configuration
#         logger.info(
#             f"CoAP Configuration Details: "
#             f"Machine ID: {machine_id}, "
#             f"IP Address: {coap_config.ip_address}, "
#             f"Port: {coap_config.port}"
#         )
#
#         # Attempt to get or create a CoAP connection
#         try:
#             # Attempt to get existing connection
#             connection = connection_manager.get_connection(machine_id)
#
#             # If no existing connection, create a new one
#             if not connection:
#                 connection = connection_manager.create_connection(
#                     machine_id,
#                     protocol="coapp",
#                     ip_address=coap_config.ip_address,
#                     port=coap_config.port
#                 )
#                 connection_manager.add_connection(machine_id, connection)
#         except Exception as conn_error:
#             logger.error(
#                 f"Connection establishment error for machine {machine_id}: {conn_error}",
#                 exc_info=True
#             )
#             raise HTTPException(
#                 status_code=500,
#                 detail=f"Could not establish CoAP connection: {str(conn_error)}"
#             )
#
#         # Fetch sensor data with enhanced error tracking
#         sensor_values = {}
#         for sensor_type in sensor_types:
#             try:
#                 # Extended logging for sensor reading attempt
#                 logger.info(f"Attempting to read sensor: {sensor_type}")
#
#                 # Attempt to read sensor data
#                 value = connection.read_sensor_data(sensor_type)
#
#                 if value is not None:
#                     # Prepare sensor value for response and database
#                     current_timestamp = datetime.utcnow()
#
#                     # Store sensor value in database
#                     new_sensor_value = models.SensorValue(
#                         machine_id=machine_id,
#                         sensor_type=sensor_type,
#                         value=value,
#                         timestamp=current_timestamp
#                     )
#                     db.add(new_sensor_value)
#
#                     # Prepare sensor data response
#                     sensor_values[sensor_type] = {
#                         "value": value,
#                         "timestamp": current_timestamp.isoformat()
#                     }
#
#                     logger.info(f"Successfully read {sensor_type} sensor: {value}")
#                 else:
#                     # Handle case where no value could be read
#                     logger.warning(f"No value retrieved for {sensor_type} sensor")
#                     sensor_values[sensor_type] = {
#                         "error": f"Could not read {sensor_type} sensor",
#                         "details": "No value returned from sensor reading attempt"
#                     }
#             except Exception as sensor_error:
#                 # Comprehensive error logging for each sensor
#                 logger.error(
#                     f"Detailed error reading {sensor_type} sensor: {sensor_error}",
#                     exc_info=True
#                 )
#                 sensor_values[sensor_type] = {
#                     "error": "Sensor reading failed",
#                     "details": str(sensor_error)
#                 }
#
#         # Commit database changes
#         db.commit()
#
#         # Return response with sensor values
#         return {
#             "status": "success",
#             "machine_id": machine_id,
#             "sensors": sensor_values
#         }
#
#     except HTTPException as http_error:
#         # Re-raise HTTP exceptions directly
#         raise http_error
#     except Exception as unexpected_error:
#         # Handle any unexpected errors
#         logger.error(
#             f"Unexpected error fetching sensor data for machine {machine_id}: {unexpected_error}",
#             exc_info=True
#         )
#         db.rollback()
#         raise HTTPException(
#             status_code=500,
#             detail=f"Unexpected error: {str(unexpected_error)}"
#         )

@app.post("/read-coapp/{machine_id}/sensors/monitor")
async def monitor_coap_sensors(
        machine_id: int,
        sensor_types: List[str] = Body(...),
        db: Session = Depends(get_db),
        connection_manager: operations.UnifiedConnectionManager = Depends(get_connection_manager)
):
    """
    Fetch sensor values for a machine from a CoAP server asynchronously

    :param machine_id: Unique identifier for the machine
    :param sensor_types: List of sensor types to fetch (e.g., ['temperature', 'humidity', 'pressure'])
    :return: Dictionary of sensor readings
    """
    try:
        # Get machine details from database
        machine = db.query(MachineDetails).filter(MachineDetails.id == machine_id).first()
        if not machine:
            raise HTTPException(status_code=404, detail="Machine not found")

        # Log machine protocol for debugging
        logger.info(f"Machine Protocol: {machine.machine_protocol}")

        # Check if the machine protocol is CoAP (case-insensitive)
        if machine.machine_protocol.lower() not in ["coapp", "coap"]:
            raise HTTPException(
                status_code=400,
                detail=f"Machine is not a CoAP device. Current protocol: {machine.machine_protocol}"
            )

        # Get CoAP configuration
        coap_config = (
            db.query(CoAPPConfigurations)
            .filter(CoAPPConfigurations.machine_id == machine_id)
            .first()
        )
        if not coap_config:
            raise HTTPException(
                status_code=404,
                detail=f"CoAP configuration not found for machine {machine_id}"
            )

        # Detailed logging of CoAP configuration
        logger.info(
            f"CoAP Configuration Details: "
            f"Machine ID: {machine_id}, "
            f"IP Address: {coap_config.ip_address}, "
            f"Port: {coap_config.port}"
        )

        # Attempt to get or create a CoAP connection
        try:
            # Attempt to get existing connection
            connection = connection_manager.get_connection(machine_id)

            # If no existing connection, create a new one
            if not connection:
                connection = connection_manager.create_connection(
                    machine_id,
                    protocol="coapp",
                    ip_address=coap_config.ip_address,
                    port=coap_config.port
                )
                connection_manager.add_connection(machine_id, connection)
        except Exception as conn_error:
            logger.error(
                f"Connection establishment error for machine {machine_id}: {conn_error}",
                exc_info=True
            )
            raise HTTPException(
                status_code=500,
                detail=f"Could not establish CoAP connection: {str(conn_error)}"
            )

        # Fetch sensor data with enhanced error tracking
        sensor_values = {}
        for sensor_type in sensor_types:
            try:
                # Extended logging for sensor reading attempt
                logger.info(f"Attempting to read sensor: {sensor_type}")

                # Attempt to read sensor data ASYNC
                value = await connection.read_sensor_data_async(sensor_type)

                if value is not None:
                    # Prepare sensor value for response and database
                    current_timestamp = datetime.utcnow()

                    # Store sensor value in database
                    new_sensor_value = models.SensorValue(
                        machine_id=machine_id,
                        sensor_type=sensor_type,
                        value=value,
                        timestamp=current_timestamp
                    )
                    db.add(new_sensor_value)

                    # Prepare sensor data response
                    sensor_values[sensor_type] = {
                        "value": value,
                        "timestamp": current_timestamp.isoformat()
                    }

                    logger.info(f"Successfully read {sensor_type} sensor: {value}")
                else:
                    # Handle case where no value could be read
                    logger.warning(f"No value retrieved for {sensor_type} sensor")
                    sensor_values[sensor_type] = {
                        "error": f"Could not read {sensor_type} sensor",
                        "details": "No value returned from sensor reading attempt"
                    }
            except Exception as sensor_error:
                # Comprehensive error logging for each sensor
                logger.error(
                    f"Detailed error reading {sensor_type} sensor: {sensor_error}",
                    exc_info=True
                )
                sensor_values[sensor_type] = {
                    "error": "Sensor reading failed",
                    "details": str(sensor_error)
                }

        # Commit database changes
        db.commit()

        # Return response with sensor values
        return {
            "status": "success",
            "machine_id": machine_id,
            "sensors": sensor_values
        }

    except HTTPException as http_error:
        # Re-raise HTTP exceptions directly
        raise http_error
    except Exception as unexpected_error:
        # Handle any unexpected errors
        logger.error(
            f"Unexpected error fetching sensor data for machine {machine_id}: {unexpected_error}",
            exc_info=True
        )
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected error: {str(unexpected_error)}"
        )
    

# Endpoint 1: Connection check
@app.get("/amqp/connect")
async def check_amqp_connection():
    try:
        connection = await aio_pika.connect_robust("amqp://guest:guest@localhost/")
        await connection.close()
        return {"status": "Connected to AMQP server"}
    except Exception as e:
        return {"status": "Connection failed", "error": str(e)}

# Endpoint 2: Receive one message and store it
@app.post("/amqp/store")
async def receive_and_store_amqp_message():
    try:
        connection = await aio_pika.connect_robust("amqp://guest:guest@localhost/")
        channel = await connection.channel()
        queue = await channel.declare_queue("sensor_data", durable=True)

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    msg = message.body.decode()

                    db: Session = SessionLocal()
                    log = AMQPLog(message=msg, timestamp=datetime.utcnow()) # type: ignore
                    db.add(log)
                    db.commit()
                    db.close()

                    await connection.close()
                    return {"status": "Message stored", "message": msg}
    except Exception as e:
        return {"status": "Error", "error": str(e)}

# Endpoint 3: Get latest stored AMQP message
@app.get("/amqp/get-latest")
def get_latest_amqp_message():
    db: Session = SessionLocal()
    latest = db.query(AMQPLog).order_by(AMQPLog.timestamp.desc()).first() # type: ignore
    db.close()

    if latest:
        return {"latest_message": latest.message}
    else:
        return {"message": "No AMQP data found"}
    
@app.post("/amqp-config/", response_model=AmqpConfigurationResponse)
def create_amqp_config(config: AmqpConfigurationCreate, db: Session = Depends(get_db)):
    db_config = AmqpConfiguration(**config.dict())
    db.add(db_config)
    db.commit()
    db.refresh(db_config)
    return AmqpConfigurationResponse.model_validate(db_config, from_attributes=True)

@app.get("/amqp-config/{machine_id}", response_model=schemas.AmqpConfigurationResponse)
def get_amqp_config(
    machine_id: int,
    db: Session = Depends(get_db)
):
    return operations.get_amqp_configuration(db, machine_id)

@app.put("/amqp-config/{machine_id}", response_model=schemas.AmqpConfigurationResponse)
def update_amqp_config(
    machine_id: int,
    amqp_config: schemas.AmqpConfigurationUpdate,                                                                                                                                                                                                                                                                                                          
    db: Session = Depends(get_db)
):
    return operations.update_amqp_configuration(db, machine_id, amqp_config)

@app.post("/amqp-subscribe/")
def subscribe_amqp_queue(
    subscription: schemas.AmqpSubscription,
    db: Session = Depends(get_db)
):
    amqp_config = operations.get_amqp_configuration(db, subscription.machine_id)
    if not amqp_config:
        raise HTTPException(status_code=404, detail="AMQP configuration not found")

    try:
        # Extract connection details
        host = amqp_config.host
        port = amqp_config.port
        username = amqp_config.username
        password = amqp_config.password
        queue = subscription.queue

        # Call helper function to read AMQP message
        data = _read_amqp_message(host, port, username, password, queue)

        return {"machine_id": subscription.machine_id, "queue": queue, "data": data}
    except Exception as e:
        logger.error(f"Error reading from AMQP queue: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

def _read_amqp_message(host, port, username, password, queue_name):
    import pika

    credentials = pika.PlainCredentials(username, password)
    parameters = pika.ConnectionParameters(host=host, port=port, credentials=credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    method_frame, header_frame, body = channel.basic_get(queue=queue_name, auto_ack=True)
    connection.close()

    if method_frame:
        return body.decode()
    else:
        raise TimeoutError("No message available in AMQP queue")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="localhost", port=8002)  # Changed port to 8003
    # uvicorn.run(app, host="172.18.100.214", port=8002)


# this schema created manualy i=using this querry in postgresql query tool
# CREATE SCHEMA IF NOT EXISTS protocols;

# to run the backend ->  python -m app.main