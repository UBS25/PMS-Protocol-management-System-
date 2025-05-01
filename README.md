PMS - Protocol Management System

Executive Summary
The Protocol Management System (PMS) is an advanced software solution designed to manage and streamline communication protocols across industrial systems. It supports a diverse range of protocols, including Modbus, MQTT, OPC UA, and CoAP, enabling efficient data acquisition, storage, and management for industrial automation and IoT applications.

This system is purpose-built for managing machine configurations, storing sensor data, and providing real-time insights through a series of robust APIs and services.

Core Features
1. Multi-Protocol Support
Modbus TCP/IP: Read and write register values seamlessly.
MQTT: Manage configuration, subscribe to topics, and retrieve broker data.
OPC UA: Establish connections, browse nodes, and fetch server-side data.
CoAP: Simulate sensor data and manage CoAP-based devices.
2. Machine Management
Comprehensive endpoints for registering and managing machine details.
Real-time WebSocket-based communication for sensor data.
3. Data Storage
Configurable storage options for raw and processed data.
Integration with databases and OPC UA servers for centralized data management.
4. User Management
Secure user registration and login.
Password hashing and JWT-based authentication.
5. Statistical Insights
Generate and manage statistical cards for machine performance and configurations.
System Architecture
Backend Framework
Developed using FastAPI, a modern web framework for building APIs with Python.
Database integration using SQLAlchemy with support for time-series databases like TimescaleDB.
Data Flow
Input: Sensor data is ingested via Modbus, MQTT, or CoAP protocols.
Processing: Data is processed and stored in the database or relayed to OPC UA servers.
Output: APIs provide real-time access to data and configurations.
Security
Secure API endpoints with JWT authentication.
Encrypted communication support for data transmission.
Installation Guide
Prerequisites
Python: Version 3.8 or higher.
Database: PostgreSQL with TimescaleDB is recommended.
Libraries: FastAPI, SQLAlchemy, Pydantic, aio-pika, and others (see requirements.txt).
Installation Steps
Clone the Repository:
bash
git clone https://github.com/UBS25/PMS-Protocol-management-System-.git
cd PMS-Protocol-management-System-
Install Dependencies:
bash
pip install -r requirements.txt
Set Up the Database:
Create a PostgreSQL database and configure the .env file with the database credentials.
Initialize the database schema:
bash
python main.py init-db
Run the Application:
bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
How to Use
API Overview
Machine Management:
Register a machine: /register
Fetch configuration: /get-machine-config/{machine_id}
Protocol-Specific APIs:
Modbus data reading: /read-raw-data-tcp
MQTT topic subscription: /mqtt-subscribe
OPC UA browsing: /opcua-fetch-nodes
CoAP sensor simulation: /sensor/temperature
Statistical Insights:
Create a new stat card: /stat-cards
Fetch all stat cards: /stat-cards
Example API Request
To retrieve machine configuration:

bash
curl -X GET "http://localhost:8000/get-machine-config/1" \
-H "Authorization: Bearer <your-token>"
Development Roadmap
Planned Enhancements
Integration with additional industrial protocols (e.g., BACnet).
Advanced data visualization dashboards.
Enhanced support for edge computing and IoT devices.
Known Limitations
Limited to the listed protocols as of now.
Requires manual configuration of some machine parameters.
Contributing Guidelines
Contributions are welcome to improve the PMS system. Please follow the steps below:

Fork the repository and create a feature branch.
Commit your changes with clear messages.
Submit a pull request for review.
For details on contributing, refer to the CONTRIBUTING.md file.

Support and Documentation
Issues: Please submit issues via the GitHub Issues Page.
Documentation: Detailed API documentation is auto-generated and available at /docs once the server is running.
License
This project is licensed under the MIT License. You are free to use, modify, and distribute this software under the terms of the license.
