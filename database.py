# from sqlalchemy import create_engine, schema
# from sqlalchemy.orm import sessionmaker, declarative_base
#
# # Database Configuration
# DB_CONFIG = {
#     'host': 'localhost',
#     'database': 'postgres',
#     'user': 'postgres',
#     'password': 'root1234',
#     'port': 5432,
#     'schema': 'protocols'
# }
#
# # Database URL
# SQLALCHEMY_DATABASE_URL = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
#
# # Create engine
# engine = create_engine(
#     SQLALCHEMY_DATABASE_URL,
#     connect_args={'options': f'-csearch_path={DB_CONFIG["schema"]}'}
# )
#
# # Create schema if it doesn't exist
# try:
#     engine.execute(schema.CreateSchema('protocols'))
# except:
#     pass
#
# # Create SessionLocal and Base
# SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
# Base = declarative_base()
#
# # Dependency
# def get_db():
#     db = SessionLocal()
#     try:
#         yield db
#     finally:
#         db.close()

# app/database.py
# from sqlalchemy import create_engine, schema
# from sqlalchemy.orm import sessionmaker, declarative_base
#
# # Database Configuration
# DB_CONFIG = {
#     'host': 'localhost',
#     'database': 'postgres',
#     'user': 'postgres',
#     'password': 'root1234',
#     'port': 5432,
#     'schema': 'protocols'
# }
#
# # Database URL
# SQLALCHEMY_DATABASE_URL = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
#
# # Create engine
# engine = create_engine(
#     SQLALCHEMY_DATABASE_URL,
#     connect_args={'options': f'-csearch_path={DB_CONFIG["schema"]}'}
# )
#
# # Create schema if it doesn't exist
# try:
#     engine.execute(schema.CreateSchema('protocols'))
# except:
#     pass
#
# # Create SessionLocal and Base
# SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
# Base = declarative_base()
#
# # Dependency
# def get_db():
#     db = SessionLocal()
#     try:
#         yield db
#     finally:
#         db.close()

# app/database.py
from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import CreateSchema

# Database URL configuration
SQLALCHEMY_DATABASE_URL = "postgresql://postgres:Ushabs25@localhost:5432/postgres"

# Create engine
engine = create_engine(SQLALCHEMY_DATABASE_URL)

# Create sessionmaker
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create base class
Base = declarative_base()


def init_db():
    # Create the 'protocols' schema if it doesn't exist
    with engine.begin() as connection:
        # Check if schema exists
        schema_exists = connection.execute(text(
            "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'protocols'"
        )).fetchone()

        if not schema_exists:
            connection.execute(CreateSchema('protocols'))
            connection.execute(text('CREATE SCHEMA IF NOT EXISTS protocols'))
            connection.commit()

    # Create all tables
    Base.metadata.create_all(bind=engine)


# Dependency for getting database session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()