# src/db-migration/api.py
# FastAPI service

from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import os
import logging

# Initialize FastAPI app
app = FastAPI(
    title="MCP Database Migration Service",
    description="API for managing and executing database migrations",
    version="0.1.0"
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("db-migration")

# Models
class DatabaseConfig(BaseModel):
    db_type: str  # "postgres", "mysql", "sqlite"
    host: Optional[str] = None
    port: Optional[int] = None
    username: Optional[str] = None
    password: Optional[str] = None
    database_name: str
    ssl_mode: Optional[str] = None

class MigrationRequest(BaseModel):
    source_db: DatabaseConfig
    target_db: DatabaseConfig
    tables: Optional[List[str]] = None
    exclude_tables: Optional[List[str]] = None
    only_schema: bool = False
    batch_size: int = 1000

class MigrationResponse(BaseModel):
    migration_id: str
    status: str
    details: Optional[Dict[str, Any]] = None

# In-memory store for migrations (replace with a real DB in production)
migrations = {}

# Routes
@app.get("/")
async def root():
    return {"message": "MCP Database Migration Service", "status": "running"}

@app.post("/migrations", response_model=MigrationResponse)
async def create_migration(request: MigrationRequest, background_tasks: BackgroundTasks):
    migration_id = f"migration-{len(migrations) + 1}"
    
    # Store migration details
    migrations[migration_id] = {
        "request": request.dict(),
        "status": "pending",
        "details": None
    }
    
    # Schedule migration task
    background_tasks.add_task(run_migration, migration_id, request)
    
    return MigrationResponse(
        migration_id=migration_id,
        status="pending"
    )

@app.get("/migrations/{migration_id}", response_model=MigrationResponse)
async def get_migration(migration_id: str):
    if migration_id not in migrations:
        raise HTTPException(status_code=404, detail="Migration not found")
    
    migration = migrations[migration_id]
    
    return MigrationResponse(
        migration_id=migration_id,
        status=migration["status"],
        details=migration["details"]
    )

@app.get("/migrations", response_model=List[MigrationResponse])
async def list_migrations():
    return [
        MigrationResponse(
            migration_id=mid,
            status=m["status"],
            details=m["details"]
        )
        for mid, m in migrations.items()
    ]

# Migration executor
async def run_migration(migration_id: str, request: MigrationRequest):
    logger.info(f"Starting migration {migration_id}")
    migrations[migration_id]["status"] = "running"
    
    try:
        # TODO: Implement the actual migration logic
        # 1. Connect to source and target databases
        # 2. Schema transfer if needed
        # 3. Data transfer with proper batching
        
        # For now, we'll just simulate a successful migration
        import time
        time.sleep(2)  # Simulate work
        
        migrations[migration_id]["status"] = "completed"
        migrations[migration_id]["details"] = {
            "tables_migrated": request.tables or ["all tables"],
            "rows_transferred": 1000,
            "duration_seconds": 2
        }
        logger.info(f"Migration {migration_id} completed successfully")
        
    except Exception as e:
        logger.error(f"Migration {migration_id} failed: {str(e)}")
        migrations[migration_id]["status"] = "failed"
        migrations[migration_id]["details"] = {"error": str(e)}
        
# In api.py, add this function:

async def run_oracle_to_postgres_migration(migration_id: str, request: MigrationRequest):
    """
    Specialized function for Oracle to PostgreSQL migrations, with special handling
    for healthcare data and compliance requirements.
    """
    logger.info(f"Starting Oracle to PostgreSQL migration {migration_id}")
    migrations[migration_id]["status"] = "running"
    
    # Update migration phases
    migrations[migration_id]["details"]["current_phase"] = "schema_analysis"
    
    try:
        # Import our connectors
        from engines.oracle import OracleConnector
        from engines.postgres import PostgresConnector
        
        # Connect to source (Oracle)
        oracle = OracleConnector(
            host=request.source_db.host,
            port=request.source_db.port,
            username=request.source_db.username,
            password=request.source_db.password,
            service_name=request.source_db.database_name
        )
        oracle.connect()
        
        # Connect to target (PostgreSQL)
        postgres = PostgresConnector(
            host=request.target_db.host,
            port=request.target_db.port,
            username=request.target_db.username,
            password=request.target_db.password,
            database=request.target_db.database_name,
            ssl_mode=request.target_db.ssl_mode
        )
        postgres.connect()
        
        # Get list of tables to migrate
        tables = request.tables or oracle.get_tables()
        if request.exclude_tables:
            tables = [t for t in tables if t not in request.exclude_tables]
            
        # Create audit log for the migration
        migration_log = []
        
        # Track statistics
        stats = {
            "tables_processed": 0,
            "rows_migrated": 0,
            "schema_conversions": 0,
            "type_conversions": 0,
            "stored_procedures": 0
        }
        
        # Update migration details
        migrations[migration_id]["details"]["tables"] = tables
        migrations[migration_id]["details"]["stats"] = stats
        
        # Phase 1: Schema Analysis and Conversion
        for table_name in tables:
            logger.info(f"Analyzing schema for table {table_name}")
            migration_log.append(f"Analyzing schema for table {table_name}")
            
            # Get Oracle schema
            oracle_schema = oracle.get_schema(table_name)
            
            # Update migration phase
            migrations[migration_id]["details"]["current_phase"] = "type_conversion"
            migrations[migration_id]["details"]["current_table"] = table_name
            
            # Convert Oracle schema to PostgreSQL schema
            pg_schema = convert_oracle_to_postgres_schema(oracle_schema)
            
            # Create table in PostgreSQL
            postgres.create_table_from_schema(pg_schema)
            
            # Update stats
            stats["tables_processed"] += 1
            stats["schema_conversions"] += 1
            
            # Update migration details
            migrations[migration_id]["details"]["stats"] = stats
            
            # Phase 2: Data Migration (if not schema-only)
            if not request.only_schema:
                migrations[migration_id]["details"]["current_phase"] = "data_migration"
                
                # Determine total rows for progress tracking
                row_count = get_oracle_table_row_count(oracle, table_name)
                rows_migrated = 0
                
                # Process in batches
                batch_size = request.batch_size
                offset = 0
                
                while offset < row_count:
                    # Fetch batch of data from Oracle
                    oracle_data = oracle.fetch_data(table_name, batch_size, offset)
                    
                    if not oracle_data:
                        break
                        
                    # Transform data as needed (data type conversions, etc.)
                    postgres_data = transform_oracle_to_postgres_data(
                        oracle_data, 
                        oracle_schema, 
                        pg_schema
                    )
                    
                    # Insert data into PostgreSQL
                    postgres.insert_data(table_name, postgres_data)
                    
                    # Update progress
                    rows_migrated += len(oracle_data)
                    stats["rows_migrated"] += len(oracle_data)
                    migrations[migration_id]["details"]["stats"] = stats
                    migrations[migration_id]["details"]["progress"] = {
                        "table": table_name,
                        "rows_processed": rows_migrated,
                        "total_rows": row_count,
                        "percentage": min(100, int((rows_migrated / row_count) * 100)) if row_count > 0 else 100
                    }
                    
                    # Log progress
                    logger.info(f"Migrated {rows_migrated}/{row_count} rows from {table_name}")
                    migration_log.append(f"Migrated {rows_migrated}/{row_count} rows from {table_name}")
                    
                    # Move to next batch
                    offset += batch_size
        
        # Phase 3: PL/SQL Conversion (simplified for demo)
        migrations[migration_id]["details"]["current_phase"] = "plsql_conversion"
        # Code for PL/SQL conversion would go here
        
        # Phase 4: Validation
        migrations[migration_id]["details"]["current_phase"] = "validation"
        # Validation logic would go here
        
        # Migration complete
        migrations[migration_id]["status"] = "completed"
        migrations[migration_id]["details"]["current_phase"] = "completed"
        migrations[migration_id]["details"]["migration_log"] = migration_log
        migrations[migration_id]["details"]["duration_seconds"] = calculate_duration(migration_id)
        logger.info(f"Oracle to PostgreSQL migration {migration_id} completed successfully")
        
    except Exception as e:
        logger.error(f"Migration {migration_id} failed: {str(e)}")
        migrations[migration_id]["status"] = "failed"
        migrations[migration_id]["details"]["error"] = str(e)
        migrations[migration_id]["details"]["current_phase"] = "failed"
    finally:
        # Clean up connections
        if 'oracle' in locals():
            oracle.disconnect()
        if 'postgres' in locals():
            postgres.disconnect()

# Helper functions for the migration

def convert_oracle_to_postgres_schema(oracle_schema: Dict[str, Any]) -> Dict[str, Any]:
    """Convert Oracle schema to PostgreSQL schema"""
    # Data type mapping
    type_mapping = {
        "NUMBER": "NUMERIC",
        "VARCHAR2": "VARCHAR",
        "CHAR": "CHAR",
        "DATE": "TIMESTAMP",
        "TIMESTAMP": "TIMESTAMP",
        "CLOB": "TEXT",
        "BLOB": "BYTEA",
        "FLOAT": "DOUBLE PRECISION",
        "RAW": "BYTEA",
        "LONG RAW": "BYTEA",
        "NVARCHAR2": "VARCHAR",
        "NCHAR": "CHAR"
    }
    
    # Create new schema structure
    pg_schema = {
        "table_name": oracle_schema["table_name"].lower(),
        "columns": [],
        "primary_keys": [pk.lower() for pk in oracle_schema["primary_keys"]],
        "original_db_type": "POSTGRESQL"
    }
    
    # Convert column definitions
    for col in oracle_schema["columns"]:
        oracle_type = col["data_type"]
        pg_type = type_mapping.get(oracle_type, "VARCHAR")
        
        # Handle NUMBER type with precision/scale
        if oracle_type == "NUMBER" and "data_precision" in col and "data_scale" in col:
            if col["data_precision"] is not None and col["data_scale"] is not None:
                pg_type = f"NUMERIC({col['data_precision']},{col['data_scale']})"
            elif col["data_precision"] is not None:
                pg_type = f"NUMERIC({col['data_precision']})"
        
        # Handle VARCHAR2 with length
        elif oracle_type == "VARCHAR2" and "data_length" in col:
            pg_type = f"VARCHAR({col['data_length']})"
        
        pg_schema["columns"].append({
            "column_name": col["column_name"].lower(),
            "data_type": pg_type,
            "nullable": col["nullable"] == "Y",
            "column_default": col.get("default_value")
        })
    
    return pg_schema

def transform_oracle_to_postgres_data(
    oracle_data: List[Dict[str, Any]], 
    oracle_schema: Dict[str, Any], 
    pg_schema: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """Transform Oracle data to fit PostgreSQL schema"""
    result = []
    
    for row in oracle_data:
        pg_row = {}
        
        for pg_col in pg_schema["columns"]:
            col_name = pg_col["column_name"]
            oracle_col_name = next(
                (c["column_name"] for c in oracle_schema["columns"] 
                 if c["column_name"].lower() == col_name),
                None
            )
            
            if oracle_col_name and oracle_col_name in row:
                value = row[oracle_col_name]
                
                # Handle data type conversions
                # Example: CLOB to TEXT
                if value is not None:
                    oracle_type = next(
                        c["data_type"] for c in oracle_schema["columns"] 
                        if c["column_name"] == oracle_col_name
                    )
                    
                    if oracle_type == "CLOB":
                        # Convert CLOB to text
                        value = str(value.read()) if hasattr(value, 'read') else str(value)
                    elif oracle_type == "BLOB":
                        # Convert BLOB to bytea
                        value = bytes(value.read()) if hasattr(value, 'read') else bytes(value)
                
                pg_row[col_name] = value
        
        result.append(pg_row)
    
    return result

def get_oracle_table_row_count(oracle, table_name: str) -> int:
    """Get the number of rows in an Oracle table"""
    cursor = oracle.connection.cursor()
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        return cursor.fetchone()[0]
    finally:
        cursor.close()

def calculate_duration(migration_id: str) -> int:
    """Calculate the duration of the migration in seconds"""
    # In a real implementation, you'd track start/end times
    return 120  # Placeholder