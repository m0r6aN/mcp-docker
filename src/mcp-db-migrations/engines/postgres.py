# src/db-migration/engines/postgres.py
import logging
import psycopg2
from psycopg2.extras import DictCursor
from typing import List, Dict, Any, Optional

logger = logging.getLogger("db-migration.postgres")

class PostgresConnector:
    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        database: str,
        ssl_mode: Optional[str] = None
    ):
        self.connection_params = {
            "host": host,
            "port": port,
            "user": username,
            "password": password,
            "dbname": database,
        }
        
        if ssl_mode:
            self.connection_params["sslmode"] = ssl_mode
            
        self.connection = None
        
    def connect(self):
        """Establish a connection to the PostgreSQL database"""
        try:
            self.connection = psycopg2.connect(**self.connection_params)
            logger.info(f"Connected to PostgreSQL database at {self.connection_params['host']}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {str(e)}")
            raise
            
    def disconnect(self):
        """Close the database connection"""
        if self.connection:
            self.connection.close()
            self.connection = None
            
    def get_tables(self) -> List[str]:
        """Get list of all tables in the database"""
        with self.connection.cursor() as cursor:
            cursor.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
            )
            return [row[0] for row in cursor.fetchall()]
            
    def get_schema(self, table_name: str) -> Dict[str, Any]:
        """Get schema definition for a specific table"""
        with self.connection.cursor(cursor_factory=DictCursor) as cursor:
            # Get column information
            cursor.execute("""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_name = %s AND table_schema = 'public'
                ORDER BY ordinal_position
            """, (table_name,))
            columns = cursor.fetchall()
            
            # Get primary key information
            cursor.execute("""
                SELECT c.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name)
                JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema
                    AND tc.table_name = c.table_name AND ccu.column_name = c.column_name
                WHERE constraint_type = 'PRIMARY KEY' AND tc.table_name = %s
            """, (table_name,))
            primary_keys = [row[0] for row in cursor.fetchall()]
            
            return {
                "table_name": table_name,
                "columns": [dict(col) for col in columns],
                "primary_keys": primary_keys
            }
            
    def create_table_from_schema(self, schema: Dict[str, Any]) -> bool:
        """Create a table based on schema definition"""
        table_name = schema["table_name"]
        columns = schema["columns"]
        primary_keys = schema["primary_keys"]
        
        # Build CREATE TABLE statement
        column_defs = []
        for col in columns:
            nullable = "NULL" if col["is_nullable"] == "YES" else "NOT NULL"
            default = f"DEFAULT {col['column_default']}" if col["column_default"] else ""
            column_defs.append(f"{col['column_name']} {col['data_type']} {nullable} {default}".strip())
            
        if primary_keys:
            column_defs.append(f"PRIMARY KEY ({', '.join(primary_keys)})")
            
        create_stmt = f"CREATE TABLE IF NOT EXISTS {table_name} (\n  " + ",\n  ".join(column_defs) + "\n)"
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(create_stmt)
            self.connection.commit()
            logger.info(f"Created table {table_name}")
            return True
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Failed to create table {table_name}: {str(e)}")
            raise
            
    def fetch_data(self, table_name: str, batch_size: int, offset: int = 0) -> List[Dict[str, Any]]:
        """Fetch a batch of data from the table"""
        with self.connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(f"SELECT * FROM {table_name} LIMIT %s OFFSET %s", (batch_size, offset))
            return [dict(row) for row in cursor.fetchall()]
            
    def insert_data(self, table_name: str, data: List[Dict[str, Any]]) -> int:
        """Insert a batch of data into the table"""
        if not data:
            return 0
            
        # Get column names from the first row
        columns = list(data[0].keys())
        placeholders = ", ".join(["%s"] * len(columns))
        column_str = ", ".join(columns)
        
        insert_stmt = f"INSERT INTO {table_name} ({column_str}) VALUES ({placeholders})"
        
        try:
            with self.connection.cursor() as cursor:
                # Convert list of dicts to list of tuples in the correct order
                values = [[row[col] for col in columns] for row in data]
                cursor.executemany(insert_stmt, values)
            self.connection.commit()
            return len(data)
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Failed to insert data into {table_name}: {str(e)}")
            raise