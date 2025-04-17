# src/mcp-db-migrations/engines/oracle.py

import logging
import cx_Oracle
from typing import List, Dict, Any, Optional

logger = logging.getLogger("db_migration.oracle")

class OracleConnector:
    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        service_name: str,
        wallet_location: Optional[str] = None,
    ):
        self.connection_params = {
            "user": username,
            "password": password,
            "dsn": f"{host}:{port}/{service_name}"
        }
        
        if wallet_location:
            self.connection_params["wallet_location"] = wallet_location
            
        self.connection = None
        
    def connect(self):
        """Establish a connection to the Oracle database"""
        try:
            self.connection = cx_Oracle.connect(**self.connection_params)
            logger.info(f"Connected to Oracle database at {self.connection_params['dsn']}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Oracle: {str(e)}")
            raise
            
    def disconnect(self):
        """Close the database connection"""
        if self.connection:
            self.connection.close()
            self.connection = None
            
    def get_tables(self) -> List[str]:
        """Get list of all tables in the database"""
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                "SELECT table_name FROM all_tables WHERE owner = :owner",
                owner=self.connection_params["user"].upper()
            )
            return [row[0] for row in cursor.fetchall()]
        finally:
            cursor.close()
            
    def get_schema(self, table_name: str) -> Dict[str, Any]:
        """Get schema definition for a specific table"""
        cursor = self.connection.cursor()
        try:
            # Get column information
            cursor.execute("""
                SELECT column_name, data_type, data_length, nullable, data_default
                FROM all_tab_columns
                WHERE table_name = :table_name AND owner = :owner
                ORDER BY column_id
            """, table_name=table_name.upper(), owner=self.connection_params["user"].upper())
            
            columns = []
            for row in cursor.fetchall():
                columns.append({
                    "column_name": row[0],
                    "data_type": row[1],
                    "data_length": row[2],
                    "nullable": row[3],
                    "default_value": row[4]
                })
            
            # Get primary key information
            cursor.execute("""
                SELECT cols.column_name
                FROM all_constraints cons, all_cons_columns cols
                WHERE cons.constraint_type = 'P'
                AND cons.constraint_name = cols.constraint_name
                AND cons.owner = cols.owner
                AND cons.table_name = :table_name
                AND cons.owner = :owner
            """, table_name=table_name.upper(), owner=self.connection_params["user"].upper())
            
            primary_keys = [row[0] for row in cursor.fetchall()]
            
            # Get stored procedures for this table
            cursor.execute("""
                SELECT name, type 
                FROM all_source 
                WHERE name LIKE :table_pattern 
                AND owner = :owner
                GROUP BY name, type
            """, table_pattern=f"%{table_name.upper()}%", owner=self.connection_params["user"].upper())
            
            procedures = [{"name": row[0], "type": row[1]} for row in cursor.fetchall()]
            
            return {
                "table_name": table_name,
                "columns": columns,
                "primary_keys": primary_keys,
                "procedures": procedures,
                "original_db_type": "ORACLE"
            }
        finally:
            cursor.close()
    
    def fetch_data(self, table_name: str, batch_size: int, offset: int = 0) -> List[Dict[str, Any]]:
        """Fetch a batch of data from the table"""
        cursor = self.connection.cursor()
        try:
            # Get column names first
            cursor.execute(f"SELECT * FROM {table_name} WHERE ROWNUM = 1")
            column_names = [d[0] for d in cursor.description]
            
            # Fetch batch of data with ROWNUM-based pagination
            cursor.execute(f"""
                SELECT * FROM (
                    SELECT a.*, ROWNUM rnum FROM (
                        SELECT * FROM {table_name} ORDER BY 1
                    ) a WHERE ROWNUM <= :end_row
                ) WHERE rnum > :start_row
            """, end_row=offset+batch_size, start_row=offset)
            
            results = []
            for row in cursor.fetchall():
                row_data = {}
                for i, col_name in enumerate(column_names):
                    row_data[col_name] = row[i]
                results.append(row_data)
                
            return results
        finally:
            cursor.close()