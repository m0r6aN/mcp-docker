# src/db-migration/main.py
import uvicorn
import os
from dotenv import load_dotenv
from .api import app

# Load environment variables
load_dotenv()

def main():
    # Get configuration from environment with defaults
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8080"))
    log_level = os.getenv("LOG_LEVEL", "info")
    
    # Start the server
    uvicorn.run(
        "src.db-migration.api:app",
        host=host,
        port=port,
        log_level=log_level,
        reload=os.getenv("ENVIRONMENT", "production") == "development"
    )

if __name__ == "__main__":
    main()