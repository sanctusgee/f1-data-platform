'''
Purpose:
Load environment variables and centralize config for API base URL and database connection.
 Keeps secrets and environment-specific settings out of the core logic.
'''

# fi_pipeline/config/settings.py

import os
from dotenv import load_dotenv
from pathlib import Path

def get_config():
    env_path = Path(__file__).resolve().parent / '.env'
    load_dotenv(dotenv_path=env_path)

    config = {
        "api_base_url": os.getenv("API_BASE_URL", "https://api.jolpi.ca/ergast/"),
        "database": {
            "host": os.getenv("DB_HOST", "localhost"),
            "port": int(os.getenv("DB_PORT", "5432")),
            "name": os.getenv("DB_NAME", "f1_analytics"),
            "user": os.getenv("DB_USER", "postgres"),
            "password": os.getenv("DB_PASSWORD", "")
        }
    }
    return config
