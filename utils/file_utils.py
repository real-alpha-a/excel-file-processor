import os
from typing import Dict, Any
from pathlib import Path

def load_and_format_sql(sql_path: str, params: Dict[str, Any]) -> str:
   
    # Resolves the project base directory 
    base_dir = Path(__file__).resolve().parent.parent.parent
    full_sql_path = base_dir / sql_path

    if not full_sql_path.exists():
        raise FileNotFoundError(f"SQL template not found: {full_sql_path}")

    try:
        with open(full_sql_path, "r") as f:
            raw_query = f.read()
        
        # Injects parameters using the .format(**params) method
        formatted_query = raw_query.format(**params)
        return formatted_query.strip()

    except KeyError as e:
        print(f"Missing parameter for SQL template: {e}")
        raise ValueError(f"Template requires missing key: {e}")
    except Exception as e:
        print(f"Error formatting SQL: {e}")
        raise