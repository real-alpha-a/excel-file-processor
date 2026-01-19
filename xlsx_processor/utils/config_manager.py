import yaml
import os
import logging
from typing import Dict, Any, List, Optional, Union
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

config = None

class ConfigManager:
    _instance = None
    _config_data = {}
    _job_index = {}
    _initialized = False

    def __new__(cls, config_path: str = ""):
        """
        Singleton Pattern: Ensures config is loaded only once.
        """
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
        return cls._instance

    def __init__(self, config_path: str = "resources/config/ingestion_config.yaml"):
        if self._initialized:
            return
        base_dir  = Path(__file__).resolve().parent.parent
        self.config_path = base_dir / config_path
        
        self.reload_config()
        self._initialized = True

    def reload_config(self):
        """Loads or reloads the YAML configuration file."""
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Configuration file not found at: {self.config_path}")
        
        #TODO: validate yaml file before loading

        try:
            with open(self.config_path, "r") as f:
                self._config_data = yaml.safe_load(f)
            
            # Create an index for fast O(1) job lookups
            self._job_index = {
                job['job_id']: job 
                for job in self._config_data.get('jobs', [])
            }
            
            logger.info(f"Configuration loaded successfully from {self.config_path}")

        except yaml.YAMLError as e:
            logger.error(f"Failed to parse YAML: {e}")
            raise

    # ==========================================
    # GLOBAL GETTERS
    # ==========================================
    @property
    def project_id(self) -> str:
        return self._config_data['global_config']['gcp_project_id']

    @property
    def continue_on_error(self) -> bool:
        return self._config_data.get('global_config', {}).get('loading_defaults', {}).get('continue_on_error', False)
    
    @property
    def skip_if_exits(self) -> bool:
        return self._config_data.get('global_config', {}).get('loading_defaults', {}).get('skip_if_exists', True)

    def get_global_bucket(self) -> str:
        # Note: Your YAML structure has bucket inside 'gcs_input' or just global?
        # Based on your YAML, global bucket isn't strictly defined, but usually it's under 'gcs_input' of a job.
        # If you have a global fallback:
        return self._config_data.get('global_config', {}).get('default_bucket')

    def get_audit_config(self) -> Dict[str, str]:
        return self._config_data['global_config'].get('audit', {})

    # ==========================================
    # JOB CONTEXT (Bucket Resolution)
    # ==========================================
    def get_job_config(self, job_id: str) -> Dict[str, Any]:
        """
        Retrieves job details and RESOLVES the bucket (Job > Global).
        """
        job = self._job_index.get(job_id)
        if not job:
            raise ValueError(f"Job ID '{job_id}' not found in configuration.")

        # Resolve Bucket: Check Job specific input first, then Global default
        job_gcs = job.get('gcs_input', {})
        job_specific_bucket = job_gcs.get('bucket')
        global_bucket = self.get_global_bucket()
        
        final_bucket = job_specific_bucket if job_specific_bucket else global_bucket
        
        if not final_bucket:
             # It's possible the bucket is defined elsewhere or not needed for this specific step
             logger.warning(f"Bucket not explicitly defined for job '{job_id}'.")

        return {
            "job_id": job['job_id'],
            "description": job.get("description"),
            "bucket": final_bucket,
            "file_selector": job.get("file_selector"),
            "post_processing": job.get("post_processing"),
            # Merge target defaults if present
            "target_defaults": job.get("target_defaults", {}) 
        }

    # ==========================================
    # HIERARCHICAL PARSING RULES (The "Smart Merge")
    # ==========================================
    def get_sheet_config(self, job_id: str, sheet_identifier: Union[str, int]) -> Dict[str, Any]:
        """
        Returns the FULL configuration for a specific sheet.
        Merges: Global Rules -> Job Rules -> Sheet Rules
        """
        job = self._job_index.get(job_id)
        if not job:
            raise ValueError(f"Job ID '{job_id}' not found.")

        # Find the specific sheet entry
        # Logic matches both 'value' (from your YAML) or 'sheet_identifier' key depending on your structure
        sheet_entry = next((s for s in job.get('sheets',[]) if s['sheet_identifier'] == sheet_identifier), None)
        # for s in job.get("sheets", []):
        #     # Check if using the 'sheet_identifier' dict structure
        #     if isinstance(s.get('sheet_identifier'), dict):
        #         if s['sheet_identifier'].get('value') == sheet_identifier:
        #             sheet_entry = s
        #             break
        #     # Check if using simple key-value (older style)
        #     elif s.get('sheet_identifier') == sheet_identifier:
        #         sheet_entry = s
        #         break
        
        if not sheet_entry:
            raise ValueError(f"Sheet '{sheet_identifier}' not found in job '{job_id}'.")

        # --- MERGE LOGIC START ---
        # 1. Start with Global Defaults
        global_defaults = self._config_data.get('global_config', {}).get('default_parsing_rules', {})
        final_rules = global_defaults.copy()

        # 2. Overlay Job Rules
        job_rules = job.get('job_parsing_rules', {})
        final_rules = self._dict_update(final_rules, job_rules)

        # 3. Overlay Sheet Rules
        sheet_rules = sheet_entry.get('parsing_rules', {}) # Use 'parsing_rules' based on YAML
        final_rules = self._dict_update(final_rules, sheet_rules)
        # --- MERGE LOGIC END ---

        # Resolve Target Table and Dataset
        # Priority: Sheet > Job Defaults > Global Defaults
        
        # Helper to fetch from hierarchy
        def resolve_target(key, global_key):
            val = sheet_entry.get('destination', {}).get(key)
            if not val:
                 val = job.get('target_defaults', {}).get(key)
            if not val:
                 val = self._config_data.get('global_config', {}).get(global_key)
            return val

        land_dataset = resolve_target('land_dataset', 'bq_land_dataset_id')
        # land_table = sheet_entry.get('destination', {}).get('land_table')
        
        ingest_dataset = resolve_target('ingest_dataset', 'bq_ingest_dataset_id')
        ingest_table = sheet_entry.get('destination', {}).get('ingest_table')

        # Identify type
        id_info = sheet_entry.get('sheet_identifier')
        id_type = sheet_entry.get('identifier_type') #id_info.get('type') if isinstance(id_info, dict) else 'unknown'
        land_table = sheet_entry.get('land_table')
        ingest_table = sheet_entry.get('ingest_table')

        return {
            "sheet_identifier": sheet_identifier,
            "identifier_type": id_type,
            "land_dataset": land_dataset,
            "land_table": land_table,
            "ingest_dataset": ingest_dataset,
            "ingest_table": ingest_table,
            "parsing_rules": final_rules,
            "column_mapping": sheet_entry.get("column_mapping", {}),
            "schema_config": sheet_entry.get("schema_config", None) # explicit schema if any
        }

    def _dict_update(self, base: Dict, override: Dict) -> Dict:
        """
        Helper to merge dictionaries.
        """
        result = base.copy()
        for key, value in override.items():
            # Special handling for 'force_string_cols' -> Union the lists
            if key == "force_string_cols" and isinstance(value, list) and isinstance(result.get(key), list):
                # Combine distinct values from both lists
                result[key] = list(set(result[key]) | set(value))
            else:
                # Standard overwrite
                result[key] = value
        return result

# Global Instance for import
# Usage in other files: from config_manager import config
try:
    config = ConfigManager()
except Exception as e:
    logger.warning(f"Config auto-load failed: {e}")