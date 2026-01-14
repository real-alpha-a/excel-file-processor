import csv
import json
import logging
import os
import re
import sys
import tempfile
import zipfile
import io
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Union

import pandas as pd
import yaml
from dateutil import parser
from google.api_core.exceptions import GoogleAPICallError, NotFound, PermissionDenied
from google.api_core.retry import Retry
from google.cloud import bigquery, storage
from google.cloud.storage.blob import Blob

# Custom local imports
from commons.bq_utils import create_or_update_table
from commons.file_converter import FileUtils
from commons.gcs_helper import GCSHelper
from utils.auditor import get_auditor
from utils.config_manager import config  # Assuming config object is exported
from utils.file_utils import load_and_format_sql

BASE_DIR = Path(__file__).resolve().parent

# Global Metadata
META_FIELDS_TYPES = {
    'bq_update_timestamp': 'DATETIME',
    'meta_file_name': 'STRING',
    'meta_file_date': 'DATETIME'
}

# Environment Setup
run_env = os.getenv("RUN_ENV", "").lower()
debug_mode = run_env == "local"
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG if debug_mode else logging.INFO)


def get_file_list(gcs_client, job_config) -> list:
    bucket_name = job_config['bucket']
    match_type = job_config['file_selector']['match_type']
    pattern = job_config['file_selector']['pattern']
    prefix = job_config['file_selector']['prefix']
    supported_formats = job_config['file_selector']['supported_formats']

    gcs_helper = GCSHelper(gcs_client=gcs_client)
    files = gcs_helper._list_blobs(
        bucket_name=bucket_name, 
        prefix=prefix, 
        extensions=supported_formats
    )

    matched_files = []
    if match_type == 'regex':
        try:
            regex = re.compile(pattern=pattern)
        except re.error as e:
            raise ValueError(f"Invalid Regex Pattern '{pattern}': {e}")
        
        for file in files:
            if regex.search(file.name):
                matched_files.append(file)
    
    elif match_type == 'name':
        for file in files:
            if file.name == pattern:
                matched_files.append(file)

    return sorted(matched_files, key=lambda b: b.name)


def handle_post_processing(
    gcs_client, 
    blob: Blob,
    status: Literal["success", "failed", "skipped"] = "skipped", 
    archive_bucket_name: str = None, 
    archive_config: dict = None
):
    action = archive_config.get('action', 'none')
    
    if action == 'none':
        print(f"Post processing action is none. Blob {blob.name} left untouched.")
        return 'SKIPPED'

    if action == 'delete':
        try:
            blob.delete()
            print(f"Deleted blob: {blob.name}")
            return 'DELETED'
        except NotFound:
            print(f"Blob already deleted: {blob.name}")
            return 'ALREADY DELETED'
        except Exception as e:
            print(f"Failed to delete blob {blob.name}: {e}")
            raise

    if action == 'archive':
        archive_path = archive_config.get('archive_path')
        try:
            if blob.exists():
                file_name = str(blob.name).split("/")[-1]
                archive_path_complete = f"{archive_path}/{status}/{file_name}"
                bucket = gcs_client.bucket(archive_bucket_name)
                
                # Move via rewrite and delete
                bucket.rename_blob(blob, archive_path_complete)
                print(f"Archived file to: {archive_path_complete}")
                return 'ARCHIVED'
        except Exception as e:
            print(f"Failed to archive blob {blob.name}: {e}")
            raise


class FileProcessor:
    def __init__(self, bq_client: bigquery.Client, gcs_client: storage.Client, config_inst):
        self.bq_client = bq_client
        self.config = config_inst
        self.gcs_helper = GCSHelper(gcs_client=gcs_client)

    def _parse_metafile_date(self, file_name):
        name, _ = os.path.splitext(file_name)
        last_part = name.split("_")[-1]
        try:
            parsed_date = parser.parse(last_part, fuzzy=False)
            return parsed_date.strftime("%Y%m%d")
        except (ValueError, OverflowError):
            raise ValueError(f"No valid date found in filename: {file_name}")
        
    def _load_file_to_land_dataset(self,csv_file_path,land_table_name,schema):

        land_load_summary = {}
        schema_details = create_or_update_table(client=self.bq_client,table_ref=land_table_name,schema=schema)

        schema_details['num_new_records_added'] = self._upload_file_to_bq(
            table_ref=land_table_name,
            schema=schema,
            file_path=csv_file_path
        )

        land_load_summary['schema'] = schema_details

        return land_load_summary


    def _upload_file_to_bq(self, table_ref, file_path, schema):
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            schema=schema,
            autodetect=False,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema_update_options=["ALLOW_FIELD_ADDITION"]
        )
        with open(file_path, "rb") as f:
            load_job = self.bq_client.load_table_from_file(f, table_ref, job_config=job_config)
        
        load_job.result()
        logger.debug(f"{load_job.output_rows} record(s) inserted to table: {table_ref}")
        return load_job.output_rows

    def _clean_bq_header(self, header: str) -> str:
        if not header:
            return "col_unknown"
        
        # Lowercase and replace special chars
        clean = re.sub(r'[^0-9a-zA-Z_]', '_', str(header).strip().lower())
        # Replace multiple underscores
        clean = re.sub(r'_+', '_', clean).strip('_')
        
        if clean and clean[0].isdigit():
            clean = f"_{clean}"
            
        return clean[:300] if clean else "col_unknown"

 

    def _convert_excel_to_csv(
        self, 
        excel_path: str, 
        output_dir: str, 
        sheet_identifier_type: str, 
        sheet_name: str,
        is_header_missing: bool = False, 
        header_row: int = 0,
        data_start_row: int = 1,
        data_end_row: Optional[int] = None, 
        ordered_header: Optional[List[str]] = None, 
        column_mapping: Optional[Dict[str, str]] = None,
        usecols: Optional[Union[str, List[int]]] = None,
        skip_footer: int = 0, 
        schema_config: Optional[Dict] = None
    ) -> Optional[str]:
        """Converts an Excel sheet to CSV with advanced parsing options."""
        
        source_file = Path(excel_path)
        if not source_file.exists():
            raise FileNotFoundError(f"Source file not found: {excel_path}")

        excel_filename = os.path.basename(excel_path)
        out_path = Path(output_dir)
        out_path.mkdir(parents=True, exist_ok=True)

        # 1. Handle Header and Row Skipping Logic
        pd_header = None if is_header_missing else header_row
        skip_rows_arg = None

        if is_header_missing:
            # Skip everything up to data start if data_start_row > 0
            if data_start_row > 0:
                skip_rows_arg = data_start_row
        else:
            # Consider the gap between header and data start row
            gap_start = header_row + 1
            gap_end = data_start_row
            if gap_end > gap_start:
                skip_rows_arg = range(gap_start, gap_end)

        # 2. Setup Row Limits
        pd_nrows = None
        if data_end_row is not None:
            pd_nrows = data_end_row - data_start_row
            if skip_footer > 0:
                print(f"Warning: 'data_end' is set, so 'skip_footer' ({skip_footer}) will be ignored.")
                skip_footer = 0

        # 3. Read Excel File
        try:
            if sheet_identifier_type == "index":
                with pd.ExcelFile(source_file) as excel_file:
                    sheet_name = excel_file.sheet_names[int(sheet_name)]

            df = pd.read_excel(
                source_file,
                sheet_name=sheet_name,
                header=pd_header,
                skiprows=skip_rows_arg,
                nrows=pd_nrows,
                usecols=usecols,
                skipfooter=skip_footer,
                dtype=str
            )
        except Exception as e:
            raise ValueError(f"Error reading Excel sheet '{sheet_name}': {e}")

        # Check if empty
        if df.empty or all(df[col].isna().all() for col in df.columns):
            print(f"Skipped generating CSV for sheet: {sheet_name}. Reason: Sheet is empty.")
            return None

        # 4. Add Metadata
        load_time = datetime.now().isoformat()
        df['bq_load_timestamp'] = pd.to_datetime(load_time)
        df['meta_file_name'] = excel_filename
        df['meta_file_date'] = pd.to_datetime(str(self._parse_metafile_date(excel_filename)))

        # 5. Apply Ordered Header (if missing)
        if is_header_missing and ordered_header:
            if len(ordered_header) != len(df.columns):
                raise ValueError(
                    f"Config mismatch: 'ordered_header' has {len(ordered_header)} names, "
                    f"but file has {len(df.columns)} columns."
                )
            df.columns = ordered_header

        # 6. Apply Column Mapping
        if column_mapping:
            missing_keys = [k for k in column_mapping.keys() if k not in df.columns]
            if missing_keys:
                print(f"Warning: Mapping keys not found in file: {missing_keys}")
                for k in missing_keys:
                    del column_mapping[k]
            df.rename(columns=column_mapping, inplace=True)

        # 7. Schema Processing & Header Cleaning
        schema = []
        fields_map = {}
        if schema_config:
            fields_map = {str(f['name']).lower(): f for f in schema_config.get("fields", [])}

        seen_cols = {}
        for col in df.columns:
            clean_col = self._clean_bq_header(col)
            
            # Handle Duplicates
            if clean_col in seen_cols:
                seen_cols[clean_col] += 1
                clean_col = f"{clean_col}_{seen_cols[clean_col]}"
            else:
                seen_cols[clean_col] = 0

            # Get Schema Details
            field_def = fields_map.get(str(col).lower(), {})
            f_type = field_def.get('type', "STRING")
            f_mode = field_def.get('mode', "NULLABLE")
            f_desc = field_def.get('description', None)

            # Date Formatting
            if f_type == "DATETIME" and col not in ['meta_load_timestamp', 'meta_file_date']:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')

            schema.append(bigquery.SchemaField(
                name=clean_col,
                field_type=f_type,
                mode=f_mode,
                description=f_desc
            ))

            df.rename(columns={col: clean_col}, inplace=True)

            clean_sheet = sheet_name.replace(" ", "_")
            csv_filename = f"{source_file.stem}_{clean_sheet}.csv"
            csv_full_path = out_path / csv_filename

            df.to_csv(
                csv_full_path,
                index=False,
                encoding="utf-8",
                quoting=1
            )
            
        return {
            'csv_path': str(csv_full_path),
            'bq_schema': schema,
            'rows': df.shape[0],
            'cols': df.shape[1]
        }

    def _process_sheet(self, xlsx_path, job_id, sheet_identifier):
        sheet_ctx = self.config.get_sheet_config(job_id, sheet_identifier)
        rules = sheet_ctx['parsing_rules']
        
        result = self._convert_excel_to_csv(
            excel_path=xlsx_path,
            output_dir=os.path.dirname(xlsx_path),
            sheet_name=sheet_identifier,
            sheet_identifier_type = sheet_ctx['identifier_type'],
            is_header_missing=rules.get('is_header_missing', False),
            header_row = rules.get('header_row', 0),
            data_start_row = rules.get('data_start_row', 1),
            data_end_row= rules.get('data_end_row', None), 
            ordered_header = rules.get('ordered_header', None), 
            column_mapping = rules.get('column_mapping', None),
            usecols = rules.get('usecols', None),
            skip_footer = rules.get('skip_footer', None), 
            schema_config = sheet_ctx['schema_config']
        )

        if not result:
            return {"sheet_name": sheet_identifier, "status": "empty"}
        
        # Load to Landing
        land_table = f"{self.config.project_id}.{sheet_ctx['land_dataset']}.{sheet_ctx['land_table']}"
        land_load_summary = self._load_file_to_land_dataset(csv_file_path=result['csv_path'], land_table_name=land_table,schema=result['bq_schema'])
        
        FileUtils.delete_file(result['csv_path'])
        return land_load_summary

    def process_file(self, file_blob, job_entry):
        xlsx_paths = self.gcs_helper._get_xlsx_from_blob(file_blob)
        xlsx_path = xlsx_paths[0]
        
        job_id = job_entry.get('job_id')
        sheets = job_entry.get('sheets', [])
        
        summaries = []
        for sheet in sheets:
            summary = self._process_sheet(xlsx_path, job_id, sheet['sheet_identifier'])
            summaries.append(summary)
            
        FileUtils.delete_file(xlsx_path)
        return {"file": file_blob.name, "sheets": summaries}

def print_separator(separator):
    print(f"{separator*80}")

def run_job(bq_client, gcs_client, job_id, job_entry, auditor):
    try:
        job_ctx = config.get_job_config(job_id)
    except Exception as e:
        print(f"!! ERROR loading Job {job_id}: {e}")
        return
    
    print_separator("=")
    print(f"\nJOB ID: {job_id}")
    print(f"Description: {job_ctx['description']}")
    print(f"Source Bucket: {job_ctx['bucket']}")
    print(f"File Pattern: {job_ctx['file_selector']['pattern']}")
    print(f"Post Process: {job_ctx['post_processing'].get('action',None)}")
    print_separator("=")

    files_to_process = get_file_list(gcs_client, job_ctx)

    if not files_to_process:
        print(f"No files found for job: {job_id}")
        return
    
    auditor._update_status_to_queued(file.name.split("/")[-1] for file in files_to_process)

    print(f"Total files to process: {len(files_to_process)} under job: {job_id}. Starting processing with Run ID: {auditor.run_id}")
    
    processor = FileProcessor(bq_client, gcs_client, config)

    for blob in files_to_process:
        file_name = blob.name.split("/")[-1]
        start_time = datetime.now(timezone.utc)
        
        if auditor._is_file_processed(file_name):
            print(f"Skipping {file_name}: Already processed.")
            status = 'skipped'
        else:
            try:
                auditor._update_to_processing(file_name)
                summary = processor.process_file(blob, job_entry)
                auditor._update_to_completed(file_name, start_time, json.dumps(summary))
                status = 'success'
            except Exception as e:
                logger.error(f"Failed to process {file_name}: {e}")
                auditor._update_to_failed(file_name, start_time, str(e))
                status = 'failed'
        
        handle_post_processing(
            gcs_client, blob, status, 
            archive_bucket_name=job_ctx['bucket'], 
            archive_config=job_ctx.get('post_processing', {})
        )


def main():
    bq_client = bigquery.Client(project=config.project_id)
    gcs_client = storage.Client(project=config.project_id)
    
    all_jobs = config._config_data.get('jobs', [])
    audit_cfg = config.get_audit_config()
    audit_table = f"{config.project_id}.{audit_cfg['dataset']}.{audit_cfg['table']}"
    auditor = get_auditor(bq_client, audit_table)

    for job_entry in all_jobs:
        run_job(bq_client, gcs_client, job_entry['job_id'], job_entry, auditor)


if __name__ == "__main__":
    main()