import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Literal, Optional, Union
import pandas as pd
from dateutil import parser
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage
from google.cloud.storage.blob import Blob
from xlsx_processor.commons.bq_utils import create_or_update_table
from xlsx_processor.commons.file_converters import FileUtils
from xlsx_processor.commons.gcs_helper import GCSHelper
from xlsx_processor.utils.auditor import get_auditor
from xlsx_processor.utils.config_manager import config  # Assuming config object is exported

BASE_DIR = Path(__file__).resolve().parent
META_FIELDS_TYPES = {
    'bq_load_timestamp': 'DATETIME',
    'bq_update_timestamp': 'DATETIME',
    'meta_file_name': 'STRING',
    'meta_file_date': 'DATETIME'
}

# Environment Setup
run_env = os.getenv("RUN_ENV", "").lower()
debug_mode = run_env == "local"
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG if debug_mode else logging.INFO)

def get_file_list(gcs_client, input_config) -> list:
    bucket_name = input_config['bucket']
    match_type = input_config['file_selector']['match_type']
    pattern = input_config['file_selector']['pattern']
    prefix = input_config['file_selector']['prefix']
    supported_formats = input_config['file_selector']['supported_formats']

    gcs_helper = GCSHelper(gcs_client=gcs_client)
    files = gcs_helper._list_blobs(bucket_name=bucket_name, prefix=prefix, extensions=supported_formats)
    matched_files = []

    if match_type == 'regex':
        try:
            regex = re.compile(pattern=pattern)
        except re.error as e:
            raise ValueError(f"Invalid Regex Pattern '{pattern}': {e}")
        
        for file in files:
            if regex.search(file.name):
                matched_files.append(file)
    if match_type == 'name':
        for file in files:
            if file.name == pattern:
                matched_files.append(file)

    sorted_files =  sorted(matched_files, key=lambda b: b.name)
    return sorted_files

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
            raise(f"Failed to delete blob {blob.name}: {e}")

    if action == 'archive':
        archive_path = archive_config.get('archive_path')
        try:
            if blob.exists():
                archive_path_complete = f"{archive_path}/{status}/{str(blob.name).split("/")[-1]}"
                bucket = gcs_client.bucket(archive_bucket_name)
                archive_blob = bucket.blob(archive_path_complete)
                archive_blob.rewrite(blob)
                blob.delete()
                print(f"Archived file to: {archive_path_complete}")
                return 'ARCHIVED'
        except Exception as e:
            raise(f"Failed to archive blob {blob.name}: {e}")

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
        
    def _load_file_to_land_dataset(self,csv_file_path,land_table_name,schema, write_mode):
        land_load_summary = {}
        _, schema_details = create_or_update_table(client=self.bq_client,table_ref=land_table_name,schema=schema)

        schema_details["num_new_records_added"] = self._upload_file_to_bq(
            table_ref=land_table_name,
            schema=schema,
            file_path=csv_file_path,
            write_mode = write_mode
        )
        land_load_summary['schema'] = schema_details
        return land_load_summary

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
        data_start: int = 1,
        data_end: Optional[int] = None, 
        ordered_header: Optional[List[str]] = None, 
        column_mapping: Optional[Dict[str, str]] = None,
        usecols: Optional[Union[str, List[int]]] = None,
        skip_footer: int = 0, 
        schema_config = None
    ) -> Optional[str]:
        """
        Converts an Excel sheet to CSV with parsing options.
        """
        source_file = Path(excel_path)
        if not source_file.exists():
            raise FileNotFoundError(f"Source file not found: {excel_path}")

        excel_filename = os.path.basename(excel_path)
        out_path = Path(output_dir)
        out_path.mkdir(parents=True, exist_ok=True)

        if not header_row:
            header_row = 0

        pd_header = None if is_header_missing else header_row
        
        skip_rows_arg = None

        if is_header_missing:
            # Skip everything up to data start if data_start > 0
            if data_start > 0:
                skip_rows_arg = data_start
        else:
            # Consider the gap between header and data start row
            gap_start = header_row + 1
            gap_end = data_start

            if gap_end > gap_start:
                skip_rows_arg = range(gap_start, gap_end)

        # Setup 'nrows' (Data End)
        pd_nrows = None
        if data_end is not None:
            pd_nrows = data_end - data_start
            if skip_footer > 0:
                print(f"Warning: 'data_end' is set, so 'skip_footer' ({skip_footer}) will be ignored.")
                skip_footer = 0

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

        if df.empty or all(df[col].isna().all() for col in df.columns):
            return {
                'status': 'skipped',
                'reason': f"Skipped generating CSV for sheet: {sheet_name}. Reason: Sheet is empty."
            }
            
         # Apply Ordered Header (if headers missing)
        if is_header_missing and ordered_header:
            if len(ordered_header) != len(df.columns):
                raise ValueError(
                    f"Config mismatch: 'ordered_header' has {len(ordered_header)} names, "
                    f"but file has {len(df.columns)} columns."
                )
            df.columns = ordered_header

         # Apply column mapping 
        if column_mapping:
            missing_keys = [k for k in column_mapping.keys() if k not in df.columns]
            if missing_keys:
                print(f"Warning: Mapping keys not found in file: {missing_keys}")
                for k in missing_keys:
                    del column_mapping[k]
            df.rename(columns=column_mapping, inplace=True)

        # Add Metadata
        load_time = datetime.now().isoformat()
        df['bq_load_timestamp'] = pd.to_datetime(load_time)
        df['meta_file_name'] = excel_filename
        df['meta_file_date'] = pd.to_datetime(str(self._parse_metafile_date(excel_filename)))

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
            if f_type == "DATETIME" and col not in META_FIELDS_TYPES.keys():
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
            'status': 'success',
            'csv_path': str(csv_full_path),
            'bq_schema': schema,
            'rows': df.shape[0],
            'cols': df.shape[1]
        }

    def _process_sheet(self, xlsx_path, job_id, sheet_identifier):
        sheet_ctx = self.config.get_sheet_config(job_id, sheet_identifier)
        print(f"Using following parsing rules for job: {job_id}, rules: {sheet_ctx}")
        rules = sheet_ctx['parsing_rules']
        destination = sheet_ctx['destination']
        
        result = self._convert_excel_to_csv(
            excel_path=xlsx_path,
            output_dir=os.path.dirname(xlsx_path),
            sheet_identifier_type = sheet_ctx['identifier_type'],
            sheet_name=sheet_identifier,
            is_header_missing=rules.get('is_header_missing', False),
            header_row = rules.get('header_row', 0),
            data_start = rules.get('data_start', 1),
            data_end= rules.get('data_end', None), 
            ordered_header = rules.get('ordered_header', None), 
            column_mapping = rules.get('column_mapping', None),
            usecols = rules.get('usecols', None),
            skip_footer = rules.get('skip_footer', 0), 
            schema_config = destination['layers']['land']['schema']
        )

        if result['status'] == 'skipped':
            print(result['reason'])
            return
            
        load_summary = {}
        load_summary["sheetname"] = sheet_identifier
        load_summary["total_rows"] = result['rows']
        load_summary["total_cols"] = result['cols']

        sheet_load_summary = {
            "sheet_name": os.path.basename(result['csv_path'])
        }

        land_table = f"{self.config.project_id}.{destination['layers']['land']['dataset']}.{destination['layers']['land']['table']}"
        sheet_load_summary['land']  = self._load_file_to_land_dataset(
            csv_file_path=result['csv_path'], 
            land_table_name=land_table,
            schema=result['bq_schema'],
            write_mode=destination['layers']['land']['write_mode']
        )

        FileUtils.delete_file(result['csv_path'])
        return sheet_load_summary
    
    def _process_file(self, file_blob, job_entry):
        xlsx_paths = self.gcs_helper._get_xlsx_from_blob(file_blob)
        xlsx_path = xlsx_paths[0]
        
        file_summary = {
            'filename': os.path.basename(xlsx_path)
        }
        job_id = job_entry.get('job_id')
        sheets = job_entry.get('sheets', [])
        summaries = []
        for sheet in sheets:
            summary = self._process_sheet(xlsx_path, job_id, sheet['sheet_identifier'])
            summaries.append(summary)

        file_summary['sheets'] = summaries    
        FileUtils.delete_file(xlsx_path)
        
        return file_summary
    
    def _upload_file_to_bq(self, table_ref, file_path, schema, write_mode):
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            schema=schema,
            autodetect=False,
            write_disposition=write_mode,
            schema_update_options=["ALLOW_FIELD_ADDITION"] if write_mode == "WRITE_APPEND" else None
        )

        with open(file_path, "rb") as f:
            load_job = self.bq_client.load_table_from_file(f, table_ref, job_config=job_config)

        load_job.result()
        logger.debug(f"{load_job.output_rows} record(s) inserted to table: {table_ref}")
        return load_job.output_rows

def print_separator(separator):
    print(f"{separator*80}")

def run_job(bq_client, gcs_client, job_id, job_entry, auditor):
    try:
        job_ctx = config.get_job_config(job_id)
    except ValueError as e:
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

    processor = FileProcessor(
        bq_client=bq_client,
        gcs_client= gcs_client,
        config_inst= config
    )

    for i, blob in enumerate(files_to_process):
        process_status = None
        file_name = blob.name.split("/")[-1]
        start_time = datetime.now(timezone.utc)
        
        global transactionLog
        transactionLog = {}
        transactionLog['file_name'] = file_name
        start_time = datetime.now(timezone.utc)

        print(f"\u23f3 Processing file {i+1}/{len(files_to_process)} - {file_name} ")
        if config.skip_if_exists and auditor._is_file_processed(file_name):
            auditor._update_to_skipped(file_name=file_name,error=auditor.SKIP_REASONS['already_processed'])
            print(f"Skipping {file_name}: Already processed.")
            process_status = 'skipped'
            continue
        else:
            try:
                auditor._update_to_processing(file_name)
                processing_summary = processor._process_file(blob, job_entry)
                transactionLog["details"] = processing_summary
                auditor._update_to_completed(file_name, start_time, json.dumps(processing_summary))
                process_status = 'success'
            except Exception as e:
                logger.error(f"Failed to process {file_name}: {e}")
                process_status = 'failed'
                auditor._update_to_failed(file_name, start_time, str(e), None)
        
        handle_post_processing(
            gcs_client=gcs_client, 
            blob=blob, 
            status=process_status, 
            archive_bucket_name=job_ctx['bucket'], 
            archive_config=job_ctx.get('post_processing', {})
        )

        if process_status == 'failed' and not config.continue_on_error:
            auditor._clear_current_queued_processing_records()
            sys.exit()
        
        print(f"File: {file_name} has been processed successfully.")

def main(bq_client, gcs_client):
    print("\n --- Pipeline Run Started ---")
    all_jobs = config._config_data.get('jobs', [])

    if not all_jobs:
        print("No jobs found in configuration! Consider adding job(s) in ingestion config.")
        sys.exit()

    audit_config = config.get_audit_config()
    audit_table = f"{config.project_id}.{audit_config['dataset']}.{audit_config['table']}"
    auditor = get_auditor(bq_client=bq_client, table_ref=audit_table )

    for job_entry in all_jobs:
        
        run_job(
            bq_client=bq_client, 
            gcs_client=gcs_client, 
            job_id=job_entry['job_id'], 
            job_entry=job_entry, 
            auditor=auditor
        )
    
    print("\n --- Pipeline Run Complete ---")

def xlsx_file_ingestion(project_name):
    bq_client = bigquery.Client(project=project_name)
    gcs_client = storage.Client(project=project_name)
    main(bq_client=bq_client, gcs_client=gcs_client)

# --- Independent Execution Block ---
if __name__ == "__main__":
    try:
        print("Running utility independently...")
        xlsx_file_ingestion(config.project_id)
    except Exception as e: 
        print(f"Error: {e}")    