from google.cloud import bigquery
import pandas as pd
from datetime import datetime, timezone
from commons.bq_utils import create_or_update_table

class AuditLogger:
    SKIP_REASONS = {
        "previous_job_crash": "skipped: leftover from a previous incomplete or crashed job run.",
        "current_job_crash": "skipped: job crashed.",
        "already_processed": "Skipped: already processed in a previous successful job run.",
        "validation_failed": "Skipped: failed pre-check validation",
        "no_data": "Skipped: no file or file contains no data to ingest",
        "duplicate_in_batch": "skipped: duplicate file detected in current batch",
        "manual_exclusion": "Skipped: manually excluded from processing",
        "bad_zip": "Skipped: the file is not a valid zip file or is corrupted.",
        "zip_extract_fail": "skipped: failed to extract zip file."
    }

    def __init__(self, bq_client: bigquery.Client, table):
        self.bq_client = bq_client
        self.table = table
        self.run_id = self._generate_run_id()
        self._ensure_audit_table()
        self._clear_old_queued_processing_records()

    def _generate_run_id(self):
        date_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"job_{date_str}"

    def _ensure_audit_table(self):
        schema = [
            bigquery.SchemaField("job_id", "STRING"),
            bigquery.SchemaField("file_group", "STRING"),
            bigquery.SchemaField("file_name", "STRING"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("start_time", "TIMESTAMP"),
            bigquery.SchemaField("end_time", "TIMESTAMP"),
            bigquery.SchemaField("time_taken_sec", "FLOAT"),
            bigquery.SchemaField("error", "STRING"),
            bigquery.SchemaField("transaction_log", "STRING")
        ]
        create_or_update_table(client=self.bq_client, table_ref=self.table, schema=schema)

    def _update_status(self, file_name, status, start_time=None, end_time=None, 
                       duration=None, error=None, transaction_log=None):
        query = f"""
            UPDATE `{self.table}`
            SET
                status = @status,
                start_time = IFNULL(@start_time, start_time),
                end_time = @end_time,
                time_taken_sec = @duration,
                error = @error,
                transaction_log = @transaction_log
            WHERE file_name = @file_name AND job_id = @job_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("status", "STRING", status),
                bigquery.ScalarQueryParameter("start_time", "TIMESTAMP", start_time),
                bigquery.ScalarQueryParameter("end_time", "TIMESTAMP", end_time),
                bigquery.ScalarQueryParameter("duration", "FLOAT64", duration),
                bigquery.ScalarQueryParameter("file_name", "STRING", file_name),
                bigquery.ScalarQueryParameter("error", "STRING", error),
                bigquery.ScalarQueryParameter("transaction_log", "STRING", transaction_log),
                bigquery.ScalarQueryParameter("job_id", "STRING", self.run_id),
            ]
        )

        result = self.bq_client.query(query, job_config=job_config).result()
        
        return result

    def _update_status_to_queued(self, file_list, file_group="default"):
        rows = [{
            "job_id": self.run_id,
            "file_group": file_group,
            "file_name": f,
            "status": "QUEUED",
            "start_time": None,
            "end_time": None,
            "time_taken_sec": None,
            "error": None,
            "transaction_log": None
        } for f in file_list]
        
        df = pd.DataFrame(rows)
        job = self.bq_client.load_table_from_dataframe(df, self.table)
        job.result()

    def _clear_old_queued_processing_records(self):
        sql = f"""
            UPDATE `{self.table}`
            SET status = 'SKIPPED', error = '{self.SKIP_REASONS['previous_job_crash']}'
            WHERE status IN ('QUEUED', 'PROCESSING') AND job_id != '{self.run_id}'
        """
        self.bq_client.query(sql).result()

    def _clear_current_queued_processing_records(self):
        sql = f"""
            UPDATE `{self.table}`
            SET status = 'SKIPPED', error = '{self.SKIP_REASONS['current_job_crash']}'
            WHERE status IN ('QUEUED', 'PROCESSING') AND job_id = '{self.run_id}'
        """
        self.bq_client.query(sql).result()

    def _is_file_processed(self, file_name):
        sql = f"SELECT COUNT(*) as count FROM `{self.table}` WHERE file_name = @file_name AND status = 'COMPLETED'"
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("file_name", "STRING", file_name)]
        )
        res = self.bq_client.query(sql, job_config=job_config).result()
        row = next(res)
        return row["count"] > 0

    def _get_failed_files_from_last_run(self, file_group):
        sql = f"""
            SELECT file_name, job_id FROM `{self.table}` 
            WHERE status = 'FAILED'
            AND job_id = (
                SELECT job_id FROM `{self.table}` 
                GROUP BY job_id 
                ORDER BY MAX(end_time) DESC LIMIT 1
            )
        """
        res = self.bq_client.query(sql).result()
        return [f"{row.job_id}/{file_group}/failed/{row.file_name}" for row in res]

    def _update_to_processing(self, file_name):
        self._update_status(file_name, "PROCESSING", start_time=datetime.now(timezone.utc))

    def _update_to_completed(self, file_name, start_time, transaction_log=None):
        end_time = datetime.now(timezone.utc)
        duration = round((end_time - start_time).total_seconds(), 2)
        self._update_status(file_name, "COMPLETED", start_time, end_time, duration, None, transaction_log)

    def _update_to_failed(self, file_name, start_time, error, transaction_log=None):
        end_time = datetime.now(timezone.utc)
        duration = round((end_time - start_time).total_seconds(), 2)
        self._update_status(file_name, "FAILED", start_time, end_time, duration, str(error), transaction_log)

    def _update_to_skipped(self, file_name, error_key):
        error_msg = self.SKIP_REASONS.get(error_key, error_key)
        self._update_status(file_name, "SKIPPED", error=error_msg)

def get_auditor(bq_client, table_ref):
    return AuditLogger(bq_client, table_ref)