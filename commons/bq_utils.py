from typing import List, Any
from google.cloud import bigquery
from google.cloud.bigquery import Client, SchemaField
from google.api_core.exceptions import NotFound


def table_exists(client: Client, table_ref: str) -> bool:
    """Checks if a BigQuery table exists."""
    try:
        client.get_table(table_ref)
        return True
    except NotFound:
        return False


def get_table_schema(client: Client, table_ref: str) -> List[SchemaField]:
    """Retrieves the current schema of a BigQuery table."""
    table = client.get_table(table_ref)
    return table.schema


def add_columns_if_missing(
    client: Client,
    table_ref: str,
    new_fields: List[SchemaField]
) -> None:
    """Compares provided schema with existing schema and adds missing columns."""
    table = client.get_table(table_ref)

    existing_field_names = {field.name for field in table.schema}
    fields_to_add = [field for field in new_fields if field.name not in existing_field_names]

    summary = {}
    if fields_to_add:
        print(f"Adding {len(fields_to_add)} missing columns: {[f.name for f in fields_to_add]}")
        updated_schema = list(table.schema) + fields_to_add
        table.schema = updated_schema
        client.update_table(table, ["schema"])
        print(f"Schema updated for table: {table_ref}.")
        
        summary = {
            'num_existing_cols': len(existing_field_names),
            'num_new_cols': len(fields_to_add),
            'new_cols': [f.name for f in fields_to_add]
        }
    else:
        print(f"Schema already up to date for table: {table_ref}. No new columns added.")
    
    return summary


def run_query(client: Client, query: str) -> List[bigquery.table.Row]:
    """Executes a SQL query and returns the result rows."""
    job = client.query(query)
    return list(job.result())


def create_table_if_not_exists(
    client: Client,
    table_ref: str,
    schema: List[SchemaField]
) -> str:
    """Creates a table only if it does not already exist. Return status ['CREATED', 'ALREADY_EXITS']"""

    if not table_exists(client=client, table_ref=table_ref):
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        print(f"✅ Table created: {table_ref}")
        return 'CREATED'
    else:
        print(f"ℹ️ Table already exists: {table_ref}")
        return 'ALREADY_EXITS'


def create_or_update_table(
    client: Client,
    table_ref: str,
    schema: List[SchemaField]
) -> tuple[bigquery.Table, dict[str,Any]]:
    """
    Create the table if it does not exist.
    If it exists, add any missing nullable columns.
    """
    summary = {}
    summary['table_name'] = table_ref

    if table_exists(client, table_ref=table_ref):
        print(f"ℹ️ Table {table_ref} exists. Checking for schema updates...")
        update_summary = add_columns_if_missing(client, table_ref, schema)
        summary['creation_status'] = 'EXISTING'
        summary.update(update_summary)
    else:
        print(f"🆕 Table {table_ref} does not exist. Creating...")
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        print(f"✅ Table created: {table_ref}")
        summary['creation_status'] = 'NEW'
    
    return summary