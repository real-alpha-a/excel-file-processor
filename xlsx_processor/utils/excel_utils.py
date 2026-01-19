
import openpyxl
import pandas as pd

def process_grouped_headers(file_path, sheet_name, group_row_idx, header_row_idx, separator="_"):
    wb = openpyxl.load_workbook(file_path, data_only=True)
    ws = wb[sheet_name]
    
    # Get standard headers (0-based list)
    df_header = pd.read_excel(file_path, sheet_name=sheet_name, header=None, 
                              skiprows=header_row_idx, nrows=1)
    headers = df_header.iloc[0].tolist()
    
    excel_row = group_row_idx + 1
    group_mapping = {}

    for merged_range in ws.merged_cells.ranges:
        if excel_row >= merged_range.min_row and excel_row <= merged_range.max_row:
            val = ws.cell(row=merged_range.min_row, column=merged_range.min_col).value
            if val:
                for col in range(merged_range.min_col, merged_range.max_col + 1):
                    group_mapping[col - 1] = str(val).strip()

    for col_idx in range(1, len(headers) + 1):
        if (col_idx - 1) not in group_mapping:
            cell_val = ws.cell(row=excel_row, column=col_idx).value
            if cell_val:
                group_mapping[col_idx - 1] = str(cell_val).strip()

    final_columns = []
    for i, col_name in enumerate(headers):
        prefix = group_mapping.get(i)
        clean_col = str(col_name).strip()
        final_columns.append({col: f"{prefix}{separator}{clean_col}" if prefix else clean_col})
            
    return final_columns