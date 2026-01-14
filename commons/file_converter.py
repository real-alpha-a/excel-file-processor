import pandas as pd
import re
import os
from datetime import datetime
from dateutil import parser

class FileUtils:

    @staticmethod
    def xlsx_to_csv(xlsx_path, output_dir=None, sheet_name=None):
        """
        Converts Excel sheets to CSV files. 
        If sheet_name is None, converts all sheets.
        """
        if output_dir is None:
            output_dir = os.path.dirname(xlsx_path)
            
        base_name = os.path.splitext(os.path.basename(xlsx_path))[0]
        
        # read_excel returns a dict of DataFrames if sheet_name is None
        sheet_dict = pd.read_excel(xlsx_path, sheet_name=sheet_name)
        
        # Ensure we are always working with a dictionary
        if isinstance(sheet_dict, pd.DataFrame):
            sheet_dict = {sheet_name or "Sheet1": sheet_dict}
            
        csv_paths = []
        for name, df in sheet_dict.items():
            # Skip empty sheets or sheets that are effectively empty strings
            if df.empty or df.dropna(how='all').empty:
                continue
                
            safe_sheet_name = str(name).replace(' ', '_').replace('/', '_')
            csv_filename = f"{base_name}_{safe_sheet_name}.csv"
            csv_path = os.path.join(output_dir, csv_filename)
            
            df.to_csv(csv_path, index=False, sep=',')
            csv_paths.append(csv_path)
            
        return csv_paths

    @staticmethod
    def camel_to_snake(name):
        """
        Converts CamelCase or PascalCase strings to snake_case.
        """
        if not isinstance(name, str):
            return name
            
        name = name.strip()
        # Replace hyphens and spaces with underscores
        name = re.sub(r'[-\s]+', '_', name)
        
        # Add underscore between lowercase/digit and uppercase
        name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name)
        
        # Add underscore between uppercase acronyms and following word
        name = re.sub(r'([A-Z])([A-Z][a-z])', r'\1_\2', name)
        
        # Normalize underscores around digits
        name = re.sub(r'(?<=[A-Za-z])(?=\d)', '_', name)
        name = re.sub(r'(?<=\d)(?=[A-Za-z])', '_', name)
        
        # Remove duplicate underscores and lower the case
        name = re.sub(r'_+', '_', name)
        return name.lower().strip('_')

    @staticmethod
    def delete_file(path):
        """Safely deletes a file if it exists."""
        if os.path.exists(path):
            os.remove(path)

    @staticmethod
    def append_meta_filedate_if_missing(file_name, date_format="%Y%m%d"):
        """
        Checks if a file name ends with a date. If not, appends the current date.
        """
        name, ext = os.path.splitext(file_name)
        parts = name.split("_")
        last_part = parts[-1]
        
        try:
            # Try to parse the last part to see if it's already a date
            parser.parse(last_part, fuzzy=False)
            return file_name
        except (ValueError, OverflowError, TypeError):
            # If parsing fails, append current date
            date_str = datetime.now().strftime(date_format)
            return f"{name}_{date_str}{ext}"