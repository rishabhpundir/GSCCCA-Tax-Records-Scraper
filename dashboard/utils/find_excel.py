import os
import logging

logger = logging.getLogger(__name__)


def find_latest_excel_file(directory, filename_prefix):
    """Find the latest Excel file with the given prefix"""
    try:
        # Look for both .xlsx and .xls files
        excel_files = list(directory.glob(f"{filename_prefix}*.xlsx")) + list(directory.glob(f"{filename_prefix}*.xls"))
        
        if not excel_files:
            return None
        
        # Return the most recently modified file
        return max(excel_files, key=os.path.getmtime)
    except Exception as e:
        logger.error(f"Error finding Excel file: {e}")
        return None
    
    
    