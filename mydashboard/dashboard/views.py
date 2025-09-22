# dashboard/views.py
from django.shortcuts import render
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
import threading
import json
import pandas as pd
import os
from pathlib import Path
from datetime import datetime
import asyncio
from dashboard.models import LienData, RealEstateData
import traceback
import logging

# ------------------ LOGGER SETUP -------------------
logger = logging.getLogger(__name__)

# Add this base directory path
BASE_DIR = Path(__file__).resolve().parent.parent
SCRAPERS_DIR = BASE_DIR / "scrapers"
OUTPUT_DIR = SCRAPERS_DIR / "Output"
DOWNLOADS_DIR = SCRAPERS_DIR / "downloads"



def my_view(request):
    logger.info("User opened dashboard view")
    logger.debug("This is a debug message for developers")
    logger.error("Something went wrong!")
    
def dashboard(request):
    lien_data = LienData.objects.all().order_by('-created_at')[:10]
    realestate_data = RealEstateData.objects.all().order_by('-created_at')[:10]
    
    return render(request, 'dashboard.html', {
        'lien_data': lien_data,
        'realestate_data': realestate_data
    })

@csrf_exempt
def start_scraper(request):
    if request.method == 'POST':
        data = json.loads(request.body)
        scraper_type = data.get('scraper_type')
        
        if scraper_type == 'lien':
            thread = threading.Thread(target=run_lien_scraper_and_save)
            thread.start()
            return JsonResponse({'status': 'Lien scraper started'})
        
        elif scraper_type == 'realestate':
            thread = threading.Thread(target=run_realestate_scraper_and_save)
            thread.start()
            return JsonResponse({'status': 'Real estate scraper started'})
    
    return JsonResponse({'error': 'Invalid request'}, status=400)

def get_latest_data(request):
    data_type = request.GET.get('type', 'lien')
    
    if data_type == 'lien':
        data = list(LienData.objects.all().order_by('-created_at')[:10].values())
    else:
        data = list(RealEstateData.objects.all().order_by('-created_at')[:10].values())
    
    return JsonResponse({'data': data})

def run_lien_scraper_and_save():
    """Run lien scraper and save results to database"""
    try:
        logger.info("Starting lien scraper...")
        
        # Import the scraper module
        from scrapers.lien_index_scraper import GSCCCAScraper
        
        # Run the lien scraper
        scraper = GSCCCAScraper()
        asyncio.run(scraper.run())
        
        # Find the latest Excel file - check the correct Output directory
        latest_file = find_latest_excel_file(OUTPUT_DIR, "LienResults")
        
        if not latest_file:
            # Check current scraper directory as fallback
            latest_file = find_latest_excel_file(SCRAPERS_DIR, "LienResults")
            
        if latest_file:
            logger.info(f"Found lien Excel file: {latest_file}")
            
            # Read and save to database
            df = pd.read_excel(latest_file)
            logger.debug(f"Excel file columns: {list(df.columns)}")
            logger.info(f"Number of rows: {len(df)}")
            
            saved_count = 0
            for index, row in df.iterrows():
                try:
                    # Helper function to safely extract and format data
                    def get_field_value(field_name, default='', max_length=None):
                        value = row.get(field_name, default)
                        if pd.isna(value):
                            value = default
                        
                        # Convert to string for consistency
                        value = str(value) if value is not None else default
                        
                        # Apply length limit if specified
                        if max_length is not None and value:
                            return value[:max_length]
                        return value
                    
                    # Create or update record
                    obj, created = LienData.objects.update_or_create(
                        direct_party_debtor=get_field_value('Direct Party (Debtor)', max_length=255),
                        reverse_party_claimant=get_field_value('Reverse Party (Claimant)', max_length=255),
                        book=get_field_value('Book', max_length=50),
                        page=get_field_value('Page', max_length=50),
                        defaults={
                            'address': get_field_value('Address'),
                            'zipcode': get_field_value('Zipcode', max_length=10),
                            'total_due': get_field_value('Total Due', max_length=50),
                            'county': get_field_value('County', max_length=100),
                            'instrument': get_field_value('Instrument', max_length=50),
                            'date_filed': get_field_value('Date Filed', max_length=50),
                            'description': get_field_value('Description'),
                            'pdf_document_url': get_field_value('PDF Document URL'),
                            'pdf_file': get_field_value('PDF', max_length=255),
                        }
                    )
                    
                    if created:
                        saved_count += 1
                        logger.debug(f"Saved record {index + 1}: {get_field_value('Direct Party (Debtor)')}")
                        
                except Exception as e:
                    logger.error(f"Error saving row {index + 1}: {e}")
                    logger.debug(f"Problematic row data: {dict(row)}")
                    traceback.print_exc()
                    
            logger.info(f"Successfully saved {saved_count} out of {len(df)} lien records to database")
        else:
            logger.warning("No lien Excel file found")
            # Check what files exist in the correct locations
            logger.info("Files in Output directory: %s", list(OUTPUT_DIR.glob("*")) if OUTPUT_DIR.exists() else "Output directory doesn't exist")
            logger.info("Files in downloads directory: %s", list(DOWNLOADS_DIR.glob("*")) if DOWNLOADS_DIR.exists() else "Downloads directory doesn't exist")
            logger.info("Files in scrapers directory: %s", list(SCRAPERS_DIR.glob("LienResults*")))

    except Exception as e:
        logger.error(f"Error running lien scraper: {e}")
        traceback.print_exc()

def run_realestate_scraper_and_save():
    """Run real estate scraper and save results to database"""
    try:
        logger.info("Starting real estate scraper...")
        
        # Import the scraper module
        from scrapers.realestate_index_scraper import RealestateIndexScraper
        
        # Run the real estate scraper
        scraper = RealestateIndexScraper()
        asyncio.run(scraper.run())
        
        # Check if we have results but no file was saved
        if hasattr(scraper, 'results') and scraper.results:
            logger.info(f"Found {len(scraper.results)} results in memory, saving directly to database")
            
            saved_count = 0
            for result in scraper.results:
                try:
                    # Create or update record with new field names
                    obj, created = RealEstateData.objects.update_or_create(
                        search_name=result.get('search_name', '')[:255],
                        entity_index=int(result.get('entity_index', 0) or 0),
                        doc_index=int(result.get('doc_index', 0) or 0),
                        defaults={
                            'pdf_viewer': result.get('pdf_viewer', ''),  # Updated field name
                            'realestate_pdf': result.get('final_url', ''),  # Map final_url to realestate_pdf
                            
                        }
                    )
                    
                    if created:
                        saved_count += 1
                        logger.info(f"Saved real estate record: {result.get('search_name', '')}")
                        
                except Exception as e:
                    logger.error(f"Error saving real estate result: {e}")
                    logger.debug(f"Problematic result: {result}")
                    traceback.print_exc()
                    
            logger.info(f"Saved {saved_count} real estate records directly from memory")
            return
        
        # File-based logic as fallback
        possible_locations = [OUTPUT_DIR, SCRAPERS_DIR, Path(".")]
        possible_patterns = ["realestate_index*", "realestate*", "RealEstate*"]
        
        latest_file = None
        for location in possible_locations:
            for pattern in possible_patterns:
                if location.exists():
                    files = list(location.glob(f"{pattern}.xlsx")) + list(location.glob(f"{pattern}.xls"))
                    if files:
                        latest_candidate = max(files, key=os.path.getmtime, default=None)
                        if latest_candidate and (latest_file is None or os.path.getmtime(latest_candidate) > os.path.getmtime(latest_file)):
                            latest_file = latest_candidate
        
        if latest_file:
            logger.info(f"Found real estate Excel file: {latest_file}")
            df = pd.read_excel(latest_file)
            logger.info(f"Excel file columns: {list(df.columns)}")
            
            saved_count = 0
            for _, row in df.iterrows():
                try:
                    row_data = {k: (str(v) if pd.notna(v) else '') for k, v in row.items()}
                    
                    obj, created = RealEstateData.objects.update_or_create(
                        search_name=row_data.get('search_name', '')[:255],
                        entity_index=int(row_data.get('entity_index', 0) or 0),
                        doc_index=int(row_data.get('doc_index', 0) or 0),
                        defaults={
                            'pdf_viewer': row_data.get('pdf_viewer', ''),
                            'realestate_pdf': row_data.get('final_url', ''),
                        }
                    )
                    
                    if created:
                        saved_count += 1
                        
                except Exception as e:
                    logger.error(f"Error saving row: {e}")
                    traceback.print_exc()

            logger.info(f"Saved {saved_count} real estate records from file")
        else:
            logger.warning("No real estate Excel file found")

    except Exception as e:
        logger.error(f"Error running real estate scraper: {e}")
        traceback.print_exc()
        
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