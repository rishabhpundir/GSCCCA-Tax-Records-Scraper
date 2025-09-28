import os
import asyncio
import logging
import traceback
from pathlib import Path

import pandas as pd
from django.conf import settings
from django.views.decorators.csrf import csrf_exempt
from dashboard.models import LienData, RealEstateData

from scrapers.lien_index_scraper import GSCCCAScraper
from dashboard.utils.find_excel import find_latest_excel_file


# ------------------ LOGGER SETUP -------------------
logger = logging.getLogger(__name__)

BASE_DIR = Path(settings.BASE_DIR)
SCRAPERS_DIR = os.path.join(BASE_DIR, "scrapers")
OUTPUT_DIR = os.path.join(SCRAPERS_DIR, "Output")
DOWNLOADS_DIR = os.path.join(SCRAPERS_DIR, "downloads")

os.makedirs(SCRAPERS_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(DOWNLOADS_DIR, exist_ok=True)  


def run_lien_scraper(params: dict):
    """Run lien scraper and save results to database"""
    try:
        logger.info("Starting lien scraper...")
        scraper = GSCCCAScraper()
        asyncio.run(scraper.run_dynamic(params))
        
        # Find the latest Excel file - check the correct Output directory
        latest_file = find_latest_excel_file(Path(OUTPUT_DIR), "LienResults")
        
        if not latest_file:
            # Check current scraper directory as fallback
            latest_file = find_latest_excel_file(Path(SCRAPERS_DIR), "LienResults")
            
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
    except Exception as e:
        logger.error(f"Error running lien scraper: {e}")
        traceback.print_exc()


def run_realestate_scraper():
    """Run real estate scraper and save results to database"""
    try:
        logger.info("Starting real estate scraper...")
        
        # Import the scraper module
        from scrapers.realestate_index_scraper import RealestateIndexScraper
        
        # Run the real estate scraper
        scraper = RealestateIndexScraper()
        asyncio.run(scraper.run())

        # Agar results hain, to pehle unhe database me save karo
        if hasattr(scraper, 'results') and scraper.results:
            logger.info(f"Found {len(scraper.results)} results in memory, saving to database first.")
            
            saved_count = 0
            for result in scraper.results:
                try:
                    # 'search_name' ko 'Search Name' se map kiya
                    search_name = result.get('Search Name', '')
                    entity_index = int(result.get('Entity Index', 0) or 0)
                    doc_index = int(result.get('Doc Index', 0) or 0)
                    pdf_viewer_url = result.get('PDF Viewer URL', '')
                    realestate_pdf_path = result.get('Real Estate PDF', '')

                    # Django ORM se database mein data save karo
                    obj, created = RealEstateData.objects.update_or_create(
                        search_name=search_name[:255],
                        entity_index=entity_index,
                        doc_index=doc_index,
                        defaults={
                            'pdf_viewer': pdf_viewer_url,
                            'realestate_pdf': realestate_pdf_path,
                        }
                    )
                    
                    if created:
                        saved_count += 1
                        logger.debug(f"Saved new real estate record: {search_name}")
                        
                except Exception as e:
                    logger.error(f"Error saving real estate result to DB: {e}")
                    logger.debug(f"Problematic result: {result}")
                    traceback.print_exc()

            logger.info(f"Successfully saved {saved_count} real estate records to database.")

            # Ab, database se data nikal kar Excel file mein save karo
            excel_path = scraper.save_results_to_excel()
            if excel_path:
                logger.info(f"Real Estate data successfully saved to Excel at: {excel_path}")
            else:
                logger.error("Failed to save Excel file from scraper results.")
        
        else:
            logger.warning("No real estate results found in scraper, nothing to save.")
            
    except Exception as e:
        logger.error(f"Error running real estate scraper: {e}")
        traceback.print_exc()

        # Agar results hain, to pehle unhe database me save karo
        if hasattr(scraper, 'results') and scraper.results:
            logger.info(f"Found {len(scraper.results)} results in memory, saving to database first.")

            saved_count = 0
            for result in scraper.results:
                try:
                    # 'search_name' ko 'Search Name' se map kiya
                    search_name = result.get('Search Name', '')
                    entity_index = int(result.get('Entity Index', 0) or 0)
                    doc_index = int(result.get('Doc Index', 0) or 0)
                    pdf_viewer_url = result.get('PDF Viewer URL', '')
                    realestate_pdf_path = result.get('Real Estate PDF', '')

                    # Django ORM se database mein data save karo
                    obj, created = RealEstateData.objects.update_or_create(
                        search_name=search_name[:255],
                        entity_index=entity_index,
                        doc_index=doc_index,
                        defaults={
                            'pdf_viewer': pdf_viewer_url,
                            'realestate_pdf': realestate_pdf_path,
                        }
                    )

                    if created:
                        saved_count += 1
                        logger.debug(f"Saved new real estate record: {search_name}")

                except Exception as e:
                    logger.error(f"Error saving real estate result to DB: {e}")
                    logger.debug(f"Problematic result: {result}")
                    traceback.print_exc()

            logger.info(f"Successfully saved {saved_count} real estate records to database.")

            # Ab, database se data nikal kar Excel file mein save karo
            excel_path = scraper.save_results_to_excel()
            if excel_path:
                logger.info(f"SUCCESS: Real Estate data successfully saved to Excel at: {excel_path}")
            else:
                logger.error("FAILED: Failed to save Excel file from scraper results.")

        else:
            logger.warning("No real estate results found in scraper, nothing to save.")
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
    
    
    