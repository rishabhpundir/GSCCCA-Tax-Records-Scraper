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
        # Import the scraper module
        from scrapers.lien_index_scraper import GSCCCAScraper
        
        # Run the lien scraper
        scraper = GSCCCAScraper()
        asyncio.run(scraper.run())
        
        # Find the latest Excel file
        output_dir = Path("Output")
        latest_file = find_latest_excel_file(output_dir, "LienResults")
        
        if latest_file:
            # Read and save to database
            df = pd.read_excel(latest_file)
            
            for _, row in df.iterrows():
                LienData.objects.update_or_create(
                    direct_party_debtor=row.get('Direct Party (Debtor)', ''),
                    reverse_party_claimant=row.get('Reverse Party (Claimant)', ''),
                    book=row.get('Book', ''),
                    page=row.get('Page', ''),
                    defaults={
                        'address': row.get('Address', ''),
                        'zipcode': row.get('Zipcode', ''),
                        'total_due': row.get('Total Due', ''),
                        'county': row.get('County', ''),
                        'instrument': row.get('Instrument', ''),
                        'date_filed': row.get('Date Filed', ''),
                        'description': row.get('Description', ''),
                        'pdf_document_url': row.get('PDF Document URL', ''),
                        'pdf_file': row.get('PDF', ''),
                    }
                )
            print(f"Saved {len(df)} lien records to database")
        else:
            print("No lien Excel file found")
            
    except Exception as e:
        print(f"Error running lien scraper: {e}")
        import traceback
        traceback.print_exc()

def run_realestate_scraper_and_save():
    """Run real estate scraper and save results to database"""
    try:
        # Import the scraper module
        from scrapers.realestate_index_scraper import RealestateIndexScraper
        
        # Run the real estate scraper
        scraper = RealestateIndexScraper()
        asyncio.run(scraper.run())
        
        # Find the latest Excel file
        latest_file = find_latest_excel_file(Path("."), "realestate_index")
        
        if latest_file:
            # Read and save to database
            df = pd.read_excel(latest_file)
            
            for _, row in df.iterrows():
                RealEstateData.objects.update_or_create(
                    search_name=row.get('search_name', ''),
                    entity_index=row.get('entity_index', 0),
                    doc_index=row.get('doc_index', 0),
                    defaults={
                        'final_url': row.get('final_url', ''),
                        'pdf_viewer': row.get('pdf_viewer', ''),
                        'screenshot': row.get('screenshot', ''),
                    }
                )
            print(f"Saved {len(df)} real estate records to database")
        else:
            print("No real estate Excel file found")
            
    except Exception as e:
        print(f"Error running real estate scraper: {e}")
        import traceback
        traceback.print_exc()

def find_latest_excel_file(directory, filename_prefix):
    """Find the latest Excel file with the given prefix"""
    try:
        excel_files = list(directory.glob(f"{filename_prefix}*.xlsx"))
        if not excel_files:
            return None
        
        # Return the most recently modified file
        return max(excel_files, key=os.path.getmtime)
    except Exception as e:
        print(f"Error finding Excel file: {e}")
        return None