# dashboard/urls.py
from django.urls import path
from . import views

urlpatterns = [
    path('', views.dashboard, name='dashboard'),
    path('start-scraper/', views.start_scraper, name='start_scraper'),
    path('get-latest-data/', views.get_latest_data, name='get_latest_data'),
    
    # New Excel download URLs
    path('download-lien-excel/', views.download_lien_excel, name='download_lien_excel'),
    path('download-all-lien-excel/', views.download_all_lien_excel, name='download_all_lien_excel'),
    path('download-realestate-excel/', views.download_realestate_excel, name='download_realestate_excel'),
    path('download-all-realestate-excel/', views.download_all_realestate_excel, name='download_all_realestate_excel'),
]