# dashboard/urls.py
from django.urls import path
from . import views

urlpatterns = [
    path('', views.dashboard, name='dashboard'),
    path('start-scraper/', views.start_scraper, name='start_scraper'),
    path('get-latest-data/', views.get_latest_data, name='get_latest_data'),
]