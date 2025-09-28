# dashboard/models.py
from django.db import models

class LienData(models.Model):
    direct_party_debtor = models.CharField(max_length=255)
    reverse_party_claimant = models.CharField(max_length=255)
    address = models.TextField()
    zipcode = models.CharField(max_length=10, blank=True, null=True)
    total_due = models.CharField(max_length=50, blank=True, null=True)
    county = models.CharField(max_length=100, blank=True, null=True)
    instrument = models.CharField(max_length=50, blank=True, null=True)
    date_filed = models.CharField(max_length=50, blank=True, null=True)
    book = models.CharField(max_length=50, blank=True, null=True)
    page = models.CharField(max_length=50, blank=True, null=True)
    description = models.TextField(blank=True, null=True)
    pdf_document_url = models.URLField(blank=True, null=True)
    pdf_file = models.CharField(max_length=255, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    
    def __str__(self):
        return f"{self.direct_party_debtor} - {self.county}"

class RealEstateData(models.Model):
    search_name = models.CharField(max_length=255)
    entity_index = models.IntegerField()
    doc_index = models.IntegerField()
    pdf_viewer = models.TextField(db_column='PDF_viewer', blank=True, null=True)  
    realestate_pdf = models.TextField(db_column='RealEstate_PDF', blank=True, null=True) 
    created_at = models.DateTimeField(auto_now_add=True)
    
    def __str__(self):
        return f"{self.search_name} - Doc {self.doc_index}"