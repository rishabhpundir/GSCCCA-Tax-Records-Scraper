# dashboard/admin.py
from django.contrib import admin
from .models import LienData, RealEstateData

@admin.register(LienData)
class LienDataAdmin(admin.ModelAdmin):
    list_display = ('direct_party_debtor', 'reverse_party_claimant', 'county', 'date_filed', 'total_due')
    list_filter = ('county', 'date_filed')
    search_fields = ('direct_party_debtor', 'reverse_party_claimant', 'address')
    readonly_fields = ('created_at',)

@admin.register(RealEstateData)
class RealEstateDataAdmin(admin.ModelAdmin):
    list_display = ('search_name', 'entity_index', 'doc_index', 'created_at')
    list_filter = ('entity_index', 'created_at')
    search_fields = ('search_name', 'final_url')
    readonly_fields = ('created_at',)