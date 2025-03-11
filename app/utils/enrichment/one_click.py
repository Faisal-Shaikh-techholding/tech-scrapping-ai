#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
One-Click Enrichment

This module contains functions for performing one-click enrichment using
multiple services in sequence.
"""

import streamlit as st
import pandas as pd
from app.utils.enrichment_utils import enrich_company_data
from app.utils.enrichment.progress_utils import should_stop_processing

def perform_one_click_enrichment(df, services, scraper, scrape_options, progress_tracker):
    """
    Perform one-click enrichment on all companies in the dataframe.
    
    Args:
        df: DataFrame containing company data
        services: List of tuples (service_name, service_instance)
        scraper: Web scraper service instance
        scrape_options: List of options for web scraping
        progress_tracker: Progress tracker object
        
    Returns:
        Enriched DataFrame
    """
    print("Starting one-click enrichment.")
    # Process each company
    total_companies = len(df)
    success_count = 0
    error_count = 0
    
    for idx, row in df.iterrows():
        # Check if processing should be stopped
        if should_stop_processing():
            print("One-click enrichment stopped by user")
            break
            
        current_company = idx + 1
        company_name = row.get('Company', 'Unknown')
        
        # Skip if already successfully enriched
        if row.get('EnrichmentStatus') == 'Success':
            success_count += 1
            progress = current_company / total_companies
            progress_tracker['update'](
                progress, 
                current_company, 
                total_companies, 
                success_count, 
                message=f"Skipped {company_name} - already enriched"
            )
            continue
        
        # Convert row to dict
        lead_data = row.to_dict()
        
        # Use the utility function to enrich the company data
        enriched_lead = enrich_company_data(
            lead_data, 
            services, 
            scraper=scraper, 
            scrape_options=scrape_options
        )
        
        # Update DataFrame with enriched data
        for key, value in enriched_lead.items():
            if key in df.columns:
                df.at[idx, key] = value
        
        # Update progress
        progress = current_company / total_companies
        if enriched_lead.get('EnrichmentStatus') == 'Success':
            success_count += 1
            progress_tracker['update'](
                progress, 
                current_company, 
                total_companies, 
                success_count, 
                message=f"Enriched {company_name} with {enriched_lead.get('EnrichmentSource', 'Unknown')}"
            )
        elif enriched_lead.get('EnrichmentStatus') == 'Cancelled':
            progress_tracker['update'](
                progress, 
                current_company, 
                total_companies, 
                success_count, 
                error_count,
                f"Cancelled enrichment for {company_name}"
            )
            break
        else:
            error_count += 1
            progress_tracker['update'](
                progress, 
                current_company, 
                total_companies, 
                success_count, 
                error_count,
                f"Failed to enrich {company_name}: {enriched_lead.get('EnrichmentNotes', '')}"
            )
    
    # Update processing status
    st.session_state.processing_status['is_processing'] = False
    
    print("One-click enrichment completed.")
    return df, success_count, total_companies 