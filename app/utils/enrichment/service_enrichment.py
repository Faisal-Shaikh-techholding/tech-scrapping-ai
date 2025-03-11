#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Service-Specific Enrichment

This module contains functions for performing enrichment using specific services.
"""

import streamlit as st
from app.utils.enrichment.progress_utils import should_stop_processing

def perform_apollo_enrichment(df, apollo_service, progress_tracker):
    """
    Perform enrichment using Apollo.io API.
    
    Args:
        df: DataFrame containing company data
        apollo_service: Apollo service instance
        progress_tracker: Progress tracker object
        
    Returns:
        Enriched DataFrame, success count, total count
    """
    total_companies = len(df)
    success_count = 0
    
    print("Starting Apollo enrichment.")
    
    for idx, row in df.iterrows():
        # Check if processing should be stopped
        if should_stop_processing():
            print("Apollo enrichment stopped by user")
            break
            
        current_company = idx + 1
        company_name = row.get('Company', 'Unknown')
        
        # Skip if already successfully enriched
        if row.get('EnrichmentStatus') == 'Success':
            success_count += 1
            continue
        
        # Convert row to dict
        lead_data = row.to_dict()
        
        # Enrich with Apollo
        enriched_lead = apollo_service.enrich_lead(lead_data)
        
        # Update DataFrame with enriched data
        for key, value in enriched_lead.items():
            if key in df.columns:
                df.at[idx, key] = value
        
        # Update progress
        progress = current_company / total_companies
        if enriched_lead.get('EnrichmentStatus') == 'Success':
            success_count += 1
        
        progress_tracker['update'](progress, current_company, total_companies, success_count)
    
    # Update processing status
    st.session_state.processing_status['is_processing'] = False
    
    print("Apollo enrichment completed.")
    
    return df, success_count, total_companies

def perform_crunchbase_enrichment(df, crunchbase_service, progress_tracker):
    """
    Perform enrichment using Crunchbase API.
    
    Args:
        df: DataFrame containing company data
        crunchbase_service: Crunchbase service instance
        progress_tracker: Progress tracker object
        
    Returns:
        Enriched DataFrame, success count, total count
    """
    total_companies = len(df)
    success_count = 0
    
    print("Starting Crunchbase enrichment.")
    
    for idx, row in df.iterrows():
        # Check if processing should be stopped
        if should_stop_processing():
            print("Crunchbase enrichment stopped by user")
            break
            
        current_company = idx + 1
        company_name = row.get('Company', 'Unknown')
        
        # Skip if already successfully enriched
        if row.get('EnrichmentStatus') == 'Success':
            success_count += 1
            continue
        
        # Convert row to dict
        company_data = row.to_dict()
        
        # Enrich with Crunchbase
        enriched_company = crunchbase_service.enrich_company(company_data)
        
        # Update DataFrame with enriched data
        for key, value in enriched_company.items():
            if key in df.columns:
                df.at[idx, key] = value
        
        # Update progress
        progress = current_company / total_companies
        if enriched_company.get('EnrichmentStatus') == 'Success':
            success_count += 1
        
        progress_tracker['update'](progress, current_company, total_companies, success_count)
    
    # Update processing status
    st.session_state.processing_status['is_processing'] = False
    
    print("Crunchbase enrichment completed.")
    
    return df, success_count, total_companies

def perform_web_scraping(df, scraper, scrape_options, progress_tracker):
    """
    Perform enrichment using web scraping.
    
    Args:
        df: DataFrame containing company data
        scraper: Web scraper service instance
        scrape_options: List of options for web scraping
        progress_tracker: Progress tracker object
        
    Returns:
        Enriched DataFrame, success count, total count
    """
    total_companies = len(df)
    success_count = 0
    
    print("Starting web scraping.")
    
    for idx, row in df.iterrows():
        # Check if processing should be stopped
        if should_stop_processing():
            print("Web scraping stopped by user")
            break
            
        current_company = idx + 1
        company_name = row.get('Company', 'Unknown')
        website = row.get('CompanyWebsite', '')
        
        # Skip if no website or already successfully enriched
        if not website or row.get('EnrichmentStatus') == 'Success':
            if row.get('EnrichmentStatus') == 'Success':
                success_count += 1
            progress = current_company / total_companies
            progress_tracker['update'](progress, current_company, total_companies, success_count)
            continue
        
        try:
            # Create a copy of the data for web scraping
            scrape_data = {
                'Company': company_name,
                'CompanyWebsite': website
            }
            
            # Enrich with web scraping
            enriched_data = scraper.enrich_company_data(scrape_data)
            
            # Update DataFrame with enriched data
            if enriched_data:
                for key, value in enriched_data.items():
                    if key in df.columns and value:
                        df.at[idx, key] = value
                
                # Mark as successful if we got any useful data
                if any(value for key, value in enriched_data.items() 
                      if key not in ['Company', 'CompanyWebsite', 'EnrichmentStatus']):
                    df.at[idx, 'EnrichmentStatus'] = 'Success'
                    df.at[idx, 'EnrichmentSource'] = 'Web Scraping'
                    df.at[idx, 'EnrichmentNotes'] = 'Data enriched via web scraping'
                    success_count += 1
            
            # Log the scraped data for each company
            print(f"Scraped data for {company_name}: {enriched_data}")
        except Exception as e:
            print(f"Error scraping {company_name}: {str(e)}")
            df.at[idx, 'EnrichmentNotes'] = f"Scraping error: {str(e)}"
        
        # Update progress
        progress = current_company / total_companies
        progress_tracker['update'](progress, current_company, total_companies, success_count)
    
    # Update processing status
    st.session_state.processing_status['is_processing'] = False
    
    print("Web scraping completed.")
    
    return df, success_count, total_companies 