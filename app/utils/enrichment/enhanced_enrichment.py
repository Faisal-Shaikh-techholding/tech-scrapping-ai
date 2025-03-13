#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Enhanced Data Enrichment Module

This module provides expanded functionality for enriching company data
with targeted field enrichment and record status tracking.
"""

import pandas as pd
from typing import Dict, Any, List, Tuple, Optional
import asyncio
from app.utils.enrichment.progress_utils import should_stop_processing

# Status constants for enrichment
STATUS_COMPLETE = 'Complete'
STATUS_PARTIAL = 'Partial'
STATUS_INCOMPLETE = 'Incomplete'
STATUS_PENDING = 'Pending'
STATUS_FAILED = 'Failed'

# Target fields for enrichment
TARGET_FIELDS = [
    'CompanyFunding',
    'CEO',
    'CTO',
    'Directors',
    'CompanySize',
    'Industry',
    'LinkedIn',
    'Twitter',
    'YearFounded',
    'Headquarters'
]

def initialize_enrichment_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Initialize enrichment columns in the DataFrame.
    
    Args:
        df: DataFrame containing company data
        
    Returns:
        DataFrame with initialized enrichment columns
    """
    # Make a copy to avoid modifying the original
    enriched_df = df.copy()
    
    # Ensure validation status exists
    if 'ValidationStatus' not in enriched_df.columns:
        enriched_df['ValidationStatus'] = STATUS_PENDING
    
    # Add enrichment status columns if they don't exist
    if 'EnrichmentStatus' not in enriched_df.columns:
        enriched_df['EnrichmentStatus'] = STATUS_PENDING
    
    if 'EnrichmentSource' not in enriched_df.columns:
        enriched_df['EnrichmentSource'] = None
    
    if 'EnrichmentNotes' not in enriched_df.columns:
        enriched_df['EnrichmentNotes'] = ''
    
    # Add target fields if they don't exist
    for field in TARGET_FIELDS:
        if field not in enriched_df.columns:
            enriched_df[field] = None
    
    return enriched_df

async def enrich_company_details(
    df: pd.DataFrame,
    apollo_service=None,
    crunchbase_service=None,
    web_scraper=None,
    web_search_service=None,
    progress_tracker=None
) -> pd.DataFrame:
    """
    Enrich company details for records with complete names and URLs.
    
    Args:
        df: DataFrame with validated company data
        apollo_service: Apollo service instance
        crunchbase_service: Crunchbase service instance
        web_scraper: Web scraper service
        web_search_service: Web search service for finding company websites
        progress_tracker: Progress tracker object
        
    Returns:
        DataFrame with enriched company data
    """
    # Print debug info
    print(f"Starting enrichment process...")
    print(f"DataFrame shape: {df.shape}")
    print(f"DataFrame columns: {df.columns.tolist()}")
    
    if df.empty:
        print("Error: DataFrame is empty")
        return df
    
    # Make a copy to avoid modifying the original
    enriched_df = initialize_enrichment_columns(df.copy())
    
    # Check if processing should be stopped
    if should_stop_processing():
        print("Enrichment process cancelled by user")
        return enriched_df
    
    # Debug info for enrichment columns
    print(f"After initialization, columns: {enriched_df.columns.tolist()}")
    
    # Debug service info
    print(f"Apollo service available: {apollo_service is not None}")
    print(f"Crunchbase service available: {crunchbase_service is not None}")
    print(f"Web scraper available: {web_scraper is not None}")
    print(f"Web search service available: {web_search_service is not None}")
    
    # Step 1: Find missing websites for companies with just names
    if web_search_service:
        print("Looking for missing company websites...")
        
        # Check if processing should be stopped
        if should_stop_processing():
            print("Website discovery cancelled by user")
            return enriched_df
            
        # Get companies with names but no websites
        missing_website_mask = (
            (enriched_df['Company'].notna()) & 
            (enriched_df['Company'] != '') & 
            (enriched_df['CompanyWebsite'].isna() | (enriched_df['CompanyWebsite'] == ''))
        )
        
        missing_website_count = missing_website_mask.sum()
        print(f"Found {missing_website_count} companies missing websites")
        
        if missing_website_count > 0:
            # Extract the subset of data that needs websites
            website_subset = enriched_df[missing_website_mask].copy()
            
            # Limit to a reasonable number of companies to process (max 50)
            max_companies = min(50, len(website_subset))
            if len(website_subset) > max_companies:
                print(f"Limiting website discovery to {max_companies} companies to save time")
                website_subset = website_subset.head(max_companies)
            
            # Define a progress update function that also checks for stop requests
            def website_progress_update(progress, current, total, found_count):
                # Check for stop request
                if should_stop_processing():
                    return True  # Signal to stop
                    
                if progress_tracker:
                    message = f"Finding company websites: {current}/{total} ({found_count} found)"
                    progress_tracker['update'](
                        progress * 0.25,  # Scale to 25% of total progress
                        current,
                        total,
                        found_count,
                        message=message
                    )
                return False  # Continue processing
            
            # Find websites for these companies
            try:
                website_result, websites_found = web_search_service.bulk_find_company_websites(
                    website_subset,
                    update_callback=website_progress_update
                )
                
                # Update the main DataFrame with found websites
                if websites_found > 0:
                    print(f"Found {websites_found} company websites")
                    # Update only the rows that were processed
                    for idx, row in website_result.iterrows():
                        if pd.notna(row['CompanyWebsite']) and row['CompanyWebsite']:
                            enriched_df.at[idx, 'CompanyWebsite'] = row['CompanyWebsite']
                            enriched_df.at[idx, 'WebsiteSource'] = 'Domain Discovery'
                            
                            # If the validation status was incomplete due to missing website,
                            # update it to complete now that we have a website
                            if enriched_df.at[idx, 'ValidationStatus'] == 'Incomplete':
                                enriched_df.at[idx, 'ValidationStatus'] = 'Complete'
                                enriched_df.at[idx, 'ValidationNotes'] = 'Website found via domain discovery'
            except Exception as e:
                print(f"Error during website discovery: {str(e)}")
    
    # Check if processing should be stopped
    if should_stop_processing():
        print("Enrichment process cancelled by user")
        return enriched_df
    
    # Only process records with complete validation
    eligible_records = enriched_df[
        (enriched_df['ValidationStatus'] == 'Complete') & 
        (enriched_df['EnrichmentStatus'] == STATUS_PENDING)
    ].index.tolist()
    
    total_eligible = len(eligible_records)
    print(f"Total eligible records for enrichment: {total_eligible}")
    
    # Limit to a reasonable number of companies to process (max 50)
    max_companies = min(50, total_eligible)
    if total_eligible > max_companies:
        print(f"Limiting enrichment to {max_companies} companies to save time")
        eligible_records = eligible_records[:max_companies]
        total_eligible = max_companies
    
    # Debug validation info
    if 'ValidationStatus' in enriched_df.columns:
        validation_counts = enriched_df['ValidationStatus'].value_counts()
        print(f"ValidationStatus distribution: {validation_counts.to_dict()}")
    else:
        print("Error: ValidationStatus column missing")
    
    # Debug enrichment status info
    if 'EnrichmentStatus' in enriched_df.columns:
        enrichment_counts = enriched_df['EnrichmentStatus'].value_counts()
        print(f"EnrichmentStatus distribution: {enrichment_counts.to_dict()}")
    else:
        print("Error: EnrichmentStatus column missing")
    
    if total_eligible == 0:
        print("No eligible records found for enrichment. Check validation status and enrichment status.")
        return enriched_df
    
    for i, idx in enumerate(eligible_records):
        # Check if processing should be stopped
        if should_stop_processing():
            print("Enrichment process cancelled by user")
            break
            
        row = enriched_df.loc[idx]
        company_name = row.get('Company', '')
        company_url = row.get('CompanyWebsite', '')
        
        print(f"Enriching {i+1}/{total_eligible}: {company_name} ({company_url})")
        
        # Skip if we don't have both name and URL
        if not company_name or not company_url:
            enriched_df.at[idx, 'EnrichmentStatus'] = STATUS_FAILED
            enriched_df.at[idx, 'EnrichmentNotes'] = 'Missing required company information'
            continue
        
        # Convert row to dict for processing
        company_data = row.to_dict()
        enrichment_source = None
        
        # Step 1: Try Apollo
        if apollo_service:
            try:
                result = await apollo_service.enrich_company_details(company_name, company_url)
                if result:
                    # Update company data with Apollo results
                    for key, value in result.items():
                        if key in enriched_df.columns and value:
                            company_data[key] = value
                    
                    enrichment_source = 'Apollo'
            except Exception as e:
                print(f"Error in Apollo enrichment: {str(e)}")
        
        # Check if processing should be stopped after Apollo
        if should_stop_processing():
            print("Enrichment process cancelled by user after Apollo")
            break
        
        # Step 2: Try Crunchbase if some fields are still missing
        if crunchbase_service and is_missing_target_fields(company_data):
            try:
                result = await crunchbase_service.enrich_company_details(company_name, company_url)
                if result:
                    # Update only missing fields from Crunchbase
                    for key, value in result.items():
                        if key in enriched_df.columns and not company_data.get(key) and value:
                            company_data[key] = value
                    
                    enrichment_source = enrichment_source or 'Crunchbase'
                    if enrichment_source != 'Crunchbase':
                        enrichment_source = f"{enrichment_source}, Crunchbase"
            except Exception as e:
                print(f"Error in Crunchbase enrichment: {str(e)}")
        
        # Check if processing should be stopped after Crunchbase
        if should_stop_processing():
            print("Enrichment process cancelled by user after Crunchbase")
            break
        
        # Step 3: Web scraping as a fallback
        if web_scraper and is_missing_target_fields(company_data):
            try:
                # Create data object for scraping
                scrape_data = {
                    'Company': company_name,
                    'CompanyWebsite': company_url
                }
                
                # Call the correct method with correct parameters
                # Since enrich_company_data is not async, wrap it in a regular function call
                result = web_scraper.enrich_company_data(scrape_data)
                
                if result:
                    # Update only missing fields from web scraping
                    for key, value in result.items():
                        if key in enriched_df.columns and not company_data.get(key) and value:
                            company_data[key] = value
                    
                    enrichment_source = enrichment_source or 'Web Scraping'
                    if enrichment_source != 'Web Scraping':
                        enrichment_source = f"{enrichment_source}, Web Scraping"
            except Exception as e:
                print(f"Error in web scraping enrichment: {str(e)}")
        
        # Update DataFrame with enriched data
        for key, value in company_data.items():
            if key in enriched_df.columns:
                enriched_df.at[idx, key] = value
        
        # Determine enrichment status
        enrichment_status = determine_enrichment_status(company_data)
        
        enriched_df.at[idx, 'EnrichmentStatus'] = enrichment_status
        enriched_df.at[idx, 'EnrichmentSource'] = enrichment_source or 'None'
        
        if enrichment_status == STATUS_COMPLETE:
            enriched_df.at[idx, 'EnrichmentNotes'] = f'Fully enriched via {enrichment_source}'
        elif enrichment_status == STATUS_PARTIAL:
            missing_fields = get_missing_fields(company_data)
            enriched_df.at[idx, 'EnrichmentNotes'] = f'Partially enriched via {enrichment_source}. Missing: {", ".join(missing_fields)}'
        else:
            enriched_df.at[idx, 'EnrichmentNotes'] = f'Failed to enrich key fields via {enrichment_source or "all sources"}'
        
        # Update progress
        if progress_tracker:
            progress = (i + 1) / total_eligible
            current_item = i + 1
            message = f"Enriched {company_name} - {enrichment_status}"
            
            progress_tracker['update'](
                progress,
                current_item,
                total_eligible,
                success_count=(enriched_df['EnrichmentStatus'] == STATUS_COMPLETE).sum(),
                message=message
            )
        
        # Add a delay between companies to avoid rate limits
        if i < len(eligible_records) - 1:
            await asyncio.sleep(0.5)
    
    return enriched_df

def determine_enrichment_status(company_data: Dict[str, Any]) -> str:
    """
    Determine the enrichment status based on which fields are populated.
    
    Args:
        company_data: Dictionary containing company data
        
    Returns:
        Enrichment status string
    """
    # Check if all target fields are present
    critical_fields = ['CompanyFunding', 'CEO']
    target_fields_present = sum(1 for field in TARGET_FIELDS if company_data.get(field))
    critical_fields_present = sum(1 for field in critical_fields if company_data.get(field))
    
    if target_fields_present == len(TARGET_FIELDS):
        return STATUS_COMPLETE
    elif target_fields_present >= len(TARGET_FIELDS) // 2 or critical_fields_present == len(critical_fields):
        return STATUS_PARTIAL
    else:
        return STATUS_INCOMPLETE

def is_missing_target_fields(company_data: Dict[str, Any]) -> bool:
    """
    Check if the company data is missing any target fields.
    
    Args:
        company_data: Dictionary containing company data
        
    Returns:
        True if any target fields are missing, False otherwise
    """
    return any(not company_data.get(field) for field in TARGET_FIELDS)

def get_missing_fields(company_data: Dict[str, Any]) -> List[str]:
    """
    Get a list of target fields that are missing from the company data.
    
    Args:
        company_data: Dictionary containing company data
        
    Returns:
        List of missing field names
    """
    return [field for field in TARGET_FIELDS if not company_data.get(field)]

def get_enrichment_statistics(df: pd.DataFrame) -> Dict[str, int]:
    """
    Get statistics on enrichment status.
    
    Args:
        df: DataFrame with enriched company data
        
    Returns:
        Dictionary with enrichment statistics
    """
    total = len(df)
    complete = (df['EnrichmentStatus'] == STATUS_COMPLETE).sum()
    partial = (df['EnrichmentStatus'] == STATUS_PARTIAL).sum()
    incomplete = (df['EnrichmentStatus'] == STATUS_INCOMPLETE).sum()
    failed = (df['EnrichmentStatus'] == STATUS_FAILED).sum()
    pending = (df['EnrichmentStatus'] == STATUS_PENDING).sum()
    
    return {
        'total': total,
        'complete': complete,
        'partial': partial,
        'incomplete': incomplete,
        'failed': failed,
        'pending': pending,
        'enriched_percent': round((complete + partial) / total * 100, 1) if total > 0 else 0
    } 