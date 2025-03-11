#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Data Processing Utilities

This module contains functions for processing, extracting, and validating CSV data.
"""

import pandas as pd
import re
import logging
from typing import Dict, List, Tuple, Optional, Union

logger = logging.getLogger('csv_processor')

def validate_csv_data(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    """
    Validate the uploaded CSV data to ensure it has the required columns
    or contains data that can be properly extracted.
    
    Args:
        df: Pandas DataFrame containing the uploaded CSV data
        
    Returns:
        Tuple containing:
            - Boolean indicating if validation passed
            - List of validation messages
    """
    messages = []
    
    # Check if DataFrame is empty
    if df.empty:
        messages.append("The uploaded file contains no data.")
        return False, messages
    
    # Check for minimum columns
    if len(df.columns) < 2:
        messages.append("The file must contain at least 2 columns of data.")
        return False, messages
    
    # Look for company/organization name column
    company_columns = [col for col in df.columns if any(kw in col.lower() for kw in 
                      ['company', 'organization', 'org', 'business', 'firm'])]
    
    if not company_columns:
        messages.append("No column for company/organization name found. Please ensure your CSV contains company information.")
        return False, messages
    else:
        messages.append(f"Found company column(s): {', '.join(company_columns)}")
    
    # Look for URL/website column
    url_columns = [col for col in df.columns if any(kw in col.lower() for kw in 
                  ['url', 'website', 'link', 'web', 'site'])]
    
    if url_columns:
        messages.append(f"Found URL column(s): {', '.join(url_columns)}")
    
    # Look for industry column
    industry_columns = [col for col in df.columns if any(kw in col.lower() for kw in 
                       ['industry', 'industries', 'sector', 'vertical', 'category'])]
    
    if industry_columns:
        messages.append(f"Found industry column(s): {', '.join(industry_columns)}")
    
    # Check if there's data in the company column
    if company_columns:
        company_col = company_columns[0]
        if df[company_col].notna().sum() == 0:
            messages.append(f"No company data found in column '{company_col}'")
            return False, messages
        else:
            messages.append(f"Found {df[company_col].notna().sum()} companies with data")
    
    # Check row count
    row_count = len(df)
    messages.append(f"Total rows found: {row_count}")
    
    return True, messages

def extract_names_companies(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract company information from the DataFrame.
    
    Args:
        df: Pandas DataFrame containing the raw CSV data
        
    Returns:
        Processed DataFrame with standardized columns
    """
    logger.info("Extracting company information from DataFrame")
    
    # Create a new DataFrame to store processed data
    processed_df = pd.DataFrame()
    
    # Find company name column
    company_columns = [col for col in df.columns if any(kw in col.lower() for kw in 
                      ['company', 'organization', 'org', 'business', 'firm'])]
    
    if company_columns:
        company_col = company_columns[0]
        processed_df['Company'] = df[company_col]
        logger.info(f"Using '{company_col}' as company name column")
    else:
        # If no obvious company column, use the first text column
        text_columns = df.select_dtypes(include=['object']).columns
        if len(text_columns) > 0:
            processed_df['Company'] = df[text_columns[0]]
            logger.info(f"No clear company column found. Using '{text_columns[0]}' as fallback")
        else:
            processed_df['Company'] = None
            logger.warning("No suitable column found for company name")
    
    # Extract URLs if available
    url_columns = [col for col in df.columns if any(kw in col.lower() for kw in 
                  ['url', 'website', 'link', 'web', 'site'])]
    
    if url_columns:
        url_col = url_columns[0]
        processed_df['CompanyWebsite'] = df[url_col]
        logger.info(f"Using '{url_col}' as website URL column")
    else:
        processed_df['CompanyWebsite'] = None
    
    # Extract industry if available
    industry_columns = [col for col in df.columns if any(kw in col.lower() for kw in 
                       ['industry', 'industries', 'sector', 'vertical', 'category'])]
    
    if industry_columns:
        industry_col = industry_columns[0]
        processed_df['Industry'] = df[industry_col]
        logger.info(f"Using '{industry_col}' as industry column")
    else:
        processed_df['Industry'] = None
    
    # Add source information
    processed_df['DataSource'] = 'CSV Import'
    
    logger.info(f"Extracted information for {len(processed_df)} companies")
    
    return processed_df

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the data by removing duplicates, handling missing values,
    and standardizing formats.
    
    Args:
        df (pandas.DataFrame): DataFrame to clean
        
    Returns:
        pandas.DataFrame: Cleaned DataFrame
    """
    # Create a copy
    cleaned_df = df.copy()
    
    # Remove duplicate rows
    initial_rows = len(cleaned_df)
    cleaned_df = cleaned_df.drop_duplicates()
    if len(cleaned_df) < initial_rows:
        logger.info(f"Removed {initial_rows - len(cleaned_df)} duplicate rows")
    
    # Handle missing values for critical columns
    cleaned_df['Company'] = cleaned_df['Company'].fillna('').astype(str).str.strip()
    
    # Remove rows where company is empty
    cleaned_df = cleaned_df[cleaned_df['Company'] != '']
    
    return cleaned_df

def prepare_for_enrichment(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare data for enrichment by adding necessary columns for tracking
    enrichment status and notes.
    
    Args:
        df: Pandas DataFrame containing the processed data
        
    Returns:
        DataFrame ready for enrichment
    """
    logger.info("Preparing data for enrichment")
    
    # Create a copy to avoid modifying the original
    enrichment_df = df.copy()
    
    # Add enrichment status columns
    if 'EnrichmentStatus' not in enrichment_df.columns:
        enrichment_df['EnrichmentStatus'] = 'Pending'
    
    if 'EnrichmentNotes' not in enrichment_df.columns:
        enrichment_df['EnrichmentNotes'] = ''
    
    if 'EnrichmentSource' not in enrichment_df.columns:
        enrichment_df['EnrichmentSource'] = None
    
    # Add placeholder columns for enriched data
    if 'CompanyDescription' not in enrichment_df.columns:
        enrichment_df['CompanyDescription'] = None
    
    if 'CompanySize' not in enrichment_df.columns:
        enrichment_df['CompanySize'] = None
    
    if 'CompanyLocation' not in enrichment_df.columns:
        enrichment_df['CompanyLocation'] = None
    
    if 'CompanyFounded' not in enrichment_df.columns:
        enrichment_df['CompanyFounded'] = None
    
    if 'CompanySocialLinks' not in enrichment_df.columns:
        enrichment_df['CompanySocialLinks'] = None
        
    if 'JobTitle' not in enrichment_df.columns:
        enrichment_df['JobTitle'] = None
    
    # Add new columns for tech leadership and funding
    if 'TechLeadership' not in enrichment_df.columns:
        enrichment_df['TechLeadership'] = None
    
    if 'TechJobListings' not in enrichment_df.columns:
        enrichment_df['TechJobListings'] = None
    
    if 'CompanyFunding' not in enrichment_df.columns:
        enrichment_df['CompanyFunding'] = None
    
    if 'CompanyFundingAmount' not in enrichment_df.columns:
        enrichment_df['CompanyFundingAmount'] = None
    
    if 'CompanyLatestFundingDate' not in enrichment_df.columns:
        enrichment_df['CompanyLatestFundingDate'] = None
    
    if 'CompanyLatestFundingStage' not in enrichment_df.columns:
        enrichment_df['CompanyLatestFundingStage'] = None
    
    if 'CompanyTechnology' not in enrichment_df.columns:
        enrichment_df['CompanyTechnology'] = None
    
    if 'CompanyTwitter' not in enrichment_df.columns:
        enrichment_df['CompanyTwitter'] = None
    
    if 'CompanyFacebook' not in enrichment_df.columns:
        enrichment_df['CompanyFacebook'] = None
    
    if 'CompanyPhone' not in enrichment_df.columns:
        enrichment_df['CompanyPhone'] = None
    
    if 'EngineeringHeadcount' not in enrichment_df.columns:
        enrichment_df['EngineeringHeadcount'] = None
    
    if 'ITHeadcount' not in enrichment_df.columns:
        enrichment_df['ITHeadcount'] = None
    
    if 'ProductHeadcount' not in enrichment_df.columns:
        enrichment_df['ProductHeadcount'] = None
    
    if 'DataScienceHeadcount' not in enrichment_df.columns:
        enrichment_df['DataScienceHeadcount'] = None
    
    logger.info(f"Prepared {len(enrichment_df)} companies for enrichment")
    
    return enrichment_df

def prepare_for_salesforce(df: pd.DataFrame) -> pd.DataFrame:
    """
    Map DataFrame columns to Salesforce fields and ensure all required fields
    are present. Also add flags for selected leads and SF submission status.
    
    Args:
        df: Pandas DataFrame containing the enriched data
        
    Returns:
        DataFrame ready for Salesforce submission
    """
    logger.info("Preparing data for Salesforce submission")
    
    # Create a copy to avoid modifying the original
    sf_df = df.copy()
    
    # Add selection column if not present
    if 'Selected' not in sf_df.columns:
        sf_df['Selected'] = True
    
    # Add SF submission status columns
    if 'SFStatus' not in sf_df.columns:
        sf_df['SFStatus'] = 'Pending'
    
    if 'SFNotes' not in sf_df.columns:
        sf_df['SFNotes'] = ''
    
    # For Account object instead of Lead
    if 'AccountName' not in sf_df.columns:
        sf_df['AccountName'] = sf_df['Company']
    
    # Add required fields for Salesforce
    if 'FirstName' not in sf_df.columns:
        sf_df['FirstName'] = 'Tech'  # Default first name
    
    if 'LastName' not in sf_df.columns:
        sf_df['LastName'] = sf_df['Company'].apply(lambda x: f"{x} Contact" if pd.notna(x) else "Unknown")
    
    if 'Email' not in sf_df.columns:
        # Try to extract email from tech leadership if available
        def extract_tech_email(row):
            if isinstance(row.get('TechLeadership'), list) and len(row.get('TechLeadership', [])) > 0:
                for leader in row['TechLeadership']:
                    if leader.get('email'):
                        return leader.get('email')
            return f"info@{row.get('CompanyWebsite', '').replace('http://', '').replace('https://', '').replace('www.', '').split('/')[0]}" if row.get('CompanyWebsite') else ''
        
        sf_df['Email'] = sf_df.apply(extract_tech_email, axis=1)
    
    if 'Phone' not in sf_df.columns:
        # Try to extract phone from tech leadership if available
        def extract_tech_phone(row):
            if isinstance(row.get('TechLeadership'), list) and len(row.get('TechLeadership', [])) > 0:
                for leader in row['TechLeadership']:
                    if leader.get('phone'):
                        return leader.get('phone')
            return row.get('CompanyPhone', '')
        
        sf_df['Phone'] = sf_df.apply(extract_tech_phone, axis=1)
    
    logger.info(f"Prepared {len(sf_df)} companies for Salesforce submission")
    
    return sf_df 