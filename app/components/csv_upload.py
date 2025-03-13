#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV Upload Component

This module renders the CSV upload interface and handles CSV parsing.
"""

import streamlit as st
import pandas as pd
import io
import numpy as np
from typing import Tuple, Dict, Optional, List
from app.utils.session_state import go_to_step

def render_csv_upload():
    """Render the CSV upload interface."""
    
    st.header("Step 1: Upload Data File")
    
    st.markdown("""
    Upload a CSV or Excel file containing company data. Our AI system will automatically analyze 
    the file structure, identify columns, and extract relevant information.
    
    **Supported formats:**
    - CSV files (.csv)
    - Excel files (.xlsx, .xls)
    
    **What happens next?**
    1. AI automatically maps columns to standard fields
    2. Missing company websites are discovered
    3. Company details are enriched with industry, size, etc.
    4. Data is prepared for Salesforce export
    
    **No manual column mapping required!**
    """)
    
    # File uploader
    uploaded_file = st.file_uploader("Choose a file", type=["csv", "xlsx", "xls"])
    
    # Process the uploaded file
    if uploaded_file is not None:
        try:
            with st.spinner("Reading file..."):
                df, error_message = _read_file(uploaded_file)
                
                if error_message:
                    st.error(error_message)
                    return
                
                # Display basic file info
                st.success(f"Upload successful! Found {len(df)} rows and {len(df.columns)} columns.")
                
                # Preview raw data
                with st.expander("Preview Raw Data", expanded=True):
                    st.dataframe(df.head(5))
                
                # Store the data in session state
                st.session_state.data = df
                
                # Proceed button
                if st.button("Continue to Next Step", type="primary"):
                    go_to_step("view")
                    st.rerun()
                
        except Exception as e:
            st.error(f"Error processing file: {str(e)}")
    else:
        # Show a placeholder for the data preview
        st.info("Please upload a CSV or Excel file to continue.")

def _read_file(uploaded_file) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Read and parse an uploaded file into a pandas DataFrame.
    
    Args:
        uploaded_file: Uploaded file object from st.file_uploader
        
    Returns:
        Tuple of (DataFrame or None, error message or None)
    """
    try:
        # Get file extension
        file_name = uploaded_file.name
        file_ext = file_name.split('.')[-1].lower()
        
        # Read file based on extension
        if file_ext == 'csv':
            df = pd.read_csv(uploaded_file)
        elif file_ext in ['xlsx', 'xls']:
            df = pd.read_excel(uploaded_file)
        else:
            return None, f"Unsupported file format: {file_ext}"
        
        # Basic validation
        if df.empty:
            return None, "The uploaded file is empty."
        
        # Return the DataFrame
        return df, None
    except Exception as e:
        return None, f"Error reading file: {str(e)}"

# -------------------------------- #
# The functions below are simplified or removed since AI will handle this now
# -------------------------------- #

def validate_csv_data(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    """
    Perform basic validation on CSV data - simplified for AI approach.
    
    Args:
        df: DataFrame to validate
        
    Returns:
        Tuple of (is_valid, list of validation messages)
    """
    messages = []
    
    # Check if there's any data
    if df.empty:
        messages.append("Error: The uploaded file is empty.")
        return False, messages
    
    # Basic row count check
    if len(df) > 5000:
        messages.append(f"Warning: Large file detected ({len(df)} rows). Processing may take longer.")
    
    # Success message
    messages.append(f"Success: File contains {len(df)} rows and {len(df.columns)} columns.")
    
    return True, messages

def apply_column_mapping(df: pd.DataFrame, column_mapping: Dict[str, str]) -> pd.DataFrame:
    """
    Apply column mapping to the DataFrame.
    
    Args:
        df: Original DataFrame with original column names
        column_mapping: Dictionary mapping application column names to original column names
        
    Returns:
        pd.DataFrame: New DataFrame with standardized column names
    """
    # Create a new DataFrame for the mapped data
    mapped_df = pd.DataFrame()
    
    # Copy data from original columns to standardized columns
    for app_column, original_column in column_mapping.items():
        if original_column in df.columns:
            mapped_df[app_column] = df[original_column]
    
    # Special handling for required columns if they're missing
    # (this shouldn't happen because the UI prevents it, but just in case)
    if 'Company' not in mapped_df.columns:
        mapped_df['Company'] = ''
    
    if 'CompanyWebsite' not in mapped_df.columns:
        mapped_df['CompanyWebsite'] = ''
    
    return mapped_df

def clean_data(df) -> pd.DataFrame:
    """
    Clean and standardize the data.
    
    Args:
        df (pd.DataFrame): Raw or mapped DataFrame
        
    Returns:
        pd.DataFrame: Cleaned DataFrame
    """
    # Make a copy to avoid modifying the original
    cleaned_df = df.copy()
    
    # If this is a mapped DataFrame, it should already have our required columns
    required_columns = ['Company', 'CompanyWebsite']
    has_required = all(col in cleaned_df.columns for col in required_columns)
    
    # If DataFrame doesn't have required columns, try to map them
    if not has_required:
        print("DataFrame doesn't have required columns. Attempting to map common column names.")
        # Convert all column names to lowercase for easier matching
        cleaned_df.columns = [col.lower().strip() for col in cleaned_df.columns]
        
        # Map common column names to standard names
        column_mapping = {
            'name': 'Company',
            'company name': 'Company',
            'company': 'Company', 
            'organization': 'Company',
            'business': 'Company',
            'url': 'CompanyWebsite',
            'website': 'CompanyWebsite',
            'website url': 'CompanyWebsite',
            'domain': 'CompanyWebsite',
            'homepage': 'CompanyWebsite',
            'description': 'Description',
            'about': 'Description',
            'company description': 'Description',
            'industry': 'Industry',
            'sector': 'Industry',
            'category': 'Industry'
        }
        
        # Rename columns if they match our mapping
        for old_name, new_name in column_mapping.items():
            if old_name in cleaned_df.columns and new_name not in cleaned_df.columns:
                cleaned_df = cleaned_df.rename(columns={old_name: new_name})
    
    # Ensure company column exists
    if 'Company' not in cleaned_df.columns:
        if len(cleaned_df.columns) > 0:
            # Use the first column as company name if not identified
            cleaned_df = cleaned_df.rename(columns={cleaned_df.columns[0]: 'Company'})
            print(f"No company column identified. Using {cleaned_df.columns[0]} as company names.")
    
    # Ensure website column exists
    if 'CompanyWebsite' not in cleaned_df.columns:
        cleaned_df['CompanyWebsite'] = ''
    
    # Add enrichment status column if it doesn't exist
    if 'ValidationStatus' not in cleaned_df.columns:
        cleaned_df['ValidationStatus'] = 'Pending'
    
    # Clean up website URLs
    if 'CompanyWebsite' in cleaned_df.columns:
        # Ensure URLs have http/https
        cleaned_df['CompanyWebsite'] = cleaned_df['CompanyWebsite'].apply(
            lambda x: f"https://{x}" if isinstance(x, str) and x and not x.startswith(('http://', 'https://')) else x
        )
    
    # Clean data
    cleaned_df = cleaned_df.replace('', np.nan)
    
    # Explicitly validate rows based on Company and CompanyWebsite
    cleaned_df['ValidationStatus'] = 'Incomplete'
    
    # Rows with either Company or CompanyWebsite are Complete
    mask_complete = cleaned_df['Company'].notna() | cleaned_df['CompanyWebsite'].notna()
    cleaned_df.loc[mask_complete, 'ValidationStatus'] = 'Complete'
    
    # Add validation notes
    cleaned_df['ValidationNotes'] = ''
    
    # Different validation notes based on what's present
    mask_both_present = cleaned_df['Company'].notna() & cleaned_df['CompanyWebsite'].notna()
    cleaned_df.loc[mask_both_present, 'ValidationNotes'] = 'Both Company Name and URL present'
    
    mask_only_company = cleaned_df['Company'].notna() & cleaned_df['CompanyWebsite'].isna()
    cleaned_df.loc[mask_only_company, 'ValidationNotes'] = 'Only Company Name present - Website will be fetched'
    
    mask_only_website = cleaned_df['Company'].isna() & cleaned_df['CompanyWebsite'].notna()
    cleaned_df.loc[mask_only_website, 'ValidationNotes'] = 'Only Website URL present - Company will be fetched'
    
    mask_missing_both = cleaned_df['Company'].isna() & cleaned_df['CompanyWebsite'].isna()
    cleaned_df.loc[mask_missing_both, 'ValidationNotes'] = 'Missing both Company Name and URL'
    
    # Drop rows where both company and website are NaN
    cleaned_df = cleaned_df.dropna(subset=['Company', 'CompanyWebsite'], how='all')
    
    # Add enrichment status column
    if 'EnrichmentStatus' not in cleaned_df.columns:
        cleaned_df['EnrichmentStatus'] = 'Pending'
    
    # Reset index
    cleaned_df = cleaned_df.reset_index(drop=True)
    
    print(f"Cleaned data: {len(cleaned_df)} rows. Validation status counts: {cleaned_df['ValidationStatus'].value_counts().to_dict()}")
    
    return cleaned_df 