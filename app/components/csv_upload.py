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
from typing import Tuple
from app.utils.session_state import go_to_step

def render_csv_upload():
    """Render the CSV upload interface."""
    
    st.header("Step 1: Upload Data File")
    
    st.markdown("""
    Upload a CSV or Excel file containing company data. The system will analyze the file
    and extract relevant information for further processing.
    
    **Supported formats:**
    - CSV files with headers
    - Excel files (.xlsx, .xls)
    
    **Required information:**
    - Company names
    - Website URLs (optional but recommended for enrichment)
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
                with st.expander("Preview Raw Data"):
                    st.dataframe(df.head(5))
                
                # Validate the data
                with st.spinner("Validating data..."):
                    is_valid, messages = validate_csv_data(df)
                
                # Only show error messages
                for message in messages:
                    if not is_valid and message.startswith("Error:"):
                        st.error(message)
                
                # Processing button
                if is_valid:
                    if st.button("Process Data", key="process_data_btn"):
                        with st.spinner("Processing data..."):
                            # Extract and clean data
                            processed_df = clean_data(df)
                            
                            # Store processed data in single data variable
                            st.session_state.data = processed_df
                            
                            print("Data processed successfully")
                            
                            # Navigate to view step
                            go_to_step("view")
                            st.rerun()
        except Exception as e:
            st.error(f"Error processing file: {str(e)}")
            print(f"Error processing uploaded file: {str(e)}")
    else:
        # Show instructions when no file is uploaded
        st.info("Please upload a CSV or Excel file to continue.")
        
        # Display file format information
        with st.expander("File Format Information"):
            st.markdown("""
            ### Expected file format
            
            Your file should contain columns with information about companies.
            The system will try to automatically identify the following information:
            
            - **Company**: Company or organization name
            - **Website**: Company website URL
            - **Industry**: Industry or sector (optional)
            - **Description**: Company description (optional)
            
            #### Example format:
            
            | Company | Website | Industry | Description |
            |---------|---------|----------|-------------|
            | Acme Inc. | acme.com | Manufacturing | A leading manufacturer of... |
            | XYZ Corp | xyz.com | Technology | Technology solutions provider... |
            
            The system is flexible and can work with different column names and formats.
            """)

def _read_file(file) -> Tuple[pd.DataFrame, str]:
    """
    Read a CSV or Excel file into a pandas DataFrame.
    
    Args:
        file: Uploaded file object
        
    Returns:
        Tuple[pd.DataFrame, str]: DataFrame and error message (if any)
    """
    df = pd.DataFrame()
    error_message = ""
    
    try:
        # Get file extension
        file_name = file.name
        file_extension = file_name.split('.')[-1].lower()
        
        # Reset file pointer
        file.seek(0)
        
        if file_extension == 'csv':
            # Try different encodings for CSV
            try:
                df = pd.read_csv(file, encoding='utf-8')
            except UnicodeDecodeError:
                file.seek(0)
                df = pd.read_csv(file, encoding='latin1')
                
        elif file_extension in ['xlsx', 'xls']:
            # Read Excel file
            df = pd.read_excel(file)
        else:
            error_message = f"Unsupported file format: {file_extension}"
            
        # Check if DataFrame is empty
        if df.empty:
            error_message = "The uploaded file is empty or could not be read properly."
            
    except Exception as e:
        error_message = f"Error reading file: {str(e)}"
        print(f"Error reading uploaded file: {str(e)}")
    
    return df, error_message

def validate_csv_data(df) -> Tuple[bool, list]:
    """
    Validate the CSV data to ensure it contains the required columns.
    
    Args:
        df (pd.DataFrame): DataFrame to validate
        
    Returns:
        Tuple[bool, list]: Validation result and list of messages
    """
    messages = []
    is_valid = True
    
    # Check for minimum required columns
    if len(df.columns) < 1:
        is_valid = False
        messages.append("Error: The file must contain at least one column.")
    
    # Try to identify company column
    company_columns = [col for col in df.columns if any(kw in col.lower() for kw in 
                      ['company', 'organization', 'org', 'business', 'name'])]
    
    if not company_columns:
        messages.append("Error: Could not identify a company name column. Please ensure your data includes company information.")
        is_valid = is_valid and False
    
    # Check for website column
    website_columns = [col for col in df.columns if any(kw in col.lower() for kw in 
                      ['website', 'url', 'domain', 'site', 'web'])]
    
    if not website_columns:
        messages.append("Warning: Website column not found. Website URLs are recommended for enrichment.")
    
    return is_valid, messages

def clean_data(df) -> pd.DataFrame:
    """
    Clean and standardize the data.
    
    Args:
        df (pd.DataFrame): Raw DataFrame
        
    Returns:
        pd.DataFrame: Cleaned DataFrame
    """
    # Make a copy to avoid modifying the original
    cleaned_df = df.copy()
    
    # Convert all column names to lowercase
    cleaned_df.columns = [col.lower().strip() for col in cleaned_df.columns]
    
    # Map common column names to standard names
    column_mapping = {
        'name': 'company',
        'company name': 'company',
        'organization': 'company',
        'business': 'company',
        'url': 'website',
        'website url': 'website',
        'domain': 'website',
        'homepage': 'website',
        'description': 'company_description',
        'about': 'company_description',
        'company description': 'company_description'
    }
    
    # Rename columns if they match our mapping
    for old_name, new_name in column_mapping.items():
        if old_name in cleaned_df.columns and new_name not in cleaned_df.columns:
            cleaned_df = cleaned_df.rename(columns={old_name: new_name})
    
    # Ensure company column exists
    if 'company' not in cleaned_df.columns:
        if len(cleaned_df.columns) > 0:
            # Use the first column as company name if not identified
            cleaned_df = cleaned_df.rename(columns={cleaned_df.columns[0]: 'company'})
            print(f"No company column identified. Using {cleaned_df.columns[0]} as company names.")
    
    # Ensure website column exists
    if 'website' not in cleaned_df.columns:
        cleaned_df['website'] = ''
    
    # Add enrichment status column
    cleaned_df['enrichment_status'] = 'Pending'
    
    # Clean data
    cleaned_df = cleaned_df.replace('', np.nan)
    
    # Drop rows where company is NaN
    if 'company' in cleaned_df.columns:
        cleaned_df = cleaned_df.dropna(subset=['company'])
    
    # Reset index
    cleaned_df = cleaned_df.reset_index(drop=True)
    
    return cleaned_df 