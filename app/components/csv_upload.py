#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV Upload Component

This module renders the CSV upload interface and handles CSV parsing.
"""

import streamlit as st
import pandas as pd
import io
import logging
from typing import Tuple
from app.utils.session_state import go_to_step
from app.utils.data_processing import validate_csv_data, extract_names_companies, clean_data

logger = logging.getLogger('csv_processor')

def render_csv_upload():
    """Render the CSV upload interface."""
    
    st.header("Upload CSV File")
    
    st.markdown("""
    Upload a CSV file containing lead data. The system will analyze the file
    and extract relevant information for further processing.
    
    **Supported formats:**
    - CSV files with headers
    - Excel files (.xlsx, .xls)
    
    **Required information:**
    - Names (first name, last name, or full name)
    - Company information
    """)
    
    # File uploader
    uploaded_file = st.file_uploader("Choose a file", type=["csv", "xlsx", "xls"])
    
    # Show sample data option
    if st.checkbox("Use sample data instead", key="use_sample_data"):
        uploaded_file = _get_sample_data()
        st.success("Using sample data. You can proceed with processing.")
    
    # Process the uploaded file
    if uploaded_file is not None:
        try:
            with st.spinner("Reading file..."):
                df, error_message = _read_file(uploaded_file)
                
                if error_message:
                    st.error(error_message)
                    return
                
                # Display basic file info
                st.info(f"File loaded successfully: {len(df)} rows, {len(df.columns)} columns")
                
                # Preview raw data
                with st.expander("Preview Raw Data"):
                    st.dataframe(df.head(5))
                
                # Validate the data
                with st.spinner("Validating data..."):
                    is_valid, messages = validate_csv_data(df)
                
                # Display validation messages
                for message in messages:
                    if not is_valid:
                        st.error(message)
                    else:
                        st.success(message)
                
                # Processing button
                if is_valid:
                    if st.button("Process Data", key="process_data_btn"):
                        with st.spinner("Processing data..."):
                            # Store raw data in session state
                            st.session_state.raw_data = df
                            
                            # Extract and clean data
                            processed_df = extract_names_companies(df)
                            processed_df = clean_data(processed_df)
                            
                            # Store processed data
                            st.session_state.processed_data = processed_df
                            
                            # Log processing
                            logger.info("CSV processed successfully: %d rows after processing", len(processed_df))
                            
                            # Navigate to preview step
                            go_to_step("preview")
                            st.rerun()
        except Exception as e:
            st.error(f"Error processing file: {str(e)}")
            logger.error("Error processing uploaded file: %s", str(e), exc_info=True)
    else:
        # Show instructions when no file is uploaded
        st.info("Please upload a CSV file to continue.")
        
        # Display file format information
        with st.expander("File Format Information"):
            st.markdown("""
            ### Expected CSV format
            
            Your CSV file should contain columns with information about leads.
            The system will try to automatically identify the following information:
            
            - **Name**: First name and last name (separate columns or full name)
            - **Company**: Company or organization name
            - **Email**: Email addresses (optional)
            - **Phone**: Phone numbers (optional)
            
            #### Example CSV format:
            
            | First Name | Last Name | Company | Email | Phone |
            |------------|-----------|---------|-------|-------|
            | John | Doe | Acme Inc. | john.doe@acme.com | 555-123-4567 |
            | Jane | Smith | XYZ Corp | jane.smith@xyz.com | 555-987-6543 |
            
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
        logger.error("Error reading uploaded file: %s", str(e), exc_info=True)
    
    return df, error_message

def _get_sample_data() -> io.BytesIO:
    """
    Generate sample data for demonstration purposes.
    
    Returns:
        io.BytesIO: In-memory file-like object containing sample CSV data
    """
    # Create sample DataFrame
    sample_data = {
        'First Name': ['John', 'Jane', 'Robert', 'Emily', 'Michael'],
        'Last Name': ['Doe', 'Smith', 'Johnson', 'Brown', 'Davis'],
        'Company': ['Acme Inc.', 'XYZ Corp', 'ABC Enterprises', 'Tech Solutions', 'Global Systems'],
        'Email': ['john.doe@acme.com', 'jane.smith@xyz.com', 'robert@abc.com', 'emily.brown@techsolutions.com', 'michael.davis@global.com'],
        'Phone': ['555-123-4567', '555-987-6543', '555-456-7890', '555-789-0123', '555-345-6789']
    }
    
    df = pd.DataFrame(sample_data)
    
    # Convert to CSV in memory
    csv_buffer = io.BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    
    # Create a BytesIO object that streamlit can use as a file
    output = io.BytesIO(csv_buffer.read())
    output.name = "sample_leads.csv"
    output.seek(0)
    
    return output 