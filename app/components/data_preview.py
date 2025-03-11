#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Data Preview Component

This module renders the data preview interface after initial CSV processing.
"""

import streamlit as st
import pandas as pd
import logging
from app.utils.session_state import go_to_step
from app.utils.data_processing import prepare_for_enrichment

logger = logging.getLogger('csv_processor')

def render_data_preview():
    """Render the data preview interface after initial CSV processing."""
    
    st.header("Data Preview")
    
    # Check if processed data exists
    if st.session_state.processed_data is None:
        st.error("No processed data found. Please upload and process a CSV file first.")
        
        if st.button("Go to Upload", key="goto_upload"):
            go_to_step("upload")
            st.rerun()
        return
    
    # Show processed data statistics
    processed_df = st.session_state.processed_data
    
    st.markdown("""
    Your data has been processed and the system has extracted key information.
    Please review the extracted data below before proceeding to the enrichment step.
    """)
    
    # Data statistics
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Total Leads", len(processed_df))
    
    with col2:
        complete_leads = len(processed_df[
            (processed_df['Company'] != '')
        ])
        st.metric("Complete Leads", complete_leads, 
                 delta=f"{int(complete_leads/len(processed_df)*100)}%" if len(processed_df) > 0 else "0%")

    
    # Show processed data table
    st.subheader("Processed Data")
    st.dataframe(processed_df, height=400)
    
    # Field mapping visualization
    st.subheader("Field Mapping")
    st.markdown("The system has mapped the data from your CSV file to standard fields:")
    
    # Get column mappings from raw to processed
    raw_df = st.session_state.raw_data
    mappings = _get_field_mappings(raw_df, processed_df)
    
    # Display mappings
    if mappings:
        mapping_data = pd.DataFrame(mappings, columns=["Original Field", "Mapped To", "Sample Value"])
        st.table(mapping_data)
    else:
        st.info("No field mappings could be determined.")
    
    # Additional options
    st.subheader("Options")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("Back to Upload", key="back_to_upload"):
            go_to_step("upload")
            st.rerun()
    
    with col2:
        if st.button("Proceed to Enrichment", key="proceed_to_enrichment"):
            with st.spinner("Preparing data for enrichment..."):
                # Prepare data for enrichment
                enrichment_df = prepare_for_enrichment(processed_df)
                
                # Store enrichment-ready data
                st.session_state.enriched_data = enrichment_df
                
                # Navigate to enrichment step
                go_to_step("enrich")
                st.rerun()

def _get_field_mappings(raw_df, processed_df):
    """
    Analyze the field mappings from raw to processed data.
    
    Args:
        raw_df (pd.DataFrame): Raw DataFrame
        processed_df (pd.DataFrame): Processed DataFrame
        
    Returns:
        list: List of tuples containing (raw_column, processed_column, sample_value)
    """
    mappings = []
    
    # Standard fields to check for
    standard_fields = ["FirstName", "LastName", "Company", "Email", "Phone"]
    
    # Name-related fields in raw data (potential sources of FirstName/LastName)
    name_columns = [col for col in raw_df.columns if 'name' in col.lower()]
    
    # Check each standard field
    for field in standard_fields:
        # If field exists in processed data and has values
        if field in processed_df.columns and not processed_df[field].isna().all():
            # Try to identify source column(s)
            source_col = None
            
            # Direct match (same column name in raw data)
            if field in raw_df.columns:
                source_col = field
            # Field-specific logic
            elif field in ('FirstName', 'LastName'):
                # For name fields, look at name-related columns
                for col in name_columns:
                    # Sample a few rows to see if they match
                    for i in range(min(5, len(processed_df))):
                        if i < len(raw_df) and i < len(processed_df):
                            # Check if the first or last part of the name appears in the raw column
                            raw_val = str(raw_df.iloc[i][col]) if pd.notna(raw_df.iloc[i][col]) else ""
                            proc_val = str(processed_df.iloc[i][field]) if pd.notna(processed_df.iloc[i][field]) else ""
                            
                            if proc_val and proc_val in raw_val:
                                source_col = col
                                break
                    if source_col:
                        break
            else:
                # For other fields, look for columns with similar names
                potential_cols = [col for col in raw_df.columns 
                                 if field.lower() in col.lower() or 
                                 col.lower() in field.lower()]
                
                if potential_cols:
                    source_col = potential_cols[0]  # Take the first match
            
            # Add to mappings if source was found
            if source_col:
                # Get a sample value from the processed data
                sample_value = processed_df[field].iloc[0] if not processed_df[field].empty else ""
                
                mappings.append((source_col, field, sample_value))
            else:
                # If no source column was identified, mark as "Generated"
                sample_value = processed_df[field].iloc[0] if not processed_df[field].empty else ""
                mappings.append(("Generated", field, sample_value))
    
    return mappings 