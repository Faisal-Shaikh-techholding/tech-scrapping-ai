#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Data View Component

This module handles the basic viewing of data before enrichment.
"""

import streamlit as st
import pandas as pd
from app.utils.session_state import go_to_step

def render_data_view():
    """Render the data view interface."""
    st.header("Step 2: View Data")
    
    # Check if data exists
    if st.session_state.data is None:
        st.error("No data available for viewing. Please upload data first.")
        
        if st.button("Go to Data Upload", key="goto_upload_from_view"):
            go_to_step("upload")
            st.rerun()
        return
    
    # Get the data
    df = st.session_state.data.copy()
    
    # Render data table
    render_data_table(df)
    
    # Navigation buttons
    col1, col2, col3 = st.columns([1, 3, 1])
    with col1:
        if st.button("Back to Upload", key="back_to_upload"):
            go_to_step("upload")
            st.rerun()
    
    with col3:
        if st.button("Continue to Enrich & Export", key="continue_to_enrich"):
            # Update session state with data
            st.session_state.data = df
            go_to_step("enrich_export")
            st.rerun()

def render_data_table(df):
    """Render the data table view with filtering options."""
    st.subheader("Company Data")
    
    # Add search functionality
    search_term = st.text_input("Search companies:", "")
    
    # Filter data based on search term if provided
    if search_term:
        filtered_df = df[df.apply(lambda row: row.astype(str).str.contains(search_term, case=False).any(), axis=1)]
        st.write(f"Found {len(filtered_df)} rows matching '{search_term}'")
    else:
        filtered_df = df
    
    # Column selector
    if not filtered_df.empty:
        all_columns = filtered_df.columns.tolist()
        selected_columns = st.multiselect(
            "Select columns to display:",
            all_columns,
            default=all_columns[:min(8, len(all_columns))]  # Display first 8 columns by default
        )
        
        if selected_columns:
            # Display data with selected columns
            st.dataframe(
                filtered_df[selected_columns], 
                height=500,  # Increased height for better visibility
                use_container_width=True
            )
        else:
            st.info("Please select at least one column to display.")
    else:
        st.warning("No data matches your search criteria.") 