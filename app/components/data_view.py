#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Data View Component

This module handles the simple viewing of data before AI enrichment.
"""

import streamlit as st
import pandas as pd
from app.utils.session_state import go_to_step

def render_data_view():
    """Render the simplified data view interface."""
    st.header("Step 2: Preview Your Data")
    
    # Check if data exists
    if st.session_state.data is None:
        st.error("No data available for viewing. Please upload data first.")
        
        if st.button("Go to Data Upload", key="goto_upload_from_view"):
            go_to_step("upload")
            st.rerun()
        return
    
    # Get the data
    df = st.session_state.data.copy()
    
    # Display info message about AI processing
    st.info("👁️ This is a preview of your raw data. In the next step, our AI will automatically analyze, map, and enrich it.")
    
    # Render data table
    render_data_table(df)
    
    # Navigation buttons
    col1, col2, col3 = st.columns([1, 3, 1])
    with col1:
        if st.button("Back to Upload", key="back_to_upload"):
            go_to_step("upload")
            st.rerun()
    
    with col3:
        if st.button("Continue to AI Enrichment", key="continue_to_enrich", type="primary"):
            go_to_step("enrich_export")
            st.rerun()

def render_data_table(df):
    """Render the data table view with filtering options."""
    st.subheader("Raw Data Preview")
    
    # Add search functionality
    search_term = st.text_input("Search:", "")
    
    # Filter data based on search term if provided
    if search_term:
        filtered_df = df[df.apply(lambda row: row.astype(str).str.contains(search_term, case=False).any(), axis=1)]
        st.write(f"Found {len(filtered_df)} rows matching '{search_term}'")
    else:
        filtered_df = df
    
    # Show row count
    st.write(f"Showing {len(filtered_df)} rows of {len(df)} total records")
    
    # Display data
    if not filtered_df.empty:
        st.dataframe(filtered_df, height=500, use_container_width=True)
    else:
        st.warning("No data matches your search criteria.") 