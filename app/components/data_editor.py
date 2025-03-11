#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Data Editor Component

This module renders the data editing interface for reviewing and modifying
enriched lead data before Salesforce export.
"""

import streamlit as st
import pandas as pd
import logging
from app.utils.session_state import go_to_step
from app.utils.data_processing import prepare_for_salesforce

logger = logging.getLogger('csv_processor')

def render_data_editor():
    """Render the data editor interface."""
    
    st.header("Review & Edit Data")
    
    # Check if enriched data exists
    if st.session_state.enriched_data is None:
        st.error("No enriched data available. Please complete previous steps first.")
        
        if st.button("Go to Data Enrichment", key="goto_enrich_from_edit"):
            go_to_step("enrich")
            st.rerun()
        return
    
    # Get the data
    if st.session_state.final_data is None:
        # First time - initialize with enriched data
        st.session_state.final_data = st.session_state.enriched_data.copy()
    
    df = st.session_state.final_data.copy()
    
    st.markdown("""
    Review and edit the company data before exporting to Salesforce. Use the filters to narrow down the list, 
    select the companies you want to export, and make any necessary edits.
    """)
    
    # Company selection and filtering
    st.subheader("Filter Companies")
    
    # Add selection checkbox column if it doesn't exist
    if 'Selected' not in df.columns:
        df['Selected'] = True
    
    # Filtering options
    col1, col2 = st.columns(2)
    
    with col1:
        # Filter by enrichment status
        enrichment_status = st.multiselect(
            "Enrichment Status",
            options=sorted(df['EnrichmentStatus'].unique()),
            default=sorted(df['EnrichmentStatus'].unique())
        )
    
    with col2:
        # Filter by industry if available
        if 'Industry' in df.columns and df['Industry'].notna().any():
            industries = st.multiselect(
                "Industry",
                options=sorted(df['Industry'].dropna().unique()),
                default=[]
            )
    
    # Apply filters
    filtered_df = df.copy()
    
    if enrichment_status:
        filtered_df = filtered_df[filtered_df['EnrichmentStatus'].isin(enrichment_status)]
    
    if 'Industry' in locals() and industries:
        filtered_df = filtered_df[filtered_df['Industry'].isin(industries)]
    
    # Search functionality
    search_term = st.text_input("Search companies", "")
    if search_term:
        filtered_df = filtered_df[
            filtered_df['Company'].str.contains(search_term, case=False, na=False) |
            filtered_df.get('CompanyDescription', '').str.contains(search_term, case=False, na=False) |
            filtered_df.get('Industry', '').str.contains(search_term, case=False, na=False)
        ]
    
    # Display filtered data count
    st.markdown(f"**Showing {len(filtered_df)} of {len(df)} companies**")
    
    # Bulk selection
    st.subheader("Selection")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("Select All Filtered"):
            for idx in filtered_df.index:
                df.at[idx, 'Selected'] = True
            st.session_state.final_data = df
            st.rerun()
    
    with col2:
        if st.button("Deselect All Filtered"):
            for idx in filtered_df.index:
                df.at[idx, 'Selected'] = False
            st.session_state.final_data = df
            st.rerun()
    
    with col3:
        # Show count of selected leads
        selected_count = df['Selected'].sum()
        st.metric("Selected Companies", selected_count)
    
    # Editable Data
    st.subheader("Review & Edit Company Data")
    
    # Highlight selected rows in the editor
    def highlight_selected(row):
        if row['Selected']:
            return ['background-color: #e6ffe6'] * len(row)
        return [''] * len(row)
    
    # Display editable dataframe
    edited_df = st.data_editor(
        filtered_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Selected": st.column_config.CheckboxColumn(
                "Select",
                help="Select companies to export",
                default=True
            ),
            "Company": st.column_config.TextColumn(
                "Company Name",
                width="large"
            ),
            "CompanyWebsite": st.column_config.TextColumn(
                "Website",
                width="medium"
            ),
            "Industry": st.column_config.TextColumn(
                "Industry",
                width="medium"
            ),
            "CompanyDescription": st.column_config.TextColumn(
                "Description",
                width="large"
            ),
            "CompanyLocation": st.column_config.TextColumn(
                "Location",
                width="medium"
            ),
            "EnrichmentStatus": st.column_config.SelectboxColumn(
                "Status",
                options=["Success", "Pending", "Failed"],
                width="small"
            )
        },
        disabled=["EnrichmentStatus", "EnrichmentSource"],
        height=500,
    )
    
    # Update the session state with the edited data (preserving rows not in filtered view)
    for idx, row in edited_df.iterrows():
        for col in edited_df.columns:
            df.at[idx, col] = row[col]
    
    st.session_state.final_data = df
    
    # Show summary of changes
    st.subheader("Summary")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric("Total Companies", len(df))
    
    with col2:
        st.metric("Selected for Export", df['Selected'].sum())
    
    # Navigation buttons
    st.subheader("Options")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("Back to Enrichment", key="back_to_enrich"):
            go_to_step("enrich")
            st.rerun()
    
    with col2:
        if df['Selected'].sum() > 0:
            if st.button("Continue to Export", key="continue_to_export"):
                go_to_step("export")
                st.rerun()
        else:
            st.warning("Please select at least one company to continue to export.") 