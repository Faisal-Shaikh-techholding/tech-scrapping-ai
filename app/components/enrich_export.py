#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Enrich and Export Component

This module handles enriching company data and exporting it to Salesforce
in a streamlined, AI-powered interface.
"""

import streamlit as st
import pandas as pd
import time
from datetime import datetime
from typing import Optional

from app.utils.session_state import go_to_step
from app.services.salesforce_service import SalesforceService
from app.utils.enrichment.progress_utils import create_progress_tracker

# Import the AI-powered enrichment module
from app.utils.enrichment.ai_enrichment import (
    render_ai_enrichment_tab
)

def render_enrich_export():
    """Render the enrich and export interface."""
    st.header("Step 3: Enrich & Export")
    
    # Check if data exists
    if st.session_state.data is None:
        st.error("No data available for enrichment. Please upload data first.")
        
        if st.button("Go to Data Upload", key="goto_upload_from_enrich"):
            go_to_step("upload")
            st.rerun()
        return
    
    # Get the data
    df = st.session_state.data.copy()
    
    # Create tabs for AI enrichment and results/export
    tabs = st.tabs([
        "AI Enrichment",  # Default tab
        "Results & Export"
    ])
    
    # AI Enrichment Tab
    with tabs[0]:
        enriched_df = render_ai_enrichment_tab(df)
        if enriched_df is not df:  # Changed?
            st.session_state.data = enriched_df
    
    # Results and Export Tab
    with tabs[1]:
        render_results_export_tab(st.session_state.data)
    
    # Navigation buttons
    col1, _ = st.columns([1, 4])
    with col1:
        if st.button("Back to Data View", key="back_to_view"):
            go_to_step("view")
            st.rerun()

def render_results_export_tab(df: pd.DataFrame):
    """
    Render the results and export tab.
    
    Args:
        df: DataFrame containing enriched company data
    """
    st.subheader("Results & Export")
    
    # Create tabs for results and export
    result_export_tabs = st.tabs(["Enrichment Results", "Export to Salesforce"])
    
    # Results Tab
    with result_export_tabs[0]:
        # Check if data has been enriched
        if 'EnrichmentStatus' not in df.columns:
            st.warning("No enrichment data found. Please run enrichment first.")
        else:
            # Display enrichment statistics
            st.subheader("Enrichment Statistics")
            
            # Calculate statistics
            stats = {
                'total': len(df),
                'complete': (df['EnrichmentStatus'] == 'Complete').sum(),
                'partial': (df['EnrichmentStatus'] == 'Partial').sum(),
                'failed': (df['EnrichmentStatus'] == 'Failed').sum() + (df['EnrichmentStatus'] == 'Incomplete').sum(),
                'pending': (df['EnrichmentStatus'] == 'Pending').sum()
            }
            
            # Calculate success rate
            if stats['total'] > 0:
                stats['success_rate'] = round(((stats['complete'] + stats['partial']) / stats['total'] * 100), 1)
            else:
                stats['success_rate'] = 0
            
            # Display statistics
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Records", stats['total'])
            col2.metric("Complete", stats['complete'])
            col3.metric("Partial", stats['partial'])
            col4.metric("Success Rate", f"{stats['success_rate']}%")
            
            # Display the enriched data
            st.subheader("Enriched Data")
            
            # Add filtering options
            filter_options = st.multiselect(
                "Filter by Enrichment Status",
                options=["Complete", "Partial", "Failed", "Pending"],
                default=["Complete", "Partial"]
            )
            
            # Apply filters
            if filter_options:
                filtered_df = df[df['EnrichmentStatus'].isin(filter_options)]
            else:
                filtered_df = df
            
            # Display the filtered data
            st.dataframe(filtered_df)
            
            # Add download button
            csv_data = filtered_df.to_csv(index=False)
            st.download_button(
                label="Download Filtered Data as CSV",
                data=csv_data,
                file_name="enriched_company_data.csv",
                mime="text/csv"
            )
    
    # Export to Salesforce Tab
    with result_export_tabs[1]:
        st.subheader("Export to Salesforce")
        
        # Check if Salesforce credentials are available
        sf_creds = st.session_state.api_keys.get('salesforce', {})
        if not sf_creds.get('username') or not sf_creds.get('password'):
            st.warning("Salesforce credentials not found. Please add them in the sidebar.")
        else:
            # UI for Salesforce export
            st.markdown("""
            Select the records you want to export to Salesforce and configure the mapping options.
            """)
            
            # Export options
            export_status = st.multiselect(
                "Export records with status",
                options=["Complete", "Partial", "Failed", "Pending"],
                default=["Complete", "Partial"]
            )
            
            # Export button
            export_to_sf = st.button("Export to Salesforce", type="primary")
            
            if export_to_sf:
                with st.spinner("Exporting to Salesforce..."):
                    # Filter records based on selected statuses
                    if export_status:
                        export_df = df[df['EnrichmentStatus'].isin(export_status)]
                    else:
                        export_df = df
                    
                    # Perform Salesforce export (simplified)
                    try:
                        # Initialize Salesforce service
                        sf_service = SalesforceService(
                            username=sf_creds.get('username'),
                            password=sf_creds.get('password'),
                            security_token=sf_creds.get('security_token', ''),
                            domain=sf_creds.get('domain', 'login')
                        )
                        
                        # Placeholder for export logic
                        st.warning("Salesforce export is not implemented in this simplified version.")
                        st.info(f"Would export {len(export_df)} records to Salesforce.")
                    except Exception as e:
                        st.error(f"Error during Salesforce export: {str(e)}")

