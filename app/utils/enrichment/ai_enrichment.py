#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AI-Powered Enrichment Module

This module provides functionality for AI-powered data enrichment of company data,
eliminating the need for manual processing and validation.
"""

import streamlit as st
import pandas as pd
import time
from typing import Dict, Any, Tuple, Optional

from app.services.ai_enrichment_service import AIEnrichmentService
from app.services.apollo_service import ApolloService
from app.services.crunchbase_service import CrunchbaseService
from app.services.salesforce_service import SalesforceService
from app.utils.enrichment.progress_utils import create_progress_tracker, should_stop_processing

# Status constants for enrichment
STATUS_COMPLETE = 'Complete'
STATUS_PARTIAL = 'Partial'
STATUS_FAILED = 'Failed'

def render_ai_enrichment_tab(df: pd.DataFrame) -> pd.DataFrame:
    """
    Render the AI-powered enrichment tab with controls and results.
    
    Args:
        df: DataFrame containing company data
        
    Returns:
        Enriched DataFrame
    """
    st.header("AI-Powered Enrichment")
    
    # Create the UI elements
    config = create_ai_enrichment_ui()
    
    # Process the data if requested
    if config['start_enrichment']:
        start_time = time.time()
        
        # Reset any previous stop requests
        if 'processing_status' in st.session_state:
            st.session_state.processing_status['stop_requested'] = False
        
        # Initialize the AI service
        with st.spinner("Setting up AI service..."):
            ai_service = AIEnrichmentService(
                aws_region=config.get('aws_region'),
                model_id=config.get('model_id', 'anthropic.claude-3-sonnet-20240229-v1:0')
            )
            
            if not ai_service.is_available():
                st.error("AI Enrichment Service is not available. Please check your AWS credentials.")
                return df
        
        # Create progress tracking elements
        progress_container = st.container()
        col1, col2 = progress_container.columns([3, 1])
        
        with col1:
            progress_bar = st.progress(0)
            status_text = st.empty()
        
        with col2:
            elapsed_text = st.empty()
            stop_button = st.button("Stop Processing", type="secondary")
        
        if stop_button:
            st.session_state.processing_status['stop_requested'] = True
            st.warning("Stop requested. Processing will halt after the current operation completes.")
            return df
        
        # Create a progress tracker function
        progress_tracker = create_progress_tracker(
            "AI Enrichment"
        )
        
        # Update progress UI function
        def update_progress_ui(progress, current, total, found_count):
            # Manually update UI elements
            progress_bar.progress(progress)
            status_text.text(f"AI processing: {current}/{total} records")
            
            # Return whether to stop processing
            return should_stop_processing()
        
        # AI-Powered Enrichment process
        try:
            # Update status
            st.session_state.processing_status['current_phase'] = 'AI Enrichment'
            
            # Process the data with AI
            enriched_df, stats = ai_service.process_dataframe(
                df,
                update_callback=update_progress_ui,
                max_batch_size=config['batch_size']
            )
            
            # Check elapsed time and display
            elapsed = time.time() - start_time
            elapsed_text.text(f"Time elapsed: {int(elapsed)}s")
            
            # Display AI enrichment results
            if stats.get('status') == 'complete':
                st.success(f"AI enrichment complete! Processed {stats.get('processed_records', 0)} records with {stats.get('success_count', 0)} successfully enriched ({stats.get('success_rate', 0)}%).")
                
                # Show statistics
                st.subheader("AI Enrichment Statistics")
                st.json(stats)
            else:
                st.error(f"AI enrichment encountered an error: {stats.get('message', 'Unknown error')}")
                return df
        
            if should_stop_processing():
                st.warning("Processing stopped by user request.")
                return enriched_df
            
            # Update the session state with the enriched data
            st.session_state.data = enriched_df
            
            # Preview the enriched data
            st.subheader("Enriched Data Preview")
            st.dataframe(enriched_df.head(10))
            
            # Create download button for the enriched data
            csv_data = enriched_df.to_csv(index=False)
            st.download_button(
                label="Download Enriched Data as CSV",
                data=csv_data,
                file_name="enriched_company_data.csv",
                mime="text/csv"
            )
            
            # Map column names to match the expected format in the rest of the app
            # This is needed because our new service uses different column naming conventions
            if 'company_name' in enriched_df.columns and 'Company' not in enriched_df.columns:
                column_mapping = {
                    'company_name': 'Company',
                    'company_website': 'CompanyWebsite',
                    'industry': 'Industry',
                    'company_size': 'CompanySize',
                    'company_description': 'CompanyDescription',
                    'founded_year': 'YearFounded', 
                    'headquarters': 'Headquarters',
                    'enrichment_status': 'EnrichmentStatus',
                    'enrichment_notes': 'EnrichmentNotes'
                }
                
                # Apply the column mapping for the standard columns
                for old_col, new_col in column_mapping.items():
                    if old_col in enriched_df.columns:
                        enriched_df[new_col] = enriched_df[old_col]
                
                # Keep both column naming conventions for compatibility
            
            return enriched_df
        
        except Exception as e:
            st.error(f"Error during AI enrichment process: {str(e)}")
            return df
    
    return df

def create_ai_enrichment_ui():
    """
    Create the UI elements for AI-powered enrichment.
    
    Returns:
        Configuration dictionary for the enrichment process
    """
    st.subheader("AI-Powered Company Data Enrichment")
    st.markdown(
        """
        This will process your data using Claude AI to:
        1. **Analyze and Map Your Data**: Automatically identify company name, website, and other fields
        2. **Find Missing Information**: Discover company websites and other missing data
        3. **Enrich Company Details**: Add industry, size, leadership, and other information
        4. **Standardize Data Format**: Prepare data for Salesforce import
        
        The process is fully automated and requires no manual column mapping or validation.
        """
    )
    
    # Process configuration options - simplified interface
    st.info("Click the button below to start the AI enrichment process. The system will use the default AWS and Claude configuration from your environment settings.")
    
    start_enrichment = st.button(
        "Start AI Enrichment", 
        type="primary", 
        use_container_width=True
    )
    
    # Return the configuration with default values
    return {
        'start_enrichment': start_enrichment,
        'aws_region': None,  # Use default from .env
        'model_id': None,    # Use default from .env
        'batch_size': 25     # Default batch size
    } 