#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Data Enrichment Component

This module renders the data enrichment interface and handles API-based enrichment.
"""

import streamlit as st
import pandas as pd
import logging
import time
from app.utils.session_state import go_to_step
from app.utils.data_processing import prepare_for_enrichment
from app.services.apollo_service import ApolloService
from app.services.crunchbase_service import CrunchbaseService
from app.services.web_scraper import WebScraperService

logger = logging.getLogger('csv_processor')

def render_data_enrichment():
    """Render the data enrichment interface."""
    
    st.header("Data Enrichment")
    
    # Check if processed data exists
    if st.session_state.processed_data is None:
        st.error("No data available for enrichment. Please complete previous steps first.")
        
        if st.button("Go to Data Preview", key="goto_preview_from_enrich"):
            go_to_step("preview")
            st.rerun()
        return
    
    # Get the data
    df = st.session_state.processed_data.copy()
    
    # Display statistics
    st.subheader("Company Data Statistics")
    
    company_count = len(df)
    companies_with_website = df['CompanyWebsite'].notna().sum()
    companies_with_industry = df['Industry'].notna().sum()
    companies_with_crunchbase = df['CrunchbaseURL'].notna().sum() if 'CrunchbaseURL' in df.columns else 0
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Companies", company_count)
    with col2:
        st.metric("With Website", companies_with_website)
    with col3:
        st.metric("With Industry", companies_with_industry)
    with col4:
        st.metric("With Crunchbase URLs", companies_with_crunchbase)
    
    st.markdown("---")
    
    # Enrichment options
    st.subheader("Enrichment Options")
    
    enrichment_tabs = st.tabs(["Apollo.io API", "Crunchbase API", "Web Scraping", "Results"])
    
    # Apollo.io API tab
    with enrichment_tabs[0]:
        st.write("Enrich company data using Apollo.io API.")
        
        # Check API keys
        apollo_api_key = st.session_state.api_keys.get('apollo', {}).get('api_key', '')
        
        if not apollo_api_key:
            st.warning("Apollo.io API key not configured. Please add it in the sidebar.")
        
        # Enrichment button
        if st.button("Enrich with Apollo.io", key="enrich_apollo", disabled=not apollo_api_key):
            # Create Apollo service
            apollo_service = ApolloService(api_key=apollo_api_key)
            
            # Prepare data for enrichment if not already done
            if st.session_state.enriched_data is None:
                enrichment_df = prepare_for_enrichment(df)
                st.session_state.enriched_data = enrichment_df
            else:
                enrichment_df = st.session_state.enriched_data.copy()
            
            # Create progress display
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            # Update session state with processing status
            st.session_state.processing_status = {
                'is_processing': True,
                'current_operation': 'Apollo.io Enrichment',
                'progress': 0,
                'total': len(enrichment_df),
                'success_count': 0,
                'error_count': 0,
                'messages': []
            }
            
            # Define callback for progress updates
            def update_progress(progress, current, total, success_count):
                # Update progress bar
                progress_bar.progress(progress)
                # Update status text
                status_text.text(f"Enriching company {current}/{total} ({success_count} enriched successfully)")
                # Update session state
                st.session_state.processing_status['progress'] = progress
                st.session_state.processing_status['success_count'] = success_count
            
            # Perform enrichment
            with st.spinner("Enriching data with Apollo.io..."):
                enriched_df = apollo_service.bulk_enrich_leads(enrichment_df, update_callback=update_progress)
                
                # Update session state
                st.session_state.enriched_data = enriched_df
                st.session_state.processing_status['is_processing'] = False
                
                # Show results
                success_count = (enriched_df['EnrichmentStatus'] == 'Success').sum()
                
                if success_count > 0:
                    st.success(f"Successfully enriched {success_count} companies!")
                else:
                    st.error("No companies were successfully enriched. Please check API key and data.")
            
            # After enrichment, go to Results tab
            enrichment_tabs[3].is_selected = True
    
    # Crunchbase API tab
    with enrichment_tabs[1]:
        st.write("Enrich company data using Crunchbase API.")
        
        # Check API keys
        crunchbase_api_key = st.session_state.api_keys.get('crunchbase', {}).get('api_key', '')
        
        if not crunchbase_api_key:
            st.warning("Crunchbase API key not configured. Please add it in the sidebar.")
            
            # Add field to the sidebar configuration
            with st.expander("Configure Crunchbase API Key"):
                new_key = st.text_input("Crunchbase API Key", type="password", key="cb_key_input")
                if new_key and st.button("Save Key", key="save_cb_key"):
                    if 'api_keys' not in st.session_state:
                        st.session_state.api_keys = {}
                    if 'crunchbase' not in st.session_state.api_keys:
                        st.session_state.api_keys['crunchbase'] = {}
                    
                    st.session_state.api_keys['crunchbase'] = {'api_key': new_key}
                    st.success("Crunchbase API key saved!")
                    st.rerun()
        
        # Enrichment button
        if st.button("Enrich with Crunchbase", key="enrich_crunchbase", disabled=not crunchbase_api_key):
            # Create Crunchbase service
            crunchbase_service = CrunchbaseService(api_key=crunchbase_api_key)
            
            # Prepare data for enrichment if not already done
            if st.session_state.enriched_data is None:
                enrichment_df = prepare_for_enrichment(df)
                st.session_state.enriched_data = enrichment_df
            else:
                enrichment_df = st.session_state.enriched_data.copy()
            
            # Create progress display
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            # Update session state with processing status
            st.session_state.processing_status = {
                'is_processing': True,
                'current_operation': 'Crunchbase Enrichment',
                'progress': 0,
                'total': len(enrichment_df),
                'success_count': 0,
                'error_count': 0,
                'messages': []
            }
            
            # Define callback for progress updates
            def update_crunchbase_progress(progress, current, total, success_count):
                # Update progress bar
                progress_bar.progress(progress)
                # Update status text
                status_text.text(f"Enriching company {current}/{total} ({success_count} enriched successfully)")
                # Update session state
                st.session_state.processing_status['progress'] = progress
                st.session_state.processing_status['success_count'] = success_count
            
            # Perform enrichment
            with st.spinner("Enriching data with Crunchbase..."):
                enriched_df = crunchbase_service.bulk_enrich_companies(
                    enrichment_df, 
                    update_callback=update_crunchbase_progress
                )
                
                # Update session state
                st.session_state.enriched_data = enriched_df
                st.session_state.processing_status['is_processing'] = False
                
                # Show results
                success_count = sum(1 for _, row in enriched_df.iterrows() 
                                   if 'Crunchbase API' in str(row.get('EnrichmentSource', '')))
                
                if success_count > 0:
                    st.success(f"Successfully enriched {success_count} companies with Crunchbase!")
                else:
                    st.error("No companies were successfully enriched with Crunchbase. Please check API key and data.")
            
            # After enrichment, go to Results tab
            enrichment_tabs[3].is_selected = True
    
    # Web Scraping tab
    with enrichment_tabs[2]:
        st.write("Scrape additional information from company websites.")
        
        # Options for web scraping
        scrape_options = st.multiselect(
            "Select information to scrape:",
            ["Company Description", "Social Media Links", "Contact Information"],
            ["Company Description", "Social Media Links", "Contact Information"]
        )
        
        if st.button("Start Web Scraping", key="start_scraping"):
            # Create web scraper service
            scraper = WebScraperService()
            
            # Prepare data for enrichment if not already done
            if st.session_state.enriched_data is None:
                enrichment_df = prepare_for_enrichment(df)
                st.session_state.enriched_data = enrichment_df
            else:
                enrichment_df = st.session_state.enriched_data.copy()
            
            # Create progress display
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            # Update session state with processing status
            st.session_state.processing_status = {
                'is_processing': True,
                'current_operation': 'Web Scraping',
                'progress': 0,
                'total': len(enrichment_df),
                'success_count': 0,
                'error_count': 0,
                'messages': []
            }
            
            # Define callback for progress updates
            def update_scraping_progress(progress, current, total, success_count):
                # Update progress bar
                progress_bar.progress(progress)
                # Update status text
                status_text.text(f"Scraping company {current}/{total} ({success_count} scraped successfully)")
                # Update session state
                st.session_state.processing_status['progress'] = progress
                st.session_state.processing_status['success_count'] = success_count
            
            # Perform web scraping
            with st.spinner("Scraping company websites..."):
                enriched_df = scraper.bulk_enrich_companies(
                    enrichment_df, 
                    update_callback=update_scraping_progress
                )
                
                # Update session state
                st.session_state.enriched_data = enriched_df
                st.session_state.processing_status['is_processing'] = False
                
                # Show results
                success_count = sum(1 for _, row in enriched_df.iterrows() 
                                   if 'Web Scraping' in str(row.get('EnrichmentSource', '')))
                
                if success_count > 0:
                    st.success(f"Successfully scraped data for {success_count} companies!")
                else:
                    st.error("No company data was successfully scraped. Please check the URLs and try again.")
            
            # After enrichment, go to Results tab
            enrichment_tabs[3].is_selected = True
    
    # Results tab
    with enrichment_tabs[3]:
        # Display enrichment results
        if st.session_state.enriched_data is not None:
            _display_enrichment_status(st.session_state.enriched_data)
            
            # Provide option to continue
            if st.button("Continue to Data Editor", key="continue_to_editor"):
                # Save final data to session state if not already done
                if st.session_state.final_data is None:
                    st.session_state.final_data = st.session_state.enriched_data.copy()
                
                # Go to next step
                go_to_step("edit")
                st.rerun()
        else:
            st.info("No enrichment performed yet. Use the API Enrichment or Web Scraping tabs to enrich your data.")

def _display_enrichment_status(df):
    """Display the current enrichment status."""
    
    st.subheader("Enrichment Results")
    
    # Calculate statistics
    total_companies = len(df)
    enriched_count = df['EnrichmentStatus'].eq('Success').sum()
    pending_count = df['EnrichmentStatus'].eq('Pending').sum()
    failed_count = df['EnrichmentStatus'].eq('Failed').sum()
    
    # Display statistics
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Enriched Companies", enriched_count)
    with col2:
        st.metric("Pending Companies", pending_count)
    with col3:
        st.metric("Failed Companies", failed_count)
    
    # Count enrichment sources
    enrichment_sources = {
        'Apollo.io API': sum(1 for _, row in df.iterrows() if 'Apollo' in str(row.get('EnrichmentSource', ''))),
        'Crunchbase API': sum(1 for _, row in df.iterrows() if 'Crunchbase' in str(row.get('EnrichmentSource', ''))),
        'Web Scraping': sum(1 for _, row in df.iterrows() if 'Web Scraping' in str(row.get('EnrichmentSource', '')))
    }
    
    # Display enrichment source counts
    st.subheader("Enrichment Sources")
    source_df = pd.DataFrame({
        'Source': list(enrichment_sources.keys()),
        'Companies Enriched': list(enrichment_sources.values())
    })
    st.dataframe(source_df, hide_index=True)
    
    # Display enrichment details
    st.subheader("Enriched Data Preview")
    
    # Show data preview with enrichment status color-coding
    styled_df = df.copy()
    
    # Select columns to display in preview
    display_cols = ['Company', 'CompanyWebsite', 'Industry', 'CompanyDescription', 
                   'CompanyLocation', 'EnrichmentStatus', 'EnrichmentSource']
    display_df = styled_df[display_cols].copy()
    
    # Create checkbox to show all columns
    if st.checkbox("Show all columns", key="show_all_enriched_cols", value=False):
        display_df = styled_df.copy()
    
    # # Apply styling
    st.dataframe(
        display_df.style.apply(
            lambda x: ['background-color: #008800' if x.EnrichmentStatus == 'Success' 
                      else 'background-color: #880000' if x.EnrichmentStatus == 'Failed'
                      else '' for _ in x],
            axis=1
        ),
        height=400
    )

    # Navigation buttons
    if st.button("Go Back to Preview"):
        go_to_step("preview")
        st.rerun()

    if st.button("Reset", key="reset_enrichment"):
        st.success("Reset complete! Return to uploader to start again.")
        go_to_step("upload")
        st.rerun()

    if st.button("Move to Data Editor", key="move_to_editor"):
        # Move to data editor step
        go_to_step("edit")
        st.rerun() 