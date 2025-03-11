#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Data Enrichment Component

This module renders the data enrichment interface and handles API-based enrichment.
"""

import streamlit as st
import pandas as pd
import logging
from app.utils.session_state import go_to_step
from app.utils.data_processing import prepare_for_enrichment
from app.services.apollo_service import ApolloService
from app.services.crunchbase_service import CrunchbaseService
from app.services.web_scraper import WebScraperService

# Import enrichment utilities
from app.utils.enrichment.progress_utils import create_progress_tracker, display_enrichment_results
from app.utils.enrichment.one_click import perform_one_click_enrichment
from app.utils.enrichment.service_enrichment import (
    perform_apollo_enrichment, 
    perform_crunchbase_enrichment,
    perform_web_scraping
)
from app.utils.enrichment.results_display import (
    display_enrichment_statistics,
    display_enrichment_sources,
    display_enrichment_log,
    display_tech_leadership_info,
    display_tech_stack_info,
    display_company_size_funding,
    display_sample_companies,
    display_full_data_table
)

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
    
    # Create enrichment tabs
    enrichment_tabs = st.tabs(["One Click Enrichment", "Apollo.io API", "Crunchbase API", "Web Scraping", "Results"])
    
    # Call the render functions for each tab
    render_one_click_tab(df, enrichment_tabs[0])
    render_apollo_tab(df, enrichment_tabs[1])
    render_crunchbase_tab(df, enrichment_tabs[2])
    render_web_scraping_tab(df, enrichment_tabs[3])
    render_results_tab(enrichment_tabs[4])

def render_one_click_tab(df, tab):
    """Render the One Click Enrichment tab."""
    with tab:
        st.write("Enrich company data using all available methods in one click.")
        
        # Check API keys
        apollo_api_key = st.session_state.api_keys.get('apollo', {}).get('api_key', '')
        crunchbase_api_key = st.session_state.api_keys.get('crunchbase', {}).get('api_key', '')
        
        # Display API key status
        col1, col2 = st.columns(2)
        with col1:
            if apollo_api_key:
                st.success("Apollo.io API key configured")
            else:
                st.warning("Apollo.io API key not configured. Please add it in the sidebar.")
        
        with col2:
            if crunchbase_api_key:
                st.success("Crunchbase API key configured")
            else:
                st.warning("Crunchbase API key not configured. Please add it in the sidebar.")
        
        # Web scraping options
        st.subheader("Web Scraping Options")
        st.write("Select information to scrape from company websites if API enrichment is incomplete:")
        
        scrape_options = st.multiselect(
            "Select information to scrape:",
            ["Company Description", "Social Media Links", "Contact Information"],
            ["Company Description", "Social Media Links", "Contact Information"],
            key="one_click_scrape_options"
        )
        
        # Enrichment button
        if st.button("Start One Click Enrichment", key="one_click_enrich", 
                    disabled=not (apollo_api_key or crunchbase_api_key)):
            # Prepare data for enrichment if not already done
            if st.session_state.enriched_data is None:
                enrichment_df = prepare_for_enrichment(df)
                st.session_state.enriched_data = enrichment_df
            else:
                enrichment_df = st.session_state.enriched_data.copy()
            
            # Create progress tracker
            progress_tracker = create_progress_tracker("One Click Enrichment")
            
            # Create services
            services = []
            if apollo_api_key:
                apollo_service = ApolloService(api_key=apollo_api_key)
                services.append(('Apollo.io', apollo_service))
            
            if crunchbase_api_key:
                crunchbase_service = CrunchbaseService(api_key=crunchbase_api_key)
                services.append(('Crunchbase', crunchbase_service))
            
            # Create web scraper
            scraper = WebScraperService()
            
            # Perform enrichment
            with st.spinner("Enriching data with all available methods..."):
                enrichment_df, success_count, total_companies = perform_one_click_enrichment(
                    enrichment_df, services, scraper, scrape_options, progress_tracker
                )
                
                # Update session state
                st.session_state.enriched_data = enrichment_df
                
                # Show results
                if display_enrichment_results(success_count, total_companies):
                    # Add button to go directly to data editor
                    if st.button("Continue to Data Editor", key="one_click_to_editor"):
                        # Ensure enriched_data is properly saved to session state
                        st.session_state.enriched_data = enrichment_df.copy()
                        logger.info(f"Saved {len(enrichment_df)} companies to session state before transitioning to editor")
                        
                        # Reset final_data to force reinitialization from enriched_data
                        st.session_state.final_data = None
                        
                        # Navigate to edit step
                        go_to_step("edit")
                        st.rerun()
            
            # After enrichment, go to Results tab
            tab.is_selected = True

def render_apollo_tab(df, tab):
    """Render the Apollo.io API tab."""
    with tab:
        st.write("Enrich company data using Apollo.io API.")
        
        # Check API key
        apollo_api_key = st.session_state.api_keys.get('apollo', {}).get('api_key', '')
        
        if not apollo_api_key:
            st.warning("Apollo.io API key not configured. Please add it in the sidebar.")
            return
        
        # Enrichment button
        if st.button("Start Apollo.io Enrichment", key="start_apollo"):
            # Prepare data for enrichment if not already done
            if st.session_state.enriched_data is None:
                enrichment_df = prepare_for_enrichment(df)
                st.session_state.enriched_data = enrichment_df
            else:
                enrichment_df = st.session_state.enriched_data.copy()
            
            # Create progress tracker
            progress_tracker = create_progress_tracker("Apollo.io Enrichment")
            
            # Create Apollo service
            apollo_service = ApolloService(api_key=apollo_api_key)
            
            # Perform enrichment
            with st.spinner("Enriching data with Apollo.io..."):
                enrichment_df, success_count, total_companies = perform_apollo_enrichment(
                    enrichment_df, apollo_service, progress_tracker
                )
                
                # Update session state
                st.session_state.enriched_data = enrichment_df
                
                # Show results
                display_enrichment_results(success_count, total_companies)
            
            # After enrichment, go to Results tab
            tab.is_selected = True

def render_crunchbase_tab(df, tab):
    """Render the Crunchbase API tab."""
    with tab:
        st.write("Enrich company data using Crunchbase API.")
        
        # Check API key
        crunchbase_api_key = st.session_state.api_keys.get('crunchbase', {}).get('api_key', '')
        
        if not crunchbase_api_key:
            st.warning("Crunchbase API key not configured. Please add it in the sidebar.")
            return
        
        # Enrichment button
        if st.button("Start Crunchbase Enrichment", key="start_crunchbase"):
            # Prepare data for enrichment if not already done
            if st.session_state.enriched_data is None:
                enrichment_df = prepare_for_enrichment(df)
                st.session_state.enriched_data = enrichment_df
            else:
                enrichment_df = st.session_state.enriched_data.copy()
            
            # Create progress tracker
            progress_tracker = create_progress_tracker("Crunchbase Enrichment")
            
            # Create Crunchbase service
            crunchbase_service = CrunchbaseService(api_key=crunchbase_api_key)
            
            # Perform enrichment
            with st.spinner("Enriching data with Crunchbase..."):
                enrichment_df, success_count, total_companies = perform_crunchbase_enrichment(
                    enrichment_df, crunchbase_service, progress_tracker
                )
                
                # Update session state
                st.session_state.enriched_data = enrichment_df
                
                # Show results
                display_enrichment_results(success_count, total_companies)
            
            # After enrichment, go to Results tab
            tab.is_selected = True

def render_web_scraping_tab(df, tab):
    """Render the Web Scraping tab."""
    with tab:
        st.write("Scrape additional information from company websites.")
        
        # Options for web scraping
        scrape_options = st.multiselect(
            "Select information to scrape:",
            ["Company Description", "Social Media Links", "Contact Information"],
            ["Company Description", "Social Media Links", "Contact Information"],
            key="web_scraping_tab_options"
        )
        
        if st.button("Start Web Scraping", key="start_scraping"):
            # Prepare data for enrichment if not already done
            if st.session_state.enriched_data is None:
                enrichment_df = prepare_for_enrichment(df)
                st.session_state.enriched_data = enrichment_df
            else:
                enrichment_df = st.session_state.enriched_data.copy()
            
            # Create progress tracker
            progress_tracker = create_progress_tracker("Web Scraping")
            
            # Create web scraper
            scraper = WebScraperService()
            
            # Perform enrichment
            with st.spinner("Scraping company websites..."):
                enrichment_df, success_count, total_companies = perform_web_scraping(
                    enrichment_df, scraper, scrape_options, progress_tracker
                )
                
                # Update session state
                st.session_state.enriched_data = enrichment_df
                
                # Show results
                display_enrichment_results(success_count, total_companies)
            
            # After enrichment, go to Results tab
            tab.is_selected = True

def render_results_tab(tab):
    """Render the Results tab."""
    with tab:
        st.write("View enrichment results and data quality.")
        
        if st.session_state.enriched_data is not None:
            enriched_df = st.session_state.enriched_data.copy()
            
            # Display enrichment statistics
            display_enrichment_statistics(enriched_df)
            
            # Display enrichment sources
            display_enrichment_sources(enriched_df)
            
            # Display enrichment log
            display_enrichment_log()
            
            # Display tech leadership information
            display_tech_leadership_info(enriched_df)
            
            # Display tech stack information
            display_tech_stack_info(enriched_df)
            
            # Display company size and funding information
            display_company_size_funding(enriched_df)
            
            # Display sample of enriched data
            display_sample_companies(enriched_df)
            
            # Display full enriched data table
            display_full_data_table(enriched_df)
            
            # Provide option to continue to data editor
            if st.button("Continue to Data Editor", key="continue_to_editor"):
                # Ensure enriched_data is properly saved to session state
                st.session_state.enriched_data = enriched_df.copy()
                logger.info(f"Saved {len(enriched_df)} companies to session state before transitioning to editor")
                
                # Reset final_data to force reinitialization from enriched_data
                st.session_state.final_data = None
                
                # Navigate to edit step
                go_to_step("edit")
                st.rerun()
        else:
            st.info("No enriched data available yet. Please use one of the enrichment methods first.") 