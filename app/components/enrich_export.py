#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Enrich and Export Component

This module handles enriching company data and exporting it to Salesforce
in a single, streamlined interface.
"""

import streamlit as st
import pandas as pd
import time
from datetime import datetime

from app.utils.session_state import go_to_step
from app.services.apollo_service import ApolloService
from app.services.crunchbase_service import CrunchbaseService
from app.services.web_scraper import WebScraperService
from app.services.salesforce_service import SalesforceService
from app.utils.enrichment.progress_utils import create_progress_tracker, should_stop_processing
from app.utils.enrichment.one_click import perform_one_click_enrichment
from app.utils.enrichment.service_enrichment import (
    perform_apollo_enrichment,
    perform_crunchbase_enrichment,
    perform_web_scraping
)
from app.utils.enrichment.results_display import (
    display_enrichment_statistics,
    display_enrichment_sources,
    display_tech_leadership_info,
    display_tech_stack_info,
    display_sample_companies,
    display_full_data_table
)

def render_enrich_export():
    """Render the enrichment and export interface."""
    st.header("Step 3: Enrich Data & Send to Salesforce")
    
    # Check if data exists
    if st.session_state.data is None:
        st.error("No data available for enrichment. Please upload data first.")
        
        if st.button("Go to Data Upload", key="goto_upload_from_enrich"):
            go_to_step("upload")
            st.rerun()
        return
    
    # Get the data
    df = st.session_state.data.copy()
    
    # Create tabs for enrichment and export options
    tabs = st.tabs(["One-Click Enrichment", "Service Enrichment", "Results", "Export to Salesforce"])
    
    # Tab 1: One-Click Enrichment
    with tabs[0]:
        df = render_one_click_tab(df)
    
    # Tab 2: Service Enrichment
    with tabs[1]:
        df = render_service_enrichment_tab(df)
    
    # Tab 3: Results Display
    with tabs[2]:
        render_results_tab(df)
    
    # Tab 4: Salesforce Export
    with tabs[3]:
        render_salesforce_tab(df)
    
    # Update session state with the potentially enriched data
    st.session_state.data = df
    
    # Navigation buttons
    col1, col2 = st.columns([1, 4])
    with col1:
        if st.button("Back to Data View", key="back_to_view"):
            go_to_step("view")
            st.rerun()

def render_one_click_tab(df):
    """Render the one-click enrichment tab."""
    st.subheader("One-Click Enrichment")
    st.write("Enrich your data using all available services with a single click.")
    
    # Using all services by default
    services = ["apollo", "crunchbase", "webscraping"]
    
    st.write("#### Enrichment Services")
    st.info("All three services (Apollo.io API, Crunchbase API, and Web Scraping) will be used for One-Click Enrichment.")
    
    # Create services
    apollo_service = None
    crunchbase_service = None
    web_scraper = None
    
    # Initialize services
    apollo_key = st.session_state.api_keys.get('apollo', '')
    if apollo_key:
        apollo_service = ApolloService(api_key=apollo_key)
    else:
        st.warning("Apollo.io API key not set. Please add it in the sidebar settings.")
    
    crunchbase_key = st.session_state.api_keys.get('crunchbase', '')
    if crunchbase_key:
        crunchbase_service = CrunchbaseService(api_key=crunchbase_key)
    else:
        st.warning("Crunchbase API key not set. Please add it in the sidebar settings.")
    
    web_scraper = WebScraperService()
    
    # Default scraping options
    scraping_options = {
        "skip_enriched": True,
        "extract_social": True,
        "extract_description": True,
        "extract_contacts": True
    }
    
    # Start enrichment button
    if st.button("Start One-Click Enrichment", key="start_one_click", 
                 disabled=st.session_state.processing_status['is_processing']):
        # Set up progress tracking
        progress_tracker = create_progress_tracker()
        
        # Update processing status
        st.session_state.processing_status['is_processing'] = True
        st.session_state.processing_status['current_operation'] = "One-Click Enrichment"
        
        # Perform enrichment
        try:
            enriched_df = perform_one_click_enrichment(
                df, 
                services, 
                web_scraper, 
                scraping_options, 
                progress_tracker
            )
            
            if not should_stop_processing():
                st.success("One-click enrichment completed!")
                # Update the dataframe
                df = enriched_df
            else:
                st.warning("Enrichment was stopped by user.")
        except Exception as e:
            st.error(f"Error during enrichment: {str(e)}")
        finally:
            # Reset processing status
            st.session_state.processing_status['is_processing'] = False
    
    return df

def render_service_enrichment_tab(df):
    """Render the service-specific enrichment tab."""
    st.subheader("Service-Specific Enrichment")
    
    service_tabs = st.tabs(["Apollo.io API", "Crunchbase API", "Web Scraping"])
    
    # Tab 1: Apollo.io API
    with service_tabs[0]:
        df = render_apollo_tab(df)
    
    # Tab 2: Crunchbase API
    with service_tabs[1]:
        df = render_crunchbase_tab(df)
    
    # Tab 3: Web Scraping
    with service_tabs[2]:
        df = render_web_scraping_tab(df)
    
    return df

def render_apollo_tab(df):
    """Render the Apollo.io API enrichment tab."""
    st.write("#### Apollo.io API Enrichment")
    st.write("Enrich company data using the Apollo.io API.")
    
    # Check if API key is set
    apollo_key = st.session_state.api_keys.get('apollo', '')
    if not apollo_key:
        st.warning("Apollo.io API key not set. Please add it in the sidebar settings.")
        return df
    
    # Company filtering options
    with st.expander("Filtering Options", expanded=False):
        skip_enriched = st.checkbox("Skip already enriched companies", True, key="apollo_skip_enriched")
    
    # Start enrichment button
    if st.button("Start Apollo.io Enrichment", key="start_apollo", 
                disabled=st.session_state.processing_status['is_processing']):
        # Set up progress tracking
        progress_tracker = create_progress_tracker()
        
        # Update processing status
        st.session_state.processing_status['is_processing'] = True
        st.session_state.processing_status['current_operation'] = "Apollo.io Enrichment"
        
        # Perform enrichment
        try:
            enriched_df = perform_apollo_enrichment(
                df,
                progress_tracker,
                skip_enriched=skip_enriched
            )
            
            if not should_stop_processing():
                st.success("Apollo.io enrichment completed!")
                # Update the dataframe
                df = enriched_df
            else:
                st.warning("Enrichment was stopped by user.")
        except Exception as e:
            st.error(f"Error during Apollo.io enrichment: {str(e)}")
        finally:
            # Reset processing status
            st.session_state.processing_status['is_processing'] = False
    
    return df

def render_crunchbase_tab(df):
    """Render the Crunchbase API enrichment tab."""
    st.write("#### Crunchbase API Enrichment")
    st.write("Enrich company data using the Crunchbase API.")
    
    # Check if API key is set
    crunchbase_key = st.session_state.api_keys.get('crunchbase', '')
    if not crunchbase_key:
        st.warning("Crunchbase API key not set. Please add it in the sidebar settings.")
        return df
    
    # Company filtering options
    with st.expander("Filtering Options", expanded=False):
        skip_enriched = st.checkbox("Skip already enriched companies", True, key="crunchbase_skip_enriched")
    
    # Start enrichment button
    if st.button("Start Crunchbase Enrichment", key="start_crunchbase", 
                disabled=st.session_state.processing_status['is_processing']):
        # Set up progress tracking
        progress_tracker = create_progress_tracker()
        
        # Update processing status
        st.session_state.processing_status['is_processing'] = True
        st.session_state.processing_status['current_operation'] = "Crunchbase Enrichment"
        
        # Perform enrichment
        try:
            enriched_df = perform_crunchbase_enrichment(
                df,
                progress_tracker,
                skip_enriched=skip_enriched
            )
            
            if not should_stop_processing():
                st.success("Crunchbase enrichment completed!")
                # Update the dataframe
                df = enriched_df
            else:
                st.warning("Enrichment was stopped by user.")
        except Exception as e:
            st.error(f"Error during Crunchbase enrichment: {str(e)}")
        finally:
            # Reset processing status
            st.session_state.processing_status['is_processing'] = False
    
    return df

def render_web_scraping_tab(df):
    """Render the web scraping enrichment tab."""
    st.write("#### Web Scraping Enrichment")
    st.write("Enrich company data by scraping company websites.")
    
    # Scraping options
    with st.expander("Scraping Options", expanded=False):
        skip_enriched = st.checkbox("Skip already enriched companies", True, key="scraping_skip_enriched")
        extract_social = st.checkbox("Extract social media links", True, key="scraping_extract_social")
        extract_description = st.checkbox("Extract company description", True, key="scraping_extract_description")
        extract_contacts = st.checkbox("Extract contact information", True, key="scraping_extract_contacts")
    
    # Start enrichment button
    if st.button("Start Web Scraping", key="start_scraping", 
                disabled=st.session_state.processing_status['is_processing']):
        # Set up progress tracking
        progress_tracker = create_progress_tracker()
        
        # Update processing status
        st.session_state.processing_status['is_processing'] = True
        st.session_state.processing_status['current_operation'] = "Web Scraping"
        
        # Perform enrichment
        try:
            scraping_options = {
                "skip_enriched": skip_enriched,
                "extract_social": extract_social,
                "extract_description": extract_description,
                "extract_contacts": extract_contacts
            }
            
            enriched_df = perform_web_scraping(
                df,
                progress_tracker,
                options=scraping_options
            )
            
            if not should_stop_processing():
                st.success("Web scraping completed!")
                # Update the dataframe
                df = enriched_df
            else:
                st.warning("Web scraping was stopped by user.")
        except Exception as e:
            st.error(f"Error during web scraping: {str(e)}")
        finally:
            # Reset processing status
            st.session_state.processing_status['is_processing'] = False
    
    return df

def render_results_tab(df):
    """Render the results display tab."""
    st.subheader("Enrichment Results")
    
    if df is None or len(df) == 0:
        st.warning("No data available to display results.")
        return
    
    # Create tabs for different result views
    results_tabs = st.tabs(["Statistics", "Technology Info", "Sample Data", "Full Data"])
    
    # Tab 1: Statistics
    with results_tabs[0]:
        col1, col2 = st.columns(2)
        with col1:
            display_enrichment_statistics(df)
        with col2:
            display_enrichment_sources(df)
    
    # Tab 2: Technology Info
    with results_tabs[1]:
        col1, col2 = st.columns(2)
        with col1:
            display_tech_leadership_info(df)
        with col2:
            display_tech_stack_info(df)
    
    # Tab 3: Sample Data
    with results_tabs[2]:
        display_sample_companies(df, max_samples=5)
    
    # Tab 4: Full Data
    with results_tabs[3]:
        display_full_data_table(df)

def render_salesforce_tab(df):
    """Render the Salesforce export tab."""
    st.subheader("Export to Salesforce")
    
    # Check if data exists and has been enriched
    if df is None or len(df) == 0:
        st.warning("No data available for export to Salesforce.")
        return
    
    # Check if any companies have been enriched
    enriched_count = len(df[df['enrichment_status'] == 'Completed'])
    if enriched_count == 0:
        st.warning("No enriched companies found. Please enrich data before exporting to Salesforce.")
        return
    
    # Check if Salesforce credentials are set
    sf_creds = st.session_state.api_keys.get('salesforce', {})
    if not sf_creds.get('username') or not sf_creds.get('password'):
        st.warning("Salesforce credentials not set. Please add them in the sidebar settings.")
        return
    
    # Display enriched data summary
    st.write(f"Ready to export {enriched_count} enriched companies to Salesforce.")
    
    # Export options
    with st.expander("Export Options", expanded=True):
        # Lead options
        st.write("#### Lead Options")
        
        # Lead Status
        lead_status = st.selectbox(
            "Lead Status:",
            ["Open - Not Contacted", "Working - Contacted", "Closed - Converted", "Closed - Not Converted"],
            index=0
        )
        
        # Lead Source
        lead_source = st.selectbox(
            "Lead Source:",
            ["Web", "Phone Inquiry", "Partner Referral", "Purchased List", "Other"],
            index=0
        )
        
        # Company selection
        st.write("#### Company Selection")
        
        # Filter options
        filter_option = st.radio(
            "Export:",
            ["All enriched companies", "Selected companies"],
            index=0
        )
        
        selected_companies = []
        if filter_option == "Selected companies":
            # Allow selection of specific companies
            company_options = df[df['enrichment_status'] == 'Completed']['company'].tolist()
            selected_companies = st.multiselect(
                "Select companies to export:",
                company_options,
                default=company_options[:min(5, len(company_options))]
            )
    
    # Start export button
    if st.button("Export to Salesforce", key="start_export", 
                disabled=st.session_state.processing_status['is_processing']):
        # Set up progress tracking
        progress_tracker = create_progress_tracker()
        
        # Update processing status
        st.session_state.processing_status['is_processing'] = True
        st.session_state.processing_status['current_operation'] = "Salesforce Export"
        
        # Get filtered dataframe for export
        if filter_option == "All enriched companies":
            export_df = df[df['enrichment_status'] == 'Completed'].copy()
        else:
            if not selected_companies:
                st.warning("No companies selected for export.")
                st.session_state.processing_status['is_processing'] = False
                return
            export_df = df[df['company'].isin(selected_companies)].copy()
        
        # Initialize Salesforce service
        sf_service = SalesforceService(
            username=sf_creds.get('username', ''),
            password=sf_creds.get('password', ''),
            security_token=sf_creds.get('security_token', ''),
            domain=sf_creds.get('domain', 'login')
        )
        
        # Perform export
        try:
            # Connect to Salesforce
            if not sf_service.connect():
                st.error("Failed to connect to Salesforce. Please check your credentials.")
                st.session_state.processing_status['is_processing'] = False
                return
            
            # Reset export results
            st.session_state.export_results = {'success': [], 'failures': []}
            
            # Export each company
            total = len(export_df)
            success_count = 0
            failure_count = 0
            
            for i, (idx, row) in enumerate(export_df.iterrows()):
                # Check if process should stop
                if should_stop_processing():
                    st.warning("Export was stopped by user.")
                    break
                
                try:
                    # Prepare lead data
                    lead_data = {
                        'Company': row.get('company', ''),
                        'Website': row.get('website', ''),
                        'Description': row.get('company_description', ''),
                        'Industry': row.get('industry', ''),
                        'Status': lead_status,
                        'LeadSource': lead_source
                    }
                    
                    # Add contact info if available
                    if 'contact_name' in row and row['contact_name']:
                        name_parts = row['contact_name'].split(' ', 1)
                        if len(name_parts) > 1:
                            lead_data['FirstName'] = name_parts[0]
                            lead_data['LastName'] = name_parts[1]
                        else:
                            lead_data['FirstName'] = ''
                            lead_data['LastName'] = name_parts[0]
                    else:
                        lead_data['FirstName'] = ''
                        lead_data['LastName'] = f"{row.get('company', 'Unknown')} Contact"
                    
                    if 'email' in row and row['email']:
                        lead_data['Email'] = row['email']
                    
                    if 'phone' in row and row['phone']:
                        lead_data['Phone'] = row['phone']
                    
                    # Create the lead
                    lead_id = sf_service.create_lead(lead_data)
                    
                    if lead_id:
                        # Store successful result
                        st.session_state.export_results['success'].append({
                            'company': row.get('company', ''),
                            'lead_id': lead_id
                        })
                        success_count += 1
                    else:
                        # Store failure result
                        st.session_state.export_results['failures'].append({
                            'company': row.get('company', ''),
                            'error': 'Failed to create lead'
                        })
                        failure_count += 1
                
                except Exception as e:
                    # Store failure result
                    st.session_state.export_results['failures'].append({
                        'company': row.get('company', ''),
                        'error': str(e)
                    })
                    failure_count += 1
                
                # Update progress
                progress = (i + 1) / total
                progress_tracker.update_progress(progress, i + 1, total, success_count)
                
                # Add a small delay to avoid overloading Salesforce API
                time.sleep(0.5)
            
            # Display final results
            if success_count > 0:
                st.success(f"Successfully exported {success_count} leads to Salesforce.")
            
            if failure_count > 0:
                st.error(f"Failed to export {failure_count} leads. See error details below.")
                for failure in st.session_state.export_results['failures']:
                    st.write(f"- {failure['company']}: {failure['error']}")
            
            # Disconnect from Salesforce
            sf_service.disconnect()
            
        except Exception as e:
            st.error(f"Error during Salesforce export: {str(e)}")
        finally:
            # Reset processing status
            st.session_state.processing_status['is_processing'] = False 