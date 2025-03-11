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
from app.utils.enrichment_utils import enrich_company_data

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
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Companies", company_count)
    with col2:
        st.metric("With Website", companies_with_website)
    with col3:
        st.metric("With Industry", companies_with_industry)
    
    st.markdown("---")
    
    # Enrichment options
    st.subheader("Enrichment Options")
    
    enrichment_tabs = st.tabs(["One Click Enrichment", "Apollo.io API", "Crunchbase API", "Web Scraping", "Results"])
    
    # One Click Enrichment tab
    with enrichment_tabs[0]:
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
            
            # Create progress display
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            # Update session state with processing status
            st.session_state.processing_status = {
                'is_processing': True,
                'current_operation': 'One Click Enrichment',
                'progress': 0,
                'total': len(enrichment_df),
                'success_count': 0,
                'error_count': 0,
                'messages': []
            }
            
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
            
            # Define callback for progress updates
            def update_progress(progress, current, total, success_count, error_count=0, message=""):
                # Update progress bar
                progress_bar.progress(progress)
                # Update status text
                status_text.text(f"Processing company {current}/{total} ({success_count} enriched successfully)")
                # Update session state
                st.session_state.processing_status['progress'] = progress
                st.session_state.processing_status['success_count'] = success_count
                st.session_state.processing_status['error_count'] = error_count
                if message:
                    st.session_state.processing_status['messages'].append(message)
            
            # Perform enrichment
            with st.spinner("Enriching data with all available methods..."):
                # Process each company
                total_companies = len(enrichment_df)
                success_count = 0
                error_count = 0
                
                for idx, row in enrichment_df.iterrows():
                    current_company = idx + 1
                    company_name = row.get('Company', 'Unknown')
                    
                    # Skip if already successfully enriched
                    if row.get('EnrichmentStatus') == 'Success':
                        success_count += 1
                        progress = current_company / total_companies
                        update_progress(progress, current_company, total_companies, success_count, 
                                       message=f"Skipped {company_name} - already enriched")
                        continue
                    
                    # Convert row to dict
                    lead_data = row.to_dict()
                    
                    # Use the utility function to enrich the company data
                    enriched_lead = enrich_company_data(
                        lead_data, 
                        services, 
                        scraper=scraper, 
                        scrape_options=scrape_options
                    )
                    
                    # Update DataFrame with enriched data
                    for key, value in enriched_lead.items():
                        if key in enrichment_df.columns:
                            enrichment_df.at[idx, key] = value
                    
                    # Update progress
                    progress = current_company / total_companies
                    if enriched_lead.get('EnrichmentStatus') == 'Success':
                        success_count += 1
                        update_progress(
                            progress, 
                            current_company, 
                            total_companies, 
                            success_count, 
                            message=f"Enriched {company_name} with {enriched_lead.get('EnrichmentSource', 'Unknown')}"
                        )
                    else:
                        error_count += 1
                        update_progress(
                            progress, 
                            current_company, 
                            total_companies, 
                            success_count, 
                            error_count,
                            f"Failed to enrich {company_name}: {enriched_lead.get('EnrichmentNotes', '')}"
                        )
                
                # Update session state
                st.session_state.enriched_data = enrichment_df
                st.session_state.processing_status['is_processing'] = False
                
                # Show results
                if success_count > 0:
                    st.success(f"Successfully enriched {success_count} out of {total_companies} companies!")
                    
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
                else:
                    st.error("No companies were successfully enriched. Please check API keys and data.")
            
            # After enrichment, go to Results tab
            enrichment_tabs[4].is_selected = True
    
    # Apollo.io API tab
    with enrichment_tabs[1]:
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
            def update_apollo_progress(progress, current, total, success_count):
                # Update progress bar
                progress_bar.progress(progress)
                # Update status text
                status_text.text(f"Enriching company {current}/{total} ({success_count} enriched successfully)")
                # Update session state
                st.session_state.processing_status['progress'] = progress
                st.session_state.processing_status['success_count'] = success_count
            
            # Perform enrichment
            with st.spinner("Enriching data with Apollo.io..."):
                enriched_df = apollo_service.bulk_enrich_leads(enrichment_df, update_callback=update_apollo_progress)
                
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
            enrichment_tabs[4].is_selected = True
    
    # Crunchbase API tab
    with enrichment_tabs[2]:
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
            enrichment_tabs[4].is_selected = True
    
    # Web Scraping tab
    with enrichment_tabs[3]:
        st.write("Scrape additional information from company websites.")
        
        # Options for web scraping
        scrape_options = st.multiselect(
            "Select information to scrape:",
            ["Company Description", "Social Media Links", "Contact Information"],
            ["Company Description", "Social Media Links", "Contact Information"],
            key="web_scraping_tab_options"
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
            enrichment_tabs[4].is_selected = True
    
    # Results tab
    with enrichment_tabs[4]:
        st.write("View enrichment results and data quality.")
        
        if st.session_state.enriched_data is not None:
            enriched_df = st.session_state.enriched_data.copy()
            
            # Display enrichment statistics
            st.subheader("Enrichment Statistics")
            
            success_count = (enriched_df['EnrichmentStatus'] == 'Success').sum()
            pending_count = (enriched_df['EnrichmentStatus'] == 'Pending').sum()
            failed_count = (enriched_df['EnrichmentStatus'] == 'Failed').sum()
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Successfully Enriched", success_count)
            with col2:
                st.metric("Pending Enrichment", pending_count)
            with col3:
                st.metric("Failed Enrichment", failed_count)
            
            # Display enrichment source statistics
            st.subheader("Enrichment Sources")
            
            # Count companies by enrichment source
            source_counts = {}
            for _, row in enriched_df.iterrows():
                source = row.get('EnrichmentSource', 'Unknown')
                if source:
                    if isinstance(source, str):
                        source_counts[source] = source_counts.get(source, 0) + 1
            
            # Display source statistics
            if source_counts:
                source_df = pd.DataFrame({
                    'Source': list(source_counts.keys()),
                    'Count': list(source_counts.values())
                })
                source_df = source_df.sort_values('Count', ascending=False)
                
                st.bar_chart(source_df.set_index('Source'))
            
            # Display enrichment messages if available
            if 'processing_status' in st.session_state and st.session_state.processing_status.get('messages'):
                st.subheader("Enrichment Process Log")
                
                with st.expander("View Enrichment Log", expanded=False):
                    for message in st.session_state.processing_status.get('messages', []):
                        st.text(message)
            
            # Display tech leadership information
            st.subheader("Technology Leadership Information")
            
            # Count companies with tech leadership data
            has_tech_leadership = 0
            for _, row in enriched_df.iterrows():
                if isinstance(row.get('TechLeadership'), list) and len(row.get('TechLeadership', [])) > 0:
                    has_tech_leadership += 1
            
            st.metric("Companies with Tech Leadership Data", has_tech_leadership)
            
            # Display tech stack information
            st.subheader("Technology Stack Information")
            
            # Count companies with technology data
            has_tech_data = (enriched_df['CompanyTechnology'].notna() & 
                            (enriched_df['CompanyTechnology'] != '')).sum()
            
            # Count companies with tech job listings
            has_job_listings = 0
            for _, row in enriched_df.iterrows():
                if isinstance(row.get('TechJobListings'), list) and len(row.get('TechJobListings', [])) > 0:
                    has_job_listings += 1
            
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Companies with Tech Stack Data", has_tech_data)
            with col2:
                st.metric("Companies with Tech Job Listings", has_job_listings)
            
            # Display company size and funding information
            st.subheader("Company Size and Funding")
            
            # Count companies with size data
            has_size_data = (enriched_df['CompanySize'].notna() & 
                            (enriched_df['CompanySize'] != '')).sum()
            
            # Count companies with funding data
            has_funding_data = (enriched_df['CompanyFunding'].notna() & 
                               (enriched_df['CompanyFunding'] != '')).sum()
            
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Companies with Size Data", has_size_data)
            with col2:
                st.metric("Companies with Funding Data", has_funding_data)
            
            # Display sample of enriched data
            st.subheader("Sample Enriched Data")
            
            # Select a sample company with good enrichment
            sample_companies = enriched_df[enriched_df['EnrichmentStatus'] == 'Success'].head(5)
            
            if not sample_companies.empty:
                for idx, row in sample_companies.iterrows():
                    with st.expander(f"Company: {row.get('Company', 'Unknown')} (Source: {row.get('EnrichmentSource', 'Unknown')})"):
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.write("**Basic Information**")
                            st.write(f"**Industry:** {row.get('Industry', 'N/A')}")
                            st.write(f"**Size:** {row.get('CompanySize', 'N/A')} employees")
                            st.write(f"**Founded:** {row.get('CompanyFounded', 'N/A')}")
                            st.write(f"**Location:** {row.get('CompanyLocation', 'N/A')}")
                            
                            if row.get('CompanyFunding'):
                                st.write(f"**Total Funding:** {row.get('CompanyFunding', 'N/A')}")
                                if row.get('CompanyLatestFundingDate'):
                                    st.write(f"**Latest Funding:** {row.get('CompanyLatestFundingStage', 'N/A')} on {row.get('CompanyLatestFundingDate', 'N/A')}")
                        
                        with col2:
                            st.write("**Technology Information**")
                            if row.get('CompanyTechnology'):
                                st.write(f"**Tech Stack:** {row.get('CompanyTechnology', 'N/A')}")
                            
                            # Display engineering headcount if available
                            eng_headcount = row.get('EngineeringHeadcount', 0)
                            it_headcount = row.get('ITHeadcount', 0)
                            product_headcount = row.get('ProductHeadcount', 0)
                            data_science_headcount = row.get('DataScienceHeadcount', 0)
                            
                            if any([eng_headcount, it_headcount, product_headcount, data_science_headcount]):
                                st.write("**Tech Department Sizes:**")
                                if eng_headcount:
                                    st.write(f"- Engineering: {eng_headcount}")
                                if it_headcount:
                                    st.write(f"- IT: {it_headcount}")
                                if product_headcount:
                                    st.write(f"- Product: {product_headcount}")
                                if data_science_headcount:
                                    st.write(f"- Data Science: {data_science_headcount}")
                        
                        # Display tech leadership contacts if available
                        tech_leaders = row.get('TechLeadership', [])
                        if tech_leaders and len(tech_leaders) > 0:
                            st.write("**Technology Leadership:**")
                            for leader in tech_leaders:
                                st.write(f"- **{leader.get('name', 'Unknown')}**, {leader.get('title', 'N/A')}")
                                contact_info = []
                                if leader.get('email'):
                                    contact_info.append(f"Email: {leader.get('email')}")
                                if leader.get('phone'):
                                    contact_info.append(f"Phone: {leader.get('phone')}")
                                if leader.get('linkedin'):
                                    contact_info.append(f"[LinkedIn Profile]({leader.get('linkedin')})")
                                
                                if contact_info:
                                    st.write("  " + " | ".join(contact_info))
                        
                        # Display tech job listings if available
                        tech_jobs = row.get('TechJobListings', [])
                        if tech_jobs and len(tech_jobs) > 0:
                            st.write("**Technology Job Listings:**")
                            for job in tech_jobs[:3]:  # Show up to 3 jobs
                                st.write(f"- **{job.get('title', 'Unknown Position')}**")
                                if job.get('url'):
                                    st.write(f"  [View Job Listing]({job.get('url')})")
            
            # Display full enriched data table
            st.subheader("Full Enriched Data")
            
            # Select columns to display
            display_columns = [
                'Company', 'CompanyWebsite', 'Industry', 'CompanySize', 
                'CompanyFunding', 'CompanyLocation', 'EnrichmentStatus', 'EnrichmentSource'
            ]
            
            # Filter columns that exist in the DataFrame
            display_columns = [col for col in display_columns if col in enriched_df.columns]
            
            # Display the table
            st.dataframe(enriched_df[display_columns])
            
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