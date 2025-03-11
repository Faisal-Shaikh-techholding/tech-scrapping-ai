#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Salesforce Export Component

This module renders the Salesforce export interface and handles the submission
of lead data to Salesforce CRM.
"""

import streamlit as st
import pandas as pd
import logging
import time
from app.utils.session_state import go_to_step
from app.services.salesforce_service import SalesforceService

logger = logging.getLogger('csv_processor')

def render_salesforce_export():
    """Render the Salesforce export interface."""
    
    st.header("Export to Salesforce")
    
    # Check if final data exists
    if st.session_state.final_data is None:
        st.error("No data available for export. Please complete previous steps first.")
        
        if st.button("Go to Data Editor", key="goto_edit_from_export"):
            go_to_step("edit")
            st.rerun()
        return
    
    # Get the data
    final_df = st.session_state.final_data.copy()
    
    # Check Salesforce credentials
    sf_creds = st.session_state.api_keys.get('salesforce', {})
    sf_username = sf_creds.get('username', '')
    sf_password = sf_creds.get('password', '')
    sf_token = sf_creds.get('security_token', '')
    sf_domain = sf_creds.get('domain', 'login')
    
    credentials_configured = sf_username and sf_password and sf_token
    
    if not credentials_configured:
        st.warning("Salesforce credentials not fully configured. Please configure in the sidebar.")
    
    st.markdown("""
    Send your selected leads to Salesforce CRM. 
    
    **Before proceeding:**
    1. Ensure you've selected the leads you want to export
    2. Verify that Salesforce credentials are configured correctly
    3. Review the field mappings below
    """)
    
    # Selected leads statistics
    if 'Selected' in final_df.columns:
        selected_df = final_df[final_df['Selected'] == True]
        selected_count = len(selected_df)
        
        if selected_count == 0:
            st.warning("No leads selected for export. Please go back to select leads.")
            
            if st.button("Back to Data Editor", key="back_to_editor"):
                go_to_step("edit")
                st.rerun()
            return
        
        st.success(f"{selected_count} leads selected for export to Salesforce")
    else:
        st.error("Selection column not found. Please go back to the editing step.")
        
        if st.button("Go to Data Editor", key="no_selection_goto_edit"):
            go_to_step("edit")
            st.rerun()
        return
    
    # Show field mappings
    st.subheader("Salesforce Field Mappings")
    
    # Define standard Salesforce Lead fields and their mappings
    sf_fields = {
        "FirstName": "FirstName",
        "LastName": "LastName",
        "Company": "Company",
        "Email": "Email",
        "Phone": "Phone",
        "JobTitle": "Title",
        "Industry": "Industry",
        "CompanyWebsite": "Website",
        "CompanyDescription": "Description",
        "CompanyLocation": "Address",
        "EnrichmentSource": "LeadSource"
    }
    
    # Create a dataframe showing the mappings
    mapping_data = []
    for app_field, sf_field in sf_fields.items():
        if app_field in final_df.columns:
            # Get a sample value
            sample_value = selected_df[app_field].iloc[0] if not selected_df[app_field].empty else ""
            mapping_data.append((app_field, sf_field, sample_value))
    
    if mapping_data:
        mapping_df = pd.DataFrame(mapping_data, columns=["Application Field", "Salesforce Field", "Sample Value"])
        st.table(mapping_df)
    
    # Show preview of selected leads
    with st.expander("Preview Selected Leads", expanded=False):
        st.dataframe(selected_df, height=200)
    
    # Export button
    st.subheader("Export to Salesforce")
    
    if credentials_configured:
        if st.button("Send to Salesforce", key="send_to_salesforce", disabled=not credentials_configured):
            with st.spinner("Connecting to Salesforce..."):
                # Initialize Salesforce service
                sf_service = SalesforceService(
                    username=sf_username,
                    password=sf_password,
                    security_token=sf_token,
                    domain=sf_domain
                )
                
                # Check connection
                connection_success = sf_service.connect()
                
                if not connection_success:
                    st.error("Failed to connect to Salesforce. Please check your credentials.")
                    return
                
                st.success("Connected to Salesforce successfully!")
                
                # Create progress display
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                # Update session state with processing status
                st.session_state.processing_status = {
                    'is_processing': True,
                    'current_operation': 'Salesforce Export',
                    'progress': 0,
                    'total': selected_count,
                    'success_count': 0,
                    'error_count': 0,
                    'messages': []
                }
                
                # Define callback for progress updates
                def update_progress(progress, current, total, success_count):
                    # Update progress bar
                    progress_bar.progress(progress)
                    # Update status text
                    status_text.text(f"Submitting lead {current}/{total} ({success_count} submitted successfully)")
                    # Update session state
                    st.session_state.processing_status['progress'] = progress
                    st.session_state.processing_status['success_count'] = success_count
                
                # Submit leads to Salesforce
                with st.spinner("Submitting leads to Salesforce..."):
                    updated_df, results = sf_service.bulk_create_leads(
                        final_df, 
                        selected_only=True,
                        update_callback=update_progress
                    )
                
                # Update session state
                st.session_state.final_data = updated_df
                st.session_state.export_results = results
                st.session_state.processing_status['is_processing'] = False
                
                # Show results
                success_count = len(results['success'])
                failure_count = len(results['failures'])
                
                if success_count > 0:
                    st.success(f"Successfully submitted {success_count} leads to Salesforce!")
                
                if failure_count > 0:
                    st.error(f"Failed to submit {failure_count} leads. See details below.")
                    
                    # Show failed submissions
                    with st.expander("View Failed Submissions", expanded=True):
                        for failure in results['failures']:
                            st.error(f"Error: {failure.get('errors', 'Unknown error')}")
                            st.write(f"Lead: {failure.get('data', {}).get('FirstName', '')} {failure.get('data', {}).get('LastName', '')}")
                
                # Show Salesforce IDs for successful submissions
                if success_count > 0:
                    with st.expander("View Successful Submissions", expanded=False):
                        success_data = []
                        for success in results['success']:
                            success_data.append({
                                'Name': f"{success.get('data', {}).get('FirstName', '')} {success.get('data', {}).get('LastName', '')}",
                                'Company': success.get('data', {}).get('Company', ''),
                                'Salesforce ID': success.get('id', '')
                            })
                        
                        if success_data:
                            st.dataframe(pd.DataFrame(success_data))
    else:
        st.info("Please configure Salesforce credentials in the sidebar to enable export.")
    
    # Navigation buttons
    st.subheader("Options")
    
    if st.button("Back to Data Editor", key="back_to_editor_from_export"):
        go_to_step("edit")
        st.rerun()
    
    # Final processing step - reset button
    if st.button("Start New Upload", key="start_new_upload"):
        # Reset session state but keep API keys
        saved_api_keys = st.session_state.api_keys.copy()
        
        # Clear processing data
        st.session_state.raw_data = None
        st.session_state.processed_data = None
        st.session_state.enriched_data = None
        st.session_state.final_data = None
        st.session_state.current_step = "upload"
        
        # Restore API keys
        st.session_state.api_keys = saved_api_keys
        
        # Navigate to upload step
        go_to_step("upload")
        st.rerun() 