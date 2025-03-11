#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sidebar Component

This module renders the sidebar for navigation and configuration.
"""

import streamlit as st
import logging
from app.utils.session_state import go_to_step, reset_session_state

logger = logging.getLogger('csv_processor')

def render_sidebar():
    """Render the sidebar for navigation and configuration."""
    
    st.sidebar.title("Process Steps")
    
    # Define the steps in the workflow
    steps = [
        ("upload", "1. Upload CSV", "📤"),
        ("preview", "2. Data Preview", "👁️"),
        ("enrich", "3. Data Enrichment", "🔍"),
        ("edit", "4. Review & Edit", "✏️"),
        ("export", "5. Salesforce Export", "🚀")
    ]
    
    # Navigation buttons
    for step_id, step_name, step_icon in steps:
        # Check if we can navigate to this step
        can_navigate = _can_navigate_to(step_id)
        
        # Highlight current step
        is_current = st.session_state.current_step == step_id
        
        # Create button with conditional formatting
        if is_current:
            st.sidebar.button(
                f"{step_icon} {step_name} ✓", 
                key=f"nav_{step_id}",
                disabled=True,
                help=f"You are currently at the {step_name} step"
            )
        elif can_navigate:
            if st.sidebar.button(
                f"{step_icon} {step_name}", 
                key=f"nav_{step_id}",
                help=f"Navigate to {step_name} step"
            ):
                go_to_step(step_id)
                st.rerun()
        else:
            st.sidebar.button(
                f"{step_icon} {step_name}", 
                key=f"nav_{step_id}",
                disabled=True,
                help="Complete the previous steps first"
            )
    
    st.sidebar.markdown("---")
    
    # API Configuration section
    st.sidebar.subheader("API Configuration")
    
    # Initialize API keys in session state if they don't exist
    if 'api_keys' not in st.session_state:
        st.session_state.api_keys = {
            'apollo': {},
            'salesforce': {},
            'crunchbase': {}
        }
    
    # Ensure api_keys has the correct structure
    if not isinstance(st.session_state.api_keys, dict):
        st.session_state.api_keys = {
            'apollo': {},
            'salesforce': {},
            'crunchbase': {}
        }
    
    # Ensure apollo is a dictionary
    if 'apollo' not in st.session_state.api_keys:
        st.session_state.api_keys['apollo'] = {}
    elif not isinstance(st.session_state.api_keys['apollo'], dict):
        # Convert old string format to new dictionary format
        old_key = st.session_state.api_keys['apollo']
        st.session_state.api_keys['apollo'] = {'api_key': old_key} if old_key else {}
    
    # Ensure salesforce is a dictionary
    if 'salesforce' not in st.session_state.api_keys:
        st.session_state.api_keys['salesforce'] = {}
    elif not isinstance(st.session_state.api_keys['salesforce'], dict):
        st.session_state.api_keys['salesforce'] = {}
    
    # Ensure crunchbase is a dictionary
    if 'crunchbase' not in st.session_state.api_keys:
        st.session_state.api_keys['crunchbase'] = {}
    elif not isinstance(st.session_state.api_keys['crunchbase'], dict):
        # Convert old string format to new dictionary format
        old_key = st.session_state.api_keys['crunchbase']
        st.session_state.api_keys['crunchbase'] = {'api_key': old_key} if old_key else {}
    
    # Apollo.io API key
    apollo_expander = st.sidebar.expander("Apollo.io API", expanded=False)
    with apollo_expander:
        apollo_key = st.text_input(
            "Apollo.io API Key",
            value=st.session_state.api_keys['apollo'].get('api_key', ''),
            type="password",
            key="apollo_api_key"
        )
        
        if apollo_key:
            st.session_state.api_keys['apollo'] = {'api_key': apollo_key}
            st.success("Apollo.io API key configured")
        else:
            st.info("Enter your Apollo.io API key to enable company data enrichment")
    
    # Crunchbase API key
    crunchbase_expander = st.sidebar.expander("Crunchbase API", expanded=False)
    with crunchbase_expander:
        crunchbase_key = st.text_input(
            "Crunchbase API Key",
            value=st.session_state.api_keys['crunchbase'].get('api_key', ''),
            type="password",
            key="crunchbase_api_key"
        )
        
        if crunchbase_key:
            st.session_state.api_keys['crunchbase'] = {'api_key': crunchbase_key}
            st.success("Crunchbase API key configured")
        else:
            st.info("Enter your Crunchbase API key to enable advanced company enrichment")
    
    # Salesforce credentials
    sf_expander = st.sidebar.expander("Salesforce Credentials", expanded=False)
    with sf_expander:
        sf_username = st.text_input(
            "Salesforce Username",
            value=st.session_state.api_keys['salesforce'].get('username', '')
        )
        
        sf_password = st.text_input(
            "Salesforce Password",
            value=st.session_state.api_keys['salesforce'].get('password', ''),
            type="password"
        )
        
        sf_token = st.text_input(
            "Salesforce Security Token",
            value=st.session_state.api_keys['salesforce'].get('security_token', ''),
            type="password"
        )
        
        sf_domain = st.selectbox(
            "Salesforce Domain",
            options=["login", "test"],
            index=0 if st.session_state.api_keys['salesforce'].get('domain', 'login') == "login" else 1
        )
        
        if sf_username and sf_password and sf_token:
            st.session_state.api_keys['salesforce'] = {
                'username': sf_username,
                'password': sf_password,
                'security_token': sf_token,
                'domain': sf_domain
            }
            st.success("Salesforce credentials configured")
        else:
            st.info("Enter your Salesforce credentials to enable data export")
    
    st.sidebar.markdown("---")
    
    # About section
    about_expander = st.sidebar.expander("About", expanded=False)
    with about_expander:
        st.markdown("""
        ## AI-Powered CSV Processor
        
        This application helps you process company data from CSV files 
        and submit it to Salesforce CRM.
        
        **Features:**
        - Upload and process CSV files with company data
        - Enrich data with Apollo.io API and web scraping
        - Review and edit data before submission
        - Export selected companies to Salesforce
        
        **Need help?** Contact support@example.com
        """)
    
    # Reset button
    st.sidebar.markdown("---")
    if st.sidebar.button("Reset Application", key="reset_app"):
        # Confirm reset
        reset_confirmed = st.sidebar.checkbox("Confirm reset (this will clear all data)", key="reset_confirm")
        if reset_confirmed:
            from app.utils.session_state import reset_session_state
            reset_session_state()
            st.rerun()

def _can_navigate_to(step):
    """
    Check if we can navigate to the specified step.
    
    Args:
        step: Step ID to check
        
    Returns:
        bool: True if navigation is allowed, False otherwise
    """
    current_step = st.session_state.current_step
    
    # Allow navigation back to any previous step
    step_order = ["upload", "preview", "enrich", "edit", "export"]
    current_idx = step_order.index(current_step)
    target_idx = step_order.index(step)
    
    # Can always go back to a previous step
    if target_idx <= current_idx:
        return True
    
    # For forward navigation, check if we have the required data
    if step == "preview":
        return st.session_state.raw_data is not None
    elif step == "enrich":
        return st.session_state.processed_data is not None
    elif step == "edit":
        return st.session_state.enriched_data is not None
    elif step == "export":
        return st.session_state.final_data is not None
    
    # Default: allow navigation
    return True 