# pylint: disable=trailing-whitespace
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sidebar Component

This module renders the sidebar for navigation and configuration.
"""

import streamlit as st
from app.utils.session_state import go_to_step, reset_session_state

def render_sidebar():
    """Render the sidebar for navigation and configuration."""
    
    st.sidebar.title("Process Steps")
    
    # Define the steps in the simplified workflow
    steps = [
        ("upload", "1. Upload Data", "📤"),
        ("view", "2. View Data", "👁️"),
        ("enrich_export", "3. Enrich & Export", "🚀")
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
    
    # Ensure api_keys has the correct structure
    if 'api_keys' not in st.session_state:
        st.session_state.api_keys = {
            'apollo': '',
            'crunchbase': '',
            'salesforce': {
                'username': '',
                'password': '',
                'security_token': '',
                'domain': 'login'
            }
        }
    
    # Apollo.io API key
    apollo_expander = st.sidebar.expander("Apollo.io API", expanded=False)
    with apollo_expander:
        apollo_key = st.text_input(
            "Apollo.io API Key",
            value=st.session_state.api_keys.get('apollo', ''),
            type="password",
            key="apollo_api_key"
        )
        
        if apollo_key:
            st.session_state.api_keys['apollo'] = apollo_key
            st.success("Apollo.io API key configured")
        else:
            st.info("Enter your Apollo.io API key to enable company data enrichment")
    
    # Crunchbase API key
    crunchbase_expander = st.sidebar.expander("Crunchbase API", expanded=False)
    with crunchbase_expander:
        crunchbase_key = st.text_input(
            "Crunchbase API Key",
            value=st.session_state.api_keys.get('crunchbase', ''),
            type="password",
            key="crunchbase_api_key"
        )
        
        if crunchbase_key:
            st.session_state.api_keys['crunchbase'] = crunchbase_key
            st.success("Crunchbase API key configured")
        else:
            st.info("Enter your Crunchbase API key to enable advanced company enrichment")
    
    # Salesforce credentials
    sf_expander = st.sidebar.expander("Salesforce Credentials", expanded=False)
    with sf_expander:
        sf_creds = st.session_state.api_keys.get('salesforce', {})
        
        sf_username = st.text_input(
            "Salesforce Username",
            value=sf_creds.get('username', '')
        )
        
        sf_password = st.text_input(
            "Salesforce Password",
            value=sf_creds.get('password', ''),
            type="password"
        )
        
        sf_token = st.text_input(
            "Salesforce Security Token",
            value=sf_creds.get('security_token', ''),
            type="password"
        )
        
        sf_domain = st.selectbox(
            "Salesforce Domain",
            options=["login", "test"],
            index=0 if sf_creds.get('domain', 'login') == "login" else 1
        )
        
        if sf_username and sf_password:
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
        - Upload and process CSV/Excel files with company data
        - View, analyze, and clean your data before enrichment
        - Enrich data with Apollo.io API, Crunchbase, and web scraping
        - Export enriched companies to Salesforce
        
        **Need help?** Contact support@example.com
        """)
    
    # Reset button
    st.sidebar.markdown("---")
    if st.sidebar.button("Reset Application", key="reset_app"):
        # Confirm reset
        reset_confirmed = st.sidebar.checkbox("Confirm reset (this will clear all data)", key="reset_confirm")
        if reset_confirmed:
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
    step_order = ["upload", "view", "enrich_export"]
    current_idx = step_order.index(current_step)
    target_idx = step_order.index(step)
    
    # Can always go back to a previous step
    if target_idx <= current_idx:
        return True
    
    # For forward navigation, check if we have the required data
    if step == "view":
        return st.session_state.data is not None
    elif step == "enrich_export":
        return st.session_state.data is not None
    
    # Default: allow navigation
    return True 