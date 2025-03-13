#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Session State Management

This module handles the initialization and management of Streamlit session state
for the simplified three-step workflow.
"""

import streamlit as st
import pandas as pd

def initialize_session_state():
    """
    Initialize Streamlit session state variables.
    These variables persist across reruns of the Streamlit app.
    """
    # Step management - Simplified to three steps
    if 'current_step' not in st.session_state:
        st.session_state.current_step = "upload"
    
    # Data storage - Simplified
    if 'data' not in st.session_state:
        st.session_state.data = None
    
    # API keys and credentials
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
    
    # Processing status
    if 'processing_status' not in st.session_state:
        st.session_state.processing_status = {
            'is_processing': False,
            'current_operation': '',
            'progress': 0,
            'total': 0,
            'success_count': 0,
            'error_count': 0,
            'messages': []
        }
    
    # Salesforce export results
    if 'export_results' not in st.session_state:
        st.session_state.export_results = {
            'success': [],
            'failures': []
        }
    
    print("Session state initialized")

def go_to_step(step):
    """
    Navigate to a specific step in the application workflow.
    
    Args:
        step (str): The step to navigate to
    """
    valid_steps = ["upload", "view", "enrich_export"]
    
    if step in valid_steps:
        st.session_state.current_step = step
        print(f"Navigated to step: {step}")
    else:
        print(f"Invalid step requested: {step}")
        st.error(f"Invalid step: {step}")

def reset_session_state():
    """Reset all session state variables to their initial values."""
    # Don't reset API keys
    saved_api_keys = st.session_state.api_keys.copy() if 'api_keys' in st.session_state else None
    
    # Clear all session state
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    
    # Re-initialize
    initialize_session_state()
    
    # Restore API keys
    if saved_api_keys:
        st.session_state.api_keys = saved_api_keys
    
    print("Session state reset") 