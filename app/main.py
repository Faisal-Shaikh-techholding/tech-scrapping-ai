#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AI-Powered CSV Processor for Company Data

This application allows users to upload CSV files with company information,
enrich the data, and submit it to Salesforce CRM.

Author: Your Name
"""

import streamlit as st
import pandas as pd
import os
import sys
import logging
from datetime import datetime

# Add the app directory to the path so we can import our modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import utility modules
from app.utils.session_state import initialize_session_state, go_to_step

# Import components
from app.components.sidebar import render_sidebar
from app.components.csv_upload import render_csv_upload
from app.components.data_preview import render_data_preview
from app.components.data_enrichment import render_data_enrichment
from app.components.data_editor import render_data_editor
from app.components.salesforce_export import render_salesforce_export



# App title and description
APP_TITLE = "AI-Powered CSV Processor for Company Data"
APP_DESCRIPTION = """
Upload CSV files with company information, enrich data using external APIs and web scraping, 
and submit the enriched data to Salesforce CRM.
"""

# Custom CSS
def set_custom_css():
    """Set custom CSS for the app."""
    st.markdown("""
    <style>
    .main .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
    h1, h2, h3 {
        margin-top: 1rem;
        margin-bottom: 1rem;
    }
    .stProgress > div > div {
        background-color: #4CAF50;
    }
    </style>
    """, unsafe_allow_html=True)

def main():
    """Main function to run the Streamlit app."""
    # Set page config
    st.set_page_config(
        page_title=APP_TITLE,
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Set custom CSS
    set_custom_css()
    
    # Initialize session state
    initialize_session_state()
    
    # Render sidebar
    render_sidebar()
    
    # Main content area
    st.title(APP_TITLE)
    
    # Render the appropriate component based on the current step
    current_step = st.session_state.current_step
    
    if current_step == "upload":
        render_csv_upload()
    elif current_step == "preview":
        render_data_preview()
    elif current_step == "enrich":
        render_data_enrichment()
    elif current_step == "edit":
        render_data_editor()
    elif current_step == "export":
        render_salesforce_export()
    else:
        st.error(f"Unknown step: {current_step}")
    
    # Footer
    st.markdown("---")
    st.markdown("© 2025 AI-Powered CSV Processor. All rights reserved.")

if __name__ == "__main__":
    main() 