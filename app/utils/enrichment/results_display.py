#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Enrichment Results Display

This module contains functions for displaying enrichment results.
"""

import streamlit as st
import pandas as pd

def display_enrichment_statistics(df):
    """
    Display enrichment statistics.
    
    Args:
        df: DataFrame containing enriched data
    """
    st.subheader("Enrichment Statistics")
    
    print("Displaying enrichment statistics.")
    
    # Calculate statistics
    success_count = (df['enrichment_status'] == 'Completed').sum()
    pending_count = (df['enrichment_status'] == 'Pending').sum()
    failed_count = (df['enrichment_status'] == 'Failed').sum()
    cancelled_count = (df['enrichment_status'] == 'Cancelled').sum()
    
    # Display statistics
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Successfully Enriched", success_count)
    with col2:
        st.metric("Pending Enrichment", pending_count)
    with col3:
        st.metric("Failed Enrichment", failed_count + cancelled_count)

def display_enrichment_sources(df):
    """
    Display enrichment source statistics.
    
    Args:
        df: DataFrame containing enriched data
    """
    st.subheader("Enrichment Sources")
    
    # Count companies by enrichment source
    source_counts = {}
    for _, row in df.iterrows():
        source = row.get('enrichment_source', 'Unknown')
        
        if source != 'Unknown':
            if source not in source_counts:
                source_counts[source] = 0
            source_counts[source] += 1
    
    # Display as bar chart if we have sources
    if source_counts:
        source_df = pd.DataFrame({
            'Source': list(source_counts.keys()),
            'Count': list(source_counts.values())
        })
        
        st.bar_chart(source_df.set_index('Source'))
    else:
        st.info("No enrichment source data available.")

def display_enrichment_log():
    """Display the enrichment process log."""
    if 'processing_status' in st.session_state and st.session_state.processing_status.get('messages'):
        st.subheader("Enrichment Process Log")
        
        with st.expander("View Enrichment Log", expanded=False):
            for message in st.session_state.processing_status.get('messages', []):
                st.text(message)

def display_tech_leadership_info(df):
    """
    Display technology leadership information.
    
    Args:
        df: DataFrame containing enriched data
    """
    st.subheader("Technology Leadership Information")
    
    # Count companies with tech leadership data
    has_tech_leadership = 0
    for _, row in df.iterrows():
        if isinstance(row.get('tech_leadership'), list) and len(row.get('tech_leadership', [])) > 0:
            has_tech_leadership += 1
    
    st.metric("Companies with Tech Leadership Data", has_tech_leadership)

def display_tech_stack_info(df):
    """
    Display technology stack information.
    
    Args:
        df: DataFrame containing enriched data
    """
    st.subheader("Technology Stack Information")
    
    # Count companies with technology data
    has_tech_data = 0
    if 'tech_stack' in df.columns:
        has_tech_data = (df['tech_stack'].notna() & (df['tech_stack'] != '')).sum()
    
    # Count companies with tech job listings
    has_job_listings = 0
    for _, row in df.iterrows():
        if isinstance(row.get('tech_job_listings'), list) and len(row.get('tech_job_listings', [])) > 0:
            has_job_listings += 1
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Companies with Tech Stack Data", has_tech_data)
    with col2:
        st.metric("Companies with Tech Job Listings", has_job_listings)

def display_company_size_funding(df):
    """
    Display company size and funding information.
    
    Args:
        df: DataFrame containing enriched data
    """
    st.subheader("Company Size and Funding")
    
    # Count companies with size data
    has_size_data = 0
    if 'company_size' in df.columns:
        has_size_data = (df['company_size'].notna() & (df['company_size'] != '')).sum()
    
    # Count companies with funding data
    has_funding_data = 0
    if 'funding_amount' in df.columns:
        has_funding_data = (df['funding_amount'].notna() & (df['funding_amount'] != '')).sum()
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Companies with Size Data", has_size_data)
    with col2:
        st.metric("Companies with Funding Data", has_funding_data)

def display_sample_companies(df, max_samples=5):
    """
    Display a sample of enriched companies.
    
    Args:
        df: DataFrame containing enriched data
        max_samples: Maximum number of companies to display
    """
    st.subheader("Sample Enriched Companies")
    
    # Get successfully enriched companies
    enriched_df = df[df['enrichment_status'] == 'Completed'].copy()
    
    if len(enriched_df) == 0:
        st.info("No enriched companies to display.")
        return
    
    # Take a sample
    sample_df = enriched_df.sample(min(max_samples, len(enriched_df)))
    
    # Display each company
    for i, (_, row) in enumerate(sample_df.iterrows(), 1):
        company_name = row.get('company', f"Company {i}")
        website = row.get('website', 'No website')
        
        with st.expander(f"{company_name} ({website})", expanded=i==1):
            # Basic info
            st.write("#### Basic Information")
            col1, col2 = st.columns(2)
            with col1:
                st.write(f"**Industry:** {row.get('industry', 'Unknown')}")
                st.write(f"**Size:** {row.get('company_size', 'Unknown')}")
                st.write(f"**Enrichment Source:** {row.get('enrichment_source', 'Unknown')}")
            with col2:
                st.write(f"**Founded:** {row.get('founded_year', 'Unknown')}")
                st.write(f"**Location:** {row.get('location', 'Unknown')}")
                st.write(f"**Funding:** {row.get('funding_amount', 'Unknown')}")
            
            # Technology info
            st.write("#### Technology Information")
            col1, col2 = st.columns(2)
            with col1:
                st.write(f"**Tech Stack:** {row.get('tech_stack', 'Unknown')}")
                st.write(f"**Tech Leadership:** {row.get('tech_leadership', 'Unknown')}")
            with col2:
                st.write(f"**Tech Job Listings:** {row.get('tech_job_listings', 'None')}")
                st.write(f"**Social Media:** {row.get('social_media', 'None')}")
            
            # Description if available
            if 'company_description' in row and row['company_description']:
                st.write("#### Company Description")
                st.write(row['company_description'])

def display_full_data_table(df):
    """
    Display the full enriched data table.
    
    Args:
        df: DataFrame containing enriched data
    """
    st.subheader("Full Enriched Data")
    
    # Select columns to display
    display_columns = [
        'company', 'website', 'industry', 'company_size', 
        'funding_amount', 'location', 'enrichment_status', 'enrichment_source'
    ]
    
    # Filter columns that exist in the DataFrame
    existing_columns = [col for col in display_columns if col in df.columns]
    
    if not existing_columns:
        st.info("No enrichment data columns available to display.")
        return
    
    # Display the data
    st.dataframe(df[existing_columns], use_container_width=True) 