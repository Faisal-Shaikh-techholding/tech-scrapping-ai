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
    success_count = (df['EnrichmentStatus'] == 'Success').sum()
    pending_count = (df['EnrichmentStatus'] == 'Pending').sum()
    failed_count = (df['EnrichmentStatus'] == 'Failed').sum()
    cancelled_count = (df['EnrichmentStatus'] == 'Cancelled').sum()
    
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
        if isinstance(row.get('TechLeadership'), list) and len(row.get('TechLeadership', [])) > 0:
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
    has_tech_data = (df['CompanyTechnology'].notna() & 
                    (df['CompanyTechnology'] != '')).sum()
    
    # Count companies with tech job listings
    has_job_listings = 0
    for _, row in df.iterrows():
        if isinstance(row.get('TechJobListings'), list) and len(row.get('TechJobListings', [])) > 0:
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
    has_size_data = (df['CompanySize'].notna() & 
                    (df['CompanySize'] != '')).sum()
    
    # Count companies with funding data
    has_funding_data = (df['CompanyFunding'].notna() & 
                       (df['CompanyFunding'] != '')).sum()
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Companies with Size Data", has_size_data)
    with col2:
        st.metric("Companies with Funding Data", has_funding_data)

def display_sample_companies(df, max_samples=5):
    """
    Display sample enriched companies.
    
    Args:
        df: DataFrame containing enriched data
        max_samples: Maximum number of samples to display
    """
    st.subheader("Sample Enriched Data")
    
    # Select a sample of companies with good enrichment
    sample_companies = df[df['EnrichmentStatus'] == 'Success'].head(max_samples)
    
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

def display_full_data_table(df):
    """
    Display the full enriched data table.
    
    Args:
        df: DataFrame containing enriched data
    """
    st.subheader("Full Enriched Data")
    
    # Select columns to display
    display_columns = [
        'Company', 'CompanyWebsite', 'Industry', 'CompanySize', 
        'CompanyFunding', 'CompanyLocation', 'EnrichmentStatus', 'EnrichmentSource'
    ]
    
    # Filter columns that exist in the DataFrame
    display_columns = [col for col in display_columns if col in df.columns]
    
    # Display the table
    st.dataframe(df[display_columns]) 