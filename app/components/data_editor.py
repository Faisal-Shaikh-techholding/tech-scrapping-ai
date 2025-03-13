#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Data Editor Component

This module renders the data editing interface for reviewing and modifying
enriched lead data before Salesforce export.
"""

import streamlit as st
import pandas as pd
import logging
from app.utils.session_state import go_to_step

logger = logging.getLogger('csv_processor')

def render_data_editor():
    """Render the data editor interface."""
    
    st.header("Review & Edit Data")
    
    # Check if enriched data exists
    if st.session_state.enriched_data is None:
        st.error("No enriched data available. Please complete previous steps first.")
        
        if st.button("Go to Data Enrichment", key="goto_enrich_from_edit"):
            go_to_step("enrich")
            st.rerun()
        return
    
    # Get the data
    if st.session_state.final_data is None or len(st.session_state.final_data) != len(st.session_state.enriched_data):
        # Initialize with enriched data or update if enriched data has changed
        st.session_state.final_data = st.session_state.enriched_data.copy()
        logger.info(f"Initialized final_data with {len(st.session_state.enriched_data)} companies from enriched_data")
    
    df = st.session_state.final_data.copy()
    
    # Log data size for debugging
    logger.info(f"Working with {len(df)} companies in data editor")
    
    st.markdown("""
    Review and edit the company data before exporting to Salesforce. Use the filters to narrow down the list, 
    select the companies you want to export, and make any necessary edits.
    """)
    
    # Company selection and filtering
    st.subheader("Filter Companies")
    
    # Add selection checkbox column if it doesn't exist
    if 'Selected' not in df.columns:
        df['Selected'] = True
    
    # Filtering options
    col1, col2 = st.columns(2)
    
    with col1:
        # Filter by enrichment status
        enrichment_status = st.multiselect(
            "Enrichment Status",
            options=sorted(df['enrichment_status'].unique()),
            default=sorted(df['enrichment_status'].unique())
        )
    
    with col2:
        # Filter by industry if available
        if 'industry' in df.columns and df['industry'].notna().any():
            industries = st.multiselect(
                "Industry",
                options=sorted(df['industry'].dropna().unique()),
                default=[]
            )
    
    # Apply filters
    filtered_df = df.copy()
    
    # Log before filtering
    logger.info(f"Before filtering: {len(filtered_df)} companies")
    
    if enrichment_status:
        filtered_df = filtered_df[filtered_df['enrichment_status'].isin(enrichment_status)]
        logger.info(f"After enrichment status filter: {len(filtered_df)} companies")
    
    if 'industries' in locals() and industries:
        # Handle potential NaN values in Industry column
        industry_filter = filtered_df['industry'].isin(industries)
        filtered_df = filtered_df[industry_filter]
        logger.info(f"After industry filter: {len(filtered_df)} companies")
    
    # Search functionality
    search_term = st.text_input("Search companies", "")
    if search_term:
        search_filter = (
            filtered_df['company'].str.contains(search_term, case=False, na=False) |
            filtered_df.get('company_description', '').str.contains(search_term, case=False, na=False) |
            filtered_df.get('industry', '').str.contains(search_term, case=False, na=False)
        )
        filtered_df = filtered_df[search_filter]
        logger.info(f"After search filter: {len(filtered_df)} companies")
    
    # Display filtered data count
    st.markdown(f"**Showing {len(filtered_df)} of {len(df)} companies**")
    
    # Bulk selection
    st.subheader("Selection")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("Select All Filtered"):
            for idx in filtered_df.index:
                df.at[idx, 'Selected'] = True
            st.session_state.final_data = df
            st.rerun()
    
    with col2:
        if st.button("Deselect All Filtered"):
            for idx in filtered_df.index:
                df.at[idx, 'Selected'] = False
            st.session_state.final_data = df
            st.rerun()
    
    with col3:
        # Show count of selected leads
        selected_count = df['Selected'].sum()
        st.metric("Selected Companies", selected_count)
    
    # Editable Data
    st.subheader("Review & Edit Company Data")
    
    # Create tabs for different views of the data
    data_tabs = st.tabs(["Basic Info", "Tech Leadership", "Tech Stack", "Funding"])
    
    # Basic Info tab
    with data_tabs[0]:
        # Highlight selected rows in the editor
        def highlight_selected(row):
            if row['Selected']:
                return ['background-color: #e6ffe6'] * len(row)
            return [''] * len(row)
        
        # Display editable dataframe
        edited_df = st.data_editor(
            filtered_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Selected": st.column_config.CheckboxColumn(
                    "Select",
                    help="Select companies to export",
                    default=True
                ),
                "company": st.column_config.TextColumn(
                    "Company Name",
                    width="large"
                ),
                "website": st.column_config.TextColumn(
                    "Website",
                    width="medium"
                ),
                "industry": st.column_config.TextColumn(
                    "Industry",
                    width="medium"
                ),
                "company_description": st.column_config.TextColumn(
                    "Description",
                    width="large"
                ),
                "location": st.column_config.TextColumn(
                    "Location",
                    width="medium"
                ),
                "enrichment_status": st.column_config.SelectboxColumn(
                    "Status",
                    options=["Completed", "Pending", "Failed", "Cancelled"],
                    width="small"
                )
            },
            disabled=["enrichment_status", "enrichment_source"],
            height=500,
        )
    
    # Tech Leadership tab
    with data_tabs[1]:
        st.write("View and edit technology leadership contacts")
        
        # Filter to only show companies with tech leadership data
        tech_leadership_companies = []
        for idx, row in filtered_df.iterrows():
            if isinstance(row.get('tech_leadership'), list) and len(row.get('tech_leadership', [])) > 0:
                tech_leadership_companies.append(idx)
        
        if tech_leadership_companies:
            tech_leaders_df = filtered_df.loc[tech_leadership_companies].copy()
            
            # Display companies with tech leadership
            for idx, row in tech_leaders_df.iterrows():
                with st.expander(f"{row['company']} - Tech Leadership"):
                    tech_leaders = row.get('tech_leadership', [])
                    
                    if not tech_leaders:
                        st.write("No technology leadership contacts found.")
                        continue
                    
                    # Create a table for each company's tech leaders
                    leaders_data = []
                    for leader in tech_leaders:
                        leaders_data.append({
                            "Name": leader.get('name', 'Unknown'),
                            "Title": leader.get('title', 'N/A'),
                            "Email": leader.get('email', 'N/A'),
                            "Phone": leader.get('phone', 'N/A'),
                            "LinkedIn": leader.get('linkedin', 'N/A')
                        })
                    
                    if leaders_data:
                        leaders_df = pd.DataFrame(leaders_data)
                        edited_leaders = st.data_editor(
                            leaders_df,
                            use_container_width=True,
                            hide_index=True,
                            column_config={
                                "Name": st.column_config.TextColumn("Name", width="medium"),
                                "Title": st.column_config.TextColumn("Title", width="medium"),
                                "Email": st.column_config.TextColumn("Email", width="medium"),
                                "Phone": st.column_config.TextColumn("Phone", width="medium"),
                                "LinkedIn": st.column_config.LinkColumn("LinkedIn", width="medium")
                            }
                        )
                        
                        # Update the tech leadership data if edited
                        if not edited_leaders.equals(leaders_df):
                            updated_leaders = []
                            for i, leader_row in edited_leaders.iterrows():
                                updated_leaders.append({
                                    "name": leader_row["Name"],
                                    "title": leader_row["Title"],
                                    "email": leader_row["Email"],
                                    "phone": leader_row["Phone"],
                                    "linkedin": leader_row["LinkedIn"]
                                })
                            
                            # Update the filtered dataframe
                            filtered_df.at[idx, 'tech_leadership'] = updated_leaders
                            
                            # Update the main dataframe
                            df.at[idx, 'tech_leadership'] = updated_leaders
        else:
            st.info("No companies with technology leadership data found in the current selection.")
    
    # Tech Stack tab
    with data_tabs[2]:
        st.write("View and edit technology stack information")
        
        # Filter to only show companies with tech stack data
        tech_stack_companies = filtered_df[
            (filtered_df['tech_stack'].notna() & (filtered_df['tech_stack'] != '')) |
            filtered_df.apply(lambda row: isinstance(row.get('tech_job_listings'), list) and len(row.get('tech_job_listings', [])) > 0, axis=1)
        ]
        
        if not tech_stack_companies.empty:
            for idx, row in tech_stack_companies.iterrows():
                with st.expander(f"{row['company']} - Tech Stack"):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write("**Current Technologies:**")
                        tech_stack = row.get('tech_stack', '')
                        edited_tech = st.text_area(
                            "Technologies", 
                            value=tech_stack,
                            key=f"tech_{idx}",
                            height=100
                        )
                        
                        # Update if changed
                        if edited_tech != tech_stack:
                            filtered_df.at[idx, 'tech_stack'] = edited_tech
                            df.at[idx, 'tech_stack'] = edited_tech
                    
                    with col2:
                        st.write("**Tech Department Sizes:**")
                        eng_headcount = row.get('engineering_headcount', 0)
                        it_headcount = row.get('it_headcount', 0)
                        product_headcount = row.get('product_headcount', 0)
                        data_science_headcount = row.get('data_science_headcount', 0)
                        
                        edited_eng = st.number_input(
                            "Engineering", 
                            value=int(eng_headcount) if eng_headcount else 0,
                            key=f"eng_{idx}"
                        )
                        edited_it = st.number_input(
                            "IT", 
                            value=int(it_headcount) if it_headcount else 0,
                            key=f"it_{idx}"
                        )
                        edited_product = st.number_input(
                            "Product", 
                            value=int(product_headcount) if product_headcount else 0,
                            key=f"product_{idx}"
                        )
                        edited_data = st.number_input(
                            "Data Science", 
                            value=int(data_science_headcount) if data_science_headcount else 0,
                            key=f"data_{idx}"
                        )
                        
                        # Update if changed
                        if edited_eng != eng_headcount:
                            filtered_df.at[idx, 'engineering_headcount'] = edited_eng
                            df.at[idx, 'engineering_headcount'] = edited_eng
                        if edited_it != it_headcount:
                            filtered_df.at[idx, 'it_headcount'] = edited_it
                            df.at[idx, 'it_headcount'] = edited_it
                        if edited_product != product_headcount:
                            filtered_df.at[idx, 'product_headcount'] = edited_product
                            df.at[idx, 'product_headcount'] = edited_product
                        if edited_data != data_science_headcount:
                            filtered_df.at[idx, 'data_science_headcount'] = edited_data
                            df.at[idx, 'data_science_headcount'] = edited_data
                    
                    # Display tech job listings if available
                    tech_jobs = row.get('tech_job_listings', [])
                    if tech_jobs and len(tech_jobs) > 0:
                        st.write("**Technology Job Listings:**")
                        for i, job in enumerate(tech_jobs):
                            st.markdown(f"**{i+1}. {job.get('title', 'Unknown Position')}**")
                            if job.get('description'):
                                st.markdown(f"*Description:* {job.get('description')[:200]}...")
                            if job.get('url'):
                                st.markdown(f"[View Job Listing]({job.get('url')})")
                            st.markdown("---")
        else:
            st.info("No companies with technology stack data found in the current selection.")
    
    # Funding tab
    with data_tabs[3]:
        st.write("View and edit funding information")
        
        # Filter to only show companies with funding data
        funding_companies = filtered_df[
            (filtered_df['funding_amount'].notna() & (filtered_df['funding_amount'] != ''))
        ]
        
        if not funding_companies.empty:
            for idx, row in funding_companies.iterrows():
                with st.expander(f"{row['company']} - Funding"):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write("**Funding Information:**")
                        funding = row.get('funding_amount', 'N/A')
                        
                        st.write(f"**Total Funding:** {funding}")
                        
                        edited_funding = st.text_input(
                            "Edit Funding", 
                            value=funding,
                            key=f"funding_{idx}"
                        )
                        
                        # Update if changed
                        if edited_funding != funding:
                            filtered_df.at[idx, 'funding_amount'] = edited_funding
                            df.at[idx, 'funding_amount'] = edited_funding
                    
                    with col2:
                        st.write("**Latest Funding Round:**")
                        funding_date = row.get('latest_funding_date', 'N/A')
                        funding_stage = row.get('latest_funding_stage', 'N/A')
                        
                        st.write(f"**Date:** {funding_date}")
                        st.write(f"**Stage:** {funding_stage}")
                        
                        edited_stage = st.text_input(
                            "Edit Stage", 
                            value=funding_stage,
                            key=f"stage_{idx}"
                        )
                        
                        # Update if changed
                        if edited_stage != funding_stage:
                            filtered_df.at[idx, 'latest_funding_stage'] = edited_stage
                            df.at[idx, 'latest_funding_stage'] = edited_stage
        else:
            st.info("No companies with funding data found in the current selection.")
    
    # Update the session state with the edited data (preserving rows not in filtered view)
    if edited_df is not None and not edited_df.empty:
        logger.info(f"Updating session state with {len(edited_df)} edited companies")
        for idx, row in edited_df.iterrows():
            for col in edited_df.columns:
                df.at[idx, col] = row[col]
    
    # Ensure we're not losing any data
    if len(df) != len(st.session_state.enriched_data):
        logger.warning(f"Data size mismatch: final_data has {len(df)} rows but enriched_data has {len(st.session_state.enriched_data)} rows")
        # If we somehow lost data, restore from enriched_data but preserve edits
        if len(df) < len(st.session_state.enriched_data):
            logger.info("Restoring missing data from enriched_data")
            missing_indices = set(st.session_state.enriched_data.index) - set(df.index)
            for idx in missing_indices:
                df.loc[idx] = st.session_state.enriched_data.loc[idx]
    
    st.session_state.final_data = df.copy()
    logger.info(f"Final data updated with {len(df)} companies")
    
    # Show summary of changes
    st.subheader("Summary")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric("Total Companies", len(df))
    
    with col2:
        st.metric("Selected for Export", df['Selected'].sum())
    
    # Navigation buttons
    st.subheader("Options")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("Back to Enrichment", key="back_to_enrich"):
            go_to_step("enrich")
            st.rerun()
    
    with col2:
        if df['Selected'].sum() > 0:
            if st.button("Continue to Export", key="continue_to_export"):
                go_to_step("export")
                st.rerun()
        else:
            st.warning("Please select at least one company to continue to export.")
    
    with col3:
        # Debug button to show data sizes
        if st.button("Debug Data", key="debug_data"):
            st.info(f"""
            Data sizes:
            - Enriched data: {len(st.session_state.enriched_data) if st.session_state.enriched_data is not None else 0} companies
            - Final data: {len(st.session_state.final_data) if st.session_state.final_data is not None else 0} companies
            - Current filtered view: {len(filtered_df)} companies
            """)
            
            # Show sample of enriched data
            if st.session_state.enriched_data is not None and not st.session_state.enriched_data.empty:
                st.write("Sample from enriched_data:")
                st.write(st.session_state.enriched_data[['company', 'industry', 'enrichment_status']].head()) 