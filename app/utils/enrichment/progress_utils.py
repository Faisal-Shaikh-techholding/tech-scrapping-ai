#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Progress Utilities for Enrichment

This module contains utility functions for tracking and updating progress
during data enrichment operations.
"""

import streamlit as st

def create_progress_tracker(operation_name):
    """
    Create a progress tracker with UI elements and session state updates.
    
    Args:
        operation_name: Name of the operation being tracked
        
    Returns:
        Dictionary containing progress bar, status text, and update function
    """
    # Create UI elements
    progress_bar = st.progress(0)
    status_text = st.empty()
    stop_button_container = st.empty()
    
    # Initialize or reset processing status
    st.session_state.processing_status = {
        'is_processing': True,
        'current_operation': operation_name,
        'progress': 0,
        'total': 0,
        'success_count': 0,
        'error_count': 0,
        'messages': [],
        'stop_requested': False
    }
    
    # Create stop button
    if stop_button_container.button(f"Stop {operation_name}", key=f"stop_{operation_name.lower().replace(' ', '_')}"):
        st.session_state.processing_status['stop_requested'] = True
        st.warning("Stopping enrichment process... Please wait for current operations to complete.")
    
    def update_progress(progress, current, total, success_count, error_count=0, message=""):
        """
        Update progress UI and session state.
        
        Args:
            progress: Progress value (0-1)
            current: Current item number
            total: Total items
            success_count: Number of successful operations
            error_count: Number of failed operations
            message: Optional message to display
        """
        # Update progress bar
        progress_bar.progress(progress)
        
        # Update status text
        status_text.text(f"Processing {current}/{total} ({success_count} successful)")
        
        # Update session state
        st.session_state.processing_status['progress'] = progress
        st.session_state.processing_status['success_count'] = success_count
        st.session_state.processing_status['error_count'] = error_count
        
        if message:
            st.session_state.processing_status['messages'].append(message)
            print(message)
    
    print("Progress tracker created.")
    
    return {
        'progress_bar': progress_bar,
        'status_text': status_text,
        'update': update_progress,
        'stop_button': stop_button_container
    }

def should_stop_processing():
    """Check if processing should be stopped."""
    return st.session_state.processing_status.get('stop_requested', False)

def display_enrichment_results(success_count, total_count):
    """Display enrichment results with appropriate message."""
    if success_count > 0:
        st.success(f"Successfully enriched {success_count} out of {total_count} companies!")
        return True
    else:
        st.error("No companies were successfully enriched. Please check API keys and data.")
        return False 