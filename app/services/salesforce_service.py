#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Salesforce API Service

This module handles integration with the Salesforce API for lead submission.
"""

import pandas as pd
import logging
import time
import random
from typing import Dict, List, Any, Tuple, Optional
from collections import deque
from simple_salesforce import Salesforce, SalesforceError

logger = logging.getLogger('csv_processor')

class SalesforceService:
    """Service for interacting with the Salesforce API."""
    
    # API limits
    MAX_CALLS_PER_WINDOW = 100  # Maximum API calls per window
    WINDOW_SECONDS = 20  # Time window in seconds
    
    def __init__(self, username: str, password: str, security_token: str, domain: str = 'login'):
        """
        Initialize the Salesforce service.
        
        Args:
            username (str): Salesforce username
            password (str): Salesforce password
            security_token (str): Salesforce security token
            domain (str, optional): Salesforce domain. Defaults to 'login'.
        """
        self.username = username
        self.password = password
        self.security_token = security_token
        self.domain = domain
        self.sf = None
        self.connected = False
        self.calls = deque()  # For rate limiting
    
    def connect(self) -> bool:
        """
        Connect to Salesforce API.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        if self.connected and self.sf:
            return True
        
        try:
            self.sf = Salesforce(
                username=self.username,
                password=self.password,
                security_token=self.security_token,
                domain=self.domain
            )
            self.connected = True
            logger.info("Connected to Salesforce successfully")
            return True
        except SalesforceError as e:
            logger.error("Failed to connect to Salesforce: %s", str(e))
            self.connected = False
            return False
        except Exception as e:
            logger.error("Unexpected error connecting to Salesforce: %s", str(e))
            self.connected = False
            return False
    
    def wait_if_needed(self):
        """
        Implement rate limiting to avoid hitting Salesforce API limits.
        Uses a sliding window approach.
        """
        now = time.time()
        
        # Remove calls that are outside the current window
        while self.calls and self.calls[0] < now - self.WINDOW_SECONDS:
            self.calls.popleft()
            
        # If we've reached the limit, wait until we can make another call
        if len(self.calls) >= self.MAX_CALLS_PER_WINDOW:
            # Calculate sleep time
            oldest_call = self.calls[0]
            sleep_time = oldest_call + self.WINDOW_SECONDS - now
            
            if sleep_time > 0:
                logger.warning("Salesforce rate limit reached. Sleeping for %.2f seconds", sleep_time)
                time.sleep(sleep_time)
                
                # After sleeping, remove expired calls again
                now = time.time()
                while self.calls and self.calls[0] < now - self.WINDOW_SECONDS:
                    self.calls.popleft()
        
        # Record this call
        self.calls.append(time.time())
    
    def create_lead(self, lead_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """
        Create a lead in Salesforce.
        
        Args:
            lead_data (Dict): Lead data to submit to Salesforce
            
        Returns:
            Tuple[bool, Dict]: Success status and result dict
        """
        if not self.connected:
            success = self.connect()
            if not success:
                return False, {"error": "Not connected to Salesforce"}
        
        # Apply rate limiting
        self.wait_if_needed()
        
        # Ensure only valid Salesforce fields are included
        sf_lead_data = self._prepare_lead_data(lead_data)
        
        try:
            # Create lead in Salesforce
            result = self.sf.Lead.create(sf_lead_data)
            
            # Check if creation was successful
            if result.get('success', False):
                logger.info("Lead created successfully in Salesforce with ID: %s", result.get('id', 'Unknown'))
                return True, {
                    "id": result.get('id'),
                    "success": True,
                    "message": "Lead created successfully"
                }
            else:
                logger.warning("Failed to create lead in Salesforce: %s", str(result))
                return False, {
                    "success": False,
                    "errors": result.get('errors', []),
                    "message": "Failed to create lead"
                }
                
        except SalesforceError as e:
            logger.error("Salesforce API error: %s", str(e))
            return False, {"error": str(e)}
        except Exception as e:
            logger.error("Unexpected error creating Salesforce lead: %s", str(e))
            return False, {"error": str(e)}
    
    def _prepare_lead_data(self, lead_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepare lead data for Salesforce submission by ensuring only valid fields are included.
        
        Args:
            lead_data (Dict): Raw lead data
            
        Returns:
            Dict: Prepared data for Salesforce
        """
        # Define standard Salesforce Lead fields
        sf_fields = {
            "FirstName", "LastName", "Company", "Email", "Phone", "Title",
            "Industry", "Website", "Description", "Address", "LeadSource",
            "City", "State", "PostalCode", "Country"
        }
        
        # Filter out non-Salesforce fields and empty values
        sf_lead_data = {
            k: v for k, v in lead_data.items() 
            if k in sf_fields and v is not None and v != ""
        }
        
        # Ensure required fields are present
        if "LastName" not in sf_lead_data or not sf_lead_data["LastName"]:
            if "FirstName" in sf_lead_data and sf_lead_data["FirstName"]:
                sf_lead_data["LastName"] = sf_lead_data["FirstName"]
            else:
                sf_lead_data["LastName"] = "Unknown"
                
        if "Company" not in sf_lead_data or not sf_lead_data["Company"]:
            sf_lead_data["Company"] = "Unknown"
        
        return sf_lead_data
    
    def bulk_create_leads(self, leads_df: pd.DataFrame, 
                          selected_only: bool = True,
                          update_callback=None) -> Tuple[pd.DataFrame, Dict[str, List]]:
        """
        Create multiple leads in Salesforce.
        
        Args:
            leads_df (pd.DataFrame): DataFrame containing lead data
            selected_only (bool, optional): Only process rows marked as selected
            update_callback (callable, optional): Callback function for progress updates
            
        Returns:
            Tuple[pd.DataFrame, Dict]: Updated DataFrame and results summary
        """
        if not self.connected:
            success = self.connect()
            if not success:
                # Update all rows with error status
                updated_df = leads_df.copy()
                updated_df['SFStatus'] = 'Error - Not connected to Salesforce'
                return updated_df, {
                    "success": [],
                    "failures": [{"error": "Not connected to Salesforce"}]
                }
        
        # Create a copy of the DataFrame to modify
        updated_df = leads_df.copy()
        
        # Filter to selected leads if requested
        if selected_only:
            process_mask = updated_df['Selected'] == True
        else:
            process_mask = pd.Series([True] * len(updated_df))
        
        # Skip if no leads to process
        if not process_mask.any():
            logger.warning("No leads selected for Salesforce submission")
            return updated_df, {
                "success": [],
                "failures": []
            }
        
        # Initialize results tracking
        results = {
            "success": [],
            "failures": []
        }
        
        # Count leads to process
        total_leads = process_mask.sum()
        processed_count = 0
        success_count = 0
        
        # Process each selected lead
        for index, row in updated_df[process_mask].iterrows():
            # Convert row to dict
            lead_data = row.to_dict()
            
            # Create lead in Salesforce
            success, result = self.create_lead(lead_data)
            
            # Update DataFrame with results
            if success:
                updated_df.at[index, 'SFStatus'] = 'Submitted'
                updated_df.at[index, 'SalesforceId'] = result.get('id', '')
                success_count += 1
                results["success"].append({
                    "index": index,
                    "id": result.get('id', ''),
                    "data": lead_data
                })
            else:
                updated_df.at[index, 'SFStatus'] = f"Error: {result.get('error', 'Unknown error')}"
                results["failures"].append({
                    "index": index,
                    "errors": result.get('error', 'Unknown error'),
                    "data": lead_data
                })
            
            processed_count += 1
            
            # Call update callback if provided
            if update_callback:
                progress = processed_count / total_leads
                update_callback(progress, processed_count, total_leads, success_count)
            
            # Add a small delay between submissions
            time.sleep(random.uniform(0.1, 0.5))
        
        logger.info("Salesforce lead submission completed. Success rate: %d/%d (%.1f%%)",
                   success_count, total_leads, (success_count / total_leads * 100) if total_leads > 0 else 0)
        
        return updated_df, results 