#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Crunchbase API Service

This module provides a service for enriching company data using the Crunchbase API.
"""

import requests
import time
import logging
import pandas as pd
from typing import Dict, Tuple, Any, Optional

logger = logging.getLogger('csv_processor')

class CrunchbaseService:
    """Service for interacting with the Crunchbase API."""
    
    BASE_URL = "https://api.crunchbase.com/api/v4"
    RATE_LIMIT = 10  # Requests per minute (adjust based on your plan)
    
    def __init__(self, api_key: str):
        """
        Initialize the Crunchbase service.
        
        Args:
            api_key: Crunchbase API key
        """
        self.api_key = api_key
        self.headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "x-cb-user-key": api_key
        }
        self.last_request_time = 0
    
    def _rate_limit(self):
        """Manage rate limiting for API requests."""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        
        # Ensure we don't exceed rate limits
        if time_since_last_request < (60 / self.RATE_LIMIT):
            sleep_time = (60 / self.RATE_LIMIT) - time_since_last_request
            logger.debug(f"Rate limiting: Sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def search_organization(self, company_name: str, domain: Optional[str] = None) -> Tuple[bool, Dict[str, Any]]:
        """
        Search for an organization by name or domain.
        
        Args:
            company_name: Name of the company to search for
            domain: Company website domain (optional)
            
        Returns:
            Tuple containing:
                - Boolean indicating success
                - Dictionary with organization data
        """
        self._rate_limit()
        
        url = f"{self.BASE_URL}/searches/organizations"
        
        # Build query using both name and domain if available
        query = f"{company_name}"
        if domain:
            query = f"{query} OR {domain}"
        
        payload = {
            "field_ids": [
                "identifier",
                "name",
                "short_description",
                "description",
                "website",
                "linkedin",
                "twitter",
                "facebook",
                "location_identifiers",
                "category_groups",
                "funding_total",
                "funding_rounds",
                "founded_on",
                "employee_count",
                "num_employees_enum",
                "operating_status"
            ],
            "query": query.strip(),
            "limit": 1  # Just get the best match
        }
        
        try:
            response = requests.post(url, json=payload, headers=self.headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('entities') and len(data['entities']) > 0:
                    return True, data['entities'][0]
                else:
                    logger.warning(f"No organizations found for {company_name}")
                    return False, {}
            else:
                logger.error(f"Error searching organizations: {response.status_code}, {response.text}")
                return False, {}
        
        except Exception as e:
            logger.error(f"Exception during Crunchbase search: {str(e)}")
            return False, {}
    
    def get_organization_details(self, uuid: str) -> Tuple[bool, Dict[str, Any]]:
        """
        Get detailed information about an organization.
        
        Args:
            uuid: Crunchbase UUID for the organization
            
        Returns:
            Tuple containing:
                - Boolean indicating success
                - Dictionary with detailed organization data
        """
        self._rate_limit()
        
        url = f"{self.BASE_URL}/entities/organizations/{uuid}"
        
        params = {
            "card_ids": "fields,founders,investors,funding_rounds,key_employees,categories"
        }
        
        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            
            if response.status_code == 200:
                return True, response.json()
            else:
                logger.error(f"Error getting organization details: {response.status_code}, {response.text}")
                return False, {}
        
        except Exception as e:
            logger.error(f"Exception during Crunchbase details fetch: {str(e)}")
            return False, {}
    
    def format_organization_data(self, org_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format the organization data from Crunchbase to a standardized format.
        
        Args:
            org_data (Dict): Raw organization data from Crunchbase
            
        Returns:
            Dict: Formatted organization data
        """
        properties = org_data.get("properties", {})
        
        formatted_data = {
            "Company": properties.get("name", ""),
            "CompanyWebsite": properties.get("website", {}).get("value", ""),
            "Industry": self._extract_industry(org_data),
            "CompanySize": self._extract_company_size(properties),
            "CompanyDescription": properties.get("short_description", ""),
            "CompanyLocation": self._extract_location(properties),
            "CompanyFounded": properties.get("founded_on", {}).get("value", ""),
            "CompanyFunding": properties.get("total_funding_usd", ""),
            "EnrichmentSource": "Crunchbase API",
            "EnrichmentStatus": "Success"
        }
        
        # Extract social links
        social_links = self._extract_social_links(properties)
        if social_links.get("linkedin"):
            formatted_data["CompanyLinkedIn"] = social_links.get("linkedin")
        if social_links.get("twitter"):
            formatted_data["CompanyTwitter"] = social_links.get("twitter")
        if social_links.get("facebook"):
            formatted_data["CompanyFacebook"] = social_links.get("facebook")
        
        return formatted_data
    
    def _extract_industry(self, org_data: Dict[str, Any]) -> str:
        """Extract industry categories from organization data."""
        try:
            categories = []
            
            # Get categories from relationships
            relationships = org_data.get("relationships", {})
            categories_data = relationships.get("categories", {}).get("items", [])
            
            for category in categories_data:
                properties = category.get("properties", {})
                category_name = properties.get("name")
                if category_name:
                    categories.append(category_name)
            
            return ", ".join(categories) if categories else ""
        except Exception as e:
            logger.error(f"Error extracting industry: {str(e)}")
            return ""
    
    def _extract_company_size(self, properties: Dict[str, Any]) -> str:
        """Extract company size information."""
        employee_count = properties.get('employee_count', '')
        if employee_count:
            return str(employee_count)
        
        # Fallback to employee range
        employee_range = properties.get('num_employees_enum', '')
        if employee_range:
            return employee_range
        
        return ''
    
    def _extract_location(self, properties: Dict[str, Any]) -> str:
        """Extract company location information."""
        try:
            locations = properties.get('location_identifiers', [])
            if locations:
                location_names = []
                for loc in locations:
                    if isinstance(loc, dict) and 'value' in loc:
                        location_names.append(loc['value'].get('properties', {}).get('name', ''))
                    elif isinstance(loc, dict) and 'properties' in loc:
                        location_names.append(loc['properties'].get('name', ''))
                    
                return ', '.join(filter(None, location_names))
            
            return ''
        
        except Exception as e:
            logger.error(f"Error extracting location: {str(e)}")
            return ''
    
    def _extract_social_links(self, properties: Dict[str, Any]) -> Dict[str, str]:
        """Extract social media links."""
        social_links = {}
        
        # Get LinkedIn URL
        linkedin = properties.get('linkedin', {}).get('value', '')
        if linkedin:
            social_links['linkedin'] = linkedin
        
        # Get Twitter URL
        twitter = properties.get('twitter', {}).get('value', '')
        if twitter:
            social_links['twitter'] = twitter
        
        # Get Facebook URL
        facebook = properties.get('facebook', {}).get('value', '')
        if facebook:
            social_links['facebook'] = facebook
        
        return social_links
    
    def enrich_company(self, company_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich a company with Crunchbase data.
        
        Args:
            company_data: Dictionary containing company data
            
        Returns:
            Dictionary with enriched company data
        """
        # Create a copy to avoid modifying the original
        enriched_data = company_data.copy()
        
        # Initialize enrichment status
        enriched_data['EnrichmentStatus'] = 'Pending'
        enriched_data['EnrichmentNotes'] = ''
        
        try:
            # Get company information
            company_name = enriched_data.get('Company', '')
            company_website = enriched_data.get('CompanyWebsite', '')
            
            # First try to search by company name and domain
            if company_name:
                success, search_results = self.search_organization(
                    company_name=company_name,
                    domain=company_website
                )
                
                if success and search_results:
                    # Get organization UUID
                    uuid = search_results.get('uuid')
                    
                    if uuid:
                        # Get detailed organization information
                        success, org_details = self.get_organization_details(uuid)
                        
                        if success and org_details:
                            # Format organization data
                            formatted_data = self.format_organization_data(org_details)
                            
                            # Update enriched data with organization information
                            for key, value in formatted_data.items():
                                if value:  # Only update if we have a value
                                    enriched_data[key] = value
                            
                            # Mark as successful
                            enriched_data['EnrichmentStatus'] = 'Success'
                            enriched_data['EnrichmentSource'] = 'Crunchbase API'
                            enriched_data['EnrichmentNotes'] = 'Company data enriched successfully'
                            return enriched_data
            
            # If we got here, enrichment failed
            enriched_data['EnrichmentStatus'] = 'Failed'
            
            if not company_name:
                enriched_data['EnrichmentNotes'] = 'Missing company name'
                logger.warning("Skipping Crunchbase enrichment for lead without company name")
            else:
                enriched_data['EnrichmentNotes'] = 'No matching company found in Crunchbase'
                logger.warning(f"No matching company found in Crunchbase for: {company_name}")
            
        except Exception as e:
            enriched_data['EnrichmentStatus'] = 'Failed'
            enriched_data['EnrichmentNotes'] = f"Error during enrichment: {str(e)}"
            logger.error(f"Error enriching company with Crunchbase: {str(e)}")
        
        return enriched_data
    
    def bulk_enrich_companies(self, companies_df: pd.DataFrame, update_callback=None) -> pd.DataFrame:
        """
        Enrich multiple companies in a DataFrame.
        
        Args:
            companies_df: DataFrame containing company data
            update_callback: Optional callback function for progress updates
            
        Returns:
            DataFrame with enriched company data
        """
        # Create a copy to avoid modifying the original
        enriched_df = companies_df.copy()
        
        # Keep track of success and failures
        success_count = 0
        total_count = len(enriched_df)
        
        # Process each company
        for i, (idx, row) in enumerate(enriched_df.iterrows()):
            # Skip already enriched records
            if row.get('EnrichmentStatus') == 'Success' and 'Crunchbase' in str(row.get('EnrichmentSource', '')):
                continue
            
            # Convert row to dictionary for processing
            company_data = row.to_dict()
            
            # Enrich company data
            enriched_data = self.enrich_company(company_data)
            
            # Update DataFrame with enriched data
            for key, value in enriched_data.items():
                enriched_df.at[idx, key] = value
            
            # Update success count if enrichment was successful
            if enriched_data.get('EnrichmentStatus') == 'Success':
                success_count += 1
            
            # Calculate progress
            progress = (i + 1) / total_count
            
            # Call update callback if provided
            if update_callback:
                update_callback(progress, i + 1, total_count, success_count)
            
            # Log progress periodically
            if (i + 1) % 10 == 0 or (i + 1) == total_count:
                logger.info(f"Crunchbase enrichment progress: {i + 1}/{total_count} ({success_count} successful)")
        
        return enriched_df 