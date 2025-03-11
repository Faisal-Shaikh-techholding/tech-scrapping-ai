#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Apollo.io API Service

This module handles integration with the Apollo.io API for lead enrichment.
"""

import requests
import pandas as pd
import time
import logging
import random
from typing import Dict, Any, Optional, Tuple

logger = logging.getLogger('csv_processor')

class ApolloService:
    """Service for interacting with the Apollo.io API."""
    
    BASE_URL = "https://api.apollo.io/api/v1"  # Updated to use api/v1 format
    RATE_LIMIT = 10  # Requests per minute
    
    def __init__(self, api_key: str):
        """
        Initialize the Apollo.io service.
        
        Args:
            api_key (str): Apollo.io API key
        """
        self.api_key = api_key
        self.headers = {
            "Content-Type": "application/json",
            "Cache-Control": "no-cache"
        }
        self.last_request_time = 0
        
    def _rate_limit(self):
        """Implement rate limiting to avoid API restrictions."""
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        
        # Ensure we don't exceed rate limit (add jitter to avoid synchronization)
        min_interval = (60.0 / self.RATE_LIMIT) + random.uniform(0.1, 0.5)
        
        if elapsed < min_interval:
            sleep_time = min_interval - elapsed
            logger.debug("Rate limiting Apollo API call, sleeping for %.2f seconds", sleep_time)
            time.sleep(sleep_time)
            
        self.last_request_time = time.time()
    
    def _extract_domain_from_url(self, url: str) -> Optional[str]:
        """
        Extract domain from URL.
        
        Args:
            url (str): Website URL
            
        Returns:
            Optional[str]: Extracted domain or None
        """
        if not url:
            return None
            
        # Remove protocol and www if present
        domain = url.lower().replace("https://", "").replace("http://", "").replace("www.", "")
        
        # Remove path and query params
        domain = domain.split("/")[0]
        
        return domain
    
    def enrich_person(self, first_name: str, last_name: str, company_name: str = None, 
                     email: str = None) -> Tuple[bool, Dict[str, Any]]:
        """
        Enrich person data using Apollo.io API.
        
        Args:
            first_name (str): First name of the person
            last_name (str): Last name of the person
            company_name (str, optional): Company name
            email (str, optional): Email address
            
        Returns:
            Tuple[bool, Dict]: Success status and enriched data dict
        """
        self._rate_limit()
        
        endpoint = f"{self.BASE_URL}/people/search"
        
        # Build query parameters
        params = {
            "api_key": self.api_key,
            "q_person_name": f"{first_name} {last_name}",
            "page": 1,
            "per_page": 5
        }
        
        # Add optional parameters if provided
        if company_name:
            params["q_organization_name"] = company_name
        
        if email:
            params["q_emails"] = email
        
        try:
            response = requests.get(endpoint, params=params, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            
            # Check if we got any results
            if data and 'people' in data and len(data['people']) > 0:
                # Return the most relevant person (first result)
                return True, self._format_person_data(data['people'][0])
            
            logger.warning("No results found in Apollo for %s %s at %s", 
                          first_name, last_name, company_name or "N/A")
            return False, {}
                
        except requests.exceptions.RequestException as e:
            logger.error("Apollo API error: %s", str(e))
            return False, {"error": str(e)}
    
    def enrich_organization_by_domain(self, domain: str) -> Tuple[bool, Dict[str, Any]]:
        """
        Enrich organization data using Apollo.io Organization Enrichment API.
        
        Args:
            domain (str): Company domain (without www, e.g., "apollo.io")
            
        Returns:
            Tuple[bool, Dict]: Success status and enriched data dict
        """
        self._rate_limit()
        
        if not domain:
            logger.warning("Cannot enrich organization without domain")
            return False, {}
        
        # Clean up domain if needed (remove protocols, www, trailing slashes)
        clean_domain = self._extract_domain_from_url(domain)
        if not clean_domain:
            logger.warning("Failed to extract valid domain from %s", domain)
            return False, {}
        
        endpoint = f"{self.BASE_URL}/organizations/enrich"
        
        # Build query parameters
        params = {
            "api_key": self.api_key,
            "domain": clean_domain
        }
        
        try:
            response = requests.get(endpoint, params=params, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Check if we got any results - organization data is in the 'organization' key
            if data and 'organization' in data and data['organization']:
                return True, self._format_company_data(data['organization'])
            
            logger.warning("No organization found in Apollo for domain: %s", clean_domain)
            return False, {}
                
        except requests.exceptions.RequestException as e:
            logger.error("Apollo API error (organization enrichment): %s", str(e))
            return False, {"error": str(e)}
    
    def enrich_company(self, company_name: str, domain: str = None) -> Tuple[bool, Dict[str, Any]]:
        """
        Enrich company data using Apollo.io API.
        
        Args:
            company_name (str): Company name to search for
            domain (str, optional): Company website domain
            
        Returns:
            Tuple[bool, Dict]: Success status and enriched data dict
        """
        # If we have a domain, use the organization enrichment API
        if domain:
            clean_domain = self._extract_domain_from_url(domain)
            if clean_domain:
                return self.enrich_organization_by_domain(clean_domain)
        
        # If no domain or domain extraction failed, fallback to organization search
        self._rate_limit()
        
        endpoint = f"{self.BASE_URL}/organizations/search"
        
        # Build query parameters
        params = {
            "api_key": self.api_key,
            "q_organization_name": company_name,
            "page": 1,
            "per_page": 1
        }
        
        try:
            response = requests.get(endpoint, params=params, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Check if we got any results
            if data and 'organizations' in data and len(data['organizations']) > 0:
                # Return the most relevant organization (first result)
                org_data = data['organizations'][0]
                
                # If we have a website_url in the results, try to enrich with the domain API
                # as it provides more comprehensive data
                if org_data.get("website_url"):
                    website_domain = self._extract_domain_from_url(org_data.get("website_url"))
                    if website_domain:
                        success, domain_enriched = self.enrich_organization_by_domain(website_domain)
                        if success:
                            return True, domain_enriched
                
                # If domain enrichment failed or wasn't possible, return search result
                return True, self._format_company_data(org_data)
            
            logger.warning("No company results found in Apollo for %s", company_name)
            return False, {}
                
        except requests.exceptions.RequestException as e:
            logger.error("Apollo API error: %s", str(e))
            return False, {"error": str(e)}
    
    def _format_person_data(self, person_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format the person data from Apollo.io to a standardized format.
        
        Args:
            person_data (Dict): Raw person data from Apollo
            
        Returns:
            Dict: Formatted person data
        """
        formatted_data = {
            "FirstName": person_data.get("first_name", ""),
            "LastName": person_data.get("last_name", ""),
            "JobTitle": person_data.get("title", ""),
            "Email": "",
            "Phone": "",
            "LinkedIn": "",
            "EnrichmentSource": "Apollo.io",
            "EnrichmentStatus": "Success"
        }
        
        # Extract email
        emails = person_data.get("email", None)
        if emails:
            formatted_data["Email"] = emails
            
        # Extract LinkedIn
        linkedin_url = person_data.get("linkedin_url", "")
        if linkedin_url:
            formatted_data["LinkedIn"] = linkedin_url
            
        # Extract phone
        phone_numbers = person_data.get("phone_numbers", [])
        if phone_numbers and len(phone_numbers) > 0:
            formatted_data["Phone"] = phone_numbers[0].get("raw_number", "")
            formatted_data["PhoneType"] = phone_numbers[0].get("type", "")
        
        # Extract company data if available
        organization = person_data.get("organization", {})
        if organization:
            formatted_data["Company"] = organization.get("name", "")
            formatted_data["CompanyWebsite"] = organization.get("website_url", "")
            formatted_data["Industry"] = organization.get("industry", "")
            formatted_data["CompanySize"] = organization.get("size", "")
        
        return formatted_data
    
    def _format_company_data(self, company_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format the company data from Apollo.io to a standardized format.
        
        Args:
            company_data (Dict): Raw company data from Apollo
            
        Returns:
            Dict: Formatted company data
        """
        formatted_data = {
            "Company": company_data.get("name", ""),
            "CompanyWebsite": company_data.get("website_url", ""),
            "Industry": company_data.get("industry", ""),
            "CompanySize": company_data.get("estimated_num_employees", ""),
            "CompanyDescription": company_data.get("short_description", "") or company_data.get("seo_description", ""),
            "CompanyLocation": ", ".join(filter(None, [
                company_data.get("city", ""),
                company_data.get("state", ""),
                company_data.get("country", "")
            ])),
            "CompanyIndustry": company_data.get("industry", ""),
            "CompanyFunding": company_data.get("total_funding_printed", "") or company_data.get("total_funding", ""),
            "CompanyFundingAmount": company_data.get("total_funding", ""),
            "CompanyLatestFundingDate": company_data.get("latest_funding_round_date", ""),
            "CompanyLatestFundingStage": company_data.get("latest_funding_stage", ""),
            "CompanyTechnology": ", ".join(company_data.get("technology_names", []) or company_data.get("technologies", []) or []),
            "CompanyLinkedIn": company_data.get("linkedin_url", ""),
            "CompanyTwitter": company_data.get("twitter_url", ""),
            "CompanyFacebook": company_data.get("facebook_url", ""),
            "CompanyFounded": company_data.get("founded_year", ""),
            "CompanyEmployeeCount": company_data.get("estimated_num_employees", ""),
            "CompanyPhone": company_data.get("phone", "") or (company_data.get("primary_phone", {}) or {}).get("number", ""),
            "EnrichmentSource": "Apollo.io",
            "EnrichmentStatus": "Success"
        }
        
        # Extract tech leadership contacts
        tech_leadership = []
        org_chart_people_ids = company_data.get("org_chart_root_people_ids", [])
        
        # If we have org chart data, extract tech leadership contacts
        if "org_chart_data" in company_data and org_chart_people_ids:
            org_chart = company_data.get("org_chart_data", {})
            for person_id in org_chart_people_ids:
                person = org_chart.get(person_id, {})
                title = person.get("title", "").lower()
                
                # Check if this is a tech leadership role
                is_tech_leader = any(tech_term in title for tech_term in [
                    "cto", "chief technology", "vp of tech", "vp tech", "vp, tech", 
                    "chief information", "cio", "head of engineering", "vp of engineering",
                    "vp engineering", "vp, engineering", "director of technology",
                    "director of engineering", "tech lead", "engineering lead"
                ])
                
                if is_tech_leader:
                    contact_info = {
                        "name": f"{person.get('first_name', '')} {person.get('last_name', '')}",
                        "title": person.get("title", ""),
                        "email": person.get("email", ""),
                        "phone": person.get("phone", ""),
                        "linkedin": person.get("linkedin_url", "")
                    }
                    tech_leadership.append(contact_info)
        
        # Store tech leadership contacts
        formatted_data["TechLeadership"] = tech_leadership
        
        # Extract job listings for tech stack insights
        job_listings = []
        if "job_listings" in company_data:
            for job in company_data.get("job_listings", []):
                if any(tech_term in job.get("title", "").lower() for tech_term in [
                    "developer", "engineer", "software", "data", "tech", "it ", "information technology",
                    "devops", "cloud", "infrastructure", "security", "web", "mobile", "frontend", "backend",
                    "full stack", "fullstack", "architect"
                ]):
                    job_info = {
                        "title": job.get("title", ""),
                        "description": job.get("description", ""),
                        "url": job.get("url", ""),
                        "posted_date": job.get("posted_date", "")
                    }
                    job_listings.append(job_info)
        
        # Store tech job listings
        formatted_data["TechJobListings"] = job_listings
        
        # Extract departmental headcount for engineering/IT
        if "departmental_head_count" in company_data:
            dept_counts = company_data.get("departmental_head_count", {})
            formatted_data["EngineeringHeadcount"] = dept_counts.get("engineering", 0)
            formatted_data["ITHeadcount"] = dept_counts.get("information_technology", 0)
            formatted_data["ProductHeadcount"] = dept_counts.get("product_management", 0)
            formatted_data["DataScienceHeadcount"] = dept_counts.get("data_science", 0)
        
        return formatted_data
    
    def enrich_lead(self, lead_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich a lead with Apollo data.
        
        Args:
            lead_data: Dictionary containing lead data
            
        Returns:
            Dictionary with enriched lead data
        """
        # Create a copy to avoid modifying the original
        enriched_data = lead_data.copy()
        
        # Initialize enrichment status
        enriched_data['EnrichmentStatus'] = 'Pending'
        enriched_data['EnrichmentNotes'] = ''
        
        try:
            # Prioritize company enrichment using domain
            company_name = enriched_data.get('Company', '')
            company_website = enriched_data.get('CompanyWebsite', '')
            
            if company_website:
                # Try to enrich by domain first (preferred method)
                domain = self._extract_domain_from_url(company_website)
                if domain:
                    success, company_result = self.enrich_organization_by_domain(domain)
                    
                    if success and company_result:
                        # Extract company data
                        company_data = company_result  # Already formatted
                        
                        # Update enriched data with company information
                        for key, value in company_data.items():
                            if value:  # Only update if we have a value
                                enriched_data[key] = value
                        
                        # Mark as successful
                        enriched_data['EnrichmentStatus'] = 'Success'
                        enriched_data['EnrichmentSource'] = 'Apollo.io API'
                        enriched_data['EnrichmentNotes'] = 'Company data enriched successfully via domain'
                        return enriched_data
            
            # If domain enrichment fails or no website, try by company name
            if company_name:
                success, company_result = self.enrich_company(
                    company_name=company_name,
                    domain=company_website
                )
                
                if success and company_result:
                    # Update enriched data with company information
                    for key, value in company_result.items():
                        if value:  # Only update if we have a value
                            enriched_data[key] = value
                    
                    # Mark as successful
                    enriched_data['EnrichmentStatus'] = 'Success'
                    enriched_data['EnrichmentSource'] = 'Apollo.io API'
                    enriched_data['EnrichmentNotes'] = 'Company data enriched successfully via name'
                    return enriched_data
            
            # If we got here, all enrichment attempts failed
            enriched_data['EnrichmentStatus'] = 'Failed'
            
            if not company_name and not company_website:
                enriched_data['EnrichmentNotes'] = 'Missing both company name and website'
                logger.warning("Skipping Apollo enrichment for lead without company information: %s", lead_data)
            else:
                enriched_data['EnrichmentNotes'] = 'All enrichment attempts failed'
            
        except Exception as e:
            enriched_data['EnrichmentStatus'] = 'Failed'
            enriched_data['EnrichmentNotes'] = f"Error during enrichment: {str(e)}"
            logger.error("Error enriching lead with Apollo.io: %s", str(e))
        
        return enriched_data
    
    def bulk_enrich_leads(self, leads_df: pd.DataFrame, 
                          update_callback=None) -> pd.DataFrame:
        """
        Enrich multiple leads in a DataFrame.
        
        Args:
            leads_df (pd.DataFrame): DataFrame containing lead data
            update_callback (callable, optional): Callback function for progress updates
            
        Returns:
            pd.DataFrame: DataFrame with enriched lead data
        """
        enriched_df = leads_df.copy()
        
        # Track success rate
        total_leads = len(enriched_df)
        success_count = 0
        
        # Process each lead
        for index, row in enriched_df.iterrows():
            # Convert row to dict
            lead_data = row.to_dict()
            
            # Enrich the lead
            enriched_lead = self.enrich_lead(lead_data)
            
            # Update DataFrame with enriched data
            for key, value in enriched_lead.items():
                if key in enriched_df.columns:
                    enriched_df.at[index, key] = value
            
            # Track success
            if enriched_lead.get("EnrichmentStatus") == "Success":
                success_count += 1
            
            # Call update callback if provided
            if update_callback:
                progress = (index + 1) / total_leads
                update_callback(progress, index + 1, total_leads, success_count)
        
        logger.info("Apollo enrichment completed. Success rate: %d/%d (%.1f%%)", 
                   success_count, total_leads, (success_count / total_leads * 100) if total_leads > 0 else 0)
        
        return enriched_df 