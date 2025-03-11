#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Enrichment Utilities

This module contains utility functions for enriching company data using various services.
"""

import logging
from typing import Dict, List, Tuple, Any, Optional

logger = logging.getLogger('csv_processor')

def enrich_company_data(company_data: Dict[str, Any], services: List[Tuple[str, Any]], 
                      scraper=None, scrape_options=None) -> Dict[str, Any]:
    """
    Enrich company data using multiple services in sequence until successful.
    
    Args:
        company_data: Dictionary containing company data
        services: List of tuples (service_name, service_instance)
        scraper: Web scraper service instance (optional)
        scrape_options: List of options for web scraping (optional)
        
    Returns:
        Dictionary with enriched company data
    """
    # Create a copy to avoid modifying the original
    enriched_data = company_data.copy()
    
    # Skip if already successfully enriched
    if enriched_data.get('EnrichmentStatus') == 'Success':
        return enriched_data
    
    # Initialize enrichment status
    if 'EnrichmentStatus' not in enriched_data:
        enriched_data['EnrichmentStatus'] = 'Pending'
    if 'EnrichmentNotes' not in enriched_data:
        enriched_data['EnrichmentNotes'] = ''
    
    # Try each service in order until successful
    for service_name, service in services:
        try:
            if service_name == 'Apollo.io':
                result = service.enrich_lead(enriched_data)
            elif service_name == 'Crunchbase':
                result = service.enrich_company(enriched_data)
            else:
                continue
            
            # Check if enrichment was successful
            if result.get('EnrichmentStatus') == 'Success':
                # Update enriched data with service results
                for key, value in result.items():
                    if value:  # Only update if we have a value
                        enriched_data[key] = value
                
                # Mark as successful
                enriched_data['EnrichmentStatus'] = 'Success'
                enriched_data['EnrichmentSource'] = service_name
                enriched_data['EnrichmentNotes'] = f'Data enriched successfully via {service_name}'
                return enriched_data
        except Exception as e:
            logger.error(f"Error enriching with {service_name}: {str(e)}")
            # Continue to next service
    
    # If API enrichment failed, try web scraping
    if scraper and scrape_options and enriched_data.get('CompanyWebsite'):
        try:
            # Create a copy of the data for web scraping
            scrape_data = {
                'Company': enriched_data.get('Company', ''),
                'CompanyWebsite': enriched_data.get('CompanyWebsite', '')
            }
            
            # Call the correct method on the WebScraperService
            scrape_result = scraper.enrich_company_data(scrape_data)
            
            if scrape_result:
                # Update with scraped data
                for key, value in scrape_result.items():
                    if value:  # Only update if we have a value
                        enriched_data[key] = value
                
                # Mark as successful if we got any useful data
                if any(value for key, value in scrape_result.items() 
                      if key not in ['Company', 'CompanyWebsite', 'EnrichmentStatus']):
                    enriched_data['EnrichmentStatus'] = 'Success'
                    enriched_data['EnrichmentSource'] = 'Web Scraping'
                    enriched_data['EnrichmentNotes'] = 'Data enriched via web scraping'
                    return enriched_data
        except AttributeError as e:
            # Handle the case where the scraper doesn't have the expected method
            logger.error(f"Web scraper method error: {str(e)}")
            logger.info("Attempting fallback to basic web scraping")
            try:
                # Basic fallback scraping if available
                if hasattr(scraper, 'fetch_page') and enriched_data.get('CompanyWebsite'):
                    success, soup, _ = scraper.fetch_page(enriched_data.get('CompanyWebsite'))
                    if success and soup:
                        # Try to extract basic information
                        title = soup.title.text if soup.title else ""
                        description = ""
                        meta_desc = soup.find('meta', attrs={'name': 'description'})
                        if meta_desc:
                            description = meta_desc.get('content', '')
                        
                        if description:
                            enriched_data['CompanyDescription'] = description
                            enriched_data['EnrichmentStatus'] = 'Success'
                            enriched_data['EnrichmentSource'] = 'Basic Web Scraping'
                            enriched_data['EnrichmentNotes'] = 'Basic data extracted from website'
                            return enriched_data
            except Exception as inner_e:
                logger.error(f"Fallback web scraping failed: {str(inner_e)}")
        except Exception as e:
            logger.error(f"Error during web scraping: {str(e)}")
    
    # If we got here, all enrichment attempts failed
    enriched_data['EnrichmentStatus'] = 'Failed'
    
    company_name = enriched_data.get('Company', '')
    company_website = enriched_data.get('CompanyWebsite', '')
    
    if not company_name and not company_website:
        enriched_data['EnrichmentNotes'] = 'Missing both company name and website'
    else:
        enriched_data['EnrichmentNotes'] = 'All enrichment attempts failed'
    
    return enriched_data
