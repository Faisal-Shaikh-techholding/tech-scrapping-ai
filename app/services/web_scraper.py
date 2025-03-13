#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Web Scraping Service

This module handles web scraping for company information using Requests and BeautifulSoup.
"""

import requests
import pandas as pd
import logging
import time
import random
import re
from typing import Dict, Any, List, Tuple, Optional
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin

logger = logging.getLogger('csv_processor')

class WebScraperService:
    """Service for scraping company websites for additional information."""
    
    # User agent strings to rotate
    USER_AGENTS = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'
    ]
    
    # Maximum number of requests per minute
    RATE_LIMIT = 10
    
    def __init__(self):
        """Initialize the web scraper service."""
        self.session = requests.Session()
        self.last_request_time = 0
    
    def _rate_limit(self):
        """Implement rate limiting to avoid being blocked."""
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        
        # Ensure we don't exceed rate limit
        min_interval = (60.0 / self.RATE_LIMIT) + random.uniform(0.5, 2.0)
        
        if elapsed < min_interval:
            sleep_time = min_interval - elapsed
            logger.debug("Rate limiting web scraping, sleeping for %.2f seconds", sleep_time)
            time.sleep(sleep_time)
            
        self.last_request_time = time.time()
    
    def _get_random_user_agent(self) -> str:
        """Get a random user agent string to avoid detection."""
        return random.choice(self.USER_AGENTS)
    
    def _clean_url(self, url) -> str:
        """
        Clean and normalize a URL.
        
        Args:
            url: URL to clean (could be string, float, or other type)
            
        Returns:
            str: Cleaned URL
        """
        # Convert URL to string if it's not already
        if not isinstance(url, str):
            try:
                url = str(url)
                # Remove any decimal points and trailing zeros if it was a number
                if '.' in url:
                    url = url.rstrip('0').rstrip('.') if '.' in url else url
            except Exception as e:
                print(f"Error converting URL to string: {e}")
                return ""
                
        # Skip processing if URL is empty
        if not url:
            return ""
        
        # Add http:// if missing
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        # Parse the URL
        try:
            parsed = urlparse(url)
            
            # Extract the domain
            domain = parsed.netloc
            
            # Remove www. if present
            if domain.startswith('www.'):
                domain = domain[4:]
                
            # Return the base URL
            return f"https://{domain}"
        except Exception as e:
            print(f"Error parsing URL {url}: {e}")
            return url
    
    def fetch_page(self, url) -> Tuple[bool, Optional[BeautifulSoup], str]:
        """
        Fetch a web page and return BeautifulSoup object.
        
        Args:
            url: URL to fetch (can be string or other type)
            
        Returns:
            Tuple[bool, Optional[BeautifulSoup], str]: Success status, soup object, and error message
        """
        # Handle non-string URLs
        if not isinstance(url, str):
            try:
                url = str(url)
            except Exception as e:
                return False, None, f"Invalid URL type: {type(url).__name__} - {str(e)}"
        
        # Clean the URL
        url = self._clean_url(url)
        
        # Check if URL is valid after cleaning
        if not url:
            return False, None, "Empty or invalid URL after cleaning"
        
        # Apply rate limiting
        self._rate_limit()
        
        # Set up headers with random user agent
        headers = {
            'User-Agent': self._get_random_user_agent(),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0'
        }
        
        try:
            # Make the request with a timeout
            response = self.session.get(url, headers=headers, timeout=10)
            
            # Check if the request was successful
            if response.status_code == 200:
                # Parse the HTML
                soup = BeautifulSoup(response.text, 'html.parser')
                return True, soup, ""
            else:
                logger.warning("Failed to fetch page: %s, status code: %d", url, response.status_code)
                return False, None, f"HTTP error: {response.status_code}"
                
        except requests.exceptions.Timeout:
            logger.warning("Timeout while fetching: %s", url)
            return False, None, "Request timed out"
        except requests.exceptions.TooManyRedirects:
            logger.warning("Too many redirects for: %s", url)
            return False, None, "Too many redirects"
        except requests.exceptions.RequestException as e:
            logger.warning("Request error for %s: %s", url, str(e))
            return False, None, str(e)
    
    def extract_company_info(self, soup: BeautifulSoup, company_name: str) -> Dict[str, Any]:
        """
        Extract company information from BeautifulSoup object.
        
        Args:
            soup (BeautifulSoup): Parsed HTML
            company_name (str): Company name for contextual extraction
            
        Returns:
            Dict[str, Any]: Extracted company information
        """
        company_info = {
            "CompanyDescription": "",
            "SocialLinks": {},
            "ContactInfo": {},
            "Address": "",
            "Founders": [],
            "Products": [],
            "EnrichmentSource": "Web Scraping",
            "EnrichmentNotes": ""
        }
        
        # Extract meta description
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc and meta_desc.get('content'):
            company_info["CompanyDescription"] = meta_desc.get('content')
        
        # If no meta description, try to find the first paragraph
        if not company_info["CompanyDescription"]:
            about_section = self._find_about_section(soup, company_name)
            if about_section:
                paragraphs = about_section.find_all('p')[:3]
                if paragraphs:
                    company_info["CompanyDescription"] = ' '.join([p.text.strip() for p in paragraphs])
        
        # Extract social media links
        social_links = self._extract_social_links(soup)
        if social_links:
            company_info["SocialLinks"] = social_links
        
        # Extract contact information
        contact_info = self._extract_contact_info(soup)
        if contact_info:
            company_info["ContactInfo"] = contact_info
        
        # Extract address
        address = self._extract_address(soup)
        if address:
            company_info["Address"] = address
        
        # Extract founders or key people
        founders = self._extract_founders(soup)
        if founders:
            company_info["Founders"] = founders
        
        # Extract products or services
        products = self._extract_products(soup)
        if products:
            company_info["Products"] = products
        
        # Add notes about what was found
        notes = []
        if company_info["CompanyDescription"]:
            notes.append("Found company description")
        if company_info["SocialLinks"]:
            notes.append(f"Found {len(company_info['SocialLinks'])} social links: {', '.join(company_info['SocialLinks'].keys())}")
        if company_info["ContactInfo"]:
            notes.append(f"Found {len(company_info['ContactInfo'])} contact details: {', '.join(f'{k}: {v}' for k, v in company_info['ContactInfo'].items())}")
        if company_info["Address"]:
            notes.append("Found company address")
        if company_info["Founders"]:
            notes.append(f"Found {len(company_info['Founders'])} founders: {', '.join(company_info['Founders'])}")
        if company_info["Products"]:
            notes.append(f"Found {len(company_info['Products'])} products/services: {', '.join(company_info['Products'])}")
        
        company_info["EnrichmentNotes"] = ". ".join(notes)
        
        return company_info
    
    def _find_about_section(self, soup: BeautifulSoup, company_name: str) -> Optional[BeautifulSoup]:
        """
        Find the about or company description section in the page.
        
        Args:
            soup (BeautifulSoup): Parsed HTML
            company_name (str): Company name
            
        Returns:
            Optional[BeautifulSoup]: About section if found
        """
        # Look for common about section identifiers
        about_keywords = ['about', 'company', 'who we are', 'mission', 'about us']
        
        # Try to find sections with these keywords in the ID, class or text
        for keyword in about_keywords:
            # Check for elements with the keyword in id or class
            elements = soup.find_all(lambda tag: keyword in tag.get('id', '').lower() or 
                                    keyword in ' '.join(tag.get('class', [])).lower())
            
            if elements:
                return elements[0]
            
            # Check for heading elements that contain the keyword
            headings = soup.find_all(['h1', 'h2', 'h3'], string=lambda s: s and keyword in s.lower())
            
            if headings:
                # Return the parent section or div
                return headings[0].parent
        
        # If we couldn't find an about section, try to find a section that mentions the company name
        company_elements = soup.find_all(lambda tag: company_name.lower() in tag.text.lower() and 
                                       tag.name in ['div', 'section', 'article'])
        
        if company_elements:
            return company_elements[0]
        
        # If all else fails, return the body
        return soup.find('body')
    
    def _extract_social_links(self, soup: BeautifulSoup) -> Dict[str, str]:
        """
        Extract social media links from the page.
        
        Args:
            soup (BeautifulSoup): Parsed HTML
            
        Returns:
            Dict[str, str]: Social media platform names and URLs
        """
        social_links = {}
        
        # Common social media domains
        social_domains = {
            'facebook.com': 'Facebook',
            'twitter.com': 'Twitter',
            'linkedin.com': 'LinkedIn',
            'instagram.com': 'Instagram',
            'youtube.com': 'YouTube',
            'github.com': 'GitHub',
            'medium.com': 'Medium'
        }
        
        # Find all links
        links = soup.find_all('a', href=True)
        
        for link in links:
            href = link['href']
            
            # Check if the link points to a social media platform
            for domain, platform in social_domains.items():
                if domain in href:
                    social_links[platform] = href
                    break
        
        return social_links
    
    def _extract_contact_info(self, soup: BeautifulSoup) -> Dict[str, str]:
        """
        Extract contact information from the page.
        
        Args:
            soup (BeautifulSoup): Parsed HTML
            
        Returns:
            Dict[str, str]: Contact information
        """
        contact_info = {}
        
        # Extract email addresses
        email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        text = soup.get_text()
        emails = re.findall(email_pattern, text)
        
        if emails:
            # Exclude common noreply/info emails
            filtered_emails = [e for e in emails if not e.startswith(('noreply@', 'no-reply@', 'donotreply@'))]
            if filtered_emails:
                contact_info['Email'] = filtered_emails[0]
        
        # Extract phone numbers
        phone_pattern = r'(?:\+\d{1,3}[-.\s]?)?(?:\(?\d{3}\)?[-.\s]?)?\d{3}[-.\s]?\d{4}'
        phones = re.findall(phone_pattern, text)
        
        if phones:
            contact_info['Phone'] = phones[0]
        
        # Look for a contact page link
        contact_links = soup.find_all('a', string=lambda s: s and 'contact' in s.lower())
        if contact_links:
            contact_info['ContactPage'] = contact_links[0]['href']
        
        return contact_info
    
    def _extract_address(self, soup: BeautifulSoup) -> str:
        """Extract the company address from the page."""
        # Implement logic to find address, e.g., using regex or specific HTML tags
        address = ""
        # Example logic (this will need to be tailored to the specific website structure)
        address_section = soup.find(text=re.compile(r'address', re.I))
        if address_section:
            address = address_section.find_next('p').text.strip()  # Adjust based on actual HTML structure
        return address
    
    def _extract_founders(self, soup: BeautifulSoup) -> List[str]:
        """Extract founders or key people from the page."""
        founders = []
        # Implement logic to find founders, e.g., using specific HTML tags or sections
        founders_section = soup.find(text=re.compile(r'founder', re.I))
        if founders_section:
            founders = [founders_section.find_next('p').text.strip()]  # Adjust based on actual HTML structure
        return founders
    
    def _extract_products(self, soup: BeautifulSoup) -> List[str]:
        """Extract products or services from the page."""
        products = []
        # Implement logic to find products/services, e.g., using specific HTML tags or sections
        products_section = soup.find(text=re.compile(r'products|services', re.I))
        if products_section:
            products = [products_section.find_next('p').text.strip()]  # Adjust based on actual HTML structure
        return products
    
    def enrich_company_data(self, company_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich company data by scraping their website.
        
        Args:
            company_data (Dict): Company data including URL
            
        Returns:
            Dict[str, Any]: Enriched company data
        """
        enriched_data = company_data.copy()
        
        # Get the company website URL
        website = company_data.get('CompanyWebsite', '')
        
        # Convert numeric website to string if needed
        if not isinstance(website, str):
            try:
                if pd.isna(website) or website is None:
                    website = ''
                else:
                    website = str(website)
                    # Clean it in case it's a number with decimal point
                    if '.' in website:
                        website = website.rstrip('0').rstrip('.')
            except Exception as e:
                print(f"Error converting website to string: {e}")
                website = ''
        
        # Skip if no website
        if not website:
            print(f"No website URL to scrape for company: {company_data.get('Company', 'Unknown')}")
            enriched_data['EnrichmentStatus'] = 'Skipped - No Website'
            return enriched_data
        
        # Fetch the page
        success, soup, error = self.fetch_page(website)
        
        if not success:
            print(f"Failed to fetch website for company {company_data.get('Company', 'Unknown')}: {error}")
            enriched_data['EnrichmentStatus'] = f'Failed - {error}'
            return enriched_data
        
        # Extract company information
        company_info = self.extract_company_info(soup, company_data.get('Company', ''))
        
        # Print the raw data to help with debugging
        print(f"Raw scraped data for {company_data.get('Company', 'Unknown')}: {company_info}")
        
        # Update enriched data
        for key, value in company_info.items():
            if key == "SocialLinks" and value:
                # Explicitly store all social links in the enriched data
                for platform, url in value.items():
                    enriched_data[platform] = url
            elif key == "ContactInfo" and value:
                # Explicitly store all contact info in the enriched data
                for contact_type, contact_info in value.items():
                    enriched_data[contact_type] = contact_info
            elif isinstance(value, dict):
                # For any other nested dictionaries
                for sub_key, sub_value in value.items():
                    enriched_data[sub_key] = sub_value
            else:
                # For direct key-value pairs
                if key not in ['EnrichmentNotes', 'EnrichmentSource'] or not enriched_data.get(key):
                    enriched_data[key] = value
        
        # Update enrichment status
        enriched_data['EnrichmentStatus'] = 'Completed'
        
        # Print the final enriched data to verify
        print(f"Final enriched data for {company_data.get('Company', 'Unknown')}: {enriched_data}")
        
        return enriched_data
    
    def bulk_enrich_companies(self, companies_df: pd.DataFrame,update_callback=None) -> pd.DataFrame:
        """
        Enrich multiple companies in a DataFrame.
        
        Args:
            companies_df (pd.DataFrame): DataFrame containing company data
            update_callback (callable, optional): Callback function for progress updates
            
        Returns:
            pd.DataFrame: DataFrame with enriched company data
        """
        enriched_df = companies_df.copy()
        print('------Enriched DF',enriched_df)
        # Track success rate
        total_companies = len(enriched_df)
        success_count = 0
        
        # Process each company
        for index, row in enriched_df.iterrows():
            # Skip already enriched companies
            if row.get('EnrichmentStatus') == 'Completed':
                continue
                
            # Convert row to dict
            company_data = row.to_dict()
            
            # Enrich the company data
            enriched_company = self.enrich_company_data(company_data)
            
            # Update DataFrame with enriched data
            for key, value in enriched_company.items():
                if key in enriched_df.columns:
                    enriched_df.at[index, key] = value
            
            # Track success
            if enriched_company.get('EnrichmentStatus') == 'Completed':
                success_count += 1
            
            # Call update callback if provided
            if update_callback:
                progress = (index + 1) / total_companies
                update_callback(progress, index + 1, total_companies, success_count)
            
            # Add a random delay to avoid detection
            time.sleep(random.uniform(1.0, 3.0))
        
        logger.info("Web scraping enrichment completed. Success rate: %d/%d (%.1f%%)", 
                   success_count, total_companies, 
                   (success_count / total_companies * 100) if total_companies > 0 else 0)
        
        return enriched_df 