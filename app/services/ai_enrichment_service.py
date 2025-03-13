#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AI Enrichment Service

This service handles interactions with AWS Bedrock to process company data using
Claude AI models, automatically identifying columns and enriching data regardless
of input schema variations.
"""

import pandas as pd
import boto3
import json
import time
import os
from typing import Dict, Any, List, Tuple, Callable, Optional
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)

class AIEnrichmentService:
    """
    Service for AI-powered data enrichment using AWS Bedrock and Claude models.
    This service automatically identifies column meanings regardless of their names
    and enriches company data.
    """
    
    def __init__(self, aws_region: str = None, 
                 model_id: str = None):
        """
        Initialize the AI Enrichment Service with AWS Bedrock.
        
        Args:
            aws_region: AWS region where Bedrock is available (defaults to .env setting)
            model_id: Claude model identifier to use (defaults to .env setting)
        """
        # Use parameters if provided, otherwise use environment variables
        self.aws_region = aws_region or os.getenv('AWS_REGION')
        self.model_id = model_id or os.getenv('AWS_BEDROCK_MODEL_ID', 'anthropic.claude-3-sonnet-20240229-v1:0')
        
        # Initialize AWS Bedrock client using environment variables for credentials
        try:
            self.bedrock_client = boto3.client(
                service_name='bedrock-runtime',
                region_name=self.aws_region,
                aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                aws_session_token=os.getenv('AWS_SESSION_TOKEN')
                
            )
            logger.info("Initialized Bedrock client with model %s in region %s", 
                        self.model_id, self.aws_region)
        except Exception as e:
            logger.error("Failed to initialize Bedrock client: %s", str(e))
            self.bedrock_client = None
    
    def is_available(self) -> bool:
        """Check if the AI service is available and properly configured."""
        return self.bedrock_client is not None
    
    def process_dataframe(self, df: pd.DataFrame, 
                         update_callback: Optional[Callable] = None,
                         max_batch_size: int = 25) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Process a DataFrame using AI to identify columns and enrich data.
        
        Args:
            df: Input DataFrame with company data
            update_callback: Function to call with progress updates
            max_batch_size: Maximum number of records to process in a single API call
            
        Returns:
            Tuple of (enriched DataFrame, statistics dictionary)
        """
        if not self.is_available():
            raise RuntimeError("AI Enrichment Service is not available")
        
        total_records = len(df)
        processed_records = 0
        processed_successfully = 0
        
        # Create a copy of the DataFrame to avoid modifying the original
        enriched_df = df.copy()
        
        # Add result columns if they don't exist
        required_columns = [
            'company_name', 'company_website', 'industry', 'company_size',
            'company_description', 'founded_year', 'headquarters', 
            'enrichment_status', 'enrichment_notes'
        ]
        
        for col in required_columns:
            if col not in enriched_df.columns:
                enriched_df[col] = None
        
        # Process in batches to avoid token limits
        batch_size = min(max_batch_size, total_records)
        
        stats = {
            'processed_records': 0,
            'success_count': 0,
            'failed_count': 0,
            'partial_count': 0,
            'status': 'in_progress',
            'start_time': time.time()
        }
        
        try:
            # Process records in batches
            for i in range(0, total_records, batch_size):
                # Get the current batch
                end_idx = min(i + batch_size, total_records)
                batch_df = df.iloc[i:end_idx].copy()
                
                # Process the batch
                processed_batch, batch_stats = self._process_batch(batch_df)
                
                # Update the enriched DataFrame with the results
                for idx, row in processed_batch.iterrows():
                    actual_idx = df.index[i + (idx - batch_df.index[0])]
                    
                    # Update the enriched DataFrame with the enriched values
                    for col in processed_batch.columns:
                        if col in enriched_df.columns:
                            enriched_df.at[actual_idx, col] = row[col]
                
                # Update statistics
                processed_records += len(batch_df)
                processed_successfully += batch_stats['success_count']
                
                # Update progress
                progress = processed_records / total_records
                if update_callback:
                    should_stop = update_callback(
                        progress, processed_records, total_records, processed_successfully
                    )
                    if should_stop:
                        logger.info("Processing stopped by user request")
                        break
            
            # Calculate final statistics
            stats.update({
                'processed_records': processed_records,
                'success_count': processed_successfully,
                'failed_count': processed_records - processed_successfully,
                'success_rate': round((processed_successfully / processed_records) * 100, 2) if processed_records > 0 else 0,
                'status': 'complete',
                'end_time': time.time(),
                'duration': round(time.time() - stats['start_time'], 2)
            })
            
            return enriched_df, stats
        
        except Exception as e:
            logger.error(f"Error processing DataFrame: {str(e)}")
            stats.update({
                'status': 'error',
                'message': str(e),
                'end_time': time.time()
            })
            return enriched_df, stats
    
    def _process_batch(self, batch_df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Process a batch of records using the AI service.
        
        Args:
            batch_df: DataFrame batch to process
            
        Returns:
            Tuple of (processed DataFrame, batch statistics)
        """
        # Convert batch to JSON for the prompt
        batch_json = batch_df.to_json(orient='records', date_format='iso')
        
        # Create the prompt for Claude
        prompt = self._create_enrichment_prompt(batch_df, batch_json)
        
        # Call Claude API via Bedrock
        try:
            response = self._call_claude_api(prompt)
            print("Claude Response",response)
            # Parse the response to extract the enriched data
            enriched_data = self._parse_claude_response(response, batch_df)
            
            # Calculate statistics for this batch
            success_count = sum(1 for status in enriched_data['enrichment_status'] if status == 'Complete')
            
            batch_stats = {
                'success_count': success_count,
                'failed_count': sum(1 for status in enriched_data['enrichment_status'] if status == 'Failed'),
                'partial_count': sum(1 for status in enriched_data['enrichment_status'] if status == 'Partial')
            }
            
            return enriched_data, batch_stats
            
        except Exception as e:
            logger.error(f"Error in AI processing batch: {str(e)}")
            # Mark all records in the batch as failed
            batch_df['enrichment_status'] = 'Failed'
            batch_df['enrichment_notes'] = f"API Error: {str(e)}"
            
            return batch_df, {
                'success_count': 0,
                'failed_count': len(batch_df),
                'partial_count': 0
            }
    
    def _create_enrichment_prompt(self, batch_df: pd.DataFrame, batch_json: str) -> str:
        """
        Create a prompt for Claude to enrich company data regardless of column names.
        
        Args:
            batch_df: DataFrame batch to process
            batch_json: JSON string representation of the batch
            
        Returns:
            Prompt string for Claude
        """
        # Get column names and sample values to help Claude understand the data
        columns_info = []
        for col in batch_df.columns:
            sample_values = batch_df[col].dropna().head(3).tolist()
            columns_info.append(f"- {col}: {sample_values}")
        
        columns_description = "\n".join(columns_info)
        
        prompt = f"""
Human: You are a data processing assistant specialized in company data enrichment. I have a CSV file with company information that needs to be processed and standardized. The column names in the file might be different from what we expect, so you'll need to analyze what each column represents.

Here are the columns in my data with some sample values:
{columns_description}

I need you to:

1. Analyze the data and identify which columns contain:
   - Company name
   - Company website
   - Industry
   - Company size
   - Any other relevant company information

2. For each record, extract and normalize this information.

3. Where information is missing, try to infer it from other fields or provide a best guess.

4. Return the processed data as a JSON array of objects with these standardized fields:
   - company_name
   - company_website
   - industry
   - company_size
   - company_description
   - founded_year (if available)
   - headquarters (if available)

Here's the data in JSON format:
{batch_json}

Format your response as JSON only, with no explanations or markdown before or after.
"""
        
        return prompt
    
    def _call_claude_api(self, prompt: str) -> str:
        """
        Call Claude API via AWS Bedrock.
        
        Args:
            prompt: The prompt to send to Claude
            
        Returns:
            Claude's response as a string
        """
        if not self.bedrock_client:
            raise ValueError("AWS Bedrock client not initialized")
        
        try:
            # Prepare the request body for Claude
            request_body = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 4096,
                "temperature": 0.1,
                "messages": [
                    {"role": "user", "content": prompt}
                ]
            }
            
            # Make the API call
            response = self.bedrock_client.invoke_model(
                modelId=self.model_id,
                body=json.dumps(request_body)
            )
            
            # Parse the response
            response_body = json.loads(response['body'].read())
            return response_body['content'][0]['text']
            
        except Exception as e:
            logger.error(f"Error calling Claude API: {str(e)}")
            raise
    
    def _parse_claude_response(self, response: str, batch_df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse Claude's response and return the enriched data.
        
        Args:
            response: Claude's response as a string
            batch_df: DataFrame batch to process
            
        Returns:
            Enriched DataFrame
        """
        # Extract the JSON array from the response
        json_start = response.find("[")
        json_end = response.rfind("]") + 1
        
        if json_start >= 0 and json_end > 0:
            try:
                json_str = response[json_start:json_end]
                enriched_data = json.loads(json_str)
                
                # Create a new DataFrame from the enriched data
                enriched_df = pd.DataFrame(enriched_data)
                
                # Ensure required columns exist
                required_columns = [
                    'company_name', 'company_website', 'industry', 'company_size',
                    'company_description', 'founded_year', 'headquarters'
                ]
                
                # Add missing columns
                for col in required_columns:
                    if col not in enriched_df.columns:
                        enriched_df[col] = None
                
                # Add or update enrichment status based on completeness of data
                enriched_df['enrichment_status'] = enriched_df.apply(self._determine_enrichment_status, axis=1)
                enriched_df['enrichment_notes'] = enriched_df.apply(self._generate_enrichment_notes, axis=1)
                
                return enriched_df
                
            except Exception as e:
                logger.error(f"Error parsing Claude response: {str(e)}")
                # Mark all records in the batch as failed
                batch_df['enrichment_status'] = 'Failed'
                batch_df['enrichment_notes'] = f'Error parsing response: {str(e)}'
                return batch_df
        else:
            logger.error("Failed to extract JSON data from Claude response")
            # Mark all records in the batch as failed
            batch_df['enrichment_status'] = 'Failed'
            batch_df['enrichment_notes'] = 'Failed to extract enriched data from AI response'
            return batch_df
            
    def _determine_enrichment_status(self, row):
        """Determine the enrichment status based on data completeness."""
        # Check for essential fields
        if (pd.notna(row.get('company_name')) and 
            pd.notna(row.get('company_website')) and 
            pd.notna(row.get('industry')) and 
            pd.notna(row.get('company_size'))):
            return 'Complete'
        elif pd.notna(row.get('company_name')) or pd.notna(row.get('company_website')):
            return 'Partial'
        else:
            return 'Failed'
    
    def _generate_enrichment_notes(self, row):
        """Generate notes based on enrichment status and available data."""
        status = self._determine_enrichment_status(row)
        
        if status == 'Complete':
            return "All required fields successfully enriched"
        elif status == 'Partial':
            # Identify missing fields
            missing = []
            if pd.isna(row.get('company_name')):
                missing.append("Company Name")
            if pd.isna(row.get('company_website')):
                missing.append("Company Website")
            if pd.isna(row.get('industry')):
                missing.append("Industry")
            if pd.isna(row.get('company_size')):
                missing.append("Company Size")
                
            return f"Partially enriched; Missing data: {', '.join(missing)}"
        else:
            return "Failed to enrich essential company information" 