#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Logging Configuration Module

This module configures the logging for the application.
"""

import logging
import os
from datetime import datetime

def setup_logging():
    """
    Set up logging configuration for the application.
    
    Returns:
        logging.Logger: Configured logger instance
    """
    # Create logs directory if it doesn't exist
    log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'logs')
    os.makedirs(log_dir, exist_ok=True)
    
    # Generate log filename with timestamp
    log_filename = os.path.join(log_dir, f"app_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler()
        ]
    )
    
    # Create logger
    logger = logging.getLogger('csv_processor')
    
    logger.info("Logging initialized")
    return logger 