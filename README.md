# AI-Powered CSV Processor for Company Data

This application allows users to upload CSV files with company information, enrich the data using multiple services, and submit it to Salesforce CRM.

## Features

- Upload and process CSV files with company information
- Enrich company data using multiple services:
  - Apollo.io API
  - Crunchbase API
  - Web scraping
- One-click enrichment that tries all services in sequence
- Review and edit enriched data
- Export data to Salesforce CRM

## Code Structure

The application is organized into the following components:

### Main Application

- `app/main.py`: Main application entry point
- `app/utils/session_state.py`: Session state management
- `app/utils/logging_config.py`: Logging configuration

### Components

- `app/components/sidebar.py`: Sidebar component
- `app/components/csv_upload.py`: CSV upload component
- `app/components/data_preview.py`: Data preview component
- `app/components/data_enrichment.py`: Data enrichment component
- `app/components/data_editor.py`: Data editor component
- `app/components/salesforce_export.py`: Salesforce export component

### Services

- `app/services/apollo_service.py`: Apollo.io API service
- `app/services/crunchbase_service.py`: Crunchbase API service
- `app/services/web_scraper.py`: Web scraping service
- `app/services/salesforce_service.py`: Salesforce API service

### Utilities

- `app/utils/data_processing.py`: Data processing utilities
- `app/utils/enrichment_utils.py`: Enrichment utilities

#### Enrichment Utilities

- `app/utils/enrichment/progress_utils.py`: Progress tracking utilities
- `app/utils/enrichment/one_click.py`: One-click enrichment functionality
- `app/utils/enrichment/service_enrichment.py`: Service-specific enrichment functions
- `app/utils/enrichment/results_display.py`: Results display utilities

## Optimization Notes

The code has been optimized for:

1. **Modularity**: Split large files into smaller, focused modules
2. **Performance**: Reduced redundancy and improved data processing efficiency
3. **User Experience**: Added stop buttons to all enrichment processes
4. **Error Handling**: Improved error handling and recovery mechanisms
5. **Code Reuse**: Created common utilities for shared functionality

## Usage

1. Upload a CSV file with company information
2. Preview and validate the data
3. Enrich the data using one or more services
4. Review and edit the enriched data
5. Export the data to Salesforce CRM

## Architecture

This application is built with a modular architecture following best practices:

```
app/
├── main.py                # Main application entry point
├── components/            # UI components
│   ├── sidebar.py         # Navigation and configuration sidebar
│   ├── csv_upload.py      # CSV upload interface
│   ├── data_preview.py    # Data preview interface
│   ├── data_enrichment.py # Data enrichment interface
│   ├── data_editor.py     # Data editing interface
│   └── salesforce_export.py # Salesforce export interface
├── utils/                 # Utility functions
│   ├── logging_config.py  # Logging configuration
│   ├── session_state.py   # Session state management
│   └── data_processing.py # Data processing utilities
└── services/              # External service integrations
    ├── apollo_service.py  # Apollo.io API service
    ├── crunchbase_service.py # Crunchbase API service
    ├── web_scraper.py     # Web scraping service
    └── salesforce_service.py # Salesforce API service
```

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/ai-csv-processor.git
cd ai-csv-processor
```

2. Create a virtual environment and activate it:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install required packages:
```bash
pip install -r requirements.txt
```

4. Create a `.env` file with your API keys (optional):
```
APOLLO_API_KEY=your_apollo_key
SF_USERNAME=your_salesforce_username
SF_PASSWORD=your_salesforce_password
SF_SECURITY_TOKEN=your_salesforce_token
```

## API Configuration

The application requires the following API credentials:

- **Apollo.io API Key**: For lead enrichment (contact details, job roles)
- **Salesforce Credentials**: For submitting leads to Salesforce CRM
  - Username
  - Password
  - Security Token
  - Domain (login or test)

These can be configured directly in the application's sidebar.

## Data Privacy & Security

- API keys and credentials are stored in the Streamlit session state and are not persisted
- Data is processed locally and not stored on external servers
- Consider implementing additional security measures for production use

## Development

### Adding New Enrichment Sources

To add a new enrichment source:

1. Create a new service in the `app/services/` directory
2. Implement the service with a similar interface to `ApolloService`
3. Add the service to the `data_enrichment.py` component

### Modifying Salesforce Integration

To customize the Salesforce integration:

1. Modify the field mappings in `salesforce_service.py`
2. Add custom validation or transformation logic as needed

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- [Streamlit](https://streamlit.io/)
- [Pandas](https://pandas.pydata.org/)
- [Simple Salesforce](https://github.com/simple-salesforce/simple-salesforce)
- [BeautifulSoup](https://www.crummy.com/software/BeautifulSoup/)
- [Apollo.io](https://www.apollo.io/) 