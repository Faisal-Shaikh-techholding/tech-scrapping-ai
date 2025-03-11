# AI-Powered CSV Processor for Lead Management

A web-based application for processing, enriching, and managing sales leads using AI and external APIs.

## Features

- **CSV/Excel Parsing**: Upload and intelligently parse lead data from CSV or Excel files
- **Data Extraction**: Automatically extract names, companies, and contact information
- **Lead Enrichment**:
  - API-based enrichment via Apollo.io
  - Web scraping for additional company details
- **Interactive UI**:
  - Visual data preview and validation
  - Interactive data editing
  - Progress tracking and enrichment status
- **Salesforce Integration**: Send enriched leads to Salesforce CRM with a single click

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

## Usage

1. Run the application:
```bash
streamlit run app/main.py
```

2. Open your browser and navigate to http://localhost:8501

3. Follow the step-by-step process:
   - Upload CSV or Excel file
   - Preview and validate extracted data
   - Enrich data using APIs and web scraping
   - Review and edit the enriched data
   - Export selected leads to Salesforce

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