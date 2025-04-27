# BetterJobs

BetterJobs is a comprehensive job search platform that retrieves job postings directly from company career portals across various Applicant Tracking Systems (ATS) such as Workday, Greenhouse, and more. This approach allows users to discover job opportunities as soon as they are posted, rather than waiting for them to appear on aggregator sites like LinkedIn or Indeed.

## Project Structure

The project consists of three main components:

1. **Pipeline**: A Dagster data pipeline for retrieving, processing, and storing job data
2. **API**: An Express server to provide data to the UI
3. **UI**: A React frontend for searching and browsing job listings

### Pipeline Components
- **URL Discovery**: Finds career site URLs for companies
- **Job Scraping**: Extracts job listings from company sites
- **Data Storage**: Stores job and company data in DuckDB

### API Components
- **Jobs API**: Endpoints for searching and retrieving job data
- **Companies API**: Endpoints for listing companies
- **Stats API**: Endpoints for viewing platform statistics

### UI Components
- **Search**: Advanced job search with filters
- **Company Browser**: View companies and their job listings
- **Stats Dashboard**: View platform statistics

## Getting Started

### Prerequisites

- Node.js (v14+)
- Python (v3.8+)
- Dagster

### Installation

1. Clone the repository
```bash
git clone [repository URL]
cd betterjobs
```

2. Set up the Dagster pipeline
```bash
cd pipeline/dagster_betterjobs
pip install -e .
```

3. Set up the API server
```bash
cd ../../api
npm install
```

4. Set up the UI
```bash
cd ../ui
npm install
```

### Running the Application

1. Start the Dagster pipeline
```bash
cd pipeline/dagster_betterjobs
dagster dev
```

2. Start the API server
```bash
cd ../../api
npm run dev
```

3. Start the UI
```bash
cd ../ui
npm start
```

### Using the CLI

The project includes a command-line interface for interacting with the job data:

```bash
cd pipeline/dagster_betterjobs
python -m dagster_betterjobs.cli search "Software Engineer"
```

Available commands:
- `search`: Search for jobs matching a keyword
- `stats`: Show job statistics
- `companies`: List companies

## Features

- **Early Job Discovery**: Find jobs as soon as they're posted
- **Direct Links**: Apply directly through company career sites
- **Comprehensive Search**: Filter by title, company, location, and more
- **Statistics**: View trends in job postings across platforms
- **Company Tracking**: Monitor job listings from specific companies

## Future Enhancements

- Add support for more ATS platforms (Lever, SmartRecruiters, ICIMS)
- Implement user accounts and saved searches
- Add email notifications for new job matches
- Improve job matching with ML-based relevance scoring

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
