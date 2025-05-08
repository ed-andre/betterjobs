# BetterJobs - Frontend

This is the frontend component of the BetterJobs project. For complete project information and pipeline setup, please refer to the [main README](../README.md) in the root directory.

BetterJobs is a personal project that aggregates job listings from various recruiting platforms into a single, searchable interface.

## ⚠️ Disclaimer

**This is a personal work in progress project.**

- This project is not officially supported
- Use at your own risk
- No warranty or support is provided
- The codebase may change significantly without notice

## Features

- 🔍 Search jobs across multiple recruiting platforms (Workday, BambooHR, Greenhouse, SmartRecruiters, etc.)
- 📋 View detailed job descriptions in a clean, consistent format
- 🏷️ Filter jobs by platform, company, or keywords
- 🔄 Regular updates from job source APIs
- 🚀 Built with React Router 7, TypeScript, and TailwindCSS

## Local Setup

### Prerequisites

- Node.js (v18+)
- npm or pnpm
- Supabase account (for data storage)
- Pipeline setup complete (see [main README](../README.md) for pipeline setup)

### Environment Configuration

Create a `.env` file in the `site` directory with the following variables:

```
SUPABASE_URL=your_supabase_url
SUPABASE_ANON_KEY=your_supabase_anon_key
```

### Installation

```bash
cd site
npm install
```

### Development

```bash
npm run dev
```

Your application will be available at `http://localhost:5173`.

## Building for Production

```bash
npm run build
```

## Technology Stack

- **Frontend**: React, TypeScript, TailwindCSS, ShadcnUI
- **Routing**: React Router 7
- **Data Storage**: Supabase (PostgreSQL)
- **Data Pipeline**: Dagster (see the [main README](../README.md) for pipeline details)

## Project Structure

The frontend is part of a larger project that includes:

1. **Pipeline**: A Dagster data pipeline for retrieving, processing, and storing job data
2. **Frontend**: This React application for searching and browsing job listings

## Contributing

This is a personal project and not actively seeking contributions. However, feel free to fork the repository if you find it useful.

---

Built as a personal project for exploring job market data and modern web technologies.
