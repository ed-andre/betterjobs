import React, { useState, useEffect } from 'react';
import { useLocation } from 'react-router-dom';
import {
  Typography,
  Box,
  Container,
  Grid,
  CircularProgress,
  Pagination,
  Divider,
  Alert,
  FormControlLabel,
  Checkbox,
  Card,
  CardContent,
} from '@mui/material';
import SearchForm from '../components/SearchForm';
import JobCard from '../components/JobCard';

function JobSearch() {
  const location = useLocation();
  const searchParams = new URLSearchParams(location.search);

  // Get search parameters from URL
  const keywordParam = searchParams.get('keyword') || '';
  const locationParam = searchParams.get('location') || '';
  const platformParam = searchParams.get('platform') || '';
  const industryParam = searchParams.get('industry') || '';

  const [jobs, setJobs] = useState([]);
  const [isLoading, setIsLoading] = useState(false);
  const [platforms, setPlatforms] = useState(['workday', 'greenhouse']);
  const [industries, setIndustries] = useState(['Technology', 'Healthcare', 'Finance', 'Manufacturing', 'Retail']);
  const [page, setPage] = useState(1);
  const [totalPages, setTotalPages] = useState(1);
  const [filters, setFilters] = useState({
    keyword: keywordParam,
    location: locationParam,
    platform: platformParam,
    industry: industryParam,
  });

  // Filter options for locations and companies
  const [locationOptions, setLocationOptions] = useState([]);
  const [selectedLocations, setSelectedLocations] = useState([]);
  const [companyOptions, setCompanyOptions] = useState([]);
  const [selectedCompanies, setSelectedCompanies] = useState([]);

  useEffect(() => {
    // In a real implementation, this would fetch data from the API
    // based on search parameters
    fetchJobs();
  }, [location.search, page]); // Re-fetch when search params or page changes

  const fetchJobs = () => {
    setIsLoading(true);

    // In a real implementation, these would be API calls
    // For now, we'll use mock data
    setTimeout(() => {
      const mockJobs = [];

      // Generate more or fewer results based on whether there are search params
      const count = keywordParam ? (Math.random() > 0.3 ? 15 : 0) : 20;

      for (let i = 1; i <= count; i++) {
        mockJobs.push({
          job_id: i,
          job_title: keywordParam
            ? `${keywordParam} ${['Engineer', 'Developer', 'Specialist', 'Manager'][Math.floor(Math.random() * 4)]}`
            : ['Frontend Developer', 'Data Scientist', 'Product Manager', 'UX Designer'][Math.floor(Math.random() * 4)],
          company_name: ['TechCorp', 'DataInsights', 'ProductLabs', 'DesignStudio'][Math.floor(Math.random() * 4)],
          company_industry: industries[Math.floor(Math.random() * industries.length)],
          platform: platforms[Math.floor(Math.random() * platforms.length)],
          location: locationParam || ['San Francisco, CA', 'New York, NY', 'Seattle, WA', 'Remote'][Math.floor(Math.random() * 4)],
          date_posted: new Date(Date.now() - Math.floor(Math.random() * 30) * 24 * 60 * 60 * 1000).toISOString(),
          job_url: `https://example.com/job/${i}`
        });
      }

      // Extract unique locations and companies for filter options
      const uniqueLocations = [...new Set(mockJobs.map(job => job.location))];
      const uniqueCompanies = [...new Set(mockJobs.map(job => job.company_name))];

      setLocationOptions(uniqueLocations);
      setCompanyOptions(uniqueCompanies);
      setTotalPages(Math.ceil(mockJobs.length / 10));
      setJobs(mockJobs);
      setIsLoading(false);
    }, 800);
  };

  const handleSearch = (searchCriteria) => {
    setPage(1); // Reset to first page on new search
    setFilters(searchCriteria);
  };

  const handleLocationFilterChange = (location) => {
    if (selectedLocations.includes(location)) {
      setSelectedLocations(selectedLocations.filter(loc => loc !== location));
    } else {
      setSelectedLocations([...selectedLocations, location]);
    }
  };

  const handleCompanyFilterChange = (company) => {
    if (selectedCompanies.includes(company)) {
      setSelectedCompanies(selectedCompanies.filter(comp => comp !== company));
    } else {
      setSelectedCompanies([...selectedCompanies, company]);
    }
  };

  // Apply additional filters (beyond the search parameters)
  const filteredJobs = jobs.filter(job => {
    if (selectedLocations.length > 0 && !selectedLocations.includes(job.location)) {
      return false;
    }
    if (selectedCompanies.length > 0 && !selectedCompanies.includes(job.company_name)) {
      return false;
    }
    return true;
  });

  // Paginate the results
  const jobsPerPage = 10;
  const startIndex = (page - 1) * jobsPerPage;
  const endIndex = startIndex + jobsPerPage;
  const paginatedJobs = filteredJobs.slice(startIndex, endIndex);

  const handlePageChange = (event, value) => {
    setPage(value);
    window.scrollTo(0, 0);
  };

  return (
    <Container maxWidth="lg">
      <Typography variant="h4" component="h1" sx={{ mb: 3 }}>
        Job Search
      </Typography>

      <SearchForm
        onSearch={handleSearch}
        platforms={platforms}
        industries={industries}
      />

      <Grid container spacing={3}>
        {/* Filters */}
        <Grid item xs={12} md={3}>
          <Card sx={{ mb: 2 }}>
            <CardContent>
              <Typography variant="h6" sx={{ mb: 2 }}>
                Filters
              </Typography>

              {/* Location filters */}
              <Box sx={{ mb: 3 }}>
                <Typography variant="subtitle1" sx={{ mb: 1, fontWeight: 600 }}>
                  Locations
                </Typography>
                <Divider sx={{ mb: 1 }} />
                {locationOptions.map(location => (
                  <FormControlLabel
                    key={location}
                    control={
                      <Checkbox
                        checked={selectedLocations.includes(location)}
                        onChange={() => handleLocationFilterChange(location)}
                        size="small"
                      />
                    }
                    label={location}
                  />
                ))}
              </Box>

              {/* Company filters */}
              <Box>
                <Typography variant="subtitle1" sx={{ mb: 1, fontWeight: 600 }}>
                  Companies
                </Typography>
                <Divider sx={{ mb: 1 }} />
                {companyOptions.map(company => (
                  <FormControlLabel
                    key={company}
                    control={
                      <Checkbox
                        checked={selectedCompanies.includes(company)}
                        onChange={() => handleCompanyFilterChange(company)}
                        size="small"
                      />
                    }
                    label={company}
                  />
                ))}
              </Box>
            </CardContent>
          </Card>
        </Grid>

        {/* Search results */}
        <Grid item xs={12} md={9}>
          <Box sx={{ mb: 2 }}>
            <Typography variant="body1">
              {isLoading ? 'Searching...' : `Found ${filteredJobs.length} jobs${keywordParam ? ` for "${keywordParam}"` : ''}`}
            </Typography>
          </Box>

          {isLoading ? (
            <Box sx={{ display: 'flex', justifyContent: 'center', my: 4 }}>
              <CircularProgress />
            </Box>
          ) : (
            <>
              {filteredJobs.length === 0 ? (
                <Alert severity="info" sx={{ mb: 2 }}>
                  No jobs found matching your criteria. Try broadening your search.
                </Alert>
              ) : (
                <>
                  {paginatedJobs.map(job => (
                    <JobCard key={job.job_id} job={job} />
                  ))}

                  {totalPages > 1 && (
                    <Box sx={{ display: 'flex', justifyContent: 'center', mt: 4 }}>
                      <Pagination
                        count={totalPages}
                        page={page}
                        onChange={handlePageChange}
                        color="primary"
                      />
                    </Box>
                  )}
                </>
              )}
            </>
          )}
        </Grid>
      </Grid>
    </Container>
  );
}

export default JobSearch;