import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  Typography,
  Box,
  Container,
  Paper,
  Grid,
  Button,
} from '@mui/material';
import SearchForm from '../components/SearchForm';
import JobCard from '../components/JobCard';

function Home() {
  const navigate = useNavigate();
  const [featuredJobs, setFeaturedJobs] = useState([]);
  const [platforms, setPlatforms] = useState(['workday', 'greenhouse']);
  const [industries, setIndustries] = useState(['Technology', 'Healthcare', 'Finance']);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    // In a real implementation, this would fetch from the API
    // For now, we'll use mock data
    const mockFeaturedJobs = [
      {
        job_id: 1,
        job_title: 'Senior Software Engineer',
        company_name: 'TechCorp',
        company_industry: 'Technology',
        platform: 'workday',
        location: 'San Francisco, CA',
        date_posted: '2023-04-10T00:00:00Z',
        job_url: 'https://example.com/job/1'
      },
      {
        job_id: 2,
        job_title: 'Data Scientist',
        company_name: 'DataInsights',
        company_industry: 'Technology',
        platform: 'greenhouse',
        location: 'Remote',
        date_posted: '2023-04-15T00:00:00Z',
        job_url: 'https://example.com/job/2'
      },
      {
        job_id: 3,
        job_title: 'Product Manager',
        company_name: 'ProductLabs',
        company_industry: 'Technology',
        platform: 'workday',
        location: 'New York, NY',
        date_posted: '2023-04-12T00:00:00Z',
        job_url: 'https://example.com/job/3'
      }
    ];

    // Simulate API call
    setTimeout(() => {
      setFeaturedJobs(mockFeaturedJobs);
      setIsLoading(false);
    }, 500);
  }, []);

  const handleSearch = (searchCriteria) => {
    navigate({
      pathname: '/search',
      search: new URLSearchParams(searchCriteria).toString()
    });
  };

  return (
    <Box>
      {/* Hero Section */}
      <Paper
        sx={{
          py: 8,
          px: 2,
          mb: 4,
          borderRadius: 0,
          background: 'linear-gradient(45deg, #1976d2 30%, #2196f3 90%)'
        }}
      >
        <Container maxWidth="md">
          <Typography
            variant="h2"
            align="center"
            sx={{
              color: 'white',
              fontWeight: 700,
              mb: 3
            }}
          >
            Find Your Dream Job Early
          </Typography>
          <Typography
            variant="h5"
            align="center"
            sx={{
              color: 'white',
              mb: 5,
              fontWeight: 300
            }}
          >
            Discover job opportunities as soon as they are posted
          </Typography>

          <SearchForm
            onSearch={handleSearch}
            platforms={platforms}
            industries={industries}
          />
        </Container>
      </Paper>

      {/* Featured Jobs Section */}
      <Container maxWidth="lg">
        <Box sx={{ mb: 6 }}>
          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
            <Typography variant="h4" component="h2">
              Featured Jobs
            </Typography>
            <Button
              variant="outlined"
              onClick={() => navigate('/search')}
            >
              View All Jobs
            </Button>
          </Box>

          {isLoading ? (
            <Typography>Loading featured jobs...</Typography>
          ) : (
            <Grid container spacing={3}>
              {featuredJobs.map(job => (
                <Grid item xs={12} key={job.job_id}>
                  <JobCard job={job} />
                </Grid>
              ))}
            </Grid>
          )}
        </Box>

        {/* Additional Sections */}
        <Grid container spacing={4} sx={{ mb: 6 }}>
          <Grid item xs={12} md={4}>
            <Paper sx={{ p: 3, height: '100%' }}>
              <Typography variant="h5" component="h3" sx={{ mb: 2 }}>
                Fast Discovery
              </Typography>
              <Typography>
                Find job postings directly from company career sites across various ATS platforms - Workday, Greenhouse, and more.
              </Typography>
            </Paper>
          </Grid>

          <Grid item xs={12} md={4}>
            <Paper sx={{ p: 3, height: '100%' }}>
              <Typography variant="h5" component="h3" sx={{ mb: 2 }}>
                Early Access
              </Typography>
              <Typography>
                Discover opportunities as soon as they are posted, before they appear on aggregator sites like LinkedIn or Indeed.
              </Typography>
            </Paper>
          </Grid>

          <Grid item xs={12} md={4}>
            <Paper sx={{ p: 3, height: '100%' }}>
              <Typography variant="h5" component="h3" sx={{ mb: 2 }}>
                Personalized Alerts
              </Typography>
              <Typography>
                Set up alerts for specific job searches and get notified when new matching positions become available.
              </Typography>
            </Paper>
          </Grid>
        </Grid>
      </Container>
    </Box>
  );
}

export default Home;