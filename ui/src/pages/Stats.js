import React, { useState, useEffect } from 'react';
import {
  Typography,
  Box,
  Container,
  Paper,
  Grid,
  CircularProgress,
  Card,
  CardContent,
  Divider,
  Button,
  Select,
  MenuItem,
  FormControl,
  InputLabel,
} from '@mui/material';
import SyncIcon from '@mui/icons-material/Sync';
import BusinessIcon from '@mui/icons-material/Business';
import WorkIcon from '@mui/icons-material/Work';
import TrendingUpIcon from '@mui/icons-material/TrendingUp';
import PublicIcon from '@mui/icons-material/Public';

// For charts, you would typically use a library like Chart.js or Recharts
// For simplicity in this prototype, we'll use mock data without actual charts

function Stats() {
  const [stats, setStats] = useState(null);
  const [isLoading, setIsLoading] = useState(true);
  const [timeRange, setTimeRange] = useState('30days');

  useEffect(() => {
    fetchStats();
  }, [timeRange]);

  const fetchStats = () => {
    setIsLoading(true);

    // In a real implementation, this would fetch from the API with the time range parameter
    // For now, we'll use mock data
    setTimeout(() => {
      const mockStats = {
        overview: {
          totalCompanies: 1250,
          totalJobs: 8750,
          companiesWithUrls: 980,
          companiesWithJobs: 760,
          jobsAddedLast24h: 145,
          jobsAddedLastWeek: 980,
        },
        platforms: [
          { name: 'Workday', companies: 580, jobs: 4200, color: '#1976d2' },
          { name: 'Greenhouse', companies: 430, jobs: 3500, color: '#2e7d32' },
          { name: 'Lever', companies: 120, jobs: 650, color: '#ed6c02' },
          { name: 'ICIMS', companies: 80, jobs: 320, color: '#9c27b0' },
          { name: 'Other', companies: 40, jobs: 80, color: '#d32f2f' },
        ],
        industries: [
          { name: 'Technology', companies: 450, jobs: 3800 },
          { name: 'Healthcare', companies: 210, jobs: 1600 },
          { name: 'Finance', companies: 180, jobs: 1200 },
          { name: 'Manufacturing', companies: 120, jobs: 800 },
          { name: 'Retail', companies: 95, jobs: 650 },
          { name: 'Other', companies: 195, jobs: 700 },
        ],
        locations: [
          { name: 'Remote', jobs: 2100 },
          { name: 'San Francisco, CA', jobs: 950 },
          { name: 'New York, NY', jobs: 820 },
          { name: 'Seattle, WA', jobs: 680 },
          { name: 'Austin, TX', jobs: 520 },
          { name: 'Boston, MA', jobs: 480 },
          { name: 'Chicago, IL', jobs: 430 },
          { name: 'Los Angeles, CA', jobs: 410 },
          { name: 'Denver, CO', jobs: 380 },
          { name: 'Other', jobs: 1980 },
        ],
        trending: {
          keywords: [
            { name: 'Developer', count: 1250 },
            { name: 'Engineer', count: 980 },
            { name: 'Data', count: 760 },
            { name: 'Manager', count: 650 },
            { name: 'Senior', count: 620 },
          ],
          companies: [
            { name: 'TechCorp', jobs: 85 },
            { name: 'HealthSolutions', jobs: 72 },
            { name: 'FinancialGroup', jobs: 68 },
            { name: 'RetailGiants', jobs: 65 },
            { name: 'DataInsights', jobs: 58 },
          ],
        },
        timeline: {
          labels: ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
          datasets: [
            {
              label: 'New Jobs',
              data: [420, 380, 450, 520, 480, 590, 620, 580, 650, 720, 680, 750],
            },
            {
              label: 'New Companies',
              data: [45, 38, 42, 50, 48, 55, 60, 52, 65, 70, 68, 75],
            },
          ],
        },
      };

      setStats(mockStats);
      setIsLoading(false);
    }, 800);
  };

  const handleTimeRangeChange = (event) => {
    setTimeRange(event.target.value);
  };

  // Simple component to display a stat with an icon
  const StatCard = ({ icon, title, value, subvalue, color }) => (
    <Card raised sx={{ height: '100%' }}>
      <CardContent>
        <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
          <Box
            sx={{
              backgroundColor: color || 'primary.main',
              borderRadius: '50%',
              width: 40,
              height: 40,
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              mr: 2
            }}
          >
            {icon}
          </Box>
          <Typography variant="h6" component="h3">
            {title}
          </Typography>
        </Box>
        <Typography variant="h4" component="p" sx={{ mb: 1, fontWeight: 600 }}>
          {value.toLocaleString()}
        </Typography>
        {subvalue && (
          <Typography variant="body2" color="text.secondary">
            {subvalue}
          </Typography>
        )}
      </CardContent>
    </Card>
  );

  return (
    <Container maxWidth="lg" sx={{ py: 4 }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
        <Typography variant="h4" component="h1">
          Statistics
        </Typography>

        <Box sx={{ display: 'flex', gap: 2 }}>
          <FormControl variant="outlined" size="small" sx={{ minWidth: 150 }}>
            <InputLabel>Time Range</InputLabel>
            <Select
              value={timeRange}
              onChange={handleTimeRangeChange}
              label="Time Range"
            >
              <MenuItem value="7days">Last 7 days</MenuItem>
              <MenuItem value="30days">Last 30 days</MenuItem>
              <MenuItem value="90days">Last 90 days</MenuItem>
              <MenuItem value="year">Last year</MenuItem>
              <MenuItem value="all">All time</MenuItem>
            </Select>
          </FormControl>

          <Button
            variant="outlined"
            startIcon={<SyncIcon />}
            onClick={fetchStats}
          >
            Refresh
          </Button>
        </Box>
      </Box>

      {isLoading ? (
        <Box sx={{ display: 'flex', justifyContent: 'center', my: 8 }}>
          <CircularProgress />
        </Box>
      ) : (
        <>
          {/* Overview Cards */}
          <Grid container spacing={3} sx={{ mb: 4 }}>
            <Grid item xs={12} sm={6} md={3}>
              <StatCard
                icon={<BusinessIcon sx={{ color: 'white' }} />}
                title="Companies"
                value={stats.overview.totalCompanies}
                subvalue={`${stats.overview.companiesWithUrls} with career URLs`}
                color="#1976d2"
              />
            </Grid>
            <Grid item xs={12} sm={6} md={3}>
              <StatCard
                icon={<WorkIcon sx={{ color: 'white' }} />}
                title="Jobs"
                value={stats.overview.totalJobs}
                subvalue={`across ${stats.overview.companiesWithJobs} companies`}
                color="#2e7d32"
              />
            </Grid>
            <Grid item xs={12} sm={6} md={3}>
              <StatCard
                icon={<TrendingUpIcon sx={{ color: 'white' }} />}
                title="New Today"
                value={stats.overview.jobsAddedLast24h}
                subvalue="jobs added in the last 24h"
                color="#ed6c02"
              />
            </Grid>
            <Grid item xs={12} sm={6} md={3}>
              <StatCard
                icon={<PublicIcon sx={{ color: 'white' }} />}
                title="This Week"
                value={stats.overview.jobsAddedLastWeek}
                subvalue="jobs added in the last 7 days"
                color="#9c27b0"
              />
            </Grid>
          </Grid>

          {/* Platform Statistics */}
          <Paper sx={{ p: 3, mb: 4 }}>
            <Typography variant="h5" component="h2" sx={{ mb: 3 }}>
              Platform Statistics
            </Typography>

            <Grid container spacing={3}>
              <Grid item xs={12} md={6}>
                {/* In a real implementation, this would be a pie chart */}
                <Typography variant="h6" component="h3" sx={{ mb: 2 }}>
                  Companies by Platform
                </Typography>

                {stats.platforms.map((platform) => (
                  <Box key={platform.name} sx={{ mb: 2 }}>
                    <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 0.5 }}>
                      <Typography variant="body1">
                        {platform.name}
                      </Typography>
                      <Typography variant="body1" fontWeight="bold">
                        {platform.companies}
                      </Typography>
                    </Box>
                    <Box
                      sx={{
                        height: 8,
                        backgroundColor: '#f0f0f0',
                        borderRadius: 4,
                        overflow: 'hidden'
                      }}
                    >
                      <Box
                        sx={{
                          height: '100%',
                          width: `${(platform.companies / stats.overview.totalCompanies) * 100}%`,
                          backgroundColor: platform.color,
                          borderRadius: 4
                        }}
                      />
                    </Box>
                  </Box>
                ))}
              </Grid>

              <Grid item xs={12} md={6}>
                {/* In a real implementation, this would be a pie chart */}
                <Typography variant="h6" component="h3" sx={{ mb: 2 }}>
                  Jobs by Platform
                </Typography>

                {stats.platforms.map((platform) => (
                  <Box key={platform.name} sx={{ mb: 2 }}>
                    <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 0.5 }}>
                      <Typography variant="body1">
                        {platform.name}
                      </Typography>
                      <Typography variant="body1" fontWeight="bold">
                        {platform.jobs}
                      </Typography>
                    </Box>
                    <Box
                      sx={{
                        height: 8,
                        backgroundColor: '#f0f0f0',
                        borderRadius: 4,
                        overflow: 'hidden'
                      }}
                    >
                      <Box
                        sx={{
                          height: '100%',
                          width: `${(platform.jobs / stats.overview.totalJobs) * 100}%`,
                          backgroundColor: platform.color,
                          borderRadius: 4
                        }}
                      />
                    </Box>
                  </Box>
                ))}
              </Grid>
            </Grid>
          </Paper>

          {/* Industry and Location Statistics */}
          <Grid container spacing={3} sx={{ mb: 4 }}>
            <Grid item xs={12} md={6}>
              <Paper sx={{ p: 3, height: '100%' }}>
                <Typography variant="h5" component="h2" sx={{ mb: 3 }}>
                  Top Industries
                </Typography>

                {stats.industries.map((industry, index) => (
                  <Box key={industry.name} sx={{ mb: 2 }}>
                    <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 0.5 }}>
                      <Typography variant="body1">
                        {industry.name}
                      </Typography>
                      <Typography variant="body1" fontWeight="bold">
                        {industry.jobs} jobs
                      </Typography>
                    </Box>
                    <Box
                      sx={{
                        height: 8,
                        backgroundColor: '#f0f0f0',
                        borderRadius: 4,
                        overflow: 'hidden'
                      }}
                    >
                      <Box
                        sx={{
                          height: '100%',
                          width: `${(industry.jobs / stats.overview.totalJobs) * 100}%`,
                          backgroundColor: `hsl(${index * 30}, 70%, 50%)`,
                          borderRadius: 4
                        }}
                      />
                    </Box>
                  </Box>
                ))}
              </Paper>
            </Grid>

            <Grid item xs={12} md={6}>
              <Paper sx={{ p: 3, height: '100%' }}>
                <Typography variant="h5" component="h2" sx={{ mb: 3 }}>
                  Top Locations
                </Typography>

                {stats.locations.slice(0, 6).map((location, index) => (
                  <Box key={location.name} sx={{ mb: 2 }}>
                    <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 0.5 }}>
                      <Typography variant="body1">
                        {location.name}
                      </Typography>
                      <Typography variant="body1" fontWeight="bold">
                        {location.jobs} jobs
                      </Typography>
                    </Box>
                    <Box
                      sx={{
                        height: 8,
                        backgroundColor: '#f0f0f0',
                        borderRadius: 4,
                        overflow: 'hidden'
                      }}
                    >
                      <Box
                        sx={{
                          height: '100%',
                          width: `${(location.jobs / stats.overview.totalJobs) * 100}%`,
                          backgroundColor: `hsl(${180 + index * 30}, 70%, 45%)`,
                          borderRadius: 4
                        }}
                      />
                    </Box>
                  </Box>
                ))}
              </Paper>
            </Grid>
          </Grid>

          {/* Trending Section */}
          <Paper sx={{ p: 3, mb: 4 }}>
            <Typography variant="h5" component="h2" sx={{ mb: 3 }}>
              Trending
            </Typography>

            <Grid container spacing={3}>
              <Grid item xs={12} md={6}>
                <Typography variant="h6" component="h3" sx={{ mb: 2 }}>
                  Popular Keywords
                </Typography>

                {stats.trending.keywords.map((keyword, index) => (
                  <Box key={keyword.name} sx={{ mb: 2 }}>
                    <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 0.5 }}>
                      <Typography variant="body1">
                        {keyword.name}
                      </Typography>
                      <Typography variant="body1" fontWeight="bold">
                        {keyword.count} jobs
                      </Typography>
                    </Box>
                    <Box
                      sx={{
                        height: 8,
                        backgroundColor: '#f0f0f0',
                        borderRadius: 4,
                        overflow: 'hidden'
                      }}
                    >
                      <Box
                        sx={{
                          height: '100%',
                          width: `${(keyword.count / 1500) * 100}%`,
                          backgroundColor: `hsl(${index * 30}, 80%, 50%)`,
                          borderRadius: 4
                        }}
                      />
                    </Box>
                  </Box>
                ))}
              </Grid>

              <Grid item xs={12} md={6}>
                <Typography variant="h6" component="h3" sx={{ mb: 2 }}>
                  Companies with Most Jobs
                </Typography>

                {stats.trending.companies.map((company, index) => (
                  <Box key={company.name} sx={{ mb: 2 }}>
                    <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 0.5 }}>
                      <Typography variant="body1">
                        {company.name}
                      </Typography>
                      <Typography variant="body1" fontWeight="bold">
                        {company.jobs} jobs
                      </Typography>
                    </Box>
                    <Box
                      sx={{
                        height: 8,
                        backgroundColor: '#f0f0f0',
                        borderRadius: 4,
                        overflow: 'hidden'
                      }}
                    >
                      <Box
                        sx={{
                          height: '100%',
                          width: `${(company.jobs / 100) * 100}%`,
                          backgroundColor: `hsl(${180 + index * 30}, 80%, 45%)`,
                          borderRadius: 4
                        }}
                      />
                    </Box>
                  </Box>
                ))}
              </Grid>
            </Grid>
          </Paper>

          {/* Growth Over Time */}
          <Paper sx={{ p: 3 }}>
            <Typography variant="h5" component="h2" sx={{ mb: 3 }}>
              Growth Over Time
            </Typography>

            {/* In a real implementation, this would be a line chart */}
            <Box sx={{ height: 300, p: 2, backgroundColor: '#f9f9f9', borderRadius: 2, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
              <Typography variant="body1" color="text.secondary">
                This would be a line chart showing growth over time.
              </Typography>
            </Box>

            <Box sx={{ mt: 3, display: 'flex', justifyContent: 'space-between' }}>
              {stats.timeline.labels.map((month, index) => (
                <Typography key={month} variant="body2" color="text.secondary">
                  {month}
                </Typography>
              ))}
            </Box>
          </Paper>
        </>
      )}
    </Container>
  );
}

export default Stats;