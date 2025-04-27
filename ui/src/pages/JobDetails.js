import React, { useState, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import {
  Typography,
  Box,
  Container,
  Paper,
  Grid,
  Button,
  Chip,
  Divider,
  CircularProgress,
  Alert,
  Card,
  CardContent,
  CardActions,
} from '@mui/material';
import BusinessIcon from '@mui/icons-material/Business';
import LocationOnIcon from '@mui/icons-material/LocationOn';
import DateRangeIcon from '@mui/icons-material/DateRange';
import ArrowBackIcon from '@mui/icons-material/ArrowBack';
import BookmarkBorderIcon from '@mui/icons-material/BookmarkBorder';
import ShareIcon from '@mui/icons-material/Share';

function JobDetails() {
  const { id } = useParams();
  const navigate = useNavigate();
  const [job, setJob] = useState(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    // In a real implementation, this would fetch job details from the API
    // For now, we'll use mock data
    const fetchJobDetails = async () => {
      setIsLoading(true);
      setError(null);

      try {
        // Simulate API call
        await new Promise(resolve => setTimeout(resolve, 800));

        // Generate mock job details
        const mockJob = {
          job_id: parseInt(id),
          job_title: 'Senior Software Engineer',
          company_name: 'TechCorp',
          company_industry: 'Technology',
          platform: 'workday',
          location: 'San Francisco, CA',
          date_posted: new Date(Date.now() - Math.floor(Math.random() * 30) * 24 * 60 * 60 * 1000).toISOString(),
          date_retrieved: new Date().toISOString(),
          job_url: `https://example.com/job/${id}`,
          job_description: `
            <h3>About the Position</h3>
            <p>We are looking for a Senior Software Engineer to join our dynamic team. This role will be responsible for designing, developing, and maintaining software applications that support our core business operations.</p>

            <h3>Responsibilities</h3>
            <ul>
              <li>Design and develop high-quality software solutions</li>
              <li>Collaborate with cross-functional teams to define, design, and ship new features</li>
              <li>Work with outside data sources and APIs</li>
              <li>Unit-test code for robustness, including edge cases, usability, and general reliability</li>
              <li>Work on bug fixing and improving application performance</li>
              <li>Continuously discover, evaluate, and implement new technologies to maximize development efficiency</li>
            </ul>

            <h3>Requirements</h3>
            <ul>
              <li>5+ years of professional software development experience</li>
              <li>Strong proficiency in JavaScript, including DOM manipulation and the JavaScript object model</li>
              <li>Thorough understanding of React.js and its core principles</li>
              <li>Experience with popular React.js workflows (such as Redux)</li>
              <li>Experience with data structure libraries (e.g., Immutable.js)</li>
              <li>Familiarity with RESTful APIs</li>
              <li>Knowledge of modern authorization mechanisms, such as JSON Web Token</li>
              <li>Familiarity with modern front-end build pipelines and tools</li>
              <li>Experience with common front-end development tools such as Babel, Webpack, NPM, etc.</li>
              <li>A knack for benchmarking and optimization</li>
              <li>Ability to understand business requirements and translate them into technical requirements</li>
            </ul>

            <h3>Benefits</h3>
            <ul>
              <li>Competitive salary</li>
              <li>Health, dental, and vision insurance</li>
              <li>401(k) plan with employer match</li>
              <li>Flexible work schedule</li>
              <li>Remote work options</li>
              <li>Professional development opportunities</li>
              <li>Paid time off</li>
            </ul>
          `
        };

        setJob(mockJob);
      } catch (err) {
        setError('Failed to load job details. Please try again later.');
        console.error(err);
      } finally {
        setIsLoading(false);
      }
    };

    fetchJobDetails();
  }, [id]);

  // Format date for display
  const formatDate = (dateString) => {
    if (!dateString) return 'Unknown date';

    const date = new Date(dateString);
    return date.toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'long',
      day: 'numeric'
    });
  };

  if (isLoading) {
    return (
      <Container maxWidth="md" sx={{ py: 4 }}>
        <Box sx={{ display: 'flex', justifyContent: 'center', my: 8 }}>
          <CircularProgress />
        </Box>
      </Container>
    );
  }

  if (error) {
    return (
      <Container maxWidth="md" sx={{ py: 4 }}>
        <Alert severity="error" sx={{ mb: 3 }}>
          {error}
        </Alert>
        <Button
          startIcon={<ArrowBackIcon />}
          onClick={() => navigate(-1)}
        >
          Go Back
        </Button>
      </Container>
    );
  }

  if (!job) {
    return (
      <Container maxWidth="md" sx={{ py: 4 }}>
        <Alert severity="warning" sx={{ mb: 3 }}>
          Job not found
        </Alert>
        <Button
          startIcon={<ArrowBackIcon />}
          onClick={() => navigate('/search')}
        >
          Back to Search
        </Button>
      </Container>
    );
  }

  return (
    <Container maxWidth="md" sx={{ py: 4 }}>
      <Button
        startIcon={<ArrowBackIcon />}
        onClick={() => navigate(-1)}
        sx={{ mb: 3 }}
      >
        Back to Results
      </Button>

      <Grid container spacing={3}>
        <Grid item xs={12} md={8}>
          {/* Main job content */}
          <Paper sx={{ p: 3, mb: 3 }}>
            <Typography
              variant="h4"
              component="h1"
              sx={{
                fontWeight: 600,
                mb: 2,
                color: 'primary.main'
              }}
            >
              {job.job_title}
            </Typography>

            <Box sx={{ mb: 3 }}>
              <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
                <BusinessIcon sx={{ mr: 1, color: 'text.secondary' }} />
                <Typography variant="h6" color="text.secondary">
                  {job.company_name}
                </Typography>
              </Box>

              <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
                <LocationOnIcon sx={{ mr: 1, color: 'text.secondary' }} />
                <Typography variant="body1" color="text.secondary">
                  {job.location}
                </Typography>
              </Box>

              <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
                <DateRangeIcon sx={{ mr: 1, color: 'text.secondary' }} />
                <Typography variant="body1" color="text.secondary">
                  Posted: {formatDate(job.date_posted)}
                </Typography>
              </Box>
            </Box>

            <Box sx={{ mb: 3 }}>
              <Chip
                label={job.platform}
                sx={{ mr: 1 }}
              />
              <Chip
                label={job.company_industry}
                variant="outlined"
                sx={{ mr: 1 }}
              />
            </Box>

            <Button
              variant="contained"
              color="primary"
              size="large"
              href={job.job_url}
              target="_blank"
              rel="noopener noreferrer"
              sx={{ mb: 3 }}
            >
              Apply Now
            </Button>

            <Divider sx={{ mb: 3 }} />

            <Typography variant="h5" component="h2" sx={{ mb: 2 }}>
              Job Description
            </Typography>

            <Box
              sx={{
                '& h3': {
                  fontSize: '1.2rem',
                  fontWeight: 600,
                  mb: 1,
                  mt: 3,
                  color: 'text.primary'
                },
                '& p': {
                  mb: 2
                },
                '& ul': {
                  pl: 4,
                  mb: 2
                },
                '& li': {
                  mb: 1
                }
              }}
              dangerouslySetInnerHTML={{ __html: job.job_description }}
            />
          </Paper>
        </Grid>

        <Grid item xs={12} md={4}>
          {/* Sidebar */}
          <Card sx={{ mb: 3, position: 'sticky', top: 20 }}>
            <CardContent>
              <Typography variant="h6" sx={{ mb: 2 }}>
                Job Summary
              </Typography>

              <Box sx={{ mb: 2 }}>
                <Typography variant="subtitle2" color="text.secondary">
                  Job ID
                </Typography>
                <Typography variant="body1">
                  {job.job_id}
                </Typography>
              </Box>

              <Box sx={{ mb: 2 }}>
                <Typography variant="subtitle2" color="text.secondary">
                  Company
                </Typography>
                <Typography variant="body1">
                  {job.company_name}
                </Typography>
              </Box>

              <Box sx={{ mb: 2 }}>
                <Typography variant="subtitle2" color="text.secondary">
                  Industry
                </Typography>
                <Typography variant="body1">
                  {job.company_industry}
                </Typography>
              </Box>

              <Box sx={{ mb: 2 }}>
                <Typography variant="subtitle2" color="text.secondary">
                  Location
                </Typography>
                <Typography variant="body1">
                  {job.location}
                </Typography>
              </Box>

              <Box sx={{ mb: 2 }}>
                <Typography variant="subtitle2" color="text.secondary">
                  Posted Date
                </Typography>
                <Typography variant="body1">
                  {formatDate(job.date_posted)}
                </Typography>
              </Box>

              <Box>
                <Typography variant="subtitle2" color="text.secondary">
                  Retrieved Date
                </Typography>
                <Typography variant="body1">
                  {formatDate(job.date_retrieved)}
                </Typography>
              </Box>
            </CardContent>

            <CardActions>
              <Button
                startIcon={<BookmarkBorderIcon />}
                size="small"
              >
                Save Job
              </Button>
              <Button
                startIcon={<ShareIcon />}
                size="small"
              >
                Share
              </Button>
            </CardActions>
          </Card>
        </Grid>
      </Grid>
    </Container>
  );
}

export default JobDetails;