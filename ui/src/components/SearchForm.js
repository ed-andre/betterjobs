import React, { useState } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import {
  Paper,
  TextField,
  Button,
  Box,
  Grid,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Autocomplete,
} from '@mui/material';
import SearchIcon from '@mui/icons-material/Search';

function SearchForm({ onSearch, platforms = [], industries = [] }) {
  const navigate = useNavigate();
  const location = useLocation();
  const searchParams = new URLSearchParams(location.search);

  // Initialize state from URL parameters
  const [keyword, setKeyword] = useState(searchParams.get('keyword') || '');
  const [jobLocation, setJobLocation] = useState(searchParams.get('location') || '');
  const [platform, setPlatform] = useState(searchParams.get('platform') || '');
  const [industry, setIndustry] = useState(searchParams.get('industry') || '');

  const handleSubmit = (e) => {
    e.preventDefault();

    // Update URL with search parameters
    const params = new URLSearchParams();
    if (keyword) params.set('keyword', keyword);
    if (jobLocation) params.set('location', jobLocation);
    if (platform) params.set('platform', platform);
    if (industry) params.set('industry', industry);

    navigate({
      pathname: '/search',
      search: params.toString()
    });

    // Call the onSearch callback with search criteria
    if (onSearch) {
      onSearch({
        keyword,
        location: jobLocation,
        platform,
        industry
      });
    }
  };

  return (
    <Paper
      elevation={3}
      component="form"
      onSubmit={handleSubmit}
      sx={{
        p: 3,
        mb: 4,
        borderRadius: 2,
        backgroundColor: 'white'
      }}
    >
      <Grid container spacing={2}>
        <Grid item xs={12} md={4}>
          <TextField
            fullWidth
            label="Job Title or Keyword"
            variant="outlined"
            value={keyword}
            onChange={(e) => setKeyword(e.target.value)}
            placeholder="e.g. Developer, Designer, Manager"
            InputProps={{
              startAdornment: <SearchIcon color="action" sx={{ mr: 1 }} />,
            }}
          />
        </Grid>

        <Grid item xs={12} md={3}>
          <TextField
            fullWidth
            label="Location"
            variant="outlined"
            value={jobLocation}
            onChange={(e) => setJobLocation(e.target.value)}
            placeholder="e.g. New York, Remote"
          />
        </Grid>

        <Grid item xs={12} sm={6} md={2}>
          <FormControl fullWidth variant="outlined">
            <InputLabel>Platform</InputLabel>
            <Select
              value={platform}
              onChange={(e) => setPlatform(e.target.value)}
              label="Platform"
            >
              <MenuItem value="">
                <em>All Platforms</em>
              </MenuItem>
              {platforms.map((p) => (
                <MenuItem key={p} value={p}>
                  {p}
                </MenuItem>
              ))}
            </Select>
          </FormControl>
        </Grid>

        <Grid item xs={12} sm={6} md={2}>
          <FormControl fullWidth variant="outlined">
            <InputLabel>Industry</InputLabel>
            <Select
              value={industry}
              onChange={(e) => setIndustry(e.target.value)}
              label="Industry"
            >
              <MenuItem value="">
                <em>All Industries</em>
              </MenuItem>
              {industries.map((ind) => (
                <MenuItem key={ind} value={ind}>
                  {ind}
                </MenuItem>
              ))}
            </Select>
          </FormControl>
        </Grid>

        <Grid item xs={12} md={1} sx={{ display: 'flex', alignItems: 'center' }}>
          <Button
            fullWidth
            type="submit"
            variant="contained"
            color="primary"
            size="large"
            sx={{ height: '56px' }}
          >
            Search
          </Button>
        </Grid>
      </Grid>
    </Paper>
  );
}

export default SearchForm;