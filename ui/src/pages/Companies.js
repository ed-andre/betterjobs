import React, { useState, useEffect } from 'react';
import { Link as RouterLink } from 'react-router-dom';
import {
  Typography,
  Box,
  Container,
  Paper,
  Grid,
  TextField,
  InputAdornment,
  CircularProgress,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TablePagination,
  Button,
  Chip,
  Tooltip,
  Alert,
} from '@mui/material';
import SearchIcon from '@mui/icons-material/Search';
import LinkIcon from '@mui/icons-material/Link';
import FilterListIcon from '@mui/icons-material/FilterList';
import SyncIcon from '@mui/icons-material/Sync';
import VerifiedIcon from '@mui/icons-material/Verified';
import ErrorOutlineIcon from '@mui/icons-material/ErrorOutline';

function Companies() {
  const [companies, setCompanies] = useState([]);
  const [filteredCompanies, setFilteredCompanies] = useState([]);
  const [searchQuery, setSearchQuery] = useState('');
  const [platformFilter, setPlatformFilter] = useState('');
  const [industryFilter, setIndustryFilter] = useState('');
  const [hasUrlFilter, setHasUrlFilter] = useState(false);
  const [isLoading, setIsLoading] = useState(true);
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(10);

  // For the platform and industry filter chips
  const [platforms, setPlatforms] = useState(['workday', 'greenhouse']);
  const [industries, setIndustries] = useState(['Technology', 'Healthcare', 'Finance', 'Manufacturing', 'Retail']);

  useEffect(() => {
    fetchCompanies();
  }, []);

  const fetchCompanies = () => {
    setIsLoading(true);

    // In a real implementation, this would fetch from the API
    // For now, we'll use mock data
    setTimeout(() => {
      const mockCompanies = [];

      for (let i = 1; i <= 75; i++) {
        mockCompanies.push({
          company_id: i,
          company_name: `Company ${i}`,
          company_industry: industries[Math.floor(Math.random() * industries.length)],
          platform: platforms[Math.floor(Math.random() * platforms.length)],
          career_url: Math.random() > 0.3 ? `https://example.com/careers/${i}` : null,
          url_verified: Math.random() > 0.5,
          last_updated: new Date(Date.now() - Math.floor(Math.random() * 30) * 24 * 60 * 60 * 1000).toISOString()
        });
      }

      setCompanies(mockCompanies);
      setFilteredCompanies(mockCompanies);
      setIsLoading(false);
    }, 800);
  };

  // Filter companies based on search query and filters
  useEffect(() => {
    let filtered = companies;

    if (searchQuery) {
      filtered = filtered.filter(company =>
        company.company_name.toLowerCase().includes(searchQuery.toLowerCase())
      );
    }

    if (platformFilter) {
      filtered = filtered.filter(company =>
        company.platform === platformFilter
      );
    }

    if (industryFilter) {
      filtered = filtered.filter(company =>
        company.company_industry === industryFilter
      );
    }

    if (hasUrlFilter) {
      filtered = filtered.filter(company =>
        company.career_url !== null
      );
    }

    setFilteredCompanies(filtered);
    setPage(0);
  }, [companies, searchQuery, platformFilter, industryFilter, hasUrlFilter]);

  const handleSearchChange = (event) => {
    setSearchQuery(event.target.value);
  };

  const handlePlatformFilterClick = (platform) => {
    if (platformFilter === platform) {
      setPlatformFilter('');
    } else {
      setPlatformFilter(platform);
    }
  };

  const handleIndustryFilterClick = (industry) => {
    if (industryFilter === industry) {
      setIndustryFilter('');
    } else {
      setIndustryFilter(industry);
    }
  };

  const handleHasUrlFilterToggle = () => {
    setHasUrlFilter(!hasUrlFilter);
  };

  const handleChangePage = (event, newPage) => {
    setPage(newPage);
  };

  const handleChangeRowsPerPage = (event) => {
    setRowsPerPage(parseInt(event.target.value, 10));
    setPage(0);
  };

  // Format date for display
  const formatDate = (dateString) => {
    if (!dateString) return 'Unknown';

    const date = new Date(dateString);
    return date.toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric'
    });
  };

  return (
    <Container maxWidth="lg" sx={{ py: 4 }}>
      <Typography variant="h4" component="h1" sx={{ mb: 3 }}>
        Companies
      </Typography>

      <Paper sx={{ p: 3, mb: 4 }}>
        <Grid container spacing={2} sx={{ mb: 3 }}>
          <Grid item xs={12} md={6}>
            <TextField
              fullWidth
              label="Search Companies"
              variant="outlined"
              value={searchQuery}
              onChange={handleSearchChange}
              InputProps={{
                startAdornment: (
                  <InputAdornment position="start">
                    <SearchIcon color="action" />
                  </InputAdornment>
                ),
              }}
            />
          </Grid>
          <Grid item xs={12} md={6} sx={{ display: 'flex', alignItems: 'center' }}>
            <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1 }}>
              <Button
                variant={hasUrlFilter ? "contained" : "outlined"}
                size="small"
                onClick={handleHasUrlFilterToggle}
                startIcon={<LinkIcon />}
                sx={{ mb: 1 }}
              >
                Has URL
              </Button>

              <Button
                variant="outlined"
                size="small"
                onClick={fetchCompanies}
                startIcon={<SyncIcon />}
                sx={{ mb: 1 }}
              >
                Refresh
              </Button>
            </Box>
          </Grid>
        </Grid>

        <Box sx={{ mb: 3 }}>
          <Typography variant="subtitle1" sx={{ mb: 1 }}>
            <FilterListIcon fontSize="small" sx={{ verticalAlign: 'middle', mr: 0.5 }} />
            Filters:
          </Typography>

          <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1 }}>
            {platforms.map(platform => (
              <Chip
                key={platform}
                label={platform}
                clickable
                color={platformFilter === platform ? "primary" : "default"}
                onClick={() => handlePlatformFilterClick(platform)}
              />
            ))}
          </Box>

          <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1, mt: 1 }}>
            {industries.map(industry => (
              <Chip
                key={industry}
                label={industry}
                variant="outlined"
                clickable
                color={industryFilter === industry ? "primary" : "default"}
                onClick={() => handleIndustryFilterClick(industry)}
              />
            ))}
          </Box>
        </Box>

        {isLoading ? (
          <Box sx={{ display: 'flex', justifyContent: 'center', my: 4 }}>
            <CircularProgress />
          </Box>
        ) : (
          <>
            {filteredCompanies.length === 0 ? (
              <Alert severity="info">
                No companies found matching your criteria. Try adjusting your search or filters.
              </Alert>
            ) : (
              <>
                <TableContainer>
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Company Name</TableCell>
                        <TableCell>Industry</TableCell>
                        <TableCell>Platform</TableCell>
                        <TableCell>URL Status</TableCell>
                        <TableCell>Last Updated</TableCell>
                        <TableCell>Actions</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {filteredCompanies
                        .slice(page * rowsPerPage, page * rowsPerPage + rowsPerPage)
                        .map((company) => (
                          <TableRow key={company.company_id}>
                            <TableCell>{company.company_name}</TableCell>
                            <TableCell>
                              {company.company_industry}
                            </TableCell>
                            <TableCell>
                              <Chip
                                label={company.platform}
                                size="small"
                              />
                            </TableCell>
                            <TableCell>
                              {company.career_url ? (
                                <Tooltip title={company.url_verified ? "URL verified" : "URL not verified"}>
                                  <Box sx={{ display: 'flex', alignItems: 'center' }}>
                                    {company.url_verified ? (
                                      <VerifiedIcon fontSize="small" color="success" sx={{ mr: 1 }} />
                                    ) : (
                                      <ErrorOutlineIcon fontSize="small" color="warning" sx={{ mr: 1 }} />
                                    )}
                                    <Typography variant="body2" component="span">
                                      {company.url_verified ? "Verified" : "Unverified"}
                                    </Typography>
                                  </Box>
                                </Tooltip>
                              ) : (
                                <Typography variant="body2" color="text.secondary">
                                  No URL
                                </Typography>
                              )}
                            </TableCell>
                            <TableCell>
                              {formatDate(company.last_updated)}
                            </TableCell>
                            <TableCell>
                              {company.career_url && (
                                <Button
                                  variant="outlined"
                                  size="small"
                                  href={company.career_url}
                                  target="_blank"
                                  rel="noopener noreferrer"
                                  startIcon={<LinkIcon />}
                                >
                                  Career Site
                                </Button>
                              )}
                            </TableCell>
                          </TableRow>
                        ))}
                    </TableBody>
                  </Table>
                </TableContainer>

                <TablePagination
                  rowsPerPageOptions={[10, 25, 50, 100]}
                  component="div"
                  count={filteredCompanies.length}
                  rowsPerPage={rowsPerPage}
                  page={page}
                  onPageChange={handleChangePage}
                  onRowsPerPageChange={handleChangeRowsPerPage}
                />
              </>
            )}
          </>
        )}
      </Paper>
    </Container>
  );
}

export default Companies;