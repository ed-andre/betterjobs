import axios from 'axios';

// Base URL for API calls
const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || 'http://localhost:3001/api';

// Create an axios instance with default config
const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Jobs API
export const jobsApi = {
  // Search for jobs
  searchJobs: async (params) => {
    try {
      const response = await api.get('/jobs', { params });
      return response.data;
    } catch (error) {
      console.error('Error searching jobs:', error);
      throw error;
    }
  },

  // Get job details by ID
  getJobById: async (jobId) => {
    try {
      const response = await api.get(`/jobs/${jobId}`);
      return response.data;
    } catch (error) {
      console.error(`Error fetching job ${jobId}:`, error);
      throw error;
    }
  },

  // Get featured jobs
  getFeaturedJobs: async () => {
    try {
      const response = await api.get('/jobs/featured');
      return response.data;
    } catch (error) {
      console.error('Error fetching featured jobs:', error);
      throw error;
    }
  },
};

// Companies API
export const companiesApi = {
  // Get all companies
  getCompanies: async (params) => {
    try {
      const response = await api.get('/companies', { params });
      return response.data;
    } catch (error) {
      console.error('Error fetching companies:', error);
      throw error;
    }
  },

  // Get company by ID
  getCompanyById: async (companyId) => {
    try {
      const response = await api.get(`/companies/${companyId}`);
      return response.data;
    } catch (error) {
      console.error(`Error fetching company ${companyId}:`, error);
      throw error;
    }
  },

  // Get companies with most jobs
  getTopCompanies: async () => {
    try {
      const response = await api.get('/companies/top');
      return response.data;
    } catch (error) {
      console.error('Error fetching top companies:', error);
      throw error;
    }
  },
};

// Stats API
export const statsApi = {
  // Get general stats
  getStats: async (timeRange = '30days') => {
    try {
      const response = await api.get('/stats', { params: { timeRange } });
      return response.data;
    } catch (error) {
      console.error('Error fetching stats:', error);
      throw error;
    }
  },

  // Get platform stats
  getPlatformStats: async () => {
    try {
      const response = await api.get('/stats/platforms');
      return response.data;
    } catch (error) {
      console.error('Error fetching platform stats:', error);
      throw error;
    }
  },

  // Get trending keywords
  getTrendingKeywords: async () => {
    try {
      const response = await api.get('/stats/trending/keywords');
      return response.data;
    } catch (error) {
      console.error('Error fetching trending keywords:', error);
      throw error;
    }
  },
};

// Metadata API
export const metadataApi = {
  // Get available platforms
  getPlatforms: async () => {
    try {
      const response = await api.get('/metadata/platforms');
      return response.data;
    } catch (error) {
      console.error('Error fetching platforms:', error);
      throw error;
    }
  },

  // Get available industries
  getIndustries: async () => {
    try {
      const response = await api.get('/metadata/industries');
      return response.data;
    } catch (error) {
      console.error('Error fetching industries:', error);
      throw error;
    }
  },

  // Get available locations
  getLocations: async () => {
    try {
      const response = await api.get('/metadata/locations');
      return response.data;
    } catch (error) {
      console.error('Error fetching locations:', error);
      throw error;
    }
  },
};

export default {
  jobsApi,
  companiesApi,
  statsApi,
  metadataApi,
};