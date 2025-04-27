const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const morgan = require('morgan');
const duckdb = require('duckdb');
const path = require('path');
require('dotenv').config();

// Initialize express app
const app = express();
const PORT = process.env.PORT || 3001;

// Middleware
app.use(cors());
app.use(helmet());
app.use(morgan('dev'));
app.use(express.json());

// Database connection
const DB_PATH = process.env.DB_PATH || path.join(__dirname, '../pipeline/dagster_betterjobs/dagster_betterjobs/db/betterjobs.db');
const db = new duckdb.Database(DB_PATH);
const conn = db.connect();

// Health check endpoint
app.get('/api/health', (req, res) => {
  res.status(200).json({ status: 'ok', message: 'API server is running' });
});

// API Routes
const jobsRoutes = require('./routes/jobs');
const companiesRoutes = require('./routes/companies');
const statsRoutes = require('./routes/stats');
const metadataRoutes = require('./routes/metadata');

app.use('/api/jobs', jobsRoutes(conn));
app.use('/api/companies', companiesRoutes(conn));
app.use('/api/stats', statsRoutes(conn));
app.use('/api/metadata', metadataRoutes(conn));

// Error handling middleware
app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).json({
    error: 'Something went wrong!',
    message: process.env.NODE_ENV === 'development' ? err.message : 'Internal Server Error'
  });
});

// Start the server
app.listen(PORT, () => {
  console.log(`BetterJobs API server running on port ${PORT}`);
});

// Clean up database connection on exit
process.on('exit', () => {
  console.log('Closing database connection');
  conn.close();
  db.close();
});

process.on('SIGINT', () => {
  process.exit();
});

module.exports = app;