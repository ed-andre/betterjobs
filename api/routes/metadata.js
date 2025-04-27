const express = require('express');

module.exports = (conn) => {
  const router = express.Router();

  // Get available platforms
  router.get('/platforms', (req, res, next) => {
    try {
      const query = `
        SELECT DISTINCT platform
        FROM companies
        WHERE platform IS NOT NULL AND platform != ''
        ORDER BY platform
      `;

      conn.all(query, [], (err, rows) => {
        if (err) {
          return next(err);
        }

        // Extract just the platform names
        const platforms = rows.map(row => row.platform);
        res.json(platforms);
      });
    } catch (err) {
      next(err);
    }
  });

  // Get available industries
  router.get('/industries', (req, res, next) => {
    try {
      const query = `
        SELECT DISTINCT company_industry as industry
        FROM companies
        WHERE company_industry IS NOT NULL AND company_industry != ''
        ORDER BY company_industry
      `;

      conn.all(query, [], (err, rows) => {
        if (err) {
          return next(err);
        }

        // Extract just the industry names
        const industries = rows.map(row => row.industry);
        res.json(industries);
      });
    } catch (err) {
      next(err);
    }
  });

  // Get available locations
  router.get('/locations', (req, res, next) => {
    try {
      const { limit = 100 } = req.query;

      const query = `
        SELECT location, COUNT(*) as count
        FROM jobs
        WHERE location IS NOT NULL AND location != ''
        GROUP BY location
        ORDER BY count DESC
        LIMIT ?
      `;

      conn.all(query, [parseInt(limit)], (err, rows) => {
        if (err) {
          return next(err);
        }

        // Extract location names
        const locations = rows.map(row => row.location);
        res.json(locations);
      });
    } catch (err) {
      next(err);
    }
  });

  // Get database statistics
  router.get('/database', (req, res, next) => {
    try {
      // Last updated timestamp
      const lastUpdatedQuery = `
        SELECT MAX(date_retrieved) as last_updated
        FROM jobs
      `;

      // Database size (this is a simplification, in SQLite/DuckDB this might need a different approach)
      const companiesCountQuery = `
        SELECT COUNT(*) as companies_count
        FROM companies
      `;

      const jobsCountQuery = `
        SELECT COUNT(*) as jobs_count
        FROM jobs
      `;

      // Execute queries
      conn.get(lastUpdatedQuery, [], (lastUpdatedErr, lastUpdatedResult) => {
        if (lastUpdatedErr) {
          return next(lastUpdatedErr);
        }

        conn.get(companiesCountQuery, [], (companiesErr, companiesResult) => {
          if (companiesErr) {
            return next(companiesErr);
          }

          conn.get(jobsCountQuery, [], (jobsErr, jobsResult) => {
            if (jobsErr) {
              return next(jobsErr);
            }

            // Combine results
            const stats = {
              lastUpdated: lastUpdatedResult?.last_updated || null,
              counts: {
                companies: companiesResult?.companies_count || 0,
                jobs: jobsResult?.jobs_count || 0
              }
            };

            res.json(stats);
          });
        });
      });
    } catch (err) {
      next(err);
    }
  });

  return router;
};