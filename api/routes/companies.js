const express = require('express');

module.exports = (conn) => {
  const router = express.Router();

  // Get all companies with optional filtering
  router.get('/', (req, res, next) => {
    try {
      const {
        name,
        platform,
        industry,
        hasUrl = 'all', // 'all', 'yes', 'no'
        limit = 100,
        offset = 0
      } = req.query;

      // Build the WHERE clause based on provided filters
      let whereClause = [];
      let params = [];

      if (name) {
        whereClause.push("LOWER(company_name) LIKE ?");
        params.push(`%${name.toLowerCase()}%`);
      }

      if (platform) {
        whereClause.push("LOWER(platform) = ?");
        params.push(platform.toLowerCase());
      }

      if (industry) {
        whereClause.push("LOWER(company_industry) = ?");
        params.push(industry.toLowerCase());
      }

      if (hasUrl === 'yes') {
        whereClause.push("career_url IS NOT NULL AND career_url != 'Not found'");
      } else if (hasUrl === 'no') {
        whereClause.push("(career_url IS NULL OR career_url = 'Not found')");
      }

      const whereStatement = whereClause.length ? `WHERE ${whereClause.join(' AND ')}` : '';

      const query = `
        SELECT
          company_id,
          company_name,
          company_industry,
          platform,
          career_url,
          url_verified,
          last_updated
        FROM
          companies
        ${whereStatement}
        ORDER BY
          platform,
          company_name
        LIMIT ? OFFSET ?
      `;

      // Add limit and offset to params
      params.push(parseInt(limit), parseInt(offset));

      // Count total matching companies
      const countQuery = `
        SELECT COUNT(*) as total
        FROM companies
        ${whereStatement}
      `;

      // Execute count query
      conn.all(countQuery, params.slice(0, params.length - 2), (countErr, countResult) => {
        if (countErr) {
          return next(countErr);
        }

        const total = countResult[0]?.total || 0;

        // Execute main query
        conn.all(query, params, (err, rows) => {
          if (err) {
            return next(err);
          }

          res.json({
            total,
            limit: parseInt(limit),
            offset: parseInt(offset),
            data: rows || []
          });
        });
      });
    } catch (err) {
      next(err);
    }
  });

  // Get company by ID
  router.get('/:id', (req, res, next) => {
    try {
      const companyId = req.params.id;

      const query = `
        SELECT
          company_id,
          company_name,
          company_industry,
          platform,
          career_url,
          url_verified,
          last_updated
        FROM
          companies
        WHERE
          company_id = ?
      `;

      conn.get(query, [companyId], (err, row) => {
        if (err) {
          return next(err);
        }

        if (!row) {
          return res.status(404).json({ error: 'Company not found' });
        }

        // Get jobs for this company
        const jobsQuery = `
          SELECT
            job_id,
            job_title,
            location,
            job_url,
            date_posted,
            date_retrieved,
            is_active
          FROM
            jobs
          WHERE
            company_id = ?
          ORDER BY
            date_retrieved DESC
          LIMIT 50
        `;

        conn.all(jobsQuery, [companyId], (jobsErr, jobs) => {
          if (jobsErr) {
            return next(jobsErr);
          }

          // Include jobs in the response
          row.jobs = jobs || [];
          res.json(row);
        });
      });
    } catch (err) {
      next(err);
    }
  });

  // Get companies with most jobs
  router.get('/top', (req, res, next) => {
    try {
      const { limit = 10 } = req.query;

      const query = `
        SELECT
          c.company_id,
          c.company_name,
          c.company_industry,
          c.platform,
          COUNT(j.job_id) as job_count
        FROM
          companies c
        JOIN
          jobs j ON c.company_id = j.company_id
        GROUP BY
          c.company_id
        ORDER BY
          job_count DESC
        LIMIT ?
      `;

      conn.all(query, [parseInt(limit)], (err, rows) => {
        if (err) {
          return next(err);
        }

        res.json(rows || []);
      });
    } catch (err) {
      next(err);
    }
  });

  return router;
};