const express = require('express');

module.exports = (conn) => {
  const router = express.Router();

  // Get all jobs with optional filtering
  router.get('/', (req, res, next) => {
    try {
      const { keyword, location, platform, industry, limit = 50, offset = 0 } = req.query;

      // Build the WHERE clause based on provided filters
      let whereClause = [];
      let params = [];

      if (keyword) {
        whereClause.push("(LOWER(j.job_title) LIKE ? OR LOWER(j.job_description) LIKE ?)");
        const keywordPattern = `%${keyword.toLowerCase()}%`;
        params.push(keywordPattern, keywordPattern);
      }

      if (location) {
        whereClause.push("LOWER(j.location) LIKE ?");
        params.push(`%${location.toLowerCase()}%`);
      }

      if (platform) {
        whereClause.push("LOWER(c.platform) = ?");
        params.push(platform.toLowerCase());
      }

      if (industry) {
        whereClause.push("LOWER(c.company_industry) = ?");
        params.push(industry.toLowerCase());
      }

      const whereStatement = whereClause.length ? `WHERE ${whereClause.join(' AND ')}` : '';

      const query = `
        SELECT
          j.job_id,
          c.company_name,
          c.company_industry,
          c.platform,
          j.job_title,
          j.location,
          j.job_url,
          j.date_posted,
          j.date_retrieved
        FROM
          jobs j
        JOIN
          companies c ON j.company_id = c.company_id
        ${whereStatement}
        ORDER BY
          j.date_retrieved DESC,
          c.company_name
        LIMIT ? OFFSET ?
      `;

      // Add limit and offset to params
      params.push(parseInt(limit), parseInt(offset));

      // Count total matching jobs
      const countQuery = `
        SELECT COUNT(*) as total
        FROM jobs j
        JOIN companies c ON j.company_id = c.company_id
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

  // Get job by ID
  router.get('/:id', (req, res, next) => {
    try {
      const jobId = req.params.id;

      const query = `
        SELECT
          j.job_id,
          j.job_title,
          j.job_description,
          j.job_url,
          j.location,
          j.date_posted,
          j.date_retrieved,
          j.is_active,
          c.company_id,
          c.company_name,
          c.company_industry,
          c.platform,
          c.career_url
        FROM
          jobs j
        JOIN
          companies c ON j.company_id = c.company_id
        WHERE
          j.job_id = ?
      `;

      conn.get(query, [jobId], (err, row) => {
        if (err) {
          return next(err);
        }

        if (!row) {
          return res.status(404).json({ error: 'Job not found' });
        }

        res.json(row);
      });
    } catch (err) {
      next(err);
    }
  });

  // Get featured jobs
  router.get('/featured', (req, res, next) => {
    try {
      const { limit = 10 } = req.query;

      const query = `
        SELECT
          j.job_id,
          c.company_name,
          c.company_industry,
          c.platform,
          j.job_title,
          j.location,
          j.job_url,
          j.date_posted,
          j.date_retrieved
        FROM
          jobs j
        JOIN
          companies c ON j.company_id = c.company_id
        WHERE
          j.is_active = TRUE
        ORDER BY
          j.date_retrieved DESC,
          c.company_name
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