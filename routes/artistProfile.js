const express = require('express');
const router = express.Router();
const { Pool } = require('pg');

// Database connection pool
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
});

// ==========================================
// ARTIST PROFILE ROUTES
// ==========================================

/**
 * POST /api/artist/profile
 * Create a new artist profile
 */
router.post('/profile', async (req, res) => {
  const client = await pool.connect();
  
  try {
    await client.query('BEGIN');
    
    const {
      artist_name,
      legal_name,
      email,
      phone,
      bio,
      career_level,
      genres,
      website_url,
      social_links,
      // Identifiers
      ipi_name_number,
      ipi_base_number,
      isni,
      ein
    } = req.body;
    
    // Insert artist
    const artistResult = await client.query(
      `INSERT INTO artists (
        artist_name, legal_name, email, phone, bio, 
        career_level, genres, website_url, social_links
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
      RETURNING *`,
      [artist_name, legal_name, email, phone, bio, career_level, genres, website_url, social_links]
    );
    
    const artist = artistResult.rows[0];
    
    // Insert identifiers if provided
    if (ipi_name_number || ipi_base_number || isni || ein) {
      await client.query(
        `INSERT INTO artist_identifiers (
          artist_id, ipi_name_number, ipi_base_number, isni, ein
        ) VALUES ($1, $2, $3, $4, $5)`,
        [artist.id, ipi_name_number, ipi_base_number, isni, ein]
      );
    }
    
    await client.query('COMMIT');
    
    res.status(201).json({
      success: true,
      data: artist,
      message: 'Artist profile created successfully'
    });
    
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('Error creating artist profile:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to create artist profile',
      error: error.message
    });
  } finally {
    client.release();
  }
});

/**
 * GET /api/artist/profile/:id
 * Get full artist profile with all identifiers
 */
router.get('/profile/:id', async (req, res) => {
  try {
    const { id } = req.params;
    
    const result = await pool.query(
      `SELECT 
        a.*,
        ai.ipi_name_number,
        ai.ipi_base_number,
        ai.isni,
        ai.ein,
        ai.verified_at as identifiers_verified_at,
        COUNT(DISTINCT r.id) as total_releases,
        COUNT(DISTINCT t.id) as total_tracks,
        COUNT(DISTINCT af.fan_id) as total_fans
      FROM artists a
      LEFT JOIN artist_identifiers ai ON a.id = ai.artist_id
      LEFT JOIN releases r ON a.id = r.artist_id
      LEFT JOIN tracks t ON r.id = t.release_id
      LEFT JOIN artist_fans af ON a.id = af.artist_id
      WHERE a.id = $1
      GROUP BY a.id, ai.ipi_name_number, ai.ipi_base_number, ai.isni, ai.ein, ai.verified_at`,
      [id]
    );
    
    if (result.rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'Artist not found'
      });
    }
    
    res.json({
      success: true,
      data: result.rows[0]
    });
    
  } catch (error) {
    console.error('Error fetching artist profile:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch artist profile',
      error: error.message
    });
  }
});

/**
 * PUT /api/artist/profile/:id
 * Update artist profile
 */
router.put('/profile/:id', async (req, res) => {
  const client = await pool.connect();
  
  try {
    await client.query('BEGIN');
    
    const { id } = req.params;
    const {
      artist_name,
      legal_name,
      phone,
      bio,
      career_level,
      genres,
      website_url,
      social_links,
      profile_image_url
    } = req.body;
    
    const result = await client.query(
      `UPDATE artists SET
        artist_name = COALESCE($1, artist_name),
        legal_name = COALESCE($2, legal_name),
        phone = COALESCE($3, phone),
        bio = COALESCE($4, bio),
        career_level = COALESCE($5, career_level),
        genres = COALESCE($6, genres),
        website_url = COALESCE($7, website_url),
        social_links = COALESCE($8, social_links),
        profile_image_url = COALESCE($9, profile_image_url),
        updated_at = CURRENT_TIMESTAMP
      WHERE id = $10
      RETURNING *`,
      [artist_name, legal_name, phone, bio, career_level, genres, website_url, social_links, profile_image_url, id]
    );
    
    if (result.rows.length === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({
        success: false,
        message: 'Artist not found'
      });
    }
    
    await client.query('COMMIT');
    
    res.json({
      success: true,
      data: result.rows[0],
      message: 'Artist profile updated successfully'
    });
    
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('Error updating artist profile:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to update artist profile',
      error: error.message
    });
  } finally {
    client.release();
  }
});

/**
 * PUT /api/artist/identifiers/:artistId
 * Update or add industry identifiers
 */
router.put('/identifiers/:artistId', async (req, res) => {
  try {
    const { artistId } = req.params;
    const { ipi_name_number, ipi_base_number, isni, ein, tax_id, business_entity } = req.body;
    
    const result = await pool.query(
      `INSERT INTO artist_identifiers (
        artist_id, ipi_name_number, ipi_base_number, isni, ein, tax_id, business_entity
      ) VALUES ($1, $2, $3, $4, $5, $6, $7)
      ON CONFLICT (artist_id)
      DO UPDATE SET
        ipi_name_number = COALESCE($2, artist_identifiers.ipi_name_number),
        ipi_base_number = COALESCE($3, artist_identifiers.ipi_base_number),
        isni = COALESCE($4, artist_identifiers.isni),
        ein = COALESCE($5, artist_identifiers.ein),
        tax_id = COALESCE($6, artist_identifiers.tax_id),
        business_entity = COALESCE($7, artist_identifiers.business_entity),
        updated_at = CURRENT_TIMESTAMP
      RETURNING *`,
      [artistId, ipi_name_number, ipi_base_number, isni, ein, tax_id, business_entity]
    );
    
    res.json({
      success: true,
      data: result.rows[0],
      message: 'Identifiers updated successfully'
    });
    
  } catch (error) {
    console.error('Error updating identifiers:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to update identifiers',
      error: error.message
    });
  }
});

/**
 * GET /api/artist/search
 * Search for artists by various identifiers
 */
router.get('/search', async (req, res) => {
  try {
    const { email, ipi, isni, artist_name } = req.query;
    
    let query = `
      SELECT a.*, ai.ipi_name_number, ai.isni, ai.ein
      FROM artists a
      LEFT JOIN artist_identifiers ai ON a.id = ai.artist_id
      WHERE 1=1
    `;
    const params = [];
    let paramCount = 1;
    
    if (email) {
      query += ` AND a.email = $${paramCount}`;
      params.push(email);
      paramCount++;
    }
    
    if (ipi) {
      query += ` AND ai.ipi_name_number = $${paramCount}`;
      params.push(ipi);
      paramCount++;
    }
    
    if (isni) {
      query += ` AND ai.isni = $${paramCount}`;
      params.push(isni);
      paramCount++;
    }
    
    if (artist_name) {
      query += ` AND a.artist_name ILIKE $${paramCount}`;
      params.push(`%${artist_name}%`);
      paramCount++;
    }
    
    const result = await pool.query(query, params);
    
    res.json({
      success: true,
      data: result.rows,
      count: result.rows.length
    });
    
  } catch (error) {
    console.error('Error searching artists:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to search artists',
      error: error.message
    });
  }
});

module.exports = router;
