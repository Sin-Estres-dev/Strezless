const express = require('express');
const bodyParser = require('body-parser');
require('dotenv').config({ path: '.env.local' });

const app = express();

// Middleware
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

// CORS (for development - configure properly for production)
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Authorization');
  res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, PATCH, OPTIONS');
  next();
});

// Routes
const artistProfileRoutes = require('./routes/artistProfile');
app.use('/api/artist', artistProfileRoutes);

// Default route
app.get('/', (req, res) => {
  res.json({
    message: 'Welcome to Strezless API 🎵',
    version: '1.0.0',
    endpoints: {
      artist: {
        createProfile: 'POST /api/artist/profile',
        getProfile: 'GET /api/artist/profile/:id',
        updateProfile: 'PUT /api/artist/profile/:id',
        updateIdentifiers: 'PUT /api/artist/identifiers/:artistId',
        search: 'GET /api/artist/search'
      }
    }
  });
});

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'healthy', timestamp: new Date().toISOString() });
});

// Error handling middleware
app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).json({
    success: false,
    message: 'Something went wrong!',
    error: process.env.NODE_ENV === 'development' ? err.message : undefined
  });
});

// Set up the port
const PORT = process.env.PORT || 3000;

// Start the server
app.listen(PORT, () => {
  console.log(`🎵 Strezless API running at http://localhost:${PORT}`);
  console.log(`📊 Environment: ${process.env.NODE_ENV || 'development'}`);
});

module.exports = app;
