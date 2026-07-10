#!/usr/bin/env node

require('dotenv').config();
const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const helmet = require('helmet');
const path = require('path');
const fs = require('fs');

// Import routes
const artistRoutes = require('./routes/artists');
const splitSheetRoutes = require('./routes/splitsheets');
const distributionRoutes = require('./routes/distribution');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(helmet());
app.use(cors());
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, '../public')));

// View engine
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, '../views'));

// Routes
app.use('/api/artists', artistRoutes);
app.use('/api/split-sheets', splitSheetRoutes);
app.use('/api/distribution', distributionRoutes);

// Home page
app.get('/', (req, res) => {
  res.render('index', { 
    title: 'Strezless Music Metadata Suite',
    version: '1.0.0'
  });
});

// Dashboard
app.get('/dashboard', (req, res) => {
  res.render('dashboard', { title: 'Dashboard' });
});

// Error handling
app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).json({ error: err.message });
});

// 404
app.use((req, res) => {
  res.status(404).json({ error: 'Route not found' });
});

app.listen(PORT, () => {
  console.log('\n🎵 Strezless Music Metadata Suite');
  console.log(`✓ Server running on http://localhost:${PORT}`);
  console.log(`✓ Dashboard: http://localhost:${PORT}/dashboard\n`);
});

module.exports = app;
