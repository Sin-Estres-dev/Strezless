const express = require('express');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const app = express();
app.use(express.json()); // Allows us to read JSON data sent to the API
app.use(express.static("public"));

// Path to our local database file
const DATA_DIR = path.join(__dirname, '../data');
const DATA_FILE = path.join(DATA_DIR, 'tracks.json');

// Create data folder and file if they don't exist
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
if (!fs.existsSync(DATA_FILE)) fs.writeFileSync(DATA_FILE, '[]');

// Helper functions to read and write data
const getTracks = () => JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
const saveTracks = (tracks) => fs.writeFileSync(DATA_FILE, JSON.stringify(tracks, null, 2));

// ROUTE 1: Add a new track with all metadata fields
app.post('/api/tracks', (req, res) => {
    const tracks = getTracks();
    
    // Create the new track object with a unique ID and timestamp
    const newTrack = {
        id: crypto.randomUUID(),
        ...req.body, // This captures all the metadata fields sent in the request
        createdAt: new Date().toISOString()
    };
    
    tracks.push(newTrack);
    saveTracks(tracks);
    
    res.status(201).json({ message: "Track metadata saved successfully!", track: newTrack });
});

// ROUTE 2: View all tracks
app.get('/api/tracks', (req, res) => {
    const tracks = getTracks();
    res.json(tracks);
});

// Start the server
const PORT = 3000;
app.listen(PORT, () => {
    console.log(`🎵 Strezless Music Metadata API is running on http://localhost:${PORT}`);
});
