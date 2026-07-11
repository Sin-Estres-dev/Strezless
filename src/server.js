const express = require('express');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const app = express();
app.use(express.json());
app.use(express.static("public"));

const DATA_DIR = path.join(__dirname, '../data');
const DATA_FILE = path.join(DATA_DIR, 'tracks.json');

if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
if (!fs.existsSync(DATA_FILE)) fs.writeFileSync(DATA_FILE, '[]');

const getTracks = () => JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
const saveTracks = (tracks) => fs.writeFileSync(DATA_FILE, JSON.stringify(tracks, null, 2));

// ROUTE 1: Add a new track
app.post('/api/tracks', (req, res) => {
    const tracks = getTracks();
    const newTrack = {
        id: crypto.randomUUID(),
        ...req.body,
        createdAt: new Date().toISOString()
    };
    tracks.push(newTrack);
    saveTracks(tracks);
    res.status(201).json({ message: "Track metadata saved successfully!", track: newTrack });
});

// ROUTE 2: View all tracks
app.get('/api/tracks', (req, res) => {
    res.json(getTracks());
});

// ROUTE 3: EXPORT TO CSV (Updated with Label & Publishing IDs)
app.get('/api/export', (req, res) => {
    const tracks = getTracks();
    if (tracks.length === 0) return res.status(404).send("No tracks to export.");

    const headers = [
        'Track Title', 'Artist Name', 'Album Title', 'Release Date', 
        'Genre', 'Language', 'ISRC', 'ISWC', 'Work ID',
        'Label Name', 'Label ISNI', 'Label IPI', 'Label EIN',
        'Songwriters', 'PRO Affiliation', 'BPM', 'Key', 'Is Explicit', 
        'Sound Recording Copyright (P)', 'Composition Copyright (C)'
    ];

    const escapeCsv = (val) => {
        if (val === undefined || val === null) return '';
        const str = String(val);
        if (str.includes(',') || str.includes('"') || str.includes('\n')) {
            return '"' + str.replace(/"/g, '""') + '"';
        }
        return str;
    };

    const rows = tracks.map(track => {
        const songwriters = Array.isArray(track.songwriters) ? track.songwriters.join('; ') : (track.songwriters || '');
        return [
            escapeCsv(track.trackTitle), escapeCsv(track.artistName), escapeCsv(track.albumTitle),
            escapeCsv(track.releaseDate), escapeCsv(track.genre), escapeCsv(track.language),
            escapeCsv(track.isrc), escapeCsv(track.iswc), escapeCsv(track.workId),
            escapeCsv(track.labelName), escapeCsv(track.labelISNI), escapeCsv(track.labelIPI), escapeCsv(track.labelEIN),
            escapeCsv(songwriters), escapeCsv(track.proAffiliation),
            escapeCsv(track.bpm), escapeCsv(track.key), escapeCsv(track.isExplicit),
            escapeCsv(track.soundRecordingCopyrightOwner), escapeCsv(track.compositionCopyrightOwner)
        ].join(',');
    });

    const csvContent = [headers.join(','), ...rows].join('\n');

    res.header('Content-Type', 'text/csv');
    res.header('Content-Disposition', 'attachment; filename=strezless_metadata.csv');
    res.send(csvContent);
});

const PORT = 3000;
app.listen(PORT, () => {
    console.log(`🎵 Strezless API running on http://localhost:${PORT}`);
});
