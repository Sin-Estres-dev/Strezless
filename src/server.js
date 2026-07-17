const express = require('express');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
require('dotenv').config();

const app = express();
app.use(express.json());
app.use(express.static("public"));

const DATA_DIR = path.join(__dirname, '../data');
const TRACKS_FILE = path.join(DATA_DIR, 'tracks.json');
const CONTRIBUTORS_FILE = path.join(DATA_DIR, 'contributors.json');
const SPLITS_FILE = path.join(DATA_DIR, 'splits.json');

if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
if (!fs.existsSync(TRACKS_FILE)) fs.writeFileSync(TRACKS_FILE, '[]');
if (!fs.existsSync(CONTRIBUTORS_FILE)) fs.writeFileSync(CONTRIBUTORS_FILE, '[]');
if (!fs.existsSync(SPLITS_FILE)) fs.writeFileSync(SPLITS_FILE, '[]');

const getTracks = () => JSON.parse(fs.readFileSync(TRACKS_FILE, 'utf8'));
const saveTracks = (tracks) => fs.writeFileSync(TRACKS_FILE, JSON.stringify(tracks, null, 2));
const getContributors = () => JSON.parse(fs.readFileSync(CONTRIBUTORS_FILE, 'utf8'));
const saveContributors = (c) => fs.writeFileSync(CONTRIBUTORS_FILE, JSON.stringify(c, null, 2));
const getSplits = () => JSON.parse(fs.readFileSync(SPLITS_FILE, 'utf8'));
const saveSplits = (s) => fs.writeFileSync(SPLITS_FILE, JSON.stringify(s, null, 2));

// --- VALIDATION ---
function validateISRC(isrc) { return /^[A-Z]{2}[A-Z0-9]{3}\d{7}$/.test(isrc); }
function validateISWC(iswc) { return /^T-\d{9}-\d$/.test(iswc); }
function validateUPC(upc) { return /^\d{12,13}$/.test(upc); }

function validateMetadata(track) {
    const errors = [];
    if (!track.trackTitle || track.trackTitle.trim().length === 0) errors.push("Track title is required");
    if (!track.artistName || track.artistName.trim().length === 0) errors.push("Artist name is required");
    if (track.isrc && !validateISRC(track.isrc)) errors.push("Invalid ISRC format. Should be: CC-XXX-YY-NNNNN");
    if (track.iswc && !validateISWC(track.iswc)) errors.push("Invalid ISWC format. Should be: T-NNNNNNNNN-C");
    if (track.upc && !validateUPC(track.upc)) errors.push("Invalid UPC format. Should be 12 or 13 digits");
    return errors;
}

// --- GRAVATAR ---
app.get('/api/gravatar/:email', (req, res) => {
    const email = req.params.email.toLowerCase().trim();
    const hash = crypto.createHash('md5').update(email).digest('hex');
    res.json({ success: true, hash, avatarUrl: `https://www.gravatar.com/avatar/${hash}?d=identicon&s=200` });
});

// --- TRACKS CRUD ---
app.post('/api/tracks', (req, res) => {
    const tracks = getTracks();
    const errors = validateMetadata(req.body);
    if (errors.length > 0) return res.status(400).json({ errors });
    
    if (req.body.isrc && tracks.find(t => t.isrc === req.body.isrc)) {
        return res.status(400).json({ errors: ["ISRC already exists in catalog."] });
    }
    
    const newTrack = { id: crypto.randomUUID(), ...req.body, createdAt: new Date().toISOString(), version: 1, changeLog: [] };
    tracks.push(newTrack);
    saveTracks(tracks);
    res.status(201).json({ message: "Track saved!", track: newTrack });
});

app.get('/api/tracks', (req, res) => { res.json(getTracks()); });

app.get('/api/tracks/:id', (req, res) => {
    const track = getTracks().find(t => t.id === req.params.id);
    if (!track) return res.status(404).send("Track not found.");
    res.json(track);
});

app.put('/api/tracks/:id', (req, res) => {
    const tracks = getTracks();
    const index = tracks.findIndex(t => t.id === req.params.id);
    if (index === -1) return res.status(404).send("Track not found.");
    
    const errors = validateMetadata(req.body);
    if (errors.length > 0) return res.status(400).json({ errors });
    
    const oldTrack = tracks[index];
    tracks[index] = { ...oldTrack, ...req.body, updatedAt: new Date().toISOString(), version: (oldTrack.version || 1) + 1, changeLog: [...(oldTrack.changeLog || []), { timestamp: new Date().toISOString(), changes: req.body }] };
    saveTracks(tracks);
    res.json({ message: "Track updated!", track: tracks[index] });
});

app.delete('/api/tracks/:id', (req, res) => {
    let tracks = getTracks().filter(t => t.id !== req.params.id);
    saveTracks(tracks);
    res.json({ message: "Track deleted successfully!" });
});

// --- CONTRIBUTORS & SPLITS ---
app.post('/api/contributors', (req, res) => {
    const contributors = getContributors();
    const newC = { id: crypto.randomUUID(), ...req.body, createdAt: new Date().toISOString() };
    contributors.push(newC); saveContributors(contributors);
    res.status(201).json({ message: "Contributor added!", contributor: newC });
});
app.get('/api/contributors', (req, res) => { res.json(getContributors()); });
app.delete('/api/contributors/:id', (req, res) => { saveContributors(getContributors().filter(c => c.id !== req.params.id)); res.json({ message: "Deleted!" }); });

app.post('/api/splits', (req, res) => {
    const splits = getSplits();
    const totalSplit = req.body.splits.reduce((sum, s) => sum + parseFloat(s.percentage), 0);
    if (Math.abs(totalSplit - 100) > 0.01) return res.status(400).json({ error: "Split percentages must add up to 100%" });
    const newS = { id: crypto.randomUUID(), trackId: req.body.trackId, splits: req.body.splits, createdAt: new Date().toISOString() };
    splits.push(newS); saveSplits(splits);
    res.status(201).json({ message: "Split sheet created!", split: newS });
});
app.get('/api/splits/:trackId', (req, res) => { res.json(getSplits().filter(s => s.trackId === req.params.trackId)); });

// --- MERGED CSV EXPORT ---
app.get('/api/export', (req, res) => {
    const tracks = getTracks();
    if (tracks.length === 0) return res.status(404).send("No tracks to export.");
    
    const headers = [
        'Track Title', 'Artist Name', 'Album Title', 'Release Date', 'Genre', 'Language',
        'ISRC', 'ISWC', 'UPC', 'IPI', 'IPN', 'PRO Affiliation', 'BPM', 'Key', 'Explicit', 'AI Disclosure',
        'Label Name', 'Label ISNI', 'Label IPI', 'Label EIN', 'Songwriters', 
        'Master Owner', 'Publishing Owner', 'Sound Recording Copyright (P)', 'Composition Copyright (C)',
        'License Type', 'Usage Terms / Restrictions', 'Licensing Contact', 'Cover Art URL'
    ];
    
    const escapeCsv = (val) => {
        if (!val) return '';
        const str = String(val);
        if (str.includes(',') || str.includes('"') || str.includes('\n')) return '"' + str.replace(/"/g, '""') + '"';
        return str;
    };
    
    const rows = tracks.map(t => {
        const songwriters = Array.isArray(t.songwriters) ? t.songwriters.join('; ') : (t.songwriters || '');
        return [
            escapeCsv(t.trackTitle), escapeCsv(t.artistName), escapeCsv(t.albumTitle), escapeCsv(t.releaseDate), escapeCsv(t.genre), escapeCsv(t.language),
            escapeCsv(t.isrc), escapeCsv(t.iswc), escapeCsv(t.upc), escapeCsv(t.ipi), escapeCsv(t.ipn), escapeCsv(t.proAffiliation),
            escapeCsv(t.bpm), escapeCsv(t.key), escapeCsv(t.explicit), escapeCsv(t.aiDisclosure),
            escapeCsv(t.labelName), escapeCsv(t.labelISNI), escapeCsv(t.labelIPI), escapeCsv(t.labelEIN), escapeCsv(songwriters),
            escapeCsv(t.masterOwner), escapeCsv(t.publishingOwner), escapeCsv(t.soundRecordingCopyrightOwner), escapeCsv(t.compositionCopyrightOwner),
            escapeCsv(t.licenseType), escapeCsv(t.usageTerms), escapeCsv(t.licensingContact), escapeCsv(t.coverArtUrl)
        ].join(',');
    });
    
    res.header('Content-Type', 'text/csv').header('Content-Disposition', 'attachment; filename=strezless_nexus_metadata.csv');
    res.send([headers.join(','), ...rows].join('\n'));
});

const PORT = 3000;
app.listen(PORT, () => { console.log(`🎵 Strezless Nexus API running on http://localhost:${PORT}`); });
