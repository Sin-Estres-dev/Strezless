// Node server (modified to respect environment var for DB path)
const express = require('express');
const bodyParser = require('body-parser');
const fs = require('fs');
const path = require('path');
const Ajv = require('ajv').default;
const { Low, JSONFile } = require('lowdb');
const { nanoid } = require('nanoid');

const app = express();
app.use(bodyParser.json());

const schemaPath = path.join(__dirname, '..', 'metadata', 'track-metadata.schema.json');
const schema = JSON.parse(fs.readFileSync(schemaPath, 'utf8'));
const ajv = new Ajv({ allErrors: true });
const validate = ajv.compile(schema);

// Respect NODE_DB_FILE env var; default to services/node/data/node-db.json
const dbFile = process.env.NODE_DB_FILE || path.join(__dirname, '..', 'data', 'node-db.json');
fs.mkdirSync(path.dirname(dbFile), { recursive: true });
const adapter = new JSONFile(dbFile);
const db = new Low(adapter);

(async () => {
  await db.read();
  db.data = db.data || { tracks: [] };
  await db.write();
})();

app.post('/validate', (req, res) => {
  const valid = validate(req.body);
  if (!valid) {
    return res.status(400).json({ valid: false, errors: validate.errors });
  }
  res.json({ valid: true });
});

app.post('/save', async (req, res) => {
  const valid = validate(req.body);
  if (!valid) {
    return res.status(400).json({ saved: false, errors: validate.errors });
  }
  const id = nanoid();
  db.data.tracks.push({ id, payload: req.body });
  await db.write();
  res.status(201).json({ saved: true, id });
});

app.post('/split', (req, res) => {
  const writers = req.body.writers || [];
  const total = writers.reduce((s, w) => s + (w.split || 0), 0);
  res.json({ writers, total });
});

const port = process.env.NODE_PORT || process.env.PORT || 5002;
app.listen(port, () => console.log(`Node validator running on ${port}`));

module.exports = app; // export for tests