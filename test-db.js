const { Pool } = require('pg');
require('dotenv').config({ path: '.env.local' });

console.log('DATABASE_URL:', process.env.DATABASE_URL);

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: false
});

pool.query('SELECT NOW()', (err, res) => {
  if (err) {
    console.error('Database connection error:', err);
  } else {
    console.log('Database connected successfully!', res.rows[0]);
  }
  pool.end();
});
