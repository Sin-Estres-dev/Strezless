const express = require('express');
const app = express();

// Set up the port (default to 3000 if not provided in environment variables)
const PORT = process.env.PORT || 3000;

// Default route for the server
app.get('/', (req, res) => {
  res.send('Hello, Strezless is running successfully!');
});

// Start the server
app.listen(PORT, () => {
  console.log(`Server is running at http://localhost:${PORT}`);
});
