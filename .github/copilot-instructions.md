# Copilot Instructions for Strezless Musick Productionz

## Project Overview
Strezless is a music production business management platform. This repository provides a backend API for managing artist profiles, music releases, distribution, and music industry business documentation.

**Domain Context**: Music business operations, including artist rights management, split sheets, PRO (Performance Rights Organization) registration, and industry standard identifiers (IPI, ISNI, ISRC, ISWC, UPC).

## Tech Stack
- **Runtime**: Node.js (CommonJS modules)
- **Framework**: Express.js 5.x
- **Database**: PostgreSQL with UUID primary keys
- **Environment**: dotenv for configuration (.env.local)
- **Key Dependencies**:
  - `pg` for PostgreSQL connection pooling
  - `body-parser` for request parsing
  - `express` for REST API

## Project Structure
```
/
├── index.js              # Main Express app entry point
├── routes/               # API route handlers
│   └── artistProfile.js  # Artist management endpoints
├── database/             # Database schema and migrations
│   └── schema.sql        # PostgreSQL schema with industry identifiers
├── docs/                 # Music business documentation
│   ├── splits/           # Split sheet templates
│   └── legal/            # Legal agreements and guides
└── .github/              # GitHub configuration
```

## Development Workflow

### Running the Application
```bash
# Install dependencies
npm install

# Development server (default port 3000)
npm run dev

# The server runs at http://localhost:3000
```

### Environment Variables
Required in `.env.local`:
- `DATABASE_URL`: PostgreSQL connection string
- `PORT`: Server port (optional, defaults to 3000)
- `NODE_ENV`: Environment (development/production)

### API Endpoints
Base URL: `http://localhost:3000`

- `POST /api/artist/profile` - Create artist profile
- `GET /api/artist/profile/:id` - Get artist profile
- `PUT /api/artist/profile/:id` - Update artist profile
- `PUT /api/artist/identifiers/:artistId` - Update industry identifiers
- `GET /api/artist/search` - Search artists by identifiers
- `GET /health` - Health check endpoint

## Coding Standards & Conventions

### JavaScript Style
- Use CommonJS modules (`require`/`module.exports`)
- Use `async/await` for asynchronous operations
- Always use transaction blocks (BEGIN/COMMIT/ROLLBACK) for multi-step database operations
- Use connection pooling (`pool.connect()`) with proper client release in `finally` blocks
- Include JSDoc comments for route handlers
- Use destructuring for request parameters

### Database Conventions
- **Primary Keys**: Use UUID with `gen_random_uuid()`
- **Timestamps**: Include `created_at` and `updated_at` (auto-managed by triggers)
- **Naming**: Use snake_case for column names, plural for table names
- **Industry Identifiers**:
  - IPI (Interested Parties Information): 11 digits
  - ISNI (International Standard Name Identifier): 16 digits
  - ISRC (International Standard Recording Code): 12 characters
  - ISWC (International Standard Musical Work Code): 11 digits + prefix
  - UPC (Universal Product Code): 12 digits
  - EIN (Employer Identification Number): US tax ID

### Error Handling
- Always wrap async database operations in try/catch
- Use transaction rollback on errors
- Return JSON responses with consistent format:
  ```javascript
  {
    success: true/false,
    data: {...},        // on success
    message: "...",     // descriptive message
    error: "..."        // error details (development only)
  }
  ```
- Log errors with `console.error()` before sending response
- Use appropriate HTTP status codes (201 for created, 404 for not found, 500 for server errors)

### API Response Format
All API responses should follow this structure:
```javascript
{
  success: boolean,
  data?: object | array,
  message?: string,
  error?: string,
  count?: number  // for list/search endpoints
}
```

### CORS Configuration
- Development: Allow all origins (configured in index.js)
- Production: Configure CORS properly with specific origins

## Music Industry Specific Guidelines

### Artist Profiles
- `career_level` must be one of: 'emerging', 'indie', 'established', 'professional', 'signed'
- Always validate industry identifiers format before storing
- Email is required and unique across artists
- Genres are stored as PostgreSQL arrays

### Releases & Tracks
- `release_type` options: 'single', 'ep', 'album', 'compilation', 'live'
- `distribution_status`: 'draft', 'submitted', 'processing', 'live', 'taken_down'
- ISRC codes are mandatory and unique for each track
- DDEX compliance tracking is required for distribution

### Split Sheets & Contributors
- Ownership percentages must sum to 100% for each track
- `role` options: 'writer', 'composer', 'producer', 'performer', 'featured_artist'
- `split_type` categories: 'publishing', 'master', 'performance'
- Always store contributor IPI/ISNI if available

### Documentation
- Split sheets are in `/docs/splits/`
- Legal templates are in `/docs/legal/`
- Maintain markdown format for all documentation
- Follow existing template structure when adding new documents

## Security Requirements
- Never commit sensitive data (API keys, database credentials, etc.)
- Use environment variables for all sensitive configuration
- Validate all user input before database queries
- Use parameterized queries to prevent SQL injection
- Store hashed passwords only (when authentication is added)
- Configure SSL for production database connections

## Testing
Current state: No test infrastructure exists yet.
When adding tests in the future:
- Consider using Jest or Mocha for testing
- Test database operations with a test database
- Mock external API calls
- Test validation logic thoroughly

## Dependencies
- Avoid adding new dependencies unless absolutely necessary
- When adding dependencies, ensure they are actively maintained
- Review security advisories before adding packages
- Update package.json and package-lock.json together

## Important Notes
- This is a music business application - understand the domain context (PRO registration, split sheets, publishing rights)
- Industry identifiers (IPI, ISNI, ISRC, etc.) follow international standards - validate format
- Transaction integrity is critical for artist profile and identifier operations
- Social links are stored as JSONB for flexibility
- Always release database clients in finally blocks
- The application uses Express 5.x (note differences from Express 4.x)

## Common Tasks

### Adding a New API Endpoint
1. Add route handler in appropriate file in `/routes/`
2. Include JSDoc comment describing the endpoint
3. Use async/await with proper error handling
4. Follow consistent response format
5. Release database connections properly

### Adding Database Tables
1. Update `/database/schema.sql`
2. Add appropriate indexes for foreign keys and frequently queried columns
3. Include `created_at` and `updated_at` timestamps
4. Add trigger for auto-updating `updated_at`
5. Use UUID for primary keys

### Working with Music Business Documentation
1. Follow existing markdown template structure
2. Store in appropriate `/docs/` subdirectory
3. Reference relevant PRO requirements and industry standards
4. Include examples where helpful

## References
- Express.js Documentation: https://expressjs.com/
- PostgreSQL Documentation: https://www.postgresql.org/docs/
- DDEX Standards: https://ddex.net/
- ISRC Information: https://www.usisrc.org/
- IPI Database: https://www.cisac.org/
