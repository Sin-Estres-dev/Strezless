# Indie Artist Hub - Professional Music Career Management Platform

Welcome to your command center for managing your music business professionally. This application was built specifically for indie artists who need to organize their industry credentials and music catalog.

## Why This App Exists

As an independent artist, you're juggling multiple professional identities - IPI numbers from your PRO, ISNI codes for international recognition, ISRC codes for every track you release, and more. This app centralizes everything so you can focus on making music while staying organized for distribution, royalty collection, and professional opportunities.

## Quick Start Guide

### First Time Setup

Navigate to the `indie-artist-app` folder and run:

```bash
npm install
npm run dev
```

Your app launches at `http://localhost:3000`

### Your First Steps

1. **Create Your Profile** - Click the purple button on the homepage
2. **Add Basic Info** - Just your legal name and email to start
3. **Add Codes Later** - As you get IPI, ISNI, etc., come back and update
4. **Build Your Catalog** - Add tracks with their ISRC codes for distribution

## Understanding Your Professional Identity

### The Core Codes Explained

**IPI (Interested Parties Information)**
- Assigned by your PRO when you register as a songwriter
- Looks like: 00123456789
- Why it matters: Required for collecting performance royalties worldwide

**ISNI (International Standard Name Identifier)**  
- Global identifier like a social security number for creatives
- Format: 0000 0001 2345 6789
- Get it at: isni.org
- Why it matters: Prevents confusion between artists with similar names

**ISRC (International Standard Recording Code)**
- Every single recording needs one
- Format: USRC17607839
- Why it matters: This is how Spotify, Apple Music, etc. track YOUR specific recording

**ISWC (International Standard Musical Work Code)**
- For the composition/song itself (not the recording)
- Format: T-123.456.789-0
- Why it matters: Separates songwriter rights from recording rights

### Distribution Codes

**UPC/UPN (Universal Product Codes)**
- For your albums or singles as products
- Your distributor usually provides these
- 12-digit barcode numbers

**DDEX**
- Technical standard for sending data to stores
- Most distributors handle this automatically
- You might need this for direct deals

**EIN (Employer ID Number)**
- Like an SSN for your music business
- Get from IRS when you form an LLC or business entity
- Needed for: band accounts, hiring musicians, business expenses

## Application Features

### Profile Dashboard

Your artist profile serves as your professional portfolio. It displays:

- All your industry codes in one place
- Your complete music catalog
- Professional presentation for potential collaborators

Each code is color-coded for easy identification:
- Purple: IPI
- Blue: ISNI  
- Green: ISRC
- Yellow: DDEX
- Pink: UPN
- Indigo: EIN

### Catalog Management System

Track metadata includes:

- Essential info: Title, album name, duration, genre
- Distribution codes: ISRC (critical), UPC, UPN
- Technical data: DDEX compatibility info

Each track card shows:
- Duration in MM:SS format
- Genre tags
- Active ISRC and UPC codes as badges
- Quick visual confirmation of what's ready for distribution

## Technical Architecture

### Database Design Philosophy

We use **unique naming conventions** to avoid conflicts:

**ArtistProfile model:**
- `profileId` instead of generic "id"
- `legalFullName` vs just "name"
- `performanceName` for stage names
- `interestedPartyInfo` for IPI (self-documenting)
- `standardNameId` for ISNI (clear purpose)

**TrackRecord model:**
- `recordId` for unique identification  
- `trackTitle` and `albumTitle` (explicit)
- `durationSeconds` (unit in name)
- `intlStandardRecordingCode` for ISRC (full description)
- `belongsToArtist` (relationship is obvious)

### API Design

Endpoints follow resource-based patterns:

**Artist Operations:**
```
POST   /api/profiles              → Create new profile
GET    /api/profiles              → List all profiles
GET    /api/profiles?contactEmail → Search by email
GET    /api/profiles/:profileId   → Get one profile
PUT    /api/profiles/:profileId   → Update profile
```

**Track Operations:**
```
POST   /api/tracks                      → Add track
GET    /api/tracks                      → List all tracks
GET    /api/tracks?belongsToArtist=:id  → Artist's tracks
```

### Technology Choices

**Next.js 16 with App Router**: Server and client components for optimal performance

**TypeScript**: Type safety prevents bugs with important data like codes and IDs

**SQLite + Prisma**: Simple local database, easy to backup, portable

**Tailwind CSS**: Utility-first styling for rapid development

**better-sqlite3 adapter**: Fast, synchronous SQLite access for Prisma 7.x

## Workflow Examples

### Scenario: New Artist Just Starting

1. Create profile with just name and email
2. As you join BMI/ASCAP → Add your IPI code
3. When registering with ISNI → Add ISNI code  
4. First track finished? → Add to catalog with basic info
5. Before distribution → Get ISRC from distributor, add to track
6. Release day → You have all codes organized for metadata submission

### Scenario: Established Artist Migrating

1. Create profile with all existing codes
2. Batch import catalog (or add tracks one by one)
3. Each track gets its ISRC, UPC from past releases
4. Now everything's centralized for future releases

## Production Deployment

### Building for Release

```bash
npm run build
npm start
```

The optimized build includes:
- Static generation where possible
- API routes as serverless functions
- Compressed assets
- Production database connection

### Environment Setup

The app uses `./prisma/dev.db` for the database. For production:

1. Database stays portable (SQLite file)
2. Can upgrade to PostgreSQL later if needed
3. Backup = copy the `.db` file

### Database Management

View your data:
```bash
npx prisma studio
```

Reset database (careful!):
```bash
npx prisma migrate reset
```

Create new migration after schema changes:
```bash
npx prisma migrate dev --name describe_your_change
```

## Customization Ideas

This foundation supports expansion:

**Artist Tools:**
- Contract storage
- Split sheet calculator  
- Revenue tracking by platform
- Release calendar

**Collaboration:**
- Invite co-writers
- Share codes with your team
- Producer credits management

**Distribution:**
- Direct API integration with distributors
- Automated metadata export
- Release checklist generator

**Analytics:**
- Stream counts
- Geographic data
- Revenue per track

## Troubleshooting

**Can't connect to database?**
Check that `./prisma/dev.db` exists. If not:
```bash
npx prisma migrate dev
```

**Types not updating?**
Regenerate Prisma client:
```bash
npx prisma generate
```

**Build failing?**
Clear Next.js cache:
```bash
rm -rf .next
npm run build
```

**Page not loading?**
Check browser console for errors. Most common: forgot to await params in API routes (Next.js 16 requirement)

## Project Structure Overview

```
indie-artist-app/
├── src/
│   ├── app/
│   │   ├── api/              # Backend endpoints
│   │   │   ├── profiles/     # Artist CRUD operations
│   │   │   └── tracks/       # Catalog CRUD operations
│   │   ├── profiles/         # Artist pages
│   │   │   ├── new/          # Profile creation form
│   │   │   └── [profileId]/  # Profile view with catalog
│   │   ├── tracks/
│   │   │   └── new/          # Track upload form
│   │   ├── page.tsx          # Homepage dashboard
│   │   ├── layout.tsx        # App wrapper
│   │   └── globals.css       # Tailwind styles
│   ├── lib/
│   │   └── database.ts       # Prisma connection with adapter
│   └── generated/
│       └── prisma/           # Auto-generated client
├── prisma/
│   ├── schema.prisma         # Database models
│   ├── migrations/           # Version controlled schema changes
│   └── dev.db               # Your SQLite database
└── package.json
```

## Contributing to Your Own Fork

This app is your foundation. Ideas to make it yours:

1. Add your branding colors to Tailwind config
2. Customize field labels for your workflow
3. Add fields specific to your genre/scene
4. Integrate with your existing tools
5. Export to formats you use

## Data Privacy

Your data lives in `prisma/dev.db` on your machine. Nothing is sent to external servers unless you deploy it. All codes and personal info stay local.

## Next Steps

Now that you understand the system:

1. Create your artist profile
2. Add your first track
3. As you get professional codes, update your profile
4. Use this as your reference when distributing music
5. Keep building on this foundation

Remember: Start simple. Add codes as you grow. This app grows with your career.

---

Built for indie artists, by developers who understand the music industry.

