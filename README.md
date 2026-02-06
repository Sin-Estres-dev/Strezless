# Strezless - Indie Artist Platform

On a quest to fulfill a childhood dream, Sin Estres has launched Strezless: a comprehensive music production platform designed to empower independent artists with professional tools for catalog management, royalty tracking, and business growth.

## About

Strezless Musick Productionz is a music production platform founded by Sin Estres (Omar Orrantia). This full-stack Next.js application provides indie musicians with industry-standard tools to manage their music business, track professional codes (IPI, ISNI, ISRC, UPC), monitor royalties, and build sustainable careers.

## Features

### 🎵 Artist Profile Management
- Create comprehensive artist profiles with professional industry codes
- Store IPI (Interested Parties Information), ISNI, ISRC, ISWC, UPC, and EIN numbers
- Built-in validation algorithms for all music industry codes
- Manage contact information, payment details, and legal documentation

### 📀 Music Catalog System
- Add songs with complete metadata (genre, tempo, key signature, duration)
- Track ISRC codes, UPC codes, and copyright information
- Manage writer splits and collaboration credits
- Album and release management with DDEX compliance

### 💰 Revenue & Analytics
- Track earnings from multiple streaming platforms
- Monitor streams, downloads, and sync licenses
- View revenue history by platform and time period
- Real-time analytics dashboard

### 🤝 Collaboration Management
- Track featured artists, producers, and writers
- Calculate automatic revenue splits
- Manage collaboration credits and roles

## Tech Stack

- **Frontend**: Next.js 15.5.12, React 19, TypeScript
- **Styling**: Tailwind CSS with custom Strezless theme
- **Database**: PostgreSQL with Prisma ORM
- **Validation**: Zod with custom industry code validators
- **Forms**: React Hook Form with custom error handling
- **API**: Next.js API Routes with comprehensive error handling

## Getting Started

### Prerequisites

- Node.js 18+ 
- PostgreSQL database
- npm or yarn package manager

### Installation

1. Clone the repository
```bash
git clone https://github.com/Sin-Estres-dev/Strezless.git
cd Strezless
```

2. Install dependencies
```bash
npm install
```

3. Configure environment variables
```bash
cp .env.local.example .env.local
```

Edit `.env.local` and add your database connection string:
```env
DATABASE_URL="postgresql://username:password@localhost:5432/strezless_db"
NEXTAUTH_SECRET="your-secret-key-here"
NEXTAUTH_URL="http://localhost:3000"
```

4. Initialize the database
```bash
npx prisma generate
npx prisma db push
```

5. Run the development server
```bash
npm run dev
```

6. Open [http://localhost:3000](http://localhost:3000) in your browser

### Building for Production

1. Set production environment variables in `.env` or your hosting platform:
```env
# Production Database
DATABASE_URL="postgresql://user:password@production-host:5432/strezless_prod"

# NextAuth Configuration (generate a secure random string)
# Use: openssl rand -base64 32
NEXTAUTH_SECRET="your-production-secret-key-min-32-characters"
NEXTAUTH_URL="https://yourdomain.com"
```

2. Build and start the application:
```bash
npm run build
npm start
```

**Security Note**: Never commit `.env` files to version control. Use your hosting platform's environment variable management for production secrets.

## API Routes

### Artists

- `POST /api/artists` - Create a new artist profile
- `GET /api/artists` - Fetch all artists (supports pagination and search)
- `GET /api/artists/[id]` - Get specific artist with stats
- `PUT /api/artists/[id]` - Update artist profile
- `DELETE /api/artists/[id]` - Remove artist profile

### Songs

- `POST /api/songs` - Create a new song
- `GET /api/songs` - Fetch songs (supports filtering by artist, genre, search)

### Request/Response Examples

**Create Artist Profile**
```bash
POST /api/artists
Content-Type: application/json

{
  "stageName": "Sin Estres",
  "legalName": "Omar Orrantia",
  "email": "artist@strezless.com",
  "biography": "Independent hip-hop artist...",
  "ipiCode": "123456789",
  "isniCode": "0000000123456789"
}
```

**Create Song**
```bash
POST /api/songs
Content-Type: application/json

{
  "artistId": "artist-id-here",
  "title": "New Track",
  "duration": 180,
  "genre": "Hip-Hop",
  "tempo": 95,
  "isrcCode": "US-XYZ-24-12345",
  "copyrightYear": 2024
}
```

## Industry Code Validation

Strezless includes custom validation algorithms for music industry codes:

- **IPI**: 9-digit code with modulo 10 check digit validation
- **ISNI**: 16-character code with checksum validation
- **ISRC**: Format CC-XXX-YY-NNNNN with structure validation
- **UPC**: 12-digit code with check digit validation
- **ISWC**: Format T-NNNNNNNNN-C
- **EIN**: Format XX-XXXXXXX

## Project Structure

```
Strezless/
├── app/
│   ├── api/
│   │   ├── artists/
│   │   │   ├── route.ts
│   │   │   └── [id]/route.ts
│   │   └── songs/
│   │       └── route.ts
│   ├── create-profile/
│   │   └── page.tsx
│   ├── layout.tsx
│   ├── page.tsx
│   └── globals.css
├── components/
│   └── ArtistProfileForm.tsx
├── lib/
│   ├── prisma.ts
│   └── validators/
│       └── music-industry-codes.ts
├── prisma/
│   └── schema.prisma
├── docs/
│   ├── CAPABILITIES.md
│   └── splits/
└── package.json
```

## Database Schema

The platform uses a comprehensive schema with the following models:

- **User**: Authentication and session management
- **Artist**: Artist profiles with professional codes
- **Song**: Music tracks with metadata and industry codes
- **Album**: Album management with cover art and catalog numbers
- **Release**: Distribution tracking with DDEX support
- **Collaboration**: Multi-artist collaboration tracking
- **Revenue**: Earnings tracking by platform
- **Analytics**: Performance metrics and insights

## Available Scripts

- `npm run dev` - Start development server
- `npm run build` - Build for production
- `npm start` - Start production server
- `npm run lint` - Run ESLint
- `npm run db:generate` - Generate Prisma client
- `npm run db:push` - Push schema changes to database
- `npm run db:studio` - Open Prisma Studio

## Future Development

- Authentication system (NextAuth.js integration)
- Music file upload and audio processing
- Streaming platform API integrations (Spotify, Apple Music)
- Advanced analytics dashboard with charts
- Distribution workflow automation
- Payment processing (Stripe integration)
- Email notifications for royalty updates
- Mobile-responsive improvements
- Admin panel for code verification

## Contributing

We welcome contributions from the community! If you have ideas for features or improvements, feel free to open an issue or submit a pull request.

### Development Guidelines

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Test thoroughly
5. Commit your changes (`git commit -m 'Add amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

## Starring Artist

**Sin Estres** (Omar Orrantia) - Founder & Lead Developer

## License

This project is open for collaboration and ideas sharing.

## Support

For support, email support@strezless.com or open an issue in the GitHub repository.

---

Built with ❤️ by indie artists, for indie artists.
