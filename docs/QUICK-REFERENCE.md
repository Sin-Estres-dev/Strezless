# Indie Artist App - Quick Reference Card

## 🚀 Get Started in 3 Steps

```bash
cd indie-artist-app
npm install
npm run dev
```

Visit: **http://localhost:3000**

---

## 📋 Professional Code Cheat Sheet

| Code | Full Name | Where to Get | When You Need It |
|------|-----------|--------------|------------------|
| **IPI** | Interested Parties Info | Your PRO (ASCAP/BMI/SESAC) | When joining a PRO |
| **ISNI** | International Standard Name ID | isni.org | Once you've released music |
| **ISRC** | Intl Standard Recording Code | Your distributor | **BEFORE every release** |
| **ISWC** | Intl Standard Musical Work Code | PRO/Publisher | For each composition |
| **UPC** | Universal Product Code | Distributor | For albums/singles |
| **UPN** | Universal Product Number | Distributor | Alternative to UPC |
| **DDEX** | Digital Data Exchange | Distributor handles | Advanced distribution |
| **EIN** | Employer ID Number | IRS (USA) | Forming a business |
| **UUID** | Universal Unique ID | Auto-generated | Internal tracking |

---

## 🎯 Common Tasks

### Create Artist Profile
1. Homepage → **"Create Your Artist Profile"**
2. Enter name + email (required)
3. Add codes as you get them
4. **Submit**

### Add Track
1. Artist Profile → **"Add Track"**
2. Enter title + duration
3. **Add ISRC before distribution!**
4. **Submit**

### View API Data
```bash
# All artists
curl http://localhost:3000/api/profiles

# Specific artist
curl http://localhost:3000/api/profiles/PROFILE_ID

# Artist's tracks
curl "http://localhost:3000/api/tracks?belongsToArtist=PROFILE_ID"
```

---

## 🎵 Release Checklist

Before releasing a track, ensure you have:

- [ ] Artist profile created
- [ ] Legal name and email added
- [ ] IPI code (if you have one)
- [ ] Track added to catalog
- [ ] **ISRC code assigned** ⚠️ CRITICAL
- [ ] UPC code (for release)
- [ ] All metadata filled in
- [ ] Genre specified
- [ ] Duration accurate

---

## 🛠️ Quick Commands

```bash
# Development
npm run dev              # Start dev server (port 3000)
npm run build            # Build for production
npm start                # Run production build

# Database
npx prisma studio        # View data in browser GUI
npx prisma generate      # Regenerate Prisma client
npx prisma migrate dev   # Create/apply migrations

# Troubleshooting
rm -rf .next             # Clear Next.js cache
rm -rf node_modules      # Remove dependencies
npm install              # Reinstall dependencies
```

---

## 📊 API Quick Reference

### Artists

```bash
# CREATE
POST /api/profiles
Body: {"legalFullName": "...", "contactEmail": "..."}

# READ ALL
GET /api/profiles

# READ ONE
GET /api/profiles/:id

# UPDATE
PUT /api/profiles/:id
Body: {...updated fields...}
```

### Tracks

```bash
# CREATE
POST /api/tracks
Body: {"trackTitle": "...", "durationSeconds": 180, "belongsToArtist": "..."}

# READ ALL
GET /api/tracks

# READ BY ARTIST
GET /api/tracks?belongsToArtist=:id
```

---

## 🎨 UI Color Codes

Professional codes are color-coded in the profile view:

- 🟣 **Purple** - IPI Code
- 🔵 **Blue** - ISNI Code  
- 🟢 **Green** - ISWC Code
- 🟡 **Yellow** - DDEX ID
- 🩷 **Pink** - UPN
- 🟣 **Indigo** - EIN

---

## ⚠️ Important Notes

### ISRC is CRITICAL
- Every recording needs its own ISRC
- Get it BEFORE distribution
- Can't change after release
- Required for royalty tracking

### IPI Code
- One per creator (not per song)
- Required for collecting performance royalties
- Get from your PRO

### UPC/EAN
- One per release (album or single)
- Like a barcode
- Distributor usually provides

---

## 🔧 Common Issues & Fixes

**Database error?**
```bash
npx prisma migrate dev
npx prisma generate
```

**Port 3000 in use?**
```bash
npm run dev -- -p 3001
```

**Types not updating?**
```bash
npx prisma generate
rm -rf .next
```

**Can't see changes?**
- Hard refresh: Ctrl+Shift+R (Cmd+Shift+R on Mac)

---

## 📂 File Locations

```
indie-artist-app/
├── src/app/page.tsx              # Homepage
├── src/app/profiles/new/         # Create profile
├── src/app/profiles/[id]/        # View profile
├── src/app/tracks/new/           # Add track
├── src/app/api/profiles/         # Artist API
├── src/app/api/tracks/           # Track API
├── prisma/schema.prisma          # Database schema
└── prisma/dev.db                 # Your data
```

---

## 💡 Pro Tips

1. **Start Simple**: Just name + email, add codes later
2. **ISRC First**: Get ISRCs before distributing anything
3. **Backup Database**: `cp prisma/dev.db prisma/backup.db`
4. **Use Prisma Studio**: Visual way to view/edit data
5. **Document Everything**: Add notes in a separate file

---

## 🎯 Workflow for New Release

```
1. Create artist profile (if not exists)
2. Get ISRC from distributor
3. Add track with ISRC to catalog
4. Get UPC for release
5. Update track with UPC
6. Export metadata from app
7. Submit to distributor
8. You're organized and professional! ✅
```

---

## 📱 Mobile/Remote Access

To access from other devices on same network:

```bash
# Find your local IP
ipconfig getifaddr en0     # Mac
hostname -I                # Linux

# Then visit from phone/tablet:
http://YOUR_IP:3000
```

---

## 🚀 Next Steps

1. ✅ Create your profile
2. ✅ Add your first track  
3. ✅ Update with codes as you get them
4. ✅ Use for every release
5. ✅ Build on this foundation

---

**Need more help?** See full docs:
- `indie-artist-app/README.md` - Technical details
- `docs/INDIE-ARTIST-APP-GUIDE.md` - Complete guide
- `docs/APP-ARCHITECTURE.md` - Visual overview

**Built for indie artists. By developers who care about music. 🎵**
