# Database Schema Updates - Royalty Tracking Enhancement

## Overview
This document describes the database schema enhancements added to support comprehensive royalty tracking, publishing agreements, split sheets, and PRO registrations for indie artists.

## New Tables

### 1. publishing_agreements
Tracks publishing deals and agreements with publishers.

**Key Features:**
- Publisher information including IPI numbers
- Agreement types: admin, co-publishing, full, sub-publishing
- Territory specifications (worldwide, US only, EU, etc.)
- Financial terms including royalty splits and advance amounts
- Recoupment status tracking

**Foreign Keys:**
- `artist_id` → `artists(id)` ON DELETE CASCADE

**Use Cases:**
- Record publishing deals with publishers
- Track advance recoupment status
- Monitor active agreements by territory
- Manage royalty split percentages

---

### 2. royalty_accruals
Tracks royalty earnings by source with detailed financial breakdowns.

**Key Features:**
- Multiple revenue sources: streaming, mechanical, performance, sync, download
- Platform tracking (Spotify, Apple Music, YouTube, Radio, etc.)
- Territory-based earnings
- Usage metrics (streams, downloads, plays, sales)
- Financial calculations with deductions and net revenue
- Split allocations between writers and publishers
- Payment status tracking

**Foreign Keys:**
- `track_id` → `tracks(id)` ON DELETE CASCADE
- `artist_id` → `artists(id)` ON DELETE CASCADE
- `release_id` → `releases(id)` ON DELETE SET NULL

**Constraints:**
- Revenue validation: Allows small rounding differences (< $0.01)
- Positive revenue check
- Payment status: accrued, paid, pending, disputed, cancelled

**Indexes:**
- `idx_royalty_accruals_artist` - Fast artist lookups
- `idx_royalty_accruals_track` - Fast track lookups
- `idx_royalty_accruals_period` - Period-based queries
- `idx_royalty_accruals_status` - Payment status filtering

**Use Cases:**
- Track earnings from multiple streaming platforms
- Monitor mechanical royalties from downloads
- Record performance royalties from radio/TV
- Track sync licensing revenue
- Generate royalty statements by period
- Calculate writer vs publisher splits

---

### 3. split_sheets
Manages song ownership splits among multiple writers with automatic validation.

**Key Features:**
- Writer information including IPI numbers and PRO affiliations
- Publisher details for each writer
- Publishing and writer share percentages
- Role tracking (writer, composer, lyricist, producer, co-writer)
- Digital signature support with IP tracking
- Status workflow: draft → pending → signed → finalized
- Automatic validation to prevent splits exceeding 100%

**Foreign Keys:**
- `track_id` → `tracks(id)` ON DELETE CASCADE

**Constraints:**
- Publishing share: 0-100%
- Writer share: 0-100%
- Total splits cannot exceed 100% (with 0.01% tolerance for rounding)

**Triggers:**
- `enforce_split_totals` - Validates that total splits don't exceed 100%

**Indexes:**
- `idx_split_sheets_track` - Fast track lookups
- `idx_split_sheets_writer_ipi` - Writer IPI lookups

**Use Cases:**
- Create split sheets for co-written songs
- Track writer shares for royalty distribution
- Manage digital signature collection
- Prevent disputes with automatic validation
- Send invitations and reminders to co-writers

---

### 4. pro_registrations
Tracks registrations with Performing Rights Organizations.

**Key Features:**
- PRO tracking (ASCAP, BMI, SESAC, SOCAN, PRS, GEMA, etc.)
- Registration status workflow
- Alternate titles support
- Submission and confirmation tracking
- Rejection reason documentation

**Foreign Keys:**
- `track_id` → `tracks(id)` ON DELETE CASCADE
- `artist_id` → `artists(id)` ON DELETE CASCADE

**Constraints:**
- Registration status: draft, pending, submitted, registered, rejected, needs_revision
- Composite unique constraint: (track_id, pro_name, registration_number)

**Indexes:**
- `idx_pro_registrations_track` - Fast track lookups
- `idx_pro_registrations_artist` - Fast artist lookups
- `idx_pro_registrations_status` - Status filtering
- `idx_pro_registrations_pro` - PRO filtering
- `idx_pro_registrations_unique` - Prevents duplicate registrations

**Use Cases:**
- Track PRO registration status for each track
- Monitor multiple PRO registrations per track
- Store confirmation numbers and documentation
- Track rejection reasons and revisions needed
- Support international PRO registrations

---

### 5. metadata_validation_log
Audit trail for metadata validation and cleanup activities.

**Key Features:**
- Entity type tracking (artist, release, track, identifier)
- Validation types: format, completeness, uniqueness, standard_compliance
- Error messages and suggested fixes
- Before/after value tracking
- Fix tracking with timestamps and user attribution

**Constraints:**
- Entity types: artist, release, track, identifier
- Validation types: format, completeness, uniqueness, standard_compliance

**Indexes:**
- `idx_metadata_validation_entity` - Entity lookups
- `idx_metadata_validation_valid` - Validity filtering

**Use Cases:**
- Track ISRC/ISWC format validations
- Monitor IPI number correctness
- Record metadata cleanup activities
- Audit trail for compliance
- Identify recurring validation issues

---

## Database Functions

### validate_split_totals()
PL/pgSQL function that validates split sheet totals.

**Behavior:**
- Calculates total publishing_share and writer_share for a track
- Excludes 'disputed' status splits from calculation
- Allows 0.01% tolerance for rounding differences
- Raises exception if totals exceed 100.01%
- Returns NEW row on success

**Triggered by:**
- INSERT on split_sheets
- UPDATE on split_sheets

---

## Triggers

### Automatic Timestamp Updates
All new tables have automatic `updated_at` timestamp updates:

- `update_publishing_agreements_updated_at`
- `update_royalty_accruals_updated_at`
- `update_split_sheets_updated_at`
- `update_pro_registrations_updated_at`

### Split Validation
- `enforce_split_totals` - Ensures split totals don't exceed 100%

---

## Migration Notes

### Prerequisites
- PostgreSQL 12+ (for gen_random_uuid() support)
- Existing tables: artists, releases, tracks

### Applying the Schema
```sql
-- Run the entire schema.sql file for new databases
psql -d your_database -f database/schema.sql

-- Or for existing databases, run only the new sections
psql -d your_database -f database/schema.sql --single-transaction
```

### Rollback
If you need to remove these tables:
```sql
DROP TABLE IF EXISTS metadata_validation_log CASCADE;
DROP TABLE IF EXISTS pro_registrations CASCADE;
DROP TABLE IF EXISTS split_sheets CASCADE;
DROP TABLE IF EXISTS royalty_accruals CASCADE;
DROP TABLE IF EXISTS publishing_agreements CASCADE;
DROP FUNCTION IF EXISTS validate_split_totals() CASCADE;
```

---

## Best Practices

### Split Sheets
1. Always create split sheets before distribution
2. Ensure all writers sign before finalizing
3. Total splits should equal exactly 100% (or very close due to rounding)
4. Document contribution descriptions clearly

### Royalty Accruals
1. Record royalties by period (monthly or quarterly)
2. Keep gross_revenue, deductions, and net_revenue in sync
3. Always specify currency and exchange_rate
4. Link to appropriate track, artist, and release

### PRO Registrations
1. Register with all relevant PROs by territory
2. Use alternate_titles array for different language versions
3. Keep submission documents for audit purposes
4. Track confirmation numbers for reference

### Publishing Agreements
1. Record start_date and end_date for all agreements
2. Update recoupment_status as advances are recouped
3. Store contract documents securely
4. Mark inactive agreements with is_active = FALSE

---

## Example Queries

### Find all unpaid royalties for an artist
```sql
SELECT * FROM royalty_accruals
WHERE artist_id = 'your-artist-uuid'
  AND payment_status = 'accrued'
ORDER BY period_end DESC;
```

### Get split sheet summary for a track
```sql
SELECT 
    track_id,
    SUM(publishing_share) as total_publishing,
    SUM(writer_share) as total_writer,
    COUNT(*) as writer_count
FROM split_sheets
WHERE track_id = 'your-track-uuid'
  AND status != 'disputed'
GROUP BY track_id;
```

### Check PRO registration status
```sql
SELECT 
    t.title,
    pr.pro_name,
    pr.registration_status,
    pr.registration_date
FROM pro_registrations pr
JOIN tracks t ON t.id = pr.track_id
WHERE pr.artist_id = 'your-artist-uuid'
ORDER BY pr.registration_date DESC;
```

### Find tracks needing metadata validation
```sql
SELECT DISTINCT entity_id, entity_type
FROM metadata_validation_log
WHERE is_valid = FALSE
  AND fixed_at IS NULL
ORDER BY created_at DESC;
```

---

## Support

For questions or issues with the schema, please refer to:
- Music industry standards (DDEX, CISAC)
- PRO documentation (ASCAP, BMI, SESAC, etc.)
- Database documentation in /docs

## Version History

- **v1.0** (2026-02-08) - Initial release with 5 new tables for royalty tracking
