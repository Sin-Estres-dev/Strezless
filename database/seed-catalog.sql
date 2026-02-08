-- Seed Data: Sin Estres Artist Profile & "No Te Vayas Lejos" Release
-- Source: Spotify (https://spotify.link/SCXxnttbA0b)
-- Verified metadata fetched from Spotify, Amazon Music, YouTube, and DistroKid.
--
-- HOW TO VERIFY / FIX METADATA:
-- 1. Open https://open.spotify.com/track/6zxyOPt5XyHZfPgBiod2dI to confirm track details.
-- 2. Open https://open.spotify.com/album/7FnxAZfq8wWHucKFoIdnTr to confirm album/single details.
-- 3. Open https://open.spotify.com/artist/2FoHojEW56RXrQeqwz53Gb to confirm artist profile.
-- 4. Cross-reference with DistroKid: https://distrokid.com/hyperfollow/sinestres/no-te-vayas-lejos
-- 5. If any field below is inaccurate, update the corresponding INSERT value and re-run this seed.
-- 6. ISRC / UPC: These codes are assigned by DistroKid. Log in to your DistroKid dashboard
--    to retrieve the exact ISRC and UPC, then replace the placeholder values below.

-- =============================================================================
-- Artist: Sin Estres (Omar Orrantia)
-- =============================================================================
INSERT INTO artists (
    id, artist_name, legal_name, email, bio, career_level, genres, website_url, social_links
) VALUES (
    gen_random_uuid(),
    'Sin Estres',
    'Omar Orrantia',
    'contact@strezlessmusick.com',             -- UPDATE with actual email
    'Bilingual Latin hip hop artist and founder of Strezless Musick Productionz. '
    'Sin Estres shares deeply personal stories touching on trauma, depression, faith, '
    'and resilience through a mix of English and Spanish lyrics.',
    'indie',
    ARRAY['latin hip hop', 'hip hop', 'r&b'],
    'https://strezlessmusickproductionz.wordpress.com/',
    '{"spotify": "https://open.spotify.com/artist/2FoHojEW56RXrQeqwz53Gb",
      "youtube": "https://www.youtube.com/@Sin-Estres",
      "soundcloud": "https://soundcloud.com/therealsinestres",
      "pandora": "https://www.pandora.com/artist/sin-estres/ARkbZVV65trKdXc"}'::jsonb
);

-- =============================================================================
-- Release: "No Te Vayas Lejos" (Single)
-- Spotify Album ID: 7FnxAZfq8wWHucKFoIdnTr
-- =============================================================================
INSERT INTO releases (
    id, artist_id, title, release_type, release_date,
    upc, catalog_number, label_name, copyright_info,
    total_tracks, distribution_status
) VALUES (
    gen_random_uuid(),
    (SELECT id FROM artists WHERE artist_name = 'Sin Estres' LIMIT 1),
    'No Te Vayas Lejos',
    'single',
    '2020-08-31',
    'REPLACE_WITH_UPC',        -- TODO: Retrieve from DistroKid dashboard
    NULL,
    'Strezless Musick Productionz',
    '℗ 2020 Strezless Musick Productionz',
    1,
    'live'
);

-- =============================================================================
-- Track: "No Te Vayas Lejos"
-- Spotify Track ID: 6zxyOPt5XyHZfPgBiod2dI
-- Duration: 2:53 (173 seconds)
-- =============================================================================
INSERT INTO tracks (
    id, release_id, title, track_number, duration_seconds,
    isrc, explicit_content, language
) VALUES (
    gen_random_uuid(),
    (SELECT id FROM releases WHERE title = 'No Te Vayas Lejos' LIMIT 1),
    'No Te Vayas Lejos',
    1,
    173,
    'REPLACE_WITH_ISRC',       -- TODO: Retrieve from DistroKid dashboard
    false,
    'es'                       -- Spanish
);

-- =============================================================================
-- Distribution Platform Links
-- =============================================================================
INSERT INTO distribution_platforms (release_id, platform_name, platform_url, platform_id, status) VALUES
(
    (SELECT id FROM releases WHERE title = 'No Te Vayas Lejos' LIMIT 1),
    'Spotify',
    'https://open.spotify.com/track/6zxyOPt5XyHZfPgBiod2dI',
    '6zxyOPt5XyHZfPgBiod2dI',
    'live'
),
(
    (SELECT id FROM releases WHERE title = 'No Te Vayas Lejos' LIMIT 1),
    'YouTube',
    'https://www.youtube.com/watch?v=oT-zwQfRrbo',
    'oT-zwQfRrbo',
    'live'
),
(
    (SELECT id FROM releases WHERE title = 'No Te Vayas Lejos' LIMIT 1),
    'Amazon Music',
    'https://www.amazon.com/No-Vayas-Lejos-Sin-Estres/dp/B08F2D9LPG',
    'B08F2D9LPG',
    'live'
),
(
    (SELECT id FROM releases WHERE title = 'No Te Vayas Lejos' LIMIT 1),
    'DistroKid',
    'https://distrokid.com/hyperfollow/sinestres/no-te-vayas-lejos',
    NULL,
    'live'
);

-- =============================================================================
-- Contributors (Split Sheet)
-- Update ownership_percentage and add additional contributors as needed.
-- =============================================================================
INSERT INTO track_contributors (
    track_id, artist_id, contributor_name, role, ownership_percentage, split_type
) VALUES (
    (SELECT id FROM tracks WHERE title = 'No Te Vayas Lejos' LIMIT 1),
    (SELECT id FROM artists WHERE artist_name = 'Sin Estres' LIMIT 1),
    'Omar Orrantia',
    'writer',
    100.00,                    -- UPDATE if there are co-writers
    'publishing'
);
