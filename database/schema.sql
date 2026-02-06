-- Artist Profile Main Table
CREATE TABLE artists (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Basic Information
    artist_name VARCHAR(255) NOT NULL,
    legal_name VARCHAR(255),
    email VARCHAR(255) UNIQUE NOT NULL,
    phone VARCHAR(50),
    bio TEXT,
    profile_image_url TEXT,
    
    -- Professional Status
    career_level VARCHAR(50) CHECK (career_level IN ('emerging', 'indie', 'established', 'professional', 'signed')),
    genres TEXT[], -- Array of genres
    
    -- Social & Web
    website_url TEXT,
    social_links JSONB, -- {instagram: '', twitter: '', spotify: '', etc}
    
    -- Account Status
    is_verified BOOLEAN DEFAULT FALSE,
    is_active BOOLEAN DEFAULT TRUE
);

-- Industry Identifiers Table
CREATE TABLE artist_identifiers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    artist_id UUID REFERENCES artists(id) ON DELETE CASCADE,
    
    -- Performer Identifiers
    ipi_name_number VARCHAR(50), -- Interested Parties Information (11 digits)
    ipi_base_number VARCHAR(50), -- IPI Base Number
    isni VARCHAR(16), -- International Standard Name Identifier (16 digits)
    
    -- Tax & Business
    ein VARCHAR(20), -- Employer Identification Number (US)
    tax_id VARCHAR(50), -- General tax ID for international
    business_entity VARCHAR(100), -- LLC, Sole Proprietor, etc
    
    -- Verification
    verification_documents JSONB, -- Store document references
    verified_at TIMESTAMP WITH TIME ZONE,
    verified_by UUID, -- Admin who verified
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(artist_id)
);

-- Music Releases Table
CREATE TABLE releases (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    artist_id UUID REFERENCES artists(id) ON DELETE CASCADE,
    
    -- Release Information
    title VARCHAR(255) NOT NULL,
    release_type VARCHAR(50) CHECK (release_type IN ('single', 'ep', 'album', 'compilation', 'live')),
    release_date DATE,
    
    -- Release Identifiers
    upc VARCHAR(12), -- Universal Product Code (12 digits)
    catalog_number VARCHAR(100),
    grid VARCHAR(18), -- Global Release Identifier
    
    -- Metadata
    label_name VARCHAR(255),
    copyright_info TEXT,
    cover_art_url TEXT,
    total_tracks INTEGER,
    
    -- Distribution
    distribution_status VARCHAR(50) CHECK (distribution_status IN ('draft', 'submitted', 'processing', 'live', 'taken_down')),
    distributed_at TIMESTAMP WITH TIME ZONE,
    
    -- DDEX Delivery
    ddex_compliant BOOLEAN DEFAULT FALSE,
    ddex_version VARCHAR(20), -- e.g., "4.1"
    ddex_delivery_status VARCHAR(50),
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Tracks/Songs Table
CREATE TABLE tracks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    release_id UUID REFERENCES releases(id) ON DELETE CASCADE,
    
    -- Track Information
    title VARCHAR(255) NOT NULL,
    track_number INTEGER,
    duration_seconds INTEGER,
    
    -- Track Identifiers
    isrc VARCHAR(12) UNIQUE NOT NULL, -- International Standard Recording Code (12 characters)
    iswc VARCHAR(15), -- International Standard Musical Work Code (11 digits + prefix)
    
    -- Audio File
    audio_file_url TEXT,
    audio_format VARCHAR(20), -- WAV, FLAC, MP3, etc
    
    -- Metadata
    explicit_content BOOLEAN DEFAULT FALSE,
    language VARCHAR(10), -- ISO 639 language code
    
    -- Publishing
    lyrics TEXT,
    lyrics_language VARCHAR(10),
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Contributors/Collaborators Table (for splits)
CREATE TABLE track_contributors (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    track_id UUID REFERENCES tracks(id) ON DELETE CASCADE,
    artist_id UUID REFERENCES artists(id) ON DELETE SET NULL,
    
    -- Contributor Info
    contributor_name VARCHAR(255) NOT NULL, -- In case not in system
    contributor_ipi VARCHAR(50),
    contributor_isni VARCHAR(16),
    
    -- Role & Rights
    role VARCHAR(50) CHECK (role IN ('writer', 'composer', 'producer', 'performer', 'featured_artist')),
    ownership_percentage DECIMAL(5,2), -- e.g., 33.33%
    
    -- Split Sheet
    split_type VARCHAR(50) CHECK (split_type IN ('publishing', 'master', 'performance')),
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT valid_percentage CHECK (ownership_percentage >= 0 AND ownership_percentage <= 100)
);

-- Distribution Platforms Table
CREATE TABLE distribution_platforms (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    release_id UUID REFERENCES releases(id) ON DELETE CASCADE,
    
    -- Platform Info
    platform_name VARCHAR(100), -- Spotify, Apple Music, YouTube Music, etc
    platform_url TEXT,
    platform_id VARCHAR(255), -- ID on that platform
    
    -- Status
    status VARCHAR(50) CHECK (status IN ('pending', 'live', 'rejected', 'removed')),
    goes_live_at TIMESTAMP WITH TIME ZONE,
    
    -- Analytics
    streams_count BIGINT DEFAULT 0,
    downloads_count BIGINT DEFAULT 0,
    last_synced_at TIMESTAMP WITH TIME ZONE,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Fan Engagement Table
CREATE TABLE fans (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email VARCHAR(255) UNIQUE NOT NULL,
    name VARCHAR(255),
    
    -- Fan Data
    discovery_source VARCHAR(100), -- How they found the artist
    country VARCHAR(2), -- ISO country code
    
    -- Engagement
    is_subscribed BOOLEAN DEFAULT TRUE,
    subscribed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_interaction_at TIMESTAMP WITH TIME ZONE,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Artist-Fan Relationship
CREATE TABLE artist_fans (
    artist_id UUID REFERENCES artists(id) ON DELETE CASCADE,
    fan_id UUID REFERENCES fans(id) ON DELETE CASCADE,
    
    -- Engagement Metrics
    total_streams INTEGER DEFAULT 0,
    total_purchases DECIMAL(10,2) DEFAULT 0,
    engagement_score INTEGER DEFAULT 0, -- Calculated score
    
    first_interaction_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_interaction_at TIMESTAMP WITH TIME ZONE,
    
    PRIMARY KEY (artist_id, fan_id)
);

-- Analytics Table
CREATE TABLE analytics (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    artist_id UUID REFERENCES artists(id) ON DELETE CASCADE,
    
    -- Metrics
    metric_type VARCHAR(50), -- streams, downloads, followers, etc
    metric_value BIGINT,
    
    -- Dimensions
    platform VARCHAR(100),
    country VARCHAR(2),
    date DATE NOT NULL,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(artist_id, metric_type, platform, country, date)
);

-- Indexes for Performance
CREATE INDEX idx_artists_email ON artists(email);
CREATE INDEX idx_artist_identifiers_artist_id ON artist_identifiers(artist_id);
CREATE INDEX idx_artist_identifiers_ipi ON artist_identifiers(ipi_name_number);
CREATE INDEX idx_artist_identifiers_isni ON artist_identifiers(isni);
CREATE INDEX idx_releases_artist_id ON releases(artist_id);
CREATE INDEX idx_releases_upc ON releases(upc);
CREATE INDEX idx_tracks_release_id ON tracks(release_id);
CREATE INDEX idx_tracks_isrc ON tracks(isrc);
CREATE INDEX idx_tracks_iswc ON tracks(iswc);
CREATE INDEX idx_track_contributors_track_id ON track_contributors(track_id);
CREATE INDEX idx_distribution_platforms_release_id ON distribution_platforms(release_id);
CREATE INDEX idx_analytics_artist_date ON analytics(artist_id, date);

-- Triggers for updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_artists_updated_at BEFORE UPDATE ON artists
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_artist_identifiers_updated_at BEFORE UPDATE ON artist_identifiers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_releases_updated_at BEFORE UPDATE ON releases
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_tracks_updated_at BEFORE UPDATE ON tracks
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
