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

-- Publishing Agreements Table
CREATE TABLE publishing_agreements (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    artist_id UUID REFERENCES artists(id) ON DELETE CASCADE,
    
    -- Publisher Information
    publisher_name VARCHAR(255) NOT NULL,
    publisher_ipi VARCHAR(50),
    publisher_address TEXT,
    
    -- Agreement Details
    agreement_type VARCHAR(50) CHECK (agreement_type IN ('admin', 'co-publishing', 'full', 'sub-publishing')),
    start_date DATE NOT NULL,
    end_date DATE,
    territory VARCHAR(100), -- e.g., 'worldwide', 'US only', 'EU'
    
    -- Financial Terms
    royalty_split_percentage DECIMAL(5,2) CHECK (royalty_split_percentage >= 0 AND royalty_split_percentage <= 100),
    advance_amount DECIMAL(12,2),
    recoupment_status VARCHAR(50) CHECK (recoupment_status IN ('unrecouped', 'recouped', 'n/a')),
    
    -- Documentation
    contract_document_url TEXT,
    notes TEXT,
    
    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Royalty Accruals Table
CREATE TABLE royalty_accruals (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    track_id UUID REFERENCES tracks(id) ON DELETE CASCADE,
    artist_id UUID REFERENCES artists(id) ON DELETE CASCADE,
    release_id UUID REFERENCES releases(id) ON DELETE SET NULL,
    
    -- Royalty Source Details
    source VARCHAR(100) NOT NULL, -- 'streaming', 'mechanical', 'performance', 'sync', 'download'
    platform VARCHAR(100), -- 'Spotify', 'Apple Music', 'YouTube', 'Radio', etc.
    territory VARCHAR(100), -- Country or region
    
    -- Period
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    statement_date DATE,
    
    -- Usage Metrics
    units INTEGER, -- streams, downloads, plays, sales
    unit_type VARCHAR(50), -- 'streams', 'downloads', 'radio_plays', 'sales'
    
    -- Financial Details
    gross_revenue DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    deductions DECIMAL(12,2) DEFAULT 0.00, -- fees, taxes, etc.
    net_revenue DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    
    -- Split Allocations
    writer_share_percentage DECIMAL(5,2),
    writer_earnings DECIMAL(12,2),
    publisher_share_percentage DECIMAL(5,2),
    publisher_earnings DECIMAL(12,2),
    
    -- Payment Status
    payment_status VARCHAR(50) CHECK (payment_status IN ('accrued', 'paid', 'pending', 'disputed', 'cancelled')) DEFAULT 'accrued',
    paid_date DATE,
    payment_reference VARCHAR(255),
    
    -- Metadata
    currency VARCHAR(3) DEFAULT 'USD',
    exchange_rate DECIMAL(10,4) DEFAULT 1.0000,
    notes TEXT,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Ensure valid financial calculations (allows small rounding differences)
    CONSTRAINT valid_revenue CHECK (ABS(net_revenue - (gross_revenue - deductions)) < 0.01),
    CONSTRAINT positive_revenue CHECK (gross_revenue >= 0)
);

-- Index for performance on common queries
CREATE INDEX idx_royalty_accruals_artist ON royalty_accruals(artist_id);
CREATE INDEX idx_royalty_accruals_track ON royalty_accruals(track_id);
CREATE INDEX idx_royalty_accruals_period ON royalty_accruals(period_start, period_end);
CREATE INDEX idx_royalty_accruals_status ON royalty_accruals(payment_status);

-- Split Sheets Table (for multiple writers per track)
CREATE TABLE split_sheets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    track_id UUID REFERENCES tracks(id) ON DELETE CASCADE,
    
    -- Writer Information
    writer_name VARCHAR(255) NOT NULL,
    writer_legal_name VARCHAR(255),
    writer_email VARCHAR(255),
    writer_ipi VARCHAR(50),
    writer_pro VARCHAR(100), -- PRO affiliation (ASCAP, BMI, SESAC, SOCAN, PRS, etc.)
    writer_publisher VARCHAR(255),
    writer_publisher_ipi VARCHAR(50),
    
    -- Ownership Splits (totals must = 100% across all writers for a track)
    publishing_share DECIMAL(5,2) NOT NULL, -- % of publishing (0-100)
    writer_share DECIMAL(5,2) NOT NULL, -- % of writer's share (0-100)
    
    -- Role/Contribution
    role VARCHAR(50) CHECK (role IN ('writer', 'composer', 'lyricist', 'producer', 'co-writer')),
    contribution_description TEXT,
    
    -- Agreement & Signatures
    agreement_date DATE,
    signed_date DATE,
    signature_url TEXT, -- Digital signature reference/URL
    ip_address INET, -- IP address when signed (for verification)
    
    -- Status
    status VARCHAR(50) CHECK (status IN ('draft', 'pending', 'signed', 'disputed', 'finalized')) DEFAULT 'draft',
    
    -- Notifications
    invitation_sent_at TIMESTAMP WITH TIME ZONE,
    reminder_sent_at TIMESTAMP WITH TIME ZONE,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Validation constraints
    CONSTRAINT valid_publishing_split CHECK (publishing_share >= 0 AND publishing_share <= 100),
    CONSTRAINT valid_writer_split CHECK (writer_share >= 0 AND writer_share <= 100)
);

-- Index for lookups
CREATE INDEX idx_split_sheets_track ON split_sheets(track_id);
CREATE INDEX idx_split_sheets_writer_ipi ON split_sheets(writer_ipi);

-- Function to validate split totals = 100%
CREATE OR REPLACE FUNCTION validate_split_totals()
RETURNS TRIGGER AS $$
DECLARE
    total_publishing DECIMAL(5,2);
    total_writer DECIMAL(5,2);
    -- Tolerance for rounding differences (0.01 = 0.01%)
    ROUNDING_TOLERANCE CONSTANT DECIMAL(5,2) := 0.01;
BEGIN
    -- Calculate totals for this track (excluding disputed splits)
    SELECT 
        COALESCE(SUM(publishing_share), 0),
        COALESCE(SUM(writer_share), 0)
    INTO total_publishing, total_writer
    FROM split_sheets
    WHERE track_id = NEW.track_id
    AND status != 'disputed';
    
    -- Allow slight rounding differences
    IF total_publishing > (100.0 + ROUNDING_TOLERANCE) OR total_writer > (100.0 + ROUNDING_TOLERANCE) THEN
        RAISE EXCEPTION 'Total splits cannot exceed 100%% (Publishing: %, Writer: %)', total_publishing, total_writer;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to validate splits on insert/update
CREATE TRIGGER enforce_split_totals
AFTER INSERT OR UPDATE ON split_sheets
FOR EACH ROW
EXECUTE FUNCTION validate_split_totals();

-- PRO (Performing Rights Organization) Registrations
CREATE TABLE pro_registrations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    track_id UUID REFERENCES tracks(id) ON DELETE CASCADE,
    artist_id UUID REFERENCES artists(id) ON DELETE CASCADE,
    
    -- PRO Details
    pro_name VARCHAR(100) NOT NULL, -- 'ASCAP', 'BMI', 'SESAC', 'SOCAN', 'PRS', 'GEMA', etc.
    pro_territory VARCHAR(100), -- 'US', 'Canada', 'UK', 'Germany', etc.
    
    -- Registration Details
    registration_number VARCHAR(100),
    work_title VARCHAR(255) NOT NULL,
    alternate_titles TEXT[], -- Array of alternate titles
    
    -- Registration Status
    registration_date DATE,
    registration_status VARCHAR(50) CHECK (registration_status IN ('draft', 'pending', 'submitted', 'registered', 'rejected', 'needs_revision')) DEFAULT 'draft',
    
    -- Submission Details
    submitted_date DATE,
    submitted_by VARCHAR(255),
    confirmation_number VARCHAR(100),
    
    -- Response/Issues
    response_date DATE,
    rejection_reason TEXT,
    notes TEXT,
    
    -- Documentation
    submission_document_url TEXT,
    confirmation_document_url TEXT,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for common queries
CREATE INDEX idx_pro_registrations_track ON pro_registrations(track_id);
CREATE INDEX idx_pro_registrations_artist ON pro_registrations(artist_id);
CREATE INDEX idx_pro_registrations_status ON pro_registrations(registration_status);
CREATE INDEX idx_pro_registrations_pro ON pro_registrations(pro_name);
CREATE UNIQUE INDEX idx_pro_registrations_unique ON pro_registrations(track_id, pro_name, registration_number) WHERE registration_number IS NOT NULL;

-- Metadata Validation Log
CREATE TABLE metadata_validation_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Entity being validated
    entity_type VARCHAR(50) CHECK (entity_type IN ('artist', 'release', 'track', 'identifier')),
    entity_id UUID NOT NULL,
    
    -- Validation Details
    field_name VARCHAR(100) NOT NULL,
    validation_type VARCHAR(50) CHECK (validation_type IN ('format', 'completeness', 'uniqueness', 'standard_compliance')),
    
    -- Results
    is_valid BOOLEAN NOT NULL,
    error_message TEXT,
    suggested_fix TEXT,
    
    -- Original vs Fixed Values
    original_value TEXT,
    corrected_value TEXT,
    
    -- Status
    fixed_at TIMESTAMP WITH TIME ZONE,
    fixed_by UUID, -- User who fixed it
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Index for lookups
CREATE INDEX idx_metadata_validation_entity ON metadata_validation_log(entity_type, entity_id);
CREATE INDEX idx_metadata_validation_valid ON metadata_validation_log(is_valid);

-- Apply triggers to all new tables with updated_at
CREATE TRIGGER update_publishing_agreements_updated_at BEFORE UPDATE ON publishing_agreements FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_royalty_accruals_updated_at BEFORE UPDATE ON royalty_accruals FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_split_sheets_updated_at BEFORE UPDATE ON split_sheets FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_pro_registrations_updated_at BEFORE UPDATE ON pro_registrations FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
