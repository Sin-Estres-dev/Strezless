-- CreateTable
CREATE TABLE "artist_profiles" (
    "profileId" TEXT NOT NULL PRIMARY KEY,
    "recordCreated" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "lastModified" DATETIME NOT NULL,
    "legalFullName" TEXT NOT NULL,
    "performanceName" TEXT,
    "dateOfBirth" DATETIME,
    "contactEmail" TEXT NOT NULL,
    "interestedPartyInfo" TEXT,
    "standardNameId" TEXT,
    "musicalWorkCode" TEXT,
    "digitalDataExchangeId" TEXT,
    "universalProductNum" TEXT,
    "employerIdNumber" TEXT,
    "internalUniqueId" TEXT NOT NULL
);

-- CreateTable
CREATE TABLE "track_records" (
    "recordId" TEXT NOT NULL PRIMARY KEY,
    "addedOn" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedOn" DATETIME NOT NULL,
    "trackTitle" TEXT NOT NULL,
    "albumTitle" TEXT,
    "durationSeconds" INTEGER NOT NULL,
    "musicGenre" TEXT,
    "universalProdCode" TEXT,
    "universalProdNumber" TEXT,
    "intlStandardRecordingCode" TEXT,
    "dataExchangeMetadata" TEXT,
    "belongsToArtist" TEXT NOT NULL,
    CONSTRAINT "track_records_belongsToArtist_fkey" FOREIGN KEY ("belongsToArtist") REFERENCES "artist_profiles" ("profileId") ON DELETE RESTRICT ON UPDATE CASCADE
);

-- CreateIndex
CREATE UNIQUE INDEX "artist_profiles_contactEmail_key" ON "artist_profiles"("contactEmail");

-- CreateIndex
CREATE UNIQUE INDEX "artist_profiles_interestedPartyInfo_key" ON "artist_profiles"("interestedPartyInfo");

-- CreateIndex
CREATE UNIQUE INDEX "artist_profiles_standardNameId_key" ON "artist_profiles"("standardNameId");

-- CreateIndex
CREATE UNIQUE INDEX "artist_profiles_musicalWorkCode_key" ON "artist_profiles"("musicalWorkCode");

-- CreateIndex
CREATE UNIQUE INDEX "artist_profiles_employerIdNumber_key" ON "artist_profiles"("employerIdNumber");

-- CreateIndex
CREATE UNIQUE INDEX "artist_profiles_internalUniqueId_key" ON "artist_profiles"("internalUniqueId");
