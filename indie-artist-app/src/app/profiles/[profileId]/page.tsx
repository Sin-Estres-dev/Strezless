'use client'

import { useEffect, useState } from 'react'
import { useParams } from 'next/navigation'
import Link from 'next/link'

interface ProfileDetails {
  profileId: string
  legalFullName: string
  performanceName: string | null
  dateOfBirth: string | null
  contactEmail: string
  interestedPartyInfo: string | null
  standardNameId: string | null
  musicalWorkCode: string | null
  digitalDataExchangeId: string | null
  universalProductNum: string | null
  employerIdNumber: string | null
  internalUniqueId: string
  trackRecords: TrackDetails[]
}

interface TrackDetails {
  recordId: string
  trackTitle: string
  albumTitle: string | null
  durationSeconds: number
  musicGenre: string | null
  universalProdCode: string | null
  intlStandardRecordingCode: string | null
}

export default function ProfileDetailPage() {
  const params = useParams()
  const [profileData, setProfileData] = useState<ProfileDetails | null>(null)
  const [isLoading, setIsLoading] = useState(true)

  useEffect(() => {
    async function loadProfileData() {
      try {
        const response = await fetch(`/api/profiles/${params.profileId}`)
        if (response.ok) {
          const data = await response.json()
          setProfileData(data)
        }
      } catch (err) {
        console.error('Error loading profile:', err)
      } finally {
        setIsLoading(false)
      }
    }

    if (params.profileId) {
      loadProfileData()
    }
  }, [params.profileId])

  const formatDuration = (seconds: number) => {
    const mins = Math.floor(seconds / 60)
    const secs = seconds % 60
    return `${mins}:${secs.toString().padStart(2, '0')}`
  }

  if (isLoading) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-purple-50 to-blue-50 p-8">
        <div className="max-w-5xl mx-auto">
          <p className="text-center text-gray-600">Loading profile...</p>
        </div>
      </div>
    )
  }

  if (!profileData) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-purple-50 to-blue-50 p-8">
        <div className="max-w-5xl mx-auto">
          <p className="text-center text-gray-600">Profile not found</p>
          <div className="text-center mt-4">
            <Link href="/" className="text-purple-600 hover:text-purple-800">
              Return to Home
            </Link>
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-purple-50 to-blue-50 p-8">
      <div className="max-w-5xl mx-auto">
        <div className="mb-6">
          <Link href="/" className="text-purple-600 hover:text-purple-800">
            ← Back to Home
          </Link>
        </div>

        <div className="bg-white rounded-xl shadow-lg p-8 mb-6">
          <div className="flex justify-between items-start mb-6">
            <div>
              <h1 className="text-4xl font-bold text-gray-900">
                {profileData.performanceName || profileData.legalFullName}
              </h1>
              {profileData.performanceName && (
                <p className="text-gray-600 mt-1">Legal Name: {profileData.legalFullName}</p>
              )}
            </div>
            <Link
              href={`/tracks/new?artistId=${profileData.profileId}`}
              className="bg-green-600 text-white px-6 py-2 rounded-lg font-semibold hover:bg-green-700 transition-colors"
            >
              Add Track
            </Link>
          </div>

          <div className="grid md:grid-cols-2 gap-6 mb-8">
            <div>
              <h3 className="text-sm font-semibold text-gray-500 uppercase mb-2">Contact</h3>
              <p className="text-gray-900">{profileData.contactEmail}</p>
              {profileData.dateOfBirth && (
                <p className="text-gray-600 text-sm mt-1">
                  Born: {new Date(profileData.dateOfBirth).toLocaleDateString()}
                </p>
              )}
            </div>

            <div>
              <h3 className="text-sm font-semibold text-gray-500 uppercase mb-2">System ID</h3>
              <p className="text-gray-600 text-xs font-mono">{profileData.internalUniqueId}</p>
            </div>
          </div>

          <div className="border-t border-gray-200 pt-6">
            <h2 className="text-2xl font-semibold text-gray-800 mb-4">
              Professional Codes
            </h2>
            <div className="grid md:grid-cols-3 gap-4">
              {profileData.interestedPartyInfo && (
                <div className="bg-purple-50 p-4 rounded-lg">
                  <p className="text-xs font-semibold text-purple-700 uppercase">IPI Code</p>
                  <p className="text-lg font-mono text-gray-900 mt-1">
                    {profileData.interestedPartyInfo}
                  </p>
                </div>
              )}
              {profileData.standardNameId && (
                <div className="bg-blue-50 p-4 rounded-lg">
                  <p className="text-xs font-semibold text-blue-700 uppercase">ISNI Code</p>
                  <p className="text-lg font-mono text-gray-900 mt-1">
                    {profileData.standardNameId}
                  </p>
                </div>
              )}
              {profileData.musicalWorkCode && (
                <div className="bg-green-50 p-4 rounded-lg">
                  <p className="text-xs font-semibold text-green-700 uppercase">ISWC Code</p>
                  <p className="text-lg font-mono text-gray-900 mt-1">
                    {profileData.musicalWorkCode}
                  </p>
                </div>
              )}
              {profileData.digitalDataExchangeId && (
                <div className="bg-yellow-50 p-4 rounded-lg">
                  <p className="text-xs font-semibold text-yellow-700 uppercase">DDEX ID</p>
                  <p className="text-lg font-mono text-gray-900 mt-1">
                    {profileData.digitalDataExchangeId}
                  </p>
                </div>
              )}
              {profileData.universalProductNum && (
                <div className="bg-pink-50 p-4 rounded-lg">
                  <p className="text-xs font-semibold text-pink-700 uppercase">UPN</p>
                  <p className="text-lg font-mono text-gray-900 mt-1">
                    {profileData.universalProductNum}
                  </p>
                </div>
              )}
              {profileData.employerIdNumber && (
                <div className="bg-indigo-50 p-4 rounded-lg">
                  <p className="text-xs font-semibold text-indigo-700 uppercase">EIN</p>
                  <p className="text-lg font-mono text-gray-900 mt-1">
                    {profileData.employerIdNumber}
                  </p>
                </div>
              )}
            </div>
            {!profileData.interestedPartyInfo && 
             !profileData.standardNameId && 
             !profileData.musicalWorkCode &&
             !profileData.digitalDataExchangeId &&
             !profileData.universalProductNum &&
             !profileData.employerIdNumber && (
              <p className="text-gray-500 italic">
                No professional codes added yet. These can be added as you obtain them.
              </p>
            )}
          </div>
        </div>

        <div className="bg-white rounded-xl shadow-lg p-8">
          <h2 className="text-3xl font-bold text-gray-800 mb-6">Music Catalog</h2>
          
          {profileData.trackRecords.length === 0 ? (
            <div className="text-center py-12">
              <p className="text-gray-500 mb-4">No tracks in catalog yet</p>
              <Link
                href={`/tracks/new?artistId=${profileData.profileId}`}
                className="inline-block bg-green-600 text-white px-6 py-2 rounded-lg font-semibold hover:bg-green-700 transition-colors"
              >
                Add Your First Track
              </Link>
            </div>
          ) : (
            <div className="space-y-4">
              {profileData.trackRecords.map(track => (
                <div 
                  key={track.recordId}
                  className="border border-gray-200 rounded-lg p-5 hover:border-purple-400 hover:shadow-md transition-all"
                >
                  <div className="flex justify-between items-start">
                    <div className="flex-1">
                      <h3 className="text-xl font-semibold text-gray-900">
                        {track.trackTitle}
                      </h3>
                      <div className="flex gap-4 mt-2 text-sm text-gray-600">
                        {track.albumTitle && (
                          <span>Album: {track.albumTitle}</span>
                        )}
                        <span>Duration: {formatDuration(track.durationSeconds)}</span>
                        {track.musicGenre && (
                          <span>Genre: {track.musicGenre}</span>
                        )}
                      </div>
                      <div className="flex gap-3 mt-3 flex-wrap">
                        {track.intlStandardRecordingCode && (
                          <span className="bg-blue-100 text-blue-800 px-3 py-1 rounded-full text-xs font-medium">
                            ISRC: {track.intlStandardRecordingCode}
                          </span>
                        )}
                        {track.universalProdCode && (
                          <span className="bg-green-100 text-green-800 px-3 py-1 rounded-full text-xs font-medium">
                            UPC: {track.universalProdCode}
                          </span>
                        )}
                      </div>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
