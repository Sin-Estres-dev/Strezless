'use client'

import { Suspense, useState } from 'react'
import { useRouter, useSearchParams } from 'next/navigation'
import Link from 'next/link'

function TrackForm() {
  const navigation = useRouter()
  const searchParams = useSearchParams()
  const artistId = searchParams.get('artistId')

  const [trackFormData, setTrackFormData] = useState({
    trackTitle: '',
    albumTitle: '',
    durationSeconds: '',
    musicGenre: '',
    universalProdCode: '',
    universalProdNumber: '',
    intlStandardRecordingCode: '',
    dataExchangeMetadata: '',
  })

  const [isSubmitting, setIsSubmitting] = useState(false)

  const handleSubmitTrack = async (event: React.FormEvent) => {
    event.preventDefault()
    
    if (!artistId) {
      alert('Artist ID is required')
      return
    }

    setIsSubmitting(true)

    try {
      const response = await fetch('/api/tracks', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          ...trackFormData,
          belongsToArtist: artistId,
        }),
      })

      if (response.ok) {
        navigation.push(`/profiles/${artistId}`)
      } else {
        alert('Failed to add track. Please try again.')
      }
    } catch (error) {
      console.error('Track submission error:', error)
      alert('An error occurred. Please try again.')
    } finally {
      setIsSubmitting(false)
    }
  }

  const updateTrackField = (fieldName: string, value: string) => {
    setTrackFormData(prev => ({ ...prev, [fieldName]: value }))
  }

  if (!artistId) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-purple-50 to-blue-50 p-8">
        <div className="max-w-3xl mx-auto text-center">
          <p className="text-gray-600 mb-4">Invalid request - Artist ID required</p>
          <Link href="/" className="text-purple-600 hover:text-purple-800">
            Return to Home
          </Link>
        </div>
      </div>
    )
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-purple-50 to-blue-50 p-8">
      <div className="max-w-3xl mx-auto">
        <div className="mb-6">
          <Link 
            href={`/profiles/${artistId}`} 
            className="text-purple-600 hover:text-purple-800"
          >
            ← Back to Profile
          </Link>
        </div>

        <div className="bg-white rounded-xl shadow-lg p-8">
          <h1 className="text-4xl font-bold text-gray-900 mb-2">
            Add New Track
          </h1>
          <p className="text-gray-600 mb-8">
            Add a track to your music catalog with all the important metadata and codes.
          </p>

          <form onSubmit={handleSubmitTrack} className="space-y-6">
            <section className="border-b border-gray-200 pb-6">
              <h2 className="text-2xl font-semibold text-gray-800 mb-4">
                Track Information
              </h2>
              
              <div className="space-y-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Track Title <span className="text-red-500">*</span>
                  </label>
                  <input
                    type="text"
                    value={trackFormData.trackTitle}
                    onChange={(e) => updateTrackField('trackTitle', e.target.value)}
                    required
                    className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-green-500 focus:border-transparent"
                    placeholder="Name of the track"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Album Title
                  </label>
                  <input
                    type="text"
                    value={trackFormData.albumTitle}
                    onChange={(e) => updateTrackField('albumTitle', e.target.value)}
                    className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-green-500 focus:border-transparent"
                    placeholder="Album name (if applicable)"
                  />
                </div>

                <div className="grid md:grid-cols-2 gap-4">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      Duration (seconds) <span className="text-red-500">*</span>
                    </label>
                    <input
                      type="number"
                      value={trackFormData.durationSeconds}
                      onChange={(e) => updateTrackField('durationSeconds', e.target.value)}
                      required
                      min="1"
                      className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-green-500 focus:border-transparent"
                      placeholder="e.g., 180"
                    />
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      Genre
                    </label>
                    <input
                      type="text"
                      value={trackFormData.musicGenre}
                      onChange={(e) => updateTrackField('musicGenre', e.target.value)}
                      className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-green-500 focus:border-transparent"
                      placeholder="e.g., Hip Hop, Rock, Jazz"
                    />
                  </div>
                </div>
              </div>
            </section>

            <section className="border-b border-gray-200 pb-6">
              <h2 className="text-2xl font-semibold text-gray-800 mb-2">
                Track Codes & Metadata
              </h2>
              <p className="text-sm text-gray-600 mb-4">
                These codes are essential for distribution and royalty tracking.
              </p>
              
              <div className="space-y-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    ISRC
                    <span className="text-xs text-gray-500 ml-2">
                      (International Standard Recording Code)
                    </span>
                  </label>
                  <input
                    type="text"
                    value={trackFormData.intlStandardRecordingCode}
                    onChange={(e) => updateTrackField('intlStandardRecordingCode', e.target.value)}
                    className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-green-500 focus:border-transparent"
                    placeholder="e.g., USRC17607839"
                  />
                  <p className="text-xs text-gray-500 mt-1">
                    Every track needs a unique ISRC for distribution
                  </p>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    UPC
                    <span className="text-xs text-gray-500 ml-2">
                      (Universal Product Code)
                    </span>
                  </label>
                  <input
                    type="text"
                    value={trackFormData.universalProdCode}
                    onChange={(e) => updateTrackField('universalProdCode', e.target.value)}
                    className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-green-500 focus:border-transparent"
                    placeholder="e.g., 123456789012"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    UPN
                    <span className="text-xs text-gray-500 ml-2">
                      (Universal Product Number)
                    </span>
                  </label>
                  <input
                    type="text"
                    value={trackFormData.universalProdNumber}
                    onChange={(e) => updateTrackField('universalProdNumber', e.target.value)}
                    className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-green-500 focus:border-transparent"
                    placeholder="Universal product number"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    DDEX Metadata
                    <span className="text-xs text-gray-500 ml-2">
                      (Digital Data Exchange format info)
                    </span>
                  </label>
                  <input
                    type="text"
                    value={trackFormData.dataExchangeMetadata}
                    onChange={(e) => updateTrackField('dataExchangeMetadata', e.target.value)}
                    className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-green-500 focus:border-transparent"
                    placeholder="DDEX compatibility info"
                  />
                </div>
              </div>
            </section>

            <div className="flex gap-4">
              <button
                type="submit"
                disabled={isSubmitting}
                className="flex-1 bg-green-600 text-white px-6 py-3 rounded-lg font-semibold hover:bg-green-700 transition-colors disabled:bg-gray-400 disabled:cursor-not-allowed"
              >
                {isSubmitting ? 'Adding Track...' : 'Add Track to Catalog'}
              </button>
              <Link
                href={`/profiles/${artistId}`}
                className="px-6 py-3 border border-gray-300 rounded-lg font-semibold hover:bg-gray-50 transition-colors text-center"
              >
                Cancel
              </Link>
            </div>
          </form>
        </div>
      </div>
    </div>
  )
}

export default function NewTrackPage() {
  return (
    <Suspense fallback={
      <div className="min-h-screen bg-gradient-to-br from-purple-50 to-blue-50 p-8">
        <div className="max-w-3xl mx-auto text-center">
          <p className="text-gray-600">Loading...</p>
        </div>
      </div>
    }>
      <TrackForm />
    </Suspense>
  )
}
