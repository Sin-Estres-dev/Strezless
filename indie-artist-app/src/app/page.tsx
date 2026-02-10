'use client'

import { useEffect, useState } from 'react'
import Link from 'next/link'

interface ProfileData {
  profileId: string
  legalFullName: string
  performanceName: string | null
  contactEmail: string
}

export default function HomePage() {
  const [profilesList, setProfilesList] = useState<ProfileData[]>([])
  const [isLoadingData, setIsLoadingData] = useState(true)

  useEffect(() => {
    async function fetchProfilesData() {
      try {
        const response = await fetch('/api/profiles')
        const profilesData = await response.json()
        setProfilesList(profilesData)
      } catch (err) {
        console.error('Failed to load profiles:', err)
      } finally {
        setIsLoadingData(false)
      }
    }
    fetchProfilesData()
  }, [])

  return (
    <div className="min-h-screen bg-gradient-to-br from-purple-50 to-blue-50 p-8">
      <div className="max-w-5xl mx-auto">
        <header className="mb-12 text-center">
          <h1 className="text-5xl font-bold text-gray-900 mb-3">
            Indie Artist Hub
          </h1>
          <p className="text-lg text-gray-600 mb-8">
            Manage your music career - profiles, codes, and catalog all in one place
          </p>
          <Link 
            href="/profiles/new" 
            className="inline-block bg-purple-600 text-white px-8 py-3 rounded-lg font-semibold hover:bg-purple-700 transition-colors shadow-lg"
          >
            Create Your Artist Profile
          </Link>
        </header>

        <section className="bg-white rounded-xl shadow-lg p-8">
          <h2 className="text-3xl font-bold text-gray-800 mb-6">Artist Profiles</h2>
          
          {isLoadingData ? (
            <p className="text-gray-500">Loading profiles...</p>
          ) : profilesList.length === 0 ? (
            <p className="text-gray-500">No artists yet. Create your first profile to get started!</p>
          ) : (
            <div className="grid gap-4">
              {profilesList.map(profile => (
                <Link
                  key={profile.profileId}
                  href={`/profiles/${profile.profileId}`}
                  className="block p-5 border border-gray-200 rounded-lg hover:border-purple-400 hover:shadow-md transition-all"
                >
                  <h3 className="text-xl font-semibold text-gray-900">
                    {profile.performanceName || profile.legalFullName}
                  </h3>
                  <p className="text-gray-600 mt-1">{profile.contactEmail}</p>
                </Link>
              ))}
            </div>
          )}
        </section>

        <section className="mt-8 bg-blue-50 border-l-4 border-blue-500 p-6 rounded">
          <h3 className="text-lg font-semibold text-blue-900 mb-2">
            What You Can Do:
          </h3>
          <ul className="space-y-2 text-blue-800">
            <li>✓ Store IPI, ISNI, ISRC, ISWC, DDEX, UPC, UPN, EIN codes</li>
            <li>✓ Manage your music catalog with proper metadata</li>
            <li>✓ Track your professional artist identity</li>
            <li>✓ Keep all music business info organized</li>
          </ul>
        </section>
      </div>
    </div>
  )
}
