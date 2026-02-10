'use client'

import { useState } from 'react'
import { useRouter } from 'next/navigation'
import Link from 'next/link'

export default function NewProfilePage() {
  const navigation = useRouter()
  const [formData, setFormData] = useState({
    legalFullName: '',
    performanceName: '',
    dateOfBirth: '',
    contactEmail: '',
    interestedPartyInfo: '',
    standardNameId: '',
    musicalWorkCode: '',
    digitalDataExchangeId: '',
    universalProductNum: '',
    employerIdNumber: '',
  })

  const [isSubmitting, setIsSubmitting] = useState(false)

  const handleFormSubmit = async (event: React.FormEvent) => {
    event.preventDefault()
    setIsSubmitting(true)

    try {
      const response = await fetch('/api/profiles', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(formData),
      })

      if (response.ok) {
        const createdProfile = await response.json()
        navigation.push(`/profiles/${createdProfile.profileId}`)
      } else {
        alert('Failed to create profile. Please try again.')
      }
    } catch (error) {
      console.error('Submission error:', error)
      alert('An error occurred. Please try again.')
    } finally {
      setIsSubmitting(false)
    }
  }

  const updateField = (fieldName: string, value: string) => {
    setFormData(prev => ({ ...prev, [fieldName]: value }))
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-purple-50 to-blue-50 p-8">
      <div className="max-w-3xl mx-auto">
        <div className="mb-6">
          <Link href="/" className="text-purple-600 hover:text-purple-800">
            ← Back to Home
          </Link>
        </div>

        <div className="bg-white rounded-xl shadow-lg p-8">
          <h1 className="text-4xl font-bold text-gray-900 mb-2">
            Create Artist Profile
          </h1>
          <p className="text-gray-600 mb-8">
            Your digital identity in the music industry. Start with basics, add codes as you grow.
          </p>

          <form onSubmit={handleFormSubmit} className="space-y-6">
            <section className="border-b border-gray-200 pb-6">
              <h2 className="text-2xl font-semibold text-gray-800 mb-4">
                Basic Information
              </h2>
              
              <div className="space-y-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Legal Full Name <span className="text-red-500">*</span>
                  </label>
                  <input
                    type="text"
                    value={formData.legalFullName}
                    onChange={(e) => updateField('legalFullName', e.target.value)}
                    required
                    className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-purple-500 focus:border-transparent"
                    placeholder="Enter your legal name"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Stage/Performance Name
                  </label>
                  <input
                    type="text"
                    value={formData.performanceName}
                    onChange={(e) => updateField('performanceName', e.target.value)}
                    className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-purple-500 focus:border-transparent"
                    placeholder="Your artist name (optional)"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Date of Birth
                  </label>
                  <input
                    type="date"
                    value={formData.dateOfBirth}
                    onChange={(e) => updateField('dateOfBirth', e.target.value)}
                    className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-purple-500 focus:border-transparent"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Contact Email <span className="text-red-500">*</span>
                  </label>
                  <input
                    type="email"
                    value={formData.contactEmail}
                    onChange={(e) => updateField('contactEmail', e.target.value)}
                    required
                    className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-purple-500 focus:border-transparent"
                    placeholder="your@email.com"
                  />
                </div>
              </div>
            </section>

            <section className="border-b border-gray-200 pb-6">
              <h2 className="text-2xl font-semibold text-gray-800 mb-2">
                Professional Codes
              </h2>
              <p className="text-sm text-gray-600 mb-4">
                Add these as you obtain them. They help establish your professional identity.
              </p>
              
              <div className="grid md:grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    IPI Code
                    <span className="text-xs text-gray-500 ml-2">(Interested Party Info)</span>
                  </label>
                  <input
                    type="text"
                    value={formData.interestedPartyInfo}
                    onChange={(e) => updateField('interestedPartyInfo', e.target.value)}
                    className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-purple-500 focus:border-transparent"
                    placeholder="e.g., 00123456789"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    ISNI Code
                    <span className="text-xs text-gray-500 ml-2">(Standard Name ID)</span>
                  </label>
                  <input
                    type="text"
                    value={formData.standardNameId}
                    onChange={(e) => updateField('standardNameId', e.target.value)}
                    className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-purple-500 focus:border-transparent"
                    placeholder="e.g., 0000 0001 2345 6789"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    ISWC Code
                    <span className="text-xs text-gray-500 ml-2">(Musical Work Code)</span>
                  </label>
                  <input
                    type="text"
                    value={formData.musicalWorkCode}
                    onChange={(e) => updateField('musicalWorkCode', e.target.value)}
                    className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-purple-500 focus:border-transparent"
                    placeholder="e.g., T-123.456.789-0"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    DDEX ID
                    <span className="text-xs text-gray-500 ml-2">(Digital Data Exchange)</span>
                  </label>
                  <input
                    type="text"
                    value={formData.digitalDataExchangeId}
                    onChange={(e) => updateField('digitalDataExchangeId', e.target.value)}
                    className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-purple-500 focus:border-transparent"
                    placeholder="DDEX identifier"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    UPN
                    <span className="text-xs text-gray-500 ml-2">(Universal Product Number)</span>
                  </label>
                  <input
                    type="text"
                    value={formData.universalProductNum}
                    onChange={(e) => updateField('universalProductNum', e.target.value)}
                    className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-purple-500 focus:border-transparent"
                    placeholder="Universal product number"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    EIN
                    <span className="text-xs text-gray-500 ml-2">(Employer ID Number)</span>
                  </label>
                  <input
                    type="text"
                    value={formData.employerIdNumber}
                    onChange={(e) => updateField('employerIdNumber', e.target.value)}
                    className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-purple-500 focus:border-transparent"
                    placeholder="e.g., 12-3456789"
                  />
                </div>
              </div>
            </section>

            <div className="flex gap-4">
              <button
                type="submit"
                disabled={isSubmitting}
                className="flex-1 bg-purple-600 text-white px-6 py-3 rounded-lg font-semibold hover:bg-purple-700 transition-colors disabled:bg-gray-400 disabled:cursor-not-allowed"
              >
                {isSubmitting ? 'Creating Profile...' : 'Create Profile'}
              </button>
              <Link
                href="/"
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
