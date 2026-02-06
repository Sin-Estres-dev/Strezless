'use client';

import { useState } from 'react';
import { useForm } from 'react-hook-form';
import { zodResolver } from '@hookform/resolvers/zod';
import { createArtistProfileSchema, type CreateArtistInput } from '@/lib/validators/music-industry-codes';

interface ArtistProfileFormProps {
  onSuccess?: (artistData: any) => void;
  initialData?: Partial<CreateArtistInput>;
  mode?: 'create' | 'edit';
  artistId?: string;
}

export function ArtistProfileForm({ 
  onSuccess, 
  initialData, 
  mode = 'create',
  artistId 
}: ArtistProfileFormProps) {
  const [submitState, setSubmitState] = useState<'idle' | 'submitting' | 'success' | 'error'>('idle');
  const [errorMessage, setErrorMessage] = useState<string>('');
  const [showProfessionalCodes, setShowProfessionalCodes] = useState(false);
  const [showAddressFields, setShowAddressFields] = useState(false);

  const {
    register,
    handleSubmit,
    formState: { errors, isDirty },
    reset,
  } = useForm<CreateArtistInput>({
    resolver: zodResolver(createArtistProfileSchema),
    defaultValues: initialData || {},
  });

  const submitArtistProfile = async (formData: CreateArtistInput) => {
    setSubmitState('submitting');
    setErrorMessage('');

    try {
      const apiEndpoint = mode === 'edit' && artistId
        ? `/api/artists/${artistId}`
        : '/api/artists';
      
      const httpMethod = mode === 'edit' ? 'PUT' : 'POST';

      const response = await fetch(apiEndpoint, {
        method: httpMethod,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(formData),
      });

      const result = await response.json();

      if (!response.ok) {
        throw new Error(result.error || 'Failed to save artist profile');
      }

      setSubmitState('success');
      
      if (mode === 'create') {
        reset();
      }
      
      if (onSuccess) {
        onSuccess(result.data);
      }

    } catch (err) {
      setSubmitState('error');
      setErrorMessage(err instanceof Error ? err.message : 'An unexpected error occurred');
    }
  };

  return (
    <div className="max-w-4xl mx-auto p-6 bg-white rounded-lg shadow-md">
      <h2 className="text-3xl font-bold text-strezless-dark mb-6">
        {mode === 'create' ? 'Create Artist Profile' : 'Edit Artist Profile'}
      </h2>

      <form onSubmit={handleSubmit(submitArtistProfile)} className="space-y-6">
        {/* Basic Information Section */}
        <section className="border-b border-gray-200 pb-6">
          <h3 className="text-xl font-semibold text-strezless-dark mb-4">Basic Information</h3>
          
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <label htmlFor="stageName" className="block text-sm font-medium text-gray-700 mb-1">
                Stage Name *
              </label>
              <input
                id="stageName"
                type="text"
                {...register('stageName')}
                className="w-full px-4 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-strezless-primary focus:border-transparent"
                placeholder="Sin Estres"
              />
              {errors.stageName && (
                <p className="mt-1 text-sm text-red-600">{errors.stageName.message}</p>
              )}
            </div>

            <div>
              <label htmlFor="legalName" className="block text-sm font-medium text-gray-700 mb-1">
                Legal Name *
              </label>
              <input
                id="legalName"
                type="text"
                {...register('legalName')}
                className="w-full px-4 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-strezless-primary focus:border-transparent"
                placeholder="Omar Orrantia"
              />
              {errors.legalName && (
                <p className="mt-1 text-sm text-red-600">{errors.legalName.message}</p>
              )}
            </div>
          </div>

          <div className="mt-4">
            <label htmlFor="email" className="block text-sm font-medium text-gray-700 mb-1">
              Email Address *
            </label>
            <input
              id="email"
              type="email"
              {...register('email')}
              className="w-full px-4 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-strezless-primary focus:border-transparent"
              placeholder="artist@strezless.com"
            />
            {errors.email && (
              <p className="mt-1 text-sm text-red-600">{errors.email.message}</p>
            )}
          </div>

          <div className="mt-4">
            <label htmlFor="biography" className="block text-sm font-medium text-gray-700 mb-1">
              Biography
            </label>
            <textarea
              id="biography"
              rows={4}
              {...register('biography')}
              className="w-full px-4 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-strezless-primary focus:border-transparent"
              placeholder="Tell your story..."
            />
            {errors.biography && (
              <p className="mt-1 text-sm text-red-600">{errors.biography.message}</p>
            )}
          </div>
        </section>

        {/* Professional Codes Section */}
        <section className="border-b border-gray-200 pb-6">
          <button
            type="button"
            onClick={() => setShowProfessionalCodes(!showProfessionalCodes)}
            className="flex items-center justify-between w-full text-left"
          >
            <h3 className="text-xl font-semibold text-strezless-dark">
              Professional Industry Codes
            </h3>
            <span className="text-2xl text-strezless-primary">
              {showProfessionalCodes ? '−' : '+'}
            </span>
          </button>

          {showProfessionalCodes && (
            <div className="mt-4 grid grid-cols-1 md:grid-cols-3 gap-4">
              <div>
                <label htmlFor="ipiCode" className="block text-sm font-medium text-gray-700 mb-1">
                  IPI Code
                </label>
                <input
                  id="ipiCode"
                  type="text"
                  {...register('ipiCode')}
                  className="w-full px-4 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-strezless-primary focus:border-transparent"
                  placeholder="123456789"
                />
                {errors.ipiCode && (
                  <p className="mt-1 text-sm text-red-600">{errors.ipiCode.message}</p>
                )}
              </div>

              <div>
                <label htmlFor="isniCode" className="block text-sm font-medium text-gray-700 mb-1">
                  ISNI Code
                </label>
                <input
                  id="isniCode"
                  type="text"
                  {...register('isniCode')}
                  className="w-full px-4 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-strezless-primary focus:border-transparent"
                  placeholder="0000000123456789"
                />
                {errors.isniCode && (
                  <p className="mt-1 text-sm text-red-600">{errors.isniCode.message}</p>
                )}
              </div>

              <div>
                <label htmlFor="einNumber" className="block text-sm font-medium text-gray-700 mb-1">
                  EIN Number
                </label>
                <input
                  id="einNumber"
                  type="text"
                  {...register('einNumber')}
                  className="w-full px-4 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-strezless-primary focus:border-transparent"
                  placeholder="12-3456789"
                />
                {errors.einNumber && (
                  <p className="mt-1 text-sm text-red-600">{errors.einNumber.message}</p>
                )}
              </div>
            </div>
          )}
        </section>

        {/* Contact Information Section */}
        <section className="border-b border-gray-200 pb-6">
          <h3 className="text-xl font-semibold text-strezless-dark mb-4">Contact Information</h3>
          
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <label htmlFor="contactEmail" className="block text-sm font-medium text-gray-700 mb-1">
                Contact Email
              </label>
              <input
                id="contactEmail"
                type="email"
                {...register('contactEmail')}
                className="w-full px-4 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-strezless-primary focus:border-transparent"
              />
              {errors.contactEmail && (
                <p className="mt-1 text-sm text-red-600">{errors.contactEmail.message}</p>
              )}
            </div>

            <div>
              <label htmlFor="phoneNumber" className="block text-sm font-medium text-gray-700 mb-1">
                Phone Number
              </label>
              <input
                id="phoneNumber"
                type="tel"
                {...register('phoneNumber')}
                className="w-full px-4 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-strezless-primary focus:border-transparent"
              />
              {errors.phoneNumber && (
                <p className="mt-1 text-sm text-red-600">{errors.phoneNumber.message}</p>
              )}
            </div>
          </div>

          <div className="mt-4">
            <label htmlFor="websiteUrl" className="block text-sm font-medium text-gray-700 mb-1">
              Website URL
            </label>
            <input
              id="websiteUrl"
              type="url"
              {...register('websiteUrl')}
              className="w-full px-4 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-strezless-primary focus:border-transparent"
              placeholder="https://strezless.com"
            />
            {errors.websiteUrl && (
              <p className="mt-1 text-sm text-red-600">{errors.websiteUrl.message}</p>
            )}
          </div>

          <button
            type="button"
            onClick={() => setShowAddressFields(!showAddressFields)}
            className="mt-4 text-sm text-strezless-primary hover:text-strezless-secondary font-medium"
          >
            {showAddressFields ? 'Hide' : 'Add'} Mailing Address
          </button>

          {showAddressFields && (
            <div className="mt-4 space-y-4">
              <div>
                <label htmlFor="addressLine1" className="block text-sm font-medium text-gray-700 mb-1">
                  Address Line 1
                </label>
                <input
                  id="addressLine1"
                  type="text"
                  {...register('addressLine1')}
                  className="w-full px-4 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-strezless-primary focus:border-transparent"
                />
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <label htmlFor="city" className="block text-sm font-medium text-gray-700 mb-1">
                    City
                  </label>
                  <input
                    id="city"
                    type="text"
                    {...register('city')}
                    className="w-full px-4 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-strezless-primary focus:border-transparent"
                  />
                </div>

                <div>
                  <label htmlFor="stateProvince" className="block text-sm font-medium text-gray-700 mb-1">
                    State/Province
                  </label>
                  <input
                    id="stateProvince"
                    type="text"
                    {...register('stateProvince')}
                    className="w-full px-4 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-strezless-primary focus:border-transparent"
                  />
                </div>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <label htmlFor="postalCode" className="block text-sm font-medium text-gray-700 mb-1">
                    Postal Code
                  </label>
                  <input
                    id="postalCode"
                    type="text"
                    {...register('postalCode')}
                    className="w-full px-4 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-strezless-primary focus:border-transparent"
                  />
                </div>

                <div>
                  <label htmlFor="country" className="block text-sm font-medium text-gray-700 mb-1">
                    Country
                  </label>
                  <input
                    id="country"
                    type="text"
                    {...register('country')}
                    className="w-full px-4 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-strezless-primary focus:border-transparent"
                  />
                </div>
              </div>
            </div>
          )}
        </section>

        {/* Submit Section */}
        <div className="flex items-center justify-between pt-4">
          {submitState === 'error' && (
            <p className="text-sm text-red-600">{errorMessage}</p>
          )}
          {submitState === 'success' && (
            <p className="text-sm text-green-600">
              Artist profile {mode === 'create' ? 'created' : 'updated'} successfully!
            </p>
          )}
          
          <div className="ml-auto flex gap-3">
            {mode === 'edit' && (
              <button
                type="button"
                onClick={() => reset(initialData)}
                disabled={!isDirty || submitState === 'submitting'}
                className="px-6 py-2 border border-gray-300 rounded-md text-gray-700 hover:bg-gray-50 disabled:opacity-50"
              >
                Reset
              </button>
            )}
            <button
              type="submit"
              disabled={submitState === 'submitting'}
              className="px-6 py-2 bg-strezless-primary text-white rounded-md hover:bg-strezless-secondary disabled:opacity-50 transition-colors"
            >
              {submitState === 'submitting' 
                ? 'Saving...' 
                : mode === 'create' 
                  ? 'Create Profile' 
                  : 'Update Profile'}
            </button>
          </div>
        </div>
      </form>
    </div>
  );
}
