import { ArtistProfileForm } from '@/components/ArtistProfileForm';

export default function CreateProfilePage() {
  return (
    <div className="py-12 bg-gray-50 min-h-screen">
      <div className="strezless-container">
        <div className="text-center mb-8">
          <h1 className="text-4xl font-bold text-strezless-dark mb-4">
            Launch Your Artist Profile
          </h1>
          <p className="text-lg text-gray-600 max-w-2xl mx-auto">
            Set up your professional music profile with industry-standard codes and start managing your catalog
          </p>
        </div>
        <ArtistProfileForm />
      </div>
    </div>
  );
}
