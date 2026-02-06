import Link from 'next/link';

export default function StrezlessHomePage() {
  return (
    <div className="bg-gradient-to-br from-purple-50 via-pink-50 to-orange-50">
      {/* Hero Section - Custom for Strezless Music Platform */}
      <section className="strezless-container py-20">
        <div className="grid lg:grid-cols-2 gap-12 items-center">
          <div className="space-y-6">
            <div className="inline-block px-4 py-2 bg-purple-100 text-purple-800 rounded-full text-sm font-semibold">
              Built by Artists, For Artists
            </div>
            <h1 className="text-5xl lg:text-6xl font-extrabold leading-tight">
              Your Music Career,
              <span className="text-transparent bg-clip-text bg-gradient-to-r from-purple-600 via-pink-600 to-orange-600">
                {' '}Organized & Monetized
              </span>
            </h1>
            <p className="text-xl text-gray-600 leading-relaxed">
              Strezless helps indie musicians manage professional codes (IPI, ISNI, ISRC), 
              track royalties across platforms, and build sustainable music businesses.
            </p>
            <div className="flex flex-wrap gap-4">
              <Link 
                href="/create-profile" 
                className="px-8 py-4 bg-gradient-to-r from-purple-600 to-pink-600 text-white rounded-lg font-bold hover:shadow-xl transform hover:-translate-y-1 transition-all"
              >
                Start Your Profile
              </Link>
              <Link 
                href="/artists" 
                className="px-8 py-4 bg-white text-purple-600 border-2 border-purple-600 rounded-lg font-bold hover:bg-purple-50 transition-all"
              >
                Explore Artists
              </Link>
            </div>
            <div className="flex items-center gap-8 pt-6">
              <div className="text-center">
                <div className="text-3xl font-bold text-purple-600">100%</div>
                <div className="text-sm text-gray-600">Free Forever</div>
              </div>
              <div className="text-center">
                <div className="text-3xl font-bold text-pink-600">∞</div>
                <div className="text-sm text-gray-600">Unlimited Songs</div>
              </div>
              <div className="text-center">
                <div className="text-3xl font-bold text-orange-600">24/7</div>
                <div className="text-sm text-gray-600">Access Anytime</div>
              </div>
            </div>
          </div>
          
          <div className="relative">
            <div className="aspect-square bg-gradient-to-br from-purple-400 via-pink-400 to-orange-400 rounded-3xl p-8 shadow-2xl transform rotate-3">
              <div className="bg-white rounded-2xl h-full p-6 -rotate-3 space-y-4">
                <div className="h-12 bg-gradient-to-r from-purple-200 to-pink-200 rounded animate-pulse"></div>
                <div className="grid grid-cols-2 gap-4">
                  <div className="h-24 bg-purple-100 rounded"></div>
                  <div className="h-24 bg-pink-100 rounded"></div>
                  <div className="h-24 bg-orange-100 rounded"></div>
                  <div className="h-24 bg-purple-100 rounded"></div>
                </div>
                <div className="h-8 bg-gradient-to-r from-orange-200 to-purple-200 rounded animate-pulse"></div>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* Features Grid - Music Industry Specific */}
      <section className="strezless-container py-16">
        <h2 className="text-4xl font-bold text-center mb-12">
          Everything You Need to <span className="text-purple-600">Succeed</span>
        </h2>
        
        <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-8">
          <div className="bg-white p-8 rounded-2xl shadow-lg hover:shadow-2xl transition-shadow border-t-4 border-purple-500">
            <div className="w-14 h-14 bg-purple-100 rounded-full flex items-center justify-center mb-4">
              <span className="text-2xl">🎵</span>
            </div>
            <h3 className="text-xl font-bold mb-3">Industry Code Management</h3>
            <p className="text-gray-600">
              Store and validate IPI, ISNI, ISRC, ISWC, and UPC codes with built-in verification algorithms.
            </p>
          </div>

          <div className="bg-white p-8 rounded-2xl shadow-lg hover:shadow-2xl transition-shadow border-t-4 border-pink-500">
            <div className="w-14 h-14 bg-pink-100 rounded-full flex items-center justify-center mb-4">
              <span className="text-2xl">💰</span>
            </div>
            <h3 className="text-xl font-bold mb-3">Royalty Tracking</h3>
            <p className="text-gray-600">
              Monitor earnings from Spotify, Apple Music, YouTube and more. Track streams, downloads, and sync licenses.
            </p>
          </div>

          <div className="bg-white p-8 rounded-2xl shadow-lg hover:shadow-2xl transition-shadow border-t-4 border-orange-500">
            <div className="w-14 h-14 bg-orange-100 rounded-full flex items-center justify-center mb-4">
              <span className="text-2xl">📊</span>
            </div>
            <h3 className="text-xl font-bold mb-3">Analytics Dashboard</h3>
            <p className="text-gray-600">
              Visualize your growth with real-time metrics on plays, followers, and revenue trends.
            </p>
          </div>

          <div className="bg-white p-8 rounded-2xl shadow-lg hover:shadow-2xl transition-shadow border-t-4 border-purple-500">
            <div className="w-14 h-14 bg-purple-100 rounded-full flex items-center justify-center mb-4">
              <span className="text-2xl">🤝</span>
            </div>
            <h3 className="text-xl font-bold mb-3">Collaboration Manager</h3>
            <p className="text-gray-600">
              Track featured artists, producers, and writers with automatic split calculations.
            </p>
          </div>

          <div className="bg-white p-8 rounded-2xl shadow-lg hover:shadow-2xl transition-shadow border-t-4 border-pink-500">
            <div className="w-14 h-14 bg-pink-100 rounded-full flex items-center justify-center mb-4">
              <span className="text-2xl">📝</span>
            </div>
            <h3 className="text-xl font-bold mb-3">Copyright Documentation</h3>
            <p className="text-gray-600">
              Maintain complete records for PROs, publishers, and legal documentation.
            </p>
          </div>

          <div className="bg-white p-8 rounded-2xl shadow-lg hover:shadow-2xl transition-shadow border-t-4 border-orange-500">
            <div className="w-14 h-14 bg-orange-100 rounded-full flex items-center justify-center mb-4">
              <span className="text-2xl">🚀</span>
            </div>
            <h3 className="text-xl font-bold mb-3">Release Management</h3>
            <p className="text-gray-600">
              Plan releases with DDEX compliance and multi-platform distribution tracking.
            </p>
          </div>
        </div>
      </section>

      {/* Featured Artists Showcase */}
      <section className="bg-gradient-to-r from-purple-900 via-pink-900 to-orange-900 text-white py-16">
        <div className="strezless-container">
          <div className="text-center mb-12">
            <h2 className="text-4xl font-bold mb-4">Featured Artists on Strezless</h2>
            <p className="text-xl text-purple-200">
              Join independent musicians building their careers with proper rights management
            </p>
          </div>
          
          <div className="grid md:grid-cols-3 gap-8">
            <div className="bg-white/10 backdrop-blur-sm p-6 rounded-xl hover:bg-white/20 transition-colors">
              <div className="aspect-square bg-gradient-to-br from-purple-400 to-pink-400 rounded-lg mb-4"></div>
              <h3 className="text-xl font-bold mb-2">Sin Estres</h3>
              <p className="text-purple-200 mb-3">Hip-Hop / Rap</p>
              <div className="flex justify-between text-sm">
                <span>12 Tracks</span>
                <span>2.5K Streams</span>
              </div>
            </div>

            <div className="bg-white/10 backdrop-blur-sm p-6 rounded-xl hover:bg-white/20 transition-colors">
              <div className="aspect-square bg-gradient-to-br from-pink-400 to-orange-400 rounded-lg mb-4"></div>
              <h3 className="text-xl font-bold mb-2">Your Name Here</h3>
              <p className="text-purple-200 mb-3">Indie / Pop</p>
              <div className="flex justify-between text-sm">
                <span>Coming Soon</span>
                <span>Join Us</span>
              </div>
            </div>

            <div className="bg-white/10 backdrop-blur-sm p-6 rounded-xl hover:bg-white/20 transition-colors">
              <div className="aspect-square bg-gradient-to-br from-orange-400 to-purple-400 rounded-lg mb-4"></div>
              <h3 className="text-xl font-bold mb-2">Start Creating</h3>
              <p className="text-purple-200 mb-3">All Genres Welcome</p>
              <div className="flex justify-between text-sm">
                <span>No Limits</span>
                <span>Free Forever</span>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* Call to Action */}
      <section className="strezless-container py-20 text-center">
        <div className="max-w-3xl mx-auto space-y-6">
          <h2 className="text-4xl lg:text-5xl font-bold">
            Ready to Take Control of Your Music Business?
          </h2>
          <p className="text-xl text-gray-600">
            Join Strezless today and start managing your catalog, tracking royalties, 
            and building a sustainable music career with industry-standard tools.
          </p>
          <div className="pt-6">
            <Link 
              href="/create-profile"
              className="inline-block px-10 py-5 bg-gradient-to-r from-purple-600 via-pink-600 to-orange-600 text-white text-lg font-bold rounded-xl hover:shadow-2xl transform hover:-translate-y-1 transition-all"
            >
              Create Your Free Artist Profile
            </Link>
          </div>
          <p className="text-sm text-gray-500">
            No credit card required • Set up in minutes • Cancel anytime (but it&apos;s free!)
          </p>
        </div>
      </section>
    </div>
  );
}
