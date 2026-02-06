import type { Metadata } from 'next';
import './globals.css';

export const metadata: Metadata = {
  title: 'Strezless - Indie Artist Platform',
  description: 'Music production platform for independent artists. Manage your catalog, track royalties, and grow your music career.',
  keywords: ['music platform', 'indie artists', 'music production', 'artist management', 'royalties tracking'],
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body>
        <header className="bg-white shadow-sm border-b border-gray-200">
          <nav className="strezless-container py-4">
            <div className="flex items-center justify-between">
              <div className="flex items-center space-x-8">
                <h1 className="text-2xl font-bold text-strezless-primary">
                  Strezless
                </h1>
                <div className="hidden md:flex space-x-6">
                  <a href="/" className="text-gray-700 hover:text-strezless-primary transition-colors">
                    Home
                  </a>
                  <a href="/artists" className="text-gray-700 hover:text-strezless-primary transition-colors">
                    Artists
                  </a>
                  <a href="/music" className="text-gray-700 hover:text-strezless-primary transition-colors">
                    Music
                  </a>
                  <a href="/about" className="text-gray-700 hover:text-strezless-primary transition-colors">
                    About
                  </a>
                </div>
              </div>
              <div className="flex items-center space-x-4">
                <a href="/create-profile" className="strezless-button-secondary text-sm">
                  Create Profile
                </a>
                <button className="strezless-button-primary text-sm">
                  Sign In
                </button>
              </div>
            </div>
          </nav>
        </header>
        
        <main className="min-h-screen">
          {children}
        </main>
        
        <footer className="bg-strezless-dark text-white mt-16">
          <div className="strezless-container py-12">
            <div className="grid grid-cols-1 md:grid-cols-4 gap-8">
              <div>
                <h3 className="text-xl font-bold mb-4">Strezless</h3>
                <p className="text-gray-400">
                  Empowering indie artists to manage their music careers.
                </p>
              </div>
              <div>
                <h4 className="font-semibold mb-4">Platform</h4>
                <ul className="space-y-2 text-gray-400">
                  <li><a href="/artists" className="hover:text-white transition-colors">Artists</a></li>
                  <li><a href="/music" className="hover:text-white transition-colors">Music Catalog</a></li>
                  <li><a href="/analytics" className="hover:text-white transition-colors">Analytics</a></li>
                </ul>
              </div>
              <div>
                <h4 className="font-semibold mb-4">Resources</h4>
                <ul className="space-y-2 text-gray-400">
                  <li><a href="/docs" className="hover:text-white transition-colors">Documentation</a></li>
                  <li><a href="/guides" className="hover:text-white transition-colors">Guides</a></li>
                  <li><a href="/support" className="hover:text-white transition-colors">Support</a></li>
                </ul>
              </div>
              <div>
                <h4 className="font-semibold mb-4">Legal</h4>
                <ul className="space-y-2 text-gray-400">
                  <li><a href="/privacy" className="hover:text-white transition-colors">Privacy Policy</a></li>
                  <li><a href="/terms" className="hover:text-white transition-colors">Terms of Service</a></li>
                </ul>
              </div>
            </div>
            <div className="border-t border-gray-700 mt-8 pt-8 text-center text-gray-400">
              <p>&copy; 2024 Strezless Musick Productionz. Founded by Sin Estres (Omar Orrantia).</p>
            </div>
          </div>
        </footer>
      </body>
    </html>
  );
}
