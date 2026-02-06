import type { Metadata } from 'next';
import Link from 'next/link';
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
                  <Link href="/" className="text-gray-700 hover:text-strezless-primary transition-colors">
                    Home
                  </Link>
                  <Link href="/artists" className="text-gray-700 hover:text-strezless-primary transition-colors">
                    Artists
                  </Link>
                  <Link href="/music" className="text-gray-700 hover:text-strezless-primary transition-colors">
                    Music
                  </Link>
                  <Link href="/about" className="text-gray-700 hover:text-strezless-primary transition-colors">
                    About
                  </Link>
                </div>
              </div>
              <div className="flex items-center space-x-4">
                <Link href="/create-profile" className="strezless-button-secondary text-sm">
                  Create Profile
                </Link>
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
                  <li><Link href="/artists" className="hover:text-white transition-colors">Artists</Link></li>
                  <li><Link href="/music" className="hover:text-white transition-colors">Music Catalog</Link></li>
                  <li><Link href="/analytics" className="hover:text-white transition-colors">Analytics</Link></li>
                </ul>
              </div>
              <div>
                <h4 className="font-semibold mb-4">Resources</h4>
                <ul className="space-y-2 text-gray-400">
                  <li><Link href="/docs" className="hover:text-white transition-colors">Documentation</Link></li>
                  <li><Link href="/guides" className="hover:text-white transition-colors">Guides</Link></li>
                  <li><Link href="/support" className="hover:text-white transition-colors">Support</Link></li>
                </ul>
              </div>
              <div>
                <h4 className="font-semibold mb-4">Legal</h4>
                <ul className="space-y-2 text-gray-400">
                  <li><Link href="/privacy" className="hover:text-white transition-colors">Privacy Policy</Link></li>
                  <li><Link href="/terms" className="hover:text-white transition-colors">Terms of Service</Link></li>
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
