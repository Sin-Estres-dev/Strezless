import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Indie Artist Hub - Music Career Management",
  description: "Manage your music career - artist profiles, industry codes, and catalog management",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body className="antialiased">
        {children}
      </body>
    </html>
  );
}
