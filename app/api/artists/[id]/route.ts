import { NextRequest, NextResponse } from 'next/server';
import { strezlessDb } from '@/lib/prisma';
import { updateArtistSchema } from '@/lib/validators/music-industry-codes';
import { ZodError } from 'zod';
import { Prisma } from '@prisma/client';

// GET - Retrieve specific artist by ID
export async function GET(
  request: NextRequest,
  { params }: { params: { id: string } }
) {
  try {
    const artistId = params.id;
    
    const artistProfile = await strezlessDb.artist.findUnique({
      where: { id: artistId },
      include: {
        user: {
          select: {
            email: true,
            createdAt: true,
          },
        },
        songs: {
          select: {
            id: true,
            title: true,
            duration: true,
            genre: true,
            streamCount: true,
            downloadCount: true,
            releaseDate: true,
            isrcCode: true,
          },
          orderBy: {
            createdAt: 'desc',
          },
        },
        albums: {
          select: {
            id: true,
            title: true,
            releaseYear: true,
            totalTracks: true,
            coverArtUrl: true,
          },
        },
        _count: {
          select: {
            songs: true,
            albums: true,
            collaborations: true,
            revenueRecords: true,
          },
        },
      },
    });
    
    if (!artistProfile) {
      return NextResponse.json(
        {
          success: false,
          error: 'Artist not found',
        },
        { status: 404 }
      );
    }
    
    // Calculate total streams across all songs
    const totalStreams = artistProfile.songs.reduce(
      (sum, song) => sum + song.streamCount,
      0
    );
    
    // Calculate total revenue
    const revenueData = await strezlessDb.revenue.aggregate({
      where: { artistId },
      _sum: { amount: true },
      _count: true,
    });
    
    return NextResponse.json({
      success: true,
      data: {
        ...artistProfile,
        stats: {
          totalStreams,
          totalRevenue: revenueData._sum.amount || 0,
          revenueRecordsCount: revenueData._count,
        },
      },
    });
    
  } catch (error) {
    console.error('Artist fetch error:', error);
    return NextResponse.json(
      {
        success: false,
        error: 'Failed to retrieve artist profile',
      },
      { status: 500 }
    );
  }
}

// PUT - Update artist profile
export async function PUT(
  request: NextRequest,
  { params }: { params: { id: string } }
) {
  try {
    const artistId = params.id;
    const requestData = await request.json();
    
    // Validate update data
    const validatedData = updateArtistSchema.parse(requestData);
    
    // Extract only artist-specific fields (remove email which belongs to User)
    const { email, ...artistUpdateData } = validatedData;
    
    // Check if artist exists
    const existingArtist = await strezlessDb.artist.findUnique({
      where: { id: artistId },
    });
    
    if (!existingArtist) {
      return NextResponse.json(
        {
          success: false,
          error: 'Artist not found',
        },
        { status: 404 }
      );
    }
    
    // Check for conflicts with professional codes
    if (artistUpdateData.ipiCode || artistUpdateData.isniCode || artistUpdateData.einNumber) {
      const conflictingArtist = await strezlessDb.artist.findFirst({
        where: {
          AND: [
            { id: { not: artistId } },
            {
              OR: [
                artistUpdateData.ipiCode ? { ipiCode: artistUpdateData.ipiCode } : {},
                artistUpdateData.isniCode ? { isniCode: artistUpdateData.isniCode } : {},
                artistUpdateData.einNumber ? { einNumber: artistUpdateData.einNumber } : {},
              ].filter(condition => Object.keys(condition).length > 0),
            },
          ],
        },
      });
      
      if (conflictingArtist) {
        return NextResponse.json(
          {
            success: false,
            error: 'Professional code already in use by another artist',
          },
          { status: 409 }
        );
      }
    }
    
    // Update artist profile with explicit type casting
    const updateData: Prisma.ArtistUpdateInput = {};
    
    if (artistUpdateData.stageName !== undefined) updateData.stageName = artistUpdateData.stageName as string;
    if (artistUpdateData.legalName !== undefined) updateData.legalName = artistUpdateData.legalName as string;
    if (artistUpdateData.biography !== undefined) updateData.biography = artistUpdateData.biography as string;
    if (artistUpdateData.websiteUrl !== undefined) updateData.websiteUrl = artistUpdateData.websiteUrl as string;
    if (artistUpdateData.ipiCode !== undefined) updateData.ipiCode = artistUpdateData.ipiCode as string;
    if (artistUpdateData.isniCode !== undefined) updateData.isniCode = artistUpdateData.isniCode as string;
    if (artistUpdateData.einNumber !== undefined) updateData.einNumber = artistUpdateData.einNumber as string;
    if (artistUpdateData.contactEmail !== undefined) updateData.contactEmail = artistUpdateData.contactEmail as string;
    if (artistUpdateData.phoneNumber !== undefined) updateData.phoneNumber = artistUpdateData.phoneNumber as string;
    if (artistUpdateData.addressLine1 !== undefined) updateData.addressLine1 = artistUpdateData.addressLine1 as string;
    if (artistUpdateData.city !== undefined) updateData.city = artistUpdateData.city as string;
    if (artistUpdateData.stateProvince !== undefined) updateData.stateProvince = artistUpdateData.stateProvince as string;
    if (artistUpdateData.postalCode !== undefined) updateData.postalCode = artistUpdateData.postalCode as string;
    if (artistUpdateData.country !== undefined) updateData.country = artistUpdateData.country as string;
    
    const updatedArtist = await strezlessDb.artist.update({
      where: { id: artistId },
      data: updateData,
      include: {
        user: {
          select: {
            email: true,
          },
        },
      },
    });
    
    return NextResponse.json({
      success: true,
      data: updatedArtist,
      message: 'Artist profile updated successfully',
    });
    
  } catch (error) {
    if (error instanceof ZodError) {
      return NextResponse.json(
        {
          success: false,
          error: 'Validation failed',
          details: error.errors.map(err => ({
            field: err.path.join('.'),
            message: err.message,
          })),
        },
        { status: 400 }
      );
    }
    
    console.error('Artist update error:', error);
    return NextResponse.json(
      {
        success: false,
        error: 'Failed to update artist profile',
      },
      { status: 500 }
    );
  }
}

// DELETE - Remove artist profile
export async function DELETE(
  request: NextRequest,
  { params }: { params: { id: string } }
) {
  try {
    const artistId = params.id;
    
    const artistToDelete = await strezlessDb.artist.findUnique({
      where: { id: artistId },
      include: {
        _count: {
          select: {
            songs: true,
            albums: true,
          },
        },
      },
    });
    
    if (!artistToDelete) {
      return NextResponse.json(
        {
          success: false,
          error: 'Artist not found',
        },
        { status: 404 }
      );
    }
    
    // Delete artist (cascade will handle related records)
    await strezlessDb.artist.delete({
      where: { id: artistId },
    });
    
    return NextResponse.json({
      success: true,
      message: `Artist profile deleted successfully. Removed ${artistToDelete._count.songs} songs and ${artistToDelete._count.albums} albums.`,
    });
    
  } catch (error) {
    console.error('Artist deletion error:', error);
    return NextResponse.json(
      {
        success: false,
        error: 'Failed to delete artist profile',
      },
      { status: 500 }
    );
  }
}
