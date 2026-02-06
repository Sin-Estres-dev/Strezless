import { NextRequest, NextResponse } from 'next/server';
import { strezlessDb } from '@/lib/prisma';
import { createArtistProfileSchema } from '@/lib/validators/music-industry-codes';
import { ZodError } from 'zod';

// POST - Create new artist profile
export async function POST(request: NextRequest) {
  try {
    const requestData = await request.json();
    
    // Validate input using custom Strezless validators
    const validatedData = createArtistProfileSchema.parse(requestData);
    
    // Check for existing artist with same stage name
    const existingArtist = await strezlessDb.artist.findFirst({
      where: {
        OR: [
          { stageName: { equals: validatedData.stageName as string, mode: 'insensitive' } },
          { ipiCode: validatedData.ipiCode as string || undefined },
          { isniCode: validatedData.isniCode as string || undefined },
        ],
      },
    });
    
    if (existingArtist) {
      return NextResponse.json(
        { 
          success: false,
          error: 'Artist profile already exists with this stage name or professional code' 
        },
        { status: 409 }
      );
    }
    
    // Create user account first
    const newUser = await strezlessDb.user.create({
      data: {
        email: validatedData.email as string,
      },
    });
    
    // Create artist profile linked to user
    const newArtist = await strezlessDb.artist.create({
      data: {
        userId: newUser.id,
        stageName: validatedData.stageName as string,
        legalName: validatedData.legalName as string,
        biography: validatedData.biography as string | undefined,
        websiteUrl: validatedData.websiteUrl as string | undefined,
        ipiCode: validatedData.ipiCode as string | undefined,
        isniCode: validatedData.isniCode as string | undefined,
        einNumber: validatedData.einNumber as string | undefined,
        contactEmail: validatedData.contactEmail as string | undefined,
        phoneNumber: validatedData.phoneNumber as string | undefined,
        addressLine1: validatedData.addressLine1 as string | undefined,
        city: validatedData.city as string | undefined,
        stateProvince: validatedData.stateProvince as string | undefined,
        postalCode: validatedData.postalCode as string | undefined,
        country: validatedData.country as string | undefined,
      },
      include: {
        user: {
          select: {
            id: true,
            email: true,
            createdAt: true,
          },
        },
      },
    });
    
    return NextResponse.json(
      {
        success: true,
        data: newArtist,
        message: 'Artist profile created successfully',
      },
      { status: 201 }
    );
    
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
    
    console.error('Artist creation error:', error);
    return NextResponse.json(
      {
        success: false,
        error: 'Failed to create artist profile',
      },
      { status: 500 }
    );
  }
}

// GET - Fetch all artists with optional filtering
export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const searchQuery = searchParams.get('search');
    const genreFilter = searchParams.get('genre');
    const limitParam = searchParams.get('limit');
    const pageParam = searchParams.get('page');
    
    const limit = limitParam ? parseInt(limitParam, 10) : 20;
    const page = pageParam ? parseInt(pageParam, 10) : 1;
    const skip = (page - 1) * limit;
    
    const whereClause: any = {};
    
    if (searchQuery) {
      whereClause.OR = [
        { stageName: { contains: searchQuery, mode: 'insensitive' } },
        { legalName: { contains: searchQuery, mode: 'insensitive' } },
        { biography: { contains: searchQuery, mode: 'insensitive' } },
      ];
    }
    
    const [artists, totalCount] = await Promise.all([
      strezlessDb.artist.findMany({
        where: whereClause,
        include: {
          _count: {
            select: {
              songs: true,
              albums: true,
              collaborations: true,
            },
          },
        },
        skip,
        take: limit,
        orderBy: {
          createdAt: 'desc',
        },
      }),
      strezlessDb.artist.count({ where: whereClause }),
    ]);
    
    return NextResponse.json({
      success: true,
      data: artists,
      pagination: {
        currentPage: page,
        totalPages: Math.ceil(totalCount / limit),
        totalItems: totalCount,
        itemsPerPage: limit,
      },
    });
    
  } catch (error) {
    console.error('Artist fetch error:', error);
    return NextResponse.json(
      {
        success: false,
        error: 'Failed to fetch artists',
      },
      { status: 500 }
    );
  }
}
