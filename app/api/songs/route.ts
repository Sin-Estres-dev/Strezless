import { NextRequest, NextResponse } from 'next/server';
import { strezlessDb } from '@/lib/prisma';
import { createSongSchema } from '@/lib/validators/music-industry-codes';
import { ZodError } from 'zod';

// POST - Create new song
export async function POST(request: NextRequest) {
  try {
    const requestData = await request.json();
    const { artistId, ...songData } = requestData;
    
    if (!artistId) {
      return NextResponse.json(
        {
          success: false,
          error: 'Artist ID is required',
        },
        { status: 400 }
      );
    }
    
    // Validate song data
    const validatedSong = createSongSchema.parse(songData);
    
    // Verify artist exists
    const artistExists = await strezlessDb.artist.findUnique({
      where: { id: artistId },
    });
    
    if (!artistExists) {
      return NextResponse.json(
        {
          success: false,
          error: 'Artist not found',
        },
        { status: 404 }
      );
    }
    
    // Check for duplicate ISRC code
    if (validatedSong.isrcCode) {
      const existingSong = await strezlessDb.song.findUnique({
        where: { isrcCode: validatedSong.isrcCode as string },
      });
      
      if (existingSong) {
        return NextResponse.json(
          {
            success: false,
            error: 'Song with this ISRC code already exists',
          },
          { status: 409 }
        );
      }
    }
    
    // Create song record
    const newSong = await strezlessDb.song.create({
      data: {
        artistId,
        title: validatedSong.title as string,
        duration: validatedSong.duration as number,
        genre: validatedSong.genre as string | undefined,
        subGenre: validatedSong.subGenre as string | undefined,
        mood: validatedSong.mood as string | undefined,
        tempo: validatedSong.tempo as number | undefined,
        keySignature: validatedSong.keySignature as string | undefined,
        isrcCode: validatedSong.isrcCode as string | undefined,
        upcCode: validatedSong.upcCode as string | undefined,
        iswcCode: validatedSong.iswcCode as string | undefined,
        releaseDate: validatedSong.releaseDate ? new Date(validatedSong.releaseDate as string) : null,
        copyrightYear: validatedSong.copyrightYear as number | undefined,
        publisherName: validatedSong.publisherName as string | undefined,
        writerSplits: (validatedSong.writerSplits as any) || [],
      },
      include: {
        artist: {
          select: {
            id: true,
            stageName: true,
            legalName: true,
          },
        },
      },
    });
    
    return NextResponse.json(
      {
        success: true,
        data: newSong,
        message: 'Song created successfully',
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
    
    console.error('Song creation error:', error);
    return NextResponse.json(
      {
        success: false,
        error: 'Failed to create song',
      },
      { status: 500 }
    );
  }
}

// GET - Fetch songs with filtering
export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const artistIdFilter = searchParams.get('artistId');
    const genreFilter = searchParams.get('genre');
    const searchQuery = searchParams.get('search');
    const limitParam = searchParams.get('limit');
    const pageParam = searchParams.get('page');
    const sortBy = searchParams.get('sortBy') || 'createdAt';
    const sortOrder = searchParams.get('sortOrder') || 'desc';
    
    const limit = limitParam ? parseInt(limitParam, 10) : 50;
    const page = pageParam ? parseInt(pageParam, 10) : 1;
    const skip = (page - 1) * limit;
    
    const whereClause: any = {};
    
    if (artistIdFilter) {
      whereClause.artistId = artistIdFilter;
    }
    
    if (genreFilter) {
      whereClause.genre = { equals: genreFilter, mode: 'insensitive' };
    }
    
    if (searchQuery) {
      whereClause.OR = [
        { title: { contains: searchQuery, mode: 'insensitive' } },
        { genre: { contains: searchQuery, mode: 'insensitive' } },
        { publisherName: { contains: searchQuery, mode: 'insensitive' } },
      ];
    }
    
    const orderByClause: any = {};
    if (sortBy === 'streams') {
      orderByClause.streamCount = sortOrder;
    } else if (sortBy === 'title') {
      orderByClause.title = sortOrder;
    } else {
      orderByClause.createdAt = sortOrder;
    }
    
    const [songs, totalCount] = await Promise.all([
      strezlessDb.song.findMany({
        where: whereClause,
        include: {
          artist: {
            select: {
              id: true,
              stageName: true,
            },
          },
          album: {
            select: {
              id: true,
              title: true,
              coverArtUrl: true,
            },
          },
          _count: {
            select: {
              collaborations: true,
            },
          },
        },
        skip,
        take: limit,
        orderBy: orderByClause,
      }),
      strezlessDb.song.count({ where: whereClause }),
    ]);
    
    return NextResponse.json({
      success: true,
      data: songs,
      pagination: {
        currentPage: page,
        totalPages: Math.ceil(totalCount / limit),
        totalItems: totalCount,
        itemsPerPage: limit,
      },
    });
    
  } catch (error) {
    console.error('Songs fetch error:', error);
    return NextResponse.json(
      {
        success: false,
        error: 'Failed to fetch songs',
      },
      { status: 500 }
    );
  }
}
