import { NextRequest, NextResponse } from 'next/server'
import { dbConnection } from '@/lib/database'

export async function POST(req: NextRequest) {
  try {
    const payload = await req.json()
    
    const newTrack = await dbConnection.trackRecord.create({
      data: {
        trackTitle: payload.trackTitle,
        albumTitle: payload.albumTitle || null,
        durationSeconds: parseInt(payload.durationSeconds, 10),
        musicGenre: payload.musicGenre || null,
        universalProdCode: payload.universalProdCode || null,
        universalProdNumber: payload.universalProdNumber || null,
        intlStandardRecordingCode: payload.intlStandardRecordingCode || null,
        dataExchangeMetadata: payload.dataExchangeMetadata || null,
        belongsToArtist: payload.belongsToArtist,
      },
    })
    
    return NextResponse.json(newTrack, { status: 201 })
  } catch (error) {
    console.error('Track creation error:', error)
    return NextResponse.json(
      { message: 'Unable to create track record' }, 
      { status: 500 }
    )
  }
}

export async function GET(req: NextRequest) {
  try {
    const { searchParams } = new URL(req.url)
    const artistQuery = searchParams.get('belongsToArtist')
    
    if (artistQuery) {
      const tracks = await dbConnection.trackRecord.findMany({
        where: { belongsToArtist: artistQuery },
      })
      return NextResponse.json(tracks)
    }
    
    const allTracks = await dbConnection.trackRecord.findMany()
    return NextResponse.json(allTracks)
  } catch (error) {
    console.error('Track retrieval error:', error)
    return NextResponse.json(
      { message: 'Unable to retrieve tracks' }, 
      { status: 500 }
    )
  }
}
