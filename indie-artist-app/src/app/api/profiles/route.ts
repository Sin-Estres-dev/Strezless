import { NextRequest, NextResponse } from 'next/server'
import { dbConnection } from '@/lib/database'

export async function POST(req: NextRequest) {
  try {
    const payload = await req.json()
    
    const newProfile = await dbConnection.artistProfile.create({
      data: {
        legalFullName: payload.legalFullName,
        performanceName: payload.performanceName || null,
        dateOfBirth: payload.dateOfBirth ? new Date(payload.dateOfBirth) : null,
        contactEmail: payload.contactEmail,
        interestedPartyInfo: payload.interestedPartyInfo || null,
        standardNameId: payload.standardNameId || null,
        musicalWorkCode: payload.musicalWorkCode || null,
        digitalDataExchangeId: payload.digitalDataExchangeId || null,
        universalProductNum: payload.universalProductNum || null,
        employerIdNumber: payload.employerIdNumber || null,
      },
    })
    
    return NextResponse.json(newProfile, { status: 201 })
  } catch (error) {
    console.error('Profile creation error:', error)
    return NextResponse.json(
      { message: 'Unable to create artist profile' }, 
      { status: 500 }
    )
  }
}

export async function GET(req: NextRequest) {
  try {
    const { searchParams } = new URL(req.url)
    const emailQuery = searchParams.get('contactEmail')
    
    if (emailQuery) {
      const profile = await dbConnection.artistProfile.findUnique({
        where: { contactEmail: emailQuery },
        include: { trackRecords: true }
      })
      
      if (!profile) {
        return NextResponse.json(
          { message: 'Profile not found' }, 
          { status: 404 }
        )
      }
      
      return NextResponse.json(profile)
    }
    
    const allProfiles = await dbConnection.artistProfile.findMany()
    return NextResponse.json(allProfiles)
  } catch (error) {
    console.error('Profile retrieval error:', error)
    return NextResponse.json(
      { message: 'Unable to retrieve profiles' }, 
      { status: 500 }
    )
  }
}
