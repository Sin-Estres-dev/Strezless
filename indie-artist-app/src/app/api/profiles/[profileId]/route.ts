import { NextRequest, NextResponse } from 'next/server'
import { dbConnection } from '@/lib/database'

export async function GET(
  req: NextRequest,
  context: { params: Promise<{ profileId: string }> }
) {
  const { profileId } = await context.params
  
  try {
    const profile = await dbConnection.artistProfile.findUnique({
      where: { profileId },
      include: { trackRecords: true }
    })
    
    if (!profile) {
      return NextResponse.json(
        { message: 'Profile not found' }, 
        { status: 404 }
      )
    }
    
    return NextResponse.json(profile)
  } catch (error) {
    console.error('Profile fetch error:', error)
    return NextResponse.json(
      { message: 'Unable to fetch profile' }, 
      { status: 500 }
    )
  }
}

export async function PUT(
  req: NextRequest,
  context: { params: Promise<{ profileId: string }> }
) {
  const { profileId } = await context.params
  
  try {
    const payload = await req.json()
    
    const updatedProfile = await dbConnection.artistProfile.update({
      where: { profileId },
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
    
    return NextResponse.json(updatedProfile)
  } catch (error) {
    console.error('Profile update error:', error)
    return NextResponse.json(
      { message: 'Unable to update profile' }, 
      { status: 500 }
    )
  }
}
