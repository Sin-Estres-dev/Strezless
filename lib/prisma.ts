import { PrismaClient } from '@prisma/client';

// Custom Prisma client factory for Strezless platform
class StrezlessDatabase {
  private static prismaInstance: PrismaClient | undefined;

  static getConnection(): PrismaClient {
    if (!StrezlessDatabase.prismaInstance) {
      StrezlessDatabase.prismaInstance = new PrismaClient({
        log: process.env.NODE_ENV === 'development' ? ['query', 'error', 'warn'] : ['error'],
      });
      
      // Custom extensions for Strezless-specific operations
      StrezlessDatabase.prismaInstance.$extends({
        name: 'strezless-extensions',
        model: {
          artist: {
            async findByStageNameOrLegal(searchTerm: string) {
              return await StrezlessDatabase.prismaInstance!.artist.findFirst({
                where: {
                  OR: [
                    { stageName: { contains: searchTerm, mode: 'insensitive' } },
                    { legalName: { contains: searchTerm, mode: 'insensitive' } },
                  ],
                },
              });
            },
          },
          song: {
            async incrementStreams(songId: string, count: number = 1) {
              return await StrezlessDatabase.prismaInstance!.song.update({
                where: { id: songId },
                data: { streamCount: { increment: count } },
              });
            },
          },
        },
      });
    }
    
    return StrezlessDatabase.prismaInstance;
  }

  static async disconnect(): Promise<void> {
    if (StrezlessDatabase.prismaInstance) {
      await StrezlessDatabase.prismaInstance.$disconnect();
      StrezlessDatabase.prismaInstance = undefined;
    }
  }
}

export const strezlessDb = StrezlessDatabase.getConnection();
export { StrezlessDatabase };
