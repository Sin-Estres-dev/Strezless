import { PrismaClient } from '@/generated/prisma'
import { PrismaBetterSqlite3 } from '@prisma/adapter-better-sqlite3'

const globalForDatabase = global as unknown as { dbConnection: PrismaClient | undefined }

function createDatabaseConnection() {
  const dbPath = './prisma/dev.db'
  const adapter = new PrismaBetterSqlite3({ url: dbPath })
  return new PrismaClient({ adapter })
}

export const dbConnection = globalForDatabase.dbConnection ?? createDatabaseConnection()

if (process.env.NODE_ENV !== 'production') {
  globalForDatabase.dbConnection = dbConnection
}
