import { z } from 'zod';

// Custom validators for music industry professional codes used in Strezless platform

// IPI (Interested Parties Information) - Format: 9 digits with optional spaces
const validateIpiStructure = (value: string): boolean => {
  const cleanedValue = value.replace(/\s/g, '');
  if (cleanedValue.length !== 9) return false;
  
  // IPI uses modulo 10 check digit algorithm
  const digits = cleanedValue.split('').map(Number);
  const checkDigit = digits[8];
  const sumOfDigits = digits.slice(0, 8).reduce((sum, digit, index) => {
    return sum + digit * (9 - index);
  }, 0);
  
  const calculatedCheck = (10 - (sumOfDigits % 10)) % 10;
  return calculatedCheck === checkDigit;
};

// ISRC (International Standard Recording Code) - Format: CC-XXX-YY-NNNNN
const validateIsrcStructure = (value: string): boolean => {
  const cleanValue = value.replace(/-/g, '').toUpperCase();
  if (cleanValue.length !== 12) return false;
  
  const countryCode = cleanValue.substring(0, 2);
  const registrantCode = cleanValue.substring(2, 5);
  const yearCode = cleanValue.substring(5, 7);
  const designationCode = cleanValue.substring(7, 12);
  
  const isValidCountry = /^[A-Z]{2}$/.test(countryCode);
  const isValidRegistrant = /^[A-Z0-9]{3}$/.test(registrantCode);
  const isValidYear = /^[0-9]{2}$/.test(yearCode);
  const isValidDesignation = /^[0-9]{5}$/.test(designationCode);
  
  return isValidCountry && isValidRegistrant && isValidYear && isValidDesignation;
};

// ISNI (International Standard Name Identifier) - Format: 16 digits in groups of 4
const validateIsniChecksum = (value: string): boolean => {
  const cleanValue = value.replace(/\s/g, '');
  if (cleanValue.length !== 16) return false;
  
  let total = 0;
  for (let i = 0; i < 15; i++) {
    const digit = parseInt(cleanValue[i], 10);
    if (isNaN(digit)) return false;
    total = (total + digit) * 2;
  }
  
  const remainder = total % 11;
  const checkValue = (12 - remainder) % 11;
  const lastChar = cleanValue[15];
  const expectedCheck = checkValue === 10 ? 'X' : checkValue.toString();
  
  return lastChar === expectedCheck;
};

// UPC (Universal Product Code) - Format: 12 digits with check digit
const validateUpcCheckDigit = (value: string): boolean => {
  const cleanValue = value.replace(/\s|-/g, '');
  if (cleanValue.length !== 12) return false;
  
  const digits = cleanValue.split('').map(Number);
  if (digits.some(d => isNaN(d))) return false;
  
  const oddSum = digits.filter((_, idx) => idx % 2 === 0).slice(0, -1).reduce((a, b) => a + b, 0);
  const evenSum = digits.filter((_, idx) => idx % 2 === 1).reduce((a, b) => a + b, 0);
  const total = (oddSum * 3) + evenSum;
  const checkDigit = (10 - (total % 10)) % 10;
  
  return checkDigit === digits[11];
};

// Zod schemas with custom validation
export const artistProfessionalCodesSchema = z.object({
  ipiCode: z.string()
    .optional()
    .refine(val => !val || validateIpiStructure(val), {
      message: 'Invalid IPI code format. Must be 9 digits with valid check digit.'
    }),
  
  isniCode: z.string()
    .optional()
    .refine(val => !val || validateIsniChecksum(val), {
      message: 'Invalid ISNI code. Must be 16 characters with valid checksum.'
    }),
  
  einNumber: z.string()
    .regex(/^\d{2}-\d{7}$/, 'EIN must follow format: XX-XXXXXXX')
    .optional()
    .or(z.literal('')),
});

export const songIndustryCodesSchema = z.object({
  isrcCode: z.string()
    .optional()
    .refine(val => !val || validateIsrcStructure(val), {
      message: 'Invalid ISRC format. Expected: CC-XXX-YY-NNNNN'
    }),
  
  upcCode: z.string()
    .optional()
    .refine(val => !val || validateUpcCheckDigit(val), {
      message: 'Invalid UPC code. Check digit does not match.'
    }),
  
  iswcCode: z.string()
    .regex(/^T-\d{9}-\d$/, 'ISWC must follow format: T-NNNNNNNNN-C')
    .optional()
    .or(z.literal('')),
});

export const createArtistProfileSchema = z.object({
  stageName: z.string().min(2, 'Stage name must be at least 2 characters').max(100),
  legalName: z.string().min(2, 'Legal name required').max(150),
  biography: z.string().max(2000).optional(),
  email: z.string().email('Valid email required'),
  
  websiteUrl: z.string().url().or(z.literal('')).optional(),
  contactEmail: z.string().email().or(z.literal('')).optional(),
  phoneNumber: z.string().regex(/^\+?[\d\s\-()]+$/).or(z.literal('')).optional(),
  
  addressLine1: z.string().max(200).optional(),
  city: z.string().max(100).optional(),
  stateProvince: z.string().max(100).optional(),
  postalCode: z.string().max(20).optional(),
  country: z.string().max(100).optional(),
}).merge(artistProfessionalCodesSchema);

export const createSongSchema = z.object({
  title: z.string().min(1, 'Song title required').max(200),
  duration: z.number().int().min(1, 'Duration must be at least 1 second').max(7200),
  
  genre: z.string().max(50).optional(),
  subGenre: z.string().max(50).optional(),
  mood: z.string().max(50).optional(),
  tempo: z.number().int().min(20).max(300).optional(),
  keySignature: z.string().max(10).optional(),
  
  releaseDate: z.string().datetime().optional(),
  copyrightYear: z.number().int().min(1900).max(2100).optional(),
  publisherName: z.string().max(200).optional(),
  
  writerSplits: z.array(z.object({
    writerName: z.string(),
    percentage: z.number().min(0).max(100),
    role: z.string(),
  })).optional(),
}).merge(songIndustryCodesSchema);

export const updateArtistSchema = createArtistProfileSchema.partial();
export const updateSongSchema = createSongSchema.partial();

// Type exports for TypeScript
export type CreateArtistInput = z.infer<typeof createArtistProfileSchema>;
export type CreateSongInput = z.infer<typeof createSongSchema>;
export type UpdateArtistInput = z.infer<typeof updateArtistSchema>;
export type UpdateSongInput = z.infer<typeof updateSongSchema>;
