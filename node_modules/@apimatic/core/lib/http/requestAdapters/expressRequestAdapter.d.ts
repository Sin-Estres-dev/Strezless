import { HttpRequest } from '@apimatic/core-interfaces';
interface ExpressRequestSubset {
    method: string;
    protocol: string;
    originalUrl: string;
    headers: Record<string, string | string[] | undefined>;
    body?: any;
    get(name: string): string | undefined;
}
/**
 * Converts an Express request into a normalized HttpRequest.
 *
 * Validates method, headers, and URL, and serializes the body.
 *
 * @param req - The Express request to convert.
 * @returns A standardized HttpRequest object.
 * @throws Error if the method, host, or URL is invalid.
 */
export declare function convertExpressRequest(req: ExpressRequestSubset): HttpRequest;
export {};
//# sourceMappingURL=expressRequestAdapter.d.ts.map