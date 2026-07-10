import {
  HttpMethod,
  HttpRequest,
  HttpRequestTextBody,
} from '@apimatic/core-interfaces';
import JSONBig from '@apimatic/json-bigint';

interface ExpressRequestSubset {
  method: string;
  protocol: string;
  originalUrl: string;
  headers: Record<string, string | string[] | undefined>;
  body?: any;
  get(name: string): string | undefined;
}

const validHttpMethods: HttpMethod[] = [
  'GET',
  'DELETE',
  'HEAD',
  'OPTIONS',
  'POST',
  'PUT',
  'PATCH',
  'LINK',
  'UNLINK',
];

/**
 * Converts an Express request into a normalized HttpRequest.
 *
 * Validates method, headers, and URL, and serializes the body.
 *
 * @param req - The Express request to convert.
 * @returns A standardized HttpRequest object.
 * @throws Error if the method, host, or URL is invalid.
 */
export function convertExpressRequest(req: ExpressRequestSubset): HttpRequest {
  const method = req.method?.toUpperCase();

  if (!isHttpMethod(method)) {
    throw new Error(`Unsupported HTTP method: ${req.method}`);
  }

  const host = req.get('host');
  if (!host || host.trim() === '' || host === 'undefined') {
    throw new Error('Missing host header');
  }

  const url = `${req.protocol}://${req.get('host')}${req.originalUrl}`;
  if (!isValidUrl(url)) {
    throw new Error(`Invalid URL: ${url}`);
  }

  const headers = req.headers;
  if (!headers || typeof headers !== 'object' || Array.isArray(headers)) {
    throw new Error(
      `Invalid headers: expected an object, got ${typeof headers}`
    );
  }

  return {
    method,
    headers: Object.entries(headers).reduce<Record<string, string>>(
      (acc, [k, v]) => {
        acc[k] = String(v);
        return acc;
      },
      {}
    ),
    url,
    body: toBodyContent(req.body),
  };
}

function isHttpMethod(method: any): method is HttpMethod {
  return validHttpMethods.includes(method);
}

function isValidUrl(url: string): boolean {
  const protocolRegex = /^https?:\/\/[^/]+/;
  const match = protocolRegex.exec(url);
  return match !== null;
}

function toBodyContent(body: unknown): HttpRequestTextBody {
  if (body === undefined || body === null) {
    return { type: 'text', content: '' };
  }

  if (Buffer.isBuffer(body)) {
    return { type: 'text', content: body.toString('utf8') };
  }

  if (typeof body === 'string') {
    return { type: 'text', content: body };
  }

  if (typeof body === 'object') {
    if (Object.keys(body).length === 0) {
      return { type: 'text', content: '' };
    }
    return {
      type: 'text',
      content: JSONBig.stringify(body),
    };
  }

  return { type: 'text', content: String(body) };
}
