import JSONBig from '@apimatic/json-bigint';
import {
  ApiResponse,
  HttpContext,
  HttpRequest,
} from '@apimatic/core-interfaces';
import { convertFromStream } from '@apimatic/convert-to-stream';

/**
 * Thrown when the HTTP status code is not okay.
 *
 * The ApiError extends the ApiResponse interface, so all ApiResponse
 * properties are available.
 */
export class ApiError<T = {}>
  extends Error
  implements ApiResponse<T | undefined>
{
  public request: HttpRequest;
  public statusCode: number;
  public headers: Record<string, string>;
  public result: T | undefined;
  public body: string | Blob | NodeJS.ReadableStream;

  constructor(context: HttpContext, message: string) {
    super(message);
    Object.setPrototypeOf(this, new.target.prototype);

    const { request, response } = context;
    this.request = request;
    this.statusCode = response.statusCode;
    this.headers = response.headers;
    this.body = response.body;
  }
}

export async function loadResult<T>(error: ApiError<T>): Promise<void> {
  const bodyString = await convertFromStream(error.body);
  try {
    error.result = JSONBig().parse(bodyString);
  } catch (_) {
    // ignore updating result if body is not a valid JSON.
  }
}
