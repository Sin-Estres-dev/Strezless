import JSONBig from '@apimatic/json-bigint';

/**
 * Unique symbol used to mark an object as a FormDataWrapper.
 */
const formDataWrapperMarker = Symbol('FormDataWrapper');

/**
 * Represents a wrapped form-data object containing the raw data
 * and optional headers to be sent with the request.
 */
export interface FormDataWrapper {
  [formDataWrapperMarker]: true;
  data: any;
  headers?: Record<string, string>;
}

/**
 * Creates a FormDataWrapper object that encapsulates form-data and optional headers.
 *
 * @param data The form-data payload or object to be wrapped.
 * @param headers Optional headers to include with the form-data.
 * @returns A FormDataWrapper instance.
 */
export function createFormData(
  data: any,
  headers?: Record<string, string>
): FormDataWrapper | undefined {
  if (data === null || data === undefined) {
    return undefined;
  }
  return {
    [formDataWrapperMarker]: true,
    data:
      Array.isArray(data) || typeof data === 'object'
        ? JSONBig.stringify(data)
        : data.toString(),
    headers,
  };
}

/**
 * Type guard that checks if a given value is a FormDataWrapper.
 *
 * @param value The value to validate.
 * @returns True if the value is a FormDataWrapper, false otherwise.
 */
export function isFormDataWrapper(value: unknown): value is FormDataWrapper {
  return (
    typeof value === 'object' &&
    value !== null &&
    formDataWrapperMarker in value
  );
}
