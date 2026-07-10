import JSONBig from '@apimatic/json-bigint';

/**
 * Unique symbol used to mark an object as a FormDataWrapper.
 */
var formDataWrapperMarker = /*#__PURE__*/Symbol('FormDataWrapper');
/**
 * Creates a FormDataWrapper object that encapsulates form-data and optional headers.
 *
 * @param data The form-data payload or object to be wrapped.
 * @param headers Optional headers to include with the form-data.
 * @returns A FormDataWrapper instance.
 */
function createFormData(data, headers) {
  var _a;
  if (data === null || data === undefined) {
    return undefined;
  }
  return _a = {}, _a[formDataWrapperMarker] = true, _a.data = Array.isArray(data) || typeof data === 'object' ? JSONBig.stringify(data) : data.toString(), _a.headers = headers, _a;
}
/**
 * Type guard that checks if a given value is a FormDataWrapper.
 *
 * @param value The value to validate.
 * @returns True if the value is a FormDataWrapper, false otherwise.
 */
function isFormDataWrapper(value) {
  return typeof value === 'object' && value !== null && formDataWrapperMarker in value;
}
export { createFormData, isFormDataWrapper };