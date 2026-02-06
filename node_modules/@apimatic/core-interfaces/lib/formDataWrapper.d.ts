/**
 * Unique symbol used to mark an object as a FormDataWrapper.
 */
declare const formDataWrapperMarker: unique symbol;
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
export declare function createFormData(data: any, headers?: Record<string, string>): FormDataWrapper | undefined;
/**
 * Type guard that checks if a given value is a FormDataWrapper.
 *
 * @param value The value to validate.
 * @returns True if the value is a FormDataWrapper, false otherwise.
 */
export declare function isFormDataWrapper(value: unknown): value is FormDataWrapper;
export {};
//# sourceMappingURL=formDataWrapper.d.ts.map