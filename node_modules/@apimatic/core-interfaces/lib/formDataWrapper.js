"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.isFormDataWrapper = exports.createFormData = void 0;
var tslib_1 = require("tslib");
var json_bigint_1 = tslib_1.__importDefault(require("@apimatic/json-bigint"));
/**
 * Unique symbol used to mark an object as a FormDataWrapper.
 */
var formDataWrapperMarker = Symbol('FormDataWrapper');
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
    return _a = {},
        _a[formDataWrapperMarker] = true,
        _a.data = Array.isArray(data) || typeof data === 'object'
            ? json_bigint_1.default.stringify(data)
            : data.toString(),
        _a.headers = headers,
        _a;
}
exports.createFormData = createFormData;
/**
 * Type guard that checks if a given value is a FormDataWrapper.
 *
 * @param value The value to validate.
 * @returns True if the value is a FormDataWrapper, false otherwise.
 */
function isFormDataWrapper(value) {
    return (typeof value === 'object' &&
        value !== null &&
        formDataWrapperMarker in value);
}
exports.isFormDataWrapper = isFormDataWrapper;
