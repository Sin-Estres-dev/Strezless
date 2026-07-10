"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.createSignatureVerificationFailure = exports.createSignatureVerificationSuccess = exports.isSignatureVerificationResult = void 0;
var signatureVerificationResultMarker = Symbol('SignatureVerificationResult');
/**
 * Type guard that checks if a value is a SignatureVerificationResult.
 *
 * @param result The value to validate.
 * @returns True if the value is a SignatureVerificationResult, false otherwise.
 */
function isSignatureVerificationResult(result) {
    return (typeof result === 'object' &&
        result !== null &&
        signatureVerificationResultMarker in result);
}
exports.isSignatureVerificationResult = isSignatureVerificationResult;
/**
 * Creates a result object representing a successful signature verification.
 *
 * @returns A SignatureVerificationSuccess instance.
 */
function createSignatureVerificationSuccess() {
    var _a;
    return _a = {},
        _a[signatureVerificationResultMarker] = true,
        _a.success = true,
        _a;
}
exports.createSignatureVerificationSuccess = createSignatureVerificationSuccess;
/**
 * Creates a result object representing a failed signature verification.
 *
 * @param error Error message describing the reason for failure.
 * @returns A SignatureVerificationFailure instance with the provided error message.
 */
function createSignatureVerificationFailure(error) {
    var _a;
    return _a = {},
        _a[signatureVerificationResultMarker] = true,
        _a.success = false,
        _a.error = error,
        _a;
}
exports.createSignatureVerificationFailure = createSignatureVerificationFailure;
