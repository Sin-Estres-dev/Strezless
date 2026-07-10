declare const signatureVerificationResultMarker: unique symbol;
export interface SignatureVerificationResultMarker {
    [signatureVerificationResultMarker]: true;
}
/**
 * Represents a successful signature verification.
 */
export interface SignatureVerificationSuccess extends SignatureVerificationResultMarker {
    success: true;
}
/**
 * Represents a failed signature verification with an error message.
 */
export interface SignatureVerificationFailure extends SignatureVerificationResultMarker {
    success: false;
    error: string;
}
/**
 * Union type for signature verification results:
 * either SignatureVerificationSuccess or SignatureVerificationFailure.
 */
export type SignatureVerificationResult = SignatureVerificationSuccess | SignatureVerificationFailure;
/**
 * Type guard that checks if a value is a SignatureVerificationResult.
 *
 * @param result The value to validate.
 * @returns True if the value is a SignatureVerificationResult, false otherwise.
 */
export declare function isSignatureVerificationResult(result: unknown): result is SignatureVerificationResult;
/**
 * Creates a result object representing a successful signature verification.
 *
 * @returns A SignatureVerificationSuccess instance.
 */
export declare function createSignatureVerificationSuccess(): SignatureVerificationSuccess;
/**
 * Creates a result object representing a failed signature verification.
 *
 * @param error Error message describing the reason for failure.
 * @returns A SignatureVerificationFailure instance with the provided error message.
 */
export declare function createSignatureVerificationFailure(error: string): SignatureVerificationFailure;
export {};
//# sourceMappingURL=signatureVerificationResult.d.ts.map