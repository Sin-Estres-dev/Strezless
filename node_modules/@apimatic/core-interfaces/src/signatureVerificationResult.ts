const signatureVerificationResultMarker = Symbol('SignatureVerificationResult');

export interface SignatureVerificationResultMarker {
  [signatureVerificationResultMarker]: true;
}

/**
 * Represents a successful signature verification.
 */
export interface SignatureVerificationSuccess
  extends SignatureVerificationResultMarker {
  success: true;
}

/**
 * Represents a failed signature verification with an error message.
 */
export interface SignatureVerificationFailure
  extends SignatureVerificationResultMarker {
  success: false;
  error: string;
}

/**
 * Union type for signature verification results:
 * either SignatureVerificationSuccess or SignatureVerificationFailure.
 */
export type SignatureVerificationResult =
  | SignatureVerificationSuccess
  | SignatureVerificationFailure;

/**
 * Type guard that checks if a value is a SignatureVerificationResult.
 *
 * @param result The value to validate.
 * @returns True if the value is a SignatureVerificationResult, false otherwise.
 */
export function isSignatureVerificationResult(
  result: unknown
): result is SignatureVerificationResult {
  return (
    typeof result === 'object' &&
    result !== null &&
    signatureVerificationResultMarker in result
  );
}

/**
 * Creates a result object representing a successful signature verification.
 *
 * @returns A SignatureVerificationSuccess instance.
 */
export function createSignatureVerificationSuccess(): SignatureVerificationSuccess {
  return {
    [signatureVerificationResultMarker]: true,
    success: true,
  };
}

/**
 * Creates a result object representing a failed signature verification.
 *
 * @param error Error message describing the reason for failure.
 * @returns A SignatureVerificationFailure instance with the provided error message.
 */
export function createSignatureVerificationFailure(
  error: string
): SignatureVerificationFailure {
  return {
    [signatureVerificationResultMarker]: true,
    success: false,
    error,
  };
}
