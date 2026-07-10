import { HttpRequest } from './httpRequest';
import { SignatureVerificationResult } from './signatureVerificationResult';
/**
 * Defines the interface for verifying signatures on HTTP requests.
 */
export interface SignatureVerifier {
    verify(req: HttpRequest): SignatureVerificationResult;
}
//# sourceMappingURL=signatureVerifier.d.ts.map