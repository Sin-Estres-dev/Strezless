import { AuthenticatorInterface } from '@apimatic/core-interfaces';
import { OAuthConfiguration } from './oAuthConfiguration';
interface OAuthTokenConstraints {
    accessToken?: string;
    expiry?: bigint;
}
export declare const requestAuthenticationProvider: <T extends OAuthTokenConstraints>(initialOAuthToken?: T | undefined, oAuthTokenProvider?: ((token: T | undefined) => Promise<T>) | undefined, oAuthOnTokenUpdate?: ((token: T) => void) | undefined, oAuthConfiguration?: OAuthConfiguration, setOAuthHeader?: ((request: any, token: T) => void) | undefined) => AuthenticatorInterface<boolean>;
export declare function isValid<T extends OAuthTokenConstraints>(oAuthToken: T | undefined): oAuthToken is T;
export declare function isExpired<T extends OAuthTokenConstraints>(oAuthToken: T, clockSkew?: number): boolean;
export {};
//# sourceMappingURL=oauthAuthenticationAdapter.d.ts.map