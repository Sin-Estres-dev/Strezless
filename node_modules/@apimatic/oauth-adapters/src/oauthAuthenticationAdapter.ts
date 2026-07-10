import {
  AuthenticatorInterface,
  passThroughInterceptor,
} from '@apimatic/core-interfaces';
import { AUTHORIZATION_HEADER, setHeader } from '@apimatic/http-headers';
import { OAuthConfiguration } from './oAuthConfiguration';

interface OAuthTokenConstraints {
  accessToken?: string;
  expiry?: bigint;
}

export const requestAuthenticationProvider = <T extends OAuthTokenConstraints>(
  initialOAuthToken?: T,
  oAuthTokenProvider?: (token: T | undefined) => Promise<T>,
  oAuthOnTokenUpdate?: (token: T) => void,
  oAuthConfiguration?: OAuthConfiguration,
  setOAuthHeader?: (request: any, token: T) => void
): AuthenticatorInterface<boolean> => {
  // This token is shared between all API calls for a client instance.
  let lastOAuthToken: Promise<T | undefined> =
    Promise.resolve(initialOAuthToken);

  return (requiresAuth?: boolean) => {
    if (!requiresAuth) {
      return passThroughInterceptor;
    }
    return async (request, options, next) => {
      let token = await lastOAuthToken;
      lastOAuthToken = refreshOAuthToken(
        token,
        oAuthTokenProvider,
        oAuthOnTokenUpdate,
        oAuthConfiguration?.clockSkew
      );
      token = await lastOAuthToken;
      setOAuthTokenInRequest(
        token,
        request,
        oAuthConfiguration?.clockSkew,
        setOAuthHeader
      );
      return next(request, options);
    };
  };
};

async function refreshOAuthToken<T extends OAuthTokenConstraints>(
  currentToken: T | undefined,
  provider?: (token: T | undefined) => Promise<T>,
  onUpdate?: (token: T) => void,
  clockSkew?: number
): Promise<T | undefined> {
  if (
    !provider ||
    (isValid(currentToken) && !isExpired(currentToken, clockSkew))
  ) {
    return currentToken;
  }
  const newToken = await provider(currentToken);
  if (newToken && onUpdate) {
    onUpdate(newToken);
  }
  return newToken;
}

function setOAuthTokenInRequest<T extends OAuthTokenConstraints>(
  oAuthToken: T | undefined,
  request: any,
  clockSkew?: number,
  setOAuthHeader?: (request: any, token: T) => void
) {
  validateAuthorization(oAuthToken, clockSkew);
  request.headers = request.headers ?? {};
  if (setOAuthHeader && oAuthToken) {
    setOAuthHeader(request, oAuthToken); // assumes it mutates the request
    return;
  }

  setHeader(
    request.headers,
    AUTHORIZATION_HEADER,
    `Bearer ${oAuthToken?.accessToken}`
  );
}

function validateAuthorization<T extends OAuthTokenConstraints>(
  oAuthToken?: T,
  clockSkew?: number
) {
  if (!isValid(oAuthToken)) {
    throw new Error(
      'Client is not authorized. An OAuth token is needed to make API calls.'
    );
  }

  if (isExpired(oAuthToken, clockSkew)) {
    throw new Error(
      'OAuth token is expired. A valid token is needed to make API calls.'
    );
  }
}

export function isValid<T extends OAuthTokenConstraints>(
  oAuthToken: T | undefined
): oAuthToken is T {
  return typeof oAuthToken !== 'undefined';
}

export function isExpired<T extends OAuthTokenConstraints>(
  oAuthToken: T,
  clockSkew?: number
) {
  if (typeof oAuthToken.expiry === 'undefined') {
    return false; // Expiry is undefined, token cannot be expired
  }

  let tokenExpiry = oAuthToken.expiry;

  // Adjust expiration time if clockSkew is provided and is not undefined
  if (clockSkew && typeof clockSkew !== 'undefined') {
    tokenExpiry -= BigInt(clockSkew); // Subtract clockSkew from expiry
  }

  return tokenExpiry < Date.now() / 1000;
}
