import { __awaiter, __generator } from 'tslib';
import { passThroughInterceptor } from '@apimatic/core-interfaces';
import { setHeader, AUTHORIZATION_HEADER } from '@apimatic/http-headers';
var requestAuthenticationProvider = function (initialOAuthToken, oAuthTokenProvider, oAuthOnTokenUpdate, oAuthConfiguration, setOAuthHeader) {
  // This token is shared between all API calls for a client instance.
  var lastOAuthToken = Promise.resolve(initialOAuthToken);
  return function (requiresAuth) {
    if (!requiresAuth) {
      return passThroughInterceptor;
    }
    return function (request, options, next) {
      return __awaiter(void 0, void 0, void 0, function () {
        var token;
        return __generator(this, function (_a) {
          switch (_a.label) {
            case 0:
              return [4 /*yield*/, lastOAuthToken];
            case 1:
              token = _a.sent();
              lastOAuthToken = refreshOAuthToken(token, oAuthTokenProvider, oAuthOnTokenUpdate, oAuthConfiguration === null || oAuthConfiguration === void 0 ? void 0 : oAuthConfiguration.clockSkew);
              return [4 /*yield*/, lastOAuthToken];
            case 2:
              token = _a.sent();
              setOAuthTokenInRequest(token, request, oAuthConfiguration === null || oAuthConfiguration === void 0 ? void 0 : oAuthConfiguration.clockSkew, setOAuthHeader);
              return [2 /*return*/, next(request, options)];
          }
        });
      });
    };
  };
};
function refreshOAuthToken(currentToken, provider, onUpdate, clockSkew) {
  return __awaiter(this, void 0, void 0, function () {
    var newToken;
    return __generator(this, function (_a) {
      switch (_a.label) {
        case 0:
          if (!provider || isValid(currentToken) && !isExpired(currentToken, clockSkew)) {
            return [2 /*return*/, currentToken];
          }
          return [4 /*yield*/, provider(currentToken)];
        case 1:
          newToken = _a.sent();
          if (newToken && onUpdate) {
            onUpdate(newToken);
          }
          return [2 /*return*/, newToken];
      }
    });
  });
}
function setOAuthTokenInRequest(oAuthToken, request, clockSkew, setOAuthHeader) {
  var _a;
  validateAuthorization(oAuthToken, clockSkew);
  request.headers = (_a = request.headers) !== null && _a !== void 0 ? _a : {};
  if (setOAuthHeader && oAuthToken) {
    setOAuthHeader(request, oAuthToken); // assumes it mutates the request
    return;
  }
  setHeader(request.headers, AUTHORIZATION_HEADER, "Bearer ".concat(oAuthToken === null || oAuthToken === void 0 ? void 0 : oAuthToken.accessToken));
}
function validateAuthorization(oAuthToken, clockSkew) {
  if (!isValid(oAuthToken)) {
    throw new Error('Client is not authorized. An OAuth token is needed to make API calls.');
  }
  if (isExpired(oAuthToken, clockSkew)) {
    throw new Error('OAuth token is expired. A valid token is needed to make API calls.');
  }
}
function isValid(oAuthToken) {
  return typeof oAuthToken !== 'undefined';
}
function isExpired(oAuthToken, clockSkew) {
  if (typeof oAuthToken.expiry === 'undefined') {
    return false; // Expiry is undefined, token cannot be expired
  }
  var tokenExpiry = oAuthToken.expiry;
  // Adjust expiration time if clockSkew is provided and is not undefined
  if (clockSkew && typeof clockSkew !== 'undefined') {
    tokenExpiry -= BigInt(clockSkew); // Subtract clockSkew from expiry
  }
  return tokenExpiry < Date.now() / 1000;
}
export { isExpired, isValid, requestAuthenticationProvider };