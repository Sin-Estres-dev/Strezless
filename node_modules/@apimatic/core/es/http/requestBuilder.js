import { __values, __read, __awaiter, __generator, __assign, __spreadArray } from 'tslib';
import JSONBig from '@apimatic/json-bigint';
import { deprecated, sanitizeUrl, updateErrorMessage, updateByJsonPointer } from '../apiHelper.js';
import { ArgumentsValidationError } from '../errors/argumentsValidationError.js';
import { ResponseValidationError } from '../errors/responseValidationError.js';
import { validateAndUnmapXml, validateAndMapXml, validateAndMap } from '@apimatic/schema';
import { JSON_CONTENT_TYPE, TEXT_CONTENT_TYPE, XML_CONTENT_TYPE, setHeaderIfNotSet, ACCEPT_HEADER, setHeader, CONTENT_TYPE_HEADER, CONTENT_LENGTH_HEADER } from '@apimatic/http-headers';
import { callHttpInterceptors } from './httpInterceptor.js';
import { SkipEncode, pathTemplate } from './pathTemplate.js';
import { urlEncodeObject, formDataEncodeObject, filterFileWrapperFromKeyValuePairs } from '@apimatic/http-query';
import { prepareArgs } from './validate.js';
import { shouldRetryRequest, RequestRetryOption, getRetryWaitTime } from './retryConfiguration.js';
import { convertToStream } from '@apimatic/convert-to-stream';
import { XmlSerialization } from '../xml/xmlSerializer.js';
import { ApiError, loadResult } from '../errors/apiError.js';
import { PathParam } from './pathParam.js';
var JSON = /*#__PURE__*/JSONBig();
function skipEncode(value, key) {
  return new SkipEncode(value, key);
}
function pathParam(value, key) {
  return new PathParam(value, key);
}
var DefaultRequestBuilder =
/*#__PURE__*/
/** @class */
function () {
  function DefaultRequestBuilder(_httpClient, _baseUrlProvider, _apiErrorCtr, _authenticationProvider, _httpMethod, _xmlSerializer, _retryConfig, _path, _apiLogger) {
    this._httpClient = _httpClient;
    this._baseUrlProvider = _baseUrlProvider;
    this._apiErrorCtr = _apiErrorCtr;
    this._authenticationProvider = _authenticationProvider;
    this._httpMethod = _httpMethod;
    this._xmlSerializer = _xmlSerializer;
    this._retryConfig = _retryConfig;
    this._path = _path;
    this._apiLogger = _apiLogger;
    this._queryParams = {};
    this._pathParams = {};
    this._headerParams = {};
    this._queryParamsPrefixFormat = {};
    this._interceptors = [];
    this._errorTypes = [];
    this._validateResponse = true;
    this._apiErrorFactory = {
      apiErrorCtor: _apiErrorCtr
    };
    this._addResponseValidator();
    this._addAuthentication();
    this._addRetryInterceptor();
    this._addErrorHandlingInterceptor();
    this._addApiLoggerInterceptors();
    this._retryOption = RequestRetryOption.Default;
    this.prepareArgs = prepareArgs.bind(this);
  }
  DefaultRequestBuilder.prototype.authenticate = function (params) {
    this._authParams = params;
  };
  DefaultRequestBuilder.prototype.requestRetryOption = function (option) {
    this._retryOption = option;
  };
  DefaultRequestBuilder.prototype.deprecated = function (methodName, message) {
    deprecated(methodName, message);
  };
  DefaultRequestBuilder.prototype.appendTemplatePath = function (strings) {
    var e_1, _a;
    var args = [];
    for (var _i = 1; _i < arguments.length; _i++) {
      args[_i - 1] = arguments[_i];
    }
    this._pathStrings = strings;
    this._pathArgs = args;
    try {
      for (var args_1 = __values(args), args_1_1 = args_1.next(); !args_1_1.done; args_1_1 = args_1.next()) {
        var arg = args_1_1.value;
        if ((arg instanceof SkipEncode || arg instanceof PathParam) && arg.key !== undefined) {
          this._pathParams[arg.key] = arg.value;
        }
      }
    } catch (e_1_1) {
      e_1 = {
        error: e_1_1
      };
    } finally {
      try {
        if (args_1_1 && !args_1_1.done && (_a = args_1.return)) _a.call(args_1);
      } finally {
        if (e_1) throw e_1.error;
      }
    }
  };
  DefaultRequestBuilder.prototype.method = function (httpMethodName) {
    this._httpMethod = httpMethodName;
  };
  DefaultRequestBuilder.prototype.baseUrl = function (arg) {
    this._baseUrlArg = arg;
  };
  DefaultRequestBuilder.prototype.appendPath = function (path) {
    this._path = this._path ? mergePath(this._path, path) : path;
  };
  DefaultRequestBuilder.prototype.acceptJson = function () {
    this._accept = JSON_CONTENT_TYPE;
  };
  DefaultRequestBuilder.prototype.accept = function (acceptHeaderValue) {
    this._accept = acceptHeaderValue;
  };
  DefaultRequestBuilder.prototype.contentType = function (contentTypeHeaderValue) {
    this._contentType = contentTypeHeaderValue;
  };
  DefaultRequestBuilder.prototype.header = function (name, value) {
    if (value === null || typeof value === 'undefined') {
      return;
    }
    this._headerParams[name] = value;
  };
  DefaultRequestBuilder.prototype.headers = function (headersToMerge) {
    var e_2, _a;
    try {
      for (var _b = __values(Object.entries(headersToMerge)), _c = _b.next(); !_c.done; _c = _b.next()) {
        var _d = __read(_c.value, 2),
          name_1 = _d[0],
          value = _d[1];
        this._headerParams[name_1] = value;
      }
    } catch (e_2_1) {
      e_2 = {
        error: e_2_1
      };
    } finally {
      try {
        if (_c && !_c.done && (_a = _b.return)) _a.call(_b);
      } finally {
        if (e_2) throw e_2.error;
      }
    }
  };
  DefaultRequestBuilder.prototype.query = function (nameOrParameters, value, prefixFormat) {
    if (nameOrParameters === null || nameOrParameters === undefined) {
      return;
    }
    if (typeof nameOrParameters === 'string') {
      this._queryParams[nameOrParameters] = value;
      if (prefixFormat) {
        this._queryParamsPrefixFormat[nameOrParameters] = prefixFormat;
      }
      return;
    }
    this.setPrefixFormats(nameOrParameters, prefixFormat);
    this.setQueryParams(nameOrParameters);
  };
  DefaultRequestBuilder.prototype.setPrefixFormats = function (parameters, prefixFormat) {
    var e_3, _a;
    if (!prefixFormat) {
      return;
    }
    try {
      for (var _b = __values(Object.keys(parameters)), _c = _b.next(); !_c.done; _c = _b.next()) {
        var key = _c.value;
        this._queryParamsPrefixFormat[key] = prefixFormat;
      }
    } catch (e_3_1) {
      e_3 = {
        error: e_3_1
      };
    } finally {
      try {
        if (_c && !_c.done && (_a = _b.return)) _a.call(_b);
      } finally {
        if (e_3) throw e_3.error;
      }
    }
  };
  DefaultRequestBuilder.prototype.setQueryParams = function (parameters) {
    var e_4, _a;
    try {
      for (var _b = __values(Object.entries(parameters)), _c = _b.next(); !_c.done; _c = _b.next()) {
        var _d = __read(_c.value, 2),
          key = _d[0],
          val = _d[1];
        if (val !== undefined && val !== null) {
          this._queryParams[key] = val;
        }
      }
    } catch (e_4_1) {
      e_4 = {
        error: e_4_1
      };
    } finally {
      try {
        if (_c && !_c.done && (_a = _b.return)) _a.call(_b);
      } finally {
        if (e_4) throw e_4.error;
      }
    }
  };
  DefaultRequestBuilder.prototype.text = function (body) {
    this._body = body;
    this._bodyType = 'text';
    this._contentTypeOptional = TEXT_CONTENT_TYPE;
  };
  DefaultRequestBuilder.prototype.json = function (data) {
    this._body = data;
    this._bodyType = 'json';
    this._contentTypeOptional = JSON_CONTENT_TYPE;
  };
  DefaultRequestBuilder.prototype.xml = function (argName, data, rootName, schema) {
    var _a;
    var mappingResult = validateAndUnmapXml(data, schema);
    if (mappingResult.errors) {
      throw new ArgumentsValidationError((_a = {}, _a[argName] = mappingResult.errors, _a));
    }
    this._body = {
      data: data,
      rootName: rootName
    };
    this._bodyType = 'xml';
    this._contentTypeOptional = XML_CONTENT_TYPE;
  };
  DefaultRequestBuilder.prototype.stream = function (file) {
    this._stream = file;
  };
  DefaultRequestBuilder.prototype.form = function (parameters, prefixFormat) {
    this._body = parameters;
    this._formPrefixFormat = prefixFormat;
    this._bodyType = 'form';
  };
  DefaultRequestBuilder.prototype.formData = function (parameters, prefixFormat) {
    this._body = parameters;
    this._formPrefixFormat = prefixFormat;
    this._bodyType = 'form-data';
  };
  DefaultRequestBuilder.prototype.toRequest = function () {
    return {
      method: this._httpMethod,
      url: this._getQueryUrl(),
      headers: this._getHttpRequestHeaders(),
      body: this._getHttpRequestBody()
    };
  };
  DefaultRequestBuilder.prototype.intercept = function (interceptor) {
    this._interceptors.push(interceptor);
  };
  DefaultRequestBuilder.prototype.interceptRequest = function (interceptor) {
    this.intercept(function (req, opt, next) {
      return next(interceptor(req), opt);
    });
  };
  DefaultRequestBuilder.prototype.interceptResponse = function (interceptor) {
    var _this = this;
    this.intercept(function (req, opt, next) {
      return __awaiter(_this, void 0, void 0, function () {
        var _a;
        return __generator(this, function (_b) {
          switch (_b.label) {
            case 0:
              _a = interceptor;
              return [4 /*yield*/, next(req, opt)];
            case 1:
              return [2 /*return*/, _a.apply(void 0, [_b.sent()])];
          }
        });
      });
    });
  };
  DefaultRequestBuilder.prototype.defaultToError = function (apiErrorCtor, message) {
    this._apiErrorFactory = {
      apiErrorCtor: apiErrorCtor,
      message: message
    };
  };
  DefaultRequestBuilder.prototype.validateResponse = function (validate) {
    this._validateResponse = validate;
  };
  DefaultRequestBuilder.prototype.throwOn = function (statusCode, errorConstructor, isTemplate) {
    var args = [];
    for (var _i = 3; _i < arguments.length; _i++) {
      args[_i - 3] = arguments[_i];
    }
    this._errorTypes.push({
      statusCode: statusCode,
      errorConstructor: errorConstructor,
      isTemplate: isTemplate,
      args: args
    });
  };
  DefaultRequestBuilder.prototype.call = function (requestOptions) {
    return __awaiter(this, void 0, void 0, function () {
      var pipeline, _a, request, response;
      var _this = this;
      return __generator(this, function (_b) {
        switch (_b.label) {
          case 0:
            pipeline = callHttpInterceptors(this._interceptors,
            // tslint:disable-next-line:no-shadowed-variable
            function (request, opt) {
              return __awaiter(_this, void 0, void 0, function () {
                var response;
                return __generator(this, function (_a) {
                  switch (_a.label) {
                    case 0:
                      return [4 /*yield*/, this._httpClient(request, opt)];
                    case 1:
                      response = _a.sent();
                      return [2 /*return*/, {
                        request: request,
                        response: response
                      }];
                  }
                });
              });
            });
            return [4 /*yield*/, pipeline(this.toRequest(), requestOptions)];
          case 1:
            _a = _b.sent(), request = _a.request, response = _a.response;
            return [2 /*return*/, __assign(__assign({}, response), {
              request: request,
              result: undefined
            })];
        }
      });
    });
  };
  DefaultRequestBuilder.prototype.callAsText = function (requestOptions) {
    return __awaiter(this, void 0, void 0, function () {
      var result;
      return __generator(this, function (_a) {
        switch (_a.label) {
          case 0:
            return [4 /*yield*/, this.call(requestOptions)];
          case 1:
            result = _a.sent();
            if (typeof result.body !== 'string') {
              throw new Error('Could not parse body as string.'); // TODO: Replace with SDK error
            }
            return [2 /*return*/, __assign(__assign({}, result), {
              result: result.body
            })];
        }
      });
    });
  };
  DefaultRequestBuilder.prototype.callAsOptionalText = function (requestOptions) {
    return __awaiter(this, void 0, void 0, function () {
      var result;
      return __generator(this, function (_a) {
        switch (_a.label) {
          case 0:
            return [4 /*yield*/, this.call(requestOptions)];
          case 1:
            result = _a.sent();
            if (typeof result.body !== 'string') {
              return [2 /*return*/, __assign(__assign({}, result), {
                result: undefined
              })];
            }
            return [2 /*return*/, __assign(__assign({}, result), {
              result: result.body
            })];
        }
      });
    });
  };
  DefaultRequestBuilder.prototype.callAsStream = function (requestOptions) {
    return __awaiter(this, void 0, void 0, function () {
      var result;
      return __generator(this, function (_a) {
        switch (_a.label) {
          case 0:
            this.interceptRequest(function (req) {
              return __assign(__assign({}, req), {
                responseType: 'stream'
              });
            });
            return [4 /*yield*/, this.call(requestOptions)];
          case 1:
            result = _a.sent();
            return [2 /*return*/, __assign(__assign({}, result), {
              result: convertToStream(result.body)
            })];
        }
      });
    });
  };
  DefaultRequestBuilder.prototype.callAsJson = function (schema, requestOptions) {
    return __awaiter(this, void 0, void 0, function () {
      var result;
      return __generator(this, function (_a) {
        switch (_a.label) {
          case 0:
            this.interceptRequest(function (request) {
              var headers = __assign({}, request.headers);
              setHeaderIfNotSet(headers, ACCEPT_HEADER, JSON_CONTENT_TYPE);
              return __assign(__assign({}, request), {
                headers: headers
              });
            });
            return [4 /*yield*/, this.call(requestOptions)];
          case 1:
            result = _a.sent();
            return [2 /*return*/, __assign(__assign({}, result), {
              result: parseJsonResult(schema, result)
            })];
        }
      });
    });
  };
  DefaultRequestBuilder.prototype.callAsXml = function (rootName, schema, requestOptions) {
    return __awaiter(this, void 0, void 0, function () {
      var result, xmlObject, error_1, mappingResult;
      return __generator(this, function (_a) {
        switch (_a.label) {
          case 0:
            this.interceptRequest(function (request) {
              var headers = __assign({}, request.headers);
              setHeaderIfNotSet(headers, ACCEPT_HEADER, XML_CONTENT_TYPE);
              return __assign(__assign({}, request), {
                headers: headers
              });
            });
            return [4 /*yield*/, this.call(requestOptions)];
          case 1:
            result = _a.sent();
            if (result.body === '') {
              throw new Error('Could not parse body as XML. The response body is empty.');
            }
            if (typeof result.body !== 'string') {
              throw new Error('Could not parse body as XML. The response body is not a string.');
            }
            _a.label = 2;
          case 2:
            _a.trys.push([2, 4,, 5]);
            return [4 /*yield*/, this._xmlSerializer.xmlDeserialize(rootName, result.body)];
          case 3:
            xmlObject = _a.sent();
            return [3 /*break*/, 5];
          case 4:
            error_1 = _a.sent();
            throw new Error("Could not parse body as XML.\n\n".concat(error_1.message));
          case 5:
            mappingResult = validateAndMapXml(xmlObject, schema);
            if (mappingResult.errors) {
              throw new ResponseValidationError(result, mappingResult.errors);
            }
            return [2 /*return*/, __assign(__assign({}, result), {
              result: mappingResult.result
            })];
        }
      });
    });
  };
  DefaultRequestBuilder.prototype.paginate = function (createPagedIterable) {
    return createPagedIterable(this, function (req) {
      return req.updateByJsonPointer.bind(req);
    });
  };
  DefaultRequestBuilder.prototype.updateByJsonPointer = function (pointer, updater) {
    var _this = this;
    if (!pointer) {
      return this;
    }
    var targets = {
      '$request.body': function (req) {
        return req._body = updateByJsonPointer(_this._body, point, updater);
      },
      '$request.path': function (req) {
        return req._pathParams = updateByJsonPointer(_this._pathParams, point, updater);
      },
      '$request.query': function (req) {
        return req._queryParams = updateByJsonPointer(_this._queryParams, point, updater);
      },
      '$request.headers': function (req) {
        return req._headerParams = updateByJsonPointer(_this._headerParams, point, updater);
      }
    };
    var _a = __read(pointer.split('#', 2), 2),
      prefix = _a[0],
      _b = _a[1],
      point = _b === void 0 ? '' : _b;
    var paramUpdater = targets[prefix];
    if (!paramUpdater) {
      return this;
    }
    var request = this._clone();
    paramUpdater(request);
    return request;
  };
  DefaultRequestBuilder.prototype._clone = function () {
    var cloned = new DefaultRequestBuilder(this._httpClient, this._baseUrlProvider, this._apiErrorCtr, this._authenticationProvider, this._httpMethod, this._xmlSerializer, this._retryConfig, this._path, this._apiLogger);
    this.cloneParameters(cloned);
    return cloned;
  };
  DefaultRequestBuilder.prototype.cloneParameters = function (cloned) {
    cloned._accept = this._accept;
    cloned._contentType = this._contentType;
    cloned._headerParams = __assign({}, this._headerParams);
    cloned._body = this._body;
    cloned._bodyType = this._bodyType;
    cloned._stream = this._stream;
    cloned._queryParams = __assign({}, this._queryParams);
    cloned._formPrefixFormat = this._formPrefixFormat;
    cloned._pathStrings = this._pathStrings;
    cloned._pathArgs = this._pathArgs;
    cloned._pathParams = this._pathParams;
    cloned._baseUrlArg = this._baseUrlArg;
    cloned._validateResponse = this._validateResponse;
    cloned._interceptors = __spreadArray([], __read(this._interceptors), false);
    cloned._authParams = this._authParams;
    cloned._retryOption = this._retryOption;
    cloned._apiErrorFactory = __assign({}, this._apiErrorFactory);
    cloned._errorTypes = __spreadArray([], __read(this._errorTypes), false);
  };
  DefaultRequestBuilder.prototype._addResponseValidator = function () {
    var _this = this;
    this.interceptResponse(function (context) {
      var _a;
      var response = context.response;
      if (_this._validateResponse && (response.statusCode < 200 || response.statusCode >= 300)) {
        if (typeof ((_a = _this._apiErrorFactory) === null || _a === void 0 ? void 0 : _a.message) === 'undefined') {
          _this._apiErrorFactory.message = "Response status code was not ok: ".concat(response.statusCode, ".");
        }
        throw new _this._apiErrorFactory.apiErrorCtor(context, _this._apiErrorFactory.message);
      }
      return context;
    });
  };
  DefaultRequestBuilder.prototype._addApiLoggerInterceptors = function () {
    var _this = this;
    if (this._apiLogger) {
      var apiLogger_1 = this._apiLogger;
      this.intercept(function (request, options, next) {
        return __awaiter(_this, void 0, void 0, function () {
          var context;
          return __generator(this, function (_a) {
            switch (_a.label) {
              case 0:
                apiLogger_1.logRequest(request);
                return [4 /*yield*/, next(request, options)];
              case 1:
                context = _a.sent();
                apiLogger_1.logResponse(context.response);
                return [2 /*return*/, context];
            }
          });
        });
      });
    }
  };
  DefaultRequestBuilder.prototype._getQueryUrl = function () {
    var e_5, _a, _b;
    var _c;
    var queryParts = [];
    try {
      for (var _d = __values(Object.entries(this._queryParams)), _e = _d.next(); !_e.done; _e = _d.next()) {
        var _f = __read(_e.value, 2),
          key = _f[0],
          value = _f[1];
        var formatter = (_c = this._queryParamsPrefixFormat) === null || _c === void 0 ? void 0 : _c[key];
        queryParts.push(urlEncodeObject((_b = {}, _b[key] = value, _b), formatter));
      }
    } catch (e_5_1) {
      e_5 = {
        error: e_5_1
      };
    } finally {
      try {
        if (_e && !_e.done && (_a = _d.return)) _a.call(_d);
      } finally {
        if (e_5) throw e_5.error;
      }
    }
    var url = mergePath(this._baseUrlProvider(this._baseUrlArg), this._buildPath());
    if (queryParts.length === 0) {
      return sanitizeUrl(url);
    }
    var separator = url.indexOf('?') === -1 ? '?' : '&';
    return sanitizeUrl(url + separator + queryParts.join('&'));
  };
  DefaultRequestBuilder.prototype._buildPath = function () {
    var e_6, _a;
    if (this._pathStrings === undefined || this._pathArgs === undefined) {
      return this._path;
    }
    try {
      for (var _b = __values(this._pathArgs), _c = _b.next(); !_c.done; _c = _b.next()) {
        var arg = _c.value;
        if ((arg instanceof SkipEncode || arg instanceof PathParam) && arg.key !== undefined && arg.key in this._pathParams) {
          arg.value = this._pathParams[arg.key];
        }
      }
    } catch (e_6_1) {
      e_6 = {
        error: e_6_1
      };
    } finally {
      try {
        if (_c && !_c.done && (_a = _b.return)) _a.call(_b);
      } finally {
        if (e_6) throw e_6.error;
      }
    }
    return pathTemplate.apply(void 0, __spreadArray([this._pathStrings], __read(this._pathArgs), false));
  };
  DefaultRequestBuilder.prototype._getHttpRequestHeaders = function () {
    var e_7, _a;
    var headers = {};
    try {
      for (var _b = __values(Object.entries(this._headerParams)), _c = _b.next(); !_c.done; _c = _b.next()) {
        var _d = __read(_c.value, 2),
          name_2 = _d[0],
          value = _d[1];
        if (typeof value === 'object') {
          setHeader(headers, name_2, JSON.stringify(value));
          continue;
        }
        setHeader(headers, name_2, String(value));
      }
    } catch (e_7_1) {
      e_7 = {
        error: e_7_1
      };
    } finally {
      try {
        if (_c && !_c.done && (_a = _b.return)) _a.call(_b);
      } finally {
        if (e_7) throw e_7.error;
      }
    }
    if (this._accept) {
      setHeader(headers, ACCEPT_HEADER, this._accept);
    }
    if (this._contentTypeOptional) {
      setHeaderIfNotSet(headers, CONTENT_TYPE_HEADER, this._contentTypeOptional);
    }
    if (this._contentType) {
      setHeader(headers, CONTENT_TYPE_HEADER, this._contentType);
    }
    setHeader(headers, CONTENT_LENGTH_HEADER);
    return headers;
  };
  DefaultRequestBuilder.prototype._getHttpRequestBody = function () {
    if (this._stream !== undefined) {
      return {
        type: 'stream',
        content: this._stream
      };
    }
    if (this._body === undefined) {
      return undefined;
    }
    switch (this._bodyType) {
      case 'text':
        return {
          type: 'text',
          content: String(this._body)
        };
      case 'json':
        return {
          type: 'text',
          content: JSON.stringify(this._body)
        };
      case 'xml':
        return {
          type: 'text',
          content: this._xmlSerializer.xmlSerialize(this._body.data, this._body.rootName)
        };
      case 'form':
      case 'form-data':
        {
          if (typeof this._body !== 'object' || this._body === null || Array.isArray(this._body)) {
            return undefined;
          }
          var type = this._bodyType;
          var encoded = formDataEncodeObject(this._body, this._formPrefixFormat);
          var content = filterFileWrapperFromKeyValuePairs(encoded);
          return type === 'form' ? {
            type: type,
            content: content
          } : {
            type: type,
            content: encoded
          };
        }
      default:
        return undefined;
    }
  };
  DefaultRequestBuilder.prototype._addAuthentication = function () {
    var _this = this;
    this.intercept(function () {
      var args = [];
      for (var _i = 0; _i < arguments.length; _i++) {
        args[_i] = arguments[_i];
      }
      var handler = _this._authenticationProvider(_this._authParams);
      return handler.apply(void 0, __spreadArray([], __read(args), false));
    });
  };
  DefaultRequestBuilder.prototype._addRetryInterceptor = function () {
    var _this = this;
    this.intercept(function (request, options, next) {
      return __awaiter(_this, void 0, void 0, function () {
        var context, allowedWaitTime, retryCount, waitTime, timeoutError, shouldRetry, error_2;
        var _a, _b;
        return __generator(this, function (_c) {
          switch (_c.label) {
            case 0:
              allowedWaitTime = this._retryConfig.maximumRetryWaitTime;
              retryCount = 0;
              waitTime = 0;
              shouldRetry = shouldRetryRequest(this._retryOption, this._retryConfig, this._httpMethod);
              _c.label = 1;
            case 1:
              timeoutError = undefined;
              if (!(retryCount > 0)) return [3 /*break*/, 3];
              return [4 /*yield*/, new Promise(function (res) {
                return setTimeout(res, waitTime * 1000);
              })];
            case 2:
              _c.sent();
              allowedWaitTime -= waitTime;
              _c.label = 3;
            case 3:
              _c.trys.push([3, 5,, 6]);
              return [4 /*yield*/, next(request, options)];
            case 4:
              context = _c.sent();
              return [3 /*break*/, 6];
            case 5:
              error_2 = _c.sent();
              timeoutError = error_2;
              return [3 /*break*/, 6];
            case 6:
              if (shouldRetry) {
                waitTime = getRetryWaitTime(this._retryConfig, allowedWaitTime, retryCount, (_a = context === null || context === void 0 ? void 0 : context.response) === null || _a === void 0 ? void 0 : _a.statusCode, (_b = context === null || context === void 0 ? void 0 : context.response) === null || _b === void 0 ? void 0 : _b.headers, timeoutError);
                retryCount++;
              }
              _c.label = 7;
            case 7:
              if (waitTime > 0) return [3 /*break*/, 1];
              _c.label = 8;
            case 8:
              if (timeoutError) {
                throw timeoutError;
              }
              if (typeof (context === null || context === void 0 ? void 0 : context.response) === 'undefined') {
                throw new Error('Response is undefined.');
              }
              return [2 /*return*/, {
                request: request,
                response: context.response
              }];
          }
        });
      });
    });
  };
  DefaultRequestBuilder.prototype._addErrorHandlingInterceptor = function () {
    var _this = this;
    this.intercept(function (req, opt, next) {
      return __awaiter(_this, void 0, void 0, function () {
        var context, _a, _b, _c, statusCode, errorConstructor, isTemplate, args, error, e_8_1;
        var e_8, _d;
        return __generator(this, function (_e) {
          switch (_e.label) {
            case 0:
              return [4 /*yield*/, next(req, opt)];
            case 1:
              context = _e.sent();
              _e.label = 2;
            case 2:
              _e.trys.push([2, 8, 9, 10]);
              _a = __values(this._errorTypes), _b = _a.next();
              _e.label = 3;
            case 3:
              if (!!_b.done) return [3 /*break*/, 7];
              _c = _b.value, statusCode = _c.statusCode, errorConstructor = _c.errorConstructor, isTemplate = _c.isTemplate, args = _c.args;
              if (!(typeof statusCode === 'number' && context.response.statusCode === statusCode || typeof statusCode !== 'number' && context.response.statusCode >= statusCode[0] && context.response.statusCode <= statusCode[1])) return [3 /*break*/, 6];
              if (isTemplate && args.length > 0) {
                args[0] = updateErrorMessage(args[0], context.response);
              }
              error = new (errorConstructor.bind.apply(errorConstructor, __spreadArray([void 0, context], __read(args), false)))();
              if (!(errorConstructor.prototype instanceof ApiError)) return [3 /*break*/, 5];
              // Load result only for the sub classes of ApiError
              return [4 /*yield*/, loadResult(error)];
            case 4:
              // Load result only for the sub classes of ApiError
              _e.sent();
              _e.label = 5;
            case 5:
              throw error;
            case 6:
              _b = _a.next();
              return [3 /*break*/, 3];
            case 7:
              return [3 /*break*/, 10];
            case 8:
              e_8_1 = _e.sent();
              e_8 = {
                error: e_8_1
              };
              return [3 /*break*/, 10];
            case 9:
              try {
                if (_b && !_b.done && (_d = _a.return)) _d.call(_a);
              } finally {
                if (e_8) throw e_8.error;
              }
              return [7 /*endfinally*/];
            case 10:
              return [2 /*return*/, context];
          }
        });
      });
    });
  };
  return DefaultRequestBuilder;
}();
function createRequestBuilderFactory(httpClient, baseUrlProvider, apiErrorConstructor, authenticationProvider, retryConfig, xmlSerializer, apiLogger) {
  if (xmlSerializer === void 0) {
    xmlSerializer = new XmlSerialization();
  }
  return function (httpMethod, path) {
    return new DefaultRequestBuilder(httpClient, baseUrlProvider, apiErrorConstructor, authenticationProvider, httpMethod, xmlSerializer, retryConfig, path, apiLogger);
  };
}
function mergePath(left, right) {
  if (!right || right === '') {
    return left;
  }
  // remove all occurances of `/` (if any) from the end of left path
  left = left.replace('/', ' ').trimEnd().replace(' ', '/');
  // remove all occurances of `/` (if any) from the start of right sub-path
  right = right.replace('/', ' ').trimStart().replace(' ', '/');
  return "".concat(left, "/").concat(right);
}
function parseJsonResult(schema, res) {
  if (typeof res.body !== 'string') {
    throw new Error('Could not parse body as JSON. The response body is not a string.');
  }
  if (res.body.trim() === '') {
    var resEmptyErr_1 = new Error('Could not parse body as JSON. The response body is empty.');
    return validateJson(schema, null, function (_) {
      return resEmptyErr_1;
    });
  }
  var parsed;
  try {
    parsed = JSON.parse(res.body);
  } catch (error) {
    var resUnParseErr_1 = new Error("Could not parse body as JSON.\n\n".concat(error.message));
    return validateJson(schema, res.body, function (_) {
      return resUnParseErr_1;
    });
  }
  var resInvalidErr = function (errors) {
    return new ResponseValidationError(res, errors);
  };
  return validateJson(schema, parsed, function (errors) {
    return resInvalidErr(errors);
  });
}
function validateJson(schema, value, errorCreater) {
  var mappingResult = validateAndMap(value, schema);
  if (mappingResult.errors) {
    throw errorCreater(mappingResult.errors);
  }
  return mappingResult.result;
}
export { DefaultRequestBuilder, createRequestBuilderFactory, pathParam, skipEncode };