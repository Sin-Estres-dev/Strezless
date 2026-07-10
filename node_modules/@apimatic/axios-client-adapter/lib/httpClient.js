"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.createFormDataOptions = exports.isBlob = exports.HttpClient = exports.DEFAULT_TIMEOUT = exports.DEFAULT_AXIOS_CONFIG_OVERRIDES = void 0;
var tslib_1 = require("tslib");
var axios_1 = require("axios");
var axios_2 = tslib_1.__importDefault(require("axios"));
var detect_node_1 = tslib_1.__importDefault(require("detect-node"));
var form_data_1 = tslib_1.__importDefault(require("form-data"));
var http_headers_1 = require("@apimatic/http-headers");
var core_interfaces_1 = require("@apimatic/core-interfaces");
var http_query_1 = require("@apimatic/http-query");
var file_wrapper_1 = require("@apimatic/file-wrapper");
var proxy_1 = require("@apimatic/proxy");
exports.DEFAULT_AXIOS_CONFIG_OVERRIDES = {
    transformResponse: [],
};
exports.DEFAULT_TIMEOUT = 30 * 1000;
/**
 * HTTP client implementation.
 *
 * This implementation is a wrapper over the Axios client.
 */
var HttpClient = /** @class */ (function () {
    function HttpClient(abortErrorFactory, _a) {
        var _b = _a === void 0 ? {} : _a, clientConfigOverrides = _b.clientConfigOverrides, _c = _b.timeout, timeout = _c === void 0 ? exports.DEFAULT_TIMEOUT : _c, httpAgent = _b.httpAgent, httpsAgent = _b.httpsAgent, proxySettings = _b.proxySettings;
        this._proxySettings = proxySettings;
        this._timeout = timeout;
        this._axiosInstance = axios_2.default.create(tslib_1.__assign(tslib_1.__assign(tslib_1.__assign({}, exports.DEFAULT_AXIOS_CONFIG_OVERRIDES), clientConfigOverrides), { httpAgent: httpAgent, httpsAgent: httpsAgent }));
        this._abortErrorFactory = abortErrorFactory;
    }
    /** Converts an HttpRequest object to an Axios request. */
    HttpClient.prototype.convertHttpRequest = function (req) {
        var e_1, _a;
        var _b, _c, _d, _e;
        var newRequest = {
            method: req.method,
            url: req.url,
            responseType: 'text',
            headers: tslib_1.__assign({}, req.headers),
        };
        var headers = new axios_1.AxiosHeaders(tslib_1.__assign({}, req.headers));
        if (req.auth) {
            // Set basic auth credentials if provided
            newRequest.auth = {
                username: req.auth.username,
                password: req.auth.password || '',
            };
        }
        var requestBody = req.body;
        if ((requestBody === null || requestBody === void 0 ? void 0 : requestBody.type) === 'text') {
            newRequest.data = requestBody.content;
        }
        else if ((requestBody === null || requestBody === void 0 ? void 0 : requestBody.type) === 'form-data' &&
            requestBody.content.some(function (item) { return (0, file_wrapper_1.isFileWrapper)(item.value); })) {
            // Create multipart request if a file is present
            var form = new form_data_1.default();
            try {
                for (var _f = tslib_1.__values(requestBody.content), _g = _f.next(); !_g.done; _g = _f.next()) {
                    var iter = _g.value;
                    if ((0, file_wrapper_1.isFileWrapper)(iter.value)) {
                        var fileData = iter.value.file;
                        // Make sure Blob has the correct content type if provided
                        if (isBlob(fileData) && ((_b = iter.value.options) === null || _b === void 0 ? void 0 : _b.contentType)) {
                            fileData = new Blob([fileData], {
                                type: iter.value.options.contentType,
                            });
                        }
                        form.append(iter.key, fileData, tslib_1.__assign(tslib_1.__assign({}, createFormDataOptions(((_c = iter.value.options) === null || _c === void 0 ? void 0 : _c.headers) || {})), { filename: (_d = iter.value.options) === null || _d === void 0 ? void 0 : _d.filename }));
                    }
                    else if ((0, core_interfaces_1.isFormDataWrapper)(iter.value)) {
                        form.append(iter.key, iter.value.data, createFormDataOptions(iter.value.headers || {}));
                    }
                    else {
                        form.append(iter.key, iter.value);
                    }
                }
            }
            catch (e_1_1) { e_1 = { error: e_1_1 }; }
            finally {
                try {
                    if (_g && !_g.done && (_a = _f.return)) _a.call(_f);
                }
                finally { if (e_1) throw e_1.error; }
            }
            newRequest.data = form;
            headers = headers.concat(form.getHeaders());
        }
        else if ((requestBody === null || requestBody === void 0 ? void 0 : requestBody.type) === 'form-data' ||
            (requestBody === null || requestBody === void 0 ? void 0 : requestBody.type) === 'form') {
            // Create form-urlencoded request
            headers = headers.set(http_headers_1.CONTENT_TYPE_HEADER, http_headers_1.FORM_URLENCODED_CONTENT_TYPE);
            newRequest.data = (0, http_query_1.urlEncodeKeyValuePairs)(requestBody.content);
        }
        else if ((requestBody === null || requestBody === void 0 ? void 0 : requestBody.type) === 'stream') {
            var contentType = 'application/octet-stream';
            if (isBlob(requestBody.content.file) && requestBody.content.file.type) {
                // Set Blob mime type as the content-type header if present
                contentType = requestBody.content.file.type;
            }
            else if ((_e = requestBody.content.options) === null || _e === void 0 ? void 0 : _e.contentType) {
                // Otherwise, use the content type if available.
                contentType = requestBody.content.options.contentType;
            }
            headers = headers.set(http_headers_1.CONTENT_TYPE_HEADER, contentType, false);
            newRequest.data = requestBody.content.file;
        }
        if (req.responseType === 'stream') {
            newRequest.responseType = detect_node_1.default ? 'stream' : 'blob';
        }
        // Prevent superagent from converting any status code to error
        newRequest.validateStatus = function () { return true; };
        // Set 30 seconds timeout
        newRequest.timeout = this._timeout;
        // set headers
        newRequest.headers = headers;
        this.setProxyAgent(newRequest);
        return newRequest;
    };
    /** Converts an Axios response to an HttpResponse object. */
    HttpClient.prototype.convertHttpResponse = function (resp) {
        return {
            body: resp.data,
            headers: this.convertAxiosResponseHeadersToHttpResponseHeaders(resp.headers),
            statusCode: resp.status,
        };
    };
    HttpClient.prototype.convertAxiosResponseHeadersToHttpResponseHeaders = function (axiosHeaders) {
        var httpResponseHeaders = {};
        // Iterate through each property of AxiosResponseHeaders
        for (var key in axiosHeaders) {
            // Check if the property is not a function (AxiosHeaders may have methods)
            if (typeof axiosHeaders[key] !== 'function') {
                // Convert property key to lowercase as HTTP headers are case-insensitive
                var lowercaseKey = key.toLowerCase();
                // Assign the value to HttpResponse headers
                httpResponseHeaders[lowercaseKey] = String(axiosHeaders[key]);
            }
        }
        return httpResponseHeaders;
    };
    /**
     * Executes the HttpRequest with the given options and returns the HttpResponse
     * or throws an error.
     */
    HttpClient.prototype.executeRequest = function (request, requestOptions) {
        return tslib_1.__awaiter(this, void 0, void 0, function () {
            var axiosRequest, cancelToken_1, _a, error_1;
            return tslib_1.__generator(this, function (_b) {
                switch (_b.label) {
                    case 0:
                        axiosRequest = this.convertHttpRequest(request);
                        if (requestOptions === null || requestOptions === void 0 ? void 0 : requestOptions.abortSignal) {
                            // throw if already aborted; do not place HTTP call
                            if (requestOptions.abortSignal.aborted) {
                                throw this.abortError();
                            }
                            cancelToken_1 = axios_2.default.CancelToken.source();
                            axiosRequest.cancelToken = cancelToken_1.token;
                            // attach abort event handler
                            requestOptions.abortSignal.addEventListener('abort', function () {
                                cancelToken_1.cancel();
                            });
                        }
                        _b.label = 1;
                    case 1:
                        _b.trys.push([1, 3, , 4]);
                        _a = this.convertHttpResponse;
                        return [4 /*yield*/, this._axiosInstance(axiosRequest)];
                    case 2: return [2 /*return*/, _a.apply(this, [_b.sent()])];
                    case 3:
                        error_1 = _b.sent();
                        // abort error should be thrown as the AbortError
                        if (axios_2.default.isCancel(error_1)) {
                            throw this.abortError();
                        }
                        throw error_1;
                    case 4: return [2 /*return*/];
                }
            });
        });
    };
    HttpClient.prototype.setProxyAgent = function (axiosRequest) {
        if (!this._proxySettings || !axiosRequest.url) {
            return;
        }
        var proxyAgents = (0, proxy_1.createProxyAgents)(this._proxySettings);
        var protocol = new URL(axiosRequest.url).protocol;
        if (protocol === 'https:') {
            axiosRequest.httpsAgent = proxyAgents === null || proxyAgents === void 0 ? void 0 : proxyAgents.httpsAgent;
        }
        else if (protocol === 'http:') {
            axiosRequest.httpAgent = proxyAgents === null || proxyAgents === void 0 ? void 0 : proxyAgents.httpAgent;
        }
    };
    HttpClient.prototype.abortError = function () {
        return new this._abortErrorFactory('The HTTP call was aborted.');
    };
    return HttpClient;
}());
exports.HttpClient = HttpClient;
/**
 * Check whether value is an instance of Blob
 *
 * @remark
 * Reference: https://github.com/sindresorhus/is-blob/blob/master/index.js
 *
 * @param value Value to check
 * @returns True if the value is a Blob instance
 */
function isBlob(value) {
    if (typeof Blob === 'undefined') {
        return false;
    }
    return (value instanceof Blob ||
        Object.prototype.toString.call(value) === '[object Blob]');
}
exports.isBlob = isBlob;
function createFormDataOptions(headers) {
    var headerKey = (0, http_headers_1.lookupCaseInsensitive)(headers, 'content-type');
    if (!headerKey) {
        return {
            header: headers,
        };
    }
    var contentType = headers[headerKey];
    delete headers[headerKey];
    return {
        contentType: contentType,
        header: headers,
    };
}
exports.createFormDataOptions = createFormDataOptions;
