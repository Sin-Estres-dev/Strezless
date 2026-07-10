import JSONBig from '@apimatic/json-bigint';
import { FileWrapper } from '@apimatic/file-wrapper';
import {
  deprecated,
  sanitizeUrl,
  updateByJsonPointer,
  updateErrorMessage,
} from '../apiHelper';
import {
  ApiResponse,
  AuthenticatorInterface,
  HttpContext,
  HttpMethod,
  HttpRequest,
  HttpInterceptorInterface,
  RequestOptions,
  RetryConfiguration,
  ApiLoggerInterface,
  HttpClientInterface,
  HttpRequestBody,
  PagedAsyncIterable,
} from '../coreInterfaces';
import { ArgumentsValidationError } from '../errors/argumentsValidationError';
import { ResponseValidationError } from '../errors/responseValidationError';
import {
  Schema,
  SchemaValidationError,
  validateAndMap,
  validateAndMapXml,
  validateAndUnmapXml,
} from '../schema';
import {
  ACCEPT_HEADER,
  CONTENT_LENGTH_HEADER,
  CONTENT_TYPE_HEADER,
  JSON_CONTENT_TYPE,
  setHeader,
  setHeaderIfNotSet,
  TEXT_CONTENT_TYPE,
  XML_CONTENT_TYPE,
} from './httpHeaders';
import { callHttpInterceptors } from './httpInterceptor';
import {
  pathTemplate,
  PathTemplatePrimitiveTypes,
  PathTemplateTypes,
  SkipEncode,
} from './pathTemplate';
import {
  filterFileWrapperFromKeyValuePairs,
  formDataEncodeObject,
  urlEncodeObject,
  ArrayPrefixFunction,
} from './queryString';
import { prepareArgs } from './validate';
import {
  getRetryWaitTime,
  shouldRetryRequest,
  RequestRetryOption,
} from './retryConfiguration';
import { convertToStream } from '@apimatic/convert-to-stream';
import { XmlSerializerInterface, XmlSerialization } from '../xml/xmlSerializer';
import { ApiError, loadResult } from '../errors/apiError';
import { PathParam } from './pathParam';

export type RequestBuilderFactory<BaseUrlParamType, AuthParams> = (
  httpMethod: HttpMethod,
  path?: string
) => RequestBuilder<BaseUrlParamType, AuthParams>;

const JSON = JSONBig();

export function skipEncode<T extends PathTemplatePrimitiveTypes>(
  value: T,
  key?: string
): SkipEncode<T> {
  return new SkipEncode(value, key);
}

export function pathParam<T extends PathTemplatePrimitiveTypes>(
  value: T,
  key: string
): PathParam<T> {
  return new PathParam(value, key);
}

export type ApiErrorConstructor = new (
  response: HttpContext,
  message: string
) => any;

export interface ErrorType<ErrorCtorArgs extends any[]> {
  statusCode: number | [number, number];
  errorConstructor: new (response: HttpContext, ...args: ErrorCtorArgs) => any;
  isTemplate?: boolean;
  args: ErrorCtorArgs;
}

export interface ApiErrorFactory {
  apiErrorCtor: ApiErrorConstructor;
  message?: string | undefined;
}

export interface RequestBuilder<BaseUrlParamType, AuthParams> {
  deprecated(methodName: string, message?: string): void;
  prepareArgs: typeof prepareArgs;
  method(httpMethodName: HttpMethod): void;
  baseUrl(arg: BaseUrlParamType): void;
  authenticate(params: AuthParams): void;
  appendPath(path: string): void;
  appendTemplatePath(
    strings: TemplateStringsArray,
    ...args: PathTemplateTypes[]
  ): void;
  acceptJson(): void;
  accept(acceptHeaderValue: string): void;
  contentType(contentTypeHeaderValue: string): void;
  header(name: string, value?: unknown): void;
  headers(headersToMerge: Record<string, string>): void;
  query(
    name: string,
    value: unknown | Record<string, unknown>,
    prefixFormat?: ArrayPrefixFunction
  ): void;
  query(
    parameters?: Record<string, unknown> | null,
    prefixFormat?: ArrayPrefixFunction
  ): void;
  form(
    parameters: Record<string, unknown>,
    prefixFormat?: ArrayPrefixFunction
  ): void;
  formData(
    parameters: Record<string, unknown>,
    prefixFormat?: ArrayPrefixFunction
  ): void;
  text(body: string | number | bigint | boolean | null | undefined): void;
  json(data: unknown): void;
  requestRetryOption(option: RequestRetryOption): void;
  xml<T>(
    argName: string,
    data: T,
    rootName: string,
    schema: Schema<T, any>
  ): void;
  stream(file?: FileWrapper): void;
  toRequest(): HttpRequest;
  intercept(
    interceptor: HttpInterceptorInterface<RequestOptions | undefined>
  ): void;
  interceptRequest(interceptor: (request: HttpRequest) => HttpRequest): void;
  interceptResponse(interceptor: (response: HttpContext) => HttpContext): void;
  defaultToError(apiErrorCtor: ApiErrorConstructor, message?: string): void;
  validateResponse(validate: boolean): void;
  throwOn<ErrorCtorArgs extends any[]>(
    statusCode: number | [number, number],
    errorConstructor: new (
      response: HttpContext,
      ...args: ErrorCtorArgs
    ) => any,
    ...args: ErrorCtorArgs
  ): void;
  throwOn<ErrorCtorArgs extends any[]>(
    statusCode: number | [number, number],
    errorConstructor: new (
      response: HttpContext,
      ...args: ErrorCtorArgs
    ) => any,
    isTemplate: boolean,
    ...args: ErrorCtorArgs
  ): void;
  updateByJsonPointer(
    pointer: string | null,
    updater: (value: any) => any
  ): RequestBuilder<BaseUrlParamType, AuthParams>;
  paginate<TItem, TPagedResponse>(
    createPagedIterable: (
      req: this,
      updater: (
        req: this
      ) => (pointer: string | null, setter: (value: any) => any) => this
    ) => PagedAsyncIterable<TItem, TPagedResponse>
  ): PagedAsyncIterable<TItem, TPagedResponse>;
  call(requestOptions?: RequestOptions): Promise<ApiResponse<void>>;
  callAsJson<T>(
    schema: Schema<T, any>,
    requestOptions?: RequestOptions
  ): Promise<ApiResponse<T>>;
  callAsStream(
    requestOptions?: RequestOptions
  ): Promise<ApiResponse<NodeJS.ReadableStream | Blob>>;
  callAsText(requestOptions?: RequestOptions): Promise<ApiResponse<string>>;
  callAsOptionalText(
    requestOptions?: RequestOptions
  ): Promise<ApiResponse<string | undefined>>;
  callAsXml<T>(
    rootName: string,
    schema: Schema<T, any>,
    requestOptions?: RequestOptions
  ): Promise<ApiResponse<T>>;
  callAsXml<T>(
    rootName: string,
    schema: Schema<T, any>,
    requestOptions?: RequestOptions
  ): Promise<ApiResponse<T>>;
}

export class DefaultRequestBuilder<BaseUrlParamType, AuthParams>
  implements RequestBuilder<BaseUrlParamType, AuthParams>
{
  protected _queryParams: Record<string, unknown> = {};
  protected _pathParams: Record<string, unknown> = {};
  protected _headerParams: Record<string, unknown> = {};
  protected _body?: any;
  protected _accept?: string;
  protected _contentType?: string;
  protected _contentTypeOptional?: string;
  protected _bodyType?: 'text' | 'json' | 'xml' | 'form' | 'form-data';
  protected _formPrefixFormat?: ArrayPrefixFunction;
  protected _queryParamsPrefixFormat: Record<string, ArrayPrefixFunction>;
  protected _stream?: FileWrapper;
  protected _pathStrings?: TemplateStringsArray;
  protected _pathArgs?: PathTemplateTypes[];
  protected _baseUrlArg?: BaseUrlParamType;
  protected _validateResponse: boolean;
  protected _interceptors: Array<
    HttpInterceptorInterface<RequestOptions | undefined>
  >;
  protected _authParams?: AuthParams;
  protected _retryOption: RequestRetryOption;
  protected _apiErrorFactory: ApiErrorFactory;
  protected _errorTypes: Array<ErrorType<any>>;
  public prepareArgs: typeof prepareArgs;

  constructor(
    protected _httpClient: HttpClientInterface,
    protected _baseUrlProvider: (arg?: BaseUrlParamType) => string,
    protected _apiErrorCtr: ApiErrorConstructor,
    protected _authenticationProvider: AuthenticatorInterface<AuthParams>,
    protected _httpMethod: HttpMethod,
    protected _xmlSerializer: XmlSerializerInterface,
    protected _retryConfig: RetryConfiguration,
    protected _path?: string,
    protected _apiLogger?: ApiLoggerInterface
  ) {
    this._queryParamsPrefixFormat = {};
    this._interceptors = [];
    this._errorTypes = [];
    this._validateResponse = true;
    this._apiErrorFactory = { apiErrorCtor: _apiErrorCtr };
    this._addResponseValidator();
    this._addAuthentication();
    this._addRetryInterceptor();
    this._addErrorHandlingInterceptor();
    this._addApiLoggerInterceptors();

    this._retryOption = RequestRetryOption.Default;
    this.prepareArgs = prepareArgs.bind(this);
  }
  public authenticate(params: AuthParams): void {
    this._authParams = params;
  }
  public requestRetryOption(option: RequestRetryOption): void {
    this._retryOption = option;
  }
  public deprecated(methodName: string, message?: string): void {
    deprecated(methodName, message);
  }
  public appendTemplatePath(
    strings: TemplateStringsArray,
    ...args: PathTemplateTypes[]
  ): void {
    this._pathStrings = strings;
    this._pathArgs = args;
    for (const arg of args) {
      if (
        (arg instanceof SkipEncode || arg instanceof PathParam) &&
        arg.key !== undefined
      ) {
        this._pathParams[arg.key] = arg.value;
      }
    }
  }
  public method(httpMethodName: HttpMethod): void {
    this._httpMethod = httpMethodName;
  }
  public baseUrl(arg: BaseUrlParamType): void {
    this._baseUrlArg = arg;
  }
  public appendPath(path: string): void {
    this._path = this._path ? mergePath(this._path, path) : path;
  }
  public acceptJson(): void {
    this._accept = JSON_CONTENT_TYPE;
  }
  public accept(acceptHeaderValue: string): void {
    this._accept = acceptHeaderValue;
  }
  public contentType(contentTypeHeaderValue: string): void {
    this._contentType = contentTypeHeaderValue;
  }
  public header(name: string, value?: unknown): void {
    if (value === null || typeof value === 'undefined') {
      return;
    }
    this._headerParams[name] = value;
  }
  public headers(headersToMerge: Record<string, string>): void {
    for (const [name, value] of Object.entries(headersToMerge)) {
      this._headerParams[name] = value;
    }
  }
  public query(
    name: string,
    value: unknown | Record<string, unknown>,
    prefixFormat?: ArrayPrefixFunction
  ): void;
  public query(
    parameters?: Record<string, unknown> | null,
    prefixFormat?: ArrayPrefixFunction
  ): void;
  public query(
    nameOrParameters: string | Record<string, unknown> | null | undefined,
    value?: unknown,
    prefixFormat?: ArrayPrefixFunction
  ): void {
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
  }

  private setPrefixFormats(
    parameters: Record<string, unknown>,
    prefixFormat?: ArrayPrefixFunction
  ): void {
    if (!prefixFormat) {
      return;
    }
    for (const key of Object.keys(parameters)) {
      this._queryParamsPrefixFormat[key] = prefixFormat;
    }
  }

  private setQueryParams(parameters: Record<string, unknown>): void {
    for (const [key, val] of Object.entries(parameters)) {
      if (val !== undefined && val !== null) {
        this._queryParams[key] = val;
      }
    }
  }
  public text(
    body: string | number | bigint | boolean | null | undefined
  ): void {
    this._body = body;
    this._bodyType = 'text';
    this._contentTypeOptional = TEXT_CONTENT_TYPE;
  }
  public json(data: unknown): void {
    this._body = data;
    this._bodyType = 'json';
    this._contentTypeOptional = JSON_CONTENT_TYPE;
  }
  public xml<T>(
    argName: string,
    data: T,
    rootName: string,
    schema: Schema<T, any>
  ): void {
    const mappingResult = validateAndUnmapXml(data, schema);
    if (mappingResult.errors) {
      throw new ArgumentsValidationError({ [argName]: mappingResult.errors });
    }
    this._body = {
      data,
      rootName,
    };
    this._bodyType = 'xml';
    this._contentTypeOptional = XML_CONTENT_TYPE;
  }
  public stream(file?: FileWrapper): void {
    this._stream = file;
  }
  public form(
    parameters: Record<string, unknown>,
    prefixFormat?: ArrayPrefixFunction
  ): void {
    this._body = parameters;
    this._formPrefixFormat = prefixFormat;
    this._bodyType = 'form';
  }
  public formData(
    parameters: Record<string, unknown>,
    prefixFormat?: ArrayPrefixFunction
  ): void {
    this._body = parameters;
    this._formPrefixFormat = prefixFormat;
    this._bodyType = 'form-data';
  }

  public toRequest(): HttpRequest {
    return {
      method: this._httpMethod,
      url: this._getQueryUrl(),
      headers: this._getHttpRequestHeaders(),
      body: this._getHttpRequestBody(),
    };
  }
  public intercept(
    interceptor: HttpInterceptorInterface<RequestOptions | undefined>
  ): void {
    this._interceptors.push(interceptor);
  }
  public interceptRequest(
    interceptor: (httpRequest: HttpRequest) => HttpRequest
  ): void {
    this.intercept((req, opt, next) => next(interceptor(req), opt));
  }
  public interceptResponse(
    interceptor: (response: HttpContext) => HttpContext
  ): void {
    this.intercept(async (req, opt, next) => interceptor(await next(req, opt)));
  }
  public defaultToError(
    apiErrorCtor: ApiErrorConstructor,
    message?: string
  ): void {
    this._apiErrorFactory = { apiErrorCtor, message };
  }
  public validateResponse(validate: boolean): void {
    this._validateResponse = validate;
  }
  public throwOn<ErrorCtorArgs extends any[]>(
    statusCode: number | [number, number],
    errorConstructor: new (
      response: HttpContext,
      ...args: ErrorCtorArgs
    ) => any,
    ...args: ErrorCtorArgs
  ): void;
  public throwOn<ErrorCtorArgs extends any[]>(
    statusCode: number | [number, number],
    errorConstructor: new (
      response: HttpContext,
      ...args: ErrorCtorArgs
    ) => any,
    isTemplate?: boolean,
    ...args: ErrorCtorArgs
  ): void {
    this._errorTypes.push({ statusCode, errorConstructor, isTemplate, args });
  }
  public async call(
    requestOptions?: RequestOptions
  ): Promise<ApiResponse<void>> {
    // Prepare the HTTP pipeline
    const pipeline = callHttpInterceptors(
      this._interceptors,
      // tslint:disable-next-line:no-shadowed-variable
      async (request, opt) => {
        // tslint:disable-next-line:no-shadowed-variable
        const response = await this._httpClient(request, opt);
        return { request, response };
      }
    );

    // Execute HTTP pipeline
    const { request, response } = await pipeline(
      this.toRequest(),
      requestOptions
    );

    return { ...response, request, result: undefined };
  }
  public async callAsText(
    requestOptions?: RequestOptions
  ): Promise<ApiResponse<string>> {
    const result = await this.call(requestOptions);
    if (typeof result.body !== 'string') {
      throw new Error('Could not parse body as string.'); // TODO: Replace with SDK error
    }
    return { ...result, result: result.body };
  }
  public async callAsOptionalText(
    requestOptions?: RequestOptions
  ): Promise<ApiResponse<string | undefined>> {
    const result = await this.call(requestOptions);
    if (typeof result.body !== 'string') {
      return { ...result, result: undefined };
    }
    return { ...result, result: result.body };
  }
  public async callAsStream(
    requestOptions?: RequestOptions
  ): Promise<ApiResponse<NodeJS.ReadableStream | Blob>> {
    this.interceptRequest((req) => ({ ...req, responseType: 'stream' }));
    const result = await this.call(requestOptions);
    return { ...result, result: convertToStream(result.body) };
  }
  public async callAsJson<T>(
    schema: Schema<T>,
    requestOptions?: RequestOptions
  ): Promise<ApiResponse<T>> {
    this.interceptRequest((request) => {
      const headers = { ...request.headers };
      setHeaderIfNotSet(headers, ACCEPT_HEADER, JSON_CONTENT_TYPE);
      return { ...request, headers };
    });
    const result = await this.call(requestOptions);

    return { ...result, result: parseJsonResult(schema, result) };
  }
  public async callAsXml<T>(
    rootName: string,
    schema: Schema<T, any>,
    requestOptions?: RequestOptions
  ): Promise<ApiResponse<T>> {
    this.interceptRequest((request) => {
      const headers = { ...request.headers };
      setHeaderIfNotSet(headers, ACCEPT_HEADER, XML_CONTENT_TYPE);
      return { ...request, headers };
    });
    const result = await this.call(requestOptions);
    if (result.body === '') {
      throw new Error(
        'Could not parse body as XML. The response body is empty.'
      );
    }
    if (typeof result.body !== 'string') {
      throw new Error(
        'Could not parse body as XML. The response body is not a string.'
      );
    }
    let xmlObject: unknown;
    try {
      xmlObject = await this._xmlSerializer.xmlDeserialize(
        rootName,
        result.body
      );
    } catch (error) {
      throw new Error(`Could not parse body as XML.\n\n${error.message}`);
    }
    const mappingResult = validateAndMapXml(xmlObject, schema);
    if (mappingResult.errors) {
      throw new ResponseValidationError(result, mappingResult.errors);
    }
    return { ...result, result: mappingResult.result };
  }
  public paginate<TItem, TPagedResponse>(
    createPagedIterable: (
      req: this,
      updater: (
        req: this
      ) => (pointer: string | null, setter: (value: any) => any) => this
    ) => PagedAsyncIterable<TItem, TPagedResponse>
  ): PagedAsyncIterable<TItem, TPagedResponse> {
    return createPagedIterable(this, (req) =>
      req.updateByJsonPointer.bind(req)
    );
  }
  public updateByJsonPointer(
    pointer: string | null,
    updater: (value: any) => any
  ): RequestBuilder<BaseUrlParamType, AuthParams> {
    if (!pointer) {
      return this;
    }
    const targets: Record<
      string,
      (req: DefaultRequestBuilder<BaseUrlParamType, AuthParams>) => void
    > = {
      '$request.body': (req) =>
        (req._body = updateByJsonPointer(this._body, point, updater)),
      '$request.path': (req) =>
        (req._pathParams = updateByJsonPointer(
          this._pathParams,
          point,
          updater
        )),
      '$request.query': (req) =>
        (req._queryParams = updateByJsonPointer(
          this._queryParams,
          point,
          updater
        )),
      '$request.headers': (req) =>
        (req._headerParams = updateByJsonPointer(
          this._headerParams,
          point,
          updater
        )),
    };
    const [prefix, point = ''] = pointer.split('#', 2);
    const paramUpdater = targets[prefix];
    if (!paramUpdater) {
      return this;
    }
    const request = this._clone();
    paramUpdater(request);
    return request;
  }
  private _clone(): DefaultRequestBuilder<BaseUrlParamType, AuthParams> {
    const cloned = new DefaultRequestBuilder(
      this._httpClient,
      this._baseUrlProvider,
      this._apiErrorCtr,
      this._authenticationProvider,
      this._httpMethod,
      this._xmlSerializer,
      this._retryConfig,
      this._path,
      this._apiLogger
    );

    this.cloneParameters(cloned);
    return cloned;
  }
  private cloneParameters(
    cloned: DefaultRequestBuilder<BaseUrlParamType, AuthParams>
  ): void {
    cloned._accept = this._accept;
    cloned._contentType = this._contentType;
    cloned._headerParams = { ...this._headerParams };
    cloned._body = this._body;
    cloned._bodyType = this._bodyType;
    cloned._stream = this._stream;
    cloned._queryParams = { ...this._queryParams };
    cloned._formPrefixFormat = this._formPrefixFormat;
    cloned._pathStrings = this._pathStrings;
    cloned._pathArgs = this._pathArgs;
    cloned._pathParams = this._pathParams;
    cloned._baseUrlArg = this._baseUrlArg;
    cloned._validateResponse = this._validateResponse;
    cloned._interceptors = [...this._interceptors];
    cloned._authParams = this._authParams;
    cloned._retryOption = this._retryOption;
    cloned._apiErrorFactory = { ...this._apiErrorFactory };
    cloned._errorTypes = [...this._errorTypes];
  }
  private _addResponseValidator(): void {
    this.interceptResponse((context) => {
      const { response } = context;
      if (
        this._validateResponse &&
        (response.statusCode < 200 || response.statusCode >= 300)
      ) {
        if (typeof this._apiErrorFactory?.message === 'undefined') {
          this._apiErrorFactory.message = `Response status code was not ok: ${response.statusCode}.`;
        }
        throw new this._apiErrorFactory.apiErrorCtor(
          context,
          this._apiErrorFactory.message
        );
      }
      return context;
    });
  }
  private _addApiLoggerInterceptors(): void {
    if (this._apiLogger) {
      const apiLogger = this._apiLogger;

      this.intercept(async (request, options, next) => {
        apiLogger.logRequest(request);
        const context = await next(request, options);
        apiLogger.logResponse(context.response);
        return context;
      });
    }
  }
  private _getQueryUrl(): string {
    const queryParts: string[] = [];
    for (const [key, value] of Object.entries(this._queryParams)) {
      const formatter = this._queryParamsPrefixFormat?.[key];
      queryParts.push(urlEncodeObject({ [key]: value }, formatter));
    }

    const url = mergePath(
      this._baseUrlProvider(this._baseUrlArg),
      this._buildPath()
    );

    if (queryParts.length === 0) {
      return sanitizeUrl(url);
    }

    const separator = url.indexOf('?') === -1 ? '?' : '&';
    return sanitizeUrl(url + separator + queryParts.join('&'));
  }
  private _buildPath(): string | undefined {
    if (this._pathStrings === undefined || this._pathArgs === undefined) {
      return this._path;
    }
    for (const arg of this._pathArgs) {
      if (
        (arg instanceof SkipEncode || arg instanceof PathParam) &&
        arg.key !== undefined &&
        arg.key in this._pathParams
      ) {
        arg.value = this._pathParams[arg.key];
      }
    }
    return pathTemplate(this._pathStrings, ...this._pathArgs);
  }
  private _getHttpRequestHeaders(): Record<string, string> {
    const headers: Record<string, string> = {};

    for (const [name, value] of Object.entries(this._headerParams)) {
      if (typeof value === 'object') {
        setHeader(headers, name, JSON.stringify(value));
        continue;
      }
      setHeader(headers, name, String(value));
    }

    if (this._accept) {
      setHeader(headers, ACCEPT_HEADER, this._accept);
    }

    if (this._contentTypeOptional) {
      setHeaderIfNotSet(
        headers,
        CONTENT_TYPE_HEADER,
        this._contentTypeOptional
      );
    }

    if (this._contentType) {
      setHeader(headers, CONTENT_TYPE_HEADER, this._contentType);
    }

    setHeader(headers, CONTENT_LENGTH_HEADER);

    return headers;
  }
  private _getHttpRequestBody(): HttpRequestBody | undefined {
    if (this._stream !== undefined) {
      return { type: 'stream', content: this._stream };
    }

    if (this._body === undefined) {
      return undefined;
    }

    switch (this._bodyType) {
      case 'text':
        return { type: 'text', content: String(this._body) };

      case 'json':
        return { type: 'text', content: JSON.stringify(this._body) };

      case 'xml':
        return {
          type: 'text',
          content: this._xmlSerializer.xmlSerialize(
            this._body.data,
            this._body.rootName
          ),
        };

      case 'form':
      case 'form-data': {
        if (
          typeof this._body !== 'object' ||
          this._body === null ||
          Array.isArray(this._body)
        ) {
          return undefined;
        }

        const type = this._bodyType;
        const encoded = formDataEncodeObject(
          this._body,
          this._formPrefixFormat
        );
        const content = filterFileWrapperFromKeyValuePairs(encoded);

        return type === 'form' ? { type, content } : { type, content: encoded };
      }

      default:
        return undefined;
    }
  }
  private _addAuthentication() {
    this.intercept((...args) => {
      const handler = this._authenticationProvider(this._authParams);
      return handler(...args);
    });
  }
  private _addRetryInterceptor() {
    this.intercept(async (request, options, next) => {
      let context: HttpContext | undefined;
      let allowedWaitTime = this._retryConfig.maximumRetryWaitTime;
      let retryCount = 0;
      let waitTime = 0;
      let timeoutError: Error | undefined;
      const shouldRetry = shouldRetryRequest(
        this._retryOption,
        this._retryConfig,
        this._httpMethod
      );
      do {
        timeoutError = undefined;
        if (retryCount > 0) {
          await new Promise((res) => setTimeout(res, waitTime * 1000));
          allowedWaitTime -= waitTime;
        }
        try {
          context = await next(request, options);
        } catch (error) {
          timeoutError = error;
        }
        if (shouldRetry) {
          waitTime = getRetryWaitTime(
            this._retryConfig,
            allowedWaitTime,
            retryCount,
            context?.response?.statusCode,
            context?.response?.headers,
            timeoutError
          );

          retryCount++;
        }
      } while (waitTime > 0);
      if (timeoutError) {
        throw timeoutError;
      }
      if (typeof context?.response === 'undefined') {
        throw new Error('Response is undefined.');
      }
      return { request, response: context.response };
    });
  }
  private _addErrorHandlingInterceptor() {
    this.intercept(async (req, opt, next) => {
      const context = await next(req, opt);
      for (const { statusCode, errorConstructor, isTemplate, args } of this
        ._errorTypes) {
        if (
          (typeof statusCode === 'number' &&
            context.response.statusCode === statusCode) ||
          (typeof statusCode !== 'number' &&
            context.response.statusCode >= statusCode[0] &&
            context.response.statusCode <= statusCode[1])
        ) {
          if (isTemplate && args.length > 0) {
            args[0] = updateErrorMessage(args[0], context.response);
          }
          const error = new errorConstructor(context, ...args);
          if (errorConstructor.prototype instanceof ApiError) {
            // Load result only for the sub classes of ApiError
            await loadResult(error);
          }
          throw error;
        }
      }
      return context;
    });
  }
}

export function createRequestBuilderFactory<BaseUrlParamType, AuthParams>(
  httpClient: HttpClientInterface,
  baseUrlProvider: (arg?: BaseUrlParamType) => string,
  apiErrorConstructor: ApiErrorConstructor,
  authenticationProvider: AuthenticatorInterface<AuthParams>,
  retryConfig: RetryConfiguration,
  xmlSerializer: XmlSerializerInterface = new XmlSerialization(),
  apiLogger?: ApiLoggerInterface
): RequestBuilderFactory<BaseUrlParamType, AuthParams> {
  return (httpMethod, path?) => {
    return new DefaultRequestBuilder(
      httpClient,
      baseUrlProvider,
      apiErrorConstructor,
      authenticationProvider,
      httpMethod,
      xmlSerializer,
      retryConfig,
      path,
      apiLogger
    );
  };
}

function mergePath(left: string, right?: string): string {
  if (!right || right === '') {
    return left;
  }
  // remove all occurances of `/` (if any) from the end of left path
  left = left.replace('/', ' ').trimEnd().replace(' ', '/');
  // remove all occurances of `/` (if any) from the start of right sub-path
  right = right.replace('/', ' ').trimStart().replace(' ', '/');

  return `${left}/${right}`;
}

function parseJsonResult<T>(schema: Schema<T, any>, res: ApiResponse<void>): T {
  if (typeof res.body !== 'string') {
    throw new Error(
      'Could not parse body as JSON. The response body is not a string.'
    );
  }
  if (res.body.trim() === '') {
    const resEmptyErr = new Error(
      'Could not parse body as JSON. The response body is empty.'
    );
    return validateJson(schema, null, (_) => resEmptyErr);
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(res.body);
  } catch (error) {
    const resUnParseErr = new Error(
      `Could not parse body as JSON.\n\n${error.message}`
    );
    return validateJson(schema, res.body, (_) => resUnParseErr);
  }
  const resInvalidErr = (errors: SchemaValidationError[]) =>
    new ResponseValidationError(res, errors);
  return validateJson(schema, parsed, (errors) => resInvalidErr(errors));
}

function validateJson<T>(
  schema: Schema<T, any>,
  value: any,
  errorCreater: (errors: SchemaValidationError[]) => Error
): T {
  const mappingResult = validateAndMap(value, schema);
  if (mappingResult.errors) {
    throw errorCreater(mappingResult.errors);
  }
  return mappingResult.result;
}
