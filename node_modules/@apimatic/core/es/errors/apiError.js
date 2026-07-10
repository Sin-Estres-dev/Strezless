import { __extends, __awaiter, __generator } from 'tslib';
import JSONBig from '@apimatic/json-bigint';
import { convertFromStream } from '@apimatic/convert-to-stream';

/**
 * Thrown when the HTTP status code is not okay.
 *
 * The ApiError extends the ApiResponse interface, so all ApiResponse
 * properties are available.
 */
var ApiError =
/*#__PURE__*/
/** @class */
function (_super) {
  __extends(ApiError, _super);
  function ApiError(context, message) {
    var _newTarget = this.constructor;
    var _this = _super.call(this, message) || this;
    Object.setPrototypeOf(_this, _newTarget.prototype);
    var request = context.request,
      response = context.response;
    _this.request = request;
    _this.statusCode = response.statusCode;
    _this.headers = response.headers;
    _this.body = response.body;
    return _this;
  }
  return ApiError;
}(Error);
function loadResult(error) {
  return __awaiter(this, void 0, void 0, function () {
    var bodyString;
    return __generator(this, function (_a) {
      switch (_a.label) {
        case 0:
          return [4 /*yield*/, convertFromStream(error.body)];
        case 1:
          bodyString = _a.sent();
          try {
            error.result = JSONBig().parse(bodyString);
          } catch (_) {
            // ignore updating result if body is not a valid JSON.
          }
          return [2 /*return*/];
      }
    });
  });
}
export { ApiError, loadResult };