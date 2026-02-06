"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.loadResult = exports.ApiError = void 0;
var tslib_1 = require("tslib");
var json_bigint_1 = tslib_1.__importDefault(require("@apimatic/json-bigint"));
var convert_to_stream_1 = require("@apimatic/convert-to-stream");
/**
 * Thrown when the HTTP status code is not okay.
 *
 * The ApiError extends the ApiResponse interface, so all ApiResponse
 * properties are available.
 */
var ApiError = /** @class */ (function (_super) {
    tslib_1.__extends(ApiError, _super);
    function ApiError(context, message) {
        var _newTarget = this.constructor;
        var _this = _super.call(this, message) || this;
        Object.setPrototypeOf(_this, _newTarget.prototype);
        var request = context.request, response = context.response;
        _this.request = request;
        _this.statusCode = response.statusCode;
        _this.headers = response.headers;
        _this.body = response.body;
        return _this;
    }
    return ApiError;
}(Error));
exports.ApiError = ApiError;
function loadResult(error) {
    return tslib_1.__awaiter(this, void 0, void 0, function () {
        var bodyString;
        return tslib_1.__generator(this, function (_a) {
            switch (_a.label) {
                case 0: return [4 /*yield*/, (0, convert_to_stream_1.convertFromStream)(error.body)];
                case 1:
                    bodyString = _a.sent();
                    try {
                        error.result = (0, json_bigint_1.default)().parse(bodyString);
                    }
                    catch (_) {
                        // ignore updating result if body is not a valid JSON.
                    }
                    return [2 /*return*/];
            }
        });
    });
}
exports.loadResult = loadResult;
