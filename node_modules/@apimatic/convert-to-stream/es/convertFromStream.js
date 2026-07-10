import { __awaiter, __generator, __asyncValues } from './_virtual/_tslib.js';
import { convertFromStream as convertFromStream$1 } from './convertFromBlob.js';
function convertFromStream(content) {
  var content_1, content_1_1;
  var e_1, _a;
  return __awaiter(this, void 0, void 0, function () {
    var chunks, chunk, e_1_1;
    return __generator(this, function (_b) {
      switch (_b.label) {
        case 0:
          if (typeof content === 'string') {
            return [2 /*return*/, content];
          }
          if (content instanceof Blob) {
            return [2 /*return*/, convertFromStream$1(content)];
          }
          chunks = [];
          _b.label = 1;
        case 1:
          _b.trys.push([1, 6, 7, 12]);
          content_1 = __asyncValues(content);
          _b.label = 2;
        case 2:
          return [4 /*yield*/, content_1.next()];
        case 3:
          if (!(content_1_1 = _b.sent(), !content_1_1.done)) return [3 /*break*/, 5];
          chunk = content_1_1.value;
          chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
          _b.label = 4;
        case 4:
          return [3 /*break*/, 2];
        case 5:
          return [3 /*break*/, 12];
        case 6:
          e_1_1 = _b.sent();
          e_1 = {
            error: e_1_1
          };
          return [3 /*break*/, 12];
        case 7:
          _b.trys.push([7,, 10, 11]);
          if (!(content_1_1 && !content_1_1.done && (_a = content_1.return))) return [3 /*break*/, 9];
          return [4 /*yield*/, _a.call(content_1)];
        case 8:
          _b.sent();
          _b.label = 9;
        case 9:
          return [3 /*break*/, 11];
        case 10:
          if (e_1) throw e_1.error;
          return [7 /*endfinally*/];
        case 11:
          return [7 /*endfinally*/];
        case 12:
          return [2 /*return*/, Buffer.concat(chunks).toString()];
      }
    });
  });
}
export { convertFromStream };