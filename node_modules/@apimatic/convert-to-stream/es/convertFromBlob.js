import { __awaiter, __generator } from './_virtual/_tslib.js';
function convertFromStream(content) {
  return __awaiter(this, void 0, void 0, function () {
    var arrayBuffer, _a, _b;
    return __generator(this, function (_c) {
      switch (_c.label) {
        case 0:
          if (typeof content === 'string') {
            return [2 /*return*/, content];
          }
          if (!(content instanceof Blob)) {
            throw new Error('Type must be Blob');
          }
          arrayBuffer = new Promise(function (resolve, reject) {
            var reader = new FileReader();
            reader.onload = function () {
              return resolve(reader.result);
            };
            reader.onerror = reject;
            reader.readAsArrayBuffer(content);
          });
          _b = (_a = Buffer).from;
          return [4 /*yield*/, arrayBuffer];
        case 1:
          return [2 /*return*/, _b.apply(_a, [_c.sent()]).toString()];
      }
    });
  });
}
export { convertFromStream };