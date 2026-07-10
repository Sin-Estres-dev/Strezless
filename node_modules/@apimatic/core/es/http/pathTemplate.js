import flatMap from 'lodash.flatmap';
import { PathParam } from './pathParam.js';

/** Marker for skipping URL-encoding when used with Path templating */
var SkipEncode =
/*#__PURE__*/
/** @class */
function () {
  function SkipEncode(value, key) {
    this.value = value;
    this.key = key;
  }
  return SkipEncode;
}();
/**
 * URL path templating method.
 *
 * Template arguments of array type are imploded using the path separator and
 * individual elements are URL-encoded.
 *
 * Template arguments are URL-encoded unless wrapped in a SkipEncode instance.
 */
function pathTemplate(strings) {
  var args = [];
  for (var _i = 1; _i < arguments.length; _i++) {
    args[_i - 1] = arguments[_i];
  }
  var values = flatMap(interweaveArrays(strings.map(function (s) {
    return new SkipEncode(s);
  }), args), encodePathTemplateSegment);
  var pathSegment = values.join('');
  return pathSegment;
}
function encodePathTemplateSegment(value) {
  var skipEncode = false;
  var encode = function (m) {
    if (typeof m === 'string' || typeof m === 'number' || typeof m === 'bigint' || typeof m === 'boolean') {
      return skipEncode || typeof m === 'bigint' ? m.toString() : encodeURIComponent(m);
    }
    return '';
  };
  if (value instanceof SkipEncode) {
    value = value.value;
    skipEncode = true;
  }
  if (value instanceof PathParam) {
    value = value.value;
  }
  return Array.isArray(value) ? value.map(encode).join('/') : [encode(value)];
}
function interweaveArrays(a, b) {
  var min = Math.min(a.length, b.length);
  return Array.apply(null, new Array(min)).reduce(function (result, _, index) {
    result.push(a[index], b[index]);
    return result;
  }, []).concat((a.length > min ? a : b).slice(min));
}
export { SkipEncode, pathTemplate };