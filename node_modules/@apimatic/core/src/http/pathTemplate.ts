import flatMap from 'lodash.flatmap';
import { PathParam } from './pathParam';

/** Marker for skipping URL-encoding when used with Path templating */
export class SkipEncode<T extends PathTemplatePrimitiveTypes> {
  public key?: string;
  constructor(public value: T, key?: string) {
    this.key = key;
  }
}

export type PathTemplatePrimitiveTypes =
  | string
  | string[]
  | number
  | number[]
  | bigint
  | Array<bigint>
  | boolean
  | boolean[]
  | unknown;

/** Path template argument type */
export type PathTemplateTypes =
  | PathTemplatePrimitiveTypes
  | SkipEncode<PathTemplatePrimitiveTypes>
  | PathParam<PathTemplatePrimitiveTypes>;

/**
 * URL path templating method.
 *
 * Template arguments of array type are imploded using the path separator and
 * individual elements are URL-encoded.
 *
 * Template arguments are URL-encoded unless wrapped in a SkipEncode instance.
 */
export function pathTemplate(
  strings: TemplateStringsArray,
  ...args: PathTemplateTypes[]
) {
  const values = flatMap(
    interweaveArrays(
      strings.map((s) => new SkipEncode(s)),
      args
    ),
    encodePathTemplateSegment
  );
  const pathSegment = values.join('');
  return pathSegment;
}

function encodePathTemplateSegment(value: PathTemplateTypes) {
  let skipEncode = false;
  const encode = (m: string | number | bigint | unknown) => {
    if (
      typeof m === 'string' ||
      typeof m === 'number' ||
      typeof m === 'bigint' ||
      typeof m === 'boolean'
    ) {
      return skipEncode || typeof m === 'bigint'
        ? m.toString()
        : encodeURIComponent(m);
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
  return Array.isArray(value)
    ? (value as unknown[]).map<unknown>(encode).join('/')
    : [encode(value)];
}

function interweaveArrays<A, B>(
  a: ReadonlyArray<A>,
  b: ReadonlyArray<B>
): Array<A | B> {
  const min = Math.min(a.length, b.length);
  return Array.apply(null, new Array(min))
    .reduce((result: Array<A | B>, _, index) => {
      result.push(a[index], b[index]);
      return result;
    }, [])
    .concat((a.length > min ? a : b).slice(min));
}
