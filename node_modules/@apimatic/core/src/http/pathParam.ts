import { PathTemplateTypes } from '../index';

export class PathParam<T extends PathTemplateTypes> {
  constructor(public value: T, public key: string) {}
}
