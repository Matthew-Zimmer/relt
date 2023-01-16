import { Location } from './ast/relt/location';

export class UserError extends Error { }

export function reportUserError(msg: string): never {
  throw new UserError(msg)
}

export class InternalError extends Error { }

export function reportInternalError(msg: string): never {
  throw new InternalError(msg)
}
