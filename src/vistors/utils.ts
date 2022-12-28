export interface Visitor<B, T> {
  process: (x: B) => T | null;
  shouldVisitChildren: (x: B) => boolean;
}

export const func = <T>(x: T) => () => x;
export const True = func(true);
