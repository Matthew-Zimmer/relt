import { Expression } from ".";
import { relt } from "../../builder";

export type Visitor<T extends { kind: string }, R> = { [K in T['kind']]?: (e: T & { kind: K }) => R }

export function kids(e: Expression): Expression[] {
  switch (e.kind) {
    case "IdentifierExpression": return [];
    case "PlaceholderExpression": return [];
    case "IntegerExpression": return [];
    case "FloatExpression": return [];
    case "StringExpression": return [];
    case "EnvExpression": return [];
    case "BooleanExpression": return [];
    case "NullExpression": return [];
    case "LetExpression": return [e.value];
    case "TableExpression": return [e.value];
    case "FunctionExpression": return [e.value];
    case "EvalExpression": return [e.node];
    case "DeclareExpression": return [e.value];
    case "SpreadExpression": return [e.value];
    case "AssignExpression": return [e.left, e.right];
    case "ConditionalExpression": return [e.left, e.right];
    case "OrExpression": return [e.left, e.right];
    case "AndExpression": return [e.left, e.right];
    case "CmpExpression": return [e.left, e.right];
    case "AddExpression": return [e.left, e.right];
    case "MulExpression": return [e.left, e.right];
    case "UnionExpression": return [e.left, e.right];
    case "WhereExpression": return [e.left, e.right];
    case "WithExpression": return [e.left, e.right];
    case "DropExpression": return [e.left, e.right];
    case "SelectExpression": return [e.left, e.right];
    case "DotExpression": return [e.left, e.right];
    case "ApplicationExpression": return [e.left, e.right];
    case "JoinExpression": return e.on === undefined ? [e.left, e.right] : [e.left, e.right, e.on];
    case "GroupByExpression": return [e.value, e.by, e.agg];
    case "BlockExpression": return e.expressions;
    case "ObjectExpression": return e.properties;
    case "ArrayExpression": return e.values;
  }
}

export function fromKids(e: Expression, kids: Expression[]): Expression {
  switch (e.kind) {
    case "LetExpression": return { ...e };
    case "TableExpression": return { ...e };
    case "FunctionExpression": return { ...e };
    case "EvalExpression": return { ...e };
    case "DeclareExpression": return { ...e };
    case "AssignExpression": return { ...e };
    case "ConditionalExpression": return { ...e };
    case "OrExpression": return { ...e };
    case "AndExpression": return { ...e };
    case "CmpExpression": return { ...e };
    case "AddExpression": return { ...e };
    case "MulExpression": return { ...e };
    case "UnionExpression": return { ...e };
    case "JoinExpression": return { ...e };
    case "GroupByExpression": return { ...e };
    case "WhereExpression": return { ...e };
    case "WithExpression": return { ...e };
    case "DropExpression": return { ...e };
    case "SelectExpression": return { ...e };
    case "DotExpression": return { ...e };
    case "ApplicationExpression": return { ...e };
    case "IdentifierExpression": return { ...e };
    case "PlaceholderExpression": return { ...e };
    case "IntegerExpression": return { ...e };
    case "FloatExpression": return { ...e };
    case "StringExpression": return { ...e };
    case "EnvExpression": return { ...e };
    case "BooleanExpression": return { ...e };
    case "NullExpression": return { ...e };
    case "BlockExpression": return { ...e };
    case "ObjectExpression": return { ...e };
    case "ArrayExpression": return { ...e };
    case "SpreadExpression": return { ...e };
  }
}

export function shallowEquals(l: Expression, r: Expression): boolean {
  switch (l.kind) {
    case "LetExpression": return r.kind === "LetExpression";
    case "TableExpression": return r.kind === "TableExpression";
    case "FunctionExpression": return r.kind === "FunctionExpression";
    case "EvalExpression": return r.kind === "EvalExpression";
    case "DeclareExpression": return r.kind === "DeclareExpression";
    case "AssignExpression": return r.kind === "AssignExpression";
    case "ConditionalExpression": return r.kind === "ConditionalExpression";
    case "OrExpression": return r.kind === "OrExpression";
    case "AndExpression": return r.kind === "AndExpression";
    case "CmpExpression": return r.kind === "CmpExpression";
    case "AddExpression": return r.kind === "AddExpression";
    case "MulExpression": return r.kind === "MulExpression";
    case "UnionExpression": return r.kind === "UnionExpression";
    case "JoinExpression": return r.kind === "JoinExpression";
    case "GroupByExpression": return r.kind === "GroupByExpression";
    case "WhereExpression": return r.kind === "WhereExpression";
    case "WithExpression": return r.kind === "WithExpression";
    case "DropExpression": return r.kind === "DropExpression";
    case "SelectExpression": return r.kind === "SelectExpression";
    case "DotExpression": return r.kind === "DotExpression";
    case "ApplicationExpression": return r.kind === "ApplicationExpression";
    case "IdentifierExpression": return r.kind === "IdentifierExpression";
    case "PlaceholderExpression": return r.kind === "PlaceholderExpression";
    case "IntegerExpression": return r.kind === "IntegerExpression";
    case "FloatExpression": return r.kind === "FloatExpression";
    case "StringExpression": return r.kind === "StringExpression";
    case "EnvExpression": return r.kind === "EnvExpression";
    case "BooleanExpression": return r.kind === "BooleanExpression";
    case "NullExpression": return r.kind === "NullExpression";
    case "BlockExpression": return r.kind === "BlockExpression";
    case "ObjectExpression": return r.kind === "ObjectExpression";
    case "ArrayExpression": return r.kind === "ArrayExpression";
    case "SpreadExpression": return r.kind === "SpreadExpression";
  }
}

export function ofKind<K extends Expression['kind']>(kind: K) {
  return (e: Expression): e is Expression & { kind: K } => e.kind === kind;
}

export function deepEquals(l: Expression, r: Expression): boolean {
  const lKids = kids(l);
  const rKids = kids(r);
  return shallowEquals(l, r) && lKids.every((_, i) => deepEquals(lKids[i], rKids[i]));
}

export function visit<R>(e: Expression, visitor: Visitor<Expression, R>, otherwise: (e: Expression) => R, combine: (x: Expression, s: R[]) => Expression): R {
  return (e.kind in visitor ? visitor[e.kind] as any : otherwise)(combine(e, kids(e).map(x => visit(x, visitor, otherwise, combine))));
}

export function visitMap(e: Expression, visitor: Visitor<Expression, Expression>): Expression {
  return visit(e, visitor, x => x, fromKids);
}

export function visitVoid(e: Expression, visitor: Visitor<Expression, void>): void {
  return visit(e, visitor, () => { }, x => x);
}

export function gather<T extends Expression>(e: Expression, f: (e: Expression) => e is T): T[] {
  return visit(e, {}, (x) => f(x) ? [x] : [], x => x).flat() as T[];
}

type Scope = Record<string, Expression>;

export function normalize(e: Expression, scope: Scope): [Expression, Scope] {
  switch (e.kind) {
    case "LetExpression": return [{ ...e }, scope];
    case "TableExpression": return [{ ...e }, scope];
    case "FunctionExpression": return [{ ...e }, scope];
    case "EvalExpression": return [{ ...e }, scope];
    case "DeclareExpression": return [{ ...e }, scope];
    case "AssignExpression": return [{ ...e }, scope];
    case "ConditionalExpression": return [{ ...e }, scope];
    case "OrExpression": return [{ ...e }, scope];
    case "AndExpression": return [{ ...e }, scope];
    case "CmpExpression": return [{ ...e }, scope];
    case "AddExpression": return [{ ...e }, scope];
    case "MulExpression": return [{ ...e }, scope];
    case "UnionExpression": return [{ ...e }, scope];
    case "JoinExpression": return [{ ...e }, scope];
    case "GroupByExpression": return [{ ...e }, scope];
    case "WhereExpression": return [{ ...e }, scope];
    case "WithExpression": return [{ ...e }, scope];
    case "DropExpression": return [{ ...e }, scope];
    case "SelectExpression": return [{ ...e }, scope];
    case "DotExpression": return [{ ...e }, scope];
    case "ApplicationExpression": return [{ ...e }, scope];
    case "IdentifierExpression": return [{ ...e }, scope];
    case "PlaceholderExpression": return [{ ...e }, scope];
    case "IntegerExpression": return [{ ...e }, scope];
    case "FloatExpression": return [{ ...e }, scope];
    case "StringExpression": return [{ ...e }, scope];
    case "EnvExpression": return [{ ...e }, scope];
    case "BooleanExpression": return [{ ...e }, scope];
    case "NullExpression": return [{ ...e }, scope];
    case "BlockExpression": return [{ ...e }, scope];
    case "ObjectExpression": return [{ ...e }, scope];
    case "ArrayExpression": return [{ ...e }, scope];
    case "SpreadExpression": return [{ ...e }, scope];
  }
}

export function rewrite(e: Expression, pattern: Expression, replacement: Expression): Expression {
  return visit(e, {}, x => {
    const cap = match(pattern, x);
    return cap === undefined ? x : substitute(replacement, cap);
  }, fromKids);
}

type Capture = Record<string, Expression>;

export function match(pattern: Expression, e: Expression): Capture | undefined {
  let t = 0;

  const imp = (p: Expression, e: Expression, c: Capture | undefined): Capture | undefined => {
    if (c === undefined) return undefined;
    if (p.kind !== e.kind || !shallowEquals(p, e))
      return undefined;

    switch (p.kind) {
      case "PlaceholderExpression": {
        if (p.name in c) {
          const x = deepEquals(c[p.name], e) ? c : undefined;
          return x;
        }
        else {
          const x = { ...c, [p.name]: e };
          return x;
        }
      }
      case "ObjectExpression": {
        if (e.kind !== 'ObjectExpression') return undefined;
        if (p.properties.length === 0) return e.properties.length === 0 ? c : undefined;
        if (p.properties.length !== 1) throw new Error(`Invalid rewrite rule cannot have a pattern on an object expression with more than 1 sub pattern`);
        if (e.properties.length < p.properties.length) return undefined;
        for (const [i, prop] of e.properties.entries()) {
          const x = imp(prop, p.properties[0], c);
          if (x !== undefined)
            return { ...x, [`_${t++}`]: relt.source.array(e.properties.filter((_, j) => j !== i)) };
        }
        return undefined;
      }
      case "ArrayExpression": {
        if (e.kind !== 'ArrayExpression') return undefined;
        return e.values.length !== p.values.length ? undefined : p.values.reduce<Capture | undefined>((c, _, i) => imp(e.values[i], p.values[i], c), c);
      }
      default: {
        const pKids = kids(p);
        const eKids = kids(e);
        return pKids.reduce<Capture | undefined>((p, _, i) => imp(pKids[i], eKids[i], p), c);
      }
    }
  }

  return imp(pattern, e, {});
}

export function applyKids(e: Expression, f: (e: Expression) => Expression): Expression {
  return fromKids(e, kids(e).map(f));
}

export function substitute(e: Expression, capture: Capture): Expression {
  return applyKids(e, x => substitute(x, capture));
}
