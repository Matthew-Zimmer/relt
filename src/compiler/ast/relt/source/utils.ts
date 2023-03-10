import { inspect } from "util";
import { Expression } from ".";
import { reportInternalError, reportUserError } from "../../../errors";
import { sugarKindConditionMap } from "../../../phases/checkSugar";
import { genLoc, Location } from "../location";

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
    case "ApplicationExpression": return [e.left, ...e.args];
    case "JoinExpression": return e.on === undefined ? [e.left, e.right] : [e.left, e.right, e.on];
    case "GroupByExpression": return [e.value, e.by, e.agg];
    case "BlockExpression": return e.expressions;
    case "ObjectExpression": return e.properties;
    case "ArrayExpression": return e.values;
    case "IndexExpression": return [e.left, e.index];
  }
}

export function fromKids(e: Expression, kids: Expression[]): Expression {
  switch (e.kind) {
    case "IdentifierExpression": return { ...e, }; // [];
    case "PlaceholderExpression": return { ...e, }; // [];
    case "IntegerExpression": return { ...e, }; // [];
    case "FloatExpression": return { ...e, }; // [];
    case "StringExpression": return { ...e, }; // [];
    case "EnvExpression": return { ...e, }; // [];
    case "BooleanExpression": return { ...e, }; // [];
    case "NullExpression": return { ...e, }; // [];
    case "LetExpression": return { ...e, value: kids[0] }; // [e.value];
    case "TableExpression": return { ...e, value: kids[0] }; // [e.value];
    case "FunctionExpression": return { ...e, value: kids[0] }; // [e.value];
    case "EvalExpression": return { ...e, node: kids[0] }; // [e.node];
    case "DeclareExpression": return { ...e, value: kids[0] }; // [e.value];
    case "SpreadExpression": return { ...e, value: kids[0] }; // [e.value];
    case "AssignExpression": return { ...e, left: kids[0], right: kids[1] }; // [e.left, e.right];
    case "ConditionalExpression": return { ...e, left: kids[0], right: kids[1] }; // [e.left, e.right];
    case "OrExpression": return { ...e, left: kids[0], right: kids[1] }; // [e.left, e.right];
    case "AndExpression": return { ...e, left: kids[0], right: kids[1] }; // [e.left, e.right];
    case "CmpExpression": return { ...e, left: kids[0], right: kids[1] }; // [e.left, e.right];
    case "AddExpression": return { ...e, left: kids[0], right: kids[1] }; // [e.left, e.right];
    case "MulExpression": return { ...e, left: kids[0], right: kids[1] }; // [e.left, e.right];
    case "UnionExpression": return { ...e, left: kids[0], right: kids[1] }; // [e.left, e.right];
    case "WhereExpression": return { ...e, left: kids[0], right: kids[1] }; // [e.left, e.right];
    case "WithExpression": return { ...e, left: kids[0], right: kids[1] }; // [e.left, e.right];
    case "DropExpression": return { ...e, left: kids[0], right: kids[1] }; // [e.left, e.right];
    case "SelectExpression": return { ...e, left: kids[0], right: kids[1] }; // [e.left, e.right];
    case "DotExpression": return { ...e, left: kids[0], right: kids[1] }; // [e.left, e.right];
    case "ApplicationExpression": return { ...e, left: kids[0], args: kids.slice(1) }; // [e.left, e.right];
    case "JoinExpression": return { ...e, left: kids[0], right: kids[1], on: kids[2] }; // e.on === undefined ? [e.left, e.right] : [e.left, e.right, e.on];
    case "GroupByExpression": return { ...e, value: kids[0], by: kids[1], agg: kids[2] }; // [e.value, e.by, e.agg];
    case "BlockExpression": return { ...e, expressions: kids }; // e.expressions;
    case "ObjectExpression": return { ...e, properties: kids }; // e.properties;
    case "ArrayExpression": return { ...e, values: kids }; // e.values;
    case "IndexExpression": return { ...e, left: kids[0], index: kids[1] };
  }
}

export function shallowEquals(l: Expression, r: Expression): boolean {
  switch (l.kind) {
    case "IdentifierExpression": return r.kind === "IdentifierExpression" && l.name === r.name;
    case "PlaceholderExpression": return r.kind === "PlaceholderExpression" && l.name === r.name;
    case "IntegerExpression": return r.kind === "IntegerExpression" && l.value === r.value;
    case "FloatExpression": return r.kind === "FloatExpression" && l.value === r.value;
    case "StringExpression": return r.kind === "StringExpression" && l.value === r.value;
    case "EnvExpression": return r.kind === "EnvExpression" && l.value === r.value;
    case "BooleanExpression": return r.kind === "BooleanExpression" && l.value === r.value;
    case "NullExpression": return r.kind === "NullExpression";
    case "LetExpression": return r.kind === "LetExpression";
    case "TableExpression": return r.kind === "TableExpression";
    case "FunctionExpression": return r.kind === "FunctionExpression";
    case "EvalExpression": return r.kind === "EvalExpression";
    case "DeclareExpression": return r.kind === "DeclareExpression";
    case "SpreadExpression": return r.kind === "SpreadExpression";
    case "AssignExpression": return r.kind === "AssignExpression";
    case "ConditionalExpression": return r.kind === "ConditionalExpression";
    case "OrExpression": return r.kind === "OrExpression";
    case "AndExpression": return r.kind === "AndExpression";
    case "CmpExpression": return r.kind === "CmpExpression";
    case "AddExpression": return r.kind === "AddExpression";
    case "MulExpression": return r.kind === "MulExpression";
    case "UnionExpression": return r.kind === "UnionExpression";
    case "WhereExpression": return r.kind === "WhereExpression";
    case "WithExpression": return r.kind === "WithExpression";
    case "DropExpression": return r.kind === "DropExpression";
    case "SelectExpression": return r.kind === "SelectExpression";
    case "DotExpression": return r.kind === "DotExpression";
    case "ApplicationExpression": return r.kind === "ApplicationExpression";
    case "JoinExpression": return r.kind === "JoinExpression";
    case "GroupByExpression": return r.kind === "GroupByExpression";
    case "BlockExpression": return r.kind === "BlockExpression";
    case "ObjectExpression": return r.kind === "ObjectExpression";
    case "ArrayExpression": return r.kind === "ArrayExpression";
    case "IndexExpression": return r.kind === "IndexExpression";
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

export function rewrite(e: Expression, pattern: Expression, replacement: Expression): [Expression, boolean] {
  const res = kids(e).map(x => rewrite(x, pattern, replacement));
  const children = res.map(x => x[0]);
  const x = fromKids(e, children);
  const cap = match(pattern, x);
  const matched = cap !== undefined;
  return [matched ? substitute(replacement, cap) : x, matched || res.some(x => x[1])];
}

type Capture = Record<string, Expression>;

export function match(pattern: Expression, e: Expression): Capture | undefined {
  let t = 0;

  const imp = (p: Expression, e: Expression, c: Capture | undefined): Capture | undefined => {
    if (c === undefined) return undefined;
    if (p.kind === 'PlaceholderExpression') {
      if (e.kind === 'PlaceholderExpression' && p.name === e.name)
        return c;
      if (p.kindCondition !== undefined && (sugarKindConditionMap as any)[p.kindCondition] !== e.kind)
        return undefined;
    }
    else if (p.kind !== e.kind || !shallowEquals(p, e)) {
      return undefined;
    }

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
            return { ...x, [`_${t++}`]: { kind: "ArrayExpression", values: e.properties.filter((_, j) => j !== i), loc: genLoc } };
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

export function extract(e: Expression, name: string): Expression {
  if (!(name in e)) return e;

  const value = (e as any)[name];
  switch (typeof value) {
    case "bigint":
    case "function":
    case "symbol":
      reportInternalError(`Bad extract ${typeof value}`);
    case "boolean":
      return { kind: "BooleanExpression", value, loc: genLoc };
    case "number":
      return { kind: "IntegerExpression", value, loc: genLoc };
    case "object":
      return value as Expression;
    case "string":
      if (`${Number(value)}` === value)
        return { kind: "FloatExpression", value, loc: genLoc };
      return { kind: "StringExpression", value, loc: genLoc };
    case "undefined":
      return e;
  }
}

export function substitute(e: Expression, capture: Capture): Expression {
  switch (e.kind) {
    case "PlaceholderExpression": {
      if (e.spread === undefined) {
        if (e.extract === undefined)
          return capture[e.name];
        else {
          return extract(capture[e.name], e.extract);
        }
      }
      const { kindCondition: cond, spread: { method, overrides } } = e;
      const kind = sugarKindConditionMap[cond! as keyof typeof sugarKindConditionMap] as Expression['kind'];
      const exprs: Expression[] = [];
      const locs: Location[] = [];
      const cap = capture[e.name];
      const walk = (e: Expression & { left: Expression, right: Expression }) => {
        if (e.kind === kind) {
          locs.push(e.loc);
          exprs.push(e.left);
          if (e.right.kind === kind) {
            walk(e.right as any);
          }
          else {
            exprs.push(e.right);
          }
        }
      };
      walk(cap as any);
      if (exprs.length < 2)
        reportUserError(`Cannot substitute and spread over binary expression since at least two elements where not discovered`);

      const override = (e: Expression, i: number) => {
        const value = overrides.find(x => x.index === i);
        return value === undefined ? e : substitute(betaReduce(value.value, { 'x': e }), capture);
      }
      let expr: any;
      switch (method) {
        case "lr": {
          expr = { kind, loc: locs[0], left: override(exprs[0], 0), right: override(exprs[1], 1) };
          for (let i = 2; i < exprs.length; i++)
            expr = { kind, loc: locs[i - 1], left: expr, right: override(exprs[i], i) };
          return expr;
        }
        case "rl": {
          expr = { kind, loc: locs[locs.length - 1], left: override(exprs[exprs.length - 2], 1), right: override(exprs[exprs.length - 1], 0) };
          for (let i = 2; i < exprs.length; i++)
            expr = { kind, loc: locs[locs.length - i - 1], left: override(exprs[exprs.length - i - 1], exprs.length - i - 1), right: expr };
          return expr;
        }
      }
    }
    default:
      return applyKids(e, x => substitute(x, capture));
  }
}

export function betaReduce(e: Expression, capture: Capture): Expression {
  switch (e.kind) {
    case "IdentifierExpression":
      return e.name in capture ? capture[e.name] : e;
    default:
      return applyKids(e, x => betaReduce(x, capture));
  }
}

