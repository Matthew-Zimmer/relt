import { RESERVED_WORDS } from "peggy";
import { TypedExpression, ValidObjectPropertyExpression } from ".";
import { relt } from "../../builder";

export type Visitor<T extends { kind: string }, R> = { [K in T['kind']]?: (e: T & { kind: K }) => R }

export function kids(e: TypedExpression): TypedExpression[] {
  switch (e.kind) {
    case "TypedLetExpression": return [e.value];
    case "TypedIntegerExpression": return [];
    case "TypedNullExpression": return [];
    case "TypedIdentifierExpression": return [];
    case "TypedApplicationExpression": return [e.left, e.right];
    case "TypedStringExpression": return [];
    case "TypedBooleanExpression": return [];
    case "TypedFloatExpression": return [];
    case "TypedObjectExpression": return e.properties;
    case "TypedArrayExpression": return e.values;
    case "TypedPlaceholderExpression": return [];
    case "TypedEvalExpression": return [e.node];
    case "TypedTableExpression": return e.columns;
    case "TypedDeclareExpression": return [e.left];
    case "TypedAssignExpression": return [e.left, e.right];
  }
}

export function fromKids(e: TypedExpression, kids: TypedExpression[]): TypedExpression {
  switch (e.kind) {
    case "TypedLetExpression": return { ...e, value: kids[0] };
    case "TypedIntegerExpression": return { ...e };
    case "TypedNullExpression": return { ...e };
    case "TypedIdentifierExpression": return { ...e };
    case "TypedApplicationExpression": return { ...e, left: kids[0], right: kids[1] };
    case "TypedStringExpression": return { ...e };
    case "TypedBooleanExpression": return { ...e };
    case "TypedFloatExpression": return { ...e };
    case "TypedObjectExpression": return { ...e, properties: kids as any };
    case "TypedArrayExpression": return { ...e, values: kids };
    case "TypedPlaceholderExpression": return { ...e };
    case "TypedEvalExpression": return { ...e, node: kids[0] };
    case "TypedTableExpression": return { ...e, columns: kids as any };
    case "TypedDeclareExpression": return { ...e, left: kids[0] };
    case "TypedAssignExpression": return { ...e, left: kids[0], right: kids[1] };
  }
}

export function shallowEquals(l: TypedExpression, r: TypedExpression): boolean {
  switch (l.kind) {
    case "TypedLetExpression": return r.kind === "TypedLetExpression" && l.name === r.name;
    case "TypedIntegerExpression": return r.kind === "TypedIntegerExpression" && l.value === r.value;
    case "TypedNullExpression": return r.kind === "TypedNullExpression";
    case "TypedIdentifierExpression": return r.kind === "TypedIdentifierExpression" && l.name == r.name;
    case "TypedApplicationExpression": return r.kind === "TypedApplicationExpression";
    case "TypedStringExpression": return r.kind === "TypedStringExpression" && l.value == r.value;
    case "TypedBooleanExpression": return r.kind === "TypedBooleanExpression" && l.value == r.value;
    case "TypedFloatExpression": return r.kind === "TypedFloatExpression" && l.value == r.value;
    case "TypedObjectExpression": return r.kind === "TypedObjectExpression";
    case "TypedArrayExpression": return r.kind === "TypedArrayExpression";
    case "TypedPlaceholderExpression": return r.kind === "TypedPlaceholderExpression" && l.name == r.name;
    case "TypedEvalExpression": return r.kind === "TypedEvalExpression";
    case "TypedTableExpression": return r.kind === "TypedTableExpression" && l.name == r.name;
    case "TypedDeclareExpression": return r.kind === "TypedDeclareExpression"; // ???
    case "TypedAssignExpression": return r.kind === "TypedAssignExpression";
  }
}

export function deepEquals(l: TypedExpression, r: TypedExpression): boolean {
  const lKids = kids(l);
  const rKids = kids(r);
  return shallowEquals(l, r) && lKids.every((_, i) => deepEquals(lKids[i], rKids[i]));
}

export function visit<R, A>(e: TypedExpression, visitor: Visitor<TypedExpression, R>, otherwise: (e: TypedExpression) => R, combine: (x: TypedExpression, s: R[]) => TypedExpression): R {
  return (e.kind in visitor ? visitor[e.kind] as any : otherwise)(combine(e, kids(e).map(x => visit(x, visitor, otherwise, combine))));
}

export function visitMap(e: TypedExpression, visitor: Visitor<TypedExpression, TypedExpression>): TypedExpression {
  return visit(e, visitor, x => x, fromKids);
}

export function visitVoid(e: TypedExpression, visitor: Visitor<TypedExpression, void>): void {
  return visit(e, visitor, () => { }, x => x);
}

type Scope = Record<string, TypedExpression>;

export function normalize(e: TypedExpression, scope: Scope): [TypedExpression, Scope] {
  switch (e.kind) {
    case "TypedIntegerExpression": return [e, scope];
    case "TypedNullExpression": return [e, scope];
    case "TypedStringExpression": return [e, scope];
    case "TypedBooleanExpression": return [e, scope];
    case "TypedFloatExpression": return [e, scope];
    case "TypedPlaceholderExpression": return [e, scope];
    case "TypedTableExpression": return [e, scope];
    case "TypedEvalExpression": return normalize(e.node, scope);
    case "TypedIdentifierExpression": return [e.name in scope ? scope[e.name] : e, scope];
    case "TypedLetExpression": {
      const [value, scope1] = normalize(e.value, scope);
      return [value, { ...scope1, [e.name]: value }];
    }
    case "TypedApplicationExpression": {
      const [left] = normalize(e.left, scope);
      const [right] = normalize(e.right, scope);
      return [{ kind: "TypedApplicationExpression", left, right, type: e.type }, scope];
    }
    case "TypedDeclareExpression": {
      const [left] = normalize(e.left, scope);
      return [{ kind: "TypedDeclareExpression", left, type: e.type }, scope];
    }
    case "TypedAssignExpression": {
      if (e.left.kind === 'TypedIdentifierExpression') {
        const [right] = normalize(e.right, scope);
        return [right, { ...scope, [e.left.name]: right }];
      }
      else {
        const [left] = normalize(e.left, scope);
        const [right] = normalize(e.right, scope);
        return [{ kind: "TypedAssignExpression", left, right, type: e.type }, scope];
      }
    }
    case "TypedArrayExpression": {
      const values = e.values.map(x => normalize(x, scope)[0]);
      return [{ kind: "TypedArrayExpression", values, type: e.type }, scope];
    }
    case "TypedObjectExpression": {
      const [properties] = e.properties.reduce<[TypedExpression[], Scope]>(([l, s], c) => {
        let a: TypedExpression;
        let b: Scope;

        switch (c.kind) {
          case "TypedAssignExpression": {
            const [d] = normalize(c.left, s);
            const [e] = normalize(c.right, s);
            a = { ...c, left: d, right: e };
            b = s;
            break;
          }
          default: {
            [a, b] = normalize(c, s);
            break;
          }
        }

        return [[...l, a], b];
      }, [[], scope]);
      return [{ kind: "TypedObjectExpression", properties: properties as ValidObjectPropertyExpression[], type: e.type }, scope];
    }
  }
}

export function rewrite(e: TypedExpression, pattern: TypedExpression, replacement: TypedExpression): [TypedExpression, boolean] {
  const res = kids(e).map(x => rewrite(x, pattern, replacement));
  const children = res.map(x => x[0]);
  const x = fromKids(e, children);
  const cap = match(pattern, x);
  const matched = cap !== undefined;
  return [matched ? substitute(replacement, cap) : x, matched || res.some(x => x[1])];
}

type Capture = Record<string, TypedExpression>;

export function match(pattern: TypedExpression, e: TypedExpression): Capture | undefined {
  let t = 0;

  const imp = (p: TypedExpression, e: TypedExpression, c: Capture | undefined): Capture | undefined => {
    if (c === undefined) return undefined;
    if (p.kind !== e.kind || !shallowEquals(p, e))
      return undefined;

    switch (p.kind) {
      case "TypedPlaceholderExpression": {
        if (p.name in c) {
          const x = deepEquals(c[p.name], e) ? c : undefined;
          return x;
        }
        else {
          const x = { ...c, [p.name]: e };
          return x;
        }
      }
      case "TypedObjectExpression": {
        if (e.kind !== 'TypedObjectExpression') return undefined;
        if (p.properties.length === 0) return e.properties.length === 0 ? c : undefined;
        if (p.properties.length !== 1) throw new Error(`Invalid rewrite rule cannot have a pattern on an object TypedExpression with more than 1 sub pattern`);
        if (e.properties.length < p.properties.length) return undefined;
        for (const [i, prop] of e.properties.entries()) {
          const x = imp(prop, p.properties[0], c);
          if (x !== undefined)
            return { ...x, [`_${t++}`]: { kind: "TypedArrayExpression", type: relt.type.array(relt.type.any), values: e.properties.filter((_, j) => j !== i) } };
        }
        return undefined;
      }
      case "TypedArrayExpression": {
        if (e.kind !== 'TypedArrayExpression') return undefined;
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

export function applyKids(e: TypedExpression, f: (e: TypedExpression) => TypedExpression): TypedExpression {
  return fromKids(e, kids(e).map(f));
}

export function substitute(e: TypedExpression, capture: Capture): TypedExpression {
  return applyKids(e, x => substitute(x, capture));
}
