import { urlToHttpOptions } from "url";


type Capture = Record<string, Expression>;

type ExpressionCtor<T extends Expression, E extends unknown[], K extends Expression[]> = new (...args: [...E, ...K]) => T;

abstract class Expression<T extends Expression = any, E extends any[] = any, K extends Expression[] = any> {
  abstract get kids(): K;
  abstract get extras(): E;

  constructor(private ctor: ExpressionCtor<T, E, K>) {
  }

  fromKids(kids: K): Expression {
    return new this.ctor(...this.extras, ...kids);
  }

  isSameKindAs(other: Expression): boolean {
    return other instanceof this.ctor;
  }

  extrasEqual(other: Expression) {
    return this.isSameKindAs(other) && this.extras.every((_, i) => this.extras[i] === other.extras[i]);
  }

  abstract get pretty(): string;

  protected matchesHelper(other: Expression, capture: Capture | undefined): Capture | undefined {
    if (capture === undefined) return undefined;
    if (!this.isSameKindAs(other) && !this.extrasEqual(other))
      return undefined;
    return this.kids.reduce<Capture | undefined>((p, _, i) => this.kids[i].matchesHelper(other.kids[i], p), capture);
  }

  matches(other: Expression): Capture | undefined {
    return this.matchesHelper(other, {});
  }

  equals(other: Expression): boolean {
    return this.isSameKindAs(other) && this.kids.every((_, i) => this.kids[i].equals(other.kids[i]));
  }

  map<T>(f: (e: Expression) => T): T[] {
    return this.kids.map(f);
  }

  applyKids(f: (e: Expression) => Expression): Expression {
    return this.fromKids(this.map(f) as K);
  }

  apply(f: (e: Expression) => Expression): Expression {
    return f(this.applyKids(f));
  }

  rewrite(pattern: Expression, replacement: Expression): Expression {
    return this.apply(x => {
      const cap = pattern.matches(x);
      return cap === undefined ? x : replacement.substitute(cap);
    });
  }

  substitute(capture: Capture): Expression {
    return this.applyKids(x => x.substitute(capture));
  }

  copy(): Expression {
    return this.fromKids(this.kids);
  }

  normalize(): Expression {
    return this.copy();
  }
}

abstract class PlainExpression<T extends Expression, K extends Expression[]> extends Expression<T, [], K> {
  get extras(): [] {
    return [];
  }
}

abstract class LeafExpression<T extends Expression, E extends unknown[]> extends Expression<T, E, []> {
  get kids(): [] {
    return [];
  }
}

abstract class PlainLeafExpression<T extends Expression> extends LeafExpression<T, []> {
  get extras(): [] {
    return [];
  }
}

abstract class UnaryExpression<T extends Expression, E extends unknown[]> extends Expression<T, E, [Expression]> {
  constructor(ctor: ExpressionCtor<T, E, [Expression]>, public kid: Expression) {
    super(ctor);
  }

  get kids(): [Expression] {
    return [this.kid];
  }
}

abstract class PlainUnaryExpression<T extends Expression> extends UnaryExpression<T, []> {
  get extras(): [] {
    return [];
  }
}

abstract class BinaryExpression<T extends Expression, E extends unknown[]> extends Expression<T, E, [Expression, Expression]> {
  constructor(ctor: ExpressionCtor<T, E, [Expression, Expression]>, public left: Expression, public right: Expression) {
    super(ctor);
  }

  get kids(): [Expression, Expression] {
    return [this.left, this.right];
  }
}

abstract class PlainBinaryExpression<T extends Expression> extends BinaryExpression<T, []> {
  get extras(): [] {
    return [];
  }
}

export class IdentifierExpression extends LeafExpression<IdentifierExpression, [string]> {
  constructor(public name: string) { super(IdentifierExpression); }

  get pretty(): string {
    return this.name;
  }

  get extras(): [string] {
    return [this.name];
  }
}

export class IntegerExpression extends LeafExpression<IntegerExpression, [number]> {
  constructor(public value: number) { super(IntegerExpression); }

  get extras(): [number] {
    return [this.value];
  }

  get pretty(): string {
    return `${this.value}`;
  }
}

export class AbstractExpression extends UnaryExpression<AbstractExpression, [string]> {
  constructor(public name: string, value: Expression) { super(AbstractExpression, value); }

  get extras(): [string] {
    return [this.name];
  }

  get pretty(): string {
    return `\\${this.name} => ${this.kid.pretty}`;
  }
}

export class ApplicationExpression extends PlainBinaryExpression<ApplicationExpression> {
  constructor(left: Expression, right: Expression) { super(ApplicationExpression, left, right); }

  get pretty(): string {
    return ``;
  }
}

export class WildcardExpression extends LeafExpression<WildcardExpression, [string]> {
  constructor(public name: string) { super(WildcardExpression); }

  get extras(): [string] {
    return [this.name];
  }

  get pretty(): string {
    return ``;
  }
}

export class StringExpression extends LeafExpression<StringExpression, [string]> {
  constructor(public name: string) { super(StringExpression); }

  get extras(): [string] {
    return [this.name];
  }

  get pretty(): string {
    return ``;
  }
}

export class WithExpression extends PlainBinaryExpression<WithExpression> {
  constructor(left: Expression, right: Expression) { super(WithExpression, left, right); }

  get pretty(): string {
    return ``;
  }
}

type JoinMethod =
  | 'inner'
  | 'left'

export class JoinExpression extends BinaryExpression<JoinExpression, [JoinMethod]> {
  constructor(public method: JoinMethod, left: Expression, right: Expression) { super(JoinExpression, left, right); }

  get extras(): [JoinMethod] {
    return [this.method];
  }

  get pretty(): string {
    return ``;
  }
}

export class GroupExpression extends PlainExpression<GroupExpression, [Expression, Expression, Expression]> {
  constructor(public left: Expression, public by: Expression, public right: Expression) { super(GroupExpression); }

  get kids(): [Expression, Expression, Expression] {
    return [this.left, this.by, this.right];
  }

  get pretty(): string {
    return ``;
  }
}

export class ArrayExpression extends PlainExpression<ArrayExpression, Expression[]> {
  public values: Expression[];

  constructor(...values: Expression[]) {
    super(ArrayExpression);
    this.values = values;
  }

  get kids() {
    return this.values;
  }

  get pretty(): string {
    return ``;
  }
}

export class DeclareExpression extends PlainBinaryExpression<DeclareExpression> {
  constructor(public left: Expression, public right: Expression) { super(DeclareExpression, left, right); }

  get pretty(): string {
    return ``;
  }
}

export class AssignExpression extends PlainBinaryExpression<AssignExpression> {
  constructor(public left: Expression, public right: Expression) { super(AssignExpression, left, right); }

  get pretty(): string {
    return ``;
  }
}

export class ObjectExpression extends PlainExpression<ObjectExpression, Expression[]> {
  public properties: Expression[];

  constructor(...properties: Expression[]) {
    super(ObjectExpression);
    this.properties = properties;
  }

  get kids() {
    return this.properties;
  }

  get pretty(): string {
    return ``;
  }
}

export class EvalExpression extends PlainUnaryExpression<EvalExpression> {
  constructor(expr: Expression) { super(EvalExpression, expr); }

  get pretty(): string {
    return ``;
  }
}



// export interface IdentifierExpression {
// kind: "IdentifierExpression";
// name: string;
// }

// export interface IntegerExpression {
// kind: "IntegerExpression";
// value: number;
// }

// export interface StringExpression {
// kind: "StringExpression";
// value: string;
// }

// export interface AbstractExpression {
// kind: "AbstractExpression";
// name: string;
// value: Expression;
// }

// export interface ApplicationExpression {
// kind: "ApplicationExpression";
// left: Expression;
// right: Expression;
// }

// export interface WildcardExpression {
// kind: "WildcardExpression";
// name: string;
// constraint?: 'IdentifierExpression' | 'IntegerExpression';
// arity: 'many' | 'single';
// }

// export interface WithExpression {
// kind: "WithExpression";
// left: Expression;
// right: Expression;
// }
// export interface JoinExpression {
// kind: "JoinExpression";
// left: Expression;
// right: Expression;
// method: "inner" | "left";
// }
// export interface GroupExpression {
// kind: "GroupExpression";
// left: Expression;
// by: Expression;
// right: Expression;
// }
// export interface ArrayExpression {
// kind: "ArrayExpression";
// values: Expression[];
// }
// export interface DeclareExpression {
// kind: "DeclareExpression";
// left: Expression;
// right: Expression;
// }
// export interface AssignExpression {
// kind: "AssignExpression";
// left: Expression;
// right: Expression;
// }
// export interface ObjectExpression {
// kind: "ObjectExpression";
// properties: Expression[];
// }
// export interface EvalExpression {
// kind: "EvalExpression";
// expr: Expression;
// }

// export const rw = {
// expr: {
//   id(name: string): IdentifierExpression { return { kind: "IdentifierExpression", name } },
//   int(value: number):                                                          IntegerExpression { return { kind: "IntegerExpression", value } },
//   abstract(name: string, value: Expression):                                   AbstractExpression { return { kind: "AbstractExpression", name, value } },
//   app(left: Expression, right: Expression):                                    ApplicationExpression { return { kind: "ApplicationExpression", left, right } },
//   wildcard(name: string, constraint?:                                          WildcardExpression['constraint'], arity?: WildcardExpression['arity']): WildcardExpression { return { kind: "WildcardExpression", name, constraint, arity: arity ?? 'single' } },
//   string(value: string):                                                       StringExpression { return { kind: "StringExpression", value } },
//   with(left: Expression, right: Expression):                                   WithExpression { return { kind: "WithExpression", left, right } },
//   join(method: JoinExpression['method'], left: Expression, right: Expression): JoinExpression { return { kind: "JoinExpression", method, left, right } },
//   group(left: Expression, by: Expression, right: Expression):                  GroupExpression { return { kind: "GroupExpression", left, by, right } },
//   array(values: Expression[]):                                                 ArrayExpression { return { kind: "ArrayExpression", values } },
//   declare(left: Expression, right: Expression):                                DeclareExpression { return { kind: "DeclareExpression", left, right } },
//   assign(left: Expression, right: Expression):                                 AssignExpression { return { kind: "AssignExpression", left, right } },
//   object(properties: Expression[]):                                            ObjectExpression { return { kind: "ObjectExpression", properties } },
//   eval(expr: Expression):                                                      EvalExpression { return { kind: "EvalExpression", expr } },
// }
// }

// export function rewrite(e: Expression, pattern: Expression, replace: Expression): Expression {
// switch (e.kind) {
//   case "AbstractExpression": {
//     const value = rewrite(e.value, pattern, replace);
//     const e1 = rw.expr.abstract(e.name, value);
//     const cap = match(e1, pattern);
//     return cap === undefined ? e1 : sub(replace, cap);
//   }
//   case "ApplicationExpression": {
//     const left = rewrite(e.left, pattern, replace);
//     const right = rewrite(e.right, pattern, replace);
//     const e1 = rw.expr.app(left, right)
//     const cap = match(e1, pattern);
//     return cap === undefined ? e1 : sub(replace, cap);
//   }
//   case "IdentifierExpression": {
//     const cap = match(e, pattern);
//     return cap === undefined ? e : sub(replace, cap);
//   }
//   case "IntegerExpression": {
//     const cap = match(e, pattern);
//     return cap === undefined ? e : sub(replace, cap);
//   }
//   case "StringExpression": {
//     const cap = match(e, pattern);
//     return cap === undefined ? e : sub(replace, cap);
//   }
//   case "WildcardExpression": {
//     return e;
//   }
//   case "WithExpression": {
//     const left = rewrite(e.left, pattern, replace);
//     const right = rewrite(e.right, pattern, replace);
//     const e1 = rw.expr.with(left, right)
//     const cap = match(e1, pattern);
//     return cap === undefined ? e1 : sub(replace, cap);
//   }
//   case "JoinExpression": {
//     const left = rewrite(e.left, pattern, replace);
//     const right = rewrite(e.right, pattern, replace);
//     const e1 = rw.expr.join(e.method, left, right)
//     const cap = match(e1, pattern);
//     return cap === undefined ? e1 : sub(replace, cap);
//   }
//   case "GroupExpression": {
//     const left = rewrite(e.left, pattern, replace);
//     const by = rewrite(e.by, pattern, replace);
//     const right = rewrite(e.right, pattern, replace);
//     const e1 = rw.expr.group(left, by, right)
//     const cap = match(e1, pattern);
//     return cap === undefined ? e1 : sub(replace, cap);
//   }
//   case "ArrayExpression": {
//     const values = e.values.map(x => rewrite(x, pattern, replace));
//     const e1 = rw.expr.array(values)
//     const cap = match(e1, pattern);
//     return cap === undefined ? e1 : sub(replace, cap);
//   }
//   case "DeclareExpression": {
//     const left = rewrite(e.left, pattern, replace);
//     const right = rewrite(e.right, pattern, replace);
//     const e1 = rw.expr.declare(left, right)
//     const cap = match(e1, pattern);
//     return cap === undefined ? e1 : sub(replace, cap);
//   }
//   case "AssignExpression": {
//     const left = rewrite(e.left, pattern, replace);
//     const right = rewrite(e.right, pattern, replace);
//     const e1 = rw.expr.assign(left, right)
//     const cap = match(e1, pattern);
//     return cap === undefined ? e1 : sub(replace, cap);
//   }
//   case "ObjectExpression": {
//     const properties = e.properties.map(x => rewrite(x, pattern, replace));
//     const e1 = rw.expr.object(properties)
//     const cap = match(e1, pattern);
//     return cap === undefined ? e1 : sub(replace, cap);
//   }
//   case "EvalExpression": {
//     throw new Error(`Eval Expression in rewrite`);
//   }
// }
// }

// type Capture = Record<string, Expression>;

// export function match(e: Expression, pattern: Expression): Capture | undefined {
// let t = 0;
// const imp = (e: Expression, p: Expression, c: Capture | undefined): Capture | undefined => {
//   if (c === undefined) return c;
//   switch (p.kind) {
//     case "WildcardExpression": {
//       if (p.name in c) {
//         const x = exprEqual(c[p.name], e) ? c : undefined;
//         return p.constraint === undefined ? x : e.kind === p.constraint ? x : undefined;
//       }
//       else {
//         const x = { ...c, [p.name]: e };
//         return p.constraint === undefined ? x : e.kind === p.constraint ? x : undefined;
//       }
//     }
//     case "AbstractExpression":
//       if (e.kind === "AbstractExpression")
//         return imp(e.value, p.value, c);
//       break;
//     case "ApplicationExpression":
//       if (e.kind === "ApplicationExpression")
//         return imp(e.right, p.right, imp(e.left, p.left, c));
//       break;
//     case "IdentifierExpression":
//       if (e.kind === "IdentifierExpression")
//         return e.name === p.name ? c : undefined;
//       break;
//     case "IntegerExpression":
//       if (e.kind === "IntegerExpression")
//         return e.value === p.value ? c : undefined;
//       break;
//     case "StringExpression":
//       if (e.kind === "StringExpression")
//         return e.value === p.value ? c : undefined;
//       break;
//     case "WithExpression":
//       if (e.kind === 'WithExpression')
//         return imp(e.right, p.right, imp(e.left, p.left, c));
//       break;
//     case "JoinExpression":
//       if (e.kind === 'JoinExpression')
//         return imp(e.right, p.right, imp(e.left, p.left, c));
//       break;
//     case "GroupExpression":
//       if (e.kind === 'GroupExpression')
//         return imp(e.right, p.right, imp(e.by, p.by, imp(e.left, p.left, c)));
//       break;
//     case "ArrayExpression":
//       if (e.kind === 'ArrayExpression')
//         return e.values.length !== p.values.length ? undefined : p.values.reduce<Capture | undefined>((c, _, i) => imp(e.values[i], p.values[i], c), c);
//       break;
//     case "DeclareExpression":
//       if (e.kind === 'DeclareExpression')
//         return imp(e.right, p.right, imp(e.left, p.left, c));
//       break;
//     case "AssignExpression":
//       if (e.kind === 'AssignExpression')
//         return imp(e.right, p.right, imp(e.left, p.left, c));
//       break;
//     case "ObjectExpression":
//       if (e.kind === 'ObjectExpression') {
//         if (p.properties.length === 0) return e.properties.length === 0 ? c : undefined;
//         if (p.properties.length !== 1) throw new Error(`Invalid rewrite rule cannot have a pattern on an object expression with more than 1 sub pattern`);
//         if (e.properties.length < p.properties.length) return undefined;
//         for (const [i, prop] of e.properties.entries()) {
//           const x = imp(prop, p.properties[0], c);
//           if (x !== undefined)
//             return { ...x, [`_${t++}`]: rw.expr.array(e.properties.filter((_, j) => j !== i)) };
//         }
//         return undefined;
//       }
//       break;
//     case "EvalExpression":
//       throw new Error(`Invalid rewrite rule contains eval expression`);
//   }
//   return undefined;
// }

// return imp(e, pattern, {});
// }

// export function sub(e: Expression, capture: Capture): Expression {
// switch (e.kind) {
//   case "AbstractExpression": {
//     return rw.expr.abstract(e.name, sub(e.value, capture));
//   }
//   case "ApplicationExpression": {
//     return rw.expr.app(sub(e.left, capture), sub(e.right, capture));
//   }
//   case "IdentifierExpression": {
//     return e;
//   }
//   case "IntegerExpression": {
//     return e;
//   }
//   case "StringExpression": {
//     return e;
//   }
//   case "WildcardExpression": {
//     return e.name in capture ? capture[e.name] : e;
//   }
//   case "WithExpression": {
//     return rw.expr.with(sub(e.left, capture), sub(e.right, capture));
//   }
//   case "JoinExpression": {
//     return rw.expr.join(e.method, sub(e.left, capture), sub(e.right, capture));
//   }
//   case "GroupExpression": {
//     return rw.expr.group(sub(e.left, capture), sub(e.by, capture), sub(e.right, capture));
//   }
//   case "ArrayExpression": {
//     return rw.expr.array(e.values.map(x => sub(x, capture)));
//   }
//   case "DeclareExpression": {
//     return rw.expr.declare(sub(e.left, capture), sub(e.right, capture));
//   }
//   case "AssignExpression": {
//     return rw.expr.assign(sub(e.left, capture), sub(e.right, capture));
//   }
//   case "ObjectExpression": {
//     return rw.expr.object(e.properties.map(x => sub(x, capture)).flatMap(x => x.kind === 'ArrayExpression' ? x.values : [x]));
//   }
//   case "EvalExpression": {
//     return normalize(sub(e.expr, capture));
//   }
// }
// }

// export function normalize(e: Expression): Expression {
// switch (e.kind) {
//   case "IdentifierExpression": {
//     return e;
//   }
//   case "IntegerExpression": {
//     return e;
//   }
//   case "AbstractExpression": {
//     return e;
//   }
//   case "ApplicationExpression": {
//     const left = normalize(e.left);
//     const right = normalize(e.right);
//     if (left.kind === 'IdentifierExpression' && left.name === 'reltChildRelation' && right.kind === 'ArrayExpression' && right.values.length === 2)
//       if (right.values[0].kind === "IdentifierExpression" && right.values[1].kind === "IdentifierExpression") {
//         const data: any = { "Person": { "Sale": rw.expr.id("personId") } }
//         const lookup = data[right.values[0].name][right.values[1].name];
//         if (lookup !== undefined)
//           return lookup;
//       }
//     return e;
//   }
//   case "WildcardExpression": {
//     throw new Error(`During evaluation: Wildcard expression appeared!`);
//   }
//   case "StringExpression": {
//     return e;
//   }
//   case "WithExpression": {
//     throw new Error(`During evaluation: Wildcard expression appeared!`);
//   }
//   case "JoinExpression": {
//     throw new Error(`During evaluation: Wildcard expression appeared!`);
//   }
//   case "GroupExpression": {
//     throw new Error(`During evaluation: Wildcard expression appeared!`);
//   }
//   case "ArrayExpression": {
//     return rw.expr.array(e.values.map(normalize));
//   }
//   case "DeclareExpression": {
//     throw new Error(`During evaluation: Wildcard expression appeared!`);
//   }
//   case "AssignExpression": {
//     throw new Error(`During evaluation: Wildcard expression appeared!`);
//   }
//   case "ObjectExpression": {
//     throw new Error(`During evaluation: Wildcard expression appeared!`);
//   }
//   case "EvalExpression": {
//     return normalize(e.expr);
//   }
// }
// }

// export function exprEqual(l: Expression, r: Expression): boolean {
// switch (l.kind) {
//   case "WildcardExpression":
//     return r.kind === "WildcardExpression" && l.name === r.name && l.constraint === r.constraint;
//   case "AbstractExpression":
//     return r.kind === "AbstractExpression" && l.name === r.name && exprEqual(l.value, r.value);
//   case "ApplicationExpression":
//     return r.kind === "ApplicationExpression" && exprEqual(l.left, r.left) && exprEqual(l.right, r.right);
//   case "IdentifierExpression":
//     return r.kind === "IdentifierExpression" && l.name === r.name;
//   case "IntegerExpression":
//     return r.kind === "IntegerExpression" && l.value === r.value;
//   case "StringExpression":
//     return r.kind === "StringExpression" && l.value === r.value;
//   case "WithExpression":
//     return r.kind === "WithExpression" && exprEqual(l.left, r.left) && exprEqual(l.right, r.right);
//   case "JoinExpression":
//     return r.kind === "JoinExpression" && l.method === r.method && exprEqual(l.left, r.left) && exprEqual(l.right, r.right);
//   case "GroupExpression":
//     return r.kind === "GroupExpression" && exprEqual(l.left, r.left) && exprEqual(l.by, r.by) && exprEqual(l.right, r.right);
//   case "ArrayExpression":
//     return r.kind === "ArrayExpression" && l.values.length === r.values.length && l.values.every((_, i) => exprEqual(l.values[i], r.values[i]));
//   case "DeclareExpression":
//     return r.kind === "DeclareExpression" && exprEqual(l.left, r.left) && exprEqual(l.right, r.right);
//   case "AssignExpression":
//     return r.kind === "AssignExpression" && exprEqual(l.left, r.left) && exprEqual(l.right, r.right);
//   case "ObjectExpression":
//     return r.kind === "ObjectExpression" && l.properties.length === r.properties.length && l.properties.every((_, i) => exprEqual(l.properties[i], r.properties[i]));
//   case "EvalExpression":
//     return false;
// }
// }

// export function pretty(e: Expression): string {
// switch (e.kind) {
//   case "AbstractExpression":
//     return `\\${e.name} => (${pretty(e.value)})`;
//   case "ApplicationExpression":
//     return `${pretty(e.left)} ${pretty(e.right)}`;
//   case "IdentifierExpression":
//     return `${e.name}`;
//   case "IntegerExpression":
//     return `${e.value}`;
//   case "StringExpression":
//     return `"${e.value}"`;
//   case "WildcardExpression":
//     return `_${e.name}`;
//   case "WithExpression":
//     return `${pretty(e.left)} with ${pretty(e.right)}`;
//   case "JoinExpression":
//     return `${pretty(e.left)} ${e.method} join ${pretty(e.right)}`;
//   case "GroupExpression":
//     return `group ${pretty(e.left)} by ${pretty(e.by)} agg ${pretty(e.right)}`;
//   case "ArrayExpression":
//     return `[${e.values.map(pretty).join(', ')}]`;
//   case "DeclareExpression":
//     return `${pretty(e.left)}: ${pretty(e.right)}`;
//   case "AssignExpression":
//     return `${pretty(e.left)} = ${pretty(e.right)}`;
//   case "ObjectExpression":
//     return `{ ${e.properties.map(pretty).join(', ')} }`;
//   case "EvalExpression":
//     return `$${pretty(e.expr)}$`;
// }
// }

// const e = rw.expr.with(rw.expr.id('Person'), rw.expr.object([
// rw.expr.declare(rw.expr.id('sales'), rw.expr.array([rw.expr.id('Sale')])),
// //rw.expr.assign(rw.expr.id('greeting'), rw.expr.string("hi")),
// ]))

// console.log(pretty(e));

// const p = rw.expr.with(rw.expr.wildcard('A'), rw.expr.object([
// rw.expr.declare(rw.expr.wildcard('B'), rw.expr.array([rw.expr.wildcard('C')])),
// ]));
// const r = rw.expr.with(rw.expr.join('left', rw.expr.wildcard('A'), rw.expr.group(rw.expr.wildcard('C'), rw.expr.eval(rw.expr.app(rw.expr.id("reltChildRelation"), rw.expr.array([rw.expr.wildcard("A"), rw.expr.wildcard("C")]))), rw.expr.object([
// rw.expr.assign(rw.expr.wildcard('C'), rw.expr.app(rw.expr.id("collect"), rw.expr.id("this")))
// ]))), rw.expr.object([
// rw.expr.wildcard('_0'),
// ]));

// const p2 = rw.expr.with(rw.expr.wildcard('A'), rw.expr.object([]));
// const r2 = rw.expr.wildcard('A');

// const e1 = rewrite(e, p, r);
// const e2 = rewrite(e1, p2, r2);

// console.log(pretty(e1));
// console.log(pretty(e2));

