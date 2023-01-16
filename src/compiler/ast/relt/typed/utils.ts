import { inspect } from "util";
import { TypedCmpExpression, TypedExpression, TypedObjectExpressionProperty, TypedAddExpression, TypedMulExpression } from ".";
import { reportInternalError, reportUserError } from "../../../errors";
import { FunctionType } from "../type";
import { format } from "./format";

export type Visitor<T extends { kind: string }, R> = { [K in T['kind']]?: (e: T & { kind: K }) => R }

export function kids(e: TypedExpression): TypedExpression[] {
  switch (e.kind) {
    case "TypedIdentifierExpression": return [];
    case "TypedPlaceholderExpression": return [];
    case "TypedIntegerExpression": return [];
    case "TypedFloatExpression": return [];
    case "TypedStringExpression": return [];
    case "TypedEnvExpression": return [];
    case "TypedBooleanExpression": return [];
    case "TypedNullExpression": return [];
    case "TypedLetExpression": return [e.value];
    case "TypedTableExpression": return [e.value];
    case "TypedFunctionExpression": return [e.value];
    case "TypedEvalExpression": return [e.node];
    case "TypedDeclareExpression": return [e.value];
    case "TypedSpreadExpression": return [e.value];
    case "TypedAssignExpression": return [e.left, e.right];
    case "TypedConditionalExpression": return [e.left, e.right];
    case "TypedOrExpression": return [e.left, e.right];
    case "TypedAndExpression": return [e.left, e.right];
    case "TypedCmpExpression": return [e.left, e.right];
    case "TypedAddExpression": return [e.left, e.right];
    case "TypedMulExpression": return [e.left, e.right];
    case "TypedUnionExpression": return [e.left, e.right];
    case "TypedWhereExpression": return [e.left, e.right];
    case "TypedWithExpression": return [e.left, e.right];
    case "TypedDropExpression": return [e.left, e.right];
    case "TypedSelectExpression": return [e.left, e.right];
    case "TypedDotExpression": return [e.left, e.right];
    case "TypedApplicationExpression": return [e.left, e.right];
    case "TypedJoinExpression": return e.on === undefined ? [e.left, e.right] : [e.left, e.right, e.on];
    case "TypedGroupByExpression": return [e.value, e.by, e.agg];
    case "TypedBlockExpression": return e.expressions;
    case "TypedObjectExpression": return e.properties;
    case "TypedArrayExpression": return e.values;
    case "TypedIndexExpression": return [e.left, e.index];
    case "TypedInternalExpression": return [];
  }
}

function assertTypedObjectExpressionKidsValid(x: TypedExpression[]): asserts x is TypedObjectExpressionProperty[] {
  if (!x.every(x => x.kind === 'TypedIdentifierExpression' || x.kind === "TypedAssignExpression" || x.kind === "TypedDeclareExpression" || x.kind === 'TypedSpreadExpression'))
    reportInternalError(`Internal error: when constructing TypedObjectExpression from kids: ${[x.map(x => x.kind).join(', ')]}`);
}

export function fromKids(e: TypedExpression, kids: TypedExpression[]): TypedExpression {
  switch (e.kind) {
    case "TypedIdentifierExpression":
      return { ...e, };
    case "TypedPlaceholderExpression":
      return { ...e, };
    case "TypedIntegerExpression":
      return { ...e, };
    case "TypedFloatExpression":
      return { ...e, };
    case "TypedStringExpression":
      return { ...e, };
    case "TypedEnvExpression":
      return { ...e, };
    case "TypedBooleanExpression":
      return { ...e, };
    case "TypedNullExpression":
      return { ...e, };
    case "TypedLetExpression":
      if (kids[0].kind !== 'TypedAssignExpression' && kids[0].kind !== 'TypedDeclareExpression')
        reportInternalError(`Internal error: when constructing ${e.kind} from kids: ${[kids.map(x => x.kind).join(', ')]}`);
      return { ...e, value: kids[0] };
    case "TypedTableExpression":
      if (kids[0].kind !== 'TypedAssignExpression')
        reportInternalError(`Internal error: when constructing ${e.kind} from kids: ${[kids.map(x => x.kind).join(', ')]}`);
      return { ...e, value: kids[0] as any }; // Tighten up check
    case "TypedFunctionExpression":
      return { ...e, value: kids[0] };
    case "TypedEvalExpression":
      return { ...e, node: kids[0] };
    case "TypedDeclareExpression":
      if (kids[0].kind !== 'TypedIdentifierExpression')
        reportInternalError(`Internal error: when constructing ${e.kind} from kids: ${[kids.map(x => x.kind).join(', ')]}`);
      return { ...e, value: kids[0] };
    case "TypedSpreadExpression":
      return { ...e, value: kids[0] };
    case "TypedAssignExpression":
      if (kids[0].kind !== 'TypedIdentifierExpression' && kids[0].kind !== 'TypedArrayExpression' && kids[0].kind !== 'TypedObjectExpression')
        reportInternalError(`Internal error: when constructing ${e.kind} from kids: ${[kids.map(x => x.kind).join(', ')]}`);
      return { ...e, left: kids[0] as any, right: kids[1] }; // Tighten up check
    case "TypedConditionalExpression":
      return { ...e, left: kids[0], right: kids[1] };
    case "TypedOrExpression":
      return { ...e, left: kids[0], right: kids[1] };
    case "TypedAndExpression":
      return { ...e, left: kids[0], right: kids[1] };
    case "TypedCmpExpression":
      return { ...e, left: kids[0], right: kids[1] };
    case "TypedAddExpression":
      return { ...e, left: kids[0], right: kids[1] };
    case "TypedMulExpression":
      return { ...e, left: kids[0], right: kids[1] };
    case "TypedUnionExpression":
      return { ...e, left: kids[0], right: kids[1] };
    case "TypedWhereExpression":
      return { ...e, left: kids[0], right: kids[1] };
    case "TypedWithExpression":
      return { ...e, left: kids[0], right: kids[1] as any }; // Tighten up check
    case "TypedDropExpression":
      if (kids[1].kind !== 'TypedIdentifierExpression' && kids[1].kind !== 'TypedArrayExpression')
        reportInternalError(`Internal error: when constructing ${e.kind} from kids: ${[kids.map(x => x.kind).join(', ')]}`);
      return { ...e, left: kids[0], right: kids[1] as any }; // Tighten up check
    case "TypedSelectExpression":
      if (kids[1].kind !== 'TypedIdentifierExpression' && kids[1].kind !== 'TypedArrayExpression')
        reportInternalError(`Internal error: when constructing ${e.kind} from kids: ${[kids.map(x => x.kind).join(', ')]}`);
      return { ...e, left: kids[0], right: kids[1] as any }; // Tighten up check
    case "TypedDotExpression":
      return { ...e, left: kids[0], right: kids[1] };
    case "TypedApplicationExpression":
      return { ...e, left: kids[0], right: kids[1] };
    case "TypedJoinExpression":
      return { ...e, left: kids[0], right: kids[1], on: kids[2] };
    case "TypedGroupByExpression":
      if (kids[1].kind !== 'TypedIdentifierExpression' && kids[1].kind !== 'TypedArrayExpression')
        reportInternalError(`Internal error: when constructing ${e.kind} from kids: ${[kids.map(x => x.kind).join(', ')]}`);
      return { ...e, value: kids[0], by: kids[1] as any, agg: kids[2] as any }; // Tighten up check
    case "TypedBlockExpression":
      return { ...e, expressions: kids };
    case "TypedObjectExpression":
      assertTypedObjectExpressionKidsValid(kids);
      return { ...e, properties: kids };
    case "TypedArrayExpression":
      return { ...e, values: kids };
    case "TypedIndexExpression":
      return { ...e, left: kids[0], index: kids[1] };
    case "TypedInternalExpression":
      return { ...e };
  }
}

export function shallowEquals(l: TypedExpression, r: TypedExpression): boolean {
  switch (l.kind) {
    case "TypedIdentifierExpression": return r.kind === "TypedIdentifierExpression" && l.name === r.name;
    case "TypedPlaceholderExpression": return r.kind === "TypedPlaceholderExpression" && l.name === r.name;
    case "TypedIntegerExpression": return r.kind === "TypedIntegerExpression" && l.value === r.value;
    case "TypedFloatExpression": return r.kind === "TypedFloatExpression" && l.value === r.value;
    case "TypedStringExpression": return r.kind === "TypedStringExpression" && l.value === r.value;
    case "TypedEnvExpression": return r.kind === "TypedEnvExpression" && l.value === r.value;
    case "TypedBooleanExpression": return r.kind === "TypedBooleanExpression" && l.value === r.value;
    case "TypedNullExpression": return r.kind === "TypedNullExpression";
    case "TypedLetExpression": return r.kind === "TypedLetExpression";
    case "TypedTableExpression": return r.kind === "TypedTableExpression";
    case "TypedFunctionExpression": return r.kind === "TypedFunctionExpression";
    case "TypedEvalExpression": return r.kind === "TypedEvalExpression";
    case "TypedDeclareExpression": return r.kind === "TypedDeclareExpression";
    case "TypedSpreadExpression": return r.kind === "TypedSpreadExpression";
    case "TypedAssignExpression": return r.kind === "TypedAssignExpression";
    case "TypedConditionalExpression": return r.kind === "TypedConditionalExpression";
    case "TypedOrExpression": return r.kind === "TypedOrExpression";
    case "TypedAndExpression": return r.kind === "TypedAndExpression";
    case "TypedCmpExpression": return r.kind === "TypedCmpExpression";
    case "TypedAddExpression": return r.kind === "TypedAddExpression";
    case "TypedMulExpression": return r.kind === "TypedMulExpression";
    case "TypedUnionExpression": return r.kind === "TypedUnionExpression";
    case "TypedWhereExpression": return r.kind === "TypedWhereExpression";
    case "TypedWithExpression": return r.kind === "TypedWithExpression";
    case "TypedDropExpression": return r.kind === "TypedDropExpression";
    case "TypedSelectExpression": return r.kind === "TypedSelectExpression";
    case "TypedDotExpression": return r.kind === "TypedDotExpression";
    case "TypedApplicationExpression": return r.kind === "TypedApplicationExpression";
    case "TypedJoinExpression": return r.kind === "TypedJoinExpression";
    case "TypedGroupByExpression": return r.kind === "TypedGroupByExpression";
    case "TypedBlockExpression": return r.kind === "TypedBlockExpression";
    case "TypedObjectExpression": return r.kind === "TypedObjectExpression";
    case "TypedArrayExpression": return r.kind === "TypedArrayExpression";
    case "TypedIndexExpression": return r.kind === "TypedIndexExpression";
    case "TypedInternalExpression": return false;
  }
}

export function ofKind<K extends TypedExpression['kind']>(kind: K) {
  return (e: TypedExpression): e is TypedExpression & { kind: K } => e.kind === kind;
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

export function gather<T extends TypedExpression>(e: TypedExpression, f: (e: TypedExpression) => e is T): T[] {
  return visit(e, {}, (x) => f(x) ? [x] : [], x => x).flat() as T[];
}

type Scope = Record<string, TypedExpression>;

type BinaryOp<T extends { op: string }> = Record<T['op'], Partial<Record<TypedExpression['kind'], Partial<Record<TypedExpression['kind'], (l: TypedExpression, r: TypedExpression) => TypedExpression>>>>>;

const cmpOps: BinaryOp<TypedCmpExpression> = {
  '==': {
    TypedIntegerExpression: {
      // @ts-expect-error
      TypedIntegerExpression: (l, r) => ({ kind: "TypedBooleanExpression", value: l.value === r.value, type: { kind: "BooleanType" } }),
      // @ts-expect-error
      TypedFloatExpression: (l, r) => ({ kind: "TypedBooleanExpression", value: l.value === r.value, type: { kind: "BooleanType" } }),
    },
    TypedFloatExpression: {
      // @ts-expect-error
      TypedFloatExpression: (l, r) => ({ kind: "TypedBooleanExpression", value: l.value === r.value, type: { kind: "BooleanType" } }),
      // @ts-expect-error
      TypedIntegerExpression: (l, r) => ({ kind: "TypedBooleanExpression", value: l.value === r.value, type: { kind: "BooleanType" } }),
    },
    TypedStringExpression: {
      // @ts-expect-error
      TypedStringExpression: (l, r) => ({ kind: "TypedBooleanExpression", value: l.value === r.value, type: { kind: "BooleanType" } }),
    },
    TypedBooleanExpression: {
      // @ts-expect-error
      TypedBooleanExpression: (l, r) => ({ kind: "TypedBooleanExpression", value: l.value === r.value, type: { kind: "BooleanType" } }),
    },
    TypedNullExpression: {
      TypedNullExpression: (l, r) => ({ kind: "TypedBooleanExpression", value: true, type: { kind: "BooleanType" } }),
    },
  },
  "!=": {
    TypedIntegerExpression: {
      // @ts-expect-error
      TypedIntegerExpression: (l, r) => ({ kind: "TypedBooleanExpression", value: l.value !== r.value, type: { kind: "BooleanType" } }),
      // @ts-expect-error
      TypedFloatExpression: (l, r) => ({ kind: "TypedBooleanExpression", value: l.value !== r.value, type: { kind: "BooleanType" } }),
    },
    TypedFloatExpression: {
      // @ts-expect-error
      TypedFloatExpression: (l, r) => ({ kind: "TypedBooleanExpression", value: l.value !== r.value, type: { kind: "BooleanType" } }),
      // @ts-expect-error
      TypedIntegerExpression: (l, r) => ({ kind: "TypedBooleanExpression", value: l.value !== r.value, type: { kind: "BooleanType" } }),
    },
    TypedStringExpression: {
      // @ts-expect-error
      TypedStringExpression: (l, r) => ({ kind: "TypedBooleanExpression", value: l.value !== r.value, type: { kind: "BooleanType" } }),
    },
    TypedBooleanExpression: {
      // @ts-expect-error
      TypedBooleanExpression: (l, r) => ({ kind: "TypedBooleanExpression", value: l.value !== r.value, type: { kind: "BooleanType" } }),
    },
    TypedNullExpression: {
      TypedNullExpression: (l, r) => ({ kind: "TypedBooleanExpression", value: false, type: { kind: "BooleanType" } }),
    },
  },
  "<": {
    TypedIntegerExpression: {
      // @ts-expect-error
      TypedIntegerExpression: (l, r) => ({ kind: "TypedBooleanExpression", value: l.value < r.value, type: { kind: "BooleanType" } }),
      // @ts-expect-error
      TypedFloatExpression: (l, r) => ({ kind: "TypedBooleanExpression", value: l.value < Number(r.value), type: { kind: "BooleanType" } }),
    },
    TypedFloatExpression: {
      // @ts-expect-error
      TypedFloatExpression: (l, r) => ({ kind: "TypedBooleanExpression", value: Number(l.value) < Number(r.value), type: { kind: "BooleanType" } }),
      // @ts-expect-error
      TypedIntegerExpression: (l, r) => ({ kind: "TypedBooleanExpression", value: Number(l.value) < r.value, type: { kind: "BooleanType" } }),
    },
  },
  "<=": {
    TypedIntegerExpression: {
      // @ts-expect-error
      TypedIntegerExpression: (l, r) => ({ kind: "TypedBooleanExpression", value: l.value <= r.value, type: { kind: "BooleanType" } }),
      // @ts-expect-error
      TypedFloatExpression: (l, r) => ({ kind: "TypedBooleanExpression", value: l.value <= Number(r.value), type: { kind: "BooleanType" } }),
    },
    TypedFloatExpression: {
      // @ts-expect-error
      TypedFloatExpression: (l, r) => ({ kind: "TypedBooleanExpression", value: Number(l.value) <= Number(r.value), type: { kind: "BooleanType" } }),
      // @ts-expect-error
      TypedIntegerExpression: (l, r) => ({ kind: "TypedBooleanExpression", value: Number(l.value) <= r.value, type: { kind: "BooleanType" } }),
    },
  },
  ">": {
    TypedIntegerExpression: {
      // @ts-expect-error
      TypedIntegerExpression: (l, r) => ({ kind: "TypedBooleanExpression", value: l.value > r.value, type: { kind: "BooleanType" } }),
      // @ts-expect-error
      TypedFloatExpression: (l, r) => ({ kind: "TypedBooleanExpression", value: l.value > Number(r.value), type: { kind: "BooleanType" } }),
    },
    TypedFloatExpression: {
      // @ts-expect-error
      TypedFloatExpression: (l, r) => ({ kind: "TypedBooleanExpression", value: Number(l.value) > Number(r.value), type: { kind: "BooleanType" } }),
      // @ts-expect-error
      TypedIntegerExpression: (l, r) => ({ kind: "TypedBooleanExpression", value: Number(l.value) > r.value, type: { kind: "BooleanType" } }),
    },
  },
  ">=": {
    TypedIntegerExpression: {
      // @ts-expect-error
      TypedIntegerExpression: (l, r) => ({ kind: "TypedBooleanExpression", value: l.value >= r.value, type: { kind: "BooleanType" } }),
      // @ts-expect-error
      TypedFloatExpression: (l, r) => ({ kind: "TypedBooleanExpression", value: l.value >= Number(r.value), type: { kind: "BooleanType" } }),
    },
    TypedFloatExpression: {
      // @ts-expect-error
      TypedFloatExpression: (l, r) => ({ kind: "TypedBooleanExpression", value: Number(l.value) >= Number(r.value), type: { kind: "BooleanType" } }),
      // @ts-expect-error
      TypedIntegerExpression: (l, r) => ({ kind: "TypedBooleanExpression", value: Number(l.value) >= r.value, type: { kind: "BooleanType" } }),
    },
  },
}

const addOps: BinaryOp<TypedAddExpression> = {
  '+': {
    TypedIntegerExpression: {
      // @ts-expect-error
      TypedIntegerExpression: (l, r) => ({ kind: "TypedIntegerExpression", value: l.value + r.value, type: { kind: "IntegerType" } }),
      // @ts-expect-error
      TypedFloatExpression: (l, r) => ({ kind: "TypedFloatExpression", value: l.value + Number(r.value), type: { kind: "FloatType" } }),
    },
    TypedFloatExpression: {
      // @ts-expect-error
      TypedFloatExpression: (l, r) => ({ kind: "TypedFloatExpression", value: Number(l.value) + Number(r.value), type: { kind: "FloatType" } }),
      // @ts-expect-error
      TypedIntegerExpression: (l, r) => ({ kind: "TypedFloatExpression", value: Number(l.value) + r.value, type: { kind: "FloatType" } }),
    },
    TypedStringExpression: {
      // @ts-expect-error
      TypedStringExpression: (l, r) => ({ kind: "TypedStringExpression", value: l.value + r.value, type: { kind: "StringType" } }),
    },
  },
  "-": {
    TypedIntegerExpression: {
      // @ts-expect-error
      TypedIntegerExpression: (l, r) => ({ kind: "TypedIntegerExpression", value: l.value - r.value, type: { kind: "IntegerType" } }),
      // @ts-expect-error
      TypedFloatExpression: (l, r) => ({ kind: "TypedFloatExpression", value: l.value - Number(r.value), type: { kind: "FloatType" } }),
    },
    TypedFloatExpression: {
      // @ts-expect-error
      TypedFloatExpression: (l, r) => ({ kind: "TypedFloatExpression", value: Number(l.value) - Number(r.value), type: { kind: "FloatType" } }),
      // @ts-expect-error
      TypedIntegerExpression: (l, r) => ({ kind: "TypedFloatExpression", value: Number(l.value) - r.value, type: { kind: "FloatType" } }),
    },
  },
}

const mulOps: BinaryOp<TypedMulExpression> = {
  '*': {
    TypedIntegerExpression: {
      // @ts-expect-error
      TypedIntegerExpression: (l, r) => ({ kind: "TypedIntegerExpression", value: l.value * r.value, type: { kind: "IntegerType" } }),
      // @ts-expect-error
      TypedFloatExpression: (l, r) => ({ kind: "TypedFloatExpression", value: l.value * Number(r.value), type: { kind: "FloatType" } }),
    },
    TypedFloatExpression: {
      // @ts-expect-error
      TypedFloatExpression: (l, r) => ({ kind: "TypedFloatExpression", value: Number(l.value) * Number(r.value), type: { kind: "FloatType" } }),
      // @ts-expect-error
      TypedIntegerExpression: (l, r) => ({ kind: "TypedFloatExpression", value: Number(l.value) * r.value, type: { kind: "FloatType" } }),
    },
  },
  "/": {
    TypedIntegerExpression: {
      // @ts-expect-error
      TypedIntegerExpression: (l, r) => ({ kind: "TypedIntegerExpression", value: Math.floor(l.value / r.value), type: { kind: "IntegerType" } }),
      // @ts-expect-error
      TypedFloatExpression: (l, r) => ({ kind: "TypedFloatExpression", value: l.value / Number(r.value), type: { kind: "FloatType" } }),
    },
    TypedFloatExpression: {
      // @ts-expect-error
      TypedFloatExpression: (l, r) => ({ kind: "TypedFloatExpression", value: Number(l.value) / Number(r.value), type: { kind: "FloatType" } }),
      // @ts-expect-error
      TypedIntegerExpression: (l, r) => ({ kind: "TypedFloatExpression", value: Number(l.value) / r.value, type: { kind: "FloatType" } }),
    },
  },
  "%": {
    TypedIntegerExpression: {
      // @ts-expect-error
      TypedIntegerExpression: (l, r) => ({ kind: "TypedIntegerExpression", value: l.value % r.value, type: { kind: "IntegerType" } }),
      // @ts-expect-error
      TypedFloatExpression: (l, r) => ({ kind: "TypedIntegerExpression", value: l.value % Number(r.value), type: { kind: "FloatType" } }),
    },
    TypedFloatExpression: {
      // @ts-expect-error
      TypedFloatExpression: (l, r) => ({ kind: "TypedFloatExpression", value: Number(l.value) % Number(r.value), type: { kind: "FloatType" } }),
      // @ts-expect-error
      TypedIntegerExpression: (l, r) => ({ kind: "TypedFloatExpression", value: Number(l.value) % r.value, type: { kind: "FloatType" } }),
    },
  },
}

function normalizeBinaryOp<T extends TypedExpression & { op: string, left: TypedExpression, right: TypedExpression }>(e: T, ops: BinaryOp<T>, scope: Scope): [TypedExpression, Scope] {
  const [l] = normalize(e.left, scope);
  const [r] = normalize(e.right, scope);
  const f = (ops as any)[e.op]?.[l.kind]?.[r.kind];
  return [f === undefined ? fromKids(e, [l, r]) : f(l, r), scope];
}

export function normalize(e: TypedExpression, scope: Scope): [TypedExpression, Scope] {
  switch (e.kind) {
    case "TypedApplicationExpression": {
      let [l] = normalize(e.left, scope);
      const [r] = normalize(e.right, scope);
      while (l.kind === "TypedApplicationExpression") {
        l = normalize(l, scope)[0];
      }
      if (l.kind === "TypedInternalExpression") {
        return [l.value(r), scope];
      }
      if (l.kind !== "TypedFunctionExpression") {
        return [{ ...e, left: l, right: r }, scope];
      }
      const arg = l.args[0];

      const name = (() => {
        switch (arg.kind) {
          case "TypedDeclareExpression":
            return arg.value.name;
          default:
            reportInternalError(``);
        }
      })();

      const [value] = normalize(betaReduction(l.value, { [name]: r }), scope);

      return [l.args.length === 1 ? value : { kind: "TypedFunctionExpression", args: l.args.slice(1), name: l.name, value, type: l.type.to as FunctionType }, scope];
    }

    case "TypedFunctionExpression": {
      if (e.args.length === 0) {
        const [value] = normalize(e.value, scope);
        return [value, e.name === undefined ? scope : { ...scope, [e.name]: value }]
      }
      return [e, scope];
    }
    case "TypedTableExpression": return [e, scope];
    case "TypedLetExpression": {
      switch (e.value.kind) {
        case "TypedAssignExpression":
          switch (e.value.left.kind) {
            case "TypedArrayExpression":
            case "TypedObjectExpression":
              reportInternalError(`TODO`);
            case "TypedIdentifierExpression": {
              const [value] = normalize(e.value.right, scope);
              return [{ ...e, value: { ...e.value, right: value } }, { ...scope, [e.value.left.name]: value }]
            }
          }
        case "TypedDeclareExpression":
          reportInternalError(`TODO`);
      }
    }
    case "TypedAssignExpression": {
      switch (e.left.kind) {
        case "TypedArrayExpression":
        case "TypedObjectExpression":
          reportInternalError(`TODO`);
        case "TypedIdentifierExpression": {
          const [value] = normalize(e.right, scope);
          return e.left.name in scope ? [value, { ...scope, [e.left.name]: value }] : [{ kind: "TypedAssignExpression", left: e.left, op: "=", right: value, type: e.type }, scope];
        }
      }
    }
    case "TypedDeclareExpression": return [e, scope];
    case "TypedEvalExpression": return normalize(e.node, scope);
    case "TypedIntegerExpression": return [e, scope];
    case "TypedFloatExpression": return [e, scope];
    case "TypedStringExpression": return [e, scope];
    case "TypedEnvExpression": {
      // maybe add a security check here
      const value = process.env[e.value];
      if (value === undefined) reportUserError(`Environment variable "${e.value}" is not set`);
      return [{ kind: "TypedStringExpression", type: { kind: "StringType" }, value: process.env[e.value]! }, scope];
    }
    case "TypedBooleanExpression": return [e, scope];
    case "TypedNullExpression": return [e, scope];
    case "TypedBlockExpression": {
      if (e.expressions.length === 0) return [e, scope];
      const [x] = e.expressions.reduce<[TypedExpression, Scope]>((p, c) => normalize(c, p[1]), [e, scope]);
      return [x, scope];
    }
    case "TypedConditionalExpression": {
      const [left] = normalize(e.left, scope);
      switch (e.op) {
        case "??": return [left.kind === "TypedNullExpression" ? normalize(e.left, scope)[0] : left, scope];
        case "!?": return [left.kind !== "TypedNullExpression" ? normalize(e.left, scope)[0] : left, scope];
      }
    }
    case "TypedOrExpression": {
      const [left] = normalize(e.left, scope);
      if (left.kind !== "TypedBooleanExpression")
        reportInternalError(``);
      switch (e.op) {
        case "||": return [left.value === true ? left : normalize(e.left, scope)[0], scope];
      }
    }
    case "TypedAndExpression": {
      const [left] = normalize(e.left, scope);
      if (left.kind !== "TypedBooleanExpression")
        reportInternalError(``);
      switch (e.op) {
        case "&&": return [left.value === false ? left : normalize(e.left, scope)[0], scope];
      }
    }
    case "TypedCmpExpression": {
      return normalizeBinaryOp(e, cmpOps, scope);
    }
    case "TypedAddExpression": {
      return normalizeBinaryOp(e, addOps, scope);
    }
    case "TypedMulExpression": {
      return normalizeBinaryOp(e, mulOps, scope);
    }
    case "TypedDotExpression": {
      const [l] = normalize(e.left, scope);
      if (l.kind !== "TypedObjectExpression")
        return [{ kind: "TypedDotExpression", left: l, right: e.right, type: e.type }, scope];
      if (e.right.kind !== "TypedIdentifierExpression")
        reportInternalError(`${e.right.kind}`);
      const name = e.right.name
      const x = l.properties.find(x => {
        switch (x.kind) {
          case "TypedAssignExpression":
            switch (x.left.kind) {
              case "TypedArrayExpression":
              case "TypedObjectExpression":
                reportUserError(`TOOD`);
              case "TypedIdentifierExpression":
                return x.left.name === name
            }
          case "TypedIdentifierExpression":
            return x.name === name;
        }
        return false;
      });

      if (x === undefined)
        reportInternalError(`${name} is not defined on dot ${format(l)} but during normalization tried to access it`);

      return [x.kind === "TypedAssignExpression" ? x.right : x, scope];
    }
    case "TypedIdentifierExpression": return e.name in scope ? [scope[e.name], scope] : [e, scope];
    case "TypedObjectExpression": {
      const properties = e.properties.map(x => normalize(x, scope)[0]) as TypedObjectExpressionProperty[];
      return [{ kind: "TypedObjectExpression", properties, type: e.type }, scope];
    }
    case "TypedArrayExpression": {
      const values = e.values.map(x => normalize(x, scope)[0]);
      return [{ kind: "TypedArrayExpression", values, type: e.type }, scope];
    }
    case "TypedIndexExpression": {
      const [left] = normalize(e.left, scope);
      const [index] = normalize(e.index, scope);
      console.log(left);
      if (left.kind === "TypedArrayExpression" && index.kind === "TypedIntegerExpression") {
        if (index.value >= left.values.length)
          reportUserError(`Out of Bounds Error duding normalization: size ${left.values.length} index: ${index.value}`);
        return [left.values[index.value], scope];
      }
      return [{ kind: "TypedIndexExpression", left, index, type: e.type }, scope];
    }
    case "TypedInternalExpression": {
      return [e, scope];
    }
    case "TypedSpreadExpression":
      reportInternalError(`${e.kind} is not allowed in normalization outside object or array expression`);
    case "TypedUnionExpression":
    case "TypedJoinExpression":
    case "TypedGroupByExpression":
    case "TypedWhereExpression":
    case "TypedWithExpression":
    case "TypedDropExpression":
    case "TypedSelectExpression":
    case "TypedPlaceholderExpression":
      reportUserError(`${e.kind} is not allowed in normalization`);
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
            return { ...x, [`_${t++}`]: { kind: "TypedArrayExpression", type: { kind: "ArrayType", of: { kind: "AnyType" } }, values: e.properties.filter((_, j) => j !== i) } };
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
  switch (e.kind) {
    case "TypedPlaceholderExpression":
      return e.name in capture ? capture[e.name] : e;
    default:
      return applyKids(e, x => substitute(x, capture));
  }
}

export function betaReduction(e: TypedExpression, capture: Capture): TypedExpression {
  switch (e.kind) {
    case "TypedIdentifierExpression":
      return e.name in capture ? capture[e.name] : e;
    default:
      return applyKids(e, x => betaReduction(x, capture));
  }
}
