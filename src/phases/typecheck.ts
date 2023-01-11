import { relt } from "../ast/builder";
import { Expression, IdentifierExpression } from "../ast/relt/source";
import { ArrayType, ObjectType, TableType, Type } from "../ast/relt/type";
import { typeEquals, typeName } from "../ast/relt/type/utils";
import { ofKind, ofLeft, TypedExpression, TypedIdentifierExpression, TypedObjectExpression, TypedTableExpression, ValidObjectPropertyExpression } from "../ast/relt/typed";
import { normalize } from "../ast/relt/typed/utils";

export type Context = Record<string, Type>;
export type Scope = Record<string, TypedExpression>;

function makeIdentifierTypeFor(name: string, ctx: Context, scope: Scope): Type {
  if (!(name in ctx))
    throw new Error(`Error: ${name} is not defined`);
  return ctx[name];
}

function makeObjectTypeFor(properties: TypedExpression[], ctx: Context, scope: Scope): [ObjectType, TypedObjectExpression['properties']] {
  const props: ValidObjectPropertyExpression[] = [];

  for (const p of properties) {
    switch (p.kind) {
      case "TypedAssignExpression":
        if (ofLeft('TypedIdentifierExpression')(p))
          props.push(p);
        else
          throw new Error(`Error: Left of ${p.kind} was no an identifier`);
        break;
      case "TypedArrayExpression": {
        props.push(...makeObjectTypeFor(p.values, ctx, scope)[1]);
        break;
      }
      default:
        throw new Error(`Error: Cannot convert ${p.kind} into an object`);
    }
  }

  return [relt.type.object(props.map(x => ({ name: x.left.name, type: x.type }))), props];
}

function makeArrayTypeFor(values: TypedExpression[], ctx: Context, scope: Scope): ArrayType {
  if (values.length === 0) return relt.type.array(relt.type.any);
  const ty = values[0].type;
  for (const [i, v] of values.entries())
    if (!typeEquals(ty, v.type))
      throw new Error(`Error: Type mismatch at position ${i} ${typeName(ty)} != ${typeName(v.type)}`);
  return relt.type.array(ty);
}

function makeTableTypeFor(name: string, columns: TypedExpression[], ctx: Context, scope: Scope): [TableType, TypedTableExpression['columns']] {
  const validColumns: TypedTableExpression['columns'] = [];

  for (const p of columns) {
    switch (p.kind) {
      case "TypedDeclareExpression":
        if (ofLeft('TypedIdentifierExpression')(p))
          validColumns.push(p);
        else
          throw new Error(`Error: Left of ${p.kind} was no an identifier`);
        break;
      case "TypedArrayExpression": {
        validColumns.push(...makeTableTypeFor(name, p.values, ctx, scope)[1]);
        break;
      }
      default:
        throw new Error(`Error: Cannot convert ${p.kind} into an table`);
    }
  }

  return [relt.type.table(name, validColumns.map(x => ({ name: x.left.name, type: x.type }))), validColumns];
}

function makeApplicationTypeFor(l: TypedExpression, r: TypedExpression, ctx: Context, scope: Scope): Type {
  if (l.type.kind !== 'FunctionType')
    throw new Error(`Cannot call non function`);
  if (typeEquals(l.type.from, r.type))
    throw new Error(`Type mismatch function ${typeName(l.type)} expected: ${typeName(l.type.from)} got: ${typeName(r.type)}`);
  return l.type.to;
}

function makeDeclareTypeFor(l: TypedExpression, t: Type, ctx: Context, scope: Scope): [Type, TypedIdentifierExpression] {
  const [left] = normalize(l, scope);
  if (left.kind !== "TypedIdentifierExpression")
    throw new Error(`Cannot declare when non left side did not normalize to a identifier`);
  return [t, left];
}

function makeAssignTypeFor(l: TypedExpression, r: TypedExpression, ctx: Context, scope: Scope): [Type, TypedIdentifierExpression] {
  const [left] = normalize(l, scope);
  if (left.kind !== "TypedIdentifierExpression")
    throw new Error(`Cannot assign when non left side did not normalize to a identifier`);
  if (!(left.name in ctx))
    throw new Error(`Cannot assign to undefined variable: ${left.name}`);
  if (typeEquals(left.type, r.type))
    throw new Error(`Cannot assign ${typeName(r.type)} to ${typeName(left.type)}`);
  return [r.type, left];
}

export function typeCheck(e: Expression, ctx: Context, scope: Scope): [TypedExpression, Context, Scope] {
  switch (e.kind) {
    case "LetExpression": {
      const [value, c0, s0] = typeCheck(e.value, ctx, scope);
      const type = value.type;
      return [{
        kind: "TypedLetExpression",
        name: e.name,
        value,
        type,
      }, { ...ctx, [e.name]: type }, { ...scope, [e.name]: value }];
    }
    case "IntegerExpression": {
      const type = relt.type.integer;
      return [{ kind: "TypedIntegerExpression", value: e.value, type }, ctx, scope];
    }
    case "StringExpression": {
      const type = relt.type.string;
      return [{ kind: "TypedStringExpression", value: e.value, type }, ctx, scope];
    }
    case "BooleanExpression": {
      const type = relt.type.boolean;
      return [{ kind: "TypedBooleanExpression", value: e.value, type }, ctx, scope];
    }
    case "FloatExpression": {
      const type = relt.type.float;
      return [{ kind: "TypedFloatExpression", value: e.value, type }, ctx, scope];
    }
    case "NullExpression": {
      const type = relt.type.null;
      return [{ kind: "TypedNullExpression", type }, ctx, scope];
    }
    case "PlaceholderExpression": {
      const type = relt.type.integer;
      return [{ kind: "TypedPlaceholderExpression", name: e.name, type }, ctx, scope];
    }
    case "EvalExpression": {
      const [node] = typeCheck(e.node, ctx, scope);
      const type = node.type;
      return [{ kind: "TypedEvalExpression", node, type }, ctx, scope];
    }
    case "IdentifierExpression": {
      const type = makeIdentifierTypeFor(e.name, ctx, scope);
      return [{ kind: "TypedIdentifierExpression", name: e.name, type }, ctx, scope];
    }
    case "ObjectExpression": {
      const [properties0] = e.properties.reduce<[TypedExpression[], Context, Scope]>(([l, c, s], x) => {
        const y = typeCheck(x, c, s);
        return [[...l, y[0]], y[1], y[2]];
      }, [[], ctx, scope]);
      const [type, properties] = makeObjectTypeFor(properties0, ctx, scope);
      return [{ kind: "TypedObjectExpression", properties, type }, ctx, scope];
    }
    case "ArrayExpression": {
      const values = e.values.map(x => typeCheck(x, ctx, scope)[0]);
      const type = makeArrayTypeFor(values, ctx, scope);
      return [{ kind: "TypedArrayExpression", values, type }, ctx, scope];
    }
    case "TableExpression": {
      const [columns0] = e.columns.reduce<[TypedExpression[], Context, Scope]>(([l, c, s], x) => {
        const y = typeCheck(x, c, s);
        return [[...l, y[0]], y[1], y[2]];
      }, [[], ctx, scope]);
      const [type, columns] = makeTableTypeFor(e.name, columns0, ctx, scope);
      return [{ kind: "TypedTableExpression", name: e.name, columns, type }, ctx, scope];
    }
    case "ApplicationExpression": {
      const [left] = typeCheck(e.left, ctx, scope);
      const [right] = typeCheck(e.right, ctx, scope);
      const type = makeApplicationTypeFor(left, right, ctx, scope);
      return [{ kind: "TypedApplicationExpression", left, right, type }, ctx, scope];
    }
    case "DeclareExpression": {
      const [left0] = typeCheck(e.left, ctx, scope);
      const [type, left] = makeDeclareTypeFor(left0, e.right, ctx, scope);
      return [{ kind: "TypedDeclareExpression", left, type }, { ...ctx, [left.name]: type }, scope];
    }
    case "AssignExpression": {
      const [left0] = typeCheck(e.left, ctx, scope);
      const [right] = typeCheck(e.right, ctx, scope);
      const [type, left] = makeAssignTypeFor(left0, right, ctx, scope);
      return [{ kind: "TypedAssignExpression", left, right, type }, ctx, { ...scope, [left.name]: right }];
    }
  }
}
