import { reportInternalError } from "../../errors";
import { IntegerType, StringType, FloatType, BooleanType, TableType, ObjectType, ArrayType, NullType, OptionalType, FunctionType, AnyType, TupleType, NeverType, IdentifierType, UnitType, Type } from "./type";
import { TypedLetExpression, TypedTableExpression, TypedFunctionExpression, TypedEvalExpression, TypedDeclareExpression, TypedAssignExpression, TypedConditionalExpression, TypedOrExpression, TypedAndExpression, TypedCmpExpression, TypedAddExpression, TypedMulExpression, TypedUnionExpression, TypedJoinExpression, TypedGroupByExpression, TypedWhereExpression, TypedWithExpression, TypedDropExpression, TypedSelectExpression, TypedDotExpression, TypedApplicationExpression, TypedIdentifierExpression, TypedPlaceholderExpression, TypedIntegerExpression, TypedFloatExpression, TypedStringExpression, TypedEnvExpression, TypedBooleanExpression, TypedNullExpression, TypedBlockExpression, TypedObjectExpression, TypedArrayExpression, TypedSpreadExpression, TypedIndexExpression, TypedInternalExpression, TypedExpression } from "./typed";

export const relt = {
  type: {
    get int(): IntegerType { return { kind: "IntegerType" }; },
    get string(): StringType { return { kind: "StringType" }; },
    get float(): FloatType { return { kind: "FloatType" }; },
    get bool(): BooleanType { return { kind: "BooleanType" }; },
    get null(): NullType { return { kind: "NullType" }; },
    get any(): AnyType { return { kind: "AnyType" }; },
    get never(): NeverType { return { kind: "NeverType" }; },
    get unit(): UnitType { return { kind: "UnitType" }; },
    id(name: string): IdentifierType { return { kind: "IdentifierType", name }; },
    array(of: Type): ArrayType { return { kind: "ArrayType", of }; },
    optional(of: Type): OptionalType { return { kind: "OptionalType", of }; },
    func(from: Type, to: Type): FunctionType { return { kind: "FunctionType", from, to }; },
    tuple(types: Type[]): TupleType { return { kind: "TupleType", types }; },
    table(name: string, columns: TableType['columns']): TableType { return { kind: "TableType", name, columns }; },
    object(properties: ObjectType['properties']): ObjectType { return { kind: "ObjectType", properties }; },
  },
  typed: {
    id(name: string): TypedIdentifierExpression { return { kind: "TypedIdentifierExpression", name, type: {}, } },
    placeholder(name: string, kindCondition?: string, typeCondition?: string): TypedPlaceholderExpression { return { kind: "TypedPlaceholderExpression", name, kindCondition, typeCondition, type: relt.type.any } },
    int(value: number): TypedIntegerExpression { return { kind: "TypedIntegerExpression", value, type: relt.type.int, } },
    float(value: string): TypedFloatExpression { return { kind: "TypedFloatExpression", value, type: relt.type.float } },
    string(value: string): TypedStringExpression { return { kind: "TypedStringExpression", value, type: relt.type.string } },
    env(value: string): TypedEnvExpression { return { kind: "TypedEnvExpression", value, type: relt.type.string } },
    bool(value: boolean): TypedBooleanExpression { return { kind: "TypedBooleanExpression", value, type: relt.type.bool } },
    get null(): TypedNullExpression { return { kind: "TypedNullExpression", type: relt.type.null } },
    let(value: TypedLetExpression['value']): TypedLetExpression { return { kind: "TypedLetExpression", value, type: value.type } },
    table(value: TypedTableExpression['value'], hooks: TypedTableExpression['hooks']): TypedTableExpression { return { kind: "TypedTableExpression", value, type: relt.type.table(value.left.name, []), hooks } },
    function(name: string, args: TypedFunctionExpression['args'], value: TypedExpression): TypedFunctionExpression { return { kind: "TypedFunctionExpression", args, value, name, type: {} } },
    lambda(args: TypedFunctionExpression['args'], value: TypedExpression): TypedFunctionExpression { return { kind: "TypedFunctionExpression", args, value, type: {} } },
    eval(node: TypedExpression): TypedEvalExpression { return { kind: "TypedEvalExpression", node, type: relt.type.never } },
    declare(value: TypedDeclareExpression['value'], type: Type): TypedDeclareExpression { return { kind: "TypedDeclareExpression", value, type } },
    assign(left: TypedAssignExpression['left'], right: TypedExpression): TypedAssignExpression { return { kind: "TypedAssignExpression", left, op: "=", right, type: {} } },
    conditional(left: TypedExpression, op: TypedConditionalExpression['op'], right: TypedExpression): TypedConditionalExpression { return { kind: "TypedConditionalExpression", left, op, right, type: {} } },
    or(left: TypedExpression, op: TypedOrExpression['op'], right: TypedExpression): TypedOrExpression { return { kind: "TypedOrExpression", left, op, right, type: {} } },
    and(left: TypedExpression, op: TypedAndExpression['op'], right: TypedExpression): TypedAndExpression { return { kind: "TypedAndExpression", left, op, right, type: {} } },
    cmp(left: TypedExpression, op: TypedCmpExpression['op'], right: TypedExpression): TypedCmpExpression { return { kind: "TypedCmpExpression", left, op, right, type: {} } },
    add(left: TypedExpression, op: TypedAddExpression['op'], right: TypedExpression): TypedAddExpression { return { kind: "TypedAddExpression", left, op, right, type: {} } },
    mul(left: TypedExpression, op: TypedMulExpression['op'], right: TypedExpression): TypedMulExpression { return { kind: "TypedMulExpression", left, op, right, type: {} } },
    app(left: TypedExpression, right: TypedExpression): TypedApplicationExpression { return { kind: "TypedApplicationExpression", left, right, type: {} } },
    union(left: TypedExpression, right: TypedExpression): TypedUnionExpression { return { kind: "TypedUnionExpression", left, right, type: {} } },
    join(method: TypedJoinExpression['method'], left: TypedExpression, right: TypedExpression, on: TypedExpression): TypedJoinExpression { return { kind: "TypedJoinExpression", left, right, on, method, type: {} } },
    groupBy(value: TypedExpression, by: TypedGroupByExpression['by'], agg: TypedGroupByExpression['agg']): TypedGroupByExpression { return { kind: "TypedGroupByExpression", value, by, agg, type: {} } },
    where(left: TypedExpression, right: TypedExpression): TypedWhereExpression { return { kind: "TypedWhereExpression", left, right, type: {} } },
    with(left: TypedExpression, right: TypedWithExpression['right']): TypedWithExpression { return { kind: "TypedWithExpression", left, right, type: {} } },
    drop(left: TypedExpression, right: TypedDropExpression['right']): TypedDropExpression { return { kind: "TypedDropExpression", left, right, type: {} } },
    select(left: TypedExpression, right: TypedSelectExpression['right']): TypedSelectExpression { return { kind: "TypedSelectExpression", left, right, type: {} } },
    dot(left: TypedExpression, right: TypedExpression): TypedDotExpression { return { kind: "TypedDotExpression", left, right, type: {} } },
    block(expressions: TypedExpression[]): TypedBlockExpression { return { kind: "TypedBlockExpression", expressions, type: {}, } },
    object(properties: TypedObjectExpression['properties']): TypedObjectExpression { return { kind: "TypedObjectExpression", properties, type: {} } },
    array(values: TypedExpression[]): TypedArrayExpression { return { kind: "TypedArrayExpression", values, type: {} } },
    spread(value: TypedExpression): TypedSpreadExpression { return { kind: "TypedSpreadExpression", value, type: {} } },
    index(left: TypedExpression, index: TypedExpression): TypedIndexExpression { return { kind: "TypedIndexExpression", left, index, type: {} } },
    internal(value: TypedInternalExpression['value'], type: FunctionType): TypedInternalExpression { return { kind: "TypedInternalExpression", value, type } },
  },
}

export function toBuilderStringMany(e: TypedExpression[]): string {
  return `[${e.map(toBuilderString).join(', ')}]`;
}

export function toBuilderString(e: TypedExpression): string {
  switch (e.kind) {
    case "TypedLetExpression": return `relt.typed.let(${toBuilderString(e.value)})`;
    case "TypedTableExpression": return `relt.typed.table(${toBuilderString(e.value)}, ${toBuilderStringMany(e.hooks)})`;
    case "TypedFunctionExpression": return e.name === undefined ? `relt.typed.lambda(${toBuilderStringMany(e.args)}, ${toBuilderString(e.value)})` : `relt.typed.function("${e.name}", ${toBuilderStringMany(e.args)}, ${toBuilderString(e.value)})`;
    case "TypedEvalExpression": return `relt.typed.eval(${toBuilderString(e.node)})`;
    case "TypedDeclareExpression": return `relt.typed.declare(${toBuilderString(e.value)}, ${toBuilderStringType(e.type)})`;
    case "TypedAssignExpression": return `relt.typed.assign(${toBuilderString(e.left)}, ${toBuilderString(e.right)})`;
    case "TypedConditionalExpression": return `relt.typed.conditional(${toBuilderString(e.left)}, "${e.op}", ${toBuilderString(e.right)})`;
    case "TypedOrExpression": return `relt.typed.or(${toBuilderString(e.left)}, "${e.op}", ${toBuilderString(e.right)})`;
    case "TypedAndExpression": return `relt.typed.and(${toBuilderString(e.left)}, "${e.op}", ${toBuilderString(e.right)})`;
    case "TypedCmpExpression": return `relt.typed.cmp(${toBuilderString(e.left)}, "${e.op}", ${toBuilderString(e.right)})`;
    case "TypedAddExpression": return `relt.typed.add(${toBuilderString(e.left)}, "${e.op}", ${toBuilderString(e.right)})`;
    case "TypedMulExpression": return `relt.typed.mul(${toBuilderString(e.left)}, "${e.op}", ${toBuilderString(e.right)})`;
    case "TypedUnionExpression": return `relt.typed.union(${toBuilderString(e.left)}, ${toBuilderString(e.right)})`;
    case "TypedJoinExpression": return `relt.typed.join("${e.method}", ${toBuilderString(e.left)}, ${toBuilderString(e.right)}, ${toBuilderString(e.on)})`;
    case "TypedGroupByExpression": return `relt.typed.groupBy(${toBuilderString(e.value)}, ${toBuilderString(e.by)}, ${toBuilderString(e.agg)})`;
    case "TypedWhereExpression": return `relt.typed.where(${toBuilderString(e.left)}, ${toBuilderString(e.right)})`;
    case "TypedWithExpression": return `relt.typed.with(${toBuilderString(e.left)}, ${toBuilderString(e.right)})`;
    case "TypedDropExpression": return `relt.typed.drop(${toBuilderString(e.left)}, ${toBuilderString(e.right)})`;
    case "TypedSelectExpression": return `relt.typed.select(${toBuilderString(e.left)}, ${toBuilderString(e.right)})`;
    case "TypedDotExpression": return `relt.typed.dot(${toBuilderString(e.left)}, ${toBuilderString(e.right)})`;
    case "TypedApplicationExpression": return `relt.typed.app(${toBuilderString(e.left)}, ${toBuilderString(e.right)})`;
    case "TypedIdentifierExpression": return `relt.typed.id("${e.name}")`;
    case "TypedPlaceholderExpression": return `relt.typed.placeholder("${e.name}", ${e.kindCondition === undefined ? undefined : `${e.kindCondition}`}, ${e.typeCondition === undefined ? undefined : `${e.typeCondition}`})`;
    case "TypedIntegerExpression": return `relt.typed.int(${e.value})`;
    case "TypedFloatExpression": return `relt.typed.float("${e.value}")`;
    case "TypedStringExpression": return `relt.typed.string("${e.value}")`;
    case "TypedEnvExpression": return `relt.typed.env("${e.value}")`;
    case "TypedBooleanExpression": return `relt.typed.bool(${e.value})`;
    case "TypedNullExpression": return `relt.typed.null`;
    case "TypedBlockExpression": return `relt.typed.block(${toBuilderStringMany(e.expressions)})`;
    case "TypedObjectExpression": return `relt.typed.object(${toBuilderStringMany(e.properties)})`;
    case "TypedArrayExpression": return `relt.typed.array(${toBuilderStringMany(e.values)})`;
    case "TypedSpreadExpression": return `relt.typed.spread(${toBuilderString(e.value)})`;
    case "TypedIndexExpression": return `relt.typed.index(${toBuilderString(e.left)}, ${toBuilderString(e.index)})`;
    case "TypedInternalExpression":
      reportInternalError(`Cannot convert `);
  }
}

export function toBuilderStringTypeMany(t: Type[]): string {
  return `[${t.map(toBuilderStringType).join(', ')}]`;
}

export function toBuilderStringType(t: Type): string {
  switch (t.kind) {
    case "IntegerType": return `relt.type.int`;
    case "StringType": return `relt.type.string`;
    case "FloatType": return `relt.type.float`;
    case "BooleanType": return `relt.type.bool`;
    case "NullType": return `relt.type.null`;
    case "AnyType": return `relt.type.any`;
    case "NeverType": return `relt.type.never`;
    case "UnitType": return `relt.type.unit`;
    case "IdentifierType": return `relt.type.id("${t.name}")`;
    case "TableType": return `relt.type.table("${t.name}", [${t.columns.map(x => `{ name: "${x.name}", type: ${toBuilderStringType(x.type)} }`).join(', ')}])`;
    case "ObjectType": return `relt.type.object([${t.properties.map(x => `{ name: "${x.name}", type: ${toBuilderStringType(x.type)} }`).join(', ')}])`;
    case "ArrayType": return `relt.type.array(${toBuilderStringType(t.of)})`;
    case "OptionalType": return `relt.type.optional(${toBuilderStringType(t.of)})`;
    case "FunctionType": return `relt.type.func(${toBuilderStringType(t.from)}, ${toBuilderStringType(t.to)})`;
    case "TupleType": return `relt.type.tuple(${toBuilderStringTypeMany(t.types)})`;
  }
}
