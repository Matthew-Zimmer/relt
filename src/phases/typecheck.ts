import { genLoc, locPath } from "../ast/relt/location";
import { AddExpression, AndExpression, ApplicationExpression, ArrayExpression, AssignExpression, BlockExpression, BooleanExpression, CmpExpression, ConditionalExpression, DeclareExpression, DotExpression, DropExpression, EnvExpression, EvalExpression, Expression, FloatExpression, FunctionExpression, GroupByExpression, IdentifierExpression, IntegerExpression, JoinExpression, LetExpression, MulExpression, NullExpression, ObjectExpression, OrExpression, PlaceholderExpression, SelectExpression, SpreadExpression, StringExpression, TableExpression, UnionExpression, WhereExpression, WithExpression } from "../ast/relt/source";
import { ArrayType, FunctionType, ObjectType, TableType, Type } from "../ast/relt/type";
import { typeEquals, typeName, unifyTypes } from "../ast/relt/type/utils";
import { TypedAddExpression, TypedAndExpression, TypedApplicationExpression, TypedArrayExpression, TypedAssignExpression, TypedAssignExpressionValue, TypedBlockExpression, TypedBooleanExpression, TypedCmpExpression, TypedConditionalExpression, TypedDeclareExpression, TypedDotExpression, TypedDropExpression, TypedEnvExpression, TypedExpression, TypedFloatExpression, TypedFunctionExpression, TypedGroupByExpression, TypedIdentifierExpression, TypedIntegerExpression, TypedJoinExpression, TypedLetExpression, TypedMulExpression, TypedNullExpression, TypedObjectExpression, TypedObjectExpressionProperty, TypedOrExpression, TypedSelectExpression, TypedSpreadExpression, TypedStringExpression, TypedTableExpression, TypedUnionExpression, TypedWhereExpression, TypedWithExpression } from "../ast/relt/typed";
import { reportUserError } from "../errors";
import { Location } from '../ast/relt/location';
import { gather, normalize, ofKind } from "../ast/relt/typed/utils";

export type Context = Record<string, Type>;
export type Scope = Record<string, TypedExpression>;

type TypeCheckArgs<E extends Expression = Expression> = [E, Context, Scope];
type TypeCheckReturn<E extends TypedExpression = TypedExpression> = [E, Context, Scope];

export function typeCheck(...[e, ctx, scope]: TypeCheckArgs): TypeCheckReturn {
  switch (e.kind) {
    case "LetExpression": return typeCheckLetExpression(e, ctx, scope);
    case "TableExpression": return typeCheckTableExpression(e, ctx, scope);
    case "FunctionExpression": return typeCheckFunctionExpression(e, ctx, scope);
    case "EvalExpression": return typeCheckEvalExpression(e, ctx, scope);
    case "DeclareExpression": return typeCheckDeclareExpression(e, ctx, scope);
    case "AssignExpression": return typeCheckAssignExpression(e, ctx, scope);
    case "ConditionalExpression": return typeCheckConditionalExpression(e, ctx, scope);
    case "OrExpression": return typeCheckOrExpression(e, ctx, scope);
    case "AndExpression": return typeCheckAndExpression(e, ctx, scope);
    case "CmpExpression": return typeCheckCmpExpression(e, ctx, scope);
    case "AddExpression": return typeCheckAddExpression(e, ctx, scope);
    case "MulExpression": return typeCheckMulExpression(e, ctx, scope);
    case "UnionExpression": return typeCheckUnionExpression(e, ctx, scope);
    case "JoinExpression": return typeCheckJoinExpression(e, ctx, scope);
    case "GroupByExpression": return typeCheckGroupByExpression(e, ctx, scope);
    case "WhereExpression": return typeCheckWhereExpression(e, ctx, scope);
    case "WithExpression": return typeCheckWithExpression(e, ctx, scope);
    case "DropExpression": return typeCheckDropExpression(e, ctx, scope);
    case "SelectExpression": return typeCheckSelectExpression(e, ctx, scope);
    case "DotExpression": return typeCheckDotExpression(e, ctx, scope);
    case "ApplicationExpression": return typeCheckApplicationExpression(e, ctx, scope);
    case "IdentifierExpression": return typeCheckIdentifierExpression(e, ctx, scope);
    case "PlaceholderExpression": return typeCheckPlaceholderExpression(e, ctx, scope);
    case "IntegerExpression": return typeCheckIntegerExpression(e, ctx, scope);
    case "FloatExpression": return typeCheckFloatExpression(e, ctx, scope);
    case "StringExpression": return typeCheckStringExpression(e, ctx, scope);
    case "EnvExpression": return typeCheckEnvExpression(e, ctx, scope);
    case "BooleanExpression": return typeCheckBooleanExpression(e, ctx, scope);
    case "NullExpression": return typeCheckNullExpression(e, ctx, scope);
    case "BlockExpression": return typeCheckBlockExpression(e, ctx, scope);
    case "ObjectExpression": return typeCheckObjectExpression(e, ctx, scope);
    case "ArrayExpression": return typeCheckArrayExpression(e, ctx, scope);
    case "SpreadExpression": return typeCheckSpreadExpression(e, ctx, scope);
  }
}

function lookupApplication(f: Type, a: Type, ctx: Context, scope: Scope): Type {
  if (f.kind !== "FunctionType")
    reportUserError(``);
  const t = unifyTypes(f.from, a)
  if (!(t === undefined || typeEquals(t, f.from)))
    reportUserError(``);
  return f.to;
}

function ctxForObjectType(t: ObjectType): Context {
  return Object.fromEntries(t.properties.map(x => [x.name, x.type]));
}

function ctxForTableType(t: TableType): Context {
  return Object.fromEntries(t.columns.map(x => [x.name, x.type]));
}

function assertIsOfColumnType(t: TypedExpression, loc: Location): asserts t is TypedIdentifierExpression | TypedArrayExpression<TypedIdentifierExpression> {
  if (!(t.kind === "TypedIdentifierExpression" || t.kind === "TypedArrayExpression" && t.values.every(x => x.kind === "TypedIdentifierExpression")))
    reportUserError(`Expected a column expression which is either an array of identifiers or a single identifier\nAt${locPath(loc)}`);
}

function assertValidAssign(l: TypedExpression, loc: Location): asserts l is TypedAssignExpressionValue {
  if (!(
    l.kind === "TypedIdentifierExpression" ||
    l.kind === "TypedArrayExpression" && l.values.forEach(x => assertValidAssign(x, loc)) ||
    l.kind === "TypedObjectExpression" && l.properties.forEach(x => assertValidAssign(x, loc))
  ))
    reportUserError(`Expected an array of identifiers or a single identifier or object of identifiers for assign expression\nAt${locPath(loc)}`);
}

function typeCheckIntegerExpression(...[e, ctx, scope]: TypeCheckArgs<IntegerExpression>): TypeCheckReturn<TypedIntegerExpression> {
  return [{ kind: "TypedIntegerExpression", value: e.value, type: { kind: "IntegerType" } }, ctx, scope];
}

function typeCheckFloatExpression(...[e, ctx, scope]: TypeCheckArgs<FloatExpression>): TypeCheckReturn<TypedFloatExpression> {
  return [{ kind: "TypedFloatExpression", value: e.value, type: { kind: "FloatType" } }, ctx, scope];
}

function typeCheckStringExpression(...[e, ctx, scope]: TypeCheckArgs<StringExpression>): TypeCheckReturn<TypedStringExpression> {
  return [{ kind: "TypedStringExpression", value: e.value, type: { kind: "StringType" } }, ctx, scope];
}

function typeCheckEnvExpression(...[e, ctx, scope]: TypeCheckArgs<EnvExpression>): TypeCheckReturn<TypedEnvExpression> {
  return [{ kind: "TypedEnvExpression", value: e.value, type: { kind: "StringType" } }, ctx, scope];
}

function typeCheckBooleanExpression(...[e, ctx, scope]: TypeCheckArgs<BooleanExpression>): TypeCheckReturn<TypedBooleanExpression> {
  return [{ kind: "TypedBooleanExpression", value: e.value, type: { kind: "BooleanType" } }, ctx, scope];
}

function typeCheckNullExpression(...[e, ctx, scope]: TypeCheckArgs<NullExpression>): TypeCheckReturn<TypedNullExpression> {
  return [{ kind: "TypedNullExpression", type: { kind: "NullType" } }, ctx, scope];
}

function typeCheckIdentifierExpression(...[e, ctx, scope]: TypeCheckArgs<IdentifierExpression>): TypeCheckReturn<TypedIdentifierExpression> {
  if (!(e.name in ctx))
    reportUserError(`${e.name} is not defined\nAt${locPath(e.loc)}`);
  const type = ctx[e.name];
  return [{ kind: "TypedIdentifierExpression", name: e.name, type }, ctx, scope];
}

function typeCheckPlaceholderExpression(...[e, ctx, scope]: TypeCheckArgs<PlaceholderExpression>): TypeCheckReturn<TypedExpression> {
  return [{ kind: "TypedPlaceholderExpression", name: e.name, kindCondition: e.kindCondition, typeCondition: e.typeCondition, type: { kind: "AnyType" } }, ctx, scope];
}

function typeCheckLetExpression(...[e, ctx, scope]: TypeCheckArgs<LetExpression>): TypeCheckReturn<TypedLetExpression> {
  switch (e.value.kind) {
    case "AssignExpression": {
      const [right] = typeCheck(e.value.right, ctx, scope);
      switch (e.value.left.kind) {
        case "IdentifierExpression": {
          return [
            { kind: "TypedLetExpression", value: { kind: "TypedAssignExpression", left: { kind: "TypedIdentifierExpression", name: e.value.left.name, type: right.type }, op: "=", right, type: right.type }, type: right.type },
            { ...ctx, [e.value.left.name]: right.type },
            { ...scope, [e.value.left.name]: right },
          ];
        }
        default:
          reportUserError(`TODO`);
      }
      break;
    }
    case "DeclareExpression":
      if (e.value.value.kind !== "IdentifierExpression")
        reportUserError(``);
      return [{ kind: "TypedLetExpression", value: { kind: "TypedDeclareExpression", value: { kind: "TypedIdentifierExpression", name: e.value.value.name, type: e.value.type }, type: e.value.type }, type: e.value.type }, { ...ctx, [e.value.value.name]: e.value.type }, scope]
    default:
      reportUserError(``);
  }
}

function typeCheckTableExpression(...[e, ctx, scope]: TypeCheckArgs<TableExpression>): TypeCheckReturn<TypedTableExpression> {
  if (e.value.kind !== "AssignExpression")
    reportUserError(``);
  if (e.value.left.kind !== "IdentifierExpression" && e.value.left.kind !== "ArrayExpression" && e.value.left.kind !== "ObjectExpression")
    reportUserError(``);
  if (e.value.left.kind !== "IdentifierExpression")
    reportUserError(``);

  const [right] = typeCheck(e.value.right, ctx, scope);

  if (right.kind !== "TypedObjectExpression")
    reportUserError(``);

  const name = e.value.left.name;
  const type: TableType = { kind: "TableType", name, columns: right.type.properties };

  return [{ kind: "TypedTableExpression", value: { kind: "TypedAssignExpression", left: { kind: "TypedIdentifierExpression", name, type }, op: "=", right, type }, type }, { ...ctx, [name]: type }, scope];
}

function typeCheckFunctionExpression(...[e, ctx, scope]: TypeCheckArgs<FunctionExpression>): TypeCheckReturn<TypedFunctionExpression> {
  const args = e.args.map(x => typeCheck(x, ctx, scope)[0]);
  const names = args.flatMap(x => gather(x, ofKind("TypedIdentifierExpression")));
  const [value] = typeCheck(e.value, { ...ctx, ...Object.fromEntries(names.map(x => [x.name, x.type])) }, scope);

  const t: TypedFunctionExpression = {
    kind: "TypedFunctionExpression",
    name: e.name,
    args,
    type: args.reduce((p, c) => ({ kind: "FunctionType", from: p, to: c.type }), value.type) as FunctionType,
    value,
  };

  return [t, { ...ctx, ...(e.name === undefined ? undefined : { [e.name]: t.type }) }, { ...scope, ...(e.name === undefined ? {} : { [e.name]: t }) }];
}

function typeCheckEvalExpression(...[e, ctx, scope]: TypeCheckArgs<EvalExpression>): TypeCheckReturn<TypedExpression> {
  const [node] = typeCheck(e.node, ctx, scope);
  // TODO think should we be using that scope change from normalize?
  return [normalize(node, scope)[0], ctx, scope];
}

function typeCheckDeclareExpression(...[e, ctx, scope]: TypeCheckArgs<DeclareExpression>): TypeCheckReturn<TypedDeclareExpression> {
  if (e.value.kind !== "IdentifierExpression")
    reportUserError(``);
  return [{ kind: "TypedDeclareExpression", type: e.type, value: { kind: "TypedIdentifierExpression", name: e.value.name, type: e.type } }, ctx, scope];
}

function typeCheckAssignExpression(...[e, ctx, scope]: TypeCheckArgs<AssignExpression>): TypeCheckReturn<TypedAssignExpression> {
  const [left] = typeCheck(e.left, ctx, scope);
  const [right] = typeCheck(e.right, ctx, scope);

  assertValidAssign(left, e.left.loc);

  if (!typeEquals(left.type, right.type))
    reportUserError(``);

  return [{ kind: "TypedAssignExpression", left, op: "=", right, type: left.type }, ctx, scope];
}

function typeCheckConditionalExpression(...[e, ctx, scope]: TypeCheckArgs<ConditionalExpression>): TypeCheckReturn<TypedConditionalExpression> {
  const [left] = typeCheck(e.left, ctx, scope);
  const [op] = typeCheck({ kind: "IdentifierExpression", name: e.op, loc: genLoc }, ctx, scope);
  const [right] = typeCheck(e.right, ctx, scope);
  const type1 = lookupApplication(op.type, left.type, ctx, scope);
  const type = lookupApplication(type1, right.type, ctx, scope);
  return [{ kind: "TypedConditionalExpression", left, right, op: e.op, type }, ctx, scope];
}

function typeCheckOrExpression(...[e, ctx, scope]: TypeCheckArgs<OrExpression>): TypeCheckReturn<TypedOrExpression> {
  const [left] = typeCheck(e.left, ctx, scope);
  const [op] = typeCheck({ kind: "IdentifierExpression", name: e.op, loc: genLoc }, ctx, scope);
  const [right] = typeCheck(e.right, ctx, scope);
  const type1 = lookupApplication(op.type, left.type, ctx, scope);
  const type = lookupApplication(type1, right.type, ctx, scope);
  return [{ kind: "TypedOrExpression", left, right, op: e.op, type }, ctx, scope];
}

function typeCheckAndExpression(...[e, ctx, scope]: TypeCheckArgs<AndExpression>): TypeCheckReturn<TypedAndExpression> {
  const [left] = typeCheck(e.left, ctx, scope);
  const [op] = typeCheck({ kind: "IdentifierExpression", name: e.op, loc: genLoc }, ctx, scope);
  const [right] = typeCheck(e.right, ctx, scope);
  const type1 = lookupApplication(op.type, left.type, ctx, scope);
  const type = lookupApplication(type1, right.type, ctx, scope);
  return [{ kind: "TypedAndExpression", left, right, op: e.op, type }, ctx, scope];
}

function typeCheckCmpExpression(...[e, ctx, scope]: TypeCheckArgs<CmpExpression>): TypeCheckReturn<TypedCmpExpression> {
  const [left] = typeCheck(e.left, ctx, scope);
  const [op] = typeCheck({ kind: "IdentifierExpression", name: e.op, loc: genLoc }, ctx, scope);
  const [right] = typeCheck(e.right, ctx, scope);
  const type1 = lookupApplication(op.type, left.type, ctx, scope);
  const type = lookupApplication(type1, right.type, ctx, scope);

  if (type.kind !== "BooleanType")
    reportUserError(`Logical Comparison did not result in a boolean type\nAt${locPath(e.loc)}`);

  return [{ kind: "TypedCmpExpression", left, right, op: e.op, type }, ctx, scope];
}

function typeCheckAddExpression(...[e, ctx, scope]: TypeCheckArgs<AddExpression>): TypeCheckReturn<TypedAddExpression> {
  const [left] = typeCheck(e.left, ctx, scope);
  const [op] = typeCheck({ kind: "IdentifierExpression", name: e.op, loc: genLoc }, ctx, scope);
  const [right] = typeCheck(e.right, ctx, scope);
  const type1 = lookupApplication(op.type, left.type, ctx, scope);
  const type = lookupApplication(type1, right.type, ctx, scope);
  return [{ kind: "TypedAddExpression", left, right, op: e.op, type }, ctx, scope];
}

function typeCheckMulExpression(...[e, ctx, scope]: TypeCheckArgs<MulExpression>): TypeCheckReturn<TypedMulExpression> {
  const [left] = typeCheck(e.left, ctx, scope);
  const [op] = typeCheck({ kind: "IdentifierExpression", name: e.op, loc: genLoc }, ctx, scope);
  const [right] = typeCheck(e.right, ctx, scope);
  const type1 = lookupApplication(op.type, left.type, ctx, scope);
  const type = lookupApplication(type1, right.type, ctx, scope);
  return [{ kind: "TypedMulExpression", left, right, op: e.op, type }, ctx, scope];
}

function typeCheckUnionExpression(...[e, ctx, scope]: TypeCheckArgs<UnionExpression>): TypeCheckReturn<TypedUnionExpression> {
  const [left] = typeCheck(e.left, ctx, scope);
  const [right] = typeCheck(e.right, ctx, scope);

  if (left.type.kind !== "TableType")
    reportUserError(``);

  if (right.type.kind !== "TableType")
    reportUserError(``);

  const tableName = `Relt_${left.type.name}Union_${right.type.name}`;
  let lCols = Object.fromEntries(left.type.columns.map(x => [x.name, x.type]));
  const columns: TableType['columns'] = [];

  for (const col of right.type.columns) {
    const name = col.name;
    if (col.name in lCols) {
      const type = unifyTypes(col.type, lCols[col.name]);
      if (type === undefined)
        reportUserError(`In union of two tables column "${col.name}" has types which could not be unified`);
      columns.push({ name, type });
      delete lCols[name];
    }
    else {
      columns.push({ name, type: { kind: "OptionalType", of: col.type } });
    }
  }

  for (const [k, v] of Object.entries(lCols))
    columns.push({ name: k, type: { kind: "OptionalType", of: v } });

  const type: TableType = { kind: "TableType", name: tableName, columns };

  return [{ kind: "TypedUnionExpression", left, right, type }, ctx, scope];
}

function typeCheckJoinExpression(...[e, ctx, scope]: TypeCheckArgs<JoinExpression>): TypeCheckReturn<TypedJoinExpression> {
  const [left] = typeCheck(e.left, ctx, scope);
  const [right] = typeCheck(e.right, ctx, scope);

  if (left.type.kind !== "TableType")
    reportUserError(``);

  if (right.type.kind !== "TableType")
    reportUserError(``);

  if (e.on === undefined)
    reportUserError(``);

  const lCtx = ctxForTableType(left.type);
  const rCtx = ctxForTableType(right.type);
  const onCtx: Context = {
    ...lCtx,
    ...rCtx,
    l: left.type,
    r: right.type,
  };

  const [on_] = typeCheck(e.on, onCtx, scope);

  const onColumns = gather(on_, ofKind("TypedIdentifierExpression"));

  const name = `Relt_${left.type.name}_${e.method}_join_${right.type.name}_on_${onColumns.map(x => x.name)}`;

  const type: TableType = { kind: "TableType", name, columns: left.type.columns.concat(right.type.columns) };

  return [{ kind: "TypedJoinExpression", method: e.method, left, right, on: on_, type }, ctx, scope];
}

function assertValidAggExpression(e: TypedExpression, loc: Location): asserts e is TypedAssignExpression<TypedIdentifierExpression> | TypedObjectExpression<TypedAssignExpression<TypedIdentifierExpression>> {
  if (e.kind === "TypedAssignExpression" && e.left.kind === "TypedIdentifierExpression") return;
  if (e.kind === "TypedObjectExpression" && e.properties.every(x => x.kind === "TypedAssignExpression" && x.left.kind === "TypedIdentifierExpression")) return;
  reportUserError(`Is not a valid agg expression\nAt${locPath(loc)}`);
}

function typeCheckGroupByExpression(...[e, ctx, scope]: TypeCheckArgs<GroupByExpression>): TypeCheckReturn<TypedGroupByExpression> {
  const [value] = typeCheck(e.value, ctx, scope);

  if (value.type.kind !== "TableType")
    reportUserError(``);

  const [by] = typeCheck(e.by, ctxForTableType(value.type), scope);

  assertIsOfColumnType(by, e.by.loc);

  const [agg] = typeCheck(e.agg, ctxForTableType(value.type), scope);

  assertValidAggExpression(agg, e.loc);

  const byColumns = by.kind === "TypedIdentifierExpression" ? [by.name] : by.values.map(x => x.name);
  const columns = agg.kind === "TypedAssignExpression" ? [{ name: agg.left.name, type: agg.type }] : agg.properties.map(x => ({ name: x.left.name, type: x.type }));

  const name = `Relt_Group_${value.type.name}_${byColumns.join('_')}_agg_${columns.map(x => x.name).join('_')}`;

  const type: TableType = { kind: "TableType", name, columns };

  return [{ kind: "TypedGroupByExpression", value, by, agg, type }, ctx, scope];
}

function createWithTableType(base: TableType, obj: TypedObjectExpression): TableType {
  const columns = new Map(base.columns.map(x => [x.name, x.type]));

  const addObject = (obj: TypedObjectExpression) => {
    for (const p of obj.properties) {
      switch (p.kind) {
        case "TypedAssignExpression":
          switch (p.left.kind) {
            case "TypedIdentifierExpression":
              addIdentifier(p.left);
              break;
            case "TypedArrayExpression":
              addArray(p.left);
              break;
            case "TypedObjectExpression":
              addObject(p.left);
          }
          break;
        case "TypedDeclareExpression":
        case "TypedIdentifierExpression":
          addIdentifier(p);
          break;
        case "TypedSpreadExpression":
          reportUserError(``);
      }
    }
  }

  const addIdentifier = (obj: TypedIdentifierExpression | TypedDeclareExpression) => {
    const name = obj.kind === "TypedDeclareExpression" ? obj.value.name : obj.name;
    columns.set(name, obj.type);
  }

  const addArray = (obj: TypedArrayExpression<TypedAssignExpressionValue>) => {
    for (const x of obj.values)
      switch (x.kind) {
        case "TypedArrayExpression":
          addArray(x);
          break;
        case "TypedIdentifierExpression":
          addIdentifier(x);
          break;
        case "TypedObjectExpression":
          addObject(x);
          break;
      }
  }

  addObject(obj);

  const columnNames = [...columns.keys()];

  const name = `Relt_${base.name}_with_${columnNames}`;

  return { kind: "TableType", name, columns: [...columns.entries()].map(([k, v]) => ({ name: k, type: v })) };
}

function typeCheckWithExpression(...[e, ctx, scope]: TypeCheckArgs<WithExpression>): TypeCheckReturn<TypedWithExpression> {
  const [left] = typeCheck(e.left, ctx, scope);

  if (left.type.kind !== "TableType")
    reportUserError(``);

  const [right] = typeCheck(e.right, ctxForTableType(left.type), scope);

  if (right.kind !== "TypedObjectExpression")
    reportUserError(``);

  const type: TableType = createWithTableType(left.type, right);

  return [{ kind: "TypedWithExpression", left, right, type }, ctx, scope];
}

function typeCheckWhereExpression(...[e, ctx, scope]: TypeCheckArgs<WhereExpression>): TypeCheckReturn<TypedWhereExpression> {
  const [left] = typeCheck(e.left, ctx, scope);

  if (left.type.kind !== "TableType")
    reportUserError(``);

  const [right] = typeCheck(e.right, ctxForTableType(left.type), scope);

  const type = left.type;

  return [{ kind: "TypedWhereExpression", left, right, type }, ctx, scope];
}

function typeCheckDropExpression(...[e, ctx, scope]: TypeCheckArgs<DropExpression>): TypeCheckReturn<TypedDropExpression> {
  const [left] = typeCheck(e.left, ctx, scope);

  if (left.type.kind !== "TableType")
    reportUserError(``);

  const [right] = typeCheck(e.right, ctxForTableType(left.type), scope);

  assertIsOfColumnType(right, e.right.loc);

  const columns = right.kind === "TypedIdentifierExpression" ? [right.name] : right.values.map(x => x.name);

  const tableName = `Relt_${left.type.name}_drop_${columns.join('_')}`

  const type: TableType = { kind: "TableType", name: tableName, columns: left.type.columns.filter(x => !columns.includes(x.name)) };

  return [{ kind: "TypedDropExpression", left, right, type }, ctx, scope];
}

function typeCheckSelectExpression(...[e, ctx, scope]: TypeCheckArgs<SelectExpression>): TypeCheckReturn<TypedSelectExpression> {
  const [left] = typeCheck(e.left, ctx, scope);

  if (left.type.kind !== "TableType")
    reportUserError(``);

  const [right] = typeCheck(e.right, ctxForTableType(left.type), scope);

  assertIsOfColumnType(right, e.right.loc);

  const columns = right.kind === "TypedIdentifierExpression" ? [right.name] : right.values.map(x => x.name);

  const tableName = `Relt_${left.type.name}_select_${columns.join('_')}`

  const type: TableType = { kind: "TableType", name: tableName, columns: left.type.columns.filter(x => columns.includes(x.name)) };

  return [{ kind: "TypedSelectExpression", left, right, type }, ctx, scope];
}

function typeCheckDotExpression(...[e, ctx, scope]: TypeCheckArgs<DotExpression>): TypeCheckReturn<TypedDotExpression> {
  const [left] = typeCheck(e.left, ctx, scope);
  if (left.type.kind !== 'ObjectType')
    reportUserError(`Cannot . a non object type expression\nAt${locPath(e.loc)}`);
  const [right] = typeCheck(e.right, ctxForObjectType(left.type), scope);
  const type = right.type;
  return [{ kind: "TypedDotExpression", left, right, type }, ctx, scope];
}

function typeCheckApplicationExpression(...[e, ctx, scope]: TypeCheckArgs<ApplicationExpression>): TypeCheckReturn<TypedApplicationExpression> {
  const [left] = typeCheck(e.left, ctx, scope);
  const [right] = typeCheck(e.right, ctx, scope);
  const type = lookupApplication(left.type, right.type, ctx, scope);
  return [{ kind: "TypedApplicationExpression", left, right, type }, ctx, scope];
}

function typeCheckBlockExpression(...[e, ctx, scope]: TypeCheckArgs<BlockExpression>): TypeCheckReturn<TypedBlockExpression> {
  const [expressions] = e.expressions.reduce<[TypedExpression[], Context, Scope]>((p, c) => {
    const n = typeCheck(c, p[1], p[2]);
    return [[...p[0], n[0]], n[1], n[2]];
  }, [[], ctx, scope]);

  const type: Type = expressions.length === 0 ? { kind: "UnitType" } : expressions[expressions.length - 1].type;

  return [{ kind: "TypedBlockExpression", expressions, type }, ctx, scope];
}

function typeCheckObjectExpression(...[e, ctx, scope]: TypeCheckArgs<ObjectExpression>): TypeCheckReturn<TypedObjectExpression> {
  const propertiesT: ObjectType['properties'] = [];
  const [properties] = e.properties.reduce<[TypedObjectExpressionProperty[], Context, Scope]>((p, c) => {
    switch (c.kind) {
      case "AssignExpression":
        if (c.left.kind !== "IdentifierExpression")
          reportUserError(``);
        const x = typeCheck(c.right, p[1], p[2]);
        propertiesT.push({ name: c.left.name, type: x[0].type });
        return [[...p[0], { kind: "TypedAssignExpression", left: { kind: "TypedIdentifierExpression", name: c.left.name, type: x[0].type }, op: "=", right: x[0], type: x[0].type }], { ...x[1], [c.left.name]: x[0].type }, { ...x[2], [c.left.name]: x[0] }];
      case "DeclareExpression":
        if (c.value.kind !== "IdentifierExpression")
          reportUserError(``);
        propertiesT.push({ name: c.value.name, type: c.type });
        return [[...p[0], { kind: "TypedDeclareExpression", value: { kind: "TypedIdentifierExpression", name: c.value.name, type: c.type }, type: c.type }], { ...p[1], [c.value.name]: c.type }, p[2]];
      case "IdentifierExpression":
      case "SpreadExpression":
        reportUserError(`TODO`);
      default:
        reportUserError(``);
    }

  }, [[], ctx, scope]);

  const type: ObjectType = { kind: "ObjectType", properties: propertiesT };
  return [{ kind: "TypedObjectExpression", properties, type }, ctx, scope];
}

function typeCheckArrayExpression(...[e, ctx, scope]: TypeCheckArgs<ArrayExpression>): TypeCheckReturn<TypedArrayExpression> {
  const values = e.values.map(x => typeCheck(x, ctx, scope)[0]);

  const type: Type = values.length === 0 ?
    { kind: "TupleType", types: [] } :
    values.every(x => typeEquals(x.type, values[0].type)) ?
      { kind: "ArrayType", of: values[0].type } :
      { kind: "TupleType", types: values.map(x => x.type) };

  return [{ kind: "TypedArrayExpression", values, type }, ctx, scope];
}

function typeCheckSpreadExpression(...[e, ctx, scope]: TypeCheckArgs<SpreadExpression>): TypeCheckReturn<TypedSpreadExpression> {
  const [value] = typeCheck(e.value, ctx, scope);

  if (value.type.kind !== "ObjectType")
    reportUserError(`Cannot spread a non object type\nAt${locPath(e.loc)}`);

  return [{ kind: "TypedSpreadExpression", value, type: value.type }, ctx, scope];
}
