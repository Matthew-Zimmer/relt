import { genLoc, locPath } from "../ast/relt/location";
import { AddExpression, AndExpression, ApplicationExpression, ArrayExpression, AssignExpression, BlockExpression, BooleanExpression, CmpExpression, ConditionalExpression, DeclareExpression, DotExpression, DropExpression, EnvExpression, EvalExpression, Expression, FloatExpression, FunctionExpression, GroupByExpression, IdentifierExpression, IndexExpression, IntegerExpression, JoinExpression, LetExpression, MulExpression, NullExpression, ObjectExpression, OrExpression, PlaceholderExpression, SelectExpression, SpreadExpression, StringExpression, TableExpression, UnionExpression, WhereExpression, WithExpression } from "../ast/relt/source";
import { ArrayType, FunctionType, ObjectType, TableType, Type } from "../ast/relt/type";
import { isAssignable, typeEquals, typeName, unifyTypes } from "../ast/relt/type/utils";
import { TypedAddExpression, TypedAndExpression, TypedApplicationExpression, TypedArrayExpression, TypedAssignExpression, TypedAssignExpressionValue, TypedBlockExpression, TypedBooleanExpression, TypedCmpExpression, TypedConditionalExpression, TypedDeclareExpression, TypedDotExpression, TypedDropExpression, TypedEnvExpression, TypedExpression, TypedFloatExpression, TypedFunctionExpression, TypedGroupByExpression, TypedIdentifierExpression, TypedIndexExpression, TypedIntegerExpression, TypedJoinExpression, TypedLetExpression, TypedMulExpression, TypedNullExpression, TypedObjectExpression, TypedObjectExpressionProperty, TypedOrExpression, TypedSelectExpression, TypedSpreadExpression, TypedStringExpression, TypedTableExpression, TypedUnionExpression, TypedWhereExpression, TypedWithExpression } from "../ast/relt/typed";
import { reportInternalError, reportUserError } from "../errors";
import { Location } from '../ast/relt/location';
import { gather, normalize, ofKind } from "../ast/relt/typed/utils";
import { format } from "../ast/relt/typed/format";
import { TopLevelExpression, TypedTopLevelExpression } from "../ast/relt/topLevel";

export type Context = Record<string, Type>;
export type Scope = Record<string, TypedExpression>;

type TypeCheckArgs<E extends Expression = Expression> = [E, Context, Scope, Context];
type TypeCheckReturn<E extends TypedExpression = TypedExpression> = [E, Context, Scope];


export function typeCheck(ast: TopLevelExpression[], ctx: Context, scope: Scope, tctx: Context): [TypedTopLevelExpression[], Context, Scope, Context] {
  const tast = ast.map<TypedTopLevelExpression>(x => {
    switch (x.kind) {
      case "SugarDirective":
        return x;
      case "TypeIntroductionExpression":
        tctx = { ...tctx, [x.name]: x.type };
        return x;
      case "SugarDefinition": {
        const pattern = shallowTypeCheckExpression(x.pattern);
        const replacement = shallowTypeCheckExpression(x.replacement);
        return { kind: "TypedSugarDefinition", name: x.name, phase: x.phase, pattern, replacement };
      }
      default: {
        const y = typeCheckExpression(x, ctx, scope, tctx);
        ctx = y[1];
        scope = y[2];
        return y[0];
      }
    }
  });
  return [tast, ctx, scope, tctx];
}



export function typeCheckExpression(...[e, ctx, scope, tctx]: TypeCheckArgs): TypeCheckReturn {
  switch (e.kind) {
    case "LetExpression": return typeCheckLetExpression(e, ctx, scope, tctx);
    case "TableExpression": return typeCheckTableExpression(e, ctx, scope, tctx);
    case "FunctionExpression": return typeCheckFunctionExpression(e, ctx, scope, tctx);
    case "EvalExpression": return typeCheckEvalExpression(e, ctx, scope, tctx);
    case "DeclareExpression": return typeCheckDeclareExpression(e, ctx, scope, tctx);
    case "AssignExpression": return typeCheckAssignExpression(e, ctx, scope, tctx);
    case "ConditionalExpression": return typeCheckConditionalExpression(e, ctx, scope, tctx);
    case "OrExpression": return typeCheckOrExpression(e, ctx, scope, tctx);
    case "AndExpression": return typeCheckAndExpression(e, ctx, scope, tctx);
    case "CmpExpression": return typeCheckCmpExpression(e, ctx, scope, tctx);
    case "AddExpression": return typeCheckAddExpression(e, ctx, scope, tctx);
    case "MulExpression": return typeCheckMulExpression(e, ctx, scope, tctx);
    case "UnionExpression": return typeCheckUnionExpression(e, ctx, scope, tctx);
    case "JoinExpression": return typeCheckJoinExpression(e, ctx, scope, tctx);
    case "GroupByExpression": return typeCheckGroupByExpression(e, ctx, scope, tctx);
    case "WhereExpression": return typeCheckWhereExpression(e, ctx, scope, tctx);
    case "WithExpression": return typeCheckWithExpression(e, ctx, scope, tctx);
    case "DropExpression": return typeCheckDropExpression(e, ctx, scope, tctx);
    case "SelectExpression": return typeCheckSelectExpression(e, ctx, scope, tctx);
    case "DotExpression": return typeCheckDotExpression(e, ctx, scope, tctx);
    case "ApplicationExpression": return typeCheckApplicationExpression(e, ctx, scope, tctx);
    case "IdentifierExpression": return typeCheckIdentifierExpression(e, ctx, scope, tctx);
    case "PlaceholderExpression": return typeCheckPlaceholderExpression(e, ctx, scope, tctx);
    case "IntegerExpression": return typeCheckIntegerExpression(e, ctx, scope, tctx);
    case "FloatExpression": return typeCheckFloatExpression(e, ctx, scope, tctx);
    case "StringExpression": return typeCheckStringExpression(e, ctx, scope, tctx);
    case "EnvExpression": return typeCheckEnvExpression(e, ctx, scope, tctx);
    case "BooleanExpression": return typeCheckBooleanExpression(e, ctx, scope, tctx);
    case "NullExpression": return typeCheckNullExpression(e, ctx, scope, tctx);
    case "BlockExpression": return typeCheckBlockExpression(e, ctx, scope, tctx);
    case "ObjectExpression": return typeCheckObjectExpression(e, ctx, scope, tctx);
    case "ArrayExpression": return typeCheckArrayExpression(e, ctx, scope, tctx);
    case "SpreadExpression": return typeCheckSpreadExpression(e, ctx, scope, tctx);
    case "IndexExpression": return typeCheckIndexExpression(e, ctx, scope, tctx);
  }
}

function lookupApplication(f: Type, a: Type, loc: Location, ctx: Context, scope: Scope, tctx: Context): Type {
  if (f.kind !== "FunctionType")
    reportUserError(`Cannot call a non function type ${typeName(f)}\nAt ${locPath(loc)}`);
  const t = unifyTypes(f.from, a, tctx)
  if (!(t === undefined || typeEquals(t, f.from, tctx)))
    reportUserError(`Expected ${typeName(f.from)} got ${typeName(a)}\nAt ${locPath(loc)}`);
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
    reportUserError(`Expected a column expression which is either an array of identifiers or a single identifier\nAt ${locPath(loc)}`);
}

function assertValidAssign(l: TypedExpression, loc: Location): asserts l is TypedAssignExpressionValue {
  if (!(
    l.kind === "TypedIdentifierExpression" ||
    l.kind === "TypedArrayExpression" && l.values.forEach(x => assertValidAssign(x, loc)) ||
    l.kind === "TypedObjectExpression" && l.properties.forEach(x => assertValidAssign(x, loc))
  ))
    reportUserError(`Expected an array of identifiers or a single identifier or object of identifiers for assign expression\nAt ${locPath(loc)}`);
}

function resolveType(t: Type, tctx: Context): Type {
  return t.kind === "IdentifierType" && t.name in tctx ? tctx[t.name] : t;
}

function typeCheckIntegerExpression(...[e, ctx, scope, tctx]: TypeCheckArgs<IntegerExpression>): TypeCheckReturn<TypedIntegerExpression> {
  return [{ kind: "TypedIntegerExpression", value: e.value, type: { kind: "IntegerType" } }, ctx, scope];
}

function typeCheckFloatExpression(...[e, ctx, scope, tctx]: TypeCheckArgs<FloatExpression>): TypeCheckReturn<TypedFloatExpression> {
  return [{ kind: "TypedFloatExpression", value: e.value, type: { kind: "FloatType" } }, ctx, scope];
}

function typeCheckStringExpression(...[e, ctx, scope, tctx]: TypeCheckArgs<StringExpression>): TypeCheckReturn<TypedStringExpression> {
  return [{ kind: "TypedStringExpression", value: e.value, type: { kind: "StringType" } }, ctx, scope];
}

function typeCheckEnvExpression(...[e, ctx, scope, tctx]: TypeCheckArgs<EnvExpression>): TypeCheckReturn<TypedEnvExpression> {
  return [{ kind: "TypedEnvExpression", value: e.value, type: { kind: "StringType" } }, ctx, scope];
}

function typeCheckBooleanExpression(...[e, ctx, scope, tctx]: TypeCheckArgs<BooleanExpression>): TypeCheckReturn<TypedBooleanExpression> {
  return [{ kind: "TypedBooleanExpression", value: e.value, type: { kind: "BooleanType" } }, ctx, scope];
}

function typeCheckNullExpression(...[e, ctx, scope, tctx]: TypeCheckArgs<NullExpression>): TypeCheckReturn<TypedNullExpression> {
  return [{ kind: "TypedNullExpression", type: { kind: "NullType" } }, ctx, scope];
}

function typeCheckIdentifierExpression(...[e, ctx, scope, tctx]: TypeCheckArgs<IdentifierExpression>): TypeCheckReturn<TypedIdentifierExpression> {
  if (!(e.name in ctx)) {
    return [{ kind: "TypedIdentifierExpression", name: e.name, type: { kind: "NeverType" } }, ctx, scope];
  }
  const type = ctx[e.name];
  return [{ kind: "TypedIdentifierExpression", name: e.name, type }, ctx, scope];
}

function typeCheckPlaceholderExpression(...[e, ctx, scope, tctx]: TypeCheckArgs<PlaceholderExpression>): TypeCheckReturn<TypedExpression> {
  return [{ kind: "TypedPlaceholderExpression", name: e.name, kindCondition: e.kindCondition, typeCondition: e.typeCondition, type: { kind: "AnyType" } }, ctx, scope];
}

const compilerNames = ['relt'];

function typeCheckLetExpression(...[e, ctx, scope, tctx]: TypeCheckArgs<LetExpression>): TypeCheckReturn<TypedLetExpression> {
  switch (e.value.kind) {
    case "AssignExpression": {
      const [right] = typeCheckExpression(e.value.right, ctx, scope, tctx);
      switch (e.value.left.kind) {
        case "IdentifierExpression": {
          return [
            { kind: "TypedLetExpression", value: { kind: "TypedAssignExpression", left: { kind: "TypedIdentifierExpression", name: e.value.left.name, type: right.type }, op: "=", right, type: right.type }, type: right.type },
            { ...ctx, [e.value.left.name]: right.type },
            compilerNames.includes(e.value.left.name) ? scope : { ...scope, [e.value.left.name]: right },
          ];
        }
        default:
          reportUserError(`TODO`);
      }
      break;
    }
    case "DeclareExpression":
      if (e.value.value.kind !== "IdentifierExpression")
        reportInternalError(``);
      return [{ kind: "TypedLetExpression", value: { kind: "TypedDeclareExpression", value: { kind: "TypedIdentifierExpression", name: e.value.value.name, type: e.value.type }, type: e.value.type }, type: e.value.type }, { ...ctx, [e.value.value.name]: e.value.type }, scope]
    default:
      reportInternalError(``);
  }
}

const tableHook: FunctionType = {
  kind: "FunctionType",
  from: {
    kind: "IdentifierType",
    name: "ReltTableHook"
  },
  to: {
    kind: "IdentifierType",
    name: "ReltTableHook"
  },
};

function typeCheckTableExpression(...[e, ctx, scope, tctx]: TypeCheckArgs<TableExpression>): TypeCheckReturn<TypedTableExpression> {
  if (e.value.kind !== "AssignExpression")
    reportInternalError(``);
  if (e.value.left.kind !== "IdentifierExpression" && e.value.left.kind !== "ArrayExpression" && e.value.left.kind !== "ObjectExpression")
    reportInternalError(`${e.value.left.kind}`);
  if (e.value.left.kind !== "IdentifierExpression")
    reportInternalError(``);

  let [right] = typeCheckExpression(e.value.right, ctx, scope, tctx);

  const hooks = e.hooks.map(x => {
    const [h] = typeCheckExpression(x, ctx, scope, tctx);
    if (isAssignable(h.type, tableHook, tctx))
      reportUserError(`Invalid table hook got ${typeName(h.type)} expected: ${typeName(tableHook)}\nAt ${locPath(x.loc)}`)
    return h;
  });

  const name = e.value.left.name;
  const type: TableType = (() => {
    if (right.type.kind === "TableType")
      return right.type = { kind: "TableType", name, columns: right.type.columns };
    else if (right.kind === "TypedObjectExpression")
      return { kind: "TableType", name, columns: right.type.properties }
    else
      reportInternalError(`${format(right)}`);
  })();

  return [{ kind: "TypedTableExpression", value: { kind: "TypedAssignExpression", left: { kind: "TypedIdentifierExpression", name, type }, op: "=", right, type }, type, hooks, }, { ...ctx, [name]: type }, scope];
}

interface Introduction {
  name: string;
  type: Type;
}

const nullTExpr: TypedNullExpression = { kind: "TypedNullExpression", type: { kind: "NullType" } };

export function getIntros(e: TypedExpression, loc: Location): Introduction[] {
  switch (e.kind) {
    case "TypedDeclareExpression":
      return [{ name: e.value.name, type: e.type }];
    case "TypedAssignExpression":
      switch (e.left.kind) {
        case "TypedIdentifierExpression":
          return [{ name: e.left.name, type: e.type }];
        case "TypedArrayExpression": {
          const type = e.right.type;
          switch (type.kind) {
            case "ArrayType":
              return e.left.values.flatMap(x => getIntros({ kind: "TypedAssignExpression", left: x, op: "=", right: nullTExpr, type: type.of }, loc))
            case "TupleType": {
              if (e.left.values.length > type.types.length)
                reportUserError(``);
              return e.left.values.flatMap((x, i) => getIntros({ kind: "TypedAssignExpression", left: x, op: "=", right: nullTExpr, type: type.types[i] }, loc));
            }
            default:
              reportUserError(`Cannot extract array introductions when the right side is non array / tuple type\nAt ${locPath(loc)}`);
          }
        }
        case "TypedObjectExpression": {
          const type = e.right.type;
          switch (type.kind) {
            case "ObjectType":
              reportInternalError(`TODO`);
            default:
              reportUserError(`Cannot extract object introductions when the right side is non object type\nAt ${locPath(loc)}`);
          }
        }
      }
    default:
      reportUserError(`Cannot extract introductions from ${e.kind}\nAt${locPath(loc)}`);
  }
}

function typeCheckFunctionExpression(...[e, ctx, scope, tctx]: TypeCheckArgs<FunctionExpression>): TypeCheckReturn<TypedFunctionExpression> {
  const args = e.args.map(x => typeCheckExpression(x, ctx, scope, tctx)[0]);
  const intros = args.flatMap((x, i) => getIntros(x, e.args[i].loc));
  const [value] = typeCheckExpression(e.value, { ...ctx, ...Object.fromEntries(intros.map(x => [x.name, x.type])) }, scope, tctx);

  const t: TypedFunctionExpression = {
    kind: "TypedFunctionExpression",
    name: e.name,
    args,
    type: (args.length === 0 ? [{ type: { kind: "UnitType" } }] as any[] : args).reduceRight<Type>((p, c) => ({ kind: "FunctionType", from: c.type, to: p }), value.type) as FunctionType,
    value,
  };

  return [t, { ...ctx, ...(e.name === undefined ? undefined : { [e.name]: t.type }) }, { ...scope, ...(e.name === undefined ? {} : { [e.name]: t }) }];
}

function typeCheckEvalExpression(...[e, ctx, scope, tctx]: TypeCheckArgs<EvalExpression>): TypeCheckReturn<TypedExpression> {
  const [node] = typeCheckExpression(e.node, ctx, scope, tctx);
  // TODO think should we be using that scope change from normalize?
  return [normalize(node, scope)[0], ctx, scope];
}

function typeCheckDeclareExpression(...[e, ctx, scope, tctx]: TypeCheckArgs<DeclareExpression>): TypeCheckReturn<TypedDeclareExpression> {
  if (e.value.kind !== "IdentifierExpression")
    reportInternalError(``);
  return [{ kind: "TypedDeclareExpression", type: e.type, value: { kind: "TypedIdentifierExpression", name: e.value.name, type: e.type } }, ctx, scope];
}

function typeCheckAssignExpression(...[e, ctx, scope, tctx]: TypeCheckArgs<AssignExpression>): TypeCheckReturn<TypedAssignExpression> {
  const [left] = typeCheckExpression(e.left, ctx, scope, tctx);
  const [right] = typeCheckExpression(e.right, ctx, scope, tctx);

  assertValidAssign(left, e.left.loc);

  if (!typeEquals(left.type, right.type, tctx))
    reportInternalError(``);

  return [{ kind: "TypedAssignExpression", left, op: "=", right, type: left.type }, ctx, scope];
}

function typeCheckConditionalExpression(...[e, ctx, scope, tctx]: TypeCheckArgs<ConditionalExpression>): TypeCheckReturn<TypedConditionalExpression> {
  const [left] = typeCheckExpression(e.left, ctx, scope, tctx);
  const [right] = typeCheckExpression(e.right, ctx, scope, tctx);
  if (left.type.kind !== "OptionalType")
    reportUserError(`Cannot use ${e.op} on a non optional type\nAt ${locPath(e.loc)}`);
  switch (e.op) {
    case "!?":
      return [{ kind: "TypedConditionalExpression", left, right, op: e.op, type: { kind: "OptionalType", of: right.type } }, ctx, scope];
    case "??":
      if (!typeEquals(left.type.of, right.type, tctx))
        reportUserError(`Left and right of ${e.op} need to be the same type: got "${typeName(left.type.of)}" and "${typeName(right.type)}"`);
      return [{ kind: "TypedConditionalExpression", left, right, op: e.op, type: left.type.of }, ctx, scope];
  }
}

function typeCheckOrExpression(...[e, ctx, scope, tctx]: TypeCheckArgs<OrExpression>): TypeCheckReturn<TypedOrExpression> {
  const [left] = typeCheckExpression(e.left, ctx, scope, tctx);
  const [op] = typeCheckExpression({ kind: "IdentifierExpression", name: e.op, loc: genLoc }, ctx, scope, tctx);
  const [right] = typeCheckExpression(e.right, ctx, scope, tctx);
  const type1 = lookupApplication(op.type, left.type, e.loc, ctx, scope, tctx);
  const type = lookupApplication(type1, right.type, e.loc, ctx, scope, tctx);
  return [{ kind: "TypedOrExpression", left, right, op: e.op, type }, ctx, scope];
}

function typeCheckAndExpression(...[e, ctx, scope, tctx]: TypeCheckArgs<AndExpression>): TypeCheckReturn<TypedAndExpression> {
  const [left] = typeCheckExpression(e.left, ctx, scope, tctx);
  const [op] = typeCheckExpression({ kind: "IdentifierExpression", name: e.op, loc: genLoc }, ctx, scope, tctx);
  const [right] = typeCheckExpression(e.right, ctx, scope, tctx);
  const type1 = lookupApplication(op.type, left.type, e.loc, ctx, scope, tctx);
  const type = lookupApplication(type1, right.type, e.loc, ctx, scope, tctx);
  return [{ kind: "TypedAndExpression", left, right, op: e.op, type }, ctx, scope];
}

function typeCheckCmpExpression(...[e, ctx, scope, tctx]: TypeCheckArgs<CmpExpression>): TypeCheckReturn<TypedCmpExpression> {
  const [left] = typeCheckExpression(e.left, ctx, scope, tctx);
  const [op] = typeCheckExpression({ kind: "IdentifierExpression", name: e.op, loc: genLoc }, ctx, scope, tctx);
  const [right] = typeCheckExpression(e.right, ctx, scope, tctx);
  const type1 = lookupApplication(op.type, left.type, e.loc, ctx, scope, tctx);
  const type = lookupApplication(type1, right.type, e.loc, ctx, scope, tctx);

  if (type.kind !== "BooleanType")
    reportUserError(`Logical Comparison did not result in a boolean type\nAt ${locPath(e.loc)}`);

  return [{ kind: "TypedCmpExpression", left, right, op: e.op, type }, ctx, scope];
}

function typeCheckAddExpression(...[e, ctx, scope, tctx]: TypeCheckArgs<AddExpression>): TypeCheckReturn<TypedAddExpression> {
  const [left] = typeCheckExpression(e.left, ctx, scope, tctx);
  const [op] = typeCheckExpression({ kind: "IdentifierExpression", name: e.op, loc: genLoc }, ctx, scope, tctx);
  const [right] = typeCheckExpression(e.right, ctx, scope, tctx);
  const type1 = lookupApplication(op.type, left.type, e.loc, ctx, scope, tctx);
  const type = lookupApplication(type1, right.type, e.loc, ctx, scope, tctx);
  return [{ kind: "TypedAddExpression", left, right, op: e.op, type }, ctx, scope];
}

function typeCheckMulExpression(...[e, ctx, scope, tctx]: TypeCheckArgs<MulExpression>): TypeCheckReturn<TypedMulExpression> {
  const [left] = typeCheckExpression(e.left, ctx, scope, tctx);
  const [op] = typeCheckExpression({ kind: "IdentifierExpression", name: e.op, loc: genLoc }, ctx, scope, tctx);
  const [right] = typeCheckExpression(e.right, ctx, scope, tctx);
  const type1 = lookupApplication(op.type, left.type, e.loc, ctx, scope, tctx);
  const type = lookupApplication(type1, right.type, e.loc, ctx, scope, tctx);
  return [{ kind: "TypedMulExpression", left, right, op: e.op, type }, ctx, scope];
}

function typeCheckUnionExpression(...[e, ctx, scope, tctx]: TypeCheckArgs<UnionExpression>): TypeCheckReturn<TypedUnionExpression> {
  const [left] = typeCheckExpression(e.left, ctx, scope, tctx);
  const [right] = typeCheckExpression(e.right, ctx, scope, tctx);

  if (left.type.kind !== "TableType")
    reportInternalError(``);

  if (right.type.kind !== "TableType")
    reportInternalError(``);

  const tableName = `Relt_${left.type.name}Union_${right.type.name}`;
  let lCols = Object.fromEntries(left.type.columns.map(x => [x.name, x.type]));
  const columns: TableType['columns'] = [];

  for (const col of right.type.columns) {
    const name = col.name;
    if (col.name in lCols) {
      const type = unifyTypes(col.type, lCols[col.name], tctx);
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

function typeCheckJoinExpression(...[e, ctx, scope, tctx]: TypeCheckArgs<JoinExpression>): TypeCheckReturn<TypedJoinExpression> {
  const [left] = typeCheckExpression(e.left, ctx, scope, tctx);
  const [right] = typeCheckExpression(e.right, ctx, scope, tctx);

  if (left.type.kind !== "TableType")
    reportInternalError(``);

  if (right.type.kind !== "TableType")
    reportInternalError(``);

  if (e.on === undefined)
    reportInternalError(``);

  const lCtx = ctxForTableType(left.type);
  const rCtx = ctxForTableType(right.type);
  const onCtx: Context = {
    ...lCtx,
    ...rCtx,
    l: left.type,
    r: right.type,
  };

  const [on_] = typeCheckExpression(e.on, onCtx, scope, tctx);

  const onColumns = gather(on_, ofKind("TypedIdentifierExpression"));

  const name = `Relt_${left.type.name}_${e.method}_join_${right.type.name}_on_${onColumns.map(x => x.name)}`;

  const type: TableType = { kind: "TableType", name, columns: left.type.columns.concat(right.type.columns) };

  return [{ kind: "TypedJoinExpression", method: e.method, left, right, on: on_, type }, ctx, scope];
}

function assertValidAggExpression(e: TypedExpression, loc: Location): asserts e is TypedAssignExpression<TypedIdentifierExpression> | TypedObjectExpression<TypedAssignExpression<TypedIdentifierExpression>> {
  if (e.kind === "TypedAssignExpression" && e.left.kind === "TypedIdentifierExpression") return;
  if (e.kind === "TypedObjectExpression" && e.properties.every(x => x.kind === "TypedAssignExpression" && x.left.kind === "TypedIdentifierExpression")) return;
  reportUserError(`Is not a valid agg expression\nAt ${locPath(loc)}`);
}

function typeCheckGroupByExpression(...[e, ctx, scope, tctx]: TypeCheckArgs<GroupByExpression>): TypeCheckReturn<TypedGroupByExpression> {
  const [value] = typeCheckExpression(e.value, ctx, scope, tctx);

  if (value.type.kind !== "TableType")
    reportInternalError(``);

  const [by] = typeCheckExpression(e.by, ctxForTableType(value.type), scope, tctx);

  assertIsOfColumnType(by, e.by.loc);

  const [agg] = typeCheckExpression(e.agg, ctxForTableType(value.type), scope, tctx);

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
          reportInternalError(``);
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

  const columnNames = [...columns.keys()].join('_');

  const name = `Relt_${base.name}_with_${columnNames}`;

  return { kind: "TableType", name, columns: [...columns.entries()].map(([k, v]) => ({ name: k, type: v })) };
}

function typeCheckWithExpression(...[e, ctx, scope, tctx]: TypeCheckArgs<WithExpression>): TypeCheckReturn<TypedWithExpression> {
  const [left] = typeCheckExpression(e.left, ctx, scope, tctx);

  if (left.type.kind !== "TableType")
    reportInternalError(``);

  const [right] = typeCheckExpression(e.right, ctxForTableType(left.type), scope, tctx);

  if (right.kind !== "TypedObjectExpression")
    reportInternalError(``);

  const type: TableType = createWithTableType(left.type, right);

  return [{ kind: "TypedWithExpression", left, right, type }, ctx, scope];
}

function typeCheckWhereExpression(...[e, ctx, scope, tctx]: TypeCheckArgs<WhereExpression>): TypeCheckReturn<TypedWhereExpression> {
  const [left] = typeCheckExpression(e.left, ctx, scope, tctx);

  if (left.type.kind !== "TableType")
    reportInternalError(``);

  const [right] = typeCheckExpression(e.right, ctxForTableType(left.type), scope, tctx);

  const type = left.type;

  return [{ kind: "TypedWhereExpression", left, right, type }, ctx, scope];
}

function typeCheckDropExpression(...[e, ctx, scope, tctx]: TypeCheckArgs<DropExpression>): TypeCheckReturn<TypedDropExpression> {
  const [left] = typeCheckExpression(e.left, ctx, scope, tctx);

  if (left.type.kind !== "TableType")
    reportInternalError(``);

  const [right] = typeCheckExpression(e.right, ctxForTableType(left.type), scope, tctx);

  assertIsOfColumnType(right, e.right.loc);

  const columns = right.kind === "TypedIdentifierExpression" ? [right.name] : right.values.map(x => x.name);

  const tableName = `Relt_${left.type.name}_drop_${columns.join('_')}`

  const type: TableType = { kind: "TableType", name: tableName, columns: left.type.columns.filter(x => !columns.includes(x.name)) };

  return [{ kind: "TypedDropExpression", left, right, type }, ctx, scope];
}

function typeCheckSelectExpression(...[e, ctx, scope, tctx]: TypeCheckArgs<SelectExpression>): TypeCheckReturn<TypedSelectExpression> {
  const [left] = typeCheckExpression(e.left, ctx, scope, tctx);

  if (left.type.kind !== "TableType")
    reportInternalError(``);

  const [right] = typeCheckExpression(e.right, ctxForTableType(left.type), scope, tctx);

  assertIsOfColumnType(right, e.right.loc);

  const columns = right.kind === "TypedIdentifierExpression" ? [right.name] : right.values.map(x => x.name);

  const tableName = `Relt_${left.type.name}_select_${columns.join('_')}`

  const type: TableType = { kind: "TableType", name: tableName, columns: left.type.columns.filter(x => columns.includes(x.name)) };

  return [{ kind: "TypedSelectExpression", left, right, type }, ctx, scope];
}

function typeCheckDotExpression(...[e, ctx, scope, tctx]: TypeCheckArgs<DotExpression>): TypeCheckReturn<TypedDotExpression> {
  const [left] = typeCheckExpression(e.left, ctx, scope, tctx);
  const resolvedType = resolveType(left.type, tctx);
  if (resolvedType.kind !== 'ObjectType')
    reportUserError(`Cannot . a non object type expression: ${left.type.kind}\nAt ${locPath(e.loc)}`);
  const [right] = typeCheckExpression(e.right, ctxForObjectType(resolvedType), scope, tctx);
  const type = right.type;
  return [{ kind: "TypedDotExpression", left, right, type }, ctx, scope];
}

function typeCheckApplicationExpression(...[e, ctx, scope, tctx]: TypeCheckArgs<ApplicationExpression>): TypeCheckReturn<TypedApplicationExpression> {
  if (e.args.length > 1) {
    return [typeCheckExpression(e.args.reduce((p, c) => ({ kind: "ApplicationExpression", left: p, args: [c], loc: genLoc }), e.left), ctx, scope, tctx)[0] as TypedApplicationExpression, ctx, scope];
  }
  const [left] = typeCheckExpression(e.left, ctx, scope, tctx);
  const [right] = typeCheckExpression(e.args[0] ?? { kind: "BlockExpression", expressions: [], loc: genLoc }, ctx, scope, tctx);
  const type = lookupApplication(left.type, right.type, e.loc, ctx, scope, tctx);
  return [{ kind: "TypedApplicationExpression", left, right, type }, ctx, scope];
}

function typeCheckBlockExpression(...[e, ctx, scope, tctx]: TypeCheckArgs<BlockExpression>): TypeCheckReturn<TypedBlockExpression> {
  const [expressions] = e.expressions.reduce<[TypedExpression[], Context, Scope]>((p, c) => {
    const n = typeCheckExpression(c, p[1], p[2], tctx);
    return [[...p[0], n[0]], n[1], n[2]];
  }, [[], ctx, scope]);

  const type: Type = expressions.length === 0 ? { kind: "UnitType" } : expressions[expressions.length - 1].type;

  return [{ kind: "TypedBlockExpression", expressions, type }, ctx, scope];
}

function typeCheckObjectExpression(...[e, ctx, scope, tctx]: TypeCheckArgs<ObjectExpression>): TypeCheckReturn<TypedObjectExpression> {
  const propertiesT: ObjectType['properties'] = [];
  const [properties] = e.properties.reduce<[TypedObjectExpressionProperty[], Context, Scope]>((p, c) => {
    switch (c.kind) {
      case "AssignExpression":
        if (c.left.kind !== "IdentifierExpression")
          reportInternalError(``);
        const x = typeCheckExpression(c.right, p[1], p[2], tctx);
        propertiesT.push({ name: c.left.name, type: x[0].type });
        return [[...p[0], { kind: "TypedAssignExpression", left: { kind: "TypedIdentifierExpression", name: c.left.name, type: x[0].type }, op: "=", right: x[0], type: x[0].type }], { ...x[1], [c.left.name]: x[0].type }, { ...x[2], [c.left.name]: x[0] }];
      case "DeclareExpression":
        if (c.value.kind !== "IdentifierExpression")
          reportInternalError(``);
        propertiesT.push({ name: c.value.name, type: c.type });
        return [[...p[0], { kind: "TypedDeclareExpression", value: { kind: "TypedIdentifierExpression", name: c.value.name, type: c.type }, type: c.type }], { ...p[1], [c.value.name]: c.type }, p[2]];
      case "IdentifierExpression":
      case "SpreadExpression":
        reportUserError(`TODO`);
      default:
        reportInternalError(`${e.kind}`);
    }

  }, [[], ctx, scope]);

  const type: ObjectType = { kind: "ObjectType", properties: propertiesT };
  return [{ kind: "TypedObjectExpression", properties, type }, ctx, scope];
}

function typeCheckArrayExpression(...[e, ctx, scope, tctx]: TypeCheckArgs<ArrayExpression>): TypeCheckReturn<TypedArrayExpression> {
  const values = e.values.map(x => typeCheckExpression(x, ctx, scope, tctx)[0]);

  const type: Type = values.length === 0 ?
    { kind: "TupleType", types: [] } :
    values.every(x => typeEquals(x.type, values[0].type, tctx)) ?
      { kind: "ArrayType", of: values[0].type } :
      { kind: "TupleType", types: values.map(x => x.type) };

  return [{ kind: "TypedArrayExpression", values, type }, ctx, scope];
}

function typeCheckSpreadExpression(...[e, ctx, scope, tctx]: TypeCheckArgs<SpreadExpression>): TypeCheckReturn<TypedSpreadExpression> {
  const [value] = typeCheckExpression(e.value, ctx, scope, tctx);

  if (value.type.kind !== "ObjectType")
    reportUserError(`Cannot spread a non object type\nAt ${locPath(e.loc)}`);

  return [{ kind: "TypedSpreadExpression", value, type: value.type }, ctx, scope];
}

function typeCheckIndexExpression(...[e, ctx, scope, tctx]: TypeCheckArgs<IndexExpression>): TypeCheckReturn<TypedIndexExpression> {
  const [left] = typeCheckExpression(e.left, ctx, scope, tctx);
  const [index] = typeCheckExpression(e.index, ctx, scope, tctx);

  if (index.type.kind !== "IntegerType")
    reportUserError(`Cannot index with a non integer type: got ${typeName(index.type)}\nAt ${locPath(e.loc)}`);

  switch (left.type.kind) {
    case "ArrayType":
      return [{ kind: "TypedIndexExpression", left, index, type: left.type.of }, ctx, scope];
    case "TupleType":
      if (index.kind !== "TypedIntegerExpression")
        reportUserError(`Cannot index a tuple with a non constant integer\nAt ${locPath(e.loc)}`);
      return [{ kind: "TypedIndexExpression", left, index, type: left.type.types[index.value] }, ctx, scope];
    default:
      reportUserError(`Cannot index a non array / tuple type got: ${typeName(left.type)}\nAt ${locPath(e.loc)}`);
  }
}

export function shallowTypeCheckExpression(e: Expression): TypedExpression {
  switch (e.kind) {
    case "LetExpression":
      return { kind: "TypedLetExpression", type: { kind: "AnyType" }, value: shallowTypeCheckExpression(e.value) as any };
    case "TableExpression":
      return { kind: "TypedTableExpression", type: { kind: "AnyType" } as any, value: shallowTypeCheckExpression(e.value) as any, hooks: e.hooks.map(shallowTypeCheckExpression) };
    case "FunctionExpression":
      return { kind: "TypedFunctionExpression", type: { kind: "AnyType" } as any, name: e.name, args: e.args.map(shallowTypeCheckExpression), value: shallowTypeCheckExpression(e.value) as any };
    case "EvalExpression":
      return { kind: "TypedEvalExpression", type: { kind: "AnyType" } as any, node: shallowTypeCheckExpression(e.node) };
    case "DeclareExpression":
      return { kind: "TypedDeclareExpression", type: e.type, value: shallowTypeCheckExpression(e.value) as any };
    case "AssignExpression":
      return { kind: "TypedAssignExpression", type: { kind: "AnyType" }, op: "=", left: shallowTypeCheckExpression(e.left) as any, right: shallowTypeCheckExpression(e.right) };
    case "ConditionalExpression":
      return { kind: "TypedConditionalExpression", type: { kind: "AnyType" }, op: e.op, left: shallowTypeCheckExpression(e.left), right: shallowTypeCheckExpression(e.right) };
    case "OrExpression":
      return { kind: "TypedOrExpression", type: { kind: "AnyType" }, op: e.op, left: shallowTypeCheckExpression(e.left), right: shallowTypeCheckExpression(e.right) };
    case "AndExpression":
      return { kind: "TypedAndExpression", type: { kind: "AnyType" }, op: e.op, left: shallowTypeCheckExpression(e.left), right: shallowTypeCheckExpression(e.right) };
    case "CmpExpression":
      return { kind: "TypedCmpExpression", type: { kind: "AnyType" } as any, op: e.op, left: shallowTypeCheckExpression(e.left), right: shallowTypeCheckExpression(e.right) };
    case "AddExpression":
      return { kind: "TypedAddExpression", type: { kind: "AnyType" }, op: e.op, left: shallowTypeCheckExpression(e.left), right: shallowTypeCheckExpression(e.right) };
    case "MulExpression":
      return { kind: "TypedMulExpression", type: { kind: "AnyType" }, op: e.op, left: shallowTypeCheckExpression(e.left), right: shallowTypeCheckExpression(e.right) };
    case "UnionExpression":
      return { kind: "TypedUnionExpression", type: { kind: "AnyType" } as any, left: shallowTypeCheckExpression(e.left), right: shallowTypeCheckExpression(e.right) };
    case "JoinExpression":
      return { kind: "TypedJoinExpression", type: { kind: "AnyType" } as any, method: e.method, left: shallowTypeCheckExpression(e.left), right: shallowTypeCheckExpression(e.right), on: e.on ? shallowTypeCheckExpression(e.on) : { kind: "TypedNullExpression", type: { kind: "NullType" } } };
    case "GroupByExpression":
      return { kind: "TypedGroupByExpression", type: { kind: "AnyType" } as any, value: shallowTypeCheckExpression(e.value), by: shallowTypeCheckExpression(e.by) as any, agg: shallowTypeCheckExpression(e.agg) as any };
    case "WhereExpression":
      return { kind: "TypedWhereExpression", type: { kind: "AnyType" } as any, left: shallowTypeCheckExpression(e.left), right: shallowTypeCheckExpression(e.right), };
    case "WithExpression":
      return { kind: "TypedWithExpression", type: { kind: "AnyType" } as any, left: shallowTypeCheckExpression(e.left), right: shallowTypeCheckExpression(e.right) as any, };
    case "DropExpression":
      return { kind: "TypedDropExpression", type: { kind: "AnyType" } as any, left: shallowTypeCheckExpression(e.left), right: shallowTypeCheckExpression(e.right) as any, };
    case "SelectExpression":
      return { kind: "TypedSelectExpression", type: { kind: "AnyType" } as any, left: shallowTypeCheckExpression(e.left), right: shallowTypeCheckExpression(e.right) as any, };
    case "DotExpression":
      return { kind: "TypedDotExpression", type: { kind: "AnyType" }, left: shallowTypeCheckExpression(e.left), right: shallowTypeCheckExpression(e.right) };
    case "ApplicationExpression":
      return e.args.reduce((p, c) => ({ kind: "TypedApplicationExpression", left: p, right: shallowTypeCheckExpression(c), type: { kind: "AnyType" }, }), shallowTypeCheckExpression(e.left));
    case "IdentifierExpression":
      return { kind: "TypedIdentifierExpression", type: { kind: "AnyType" }, name: e.name };
    case "PlaceholderExpression":
      return { ...e, kind: "TypedPlaceholderExpression", type: { kind: "AnyType" } };
    case "IntegerExpression":
      return { kind: "TypedIntegerExpression", type: { kind: "AnyType" } as any, value: e.value };
    case "FloatExpression":
      return { kind: "TypedFloatExpression", type: { kind: "AnyType" } as any, value: e.value };
    case "StringExpression":
      return { kind: "TypedStringExpression", type: { kind: "AnyType" } as any, value: e.value };
    case "EnvExpression":
      return { kind: "TypedEnvExpression", type: { kind: "AnyType" } as any, value: e.value };
    case "BooleanExpression":
      return { kind: "TypedBooleanExpression", type: { kind: "AnyType" } as any, value: e.value };
    case "NullExpression":
      return { kind: "TypedNullExpression", type: { kind: "AnyType" } as any, };
    case "BlockExpression":
      return { kind: "TypedBlockExpression", type: { kind: "AnyType" }, expressions: e.expressions.map(shallowTypeCheckExpression) };
    case "ObjectExpression":
      return { kind: "TypedObjectExpression", type: { kind: "AnyType" } as any, properties: e.properties.map(shallowTypeCheckExpression) as any[], };
    case "ArrayExpression":
      return { kind: "TypedArrayExpression", type: { kind: "AnyType" } as any, values: e.values.map(shallowTypeCheckExpression) };
    case "SpreadExpression":
      return { kind: "TypedSpreadExpression", type: { kind: "AnyType" } as any, value: shallowTypeCheckExpression(e.value) };
    case "IndexExpression":
      return { kind: "TypedIndexExpression", type: { kind: "AnyType" } as any, left: shallowTypeCheckExpression(e.left), index: shallowTypeCheckExpression(e.index) };
  }
}