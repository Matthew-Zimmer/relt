import { TypedExpression, TypedIdentifierExpression } from "../asts/expression/typed";
import { DatasetId, ScalaCaseClass, ScalaType, SparkAggregation, SparkDatasetHandler, SparkDependencyVertex, SparkMapRule, SparkProject } from "../asts/scala";
import { arrayType, functionType, optionalType, structType, StructType, Type, unitType } from "../asts/type";
import { TypedAggProperty, TypedGroupByTypeExpression, TypedJoinTypeExpression, TypedRuleProperty, TypedRuleTypeProperty, TypedRuleValueProperty, TypedStructLikeTypeExpression, TypedTypeExpression, TypedTypeIntroExpression, TypedWithTypeExpression } from "../asts/typeExpression/typed";
import { RuleValueProperty } from "../asts/typeExpression/untyped";
import { DependencyGraph, namedTypeDependencyGraph } from "../graph";
import { ReltProject } from "../project";
import { print, throws } from "../utils";
import { visitTypedTypeExpression } from "../vistors/typeExpression";
import { True } from "../vistors/utils";
import { evaluate, Scope } from "./evaluate";
import { generateScalaType } from "./scalaToLines";
import { hasOverload } from "./typeCheck/expression";
import { groupByType, joinRelation, joinType, propertyLookup, withType } from "./typeCheck/typeExpression";
import { Context, typeEquals } from "./typeCheck/utils";



export function deriveSparkVertices(dg: DependencyGraph): SparkDependencyVertex[] {
  return dg.vertices.map<SparkDependencyVertex>(x => ({
    kind: "SparkDependencyVertex",
    id: x.id,
    name: x.value,
    incoming: dg.parents(x.id),
    outgoing: dg.children(x.id),
  }));
}

// export function deriveSparkProject(
//   retlConfig: Required<ReltProject>,
//   intros: TypedTypeExpression[],
//   expressionContext: Context,
//   scope: Scope,
//   dg: DependencyGraph,
// ): SparkProject {

//   const deriveConnectionInfo = (t: TypedTypeIntroExpression) => {
//     const [value] = evaluate({
//       kind: "TypedApplicationExpression",
//       func: {
//         kind: "TypedIdentifierExpression",
//         name: 'source',
//         type: { kind: "FunctionType", from: [identifierType(t.name)], to: unitType() }
//       },
//       args: [{ kind: "TypedIdentifierExpression", name: "__0", type: identifierType(t.name) }],
//       type: unitType(),
//     }, { ...scope, __0: undefined });

//     if (!(typeof value === 'object' && "kind" in value && typeof value.kind === 'string' && ["db"].includes(value.kind)))
//       throws(`function "source" for "${t.name}" did not return valid data`);
//     const kind = value.kind as "db";
//     // TODO use a library like zod to actually check these values
//     switch (kind) {
//       case "db":
//         return {
//           kind: "db",
//           host: value.host as string,
//           port: value.port as number,
//           user: value.user as string,
//           password: value.password as string,
//           table: value.table as string,
//           columns: [],
//         };
//     }
//   }

//   let stack: [string, ObjectType][] = [];

//   const scalaType = (t: Type): string => {
//     if (t.kind === 'ObjectType') {
//       const result = stack.find(x => typeEquals(t, x[1]));
//       if (result !== undefined) return result[0];
//     }
//     return generateScalaType(convertTypeToScalaType(t));
//   }

//   const deriveScalaCaseClasses = (intros: TypedTypeIntroExpression<ObjectType>[]): ScalaCaseClass[] => {
//     const pushStack = (ty: ObjectType): IdentifierType => {
//       const result = stack.find(x => typeEquals(ty, x[1]));
//       if (result === undefined) {
//         const name = `ImplicitCaseClass${stack.length}`;
//         stack.push([name, ty]);
//         return identifierType(name);
//       }
//       else {
//         return identifierType(result[0]);
//       }
//     }

//     const stackSubstitute = (oldTy: IdentifierType, newTy: IdentifierType): [string, ObjectType][] => {
//       return stack.map(x => [
//         x[0] === oldTy.name ? newTy.name : x[0],
//         objectType(...x[1].properties.map(p => ({
//           name: p.name,
//           type: p.type.kind === 'IdentifierType' && p.type.name === oldTy.name ? newTy : p.type,
//         })))
//       ]);
//     }

//     const imp = (t: Type): PrimitiveType => {
//       switch (t.kind) {
//         case "IntegerType":
//         case "FloatType":
//         case "BooleanType":
//         case "StringType":
//         case "IdentifierType":
//         case "UnitType":
//         case "PrimaryKeyType":
//         case "ForeignKeyType":
//           return t;
//         case "TypeType":
//         case "FunctionType":
//         case "UnionType":
//           throws(`Cannot flatten ${t.kind}`);
//         case "ArrayType":
//         case "OptionalType":
//           return { ...t, of: imp(t.of) };
//         case "ObjectType":
//           return pushStack(objectType(...t.properties.map(x => ({ name: x.name, type: imp(x.type) }))));
//       }
//     }

//     intros.forEach(e => {
//       const type = imp(e.type) as IdentifierType;
//       stack = stackSubstitute(type, identifierType(e.name));
//     });

//     return stack.map<ScalaCaseClass>(x => ({
//       kind: "ScalaCaseClass",
//       name: x[0],
//       properties: x[1].properties.map(x => ({ name: x.name, type: convertTypeToScalaType(x.type) })),
//     }));
//   }

//   const indices = new Map(intros.map((x, i) => [x.name, i]));

//   const datasetIdFor = (name: string): DatasetId => {
//     const idx = indices.get(name);
//     if (idx === undefined) throws(`Not a valid dataset handler type ${name}`);
//     return { name, idx };
//   };

//   const isSourceType = (t: TypedTypeIntroExpression): boolean => {
//     return hasOverload('source', [identifierType(t.name)], expressionContext);
//   }

//   const deriveAggregations = (e: NamedTypedExpression[]): SparkAggregation[] => {
//     const aggs: SparkAggregation[] = [];

//     const imp = (e: TypedExpression) => {
//       switch (e.kind) {
//         case "TypedIntegerExpression":
//         case "TypedBooleanExpression":
//         case "TypedFloatExpression":
//         case "TypedStringExpression":
//         case "TypedIdentifierExpression":
//         case "TypedLetExpression":
//         case "TypedObjectExpression":
//         case "TypedFunctionExpression":
//         case "TypedBlockExpression":
//         case "TypedAddExpression":
//         case "TypedDefaultExpression":
//         case "TypedArrayExpression":
//           throws(`Expression ${e.kind} cannot be converted to map rules as of now`);
//         case "TypedApplicationExpression": {
//           if (e.func.kind !== 'TypedIdentifierExpression')
//             throws(`Cannot convert application with non identifier func`);
//           switch (e.func.name) {
//             case "collect":
//               aggs.push({
//                 kind: "SparkCollectListAggregation",
//                 name: "",
//                 columns: (e.args[0].type as ObjectType).properties.map(x => x.name),
//               });
//               break;
//             case "sum":
//             case "count":
//             case "max":
//             case "min":
//               aggs.push({
//                 kind: "SparkSqlAggregation",
//                 name: "",
//                 func: e.func.name,
//                 column: (e.args[0] as TypedIdentifierExpression).name,
//               });
//               break;
//             default:
//               throws(`Error: Non aggregation function ${e.func.name}`);
//           }
//           break;
//         }
//       }
//     }

//     e.forEach(x => {
//       imp(x.value);
//       if (aggs.length > 0) {
//         aggs[aggs.length - 1].name = x.name;
//       }
//     });

//     return aggs;
//   }

//   const deriveMappingRules = (e: NamedTypedExpression[], seen: Map<string, number>): [SparkMapRule[], Map<string, number>] => {
//     let c = 0;
//     const rules: SparkMapRule[] = [];

//     const imp = (e: TypedExpression): string => {
//       switch (e.kind) {
//         case "TypedIntegerExpression":
//         case "TypedBooleanExpression":
//           return `${e.value}`;
//         case "TypedFloatExpression":
//           return `${e.value.toFixed(20)}`;
//         case "TypedStringExpression":
//           return `"${e.value}"`;
//         case "TypedIdentifierExpression":
//           return seen.has(e.name) ? `${e.name}${seen.get(e.name)}` : e.name;
//         case "TypedLetExpression":
//         case "TypedObjectExpression":
//         case "TypedFunctionExpression":
//         case "TypedBlockExpression":
//           throws(`Expression ${e.kind} cannot be converted to map rules as of now`);
//         case "TypedApplicationExpression": {
//           const func = imp(e.func);
//           const args = e.args.map(imp);
//           const name = `_v${c++}`;
//           rules.push({ kind: "SparkApplicationRule", name, args, func });
//           return name;
//         }
//         case "TypedAddExpression": {
//           const left = imp(e.left);
//           const right = imp(e.right);
//           const name = `_v${c++}`;
//           rules.push({ kind: "SparkBinaryOperationRule", name, left, op: e.op, right });
//           return name;
//         }
//         case "TypedDefaultExpression": {
//           const left = imp(e.left);
//           const right = imp(e.right);
//           const name = `_v${c++}`;
//           rules.push({ kind: "SparkGetOrElseRule", name, left, right });
//           return name;
//         }
//         case "TypedArrayExpression": {
//           const args = e.values.map(imp);
//           const name = `_v${c++}`;
//           rules.push({ kind: "SparkApplicationRule", name, args, func: `Array[${scalaType(e.type.of)}]` });
//           return name;
//         }
//       }
//     }

//     e.forEach(x => {
//       imp(x.value);
//       if (rules.length > 0) {
//         const c = (seen.get(x.name) ?? -1) + 1;
//         rules[rules.length - 1].name = `${x.name}${c}`;
//         seen.set(x.name, c);
//       }
//     });

//     return [rules, seen];
//   }

//   const deriveDatasetHandler = (e: TypedTypeIntroExpression<ObjectType>): SparkDatasetHandler[] => {
//     const v = e.value;
//     switch (v.kind) {
//       case "TypedIntegerTypeExpression":
//       case "TypedFloatTypeExpression":
//       case "TypedBooleanTypeExpression":
//       case "TypedStringTypeExpression":
//       case "TypedIdentifierTypeExpression":
//       case "TypedTypeIntroExpression":
//       case "TypedPrimaryKeyTypeExpression":
//       case "TypedForeignKeyTypeExpression":
//       case "TypedArrayTypeExpression":
//         return [];
//       case "TypedObjectTypeExpression": {
//         if (!isSourceType(e)) return [];

//         const conn = deriveConnectionInfo(e);

//         if (conn.kind === 'db') {
//           return [{
//             kind: "SparkDBSourceDatasetHandler",
//             output: datasetIdFor(e.name),
//             host: conn.host,
//             port: conn.port,
//             user: conn.user,
//             password: conn.password,
//             table: conn.table,
//             columns: conn.columns,
//           }];
//         }
//         else {
//           throws(`Error unknown connection kind: ${conn.kind}`);
//         }
//       }
//       case "TypedJoinTypeExpression":
//         return [{
//           kind: "SparkJoinDatasetHandler",
//           leftInput: datasetIdFor(v.left.name),
//           rightInput: datasetIdFor(v.right.name),
//           output: datasetIdFor(e.name),
//           leftColumn: v.leftColumn,
//           rightColumn: v.rightColumn,
//           method: v.method,
//         }];
//       case "TypedDropTypeExpression":
//         return [{
//           kind: "SparkDropDatasetHandler",
//           input: datasetIdFor(v.left.name),
//           output: datasetIdFor(e.name),
//           properties: v.properties,
//         }];
//       case "TypedWithTypeExpression":
//         const [rules, seen] = deriveMappingRules(v.rules, new Map(v.left.type.properties.map(x => [x.name, 0])));
//         return [{
//           kind: "SparkMapDatasetHandler",
//           input: datasetIdFor(v.left.name),
//           output: datasetIdFor(e.name),
//           rules: [
//             ...v.left.type.properties.map<SparkMapRule>(p => ({ kind: "SparkRowExtractRule", name: `${p.name}0`, property: p.name })),
//             ...rules,
//             { kind: "SparkIdentityRule", name: `${e.name}(${e.type.properties.map(p => `${p.name}${seen.get(p.name) ?? 0}`).join(', ')})` }
//           ],
//         }];
//       case "TypedUnionTypeExpression":
//         return [{
//           kind: "SparkUnionDatasetHandler",
//           leftInput: datasetIdFor(v.left.name),
//           rightInput: datasetIdFor(v.right.name),
//           output: datasetIdFor(e.name),
//         }];
//       case "TypedGroupByTypeExpression":
//         return [{
//           kind: "SparkGroupDatasetHandler",
//           input: datasetIdFor(v.left.name),
//           output: datasetIdFor(e.name),
//           column: v.column,
//           aggregations: [
//             ...deriveAggregations(v.aggregations)
//           ],
//         }];
//     }
//   };

//   const caseClasses = deriveScalaCaseClasses(intros);
//   const datasetHandlers = intros.flatMap(deriveDatasetHandler);

//   return {
//     kind: "SparkProject",
//     name: retlConfig.name,
//     package: retlConfig.package,
//     caseClasses,
//     datasetHandlers,
//     vertices: deriveSparkVertices(dg),
//   };
// }

function desugarImplicitTypeRules(e: TypedWithTypeExpression): TypedTypeExpression {
  const typeRules: TypedRuleTypeProperty[] = [];
  const dependentValueRules: Map<number, TypedRuleValueProperty[]> = new Map();
  const independentValueRules: TypedRuleValueProperty[] = [];

  const isDependent = (r: TypedRuleValueProperty): number | undefined => {
    return undefined;
  };

  e.rules.forEach(r => {
    switch (r.kind) {
      case "TypedRuleTypeProperty":
        typeRules.push(r);
        break;
      case "TypedRuleValueProperty": {
        const result = isDependent(r);
        result === undefined ? independentValueRules.push(r) : dependentValueRules.set(result, [...dependentValueRules.get(result) ?? [], r]);
      }
    }
  });

  let expr: TypedStructLikeTypeExpression = independentValueRules.length === 0 ? e.left : {
    kind: "TypedWithTypeExpression",
    left: e.left,
    rules: independentValueRules,
    type: e.type,
  };

  for (const [i, r] of typeRules.entries()) {
    // TODO handle these!!!!
    const dr = dependentValueRules.get(i) ?? [];
    const v = r.value;
    switch (v.kind) {
      // these will (probably) not have any desugar rules
      case "TypedPrimaryKeyTypeExpression":
      case "TypedForeignKeyTypeExpression":
      case "TypedObjectTypeExpression":
        break;

      // These can add optional implicit meta data
      // none rn
      case "TypedIntegerTypeExpression":
      case "TypedFloatTypeExpression":
      case "TypedBooleanTypeExpression":
      case "TypedStringTypeExpression":
        break;

      // This is an implicit
      // join -> map
      case "TypedIdentifierTypeExpression":
        // TODO implement this rule

        // expr = {
        //   kind: "JoinTypeExpression",
        //   left: expr,
        //   method: "inner",
        //   right: {
        //     kind: "WithTypeExpression",
        //     left: v,
        //     rules: [{ kind: "RuleValueProperty", name: r.name, value: { kind: "", func: { kind: "IdentifierExpression", name: v.name }, args: [] } }]
        //   }
        // };
        break;

      // This is an implicit
      // group -> left join -> map
      case "TypedArrayTypeExpression":
        switch (v.of.kind) {
          case "TypedIdentifierTypeExpression": {

            const ty = v.of.type as StructType;
            const aty = arrayType(ty);

            const [leftColumn, rightColumn] = joinRelation(expr.type, undefined, ty, undefined);

            const aggregations: TypedAggProperty[] = [{
              kind: "TypedAggProperty",
              name: r.name,
              value: {
                kind: "TypedApplicationExpression",
                func: { kind: "TypedIdentifierExpression", name: "collect", type: functionType([ty], aty) },
                args: [{ kind: "TypedIdentifierExpression", name: "this", type: ty }],
                type: aty,
              }
            }];

            const group: TypedGroupByTypeExpression = {
              kind: "TypedGroupByTypeExpression",
              left: v.of as TypedStructLikeTypeExpression,
              column: rightColumn,
              aggregations,
              type: groupByType(ty, rightColumn, aggregations)
            };

            const join: TypedJoinTypeExpression = {
              kind: "TypedJoinTypeExpression",
              left: expr,
              method: "left",
              right: group,
              leftColumn,
              rightColumn,
              type: joinType("left", expr.type, leftColumn, group.type, rightColumn),
            };

            const rules: TypedRuleProperty[] = [{
              kind: "TypedRuleValueProperty",
              name: r.name,
              value: {
                kind: "TypedDefaultExpression",
                left: { kind: "TypedIdentifierExpression", name: r.name, type: optionalType(arrayType(v.of.type)) },
                op: "??",
                right: { kind: "TypedArrayExpression", values: [], type: arrayType(v.of.type) },
                type: arrayType(v.of.type),
              }
            }];

            const _with: TypedWithTypeExpression = {
              kind: "TypedWithTypeExpression",
              left: join,
              rules,
              type: withType(join.type, rules),
            };

            return _with;
          }
        }
        break;

      // TODO 
      // desugar nested expressions
      case "TypedDropTypeExpression":
      case "TypedGroupByTypeExpression":
      case "TypedJoinTypeExpression":
      case "TypedTypeIntroExpression":
      case "TypedUnionTypeExpression":
      case "TypedWithTypeExpression":
        throws(`TODO desugar nested type expressions`);
    }
  }

  return expr;
}

function desugar(expressions: TypedTypeExpression[]): TypedTypeExpression[] {
  const walk = (e: TypedTypeExpression): TypedTypeExpression => {
    switch (e.kind) {
      case "TypedIntegerTypeExpression":
      case "TypedFloatTypeExpression":
      case "TypedBooleanTypeExpression":
      case "TypedStringTypeExpression":
      case "TypedIdentifierTypeExpression":
      case "TypedForeignKeyTypeExpression":
      case "TypedPrimaryKeyTypeExpression":
        return e;

      case "TypedTypeIntroExpression": {
        const value = walk(e.value);
        return { ...e, value };
      }
      case "TypedObjectTypeExpression": {
        const properties = e.properties.map(x => ({ ...x, value: walk(x.value) }));
        return { ...e, properties };
      }
      case "TypedArrayTypeExpression": {
        const of = walk(e.of);
        return { ...e, of };
      }
      case "TypedJoinTypeExpression": {
        const left = walk(e.left) as TypedStructLikeTypeExpression;
        const right = walk(e.right) as TypedStructLikeTypeExpression;
        return { ...e, left, right };
      }
      case "TypedDropTypeExpression": {
        const left = walk(e.left) as TypedStructLikeTypeExpression;
        return { ...e, left };
      }
      case "TypedUnionTypeExpression": {
        const left = walk(e.left) as TypedStructLikeTypeExpression;
        const right = walk(e.right) as TypedStructLikeTypeExpression;
        return { ...e, left, right };
      }
      case "TypedGroupByTypeExpression": {
        const left = walk(e.left) as TypedStructLikeTypeExpression;
        return { ...e, left };
      }

      case "TypedWithTypeExpression": {
        const left = walk(e.left) as TypedStructLikeTypeExpression;
        return desugarImplicitTypeRules({ ...e, left });
      }
    }
  }

  return expressions.map(walk);
}

export function deriveSparkProject(
  retlConfig: Required<ReltProject>,
  expressions: TypedTypeExpression[],
  expressionContext: Context,
  scope: Scope,
): SparkProject {
  const structTypes = new Map<string, StructType>();
  const indexes = new Map<string, number>();
  const datasetHandlers: SparkDatasetHandler[] = [];

  const addStructType = (t: StructType) => {
    structTypes.set(t.name, t);
  };

  const convertTypeToScalaType = (t: Type): ScalaType => {
    switch (t.kind) {
      case 'BooleanType':
        return { kind: "ScalaBooleanType" };
      case 'FloatType':
        return { kind: "ScalaDoubleType" };
      case 'StringType':
        return { kind: "ScalaStringType" };
      case 'IntegerType':
        return { kind: "ScalaIntType" };
      case 'ForeignKeyType': {
        const ty = structTypes.get(t.table);
        if (ty === undefined)
          throws(`Error: cannot convert Foreign key type to scala since ${t.table} does not exist`);
        const cty = propertyLookup(ty, t.column);
        if (cty === undefined)
          throws(`Error: cannot convert Foreign key type to scala since ${t.column} does not exist on ${t.table}`);
        return convertTypeToScalaType(cty);
      }
      case 'PrimaryKeyType':
        return convertTypeToScalaType(t.of);
      case 'ArrayType':
        return { kind: "ScalaArrayType", of: convertTypeToScalaType(t.of) };
      case 'OptionalType':
        return { kind: "ScalaOptionalType", of: convertTypeToScalaType(t.of) };
      case 'FunctionType':
        throws(`Cannot convert a function type to scala`);
      case 'UnitType':
        return { kind: "ScalaUnitType" };
      case 'UnionType':
        throws(`Cannot convert union type to scala`);
      case "StructType":
        return { kind: "ScalaIdentifierType", name: t.name };
    }
  }

  const scalaCaseClass = (t: StructType): ScalaCaseClass => {
    return {
      kind: "ScalaCaseClass",
      name: t.name,
      properties: t.properties.map(({ name, type }) => ({ name, type: convertTypeToScalaType(type) })),
    };
  }

  const datasetIdFor = (t: StructType): DatasetId => {
    if (!indexes.has(t.name))
      indexes.set(t.name, indexes.size);
    return { name: t.name, idx: indexes.get(t.name)! };
  }

  const isSourceType = (t: StructType): boolean => {
    return hasOverload('source', [t], expressionContext);
  }

  const deriveConnectionInfo = (t: StructType) => {
    const [value] = evaluate({
      kind: "TypedApplicationExpression",
      func: {
        kind: "TypedIdentifierExpression",
        name: 'source',
        type: functionType([t], unitType())
      },
      args: [{ kind: "TypedIdentifierExpression", name: "__0", type: t }],
      type: unitType(),
    }, { ...scope, __0: undefined });

    if (!(typeof value === 'object' && "kind" in value && typeof value.kind === 'string' && ["db"].includes(value.kind)))
      throws(`function "source" for "${t.name}" did not return valid data`);
    const kind = value.kind as "db";
    // TODO use a library like zod to actually check these values
    switch (kind) {
      case "db":
        return {
          kind: "db",
          host: value.host as string,
          port: value.port as number,
          user: value.user as string,
          password: value.password as string,
          table: value.table as string,
          columns: [],
        };
    }
  }

  const deriveAggregations = (e: TypedAggProperty[]): SparkAggregation[] => {
    const aggs: SparkAggregation[] = [];

    const imp = (e: TypedExpression) => {
      switch (e.kind) {
        case "TypedIntegerExpression":
        case "TypedBooleanExpression":
        case "TypedFloatExpression":
        case "TypedStringExpression":
        case "TypedIdentifierExpression":
        case "TypedLetExpression":
        case "TypedObjectExpression":
        case "TypedFunctionExpression":
        case "TypedBlockExpression":
        case "TypedAddExpression":
        case "TypedDefaultExpression":
        case "TypedArrayExpression":
          throws(`Expression ${e.kind} cannot be converted to map rules as of now`);
        case "TypedApplicationExpression": {
          if (e.func.kind !== 'TypedIdentifierExpression')
            throws(`Cannot convert application with non identifier func`);
          switch (e.func.name) {
            case "collect":
              aggs.push({
                kind: "SparkCollectListAggregation",
                name: "",
                columns: (e.args[0].type as StructType).properties.map(x => x.name),
              });
              break;
            case "sum":
            case "count":
            case "max":
            case "min":
              aggs.push({
                kind: "SparkSqlAggregation",
                name: "",
                func: e.func.name,
                column: (e.args[0] as TypedIdentifierExpression).name,
              });
              break;
            default:
              throws(`Error: Non aggregation function ${e.func.name}`);
          }
          break;
        }
      }
    }

    e.forEach(x => {
      imp(x.value);
      if (aggs.length > 0) {
        aggs[aggs.length - 1].name = x.name;
      }
    });

    return aggs;
  }

  const deriveMappingRules = (e: TypedRuleProperty[], seen: Map<string, number>): [SparkMapRule[], Map<string, number>] => {
    let c = 0;
    const rules: SparkMapRule[] = [];

    const imp = (e: TypedExpression): string => {
      switch (e.kind) {
        case "TypedIntegerExpression":
        case "TypedBooleanExpression":
          return `${e.value}`;
        case "TypedFloatExpression":
          return `${e.value.toFixed(20)}`;
        case "TypedStringExpression":
          return `"${e.value}"`;
        case "TypedIdentifierExpression":
          return seen.has(e.name) ? `${e.name}${seen.get(e.name)}` : e.name;
        case "TypedLetExpression":
        case "TypedObjectExpression":
        case "TypedFunctionExpression":
        case "TypedBlockExpression":
          throws(`Expression ${e.kind} cannot be converted to map rules as of now`);
        case "TypedApplicationExpression": {
          const func = imp(e.func);
          const args = e.args.map(imp);
          const name = `_v${c++}`;
          rules.push({ kind: "SparkApplicationRule", name, args, func });
          return name;
        }
        case "TypedAddExpression": {
          const left = imp(e.left);
          const right = imp(e.right);
          const name = `_v${c++}`;
          rules.push({ kind: "SparkBinaryOperationRule", name, left, op: e.op, right });
          return name;
        }
        case "TypedDefaultExpression": {
          const left = imp(e.left);
          const right = imp(e.right);
          const name = `_v${c++}`;
          rules.push({ kind: "SparkGetOrElseRule", name, left, right });
          return name;
        }
        case "TypedArrayExpression": {
          const args = e.values.map(imp);
          const name = `_v${c++}`;
          rules.push({ kind: "SparkApplicationRule", name, args, func: `Array[${generateScalaType(convertTypeToScalaType(e.type.of))}]` });
          return name;
        }
      }
    }

    e.forEach(x => {
      if (x.kind === 'TypedRuleValueProperty') {
        imp(x.value);
        if (rules.length > 0) {
          const c = (seen.get(x.name) ?? -1) + 1;
          rules[rules.length - 1].name = `${x.name}${c}`;
          seen.set(x.name, c);
        }
      }
    });

    return [rules, seen];
  }

  const walk = (e: TypedTypeExpression): boolean => {
    switch (e.kind) {
      case "TypedObjectTypeExpression":
      case "TypedIntegerTypeExpression":
      case "TypedFloatTypeExpression":
      case "TypedBooleanTypeExpression":
      case "TypedStringTypeExpression":
      case "TypedIdentifierTypeExpression":
      case "TypedForeignKeyTypeExpression":
      case "TypedPrimaryKeyTypeExpression":
      case "TypedArrayTypeExpression":
        return false;

      case "TypedTypeIntroExpression": {
        walk(e.value);
        if (e.type.kind !== 'StructType') return true;
        if (!isSourceType(e.type)) return true;

        addStructType(e.type);

        const conn = deriveConnectionInfo(e.type);

        if (conn.kind === 'db') {
          datasetHandlers.push({
            kind: "SparkDBSourceDatasetHandler",
            output: datasetIdFor(e.type),
            host: conn.host,
            port: conn.port,
            user: conn.user,
            password: conn.password,
            table: conn.table,
            columns: conn.columns,
          });
        }
        else {
          throws(`Error unknown connection kind: ${conn.kind}`);
        }
        return true;
      }
      case "TypedJoinTypeExpression": {
        walk(e.left);
        walk(e.right);
        addStructType(e.type);
        datasetHandlers.push({
          kind: "SparkJoinDatasetHandler",
          leftInput: datasetIdFor(e.left.type),
          rightInput: datasetIdFor(e.right.type),
          output: datasetIdFor(e.type),
          leftColumn: e.leftColumn,
          rightColumn: e.rightColumn,
          method: e.method,
        });
        return true;
      }
      case "TypedDropTypeExpression": {
        walk(e.left);
        addStructType(e.type);
        datasetHandlers.push({
          kind: "SparkDropDatasetHandler",
          input: datasetIdFor(e.left.type),
          output: datasetIdFor(e.type),
          properties: e.properties,
        });
        return true;
      }
      case "TypedWithTypeExpression": {
        walk(e.left);
        addStructType(e.type);
        const [rules, seen] = deriveMappingRules(e.rules, new Map(e.left.type.properties.map(x => [x.name, 0])));
        datasetHandlers.push({
          kind: "SparkMapDatasetHandler",
          input: datasetIdFor(e.left.type),
          output: datasetIdFor(e.type),
          rules: [
            ...e.left.type.properties.map<SparkMapRule>(p => ({ kind: "SparkRowExtractRule", name: `${p.name}0`, property: p.name })),
            ...rules,
            { kind: "SparkIdentityRule", name: `${e.type.name}(${e.type.properties.map(p => `${p.name}${seen.get(p.name) ?? 0}`).join(', ')})` }
          ],
        });
        return true;
      }
      case "TypedUnionTypeExpression": {
        walk(e.left);
        walk(e.right);
        addStructType(e.type);
        datasetHandlers.push({
          kind: "SparkUnionDatasetHandler",
          leftInput: datasetIdFor(e.left.type),
          rightInput: datasetIdFor(e.right.type),
          output: datasetIdFor(e.type),
        });
        return true;
      }
      case "TypedGroupByTypeExpression": {
        walk(e.left);
        addStructType(e.type);
        datasetHandlers.push({
          kind: "SparkGroupDatasetHandler",
          input: datasetIdFor(e.left.type),
          output: datasetIdFor(e.type),
          column: e.column,
          aggregations: [
            ...deriveAggregations(e.aggregations)
          ],
        });
        return true;
      }
    }
  }

  const dtes = desugar(expressions);

  const dg = namedTypeDependencyGraph(dtes);

  dtes.forEach(walk);

  const caseClasses = [...structTypes.values()].map(scalaCaseClass);

  return {
    kind: "SparkProject",
    name: retlConfig.name,
    package: retlConfig.package,
    caseClasses,
    datasetHandlers,
    vertices: deriveSparkVertices(dg),
  };
}
