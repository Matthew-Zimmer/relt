import { TypedExpression, TypedIdentifierExpression } from "../asts/expression/typed";
import { DatasetId, ScalaCaseClass, ScalaType, SparkAggregation, SparkDatasetHandler, SparkDependencyVertex, SparkMapRule, SparkProject } from "../asts/scala";
import { IdentifierType, identifierType, objectType, ObjectType, PrimitiveType, Type, unitType } from "../asts/type";
import { NamedExpression } from "../asts/typeExpression/flat";
import { NamedTypedExpression, TypedTypeExpression, TypedTypeIntroExpression } from "../asts/typeExpression/typed";
import { DependencyGraph } from "../graph";
import { ReltProject } from "../project";
import { print, throws } from "../utils";
import { evaluate, Scope } from "./evaluate";
import { generateScalaType } from "./scalaToLines";
import { hasOverload } from "./typeCheck/expression";
import { Context, typeEquals } from "./typeCheck/utils";

// let implicitCaseClasses: [string, ObjectType][] = [];

// function implicitCaseClassFor(t: ObjectType): string {
//   const result = implicitCaseClasses.find(x => typeEquals(x[1], t));
//   if (result) return result[0];
//   const name = `ImplicitCaseClass_${implicitCaseClasses.length}`;
//   implicitCaseClasses.push([
//     name,
//     t,
//   ]);
//   return name;
// }

// export function deriveScalaCaseClass(t: TypedTypeIntroExpression<ObjectType>): ScalaCaseClass {
//   return {
//     kind: "ScalaCaseClass",
//     name: t.name,
//     properties: t.type.properties.map(x => ({ name: x.name, type: convertToScalaType(x.type) })),
//   };
// }

// function hasCompoundTypes(t: TypedTypeExpression): boolean {
//   switch (t.kind) {
//     case "TypedIntegerTypeExpression":
//     case "TypedFloatTypeExpression":
//     case "TypedBooleanTypeExpression":
//     case "TypedStringTypeExpression":
//     case "TypedForeignKeyTypeExpression":
//     case "TypedPrimaryKeyTypeExpression":
//       return false;
//     case "TypedJoinTypeExpression":
//     case "TypedDropTypeExpression":
//     case "TypedWithTypeExpression":
//     case "TypedUnionTypeExpression":
//     case "TypedGroupByTypeExpression":
//       return true;

//     case "TypedIdentifierTypeExpression": // this might be wrong
//       return true;

//     case 'TypedArrayTypeExpression':
//       return hasCompoundTypes(t.of);

//     case "TypedObjectTypeExpression":
//       return t.properties.some(p => hasCompoundTypes(p.value));
//     case "TypedTypeIntroExpression":
//       return hasCompoundTypes(t.value);
//   }
// }

// export function isSourceType(t: TypedTypeIntroExpression, ectx: Context): boolean {
//   if (hasCompoundTypes(t)) return false;

//   if (!hasOverload('source', [identifierType(t.name)], ectx))
//     throws(`type ${t.name} is not a compound type but it is missing its "source" function`);

//   return true;
// }

// export function transformationVarName(i: number): string {
//   return `_v${i}`;
// }

// export function namedTransformations(t: TypedExpression, i: number): [string, SparkMapTransformation[], number] {
//   const [rules, varCount1] = deriveTransformationsFromExpression(t, i);

//   if (rules.length === 0) {
//     throws(``);
//   }

//   return [rules[rules.length - 1].name, rules.filter(x => x.kind !== 'SparkIdentityTransformation'), varCount1];
// }

// function deriveTransformationsFromExpression(e: TypedExpression, i: number): [SparkMapTransformation[], number] {
//   switch (e.kind) {
//     case "TypedIntegerExpression":
//     case "TypedFloatExpression":
//     case "TypedBooleanExpression":
//       return [[{ kind: "SparkIdentityTransformation", name: `${e.value}` }], i];
//     case "TypedStringExpression":
//       return [[{ kind: "SparkIdentityTransformation", name: `"${e.value}"` }], i];
//     case "TypedIdentifierExpression":
//       return [[{ kind: "SparkIdentityTransformation", name: e.name }], i];
//     case "TypedApplicationExpression": {
//       const [func, left, i1] = namedTransformations(e.func, i);
//       const [args, right, i2] = e.args.reduce<[string[], SparkMapTransformation[], number]>(([n, l, i], c) => {
//         const x = namedTransformations(c, i);
//         return [[n, x[0]].flat(), [l, x[1]].flat(), x[2]];
//       }, [[], [], i1]);

//       return [[
//         ...left,
//         ...right,
//         { kind: "SparkApplicationTransformation", name: transformationVarName(i2), func, args },
//       ], i2 + 1];
//     }
//     case "TypedAddExpression": {
//       const [lName, left, i1] = namedTransformations(e.left, i);
//       const [rName, right, i2] = namedTransformations(e.right, i1);

//       return [[
//         ...left,
//         ...right,
//         { kind: "SparkBinaryOperationTransformation", name: transformationVarName(i2), left: lName, op: e.op, right: rName },
//       ], i2 + 1];
//     }
//     case "TypedDefaultExpression": {
//       const [lName, left, i1] = namedTransformations(e.left, i);
//       const [rName, right, i2] = namedTransformations(e.right, i1);

//       return [[
//         ...left,
//         ...right,
//         { kind: "SparkGetOrElseTransformation", name: transformationVarName(i2), left: lName, right: rName },
//       ], i2 + 1];
//     }
//     case "TypedObjectExpression":
//     case "TypedLetExpression":
//     case "TypedFunctionExpression":
//     case "TypedBlockExpression":
//       throws(`Error: ${e.kind} cannot be converted to transform expressions (as of now)`);
//     case "TypedArrayExpression": {
//       const [names, rules, i1] = e.values.reduce<[string[], SparkMapTransformation[], number]>(([n, r, i], c) => {
//         const x = namedTransformations(c, i);
//         return [[n, x[0]].flat(), [r, x[1]].flat(), x[2]];
//       }, [[], [], i]);

//       return [[
//         ...rules,
//         { kind: "SparkApplicationTransformation", name: tempVarName(i1), func: "Array", args: names }
//       ], i1 + 1];
//     }
//   }
// }

// function deriveTransformationsFromRule(r: NamedTypedExpression, i: number): [SparkMapTransformation[], number] {
//   const [rules, i1] = deriveTransformationsFromExpression(r.value, i);
//   if (rules.length === 0)
//     throws(`Internal Error: When deriving transformations from a rule, resulting rules was empty this is not allowed`);
//   const front = rules.slice(0, -1);
//   const last = rules[rules.length - 1];
//   return [[...front, { ...last, name: r.name }], i1];
// }

// function deriveAggregation(p: NamedTypedExpression, i: number): [SparkAggregation, number] {
//   let agg: SparkAggregation;

//   switch (p.value.kind) {
//     case "TypedApplicationExpression": {
//       if (p.value.func.kind !== 'TypedIdentifierExpression')
//         throws(`Cannot convert application with non identifier func`);
//       switch (p.value.func.name) {
//         case "collect":
//           agg = {
//             kind: "SparkCollectListAggregation",
//             name: p.name,
//             columns: (p.value.args[0].type as ObjectType).properties.map(x => x.name),
//           };
//           break;
//         case "sum":
//         case "count":
//         case "max":
//         case "min":
//           agg = {
//             kind: "SparkSqlAggregation",
//             name: p.name,
//             func: p.value.func.name,
//             column: (p.value.args[0] as TypedIdentifierExpression).name,
//           };
//           break;
//         default:
//           throws(`Error: Non aggregation function ${p.value.func.name}`);
//       }
//       break;
//     }
//     default:
//       throws(`Cannot convert ${p.value.kind} to agg needs to be an application expression`);
//   }

//   return [agg, i];
// }

// export function namedSparkRules(t: TypedTypeExpression, varCount: number): [string, SparkRule[], number] {
//   const [rules, varCount1] = deriveSparkRules(t, varCount);

//   if (rules.length === 0) {
//     throws(``);
//   }

//   return [rules[rules.length - 1].name, rules, varCount1];
// }

// function tempVarName(count: number): string {
//   return `_ds${count}`;
// }

// export function deriveSparkRules(t: TypedTypeExpression, varCount: number): [SparkRule[], number] {
//   switch (t.kind) {
//     case 'TypedBooleanTypeExpression':
//     case 'TypedFloatTypeExpression':
//     case 'TypedStringTypeExpression':
//     case 'TypedIntegerTypeExpression':
//     case 'TypedObjectTypeExpression':
//     case 'TypedPrimaryKeyTypeExpression':
//     case 'TypedForeignKeyTypeExpression':
//       return [[], varCount];
//     case 'TypedIdentifierTypeExpression':
//       return [[
//         { kind: "SparkIdentityRule", name: `${uncap(t.name)}DS` },
//       ], varCount];
//     case 'TypedTypeIntroExpression':
//       return deriveSparkRules(t.value, varCount);
//     case 'TypedJoinTypeExpression': {
//       const [leftName, leftRules, varCount1] = namedSparkRules(t.left, varCount);
//       const [rightName, rightRules, varCount2] = namedSparkRules(t.right, varCount1);

//       const var1 = tempVarName(varCount2);
//       const var2 = tempVarName(varCount2 + 1);

//       return [[
//         ...leftRules,
//         ...rightRules,
//         { kind: "SparkJoinRule", name: var1, left: leftName, right: rightName, leftColumn: t.leftColumn, rightColumn: t.rightColumn, type: t.method },
//         { kind: "SparkAsRule", name: var2, dataset: var1, type: t.shallowTypeValue.name },
//       ], varCount2 + 2];
//     }
//     case 'TypedDropTypeExpression': {
//       const [leftName, leftRules, varCount1] = namedSparkRules(t.left, varCount);

//       const props = t.type.properties;

//       return [[
//         ...leftRules,
//         {
//           kind: "SparkMapRule", name: tempVarName(varCount1), dataset: leftName, transformations: [
//             ...props.map<SparkMapTransformation>(p => ({
//               kind: 'SparkRowExtractTransformation',
//               name: p.name,
//               property: p.name,
//             })),
//             { kind: "SparkApplicationTransformation", name: "_ret", func: t.shallowTypeValue.name, args: props.map(x => x.name) },
//             { kind: "SparkIdentityTransformation", name: "_ret" },
//           ]
//         }
//       ], varCount1 + 1];
//     }
//     case 'TypedWithTypeExpression': {
//       const [leftName, leftRules, varCount1] = namedSparkRules(t.left, varCount);

//       const props = t.left.type.properties;
//       const [transformations] = t.rules.reduce<[SparkMapTransformation[], number]>(([l, i], r) => {
//         const [l1, i1] = deriveTransformationsFromRule(r, i);
//         return [[l, l1].flat(), i1];
//       }, [[], 0]);

//       return [[
//         ...leftRules,
//         {
//           kind: "SparkMapRule", name: tempVarName(varCount1), dataset: leftName, transformations: [
//             ...props.map<SparkMapTransformation>(p => ({
//               kind: 'SparkRowExtractTransformation',
//               name: p.name,
//               property: p.name,
//             })),
//             ...transformations,
//             { kind: "SparkApplicationTransformation", name: "_ret", func: t.shallowTypeValue.name, args: [props, t.rules].flat().map(x => x.name) },
//             { kind: "SparkIdentityTransformation", name: "_ret" },
//           ]
//         }
//       ], varCount1 + 1];
//     }
//     case 'TypedUnionTypeExpression':
//       throws(`TODO deriveSparkRules:TypedUnionTypeExpression`);
//     case "TypedArrayTypeExpression":
//       throws(`TODO deriveSparkRules:TypedArrayTypeExpression`);
//     case "TypedGroupByTypeExpression": {
//       const [leftName, leftRules, varCount1] = namedSparkRules(t.left, varCount);

//       const [aggregations] = t.aggregations.reduce<[SparkAggregation[], number]>(([l, i], r) => {
//         const [l1, i1] = deriveAggregation(r, i);
//         return [[l, l1].flat(), i1];
//       }, [[], 0]);

//       return [[
//         ...leftRules,
//         {
//           kind: "SparkGroupAggRule",
//           name: tempVarName(varCount1),
//           dataset: leftName,
//           groupColumn: t.column,
//           aggregations,
//         }
//       ], varCount1 + 1];
//     }
//   }
// }

// export function deriveConnectionInfo(t: TypedTypeIntroExpression, scope: Scope): SparkConnectionInfo {
//   const [value] = evaluate({
//     kind: "TypedApplicationExpression",
//     func: {
//       kind: "TypedIdentifierExpression",
//       name: 'source',
//       type: { kind: "FunctionType", from: [identifierType(t.name)], to: unitType() }
//     },
//     args: [{ kind: "TypedIdentifierExpression", name: "__0", type: identifierType(t.name) }],
//     type: unitType(),
//   }, { ...scope, __0: undefined });

//   if (!(typeof value === 'object' && "kind" in value && typeof value.kind === 'string' && ["db"].includes(value.kind)))
//     throws(`function "source" for "${t.name}" did not return valid data`);
//   const kind = value.kind as "db";
//   // TODO use a library like zod to actually check these values
//   switch (kind) {
//     case "db":
//       return {
//         kind: "SparkDBConnectionInfo",
//         host: value.host as string,
//         port: value.port as number,
//         user: value.user as string,
//         password: value.password as string,
//         table: value.table as string,
//         //columns: value.columns as string[],
//       };
//   }
// }

// export function deriveSourceDatasetHandler(t: TypedTypeIntroExpression, scope: Scope, idx: number, count: number): SourceDatasetHandler {
//   return {
//     kind: "SourceDatasetHandler",
//     typeName: t.name,
//     datasetIndex: idx,
//     datasetCount: count,
//     connectionInfo: deriveConnectionInfo(t, scope),
//   };
// }

// export function deriveDerivedDatasetHandler(t: TypedTypeIntroExpression, idx: number, count: number, parents: { name: string, index: number }[]): DerivedDatasetHandler {
//   const [name, rules] = namedSparkRules(t.value, 0);
//   return {
//     kind: "DerivedDatasetHandler",
//     typeName: t.name,
//     datasetIndex: idx,
//     datasetCount: count,
//     parentDatasets: parents,
//     rules: [
//       ...rules,
//       { kind: "SparkReturnRule", name }
//     ],
//   };
// }

// export function deriveDatasetHandler(t: TypedTypeIntroExpression, ectx: Context, scope: Scope, indices: Map<string, number>, dg: DependencyGraph): DatasetHandler {
//   const idx = indices.get(t.name)!;
//   const count = indices.size;
//   const parents = dg.parentsOf(t.name).map(x => ({ name: x, index: indices.get(x)! }));
//   return isSourceType(t, ectx) ? deriveSourceDatasetHandler(t, scope, idx, count) : deriveDerivedDatasetHandler(t, idx, count, parents);
// }

// export function deriveSparkType(t: TypedTypeIntroExpression<ObjectType>, ectx: Context, scope: Scope, indices: Map<string, number>, dg: DependencyGraph): SparkType {
//   return {
//     kind: "SparkType",
//     caseClass: deriveScalaCaseClass(t),
//     datasetHandler: deriveDatasetHandler(t, ectx, scope, indices, dg),
//   };
// }

// export function deriveSparkVertices(dg: DependencyGraph): SparkDependencyVertex[] {
//   return dg.vertices.map<SparkDependencyVertex>(x => ({
//     kind: "SparkDependencyVertex",
//     id: x.id,
//     name: x.value,
//     incoming: dg.parents(x.id),
//     outgoing: dg.children(x.id),
//   }));
// }

// export function deriveSparkProject(reltProject: Required<ReltProject>, namedTypeExpressions: TypedTypeIntroExpression<ObjectType>[], ectx: Context, scope: Scope, dg: DependencyGraph): SparkProject {
//   const indexMapping = new Map(namedTypeExpressions.map((x, i) => [x.name, i]));

//   return {
//     kind: "SparkProject",
//     name: reltProject.name,
//     package: reltProject.package,
//     types: namedTypeExpressions.map(x => deriveSparkType(x, ectx, scope, indexMapping, dg)),
//     vertices: deriveSparkVertices(dg),
//     implicitCaseClasses: implicitCaseClasses.map(([name, t]) => (
//       {
//         kind: "ScalaCaseClass",
//         name: name,
//         properties: t.properties.map(x => ({ name: x.name, type: convertToScalaType(x.type) })),
//       }
//     )),
//   };
// }

export function convertTypeToScalaType(t: Type): ScalaType {
  switch (t.kind) {
    case 'BooleanType':
      return { kind: "ScalaBooleanType" };
    case 'FloatType':
      return { kind: "ScalaDoubleType" };
    case 'StringType':
      return { kind: "ScalaStringType" };
    case 'IntegerType':
      return { kind: "ScalaIntType" };
    case 'IdentifierType':
      return { kind: "ScalaIdentifierType", name: t.name };
    case 'ForeignKeyType':
      return convertTypeToScalaType(t.of);
    case 'PrimaryKeyType':
      return convertTypeToScalaType(t.of);
    case 'ArrayType':
      return { kind: "ScalaArrayType", of: convertTypeToScalaType(t.of) };
    case 'OptionalType':
      return { kind: "ScalaOptionalType", of: convertTypeToScalaType(t.of) };
    case 'FunctionType':
      throws(`Cannot convert a function type to scala`);
    case 'ObjectType':
      throws(`Cannot convert object type to scala (this could be done but is not supported as I don't think it is useful)`);
    case 'TypeType':
      throws(`Cannot convert type type to scala`);
    case 'UnitType':
      return { kind: "ScalaUnitType" };
    case 'UnionType':
      throws(`Cannot convert union type to scala`);
  }
}

export function scalaCaseClass(name: string, ty: ObjectType): ScalaCaseClass {
  return {
    kind: "ScalaCaseClass",
    name,
    properties: ty.properties.map(({ name, type }) => ({ name, type: convertTypeToScalaType(type) })),
  };
}

export function deriveSparkVertices(dg: DependencyGraph): SparkDependencyVertex[] {
  return dg.vertices.map<SparkDependencyVertex>(x => ({
    kind: "SparkDependencyVertex",
    id: x.id,
    name: x.value,
    incoming: dg.parents(x.id),
    outgoing: dg.children(x.id),
  }));
}

export function deriveSparkProject(
  retlConfig: Required<ReltProject>,
  intros: TypedTypeIntroExpression<ObjectType>[],
  expressionContext: Context,
  scope: Scope,
  dg: DependencyGraph,
): SparkProject {

  const deriveConnectionInfo = (t: TypedTypeIntroExpression) => {
    const [value] = evaluate({
      kind: "TypedApplicationExpression",
      func: {
        kind: "TypedIdentifierExpression",
        name: 'source',
        type: { kind: "FunctionType", from: [identifierType(t.name)], to: unitType() }
      },
      args: [{ kind: "TypedIdentifierExpression", name: "__0", type: identifierType(t.name) }],
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

  let stack: [string, ObjectType][] = [];

  const scalaType = (t: Type): string => {
    if (t.kind === 'ObjectType') {
      const result = stack.find(x => typeEquals(t, x[1]));
      if (result !== undefined) return result[0];
    }
    return generateScalaType(convertTypeToScalaType(t));
  }

  const deriveScalaCaseClasses = (intros: TypedTypeIntroExpression<ObjectType>[]): ScalaCaseClass[] => {
    const pushStack = (ty: ObjectType): IdentifierType => {
      const result = stack.find(x => typeEquals(ty, x[1]));
      if (result === undefined) {
        const name = `ImplicitCaseClass${stack.length}`;
        stack.push([name, ty]);
        return identifierType(name);
      }
      else {
        return identifierType(result[0]);
      }
    }

    const stackSubstitute = (oldTy: IdentifierType, newTy: IdentifierType): [string, ObjectType][] => {
      return stack.map(x => [
        x[0] === oldTy.name ? newTy.name : x[0],
        objectType(...x[1].properties.map(p => ({
          name: p.name,
          type: p.type.kind === 'IdentifierType' && p.type.name === oldTy.name ? newTy : p.type,
        })))
      ]);
    }

    const imp = (t: Type): PrimitiveType => {
      switch (t.kind) {
        case "IntegerType":
        case "FloatType":
        case "BooleanType":
        case "StringType":
        case "IdentifierType":
        case "UnitType":
        case "PrimaryKeyType":
        case "ForeignKeyType":
          return t;
        case "TypeType":
        case "FunctionType":
        case "UnionType":
          throws(`Cannot flatten ${t.kind}`);
        case "ArrayType":
        case "OptionalType":
          return { ...t, of: imp(t.of) };
        case "ObjectType":
          return pushStack(objectType(...t.properties.map(x => ({ name: x.name, type: imp(x.type) }))));
      }
    }

    intros.forEach(e => {
      const type = imp(e.type) as IdentifierType;
      stack = stackSubstitute(type, identifierType(e.name));
    });

    return stack.map<ScalaCaseClass>(x => ({
      kind: "ScalaCaseClass",
      name: x[0],
      properties: x[1].properties.map(x => ({ name: x.name, type: convertTypeToScalaType(x.type) })),
    }));
  }

  const indices = new Map(intros.map((x, i) => [x.name, i]));

  const datasetIdFor = (name: string): DatasetId => {
    const idx = indices.get(name);
    if (idx === undefined) throws(`Not a valid dataset handler type ${name}`);
    return { name, idx };
  };

  const isSourceType = (t: TypedTypeIntroExpression): boolean => {
    return hasOverload('source', [identifierType(t.name)], expressionContext);
  }

  const deriveAggregations = (e: NamedTypedExpression[]): SparkAggregation[] => {
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
                columns: (e.args[0].type as ObjectType).properties.map(x => x.name),
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

  const deriveMappingRules = (e: NamedTypedExpression[], seen: Map<string, number>): [SparkMapRule[], Map<string, number>] => {
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
          rules.push({ kind: "SparkApplicationRule", name, args, func: `Array[${scalaType(e.type.of)}]` });
          return name;
        }
      }
    }

    e.forEach(x => {
      imp(x.value);
      if (rules.length > 0) {
        const c = (seen.get(x.name) ?? -1) + 1;
        rules[rules.length - 1].name = `${x.name}${c}`;
        seen.set(x.name, c);
      }
    });

    return [rules, seen];
  }

  const deriveDatasetHandler = (e: TypedTypeIntroExpression<ObjectType>): SparkDatasetHandler[] => {
    const v = e.value;
    switch (v.kind) {
      case "TypedIntegerTypeExpression":
      case "TypedFloatTypeExpression":
      case "TypedBooleanTypeExpression":
      case "TypedStringTypeExpression":
      case "TypedIdentifierTypeExpression":
      case "TypedTypeIntroExpression":
      case "TypedPrimaryKeyTypeExpression":
      case "TypedForeignKeyTypeExpression":
      case "TypedArrayTypeExpression":
        return [];
      case "TypedObjectTypeExpression": {
        if (!isSourceType(e)) return [];

        const conn = deriveConnectionInfo(e);

        if (conn.kind === 'db') {
          return [{
            kind: "SparkDBSourceDatasetHandler",
            output: datasetIdFor(e.name),
            host: conn.host,
            port: conn.port,
            user: conn.user,
            password: conn.password,
            table: conn.table,
            columns: conn.columns,
          }];
        }
        else {
          throws(`Error unknown connection kind: ${conn.kind}`);
        }
      }
      case "TypedJoinTypeExpression":
        return [{
          kind: "SparkJoinDatasetHandler",
          leftInput: datasetIdFor(v.left.name),
          rightInput: datasetIdFor(v.right.name),
          output: datasetIdFor(e.name),
          leftColumn: v.leftColumn,
          rightColumn: v.rightColumn,
          method: v.method,
        }];
      case "TypedDropTypeExpression":
        return [{
          kind: "SparkDropDatasetHandler",
          input: datasetIdFor(v.left.name),
          output: datasetIdFor(e.name),
          properties: v.properties,
        }];
      case "TypedWithTypeExpression":
        const [rules, seen] = deriveMappingRules(v.rules, new Map(v.left.type.properties.map(x => [x.name, 0])));
        return [{
          kind: "SparkMapDatasetHandler",
          input: datasetIdFor(v.left.name),
          output: datasetIdFor(e.name),
          rules: [
            ...v.left.type.properties.map<SparkMapRule>(p => ({ kind: "SparkRowExtractRule", name: `${p.name}0`, property: p.name })),
            ...rules,
            { kind: "SparkIdentityRule", name: `${e.name}(${e.type.properties.map(p => `${p.name}${seen.get(p.name) ?? 0}`).join(', ')})` }
          ],
        }];
      case "TypedUnionTypeExpression":
        return [{
          kind: "SparkUnionDatasetHandler",
          leftInput: datasetIdFor(v.left.name),
          rightInput: datasetIdFor(v.right.name),
          output: datasetIdFor(e.name),
        }];
      case "TypedGroupByTypeExpression":
        return [{
          kind: "SparkGroupDatasetHandler",
          input: datasetIdFor(v.left.name),
          output: datasetIdFor(e.name),
          column: v.column,
          aggregations: [
            ...deriveAggregations(v.aggregations)
          ],
        }];
    }
  };

  // const deriveScalaCaseClasses = (e: TypedTypeExpression[]): ScalaCaseClass[] => {
  //   const stack: [string, ObjectType][] = [];
  //   const imp = (t: Type) => {
  //     switch (t.kind) {

  //     }
  //   }

  //   let c = 0;
  //   e.forEach(imp);

  //   // BIG TODO!!!
  //   // Need to replace all nested object types
  //   // with the correlated identifier type
  //   return stack.map(([name, ty]) => scalaCaseClass(name === '' ? `T_${c++}` : name, ty));
  // }

  const caseClasses = deriveScalaCaseClasses(intros);
  const datasetHandlers = intros.flatMap(deriveDatasetHandler);

  return {
    kind: "SparkProject",
    name: retlConfig.name,
    package: retlConfig.package,
    caseClasses,
    datasetHandlers,
    vertices: deriveSparkVertices(dg),
  };
}