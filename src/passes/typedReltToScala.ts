import { ScalaType, ScalaCaseClass, SourceDatasetHandler, SparkRule, SparkMapTransformation, DerivedDatasetHandler, DatasetHandler, SparkType, SparkProject, SparkConnectionInfo } from "../asts/scala";
import { identifierType, Type, unitType } from "../asts/type";
import { TypedTypeIntroExpression, TypedTypeExpression } from "../asts/typeExpression/typed";
import { DependencyGraph } from "../graph";
import { print, throws, uncap } from "../utils";
import { evaluate, Scope } from "./evaluate";
import { hasOverload, isValidExpression } from "./typeCheck/expression";
import { Context } from "./typeCheck/utils";

export function convertToScalaType(t: Type): ScalaType {
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
      return { kind: "ScalaBooleanType" };
    case 'FunctionType':
      throws(`Cannot convert a function type to scala`);
    case 'ObjectType':
      throws(`Cannot convert object type to scala (this could be done but is not supported as I don't think it is useful)`);
    case 'TypeType':
      throws(`Cannot convert type type to scala`);
    case 'UnitType':
      throws(`Cannot convert unit type to scala (this could be done but is not supported as I don't think it is useful)`);
    case 'UnionType':
      throws(`Cannot convert union type to scala`);
  }
}

export function deriveScalaCaseClass(t: TypedTypeIntroExpression): ScalaCaseClass {
  const type = t.deepTypeValue;

  if (type.kind !== 'ObjectType')
    throws(`Cannot convert non object type to scala case class`);

  return {
    kind: "ScalaCaseClass",
    name: t.name,
    properties: type.properties.map(x => ({ name: x.name, type: convertToScalaType(x.type) })),
  };
}

function hasCompoundTypes(t: TypedTypeExpression): boolean {
  switch (t.kind) {
    case "TypedIntegerTypeExpression":
    case "TypedFloatTypeExpression":
    case "TypedBooleanTypeExpression":
    case "TypedStringTypeExpression":
      return false;
    case "TypedJoinTypeExpression":
    case "TypedDropTypeExpression":
    case "TypedWithTypeExpression":
    case "TypedUnionTypeExpression":
      return true;

    case "TypedIdentifierTypeExpression": // this might be wrong
      return true;

    case "TypedObjectTypeExpression":
      return t.properties.some(p => hasCompoundTypes(p.value));
    case "TypedTypeIntroExpression":
      return hasCompoundTypes(t.value);
  }
}

export function isSourceType(t: TypedTypeIntroExpression, ectx: Context): boolean {
  if (hasCompoundTypes(t)) return false;

  if (!hasOverload('source', [identifierType(t.name)], ectx))
    throws(`type ${t.name} is not a compound type but it is missing its "source" function`);

  return true;
}

export function namedSparkRules(t: TypedTypeExpression, varCount: number): [string, SparkRule[], number] {
  const [rules, varCount1] = deriveSparkRules(t, varCount);

  if (rules.length === 0) {
    throws(``);
  }

  return [rules[rules.length - 1].name, rules, varCount1];
}

function tempVarName(count: number): string {
  return `_ds${count}`;
}

export function deriveSparkRules(t: TypedTypeExpression, varCount: number): [SparkRule[], number] {
  switch (t.kind) {
    case 'TypedBooleanTypeExpression':
    case 'TypedFloatTypeExpression':
    case 'TypedStringTypeExpression':
    case 'TypedIntegerTypeExpression':
    case 'TypedObjectTypeExpression':
      return [[], varCount];
    case 'TypedIdentifierTypeExpression':
      return [[
        { kind: "SparkIdentityRule", name: `${uncap(t.name)}DS` },
      ], varCount];
    case 'TypedTypeIntroExpression':
      return deriveSparkRules(t.value, varCount);
    case 'TypedJoinTypeExpression': {
      const [leftName, leftRules, varCount1] = namedSparkRules(t.left, varCount);
      const [rightName, rightRules, varCount2] = namedSparkRules(t.right, varCount1);

      const var1 = tempVarName(varCount2);
      const var2 = tempVarName(varCount2 + 1);

      return [[
        ...leftRules,
        ...rightRules,
        { kind: "SparkJoinRule", name: var1, left: leftName, right: rightName, leftColumn: t.leftColumn, rightColumn: t.rightColumn, type: t.type },
        { kind: "SparkAsRule", name: var2, dataset: var1, type: t.shallowTypeValue.name },
      ], varCount2 + 2];
    }
    case 'TypedDropTypeExpression': {
      const [leftName, leftRules, varCount1] = namedSparkRules(t.left, varCount);

      const props = t.deepTypeValue.properties;

      return [[
        ...leftRules,
        {
          kind: "SparkMapRule", name: tempVarName(varCount1), dataset: leftName, transformations: [
            ...props.map<SparkMapTransformation>(p => ({
              kind: 'SparkRowExtractTransformation',
              name: p.name,
              property: p.name,
            })),
            { kind: "SparkApplicationTransformation", name: "_ret", func: t.shallowTypeValue.name, args: props.map(x => x.name) },
            { kind: "SparkReturnTransformation", name: "_ret" },
          ]
        }
      ], varCount1 + 1];
    }
    case 'TypedWithTypeExpression': {
      // const [leftName, leftRules] = namedSparkRules(t.left);
      // const [type] = normalize(t.left, {});

      // if (type.kind !== 'ObjectType')
      //   throws(`deriveSparkRules:WithTypeExpression internal error`);

      // const props = type.properties;

      // // const [rightName, rightRules] = namedSparkRules(t.right);

      // return [
      //   ...leftRules,
      //   {
      //     kind: "SparkMapRule", name: "????", dataset: leftName, transformations: [
      //       ...props.map<SparkMapTransformation>(p => ({
      //         kind: "SparkRowExtractTransformation",
      //         name: p.name,
      //         property: p.name,
      //       })),
      //       { kind: "SparkApplicationTransformation", name: "_ret", func: t.shallowTypeValue.name, args: props.map(x => x.name) },
      //       { kind: "SparkReturnTransformation", name: "_ret" },
      //     ]
      //   },
      // ];

      throws(`TODO deriveSparkRules:TypedWithTypeExpression`);
    }
    case 'TypedUnionTypeExpression':
      throws(`TODO deriveSparkRules:TypedUnionTypeExpression`);
  }
}

export function deriveConnectionInfo(t: TypedTypeIntroExpression, scope: Scope): SparkConnectionInfo {
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
        kind: "SparkDBConnectionInfo",
        host: value.host as string,
        port: value.port as number,
        user: value.user as string,
        password: value.password as string,
        table: value.table as string,
        //columns: value.columns as string[],
      };
  }
}

export function deriveSourceDatasetHandler(t: TypedTypeIntroExpression, scope: Scope, idx: number, count: number): SourceDatasetHandler {
  return {
    kind: "SourceDatasetHandler",
    typeName: t.name,
    datasetIndex: idx,
    datasetCount: count,
    connectionInfo: deriveConnectionInfo(t, scope),
  };
}

export function deriveDerivedDatasetHandler(t: TypedTypeIntroExpression, idx: number, count: number, parents: { name: string, index: number }[]): DerivedDatasetHandler {
  const [name, rules] = namedSparkRules(t.value, 0);
  return {
    kind: "DerivedDatasetHandler",
    typeName: t.name,
    datasetIndex: idx,
    datasetCount: count,
    parentDatasets: parents,
    rules: [
      ...rules,
      { kind: "SparkReturnRule", name }
    ],
  };
}

export function deriveDatasetHandler(t: TypedTypeIntroExpression, ectx: Context, scope: Scope, indices: Map<string, number>, dg: DependencyGraph): DatasetHandler {
  const idx = indices.get(t.name)!;
  const count = indices.size;
  const parents = dg.parentsOf(t.name).map(x => ({ name: x, index: indices.get(x)! }));
  return isSourceType(t, ectx) ? deriveSourceDatasetHandler(t, scope, idx, count) : deriveDerivedDatasetHandler(t, idx, count, parents);
}

export function deriveSparkType(t: TypedTypeIntroExpression, ectx: Context, scope: Scope, indices: Map<string, number>, dg: DependencyGraph): SparkType {
  return {
    kind: "SparkType",
    caseClass: deriveScalaCaseClass(t),
    datasetHandler: deriveDatasetHandler(t, ectx, scope, indices, dg),
  };
}

export function deriveSparkProject(namedTypeExpressions: TypedTypeIntroExpression[], ectx: Context, scope: Scope, dg: DependencyGraph): SparkProject {
  const indexMapping = new Map(namedTypeExpressions.map((x, i) => [x.name, i]));

  return {
    kind: "SparkProject",
    name: "libname",
    types: namedTypeExpressions.map(x => deriveSparkType(x, ectx, scope, indexMapping, dg)),
  };
}
