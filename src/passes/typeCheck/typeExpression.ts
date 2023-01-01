import { arrayType, booleanType, fkType, floatType, ForeignKeyType, functionType, IdentifierType, identifierType, integerType, objectType, ObjectType, optionalType, pkType, PrimaryKeyType, stringType, Type, unionType } from "../../asts/type";
import { TypedAggProperty, TypedIntegerTypeExpression, TypedJoinTypeExpression, TypedObjectTypeExpression, TypedRuleProperty, TypedStringTypeExpression, TypedTypeExpression, TypedTypeIntroExpression } from "../../asts/typeExpression/typed";
import { LinearJoinTypeExpression, LinearTypeExpression, LinearTypeIntroExpression } from "../../asts/typeExpression/linear";
import { throws, print } from "../../utils";
import { Context, typeEquals } from "./utils";
import { typeCheckExpression } from "./expression";

function resolve(name: string, ctx: Context): Exclude<Type, IdentifierType> {
  if (!(name in ctx))
    throws(`Type ${name} is not defined`);
  const type = ctx[name];
  return type.kind === 'IdentifierType' ? resolve(type.name, ctx) : type;
}

function mergeObjectTypes(...types: ObjectType[]): ObjectType {
  const props = new Map<string, number>();
  const properties: ObjectType['properties'] = [];

  types.forEach(type => type.properties.forEach(prop => {
    if (props.has(prop.name)) {
      properties[props.get(prop.name)!] = prop;
    }
    else {
      properties.push(prop);
    }
    props.set(prop.name, properties.length);
  }));

  return {
    kind: "ObjectType",
    properties,
  };
}

function makeOptional(t: ObjectType): ObjectType {
  return objectType(...t.properties.map(x => ({ name: x.name, type: optionalType(x.type) })));
}

function mergeJoinTypes(type: TypedJoinTypeExpression['type'], l: ObjectType, r: ObjectType): ObjectType {
  switch (type) {
    case 'inner':
      return mergeObjectTypes(l, r);
    case 'left':
      return mergeObjectTypes(l, makeOptional(r));
    case 'right':
      return mergeObjectTypes(makeOptional(l), r);
    case 'outer':
      return mergeObjectTypes(makeOptional(l), makeOptional(r));
  }
}

function makeNewNamedType(ty: ObjectType, ctx: Context): [IdentifierType, Context] {
  const name = `T_IntermediateType_${Object.keys(ctx).length}`;
  return [identifierType(name), { ...ctx, [name]: ty }];
}

function lookupShallowType(ty: ObjectType, ctx: Context): [IdentifierType, Context] {
  const options = Object.entries(ctx).filter(([, v]) => typeEquals(ty, v));
  return options.length === 0 ? makeNewNamedType(ty, ctx) : [identifierType(options[0][0]), ctx];
}

function removeProperties(ty: ObjectType, propertyNames: string[]): ObjectType {
  const props = new Map<string, Type>(ty.properties.map(x => [x.name, x.type]));

  propertyNames.forEach(prop => {
    if (!(props.has(prop)))
      throws(`Property ${prop} is not defined in object type`);
    props.delete(prop);
  });

  return {
    kind: "ObjectType",
    properties: [...props.entries()].map(x => ({ name: x[0], type: x[1] })),
  };
}

function hasProperty(ty: ObjectType, name: string): boolean {
  return ty.properties.some(x => x.name === name);
}

function foreignKeys(t: ObjectType) {
  return t.properties.filter((x): x is { name: string, type: ForeignKeyType } => x.type.kind === 'ForeignKeyType')
}

function relationsFor(l: { shallowTypeValue: IdentifierType, deepTypeValue: ObjectType }, r: { shallowTypeValue: IdentifierType, deepTypeValue: ObjectType }): [string, string][] {
  const lfKeys = foreignKeys(l.deepTypeValue);
  const rfKeys = foreignKeys(r.deepTypeValue);
  const lRelations = lfKeys.filter(x => x.type.table === r.shallowTypeValue.name).map<[string, string]>(x => [x.name, x.type.column]);
  const rRelations = rfKeys.filter(x => x.type.table === l.shallowTypeValue.name).map<[string, string]>(x => [x.type.column, x.name]);
  return [lRelations, rRelations].flat();
}

function checkJoinRelation(l: TypedTypeExpression, r: TypedTypeExpression, relation: [string | undefined, string | undefined]): [string, string] {
  if (l.deepTypeValue.kind !== 'ObjectType')
    throws(`Left of join did not resolve to object type`);
  if (r.deepTypeValue.kind !== 'ObjectType')
    throws(`Right of join did not resolve to object type`);

  if (l.shallowTypeValue.kind !== 'IdentifierType')
    throws(`Left of join did not resolve to object type`);
  if (r.shallowTypeValue.kind !== 'IdentifierType')
    throws(`Right of join did not resolve to object type`);

  const relations = relationsFor(l as { shallowTypeValue: IdentifierType, deepTypeValue: ObjectType }, r as { shallowTypeValue: IdentifierType, deepTypeValue: ObjectType });

  const lCol = relation[0] ?? (relations.length === 1 ? relations[0][0] : throws(`For relation ${l.shallowTypeValue.name}, ${r.shallowTypeValue.name} you must provide an explicit on clause`));
  const rCol = relation[1] ?? (relations.length === 1 ? relations[0][1] : throws(`For relation ${l.shallowTypeValue.name}, ${r.shallowTypeValue.name} you must provide an explicit on clause`));

  if (lCol && !hasProperty(l.deepTypeValue, lCol))
    throws(`Column ${lCol} is not defined on left type of join`);
  if (rCol && !hasProperty(r.deepTypeValue, rCol))
    throws(`Column ${rCol} is not defined on right type of join`);

  return [lCol, rCol]
}

export function typeCheckTypeExpression(e: LinearTypeExpression, ctx: Context): [TypedTypeExpression, Context] {
  switch (e.kind) {
    case "LinearIntegerTypeExpression": {
      return [{ kind: "TypedIntegerTypeExpression", shallowTypeValue: integerType(), deepTypeValue: integerType() }, ctx];
    }
    case "LinearFloatTypeExpression": {
      return [{ kind: "TypedFloatTypeExpression", shallowTypeValue: floatType(), deepTypeValue: floatType() }, ctx];
    }
    case "LinearBooleanTypeExpression": {
      return [{ kind: "TypedBooleanTypeExpression", shallowTypeValue: booleanType(), deepTypeValue: booleanType() }, ctx];
    }
    case "LinearStringTypeExpression": {
      return [{ kind: "TypedStringTypeExpression", shallowTypeValue: stringType(), deepTypeValue: stringType() }, ctx];
    }
    case "LinearPrimaryKeyTypeExpression": {
      const of = typeCheckTypeExpression(e.of, ctx)[0] as TypedIntegerTypeExpression | TypedStringTypeExpression;
      return [{ kind: "TypedPrimaryKeyTypeExpression", of, shallowTypeValue: pkType(of.shallowTypeValue), deepTypeValue: pkType(of.deepTypeValue) }, ctx];
    }
    case "LinearForeignKeyTypeExpression": {
      if (!(e.table in ctx))
        throws(`Error: unknown type ${e.table}`);

      const ty = ctx[e.table];

      if (ty.kind !== 'ObjectType')
        throws(`Error: ${e.table} is not an object`);

      const prop = ty.properties.find(x => x.name === e.column);

      if (prop === undefined)
        throws(`Error: ${e.table} does not contain property ${e.column}`);

      if (prop.type.kind !== "IntegerType" && prop.type.kind !== 'StringType' && prop.type.kind !== 'PrimaryKeyType' && prop.type.kind !== 'ForeignKeyType')
        throws(`Error: Cannot have a foreign key to a non integer or string field`);

      const tyv = fkType(e.table, e.column, prop.type);

      return [{ kind: "TypedForeignKeyTypeExpression", table: e.table, column: e.column, shallowTypeValue: tyv, deepTypeValue: tyv }, ctx];
    }
    case "LinearObjectTypeExpression": {
      const [properties, ctx1] = e.properties.reduce<[TypedObjectTypeExpression['properties'], Context]>(([a, c], v) => {
        const [e, u] = typeCheckTypeExpression(v.value, c);
        return [[...a, { name: v.name, value: e }], u];
      }, [[], ctx]);

      return [{
        kind: "TypedObjectTypeExpression",
        properties,
        shallowTypeValue: {
          kind: "ObjectType",
          properties: properties.map(x => ({ name: x.name, type: x.value.shallowTypeValue }))
        },
        deepTypeValue: {
          kind: "ObjectType",
          properties: properties.map(x => ({ name: x.name, type: x.value.deepTypeValue }))
        },
      }, ctx1];
    }

    case "LinearIdentifierTypeExpression": {
      return [{ kind: "TypedIdentifierTypeExpression", name: e.name, shallowTypeValue: identifierType(e.name), deepTypeValue: resolve(e.name, ctx) }, ctx];
    }
    case "LinearTypeIntroExpression": {
      const [value, ctx1] = typeCheckTypeExpression(e.value, ctx);
      if (value.shallowTypeValue.kind === 'IdentifierType') {
        const overrideShallowType = identifierType(e.name);
        return [
          // @ts-expect-error
          { kind: "TypedTypeIntroExpression", name: e.name, value: { ...value, shallowTypeValue: overrideShallowType }, shallowTypeValue: overrideShallowType, deepTypeValue: value.deepTypeValue },
          { ...ctx1, [e.name]: ctx1[value.shallowTypeValue.name] }];
      }
      else {
        return [
          { kind: "TypedTypeIntroExpression", name: e.name, value, shallowTypeValue: value.shallowTypeValue, deepTypeValue: value.deepTypeValue },
          {
            ...ctx1, [e.name]: value.shallowTypeValue
          }];
      }
    }

    case "LinearJoinTypeExpression": {
      const [left, ctx1] = typeCheckTypeExpression(e.left, ctx);
      const [right, ctx2] = typeCheckTypeExpression(e.right, ctx1);
      const [leftColumn, rightColumn] = checkJoinRelation(left, right, [e.leftColumn, e.rightColumn]);

      const deepTypeValue = mergeJoinTypes(e.type, left.deepTypeValue as ObjectType, right.deepTypeValue as ObjectType);
      const [shallowTypeValue, ctx3] = lookupShallowType(deepTypeValue, ctx2);

      return [{
        kind: "TypedJoinTypeExpression",
        left,
        right,
        leftColumn,
        rightColumn,
        type: e.type,
        shallowTypeValue,
        deepTypeValue,
      }, ctx3];
    }
    case "LinearDropTypeExpression": {
      const [left, ctx1] = typeCheckTypeExpression(e.left, ctx);
      if (left.deepTypeValue.kind !== 'ObjectType')
        throws(`Left of join did not resolve to object type`);

      const deepTypeValue = removeProperties(left.deepTypeValue, e.properties);
      const [shallowTypeValue, ctx2] = lookupShallowType(deepTypeValue, ctx1);

      return [{
        kind: "TypedDropTypeExpression",
        left,
        properties: e.properties,
        shallowTypeValue, // I don't this this is what I want! (Update: Maybe I don't remember why)
        deepTypeValue,
      }, ctx2];
    }
    case "LinearWithTypeExpression": {
      const [left, ctx1] = typeCheckTypeExpression(e.left, ctx);
      if (left.deepTypeValue.kind !== 'ObjectType')
        throws(`Left of join did not resolve to object type`);

      const ectx: Context = Object.fromEntries(left.deepTypeValue.properties.map(x => [x.name, x.type]));

      // TODO: maybe use a reduce if we want it to be more like let*
      const rules = e.rules.map<TypedRuleProperty>(r => {
        switch (r.kind) {
          case 'LinearRuleTypeProperty':
            return {
              kind: "TypedRuleTypeProperty",
              name: r.name,
              value: typeCheckTypeExpression(r.value, ctx)[0],
            };
          case 'LinearRuleValueProperty':
            return {
              kind: "TypedRuleValueProperty",
              name: r.name,
              value: typeCheckExpression(r.value, ectx)[0],
            };
        }
      });

      const deepTypeValue = mergeObjectTypes(left.deepTypeValue, objectType(...rules.map(r => {
        switch (r.kind) {
          case 'TypedRuleTypeProperty':
            return { name: r.name, type: r.value.deepTypeValue };
          case 'TypedRuleValueProperty':
            return { name: r.name, type: r.value.type };
        }
      })));
      const [shallowTypeValue, ctx2] = lookupShallowType(deepTypeValue, ctx1);

      return [{
        kind: "TypedWithTypeExpression",
        left,
        rules,
        deepTypeValue,
        shallowTypeValue,
      }, ctx2];
    }
    case "LinearUnionTypeExpression": {
      throws(`TODO typeCheckTypeExpression:LinearUnionTypeExpression`);
    }
    case 'LinearArrayTypeExpression': {
      const [of] = typeCheckTypeExpression(e.of, ctx);

      const shallowTypeValue = arrayType(of.shallowTypeValue);
      const deepTypeValue = arrayType(of.deepTypeValue);

      return [{ kind: "TypedArrayTypeExpression", of, shallowTypeValue, deepTypeValue }, ctx];
    }
    case "LinearGroupByTypeExpression": {
      const [left] = typeCheckTypeExpression(e.left, ctx);

      if (left.deepTypeValue.kind !== 'ObjectType')
        throws(`Error: Left side of group by expression needs to have an object type`);

      if (!hasProperty(left.deepTypeValue, e.column))
        throws(`Error: Cannot group by ${e.column} since it in not on the left side object`);

      const ectx: Context = {
        ...Object.fromEntries(left.deepTypeValue.properties.map(x => [x.name, x.type])),
        this: left.deepTypeValue,
        collect: unionType(
          functionType([integerType()], arrayType(integerType())),
          functionType([floatType()], arrayType(floatType())),
          functionType([stringType()], arrayType(stringType())),
          functionType([booleanType()], arrayType(booleanType())),
          functionType([left.deepTypeValue], arrayType(left.deepTypeValue)),
        ),
        sum: unionType(
          functionType([integerType()], integerType()),
          functionType([floatType()], floatType()),
        ),
        count: unionType(
          functionType([integerType()], integerType()),
          functionType([floatType()], integerType()),
          functionType([stringType()], integerType()),
          functionType([booleanType()], integerType()),
        ),
        max: unionType(
          functionType([integerType()], integerType()),
          functionType([floatType()], floatType()),
        ),
        min: unionType(
          functionType([integerType()], integerType()),
          functionType([floatType()], floatType()),
        ),
      };

      const aggregations = e.aggregations.map<TypedAggProperty>(p => {
        switch (p.kind) {
          case 'LinearAggProperty':
            return {
              kind: "TypedAggProperty",
              name: p.name,
              value: typeCheckExpression(p.value, ectx)[0],
            };
        }
      });

      const deepTypeValue = mergeObjectTypes(left.deepTypeValue, objectType(...aggregations.map(p => {
        switch (p.kind) {
          case 'TypedAggProperty':
            return { name: p.name, type: p.value.type };
        }
      })));
      const [shallowTypeValue, ctx1] = lookupShallowType(deepTypeValue, ctx);

      return [{
        kind: "TypedGroupByTypeExpression",
        left,
        column: e.column,
        aggregations,
        shallowTypeValue,
        deepTypeValue,
      }, ctx1];
    }
  }
}

export function typeCheckAllTypeExpressions(linearTypeExpressions: LinearTypeIntroExpression[]): [TypedTypeIntroExpression[], Context] {
  return linearTypeExpressions.reduce<[TypedTypeIntroExpression[], Context]>(([a, c], e) => {
    const [e1, c1] = typeCheckTypeExpression(e, c) as [TypedTypeIntroExpression, Context];
    return [[...a, e1], c1];
  }, [[], {}]);
}

