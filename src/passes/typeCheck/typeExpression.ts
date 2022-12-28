import { booleanType, floatType, IdentifierType, identifierType, integerType, ObjectType, stringType, Type } from "../../asts/type";
import { TypedObjectTypeExpression, TypedTypeExpression, TypedTypeIntroExpression } from "../../asts/typeExpression/typed";
import { LinearTypeExpression, LinearTypeIntroExpression } from "../../asts/typeExpression/linear";
import { throws, print } from "../../utils";
import { Context, typeEquals } from "./utils";

function resolve(name: string, ctx: Context): Exclude<Type, IdentifierType> {
  if (!(name in ctx))
    throws(`Type ${name} is not defined`);
  const type = ctx[name];
  return type.kind === 'IdentifierType' ? resolve(type.name, ctx) : type;
}

function mergeObjectTypes(...types: ObjectType[]): ObjectType {
  const props = new Set<string>();
  const properties: ObjectType['properties'] = [];

  types.forEach(type => type.properties.forEach(prop => {
    if (props.has(prop.name))
      throws(`Property ${prop.name} is already defined in object type merge`);
    props.add(prop.name);
    properties.push(prop);
  }));

  return {
    kind: "ObjectType",
    properties,
  };
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
      if (left.deepTypeValue.kind !== 'ObjectType')
        throws(`Left of join did not resolve to object type`);
      if (!hasProperty(left.deepTypeValue, e.leftColumn))
        throws(`Column ${e.leftColumn} is not defined on left type of join`);

      const [right, ctx2] = typeCheckTypeExpression(e.right, ctx1);
      if (right.deepTypeValue.kind !== 'ObjectType')
        throws(`Right of join did not resolve to object type`);
      if (!hasProperty(right.deepTypeValue, e.rightColumn))
        throws(`Column ${e.rightColumn} is not defined on right type of join`);

      const deepTypeValue = mergeObjectTypes(left.deepTypeValue, right.deepTypeValue);
      const [shallowTypeValue, ctx3] = lookupShallowType(deepTypeValue, ctx2);

      return [{
        kind: "TypedJoinTypeExpression",
        left,
        right,
        leftColumn: e.leftColumn,
        rightColumn: e.rightColumn,
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
        shallowTypeValue, // I don't this this is what I want!
        deepTypeValue,
      }, ctx2];
    }
    case "LinearWithTypeExpression": {
      throws(`TODO typeCheckTypeExpression:LinearWithTypeExpression`);
    }
    case "LinearUnionTypeExpression": {
      // return [{}, ctx];
      throws(`TODO typeCheckTypeExpression:LinearUnionTypeExpression`);
    }
  }
}

export function typeCheckAllTypeExpressions(linearTypeExpressions: LinearTypeIntroExpression[]): [TypedTypeIntroExpression[], Context] {
  return linearTypeExpressions.reduce<[TypedTypeIntroExpression[], Context]>(([a, c], e) => {
    const [e1, c1] = typeCheckTypeExpression(e, c) as [TypedTypeIntroExpression, Context];
    return [[...a, e1], c1];
  }, [[], {}]);
}

