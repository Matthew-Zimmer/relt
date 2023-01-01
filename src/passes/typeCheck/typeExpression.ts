import { arrayType, booleanType, fkType, floatType, ForeignKeyType, functionType, IdentifierType, identifierType, integerType, objectType, ObjectType, optionalType, pkType, PrimaryKeyType, stringType, Type, unionType } from "../../asts/type";
import { NamedTypedExpression, TypedIdentifierTypeExpression, TypedIntegerTypeExpression, TypedJoinTypeExpression, TypedObjectTypeExpression, TypedStringTypeExpression, TypedTypeExpression, TypedTypeIntroExpression } from "../../asts/typeExpression/typed";
import { FlatTypeExpression, FlatTypeIntroExpression } from "../../asts/typeExpression/flat";
import { throws, print } from "../../utils";
import { Context, typeEquals } from "./utils";
import { typeCheckExpression } from "./expression";
import { TypedIdentifierExpression } from "../../asts/expression/typed";

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
      props.set(prop.name, properties.length);
      properties.push(prop);
    }
  }));

  return {
    kind: "ObjectType",
    properties,
  };
}

function makeOptional(t: ObjectType): ObjectType {
  return objectType(...t.properties.map(x => ({ name: x.name, type: optionalType(x.type) })));
}

function mergeJoinTypes(method: TypedJoinTypeExpression['method'], l: ObjectType, r: ObjectType, rCol: string): ObjectType {
  const base = (() => {
    switch (method) {
      case 'inner':
        return mergeObjectTypes(l, r);
      case 'left':
        return mergeObjectTypes(l, makeOptional(r));
      case 'right':
        return mergeObjectTypes(makeOptional(l), r);
      case 'outer':
        return mergeObjectTypes(makeOptional(l), makeOptional(r));
    }
  })();
  return removeProperties(base, [rCol]);
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

function relationsFor(l: ObjectType, r: ObjectType, ctx: Context): [string, string][] {
  const lfKeys = foreignKeys(l);
  const rfKeys = foreignKeys(r);
  const lRelations = lfKeys.filter(x => typeEquals(ctx[x.type.table], r)).map<[string, string]>(x => [x.name, x.type.column]);
  const rRelations = rfKeys.filter(x => typeEquals(ctx[x.type.table], l)).map<[string, string]>(x => [x.type.column, x.name]);
  return [lRelations, rRelations].flat();
}

function checkJoinRelation(l: ObjectType, r: ObjectType, relation: [string | undefined, string | undefined], ctx: Context): [string, string] {
  const relations = relationsFor(l, r, ctx);

  const lCol = relation[0] ?? (relations.length === 1 ? relations[0][0] : throws(`For relation __ you must provide an explicit on clause`));
  const rCol = relation[1] ?? (relations.length === 1 ? relations[0][1] : throws(`For relation __ you must provide an explicit on clause`));

  if (lCol && !hasProperty(l, lCol))
    throws(`Column ${lCol} is not defined on left type of join`);
  if (rCol && !hasProperty(r, rCol))
    throws(`Column ${rCol} is not defined on right type of join`);

  return [lCol, rCol];
}

export function isTypeIdentifierOf<K extends Type['kind']>(e: TypedTypeExpression, kind: K): e is TypedIdentifierTypeExpression<Type & { kind: K }> {
  return e.kind === 'TypedIdentifierTypeExpression' && e.type.kind === kind;
}

export function typeCheckTypeExpression(e: FlatTypeExpression, ctx: Context): [TypedTypeExpression, Context] {
  switch (e.kind) {
    case "FlatIntegerTypeExpression": {
      return [{ kind: "TypedIntegerTypeExpression", type: integerType() }, ctx];
    }
    case "FlatFloatTypeExpression": {
      return [{ kind: "TypedFloatTypeExpression", type: floatType() }, ctx];
    }
    case "FlatBooleanTypeExpression": {
      return [{ kind: "TypedBooleanTypeExpression", type: booleanType() }, ctx];
    }
    case "FlatStringTypeExpression": {
      return [{ kind: "TypedStringTypeExpression", type: stringType() }, ctx];
    }
    case "FlatPrimaryKeyTypeExpression": {
      const of = typeCheckTypeExpression(e.of, ctx)[0] as TypedIntegerTypeExpression | TypedStringTypeExpression;
      return [{ kind: "TypedPrimaryKeyTypeExpression", of, type: pkType(of.type) }, ctx];
    }
    case "FlatForeignKeyTypeExpression": {
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

      return [{ kind: "TypedForeignKeyTypeExpression", table: e.table, column: e.column, type: tyv }, ctx];
    }
    case "FlatObjectTypeExpression": {
      const properties = e.properties.map(x => ({ name: x.name, value: typeCheckTypeExpression(x.value, ctx)[0] }));

      return [{
        kind: "TypedObjectTypeExpression",
        properties,
        type: objectType(...properties.map(x => ({ name: x.name, type: x.value.type }))),
      }, ctx];
    }

    case "FlatIdentifierTypeExpression": {
      return [{ kind: "TypedIdentifierTypeExpression", name: e.name, type: resolve(e.name, ctx) }, ctx];
    }
    case "FlatTypeIntroExpression": {
      const [value] = typeCheckTypeExpression(e.value, ctx);
      return [{ kind: "TypedTypeIntroExpression", name: e.name, value, type: value.type }, { ...ctx, [e.name]: value.type }];
    }
    case "FlatJoinTypeExpression": {
      const [left] = typeCheckTypeExpression(e.left, ctx);
      const [right] = typeCheckTypeExpression(e.right, ctx);

      if (!isTypeIdentifierOf(left, 'ObjectType'))
        throws(`Bad Identifier Type`);
      if (!isTypeIdentifierOf(right, 'ObjectType'))
        throws(`Bad Identifier Type`);

      const [leftColumn, rightColumn] = checkJoinRelation(left.type, right.type, [e.leftColumn, e.rightColumn], ctx);

      const type = mergeJoinTypes(e.method, left.type, right.type, rightColumn);

      return [{
        kind: "TypedJoinTypeExpression",
        left,
        right,
        leftColumn,
        rightColumn,
        type,
        method: e.method,
      }, ctx];
    }
    case "FlatDropTypeExpression": {
      const [left] = typeCheckTypeExpression(e.left, ctx);

      if (!isTypeIdentifierOf(left, 'ObjectType'))
        throws(`Bad Identifier Type`);

      return [{
        kind: "TypedDropTypeExpression",
        left,
        properties: e.properties,
        type: removeProperties(left.type, e.properties),
      }, ctx];
    }
    case "FlatWithTypeExpression": {
      const [left] = typeCheckTypeExpression(e.left, ctx);

      if (!isTypeIdentifierOf(left, 'ObjectType'))
        throws(`Bad Identifier Type`);

      const ectx: Context = Object.fromEntries(left.type.properties.map(x => [x.name, x.type]));

      // TODO: maybe use a reduce if we want it to be more like let*
      const rules = e.rules.map<NamedTypedExpression>(r => {
        return {
          name: r.name,
          value: typeCheckExpression(r.value, ectx)[0],
        };
      });

      const type = mergeObjectTypes(left.type, objectType(...rules.map(r => {
        return { name: r.name, type: r.value.type };
      })));

      return [{
        kind: "TypedWithTypeExpression",
        left,
        rules,
        type,
      }, ctx];
    }
    case "FlatUnionTypeExpression": {
      throws(`TODO typeCheckTypeExpression:FlatUnionTypeExpression`);
    }
    case 'FlatArrayTypeExpression': {
      const [of] = typeCheckTypeExpression(e.of, ctx);

      return [{ kind: "TypedArrayTypeExpression", of, type: arrayType(of.type) }, ctx];
    }
    case "FlatGroupByTypeExpression": {
      const [left] = typeCheckTypeExpression(e.left, ctx);

      if (!isTypeIdentifierOf(left, 'ObjectType'))
        throws(`Bad Identifier Type`);

      if (!hasProperty(left.type, e.column))
        throws(`Error: Cannot group by ${e.column} since it in not on the left side object`);

      const ectx: Context = {
        ...Object.fromEntries(left.type.properties.map(x => [x.name, x.type])),
        this: left.type,
        collect: unionType(
          functionType([integerType()], arrayType(integerType())),
          functionType([floatType()], arrayType(floatType())),
          functionType([stringType()], arrayType(stringType())),
          functionType([booleanType()], arrayType(booleanType())),
          functionType([left.type], arrayType(left.type)),
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

      const aggregations = e.aggregations.map(p => {
        return {
          name: p.name,
          value: typeCheckExpression(p.value, ectx)[0],
        };
      });

      const type = mergeObjectTypes(objectType({ name: e.column, type: left.type.properties.find(x => x.name === e.column)!.type }), objectType(...aggregations.map(p => {
        return { name: p.name, type: p.value.type };
      })));

      return [{
        kind: "TypedGroupByTypeExpression",
        left,
        column: e.column,
        aggregations,
        type,
      }, ctx];
    }
  }
}

export function typeCheckAllTypeExpressions(flatTypeExpressions: FlatTypeIntroExpression[]): [TypedTypeIntroExpression[], Context] {
  return flatTypeExpressions.reduce<[TypedTypeIntroExpression[], Context]>(([a, c], e) => {
    const [e1, c1] = typeCheckTypeExpression(e, c) as [TypedTypeIntroExpression, Context];
    return [[...a, e1], c1];
  }, [[], {}]);
}

