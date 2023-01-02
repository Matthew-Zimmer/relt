import { arrayType, booleanType, fkType, floatType, ForeignKeyType, functionType, IdentifierType, identifierType, integerType, objectType, ObjectType, optionalType, pkType, PrimaryKeyType, stringType, Type, unionType } from "../../asts/type";
import { NamedTypedExpression, TypedIdentifierTypeExpression, TypedIntegerTypeExpression, TypedJoinTypeExpression, TypedObjectTypeExpression, TypedStringTypeExpression, TypedTypeExpression, TypedTypeIntroExpression } from "../../asts/typeExpression/typed";
import { FlatTypeExpression, FlatTypeIntroExpression } from "../../asts/typeExpression/flat";
import { throws, print } from "../../utils";
import { Context, typeEquals } from "./utils";
import { typeCheckExpression } from "./expression";
import { TypedIdentifierExpression } from "../../asts/expression/typed";
import { Id } from "../../asts/typeExpression/util";
import { typeName } from "../evaluate";

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

const expressionTypeMap = new Map<Id, Type>();

export function lookupExpressionTypeById(id: Id): Type {
  const ty = expressionTypeMap.get(id);
  if (ty === undefined)
    throws(`Internal Error: Cannot lookup the type for expression with id ${id}`);
  return ty;
}

export function setExprType(id: Id, ty: Type) {
  if (expressionTypeMap.has(id) && !typeEquals(ty, expressionTypeMap.get(id)!))
    throws(`Internal Error: Cannot set the type for expression with id ${id} already defined old ${typeName(expressionTypeMap.get(id)!)} new ${typeName(ty)}`);
  expressionTypeMap.set(id, ty);
}

export function ensure<K extends Type['kind']>(ty: Type, kind: K): Type & { kind: K } {
  if (ty.kind !== kind) throws(`Internal Error: Expected ${kind} got ${ty.kind}`);
  return ty as Type & { kind: K };
}

export function typeCheckTypeExpression(e: FlatTypeExpression, ctx: Context, id: Id): [TypedTypeExpression, Context, Id] {
  let idCount = id;
  const typeCheck = (e: FlatTypeExpression, ctx: Context): [TypedTypeExpression, Context] => {
    switch (e.kind) {
      case "FlatIntegerTypeExpression": {
        const type = integerType();
        return [{ kind: "TypedIntegerTypeExpression", type, id: e.id }, ctx];
      }
      case "FlatFloatTypeExpression": {
        const type = floatType();
        setExprType(e.id, type);
        return [{ kind: "TypedFloatTypeExpression", type, id: e.id }, ctx];
      }
      case "FlatBooleanTypeExpression": {
        const type = booleanType();
        setExprType(e.id, type);
        return [{ kind: "TypedBooleanTypeExpression", type, id: e.id }, ctx];
      }
      case "FlatStringTypeExpression": {
        const type = stringType();
        setExprType(e.id, type);
        return [{ kind: "TypedStringTypeExpression", type, id: e.id }, ctx];
      }
      case "FlatPrimaryKeyTypeExpression": {
        const of = typeCheck(e.of, ctx)[0] as TypedIntegerTypeExpression | TypedStringTypeExpression;
        const type = pkType(of.type);
        setExprType(e.id, type);
        return [{ kind: "TypedPrimaryKeyTypeExpression", of, type, id: e.id }, ctx];
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

        setExprType(e.id, tyv);

        return [{ kind: "TypedForeignKeyTypeExpression", table: e.table, column: e.column, type: tyv, id: e.id }, ctx];
      }
      case "FlatObjectTypeExpression": {
        const properties = e.properties.map(x => ({ name: x.name, value: typeCheck(x.value, ctx)[0] }));

        const type = objectType(...properties.map(x => ({ name: x.name, type: x.value.type })));
        setExprType(e.id, type);

        return [{
          kind: "TypedObjectTypeExpression",
          properties,
          type,
          id: e.id,
        }, ctx];
      }

      case "FlatIdentifierTypeExpression": {
        const type = resolve(e.name, ctx);
        setExprType(e.id, type);
        return [{ kind: "TypedIdentifierTypeExpression", name: e.name, type, id: e.id }, ctx];
      }
      case "FlatTypeIntroExpression": {
        const [value] = typeCheck(e.value, ctx);
        const type = value.type;
        setExprType(e.id, type);
        return [{ kind: "TypedTypeIntroExpression", name: e.name, value, type, id: e.id }, { ...ctx, [e.name]: value.type }];
      }
      case "FlatJoinTypeExpression": {
        const [left] = typeCheck(e.left, ctx);
        const [right] = typeCheck(e.right, ctx);

        if (!isTypeIdentifierOf(left, 'ObjectType'))
          throws(`Bad Identifier Type`);
        if (!isTypeIdentifierOf(right, 'ObjectType'))
          throws(`Bad Identifier Type`);

        const [leftColumn, rightColumn] = checkJoinRelation(left.type, right.type, [e.leftColumn, e.rightColumn], ctx);

        const type = mergeJoinTypes(e.method, left.type, right.type, rightColumn);
        setExprType(e.id, type);

        return [{
          kind: "TypedJoinTypeExpression",
          left,
          right,
          leftColumn,
          rightColumn,
          type,
          method: e.method,
          id: e.id,
        }, ctx];
      }
      case "FlatDropTypeExpression": {
        const [left] = typeCheck(e.left, ctx);

        if (!isTypeIdentifierOf(left, 'ObjectType'))
          throws(`Bad Identifier Type`);

        const type = removeProperties(left.type, e.properties)
        setExprType(e.id, type);

        return [{
          kind: "TypedDropTypeExpression",
          left,
          properties: e.properties,
          type,
          id: e.id,
        }, ctx];
      }
      case "FlatWithTypeExpression": {
        const [left] = typeCheck(e.left, ctx);

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
        setExprType(e.id, type);

        return [{
          kind: "TypedWithTypeExpression",
          left,
          rules,
          type,
          id: e.id,
        }, ctx];
      }
      case "FlatUnionTypeExpression": {
        throws(`TODO typeCheckTypeExpression:FlatUnionTypeExpression`);
      }
      case 'FlatArrayTypeExpression': {
        const [of] = typeCheck(e.of, ctx);
        const type = arrayType(of.type)
        setExprType(e.id, type);

        return [{ kind: "TypedArrayTypeExpression", of, type, id: e.id }, ctx];
      }
      case "FlatGroupByTypeExpression": {
        const [left] = typeCheck(e.left, ctx);

        if (!isTypeIdentifierOf(left, 'ObjectType'))
          throws(`Bad Identifier Type`);

        const column: string = typeof e.column === 'string' ? e.column : checkJoinRelation(
          ensure(lookupExpressionTypeById(e.column[0]), 'ObjectType'),
          ensure(lookupExpressionTypeById(e.column[1]), 'ObjectType'),
          [undefined, undefined],
          ctx,
        )[1];

        if (!hasProperty(left.type, column))
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

        const type = mergeObjectTypes(objectType({ name: column, type: left.type.properties.find(x => x.name === column)!.type }), objectType(...aggregations.map(p => {
          return { name: p.name, type: p.value.type };
        })));
        setExprType(e.id, type);

        return [{
          kind: "TypedGroupByTypeExpression",
          left,
          column,
          aggregations,
          type,
          id: e.id,
        }, ctx];
      }
    }
  }

  return [...typeCheck(e, ctx), idCount];
}

export function typeCheckAllTypeExpressions(flatTypeExpressions: FlatTypeIntroExpression[], id: Id): [TypedTypeIntroExpression[], Context, Id] {
  return flatTypeExpressions.reduce<[TypedTypeIntroExpression[], Context, Id]>(([a, c, id], e) => {
    const [e1, c1, id1] = typeCheckTypeExpression(e, c, id) as [TypedTypeIntroExpression, Context, Id];
    return [[...a, e1], c1, id1];
  }, [[], {}, id]);
}

