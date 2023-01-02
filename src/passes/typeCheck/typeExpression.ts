import { arrayType, booleanType, fkType, floatType, ForeignKeyType, functionType, integerType, optionalType, pkType, stringType, StructType, structType, Type, unionType } from "../../asts/type";
import { TypedAggProperty, TypedIdentifierTypeExpression, TypedIntegerTypeExpression, TypedJoinTypeExpression, TypedRuleProperty, TypedStringTypeExpression, TypedStructLikeTypeExpression, TypedTypeExpression, TypedTypeIntroExpression } from "../../asts/typeExpression/typed";
import { throws, print } from "../../utils";
import { Context, typeEquals } from "./utils";
import { typeCheckExpression } from "./expression";
import { typeName } from "../evaluate";
import { AggProperty, JoinTypeExpression, RuleProperty, TypeExpression } from "../../asts/typeExpression/untyped";
import { TypedExpression, TypedIntegerExpression } from "../../asts/expression/typed";

// function resolve(name: string, ctx: Context): Exclude<Type, IdentifierType> {
//   if (!(name in ctx))
//     throws(`Type ${name} is not defined`);
//   const type = ctx[name];
//   return type.kind === 'IdentifierType' ? resolve(type.name, ctx) : type;
// }

// function mergeObjectTypes(...types: ObjectType[]): ObjectType {
//   const props = new Map<string, number>();
//   const properties: ObjectType['properties'] = [];

//   types.forEach(type => type.properties.forEach(prop => {
//     if (props.has(prop.name)) {
//       properties[props.get(prop.name)!] = prop;
//     }
//     else {
//       props.set(prop.name, properties.length);
//       properties.push(prop);
//     }
//   }));

//   return {
//     kind: "ObjectType",
//     properties,
//   };
// }

// function makeOptional(t: ObjectType): ObjectType {
//   return objectType(...t.properties.map(x => ({ name: x.name, type: optionalType(x.type) })));
// }

// function mergeJoinTypes(method: TypedJoinTypeExpression['method'], l: ObjectType, r: ObjectType, rCol: string): ObjectType {
//   const base = (() => {
//     switch (method) {
//       case 'inner':
//         return mergeObjectTypes(l, r);
//       case 'left':
//         return mergeObjectTypes(l, makeOptional(r));
//       case 'right':
//         return mergeObjectTypes(makeOptional(l), r);
//       case 'outer':
//         return mergeObjectTypes(makeOptional(l), makeOptional(r));
//     }
//   })();
//   return removeProperties(base, [rCol]);
// }

// function makeNewNamedType(ty: ObjectType, ctx: Context): [IdentifierType, Context] {
//   const name = `T_IntermediateType_${Object.keys(ctx).length}`;
//   return [identifierType(name), { ...ctx, [name]: ty }];
// }

// function lookupShallowType(ty: ObjectType, ctx: Context): [IdentifierType, Context] {
//   const options = Object.entries(ctx).filter(([, v]) => typeEquals(ty, v));
//   return options.length === 0 ? makeNewNamedType(ty, ctx) : [identifierType(options[0][0]), ctx];
// }

// function removeProperties(ty: ObjectType, propertyNames: string[]): ObjectType {
//   const props = new Map<string, Type>(ty.properties.map(x => [x.name, x.type]));

//   propertyNames.forEach(prop => {
//     if (!(props.has(prop)))
//       throws(`Property ${prop} is not defined in object type`);
//     props.delete(prop);
//   });

//   return {
//     kind: "ObjectType",
//     properties: [...props.entries()].map(x => ({ name: x[0], type: x[1] })),
//   };
// }

// function hasProperty(ty: ObjectType, name: string): boolean {
//   return ty.properties.some(x => x.name === name);
// }

// function foreignKeys(t: ObjectType) {
//   return t.properties.filter((x): x is { name: string, type: ForeignKeyType } => x.type.kind === 'ForeignKeyType')
// }

// function relationsFor(l: ObjectType, r: ObjectType, ctx: Context): [string, string][] {
//   const lfKeys = foreignKeys(l);
//   const rfKeys = foreignKeys(r);
//   const lRelations = lfKeys.filter(x => typeEquals(ctx[x.type.table], r)).map<[string, string]>(x => [x.name, x.type.column]);
//   const rRelations = rfKeys.filter(x => typeEquals(ctx[x.type.table], l)).map<[string, string]>(x => [x.type.column, x.name]);
//   return [lRelations, rRelations].flat();
// }

// function checkJoinRelation(l: ObjectType, r: ObjectType, relation: [string | undefined, string | undefined], ctx: Context): [string, string] {
//   const relations = relationsFor(l, r, ctx);

//   const lCol = relation[0] ?? (relations.length === 1 ? relations[0][0] : throws(`For relation __ you must provide an explicit on clause`));
//   const rCol = relation[1] ?? (relations.length === 1 ? relations[0][1] : throws(`For relation __ you must provide an explicit on clause`));

//   if (lCol && !hasProperty(l, lCol))
//     throws(`Column ${lCol} is not defined on left type of join`);
//   if (rCol && !hasProperty(r, rCol))
//     throws(`Column ${rCol} is not defined on right type of join`);

//   return [lCol, rCol];
// }

// export function isTypeIdentifierOf<K extends Type['kind']>(e: TypedTypeExpression, kind: K): e is TypedIdentifierTypeExpression<Type & { kind: K }> {
//   return e.kind === 'TypedIdentifierTypeExpression' && e.type.kind === kind;
// }

// const expressionTypeMap = new Map<Id, Type>();

// export function lookupExpressionTypeById(id: Id): Type {
//   const ty = expressionTypeMap.get(id);
//   if (ty === undefined)
//     throws(`Internal Error: Cannot lookup the type for expression with id ${id}`);
//   return ty;
// }

// export function setExprType(id: Id, ty: Type) {
//   if (expressionTypeMap.has(id) && !typeEquals(ty, expressionTypeMap.get(id)!))
//     throws(`Internal Error: Cannot set the type for expression with id ${id} already defined old ${typeName(expressionTypeMap.get(id)!)} new ${typeName(ty)}`);
//   expressionTypeMap.set(id, ty);
// }

// export function ensure<K extends Type['kind']>(ty: Type, kind: K): Type & { kind: K } {
//   if (ty.kind !== kind) throws(`Internal Error: Expected ${kind} got ${ty.kind}`);
//   return ty as Type & { kind: K };
// }

// export function typeCheckTypeExpression(e: FlatTypeExpression, ctx: Context, id: Id): [TypedTypeExpression, Context, Id] {
//   let idCount = id;
//   const typeCheck = (e: FlatTypeExpression, ctx: Context): [TypedTypeExpression, Context] => {
//     switch (e.kind) {
//       case "FlatIntegerTypeExpression": {
//         const type = integerType();
//         return [{ kind: "TypedIntegerTypeExpression", type, id: e.id }, ctx];
//       }
//       case "FlatFloatTypeExpression": {
//         const type = floatType();
//         setExprType(e.id, type);
//         return [{ kind: "TypedFloatTypeExpression", type, id: e.id }, ctx];
//       }
//       case "FlatBooleanTypeExpression": {
//         const type = booleanType();
//         setExprType(e.id, type);
//         return [{ kind: "TypedBooleanTypeExpression", type, id: e.id }, ctx];
//       }
//       case "FlatStringTypeExpression": {
//         const type = stringType();
//         setExprType(e.id, type);
//         return [{ kind: "TypedStringTypeExpression", type, id: e.id }, ctx];
//       }
//       case "FlatPrimaryKeyTypeExpression": {
//         const of = typeCheck(e.of, ctx)[0] as TypedIntegerTypeExpression | TypedStringTypeExpression;
//         const type = pkType(of.type);
//         setExprType(e.id, type);
//         return [{ kind: "TypedPrimaryKeyTypeExpression", of, type, id: e.id }, ctx];
//       }
//       case "FlatForeignKeyTypeExpression": {
//         if (!(e.table in ctx))
//           throws(`Error: unknown type ${e.table}`);

//         const ty = ctx[e.table];

//         if (ty.kind !== 'ObjectType')
//           throws(`Error: ${e.table} is not an object`);

//         const prop = ty.properties.find(x => x.name === e.column);

//         if (prop === undefined)
//           throws(`Error: ${e.table} does not contain property ${e.column}`);

//         if (prop.type.kind !== "IntegerType" && prop.type.kind !== 'StringType' && prop.type.kind !== 'PrimaryKeyType' && prop.type.kind !== 'ForeignKeyType')
//           throws(`Error: Cannot have a foreign key to a non integer or string field`);

//         const tyv = fkType(e.table, e.column, prop.type);

//         setExprType(e.id, tyv);

//         return [{ kind: "TypedForeignKeyTypeExpression", table: e.table, column: e.column, type: tyv, id: e.id }, ctx];
//       }
//       case "FlatObjectTypeExpression": {
//         const properties = e.properties.map(x => ({ name: x.name, value: typeCheck(x.value, ctx)[0] }));

//         const type = objectType(...properties.map(x => ({ name: x.name, type: x.value.type })));
//         setExprType(e.id, type);

//         return [{
//           kind: "TypedObjectTypeExpression",
//           properties,
//           type,
//           id: e.id,
//         }, ctx];
//       }

//       case "FlatIdentifierTypeExpression": {
//         const type = resolve(e.name, ctx);
//         setExprType(e.id, type);
//         return [{ kind: "TypedIdentifierTypeExpression", name: e.name, type, id: e.id }, ctx];
//       }
//       case "FlatTypeIntroExpression": {
//         const [value] = typeCheck(e.value, ctx);
//         const type = value.type;
//         setExprType(e.id, type);
//         return [{ kind: "TypedTypeIntroExpression", name: e.name, value, type, id: e.id }, { ...ctx, [e.name]: value.type }];
//       }
//       case "FlatJoinTypeExpression": {
//         const [left] = typeCheck(e.left, ctx);
//         const [right] = typeCheck(e.right, ctx);

//         if (!isTypeIdentifierOf(left, 'ObjectType'))
//           throws(`Bad Identifier Type`);
//         if (!isTypeIdentifierOf(right, 'ObjectType'))
//           throws(`Bad Identifier Type`);

//         const [leftColumn, rightColumn] = checkJoinRelation(left.type, right.type, [e.leftColumn, e.rightColumn], ctx);

//         const type = mergeJoinTypes(e.method, left.type, right.type, rightColumn);
//         setExprType(e.id, type);

//         return [{
//           kind: "TypedJoinTypeExpression",
//           left,
//           right,
//           leftColumn,
//           rightColumn,
//           type,
//           method: e.method,
//           id: e.id,
//         }, ctx];
//       }
//       case "FlatDropTypeExpression": {
//         const [left] = typeCheck(e.left, ctx);

//         if (!isTypeIdentifierOf(left, 'ObjectType'))
//           throws(`Bad Identifier Type`);

//         const type = removeProperties(left.type, e.properties)
//         setExprType(e.id, type);

//         return [{
//           kind: "TypedDropTypeExpression",
//           left,
//           properties: e.properties,
//           type,
//           id: e.id,
//         }, ctx];
//       }
//       case "FlatWithTypeExpression": {
//         const [left] = typeCheck(e.left, ctx);

//         if (!isTypeIdentifierOf(left, 'ObjectType'))
//           throws(`Bad Identifier Type`);

//         const ectx: Context = Object.fromEntries(left.type.properties.map(x => [x.name, x.type]));

//         // TODO: maybe use a reduce if we want it to be more like let*
//         const rules = e.rules.map<NamedTypedExpression>(r => {
//           return {
//             name: r.name,
//             value: typeCheckExpression(r.value, ectx)[0],
//           };
//         });

//         const type = mergeObjectTypes(left.type, objectType(...rules.map(r => {
//           return { name: r.name, type: r.value.type };
//         })));
//         setExprType(e.id, type);

//         return [{
//           kind: "TypedWithTypeExpression",
//           left,
//           rules,
//           type,
//           id: e.id,
//         }, ctx];
//       }
//       case "FlatUnionTypeExpression": {
//         const [left] = typeCheck(e.left, ctx);
//         const [right] = typeCheck(e.right, ctx);

//         if (!isTypeIdentifierOf(left, 'ObjectType'))
//           throws(`Bad Identifier Type`);
//         if (!isTypeIdentifierOf(right, 'ObjectType'))
//           throws(`Bad Identifier Type`);

//         if (!typeEquals(left.type, right.type))
//           throws(`Error: Union left and right type must match`);

//         const type = left.type;
//         setExprType(e.id, type);

//         return [{
//           kind: "TypedUnionTypeExpression",
//           left,
//           right,
//           type,
//           id: e.id,
//         }, ctx];
//       }
//       case 'FlatArrayTypeExpression': {
//         const [of] = typeCheck(e.of, ctx);
//         const type = arrayType(of.type)
//         setExprType(e.id, type);

//         return [{ kind: "TypedArrayTypeExpression", of, type, id: e.id }, ctx];
//       }
//       case "FlatGroupByTypeExpression": {
//         const [left] = typeCheck(e.left, ctx);

//         if (!isTypeIdentifierOf(left, 'ObjectType'))
//           throws(`Bad Identifier Type`);

//         const column: string = typeof e.column === 'string' ? e.column : checkJoinRelation(
//           ensure(lookupExpressionTypeById(e.column[0]), 'ObjectType'),
//           ensure(lookupExpressionTypeById(e.column[1]), 'ObjectType'),
//           [undefined, undefined],
//           ctx,
//         )[1];

//         if (!hasProperty(left.type, column))
//           throws(`Error: Cannot group by ${e.column} since it in not on the left side object`);

//         const ectx: Context = {
//           ...Object.fromEntries(left.type.properties.map(x => [x.name, x.type])),
//           this: left.type,
//           collect: unionType(
//             functionType([integerType()], arrayType(integerType())),
//             functionType([floatType()], arrayType(floatType())),
//             functionType([stringType()], arrayType(stringType())),
//             functionType([booleanType()], arrayType(booleanType())),
//             functionType([left.type], arrayType(left.type)),
//           ),
//           sum: unionType(
//             functionType([integerType()], integerType()),
//             functionType([floatType()], floatType()),
//           ),
//           count: unionType(
//             functionType([integerType()], integerType()),
//             functionType([floatType()], integerType()),
//             functionType([stringType()], integerType()),
//             functionType([booleanType()], integerType()),
//           ),
//           max: unionType(
//             functionType([integerType()], integerType()),
//             functionType([floatType()], floatType()),
//           ),
//           min: unionType(
//             functionType([integerType()], integerType()),
//             functionType([floatType()], floatType()),
//           ),
//         };

//         const aggregations = e.aggregations.map(p => {
//           return {
//             name: p.name,
//             value: typeCheckExpression(p.value, ectx)[0],
//           };
//         });

//         const type = mergeObjectTypes(objectType({ name: column, type: left.type.properties.find(x => x.name === column)!.type }), objectType(...aggregations.map(p => {
//           return { name: p.name, type: p.value.type };
//         })));
//         setExprType(e.id, type);

//         return [{
//           kind: "TypedGroupByTypeExpression",
//           left,
//           column,
//           aggregations,
//           type,
//           id: e.id,
//         }, ctx];
//       }
//     }
//   }

//   return [...typeCheck(e, ctx), idCount];
// }

// export function typeCheckAllTypeExpressions(flatTypeExpressions: FlatTypeIntroExpression[], id: Id): [TypedTypeIntroExpression[], Context, Id] {
//   return flatTypeExpressions.reduce<[TypedTypeIntroExpression[], Context, Id]>(([a, c, id], e) => {
//     const [e1, c1, id1] = typeCheckTypeExpression(e, c, id) as [TypedTypeIntroExpression, Context, Id];
//     return [[...a, e1], c1, id1];
//   }, [[], {}, id]);
// }

function isStructLikeTypeExpression(e: TypedTypeExpression): e is TypedStructLikeTypeExpression {
  return e.type.kind === 'StructType';
}

function canOperateLikeStruct(e: TypedTypeExpression, prefix: string): asserts e is TypedStructLikeTypeExpression {
  if (!isStructLikeTypeExpression(e))
    throws(`Error: ${prefix} is not a struct type (${e.type.kind})`);
  if (e.type.name === '')
    throws(`Error: ${prefix} is an anonymous struct type`);
}

export function mergeProperties<T>(props: StructType['properties'][], onHit: (key: string, oTy: Type, nTy: Type) => T): StructType['properties'] {
  const pMap = new Map<string, Type>();
  props.forEach(p => p.forEach(({ name, type }) => {
    if (pMap.has(name))
      onHit(name, pMap.get(name)!, type);
    pMap.set(name, type);
  }));
  return [...pMap.entries()].map(x => ({ name: x[0], type: x[1] }));
}

export function optionalProperties(props: StructType['properties']): StructType['properties'] {
  return props.map(x => ({ name: x.name, type: optionalType(x.type) }));
}

export function dropProperties(props: StructType['properties'], names: string[]): StructType['properties'] {
  return props.filter(x => !names.includes(x.name));
}

export function propertyLookup(t: StructType, name: string): Type | undefined {
  return t.properties.find(x => x.name === name)?.type;
}

export function assertContainsProperty(t: StructType, name: string) {
  if (!propertyLookup(t, name))
    throws(`Error: ${name} does not exist in struct type ${t.name}`);
}

export function joinType(method: JoinTypeExpression['method'], left: StructType, lCol: string, right: StructType, rCol: string): StructType {
  const name = `Relt_${left.name}${method}Join${right.name}`;
  const lp = left.properties;
  const rp = dropProperties(right.properties, [rCol]);

  const onHit = (name: string) => throws(`Error: Duplicate keys ${name} when joining ${left.name} and ${right.name}`);

  switch (method) {
    case "inner":
      return structType(name, mergeProperties([lp, rp], onHit));
    case "left":
      return structType(name, mergeProperties([lp, optionalProperties(rp)], onHit));
    case "right":
      return structType(name, mergeProperties([optionalProperties(lp), rp], onHit));
    case "outer":
      return structType(name, mergeProperties([optionalProperties(lp), optionalProperties(rp)], onHit));
  }
}

export function dropType(left: StructType, props: string[]): StructType {
  for (const p of props) assertContainsProperty(left, p);
  const name = `Relt_${left.name}Drop${props.join('_')}`;
  return structType(name, dropProperties(left.properties, props));
}

export function withType(left: StructType, rules: TypedRuleProperty[]): StructType {
  const props = rules.map(rule => {
    switch (rule.kind) {
      case "TypedRuleTypeProperty":
      case "TypedRuleValueProperty":
        return { name: rule.name, type: rule.value.type };
    }
  });
  const name = `Relt_${left.name}With${props.map(x => x.name).join('_')}`;

  const onHit = (name: string, oTy: Type, nTy: Type) => { };

  return structType(name, mergeProperties([left.properties, props], onHit));
}

export function unionStructType(left: StructType, right: StructType): StructType {
  if (!typeEquals(structType("", left.properties), structType("", right.properties)))
    throws(`Error: Cannot union ${left.name} and ${right.name} since their properties do not match`);
  const name = `Relt_${left.name}Union${right.name}`;
  return structType(name, left.properties);
}

export function groupByType(left: StructType, column: string, aggs: TypedAggProperty[]): StructType {
  const colType = propertyLookup(left, column);
  if (colType === undefined)
    throws(`Error: Cannot aggregate ${left.name} on "${column}" since that is a non existent property`);

  const props = aggs.map(agg => {
    switch (agg.kind) {
      case "TypedAggProperty":
        return { name: agg.name, type: agg.value.type };
    }
  });
  const name = `Relt_${left.name}Group${column}${props.map(x => x.name).join('_')}`;

  const onHit = (name: string, oTy: Type, nTy: Type) => {
    if (!typeEquals(oTy, nTy))
      throws(`Error: Duplicate key "${name}" with non equal type when applied with to ${left.name}`);
  }

  return structType(name, mergeProperties([[{ name: column, type: colType }], props], onHit));
}

function foreignKeys(t: StructType): [string, ForeignKeyType][] {
  return t.properties.filter(x => x.type.kind === 'ForeignKeyType').map(x => [x.name, x.type as ForeignKeyType]);
}

export function joinRelation(left: StructType, lCol: string | undefined, right: StructType, rCol: string | undefined): [string, string] {
  if (lCol !== undefined && rCol !== undefined) return [lCol, rCol];

  const lfks = foreignKeys(left).filter(x => x[1].table === right.name).map<[string, string]>(x => [x[0], x[1].column]);
  const rfks = foreignKeys(right).filter(x => x[1].table === left.name).map<[string, string]>(x => [x[1].column, x[0]]);

  const relations = [lfks, rfks].flat();

  if (relations.length !== 1)
    throws(`Error: Cannot not infer any relation between ${left.name} and ${right.name} either add an "on" clause to the join or add foreign keys to the types`);

  return relations[0];
}

function makeInitialExpressionContext(t: StructType): Context {
  return Object.fromEntries(t.properties.map(x => [x.name, x.type]));
}

function makeInitialAggExpressionContext(t: StructType): Context {
  const int = integerType();
  const float = floatType();
  const bool = booleanType();
  const string = stringType();

  return {
    ...makeInitialExpressionContext(t),
    this: t,
    sum: unionType(
      functionType([int], int),
      functionType([float], float),
    ),
    max: unionType(
      functionType([int], int),
      functionType([float], float),
    ),
    min: unionType(
      functionType([int], int),
      functionType([float], float),
    ),
    count: unionType(
      functionType([int], int),
      functionType([float], int),
      functionType([bool], int),
      functionType([string], int),
    ),
    collect: functionType([t], arrayType(t)),
  };
}

function sub(e: TypedTypeExpression, oldName: string, newName: string): TypedTypeExpression {
  const subType = (t: Type): Type => {
    return t.kind === 'StructType' && t.name === oldName ? structType(newName, t.properties) : t;
  }
  switch (e.kind) {
    case "TypedObjectTypeExpression": {
      const properties = e.properties.map(x => ({ name: x.name, value: sub(x.value, oldName, newName) }));
      return { kind: "TypedObjectTypeExpression", properties, type: subType(e.type) as StructType };
    }
    case "TypedIntegerTypeExpression":
    case "TypedFloatTypeExpression":
    case "TypedBooleanTypeExpression":
    case "TypedStringTypeExpression":
    case "TypedForeignKeyTypeExpression":
    case "TypedPrimaryKeyTypeExpression":
      return e;
    case "TypedArrayTypeExpression": {
      const of = sub(e.of, oldName, newName);
      return { kind: "TypedArrayTypeExpression", of, type: e.type };
    }
    case "TypedIdentifierTypeExpression": {
      return { kind: "TypedIdentifierTypeExpression", name: e.name, type: subType(e.type) };
    }
    case "TypedTypeIntroExpression": {
      const value = sub(e.value, oldName, newName);
      return { kind: "TypedTypeIntroExpression", name: e.name, value, type: subType(e.type) };
    }
    case "TypedJoinTypeExpression": {
      const left = sub(e.left, oldName, newName) as TypedStructLikeTypeExpression;
      const right = sub(e.right, oldName, newName) as TypedStructLikeTypeExpression;
      return { ...e, left, right, type: subType(e.type) as StructType };
    }
    case "TypedDropTypeExpression": {
      const left = sub(e.left, oldName, newName) as TypedStructLikeTypeExpression;
      return { ...e, left, type: subType(e.type) as StructType };
    }
    case "TypedWithTypeExpression": {
      const left = sub(e.left, oldName, newName) as TypedStructLikeTypeExpression;
      const rules = e.rules.map(r => {
        switch (r.kind) {
          case "TypedRuleTypeProperty": {
            const value = sub(r.value, oldName, newName);
            return { ...r, value };
          }
          case "TypedRuleValueProperty":
            return r;
        }
      });
      return { ...e, rules, left, type: subType(e.type) as StructType };
    }
    case "TypedUnionTypeExpression": {
      const left = sub(e.left, oldName, newName) as TypedStructLikeTypeExpression;
      const right = sub(e.right, oldName, newName) as TypedStructLikeTypeExpression;
      return { ...e, left, right, type: subType(e.type) as StructType };
    }
    case "TypedGroupByTypeExpression": {
      const left = sub(e.left, oldName, newName) as TypedStructLikeTypeExpression;
      return { ...e, left, type: subType(e.type) as StructType };
    }
  }
}

export function typeCheckTypeExpressions(expressions: TypeExpression[]): [TypedTypeExpression[], Context] {
  let ctx: Context = {};

  const inLocalContext = <T>(f: () => T) => {
    const old = { ...ctx };
    const x = f();
    ctx = old;
    return x;
  };

  const typeCheckRuleProperties = (t: StructType, rules: RuleProperty[]): TypedRuleProperty[] => {
    let eCtx = makeInitialExpressionContext(t);
    const typeCheckRule = (rule: RuleProperty): TypedRuleProperty => {
      switch (rule.kind) {
        case "RuleTypeProperty":
          return { kind: "TypedRuleTypeProperty", name: rule.name, value: typeCheck(rule.value) };
        case "RuleValueProperty": {
          const [e, c] = typeCheckExpression(rule.value, eCtx);
          eCtx = c;
          return { kind: "TypedRuleValueProperty", name: rule.name, value: e };
        }
      }
    }
    return inLocalContext(() => rules.map(typeCheckRule));
  };

  const typeCheckAggProperties = (t: StructType, aggs: AggProperty[]): TypedAggProperty[] => {
    let eCtx = makeInitialAggExpressionContext(t);
    const typeCheckAgg = (agg: AggProperty): TypedAggProperty => {
      switch (agg.kind) {
        case "AggProperty": {
          const [e, c] = typeCheckExpression(agg.value, eCtx);
          eCtx = c;
          return { kind: "TypedAggProperty", name: agg.name, value: e };
        }
      }
    }
    return inLocalContext(() => aggs.map(typeCheckAgg));
  };

  const typeCheck = (e: TypeExpression): TypedTypeExpression => {
    switch (e.kind) {
      case "IntegerTypeExpression": {
        const type = integerType();
        return { kind: "TypedIntegerTypeExpression", type };
      }
      case "FloatTypeExpression": {
        const type = floatType();
        return { kind: "TypedFloatTypeExpression", type };
      }
      case "BooleanTypeExpression": {
        const type = booleanType();
        return { kind: "TypedBooleanTypeExpression", type };
      }
      case "StringTypeExpression": {
        const type = stringType();
        return { kind: "TypedStringTypeExpression", type };
      }
      case "ForeignKeyTypeExpression": {
        const type = fkType(e.table, e.column);
        return { kind: "TypedForeignKeyTypeExpression", table: e.table, column: e.column, type };
      }
      case "PrimaryKeyTypeExpression": {
        const of = typeCheck(e.of) as TypedIntegerTypeExpression | TypedStringTypeExpression;
        const type = pkType(of.type);
        return { kind: "TypedPrimaryKeyTypeExpression", of, type };
      }
      case "ObjectTypeExpression": {
        const properties = e.properties.map(x => ({ ...x, value: typeCheck(x.value) }));
        const type = structType("", properties.map(x => ({ ...x, type: x.value.type })));
        return { kind: "TypedObjectTypeExpression", properties, type };
      }
      case "ArrayTypeExpression": {
        const of = typeCheck(e.of);
        const type = arrayType(of.type);
        return { kind: "TypedArrayTypeExpression", of, type };
      }
      case "IdentifierTypeExpression": {
        if (!(e.name in ctx)) throws(`Error: type ${e.name} is not defined`);
        const type = ctx[e.name];
        return { kind: "TypedIdentifierTypeExpression", name: e.name, type };
      }
      case "TypeIntroExpression": {
        if (e.name in ctx) throws(`Error: type ${e.name} is already defined`);
        let value = typeCheck(e.value);
        let type: Type;
        if (value.type.kind === 'StructType') {
          type = structType(e.name, value.type.properties);
          value = sub(value, value.type.name, e.name)
        }
        else {
          type = value.type;
        }
        ctx[e.name] = type;
        return { kind: "TypedTypeIntroExpression", name: e.name, value, type };
      }
      case "JoinTypeExpression": {
        const left = typeCheck(e.left);
        const right = typeCheck(e.right);

        canOperateLikeStruct(left, 'Left side of Join expression');
        canOperateLikeStruct(right, 'Right side of Join expression');

        const [leftColumn, rightColumn] = joinRelation(left.type, e.leftColumn, right.type, e.rightColumn);

        const type = joinType(e.method, left.type, leftColumn, right.type, rightColumn);
        return { kind: "TypedJoinTypeExpression", left, leftColumn, method: e.method, right, rightColumn, type };
      }
      case "DropTypeExpression": {
        const left = typeCheck(e.left);

        canOperateLikeStruct(left, 'Left side of Join expression');

        const type = dropType(left.type, e.properties);
        return { kind: "TypedDropTypeExpression", left, properties: e.properties, type };
      }
      case "WithTypeExpression": {
        const left = typeCheck(e.left);

        canOperateLikeStruct(left, 'Left side of Join expression');

        const rules = typeCheckRuleProperties(left.type, e.rules);

        const type = withType(left.type, rules);
        return { kind: "TypedWithTypeExpression", left, rules, type };
      }
      case "UnionTypeExpression": {
        const left = typeCheck(e.left);
        const right = typeCheck(e.right);

        canOperateLikeStruct(left, 'Left side of Join expression');
        canOperateLikeStruct(right, 'Right side of Join expression');

        const type = unionStructType(left.type, right.type);
        return { kind: "TypedUnionTypeExpression", left, right, type };
      }
      case "GroupByTypeExpression": {
        const left = typeCheck(e.left);

        canOperateLikeStruct(left, 'Left side of Join expression');

        const aggregations = typeCheckAggProperties(left.type, e.aggregations);

        const type = groupByType(left.type, e.column, aggregations);
        return { kind: "TypedGroupByTypeExpression", left, column: e.column, aggregations, type };
      }
    }
  }

  return [expressions.map(typeCheck), ctx];
}

