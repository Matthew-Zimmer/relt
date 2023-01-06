import { arrayType, booleanType, fkType, floatType, ForeignKeyType, functionType, integerType, optionalType, pkType, stringType, StructType, structType, Type, unionType } from "../../asts/type";
import { TypedAggProperty, TypedIntegerTypeExpression, TypedRuleProperty, TypedStringTypeExpression, TypedStructLikeTypeExpression, TypedTypeExpression } from "../../asts/typeExpression/typed";
import { throws } from "../../utils";
import { Context, typeEquals } from "./utils";
import { typeCheckExpression } from "./expression";
import { AggProperty, JoinTypeExpression, RuleProperty, TypeExpression } from "../../asts/typeExpression/untyped";
import { Expression } from "../../asts/expression/untyped";
import { TypedExpression } from "../../asts/expression/typed";

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

function makeInitialExpressionContext(t: StructType, extra: Context): Context {
  return {
    ...extra,
    ...Object.fromEntries(t.properties.map(x => [x.name, x.type])),
  };
}

function makeInitialAggExpressionContext(t: StructType, extra: Context): Context {
  const int = integerType();
  const float = floatType();
  const bool = booleanType();
  const string = stringType();

  return {
    ...makeInitialExpressionContext(t, extra),
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
    case "TypedJoinTypeExpression":
    case "TypedUnionTypeExpression": {
      const left = sub(e.left, oldName, newName) as TypedStructLikeTypeExpression;
      const right = sub(e.right, oldName, newName) as TypedStructLikeTypeExpression;
      return { ...e, left, right, type: subType(e.type) as StructType };
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
    case "TypedGroupByTypeExpression":
    case "TypedSortTypeExpression":
    case "TypedWhereTypeExpression":
    case "TypedDistinctTypeExpression":
    case "TypedDropTypeExpression": {
      const left = sub(e.left, oldName, newName) as TypedStructLikeTypeExpression;
      return { ...e, left, type: subType(e.type) as StructType };
    }
  }
}

export function typeCheckTypeExpressions(expressions: TypeExpression[], initialExpressionContext?: Context): [TypedTypeExpression[], Context] {
  let ctx: Context = {};

  const inLocalContext = <T>(f: () => T) => {
    const old = { ...ctx };
    const x = f();
    ctx = old;
    return x;
  };

  const typeCheckRuleProperties = (t: StructType, rules: RuleProperty[]): TypedRuleProperty[] => {
    let eCtx = makeInitialExpressionContext(t, initialExpressionContext ?? {});
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
    let eCtx = makeInitialAggExpressionContext(t, initialExpressionContext ?? {});
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

  const typeCheckExpressionUnder = (e: Expression, t: StructType): TypedExpression => {
    let eCtx = makeInitialAggExpressionContext(t, initialExpressionContext ?? {});
    return inLocalContext(() => typeCheckExpression(e, eCtx)[0]);
  };

  const checkColumnsExist = (t: StructType, columns: string[]): string[] => {
    return columns.map(c => propertyLookup(t, c) === undefined ? `Cannot sort on ${c} since not exist on type ${t.name}` : '').filter(x => x !== '');
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

        canOperateLikeStruct(left, 'Left side of Drop expression');

        const type = dropType(left.type, e.properties);
        return { kind: "TypedDropTypeExpression", left, properties: e.properties, type };
      }
      case "WithTypeExpression": {
        const left = typeCheck(e.left);

        canOperateLikeStruct(left, 'Left side of With expression');

        const rules = typeCheckRuleProperties(left.type, e.rules);

        const type = withType(left.type, rules);
        return { kind: "TypedWithTypeExpression", left, rules, type };
      }
      case "UnionTypeExpression": {
        const left = typeCheck(e.left);
        const right = typeCheck(e.right);

        canOperateLikeStruct(left, 'Left side of Union expression');
        canOperateLikeStruct(right, 'Right side of Union expression');

        const type = unionStructType(left.type, right.type);
        return { kind: "TypedUnionTypeExpression", left, right, type };
      }
      case "GroupByTypeExpression": {
        const left = typeCheck(e.left);

        canOperateLikeStruct(left, 'Left side of Group expression');

        const aggregations = typeCheckAggProperties(left.type, e.aggregations);

        const type = groupByType(left.type, e.column, aggregations);
        return { kind: "TypedGroupByTypeExpression", left, column: e.column, aggregations, type };
      }
      case "SortTypeExpression": {
        const left = typeCheck(e.left);

        canOperateLikeStruct(left, 'Left side of Sort expression'); // Its really the right side?

        const errors = checkColumnsExist(left.type, e.columns.map(x => x.name));
        if (errors.length !== 0)
          throws(`Error: ${errors.join(' and\n')}`)

        const type = structType(`Relt_Sorted${left.type.name}${e.columns.map(x => `${x.name}${x.order}${x.nulls}`).join('_')}`, left.type.properties);
        return { kind: "TypedSortTypeExpression", left, columns: e.columns, type };
      }
      case "DistinctTypeExpression": {
        const left = typeCheck(e.left);

        canOperateLikeStruct(left, 'Left side of Distinct expression'); // Its really the right side?

        const errors = checkColumnsExist(left.type, e.columns);
        if (errors.length !== 0)
          throws(`Error: ${errors.join(' and\n')}`)

        const type = structType(`Relt_Distinct${left.type.name}${e.columns.join('_')}`, left.type.properties);
        return { kind: "TypedDistinctTypeExpression", left, columns: e.columns, type };
      }
      case "WhereTypeExpression": {
        const left = typeCheck(e.left);

        canOperateLikeStruct(left, 'Left side of Join expression');// Its really the right side?

        const condition = typeCheckExpressionUnder(e.condition, left.type);

        if (condition.type.kind !== 'BooleanType')
          throws(`Condition of where type needs to be a boolean, got: (${condition.type.kind})`);

        const type = structType(`Relt_Condition${left.type.name}`, left.type.properties); // TODO need way to extract what columns are filtered
        return { kind: "TypedWhereTypeExpression", left, condition, type };
      }
    }
  }

  return [expressions.map(typeCheck), ctx];
}

