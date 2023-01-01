import { FlatIntegerTypeExpression, FlatObjectTypeExpression, FlatStringTypeExpression, FlatTypeIntroExpression, PrimitiveFlatTypeExpression } from "../asts/typeExpression/flat";
import { RuleTypeProperty, TypeExpression } from "../asts/typeExpression/untyped";
import { throws } from "../utils";
import { typeCheckExpression } from "./typeCheck/expression";

function namedFlatten(e: TypeExpression): [string, PrimitiveFlatTypeExpression[]] {
  const exprs = flatten(e);
  if (exprs.length === 0)
    throws(`Named Flatization was empty`);
  const last = exprs[exprs.length - 1];
  if (last.kind !== 'FlatIdentifierTypeExpression' && last.kind !== 'FlatTypeIntroExpression')
    throws(`Named Flatization did not result in named type`);
  return [last.name, exprs.filter(x => x.kind === 'FlatTypeIntroExpression')];
}

export function flatten(e: TypeExpression): PrimitiveFlatTypeExpression[] {
  switch (e.kind) {
    case "IntegerTypeExpression":
      return [{ kind: "FlatIntegerTypeExpression" }];
    case "FloatTypeExpression":
      return [{ kind: "FlatFloatTypeExpression" }];
    case "BooleanTypeExpression":
      return [{ kind: "FlatBooleanTypeExpression" }];
    case "StringTypeExpression":
      return [{ kind: "FlatStringTypeExpression" }];
    case "IdentifierTypeExpression":
      return [{ kind: "FlatIdentifierTypeExpression", name: e.name }];
    case "PrimaryKeyTypeExpression":
      return [{ kind: "FlatPrimaryKeyTypeExpression", of: flatten(e.of)[0] as FlatStringTypeExpression | FlatIntegerTypeExpression }];
    case "ForeignKeyTypeExpression":
      return [{ kind: "FlatForeignKeyTypeExpression", table: e.table, column: e.column }];
    case "ObjectTypeExpression": {
      const [sub, properties] = e.properties.reduce<[PrimitiveFlatTypeExpression[], FlatObjectTypeExpression['properties']]>(([s, a], p) => {
        const x = flatten(p.value);
        if (x.length === 0)
          throws(`Object flatization resulted in length 0 for sub expressions`);
        const front = x.slice(0, -1);
        const last = x[x.length - 1];
        return [[...s, ...front], [...a, { name: p.name, value: last }]];
      }, [[], []]);

      return [
        ...sub,
        {
          kind: "FlatTypeIntroExpression",
          name: `SubStep_Object${e.properties.map(x => x.name).join('_')}`,
          value: {
            kind: "FlatObjectTypeExpression",
            properties,
          }
        }
      ]
    }
    case "JoinTypeExpression": {
      const [lName, left] = namedFlatten(e.left);
      const [rName, right] = namedFlatten(e.right);
      return [
        ...left,
        ...right,
        {
          kind: "FlatTypeIntroExpression",
          name: `SubStep_${lName}${e.method}Join${rName}`,
          value: {
            kind: "FlatJoinTypeExpression",
            left: { kind: "FlatIdentifierTypeExpression", name: lName },
            leftColumn: e.leftColumn,
            right: { kind: "FlatIdentifierTypeExpression", name: rName },
            rightColumn: e.rightColumn,
            method: e.method
          }
        },
      ];
    }
    case "DropTypeExpression": {
      const [lName, left] = namedFlatten(e.left);
      return [
        ...left,
        {
          kind: "FlatTypeIntroExpression",
          name: `SubStep_${lName}Drop${e.properties.join('_')}`,
          value: {
            kind: "FlatDropTypeExpression",
            left: { kind: "FlatIdentifierTypeExpression", name: lName },
            properties: e.properties,
          }
        },
      ];
    }
    case "WithTypeExpression": {
      const [lName, left] = namedFlatten(e.left);
      const typeRules = e.rules.filter((x): x is RuleTypeProperty => x.kind === 'RuleTypeProperty');
      const steps = typeRules.map(r => namedFlatten(r.value));
      let s = 0;

      return [
        ...left,
        ...steps.flatMap(x => x[1]),
        {
          kind: "FlatTypeIntroExpression",
          name: `SubStep_${lName}With${e.rules.map(x => x.name).join('_')}`,
          value: {
            kind: "FlatWithTypeExpression",
            left: { kind: "FlatIdentifierTypeExpression", name: lName },
            rules: e.rules.map(r => {
              switch (r.kind) {
                case 'RuleTypeProperty':
                  throws(`TODO`);
                // return {
                //   kind: "FlatRuleTypeProperty",
                //   name: r.name,
                //   value: {
                //     kind: "FlatIdentifierTypeExpression",
                //     name: steps[s++][0],
                //   }
                // };
                case 'RuleValueProperty':
                  return {
                    name: r.name,
                    value: r.value,
                  };
              }
            }),
          }
        }
      ];
    }
    case "UnionTypeExpression":
      throw 'TODO';
    case "TypeIntroExpression": {
      const values = flatten(e.value);
      if (values.length === 0)
        throws(`Type intro ${e.name} resulted in no sub flat expressions`);
      const front = values.slice(0, -1);
      const last = values[values.length - 1];
      const me: FlatTypeIntroExpression = {
        kind: "FlatTypeIntroExpression",
        name: e.name,
        value: last.kind === 'FlatTypeIntroExpression' ? last.value : last,
      };
      return [
        ...front,
        me
      ];
    }
    case "ArrayTypeExpression": {
      const [name, of] = namedFlatten(e.of);
      return [
        ...of,
        {
          kind: "FlatArrayTypeExpression",
          of: { kind: "FlatIdentifierTypeExpression", name }
        }
      ];
    }
    case "GroupByTypeExpression": {
      const [name, left] = namedFlatten(e.left);
      return [
        ...left,
        {
          kind: "FlatTypeIntroExpression",
          name: `SubStep_${name}GroupBy${e.column}`,
          value: {
            kind: "FlatGroupByTypeExpression",
            column: e.column,
            left: { kind: "FlatIdentifierTypeExpression", name },
            aggregations: e.aggregations.map(x => ({
              ...x,
              kind: "FlatAggProperty",
            })),
          }
        }
      ]
    }
  }
}


