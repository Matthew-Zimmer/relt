import { FlatIdentifierTypeExpression, FlatIntegerTypeExpression, FlatObjectTypeExpression, FlatStringTypeExpression, FlatTypeIntroExpression, NamedExpression, PrimitiveFlatTypeExpression } from "../asts/typeExpression/flat";
import { RuleTypeProperty, RuleValueProperty, TypeExpression, WithTypeExpression } from "../asts/typeExpression/untyped";
import { Id } from "../asts/typeExpression/util";
import { throws } from "../utils";

export function flattenTypeExpressions(e: TypeExpression[], id: Id): [PrimitiveFlatTypeExpression[], Id] {
  let idCount = id;
  const nameIds = new Map<string, Id>();
  const pointerIds = new Map<Id, Id>();

  function resolvePointerId(id: Id): Id {
    while (pointerIds.has(id))
      id = pointerIds.get(id)!;
    return id;
  }

  function namedFlatten(e: TypeExpression,): [string, Id, PrimitiveFlatTypeExpression[]] {
    const exprs = flatten(e);
    if (exprs.length === 0)
      throws(`Named Flatization was empty`);
    const last = exprs[exprs.length - 1];
    if (last.kind !== 'FlatIdentifierTypeExpression' && last.kind !== 'FlatTypeIntroExpression')
      throws(`Named Flatization did not result in named type`);
    return [last.name, last.id, exprs.filter(x => x.kind === 'FlatTypeIntroExpression')];
  }

  function desugarImplicitTypeRules(e: WithTypeExpression): TypeExpression {
    const typeRules: RuleTypeProperty[] = [];
    const dependentValueRules: Map<number, RuleValueProperty[]> = new Map();
    const independentValueRules: RuleValueProperty[] = [];

    const isDependent = (r: RuleValueProperty): number | undefined => {
      return 0;
    };

    e.rules.forEach(r => {
      switch (r.kind) {
        case "RuleTypeProperty":
          typeRules.push(r);
          break;
        case "RuleValueProperty": {
          const result = isDependent(r);
          result === undefined ? independentValueRules.push(r) : dependentValueRules.set(result, [...dependentValueRules.get(result) ?? [], r]);
        }
      }
    });

    let expr: TypeExpression = independentValueRules.length === 0 ? e.left : {
      kind: "WithTypeExpression",
      id: e.id,
      left: e.left,
      rules: independentValueRules,
    };

    for (const [i, r] of typeRules.entries()) {
      // TODO handle these!!!!
      const dr = dependentValueRules.get(i) ?? [];
      const v = r.value;
      switch (v.kind) {
        // these will (probably) not have any desugar rules
        case "PrimaryKeyTypeExpression":
        case "ForeignKeyTypeExpression":
        case "ObjectTypeExpression":
          break;

        // These can add optional implicit meta data
        // none rn
        case "IntegerTypeExpression":
        case "FloatTypeExpression":
        case "BooleanTypeExpression":
        case "StringTypeExpression":
          break;

        // This is an implicit
        // join -> map
        case "IdentifierTypeExpression":
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
        case "ArrayTypeExpression":
          switch (v.of.kind) {
            case "IdentifierTypeExpression":
              expr = {
                kind: "WithTypeExpression",
                id: idCount++,
                left: {
                  kind: "JoinTypeExpression",
                  id: idCount++,
                  left: expr,
                  method: "left",
                  right: {
                    kind: "GroupByTypeExpression",
                    id: idCount++,
                    left: v.of,
                    column: [expr.id, v.of.id],
                    aggregations: [{
                      kind: "AggProperty",
                      name: r.name,
                      value: {
                        kind: "ApplicationExpression",
                        func: { kind: "IdentifierExpression", name: "collect" },
                        args: [{ kind: "IdentifierExpression", name: "this" }]
                      }
                    }]
                  }
                },
                rules: [
                  {
                    kind: "RuleValueProperty",
                    name: r.name,
                    value: {
                      kind: "DefaultExpression",
                      left: { kind: "IdentifierExpression", name: r.name },
                      op: "??",
                      right: { kind: "ArrayExpression", values: [] },
                    }
                  }
                ]
              };
              break;
          }
          break;

        // TODO 
        // desugar nested expressions
        case "DropTypeExpression":
        case "GroupByTypeExpression":
        case "JoinTypeExpression":
        case "TypeIntroExpression":
        case "UnionTypeExpression":
        case "WithTypeExpression":
          throws(`TODO desugar nested type expressions`);
      }
    }

    return expr;
  }

  function isSimpleWithTypeExpression(x: WithTypeExpression): x is WithTypeExpression<RuleValueProperty> {
    return !x.rules.some(x => x.kind === 'RuleTypeProperty');
  }

  function flatten(e: TypeExpression): PrimitiveFlatTypeExpression[] {
    switch (e.kind) {
      case "IntegerTypeExpression":
        return [{ kind: "FlatIntegerTypeExpression", id: e.id }];
      case "FloatTypeExpression":
        return [{ kind: "FlatFloatTypeExpression", id: e.id }];
      case "BooleanTypeExpression":
        return [{ kind: "FlatBooleanTypeExpression", id: e.id }];
      case "StringTypeExpression":
        return [{ kind: "FlatStringTypeExpression", id: e.id }];
      case "IdentifierTypeExpression": {
        if (nameIds.has(e.name)) {
          const id = nameIds.get(e.name)!;
          pointerIds.set(e.id, id);
          return [{ kind: "FlatIdentifierTypeExpression", name: e.name, id }];
        }
        else if (pointerIds.has(e.id)) {
          const id = resolvePointerId(e.id);
          return [{ kind: "FlatIdentifierTypeExpression", name: e.name, id }];
        }
        else {
          return [{ kind: "FlatIdentifierTypeExpression", name: e.name, id: e.id }];
        }
      }
      case "PrimaryKeyTypeExpression":
        return [{ kind: "FlatPrimaryKeyTypeExpression", of: flatten(e.of)[0] as FlatStringTypeExpression | FlatIntegerTypeExpression, id: e.id }];
      case "ForeignKeyTypeExpression":
        return [{ kind: "FlatForeignKeyTypeExpression", table: e.table, column: e.column, id: e.id }];
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
              id: e.id,
              properties,
            },
            id: idCount++,
          }
        ]
      }
      case "JoinTypeExpression": {
        const [lName, lId, left] = namedFlatten(e.left);
        const [rName, rId, right] = namedFlatten(e.right);
        return [
          ...left,
          ...right,
          {
            kind: "FlatTypeIntroExpression",
            id: idCount++,
            name: `SubStep_${lName}${e.method}Join${rName}`,
            value: {
              kind: "FlatJoinTypeExpression",
              id: e.id,
              left: { kind: "FlatIdentifierTypeExpression", name: lName, id: lId, },
              leftColumn: e.leftColumn,
              right: { kind: "FlatIdentifierTypeExpression", name: rName, id: rId, },
              rightColumn: e.rightColumn,
              method: e.method
            }
          },
        ];
      }
      case "DropTypeExpression": {
        const [lName, lId, left] = namedFlatten(e.left);
        return [
          ...left,
          {
            kind: "FlatTypeIntroExpression",
            id: idCount++,
            name: `SubStep_${lName}Drop${e.properties.join('_')}`,
            value: {
              kind: "FlatDropTypeExpression",
              id: e.id,
              left: { kind: "FlatIdentifierTypeExpression", name: lName, id: lId, },
              properties: e.properties,
            }
          },
        ];
      }
      case "WithTypeExpression": {
        if (!isSimpleWithTypeExpression(e)) return flatten(desugarImplicitTypeRules(e));

        const [lName, lId, left] = namedFlatten(e.left);

        return [
          ...left,
          {
            kind: "FlatTypeIntroExpression",
            id: idCount++,
            name: `SubStep_${lName}With${e.rules.map(x => x.name).join('_')}`,
            value: {
              kind: "FlatWithTypeExpression",
              id: e.id,
              left: { kind: "FlatIdentifierTypeExpression", name: lName, id: lId, },
              rules: e.rules,
            }
          }
        ];
      }
      case "UnionTypeExpression":
        const [lName, lId, left] = namedFlatten(e.left);
        const [rName, rId, right] = namedFlatten(e.right);
        return [
          ...left,
          ...right,
          {
            kind: "FlatTypeIntroExpression",
            id: idCount++,
            name: `SubStep_${lName}Union${rName}`,
            value: {
              kind: "FlatUnionTypeExpression",
              id: e.id,
              left: { kind: "FlatIdentifierTypeExpression", name: lName, id: lId, },
              right: { kind: "FlatIdentifierTypeExpression", name: rName, id: rId, },
            }
          },
        ];
      case "TypeIntroExpression": {
        const values = flatten(e.value);
        if (values.length === 0)
          throws(`Type intro ${e.name} resulted in no sub flat expressions`);
        const front = values.slice(0, -1);
        const last = values[values.length - 1];
        const me: FlatTypeIntroExpression = {
          kind: "FlatTypeIntroExpression",
          id: e.id,
          name: e.name,
          value: last.kind === 'FlatTypeIntroExpression' ? last.value : last,
        };
        nameIds.set(e.name, e.id);
        return [
          ...front,
          me
        ];
      }
      case "ArrayTypeExpression": {
        const [name, ofId, of] = namedFlatten(e.of);
        return [
          ...of,
          {
            kind: "FlatArrayTypeExpression",
            id: e.id,
            of: { kind: "FlatIdentifierTypeExpression", name, id: ofId, }
          }
        ];
      }
      case "GroupByTypeExpression": {
        const [name, lId, left] = namedFlatten(e.left);
        return [
          ...left,
          {
            kind: "FlatTypeIntroExpression",
            id: idCount++,
            name: `SubStep_${name}GroupBy${typeof e.column === 'string' ? e.column : `${e.column[0]}_${e.column[1]}`}`,
            value: {
              kind: "FlatGroupByTypeExpression",
              id: e.id,
              column: typeof e.column === 'string' ? e.column : [resolvePointerId(e.column[0]), resolvePointerId(e.column[1])],
              left: { kind: "FlatIdentifierTypeExpression", name, id: lId, },
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

  return [e.flatMap(x => flatten(x)), idCount];
}

