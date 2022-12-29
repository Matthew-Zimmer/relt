import { LinearObjectTypeExpression, LinearTypeIntroExpression, PrimitiveLinearTypeExpression } from "../asts/typeExpression/linear";
import { TypeExpression } from "../asts/typeExpression/untyped";
import { throws } from "../utils";
import { typeCheckExpression } from "./typeCheck/expression";

function namedLinearize(e: TypeExpression): [string, PrimitiveLinearTypeExpression[]] {
  const exprs = linearize(e);
  if (exprs.length === 0)
    throws(`Named Linearization was empty`);
  const last = exprs[exprs.length - 1];
  if (last.kind !== 'LinearIdentifierTypeExpression' && last.kind !== 'LinearTypeIntroExpression')
    throws(`Named Linearization did not result in named type`);
  return [last.name, exprs.filter(x => x.kind === 'LinearTypeIntroExpression')];
}

export function linearize(e: TypeExpression): PrimitiveLinearTypeExpression[] {
  switch (e.kind) {
    case "IntegerTypeExpression":
      return [{ kind: "LinearIntegerTypeExpression" }];
    case "FloatTypeExpression":
      return [{ kind: "LinearFloatTypeExpression" }];
    case "BooleanTypeExpression":
      return [{ kind: "LinearBooleanTypeExpression" }];
    case "StringTypeExpression":
      return [{ kind: "LinearStringTypeExpression" }];
    case "IdentifierTypeExpression":
      return [{ kind: "LinearIdentifierTypeExpression", name: e.name }];
    case "ObjectTypeExpression": {
      const [sub, properties] = e.properties.reduce<[PrimitiveLinearTypeExpression[], LinearObjectTypeExpression['properties']]>(([s, a], p) => {
        const x = linearize(p.value);
        if (x.length === 0)
          throws(`Object linearization resulted in length 0 for sub expressions`);
        const front = x.slice(0, -1);
        const last = x[x.length - 1];
        return [[...s, ...front], [...a, { name: p.name, value: last }]];
      }, [[], []]);

      return [
        ...sub,
        {
          kind: "LinearTypeIntroExpression",
          name: `SubStep_Object${e.properties.map(x => x.name).join('_')}`,
          value: {
            kind: "LinearObjectTypeExpression",
            properties,
          }
        }
      ]
    }
    case "JoinTypeExpression": {
      const [lName, left] = namedLinearize(e.left);
      const [rName, right] = namedLinearize(e.right);
      return [
        ...left,
        ...right,
        {
          kind: "LinearTypeIntroExpression",
          name: `SubStep_${lName}${e.type}Join${rName}`,
          value: {
            kind: "LinearJoinTypeExpression",
            left: { kind: "LinearIdentifierTypeExpression", name: lName },
            leftColumn: e.leftColumn,
            right: { kind: "LinearIdentifierTypeExpression", name: rName },
            rightColumn: e.rightColumn,
            type: e.type
          }
        },
      ];
    }
    case "DropTypeExpression": {
      const [lName, left] = namedLinearize(e.left);
      return [
        ...left,
        {
          kind: "LinearTypeIntroExpression",
          name: `SubStep_${lName}Drop${e.properties.join('_')}`,
          value: {
            kind: "LinearDropTypeExpression",
            left: { kind: "LinearIdentifierTypeExpression", name: lName },
            properties: e.properties,
          }
        },
      ];
    }
    case "WithTypeExpression": {
      const [lName, left] = namedLinearize(e.left);
      return [
        ...left,
        {
          kind: "LinearTypeIntroExpression",
          name: `SubStep_${lName}With${e.rules.map(x => x.name).join('_')}`,
          value: {
            kind: "LinearWithTypeExpression",
            left: { kind: "LinearIdentifierTypeExpression", name: lName },
            rules: e.rules,
          }
        }
      ];
    }
    case "UnionTypeExpression":
      throw 'TODO';
    case "TypeIntroExpression": {
      const values = linearize(e.value);
      if (values.length === 0)
        throws(`Type intro ${e.name} resulted in no sub linear expressions`);
      const front = values.slice(0, -1);
      const last = values[values.length - 1];
      const me: LinearTypeIntroExpression = {
        kind: "LinearTypeIntroExpression",
        name: e.name,
        value: last.kind === 'LinearTypeIntroExpression' ? last.value : last,
      };
      return [
        ...front,
        me
      ];
    }
  }
}


