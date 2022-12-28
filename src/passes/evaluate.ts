import { throws } from "../utils";
import { TypedExpression } from "../asts/expression/typed";
import { Value } from "../asts/value";
import { Type } from "../asts/type";

export type Scope = Record<string, Value>;

export function typeName(t: Type): string {
  switch (t.kind) {
    case "IntegerType":
      return "int";
    case "FloatType":
      return "float";
    case "BooleanType":
      return "bool";
    case "StringType":
      return "string";
    case "IdentifierType":
      return t.name;
    case "UnitType":
    case "ObjectType":
    case "FunctionType":
    case "TypeType":
    case "UnionType":
      throws(`Cannot convert ${t.kind} to a type name`);
  }
}

function mangleName(name: string, types: Type[]): string {
  return `${name}__${types.map(typeName).join('__')}`;
}

export function evaluate(e: TypedExpression, scope: Scope): [Value, Scope] {
  switch (e.kind) {
    case "TypedIntegerExpression":
    case "TypedFloatExpression":
    case "TypedBooleanExpression":
    case "TypedStringExpression":
      return [e.value, scope];
    case "TypedIdentifierExpression": {
      if (!(e.name in scope))
        throws(`During evaluation ${e.name} is not defined`);
      return [scope[e.name], scope];
    }
    case "TypedObjectExpression": {
      const properties = e.properties.map<[string, Value]>(x => [x.name, evaluate(x.value, scope)[0]]);
      return [Object.fromEntries(properties), scope];
    }
    case "TypedBlockExpression": {
      const [values] = e.values.reduce<[Value[], Scope]>(([v, s], c) => {
        const value = evaluate(c, s);
        return [[...v, value[0]], value[1]];
      }, [[], scope]);
      return [values[values.length - 1], scope];
    }
    case "TypedLetExpression": {
      const [value] = evaluate(e.value, scope);
      return [value, { ...scope, [e.name]: value }];
    }
    case "TypedFunctionExpression": {
      const name = mangleName(e.name, e.parameters.map(x => x.type));
      const value = (...args: Value[]) => evaluate(e.value, { ...scope, ...Object.fromEntries(e.parameters.map((p, i) => [p.name, args[i]])) })[0];
      return [value, { ...scope, [name]: value }];
    }
    case "TypedApplicationExpression": {
      const mangledFunc: TypedExpression = e.func.kind !== "TypedIdentifierExpression" ? e.func : {
        kind: "TypedIdentifierExpression",
        name: mangleName(e.func.name, e.args.map(x => x.type)),
        type: e.type,
      };

      const [func] = evaluate(mangledFunc, scope);
      const args = e.args.map(x => evaluate(x, scope)[0]);

      if (typeof func !== 'function')
        throws(`During evaluation of application tried to call non function`);

      return [func(...args), scope];
    }
  }
}

export function evaluateAllExpressions(expressions: TypedExpression[]): [Value[], Scope] {
  return expressions.reduce<[Value[], Scope]>(([a, c], e) => {
    const [e1, c1] = evaluate(e, c);
    return [[...a, e1], c1];
  }, [[], {}]);
}
