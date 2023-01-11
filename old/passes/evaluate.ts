import { throws } from "../utils";
import { TypedExpression } from "../asts/expression/typed";
import { Value, ValueObject } from "../asts/value";
import { Type } from "../asts/type";
import { generateType } from "../debug/debug";

export type Scope = Record<string, Value>;

export function typeName(t: Type): string {
  switch (t.kind) {
    case "StructType":
      return t.name;
    default:
      return generateType(t);
  }
}

function mangleName(name: string, types: Type[]): string {
  return `${name}__${types.map(typeName).join('__')}`;
}

function addFloatFloat(l: number, r: number): number {
  return l + r;
}

function addIntInt(l: number, r: number): number {
  return l + r;
}

function addStringString(l: string, r: string): string {
  return l + r;
}

function addObjectObject(l: ValueObject, r: ValueObject): ValueObject {
  return { ...l, ...r };
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
    case "TypedAddExpression": {
      const [left] = evaluate(e.left, scope);
      const [right] = evaluate(e.right, scope);

      switch (e.op) {
        case "+":
          switch (e.left.type.kind) {
            case "FloatType":
              switch (e.right.type.kind) {
                case "FloatType":
                  return [addFloatFloat(left as number, right as number), scope];
              }
              break;
            case "IntegerType":
              switch (e.right.type.kind) {
                case "IntegerType":
                  return [addIntInt(left as number, right as number), scope];
              }
              break;
            case "StringType":
              switch (e.right.type.kind) {
                case "StringType":
                  return [addStringString(left as string, right as string), scope];
              }
              break;
            case "StructType":
              switch (e.right.type.kind) {
                case "StructType":
                  return [addObjectObject(left as ValueObject, right as ValueObject), scope];
              }
              break;
          }
          break;
      }

      throws(`During evaluation can not perform ${e.op} operation ${e.left.type.kind} with ${e.right.type.kind}`);
    }
    case "TypedDefaultExpression": {
      const [left] = evaluate(e.left, scope);
      if (left !== null && left !== undefined) return [left, scope];
      return [evaluate(e.right, scope)[0], scope];
    }
    case "TypedCmpExpression": {
      const [left] = evaluate(e.left, scope);
      const [right] = evaluate(e.left, scope);

      const value: Value = (() => {
        switch (e.op) {
          case "!=":
            return left !== right;
          case "==":
            return left !== right;
          case "<":
            return (left as number) < (right as number);
          case ">":
            return (left as number) > (right as number);
          case "<=":
            return (left as number) <= (right as number);
          case ">=":
            return (left as number) >= (right as number);
        }
      })();

      return [, scope];
    }
    case "TypedArrayExpression": {
      const values = e.values.map(x => evaluate(x, scope)[0]);
      return [values, scope];
    }
    case "TypedDotExpression": {
      const [left] = evaluate(e.left, scope) as [ValueObject, Scope];
      const [right] = evaluate(e.right, left);
      return [right, scope];
    }
  }
}

export function evaluateAllExpressions(expressions: TypedExpression[]): [Value[], Scope] {
  return expressions.reduce<[Value[], Scope]>(([a, c], e) => {
    const [e1, c1] = evaluate(e, c);
    return [[...a, e1], c1];
  }, [[], {}]);
}