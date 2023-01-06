import { TypedBlockExpression, TypedExpression, TypedFunctionExpression, TypedIdentifierExpression } from '../../asts/expression/typed';
import { Expression } from '../../asts/expression/untyped';
import { arrayType, booleanType, floatType, FunctionType, functionType, integerType, OptionalType, stringType, StructType, structType, Type, unionType, unitType } from '../../asts/type';
import { Context, typeEquals } from './utils';
import { throws } from '../../utils';
import { typeName } from '../evaluate';

function addFunctionToContext(ctx: Context, name: string, type: Type): Context {
  if (!(name in ctx))
    return { ...ctx, [name]: type };
  const existingType = ctx[name];
  switch (existingType.kind) {
    case 'FunctionType':
      return { ...ctx, [name]: unionType(existingType, type) };
    case 'UnionType':
      if (existingType.types.every(x => x.kind === 'FunctionType'))
        return { ...ctx, [name]: unionType(...existingType.types, type) };
    default:
      throws(`${name} is already defined and it not a function so overloading is not allowed`);
  }
}

function applicationResultType(func: FunctionType, args: Type[]): FunctionType {
  if (args.length !== func.from.length)
    throws(`Cannot all function expecting ${func.from.length} args with ${args.length} args`);
  for (const i of func.from.keys())
    if (!typeEquals(func.from[i], args[i]))
      throws(`func call ${i}th args does not match expected ${func.from[i].kind} got ${args[i].kind}`);
  return func;
}

function overloadedApplicationResultType(func: Type, args: Type[]): FunctionType {
  switch (func.kind) {
    case 'FunctionType':
      return applicationResultType(func, args);
    case 'UnionType': {
      const types = func.types.filter(x => x.kind === 'FunctionType') as FunctionType[];
      for (const type of types) {
        try {
          return applicationResultType(type, args);
        }
        catch (e) {
        }
      }
      throws(`function overload all failed`);
    }
    default:
      throws(`Cannot call non function type`);
  }
}

export function typeCheckExpression(e: Expression, ctx: Context): [TypedExpression, Context] {
  switch (e.kind) {
    case "IntegerExpression": {
      return [{ kind: "TypedIntegerExpression", value: e.value, type: integerType() }, ctx];
    }
    case "FloatExpression": {
      return [{ kind: "TypedFloatExpression", value: e.value, type: floatType() }, ctx];
    }
    case "BooleanExpression": {
      return [{ kind: "TypedBooleanExpression", value: e.value, type: booleanType() }, ctx];
    }
    case "StringExpression": {
      return [{ kind: "TypedStringExpression", value: e.value, type: stringType() }, ctx];
    }
    case "IdentifierExpression": {
      if (!(e.name in ctx))
        throws(`${e.name} is not defined`);
      return [{ kind: "TypedIdentifierExpression", name: e.name, type: ctx[e.name] }, ctx];
    }
    case "LetExpression": {
      if (e.name in ctx)
        throws(`${e.name} is already defined`);
      const [value] = typeCheckExpression(e.value, ctx);
      return [{ kind: "TypedLetExpression", name: e.name, value, type: value.type }, { ...ctx, [e.name]: value.type }]
    }
    case "ObjectExpression": {
      const properties = e.properties.map(x => ({ name: x.name, value: typeCheckExpression(x.value, ctx)[0] }));
      return [{ kind: "TypedObjectExpression", properties, type: structType("", properties.map(x => ({ name: x.name, type: x.value.type }))) }, ctx];
    }
    case "FunctionExpression": {
      const [value] = typeCheckExpression(e.value, { ...ctx, ...Object.fromEntries(e.parameters.map(x => [x.name, x.type])) });
      const type = functionType(e.parameters.map(x => x.type), value.type);
      return [{ kind: 'TypedFunctionExpression', name: e.name, parameters: e.parameters, value: value as TypedBlockExpression, type }, addFunctionToContext(ctx, e.name, type)];
    }
    case "BlockExpression": {
      const [values] = e.values.reduce<[TypedExpression[], Context]>(([a, c], x) => {
        const [u, s] = typeCheckExpression(x, c);
        return [[...a, u], s];
      }, [[], ctx]);
      const type = values.length === 0 ? unitType() : values[values.length - 1].type;
      return [{ kind: "TypedBlockExpression", values, type }, ctx];
    }
    case "ApplicationExpression": {
      const [func] = typeCheckExpression(e.func, ctx);
      const args = e.args.map(x => typeCheckExpression(x, ctx)[0]);

      const type = overloadedApplicationResultType(func.type, args.map(x => x.type));

      const func1 = { ...func, type } as TypedExpression<FunctionType>;

      return [{ kind: "TypedApplicationExpression", func: func1, args, type: type.to }, ctx];
    }
    case "AddExpression": {
      const [left] = typeCheckExpression(e.left, ctx);
      const [right] = typeCheckExpression(e.right, ctx);

      switch (e.op) {
        case "+":
          switch (left.type.kind) {
            case "FloatType":
              switch (right.type.kind) {
                case "FloatType":
                  return [{ kind: "TypedAddExpression", left, op: e.op, right, type: floatType() }, ctx];
              }
              break;
            case "IntegerType":
              switch (right.type.kind) {
                case "IntegerType":
                  return [{ kind: "TypedAddExpression", left, op: e.op, right, type: integerType() }, ctx];
              }
              break;
            case "StringType":
              switch (right.type.kind) {
                case "StringType":
                  return [{ kind: "TypedAddExpression", left, op: e.op, right, type: stringType() }, ctx];
              }
              break;
            case "StructType":
              switch (right.type.kind) {
                case "StructType":
                  return [{ kind: "TypedAddExpression", left, op: e.op, right, type: structType("", [left.type.properties, right.type.properties].flat()) }, ctx];
              }
              break;
          }
          break;
      }

      throws(`Error: Cannot ${e.op} ${left.type.kind} with ${right.type.kind}`);
    }
    case "CmpExpression": {
      const [left] = typeCheckExpression(e.left, ctx);
      const [right] = typeCheckExpression(e.right, ctx);

      switch (e.op) {
        case "==":
        case "!=":
          switch (left.type.kind) {
            case "FloatType":
              switch (right.type.kind) {
                case "FloatType":
                  return [{ kind: "TypedCmpExpression", left, op: e.op, right, type: booleanType() }, ctx];
              }
              break;
            case "IntegerType":
              switch (right.type.kind) {
                case "IntegerType":
                  return [{ kind: "TypedCmpExpression", left, op: e.op, right, type: booleanType() }, ctx];
                case "PrimaryKeyType":
                  switch (right.type.of.kind) {
                    case "IntegerType":
                      return [{ kind: "TypedCmpExpression", left, op: e.op, right, type: booleanType() }, ctx];
                  }
              }
              break;
            case "StringType":
              switch (right.type.kind) {
                case "StringType":
                  return [{ kind: "TypedCmpExpression", left, op: e.op, right, type: booleanType() }, ctx];
                case "PrimaryKeyType":
                  switch (right.type.of.kind) {
                    case "StringType":
                      return [{ kind: "TypedCmpExpression", left, op: e.op, right, type: booleanType() }, ctx];
                  }
              }
              break;
            case "BooleanType":
              switch (right.type.kind) {
                case "BooleanType":
                  return [{ kind: "TypedCmpExpression", left, op: e.op, right, type: booleanType() }, ctx];
              }
              break;
            case "PrimaryKeyType":
              switch (left.type.of.kind) {
                case "IntegerType":
                  switch (right.type.kind) {
                    case "IntegerType":
                      return [{ kind: "TypedCmpExpression", left, op: e.op, right, type: booleanType() }, ctx];
                    case "PrimaryKeyType":
                      switch (right.type.of.kind) {
                        case "IntegerType":
                          return [{ kind: "TypedCmpExpression", left, op: e.op, right, type: booleanType() }, ctx];
                      }
                  }
                case "StringType":
                  switch (right.type.kind) {
                    case "StringType":
                      return [{ kind: "TypedCmpExpression", left, op: e.op, right, type: booleanType() }, ctx];
                    case "PrimaryKeyType":
                      switch (right.type.of.kind) {
                        case "StringType":
                          return [{ kind: "TypedCmpExpression", left, op: e.op, right, type: booleanType() }, ctx];
                      }
                  }
              }
          }
          break;
        case "<":
        case ">":
        case ">=":
        case "<=":
          switch (left.type.kind) {
            case "FloatType":
              switch (right.type.kind) {
                case "FloatType":
                  return [{ kind: "TypedCmpExpression", left, op: e.op, right, type: booleanType() }, ctx];
              }
              break;
            case "IntegerType":
              switch (right.type.kind) {
                case "IntegerType":
                  return [{ kind: "TypedCmpExpression", left, op: e.op, right, type: booleanType() }, ctx];
                case "PrimaryKeyType":
                  switch (right.type.of.kind) {
                    case "IntegerType":
                      return [{ kind: "TypedCmpExpression", left, op: e.op, right, type: booleanType() }, ctx];
                  }
              }
              break;
            case "PrimaryKeyType":
              switch (left.type.of.kind) {
                case "IntegerType":
                  switch (right.type.kind) {
                    case "IntegerType":
                      return [{ kind: "TypedCmpExpression", left, op: e.op, right, type: booleanType() }, ctx];
                    case "PrimaryKeyType":
                      switch (right.type.of.kind) {
                        case "IntegerType":
                          return [{ kind: "TypedCmpExpression", left, op: e.op, right, type: booleanType() }, ctx];
                      }
                  }
              }
          }
          break;
      }

      throws(`Error: Cannot ${e.op} ${left.type.kind} with ${right.type.kind}`);
    }
    case "DefaultExpression": {
      const [left] = typeCheckExpression(e.left, ctx);

      if (left.type.kind !== 'OptionalType')
        throws(`Error: Cannot apply a default expression to a non optional expression`);

      const [right] = typeCheckExpression(e.right, ctx);

      if (right.type.kind === 'ArrayType' && right.type.of.kind === 'UnitType')
        right.type = left.type.of;

      if (!typeEquals(left.type.of, right.type))
        throws(`Error: Cannot change the type of the left side of a default expression was ${typeName(left.type.of)} trying to change it to ${typeName(right.type)}`);

      return [{
        kind: "TypedDefaultExpression",
        left: left as TypedExpression<OptionalType>,
        right,
        op: "??",
        type: left.type.of,
      }, ctx];
    }
    case "ArrayExpression": {
      const values = e.values.map(x => typeCheckExpression(x, ctx)[0]);
      const types = values.map(x => x.type);
      if (types.length === 0)
        return [{ kind: "TypedArrayExpression", values, type: arrayType(unitType()) }, ctx];
      for (const [i, t] of types.entries())
        if (!typeEquals(types[0], t))
          throws(`Error: array value at idx ${i} is ${typeName(t)} which does not equal ${typeName(types[0])}`);
      return [{ kind: "TypedArrayExpression", values, type: arrayType(types[0]) }, ctx];
    }
    case "DotExpression": {
      const [left] = typeCheckExpression(e.left, ctx);

      if (left.type.kind !== 'StructType')
        throws(`Error: Left side of a dot expression needs to be a struct type`);
      if (e.right.kind !== 'IdentifierExpression')
        throws(`Error: Left side of a dot expression needs to be an identifier expression`);

      const [right] = typeCheckExpression(e.right, Object.fromEntries(left.type.properties.map(x => [x.name, x.type])));
      const type = right.type;

      return [{ kind: "TypedDotExpression", left: left as TypedIdentifierExpression<StructType>, right: right as TypedIdentifierExpression, type }, ctx];
    }
  }
}

export function typeCheckAllExpressions(expressions: Expression[], ctx?: Context): [TypedExpression[], Context] {
  return expressions.reduce<[TypedExpression[], Context]>(([a, c], e) => {
    const [e1, c1] = typeCheckExpression(e, c);
    return [[...a, e1], c1];
  }, [[], ctx ?? {}]);
}

export function isValidExpression(e: Expression, ctx: Context): boolean {
  try {
    typeCheckExpression(e, ctx);
    return true;
  }
  catch (e) {
    return false;
  }
}

export function hasOverload(name: string, args: Type[], ctx: Context): boolean {
  return isValidExpression({
    kind: "ApplicationExpression",
    func: { kind: "IdentifierExpression", name },
    args: args.map((_, i) => ({ kind: "IdentifierExpression", name: `__${i}` }))
  }, { ...ctx, ...Object.fromEntries(args.map((t, i) => [`__${i}`, t])) });
}
