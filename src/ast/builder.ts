import { ApplicationExpression, ArrayExpression, AssignExpression, BooleanExpression, DeclareExpression, EvalExpression, Expression, FloatExpression, IdentifierExpression, IntegerExpression, LetExpression, NullExpression, ObjectExpression, PlaceholderExpression, StringExpression, TableExpression } from "./relt/source";
import { AnyType, ArrayType, BooleanType, FloatType, IntegerType, NullType, ObjectType, OptionalType, StringType, TableType, Type } from "./relt/type";

export const relt = {
  type: {
    get integer(): IntegerType {
      return { kind: "IntegerType" };
    },
    get string(): StringType {
      return { kind: "StringType" };
    },
    get float(): FloatType {
      return { kind: "FloatType" };
    },
    get boolean(): BooleanType {
      return { kind: "BooleanType" };
    },
    get null(): NullType {
      return { kind: "NullType" };
    },
    get any(): AnyType {
      return { kind: "AnyType" };
    },
    table(name: string, columns: TableType['columns']): TableType {
      return { kind: "TableType", name, columns };
    },
    object(properties: ObjectType['properties']): ObjectType {
      return { kind: "ObjectType", properties };
    },
    array(of: Type): ArrayType {
      return { kind: "ArrayType", of };
    },
    optional(of: Type): OptionalType {
      return { kind: "OptionalType", of };
    },
  },
  source: {
    let(name: string, value: Expression): LetExpression {
      return { kind: "LetExpression", name, value };
    },
    integer(value: number): IntegerExpression {
      return { kind: "IntegerExpression", value };
    },
    identifier(name: string): IdentifierExpression {
      return { kind: "IdentifierExpression", name };
    },
    string(value: string): StringExpression {
      return { kind: "StringExpression", value };
    },
    boolean(value: boolean): BooleanExpression {
      return { kind: "BooleanExpression", value };
    },
    float(value: string): FloatExpression {
      return { kind: "FloatExpression", value };
    },
    object(properties: Expression[]): ObjectExpression {
      return { kind: "ObjectExpression", properties };
    },
    array(values: Expression[]): ArrayExpression {
      return { kind: "ArrayExpression", values };
    },
    application(left: Expression, right: Expression): ApplicationExpression {
      return { kind: "ApplicationExpression", left, right };
    },
    placeholder(name: string): PlaceholderExpression {
      return { kind: "PlaceholderExpression", name };
    },
    eval(node: Expression): EvalExpression {
      return { kind: "EvalExpression", node };
    },
    table(name: string, columns: Expression[]): TableExpression {
      return { kind: "TableExpression", name, columns };
    },
    declare(left: Expression, right: Type): DeclareExpression {
      return { kind: "DeclareExpression", left, right };
    },
    assign(left: Expression, right: Expression): AssignExpression {
      return { kind: "AssignExpression", left, right };
    },
    get null(): NullExpression {
      return { kind: "NullExpression" }
    },
  }
}
