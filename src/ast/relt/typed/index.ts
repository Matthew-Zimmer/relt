import { ArrayType, BooleanType, FloatType, FunctionType, IntegerType, NeverType, NullType, ObjectType, OptionalType, StringType, TableType, TupleType, Type } from '../type';

export type TypedExpression<T extends Type = Type> =
  | TypedLetExpression
  | TypedTableExpression
  | TypedFunctionExpression
  | TypedEvalExpression
  | TypedDeclareExpression
  | TypedAssignExpression
  | TypedConditionalExpression
  | TypedOrExpression
  | TypedAndExpression
  | TypedCmpExpression
  | TypedAddExpression
  | TypedMulExpression
  | TypedUnionExpression
  | TypedJoinExpression
  | TypedGroupByExpression
  | TypedWhereExpression
  | TypedWithExpression
  | TypedDropExpression
  | TypedSelectExpression
  | TypedDotExpression
  | TypedApplicationExpression
  | TypedIdentifierExpression
  | TypedPlaceholderExpression
  | TypedIntegerExpression
  | TypedFloatExpression
  | TypedStringExpression
  | TypedEnvExpression
  | TypedBooleanExpression
  | TypedNullExpression
  | TypedBlockExpression
  | TypedObjectExpression
  | TypedArrayExpression
  | TypedSpreadExpression
  | TypedIndexExpression
  | TypedInternalExpression

export type TypedLetExpressionValue =
  | TypedAssignExpression
  | TypedDeclareExpression

export interface TypedLetExpression<V extends TypedLetExpressionValue = TypedLetExpressionValue, T extends Type = Type> {
  kind: "TypedLetExpression";
  value: V;
  type: T;
}

export type TypedTableExpressionValue =
  | TypedAssignExpression<TypedIdentifierExpression>

export interface TypedTableExpression<V extends TypedTableExpressionValue = TypedTableExpressionValue, T extends Type = Type> {
  kind: "TypedTableExpression";
  value: V;
  type: TableType;
}

export interface TypedFunctionExpression {
  kind: "TypedFunctionExpression";
  name?: string;
  args: TypedExpression[];
  value: TypedExpression;
  type: FunctionType;
}

export interface TypedEvalExpression {
  kind: "TypedEvalExpression";
  node: TypedExpression;
  type: NeverType;
}

export type TypedTypedDeclareExpressionValue =
  | TypedIdentifierExpression

export interface TypedDeclareExpression<V extends TypedTypedDeclareExpressionValue = TypedTypedDeclareExpressionValue, T extends Type = Type> {
  kind: "TypedDeclareExpression";
  value: V;
  type: T;
}

export type TypedAssignExpressionValue =
  | TypedIdentifierExpression
  | TypedArrayExpression<TypedAssignExpressionValue>
  | TypedObjectExpression<TypedAssignExpression>

export interface TypedAssignExpression<V extends TypedAssignExpressionValue = TypedAssignExpressionValue, T extends Type = Type> {
  kind: "TypedAssignExpression";
  left: V;
  op: "=";
  right: TypedExpression<T>;
  type: T;
}

export interface TypedConditionalExpression<T extends Type = Type> {
  kind: "TypedConditionalExpression";
  left: TypedExpression<OptionalType<T>>;
  op: "??" | "!?";
  right: TypedExpression<T>;
  type: T;
}

export interface TypedOrExpression {
  kind: "TypedOrExpression";
  left: TypedExpression;
  op: "||";
  right: TypedExpression;
  type: Type;
}

export interface TypedAndExpression {
  kind: "TypedAndExpression";
  left: TypedExpression;
  op: "&&";
  right: TypedExpression;
  type: Type;
}

export interface TypedCmpExpression {
  kind: "TypedCmpExpression";
  left: TypedExpression;
  op: "==" | "!=" | "<=" | ">=" | "<" | ">";
  right: TypedExpression;
  type: BooleanType;
}

export interface TypedAddExpression {
  kind: "TypedAddExpression";
  left: TypedExpression;
  op: "+" | "-";
  right: TypedExpression;
  type: Type;
}

export interface TypedMulExpression {
  kind: "TypedMulExpression";
  left: TypedExpression;
  op: "*" | "/" | "%";
  right: TypedExpression;
  type: Type;
}

export interface TypedUnionExpression {
  kind: "TypedUnionExpression";
  left: TypedExpression<TableType>;
  right: TypedExpression<TableType>;
  type: TableType;
}

export interface TypedJoinExpression {
  kind: "TypedJoinExpression";
  left: TypedExpression<TableType>;
  right: TypedExpression<TableType>;
  method: "inner" | "left" | "right";
  on: TypedExpression;
  type: TableType;
}

export type TypedColumnType =
  | TypedIdentifierExpression
  | TypedArrayExpression<TypedIdentifierExpression>

export interface TypedGroupByExpression<C extends TypedColumnType = TypedColumnType> {
  kind: "TypedGroupByExpression";
  value: TypedExpression<TableType>;
  by: C;
  agg: TypedAssignExpression<TypedIdentifierExpression> | TypedObjectExpression<TypedAssignExpression<TypedIdentifierExpression>>;
  type: TableType;
}

export interface TypedWhereExpression {
  kind: "TypedWhereExpression";
  left: TypedExpression<TableType>;
  right: TypedExpression<TableType>;
  type: TableType;
}

export interface TypedWithExpression {
  kind: "TypedWithExpression";
  left: TypedExpression<TableType>;
  right: TypedObjectExpression;
  type: TableType;
}

export interface TypedDropExpression<C extends TypedColumnType = TypedColumnType> {
  kind: "TypedDropExpression";
  left: TypedExpression<TableType>;
  right: C;
  type: TableType;
}

export interface TypedSelectExpression<C extends TypedColumnType = TypedColumnType> {
  kind: "TypedSelectExpression";
  left: TypedExpression<TableType>;
  right: C;
  type: TableType;
}

export interface TypedDotExpression {
  kind: "TypedDotExpression";
  left: TypedExpression<ObjectType>;
  right: TypedExpression;
  type: Type;
}

export interface TypedApplicationExpression {
  kind: "TypedApplicationExpression";
  left: TypedExpression<FunctionType>;
  right: TypedExpression;
  type: Type;
}

export interface TypedIdentifierExpression {
  kind: "TypedIdentifierExpression";
  name: string;
  type: Type;
}

export interface TypedPlaceholderExpression {
  kind: "TypedPlaceholderExpression";
  name: string;
  typeCondition?: string;
  kindCondition?: string;
  type: Type;
}

export interface TypedIntegerExpression {
  kind: "TypedIntegerExpression";
  value: number;
  type: IntegerType;
}

export interface TypedFloatExpression {
  kind: "TypedFloatExpression";
  value: string;
  type: FloatType;
}

export interface TypedStringExpression {
  kind: "TypedStringExpression";
  value: string;
  type: StringType;
}

export interface TypedEnvExpression {
  kind: "TypedEnvExpression";
  value: string;
  type: StringType;
}

export interface TypedBooleanExpression {
  kind: "TypedBooleanExpression";
  value: boolean;
  type: BooleanType;
}

export interface TypedNullExpression {
  kind: "TypedNullExpression";
  type: NullType;
}

export interface TypedBlockExpression<T extends Type = Type> {
  kind: "TypedBlockExpression";
  expressions: TypedExpression[];
  type: T;
}

export type TypedObjectExpressionProperty =
  | TypedIdentifierExpression
  | TypedDeclareExpression
  | TypedAssignExpression
  | TypedSpreadExpression

export interface TypedObjectExpression<P extends TypedObjectExpressionProperty = TypedObjectExpressionProperty> {
  kind: "TypedObjectExpression";
  properties: P[];
  type: ObjectType;
}

export interface TypedArrayExpression<T extends TypedExpression = TypedExpression> {
  kind: "TypedArrayExpression";
  values: T[];
  type: ArrayType | TupleType;
}

export interface TypedSpreadExpression {
  kind: "TypedSpreadExpression";
  value: TypedExpression<ObjectType>;
  type: ObjectType;
}

export interface TypedIndexExpression {
  kind: "TypedIndexExpression";
  left: TypedExpression<ArrayType | TupleType>;
  index: TypedExpression<IntegerType>;
  type: Type;
}

export interface TypedInternalExpression {
  kind: "TypedInternalExpression";
  value: (e: TypedExpression) => TypedExpression,
  type: NeverType;
}

