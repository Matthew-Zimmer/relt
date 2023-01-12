import { BooleanType, FloatType, FunctionType, IntegerType, NeverType, NullType, ObjectType, OptionalType, StringType, TableType, Type } from '../type';

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
  type: T;
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
  type: Type;
}

export interface TypedSpreadExpression {
  kind: "TypedSpreadExpression";
  value: TypedExpression<ObjectType>;
  type: ObjectType;
}

// export interface TypedTypedLetExpression {
//   kind: "TypedTypedLetExpression";
//   name: string;
//   value: TypedTypedExpression;
//   type: Type;
// }

// export interface TypedTypedIntegerExpression {
//   kind: "TypedTypedIntegerExpression";
//   value: number;
//   type: Type;
// }

// export interface TypedTypedIdentifierExpression {
//   kind: "TypedTypedIdentifierExpression";
//   name: string;
//   type: Type;
// }

// export interface TypedTypedApplicationExpression {
//   kind: "TypedTypedApplicationExpression";
//   left: TypedTypedExpression;
//   right: TypedTypedExpression;
//   type: Type;
// }

// export interface TypedTypedStringExpression {
//   kind: "TypedTypedStringExpression";
//   value: string;
//   type: Type;
// }

// export interface TypedTypedBooleanExpression {
//   kind: "TypedTypedBooleanExpression";
//   value: boolean;
//   type: Type;
// }

// export interface TypedTypedFloatExpression {
//   kind: "TypedTypedFloatExpression";
//   value: string;
//   type: Type;
// }

// export type TypedValidObjectPropertyExpression =
//   | TypedTypedAssignExpression<TypedTypedIdentifierExpression>

// export interface TypedTypedObjectExpression {
//   kind: "TypedTypedObjectExpression";
//   properties: TypedValidObjectPropertyExpression[];
//   type: Type;
// }

// export interface TypedTypedArrayExpression {
//   kind: "TypedTypedArrayExpression";
//   values: TypedTypedExpression[];
//   type: Type;
// }

// export interface TypedTypedPlaceholderExpression {
//   kind: "TypedTypedPlaceholderExpression";
//   name: string;
//   type: Type;
// }

// export interface TypedTypedEvalExpression {
//   kind: "TypedTypedEvalExpression";
//   node: TypedTypedExpression;
//   type: Type;
// }

// export type TypedValidTableColumnExpression =
//   | TypedTypedDeclareExpression<TypedTypedIdentifierExpression>

// export interface TypedTypedTableExpression {
//   kind: "TypedTypedTableExpression";
//   name: string;
//   columns: TypedValidTableColumnExpression[];
//   type: TableType;
// }

// export interface TypedTypedDeclareExpression<L extends TypedTypedExpression = TypedTypedExpression, T extends Type = Type> {
//   kind: "TypedTypedDeclareExpression";
//   left: L;
//   type: T;
// }

// export interface TypedTypedAssignExpression<L extends TypedTypedExpression = TypedTypedExpression, R extends TypedTypedExpression = TypedTypedExpression, T extends Type = Type> {
//   kind: "TypedTypedAssignExpression";
//   left: L;
//   right: R;
//   type: T;
// }

// export interface TypedTypedNullExpression {
//   kind: "TypedTypedNullExpression";
//   type: Type;
// }

// export function ofKind<K extends TypedTypedExpression['kind']>(kind: K) {
//   return <T extends TypedTypedExpression>(x: T): x is T & { kind: K } => x.kind === kind;
// }

// export function ofType<K extends Type['kind']>(kind: K) {
//   return <T extends TypedTypedExpression>(x: T): x is T & { type: Type & { kind: K } } => x.type.kind === kind;
// }

// export function ofLeft<K extends TypedTypedExpression['kind']>(kind: K) {
//   return <T extends { left: TypedTypedExpression }>(x: T): x is T & { left: TypedTypedExpression & { kind: K } } => x.left.kind === kind;
// }

// export function ofRight<K extends TypedTypedExpression['kind']>(kind: K) {
//   return <T extends { right: TypedTypedExpression }>(x: T): x is T & { right: TypedTypedExpression & { kind: K } } => x.right.kind === kind;
// }
