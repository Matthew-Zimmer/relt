import { LibraryDeclaration } from "./topLevel";

export interface ScalaCaseClass {
  kind: "ScalaCaseClass";
  name: string;
  properties: { name: string, type: ScalaType }[];
}

export type ScalaType =
  | ScalaIntType
  | ScalaDoubleType
  | ScalaBooleanType
  | ScalaStringType
  | ScalaDateType
  | ScalaIdentifierType
  | ScalaArrayType
  | ScalaOptionalType
  | ScalaUnitType

export interface ScalaIntType {
  kind: "ScalaIntType";
}

export interface ScalaDoubleType {
  kind: "ScalaDoubleType";
}

export interface ScalaBooleanType {
  kind: "ScalaBooleanType";
}

export interface ScalaStringType {
  kind: "ScalaStringType";
}

export interface ScalaDateType {
  kind: "ScalaDateType";
}

export interface ScalaIdentifierType {
  kind: "ScalaIdentifierType";
  name: string;
}

export interface ScalaArrayType {
  kind: "ScalaArrayType";
  of: ScalaType;
}

export interface ScalaOptionalType {
  kind: "ScalaOptionalType";
  of: ScalaType;
}

export interface ScalaUnitType {
  kind: "ScalaUnitType";
}

export type SparkAggregation =
  | SparkCollectListAggregation
  | SparkSqlAggregation

export interface SparkCollectListAggregation {
  kind: "SparkCollectListAggregation";
  name: string;
  columns: string[];
}

export interface SparkSqlAggregation {
  kind: "SparkSqlAggregation";
  func: "sum" | "count" | "max" | "min";
  name: string;
  column: string;
}

export type SparkMapRule =
  | SparkApplicationRule
  | SparkIdentityRule
  | SparkBinaryOperationRule
  | SparkGetOrElseRule
  | SparkDotRule

export interface SparkRowExtractRule {
  kind: "SparkRowExtractRule";
  name: string;
  property: string;
}

export interface SparkApplicationRule {
  kind: "SparkApplicationRule";
  name: string;
  func: string;
  args: string[];
}

export interface SparkIdentityRule {
  kind: "SparkIdentityRule";
  name: string;
}

export interface SparkBinaryOperationRule {
  kind: "SparkBinaryOperationRule";
  name: string;
  left: string;
  op: "+" | "==" | "!=" | "<=" | ">=" | "<" | ">";
  right: string;
}

export interface SparkGetOrElseRule {
  kind: "SparkGetOrElseRule";
  name: string;
  left: string;
  right: string;
}

export interface SparkDotRule {
  kind: "SparkDotRule";
  name: string;
  left: string;
  right: string;
}

export interface BaseSparkDatasetHandler {
  show?: boolean;
}

export type SparkDatasetHandler =
  | SparkDBSourceDatasetHandler
  | SparkFileSourceDatasetHandler
  | SparkJoinDatasetHandler
  | SparkDropDatasetHandler
  | SparkMapDatasetHandler
  | SparkGroupDatasetHandler
  | SparkUnionDatasetHandler
  | SparkSortDatasetHandler
  | SparkFilterDatasetHandler
  // | SparkWindowDatasetHandler
  | SparkDistinctDatasetHandler
  | SparkRepartitionDatasetHandler

export interface DatasetId {
  name: string;
  idx: number;
}

export interface SparkDBSourceDatasetHandler extends BaseSparkDatasetHandler {
  kind: "SparkDBSourceDatasetHandler";
  output: DatasetId;
  host: string;
  port: number;
  user: string;
  password: string;
  table: string;
  columns: string[];
}

export interface SparkFileSourceDatasetHandler extends BaseSparkDatasetHandler {
  kind: "SparkFileSourceDatasetHandler";
  output: DatasetId;
  path: string;
  format: 'json';
}

export interface SparkJoinDatasetHandler extends BaseSparkDatasetHandler {
  kind: "SparkJoinDatasetHandler";
  leftInput: DatasetId;
  rightInput: DatasetId;
  output: DatasetId;
  leftColumn: string;
  rightColumn: string;
  method: string;
}

export interface SparkUnionDatasetHandler extends BaseSparkDatasetHandler {
  kind: "SparkUnionDatasetHandler";
  leftInput: DatasetId;
  rightInput: DatasetId;
  output: DatasetId;
}

export interface SparkDropDatasetHandler extends BaseSparkDatasetHandler {
  kind: "SparkDropDatasetHandler";
  input: DatasetId;
  output: DatasetId;
  properties: string[];
}

export interface SparkMapDatasetHandler extends BaseSparkDatasetHandler {
  kind: "SparkMapDatasetHandler";
  input: DatasetId;
  output: DatasetId;
  rules: SparkMapRule[];
}

export interface SparkGroupDatasetHandler extends BaseSparkDatasetHandler {
  kind: "SparkGroupDatasetHandler";
  input: DatasetId;
  output: DatasetId;
  column: string;
  aggregations: SparkAggregation[];
}

export interface SparkSortDatasetHandler extends BaseSparkDatasetHandler {
  kind: "SparkSortDatasetHandler";
  input: DatasetId;
  output: DatasetId;
  columns: SparkSortExpression[];
}

export interface SparkFilterDatasetHandler extends BaseSparkDatasetHandler {
  kind: "SparkFilterDatasetHandler";
  input: DatasetId;
  output: DatasetId;
  condition: SparkExpression;
}

export interface SparkWindowDatasetHandler extends BaseSparkDatasetHandler {
  kind: "SparkWindowDatasetHandler";
  input: DatasetId;
  output: DatasetId;
}

export interface SparkDistinctDatasetHandler extends BaseSparkDatasetHandler {
  kind: "SparkDistinctDatasetHandler";
  input: DatasetId;
  output: DatasetId;
  columns: SparkStringExpression[];
}

export interface SparkRepartitionDatasetHandler extends BaseSparkDatasetHandler {
  kind: "SparkRepartitionDatasetHandler";
  input: DatasetId;
  output: DatasetId;
  count: number;
}

export interface SparkDependencyVertex {
  kind: "SparkDependencyVertex";
  id: number;
  name: string;
  incoming: number[];
  outgoing: number[];
}

export interface SparkProject {
  kind: "SparkProject";
  name: string;
  package: string;
  datasetHandlers: SparkDatasetHandler[];
  caseClasses: ScalaCaseClass[];
  vertices: SparkDependencyVertex[];
  libraries: LibraryDeclaration[];
}


export type SparkSourceDataSet =
  | SparkDBSourceDatasetHandler
  | SparkFileSourceDatasetHandler

export function isSparkSourceDataSet(h: SparkDatasetHandler): h is SparkSourceDataSet {
  switch (h.kind) {
    case "SparkDBSourceDatasetHandler":
    case "SparkFileSourceDatasetHandler":
      return true;
    case "SparkDropDatasetHandler":
    case "SparkGroupDatasetHandler":
    case "SparkJoinDatasetHandler":
    case "SparkMapDatasetHandler":
    case "SparkUnionDatasetHandler":
    case "SparkSortDatasetHandler":
    case "SparkFilterDatasetHandler":
    case "SparkDistinctDatasetHandler":
    case "SparkRepartitionDatasetHandler":
      return false;
  }
}

export type SparkExpression =
  | SparkBinaryOperatorExpression
  | SparkLeftUnaryOperatorExpression
  | SparkRightUnaryOperatorExpression
  | SparkIdentifierExpression
  | SparkIntegerExpression
  | SparkFloatExpression
  | SparkBooleanExpression
  | SparkStringExpression
  | SparkApplicationExpression

export type SparkBinaryOp = "&&" | "||" | "===" | "." | "+" | "-" | "*" | "/" | "<=" | ">=" | "=!=" | "<" | ">";
export type SparkLeftUnaryOp = "";
export type SparkRightUnaryOp = "!";

export interface SparkBinaryOperatorExpression<
  L extends SparkExpression = SparkExpression,
  O extends SparkBinaryOp = SparkBinaryOp,
  R extends SparkExpression = SparkExpression,
> {
  kind: "SparkBinaryOperatorExpression";
  left: L;
  op: O;
  right: R;
}

export interface SparkLeftUnaryOperatorExpression<
  L extends SparkExpression = SparkExpression,
  O extends SparkLeftUnaryOp = SparkLeftUnaryOp,
> {
  kind: "SparkLeftUnaryOperatorExpression";
  op: O;
  left: L;
}

export interface SparkRightUnaryOperatorExpression<
  O extends SparkRightUnaryOp = SparkRightUnaryOp,
  R extends SparkExpression = SparkExpression,
> {
  kind: "SparkRightUnaryOperatorExpression";
  op: O;
  right: R;
}

export interface SparkApplicationExpression<
  F extends SparkExpression = SparkExpression,
  A extends SparkExpression[] = SparkExpression[],
> {
  kind: "SparkApplicationExpression";
  func: F;
  args: A;
}

export interface SparkIdentifierExpression<I extends string = string> {
  kind: "SparkIdentifierExpression";
  name: I;
}

export interface SparkIntegerExpression {
  kind: "SparkIntegerExpression";
  value: number;
}

export interface SparkFloatExpression {
  kind: "SparkFloatExpression";
  value: number;
}

export interface SparkBooleanExpression {
  kind: "SparkBooleanExpression";
  value: boolean;
}

export interface SparkStringExpression {
  kind: "SparkStringExpression";
  value: string;
}

// Compound types
export type SparkDatasetColumnExpression = SparkApplicationExpression<SparkBinaryOperatorExpression<SparkIdentifierExpression, ".", SparkIdentifierExpression<'col'>>, [SparkStringExpression]>;
export type SparkSortExpression = SparkBinaryOperatorExpression<SparkDatasetColumnExpression, '.', SparkIdentifierExpression<'asc_nulls_first' | 'asc_nulls_last' | 'desc_nulls_first' | 'desc_nulls_last'>>;

// constructors
export const spark = {
  expr: {
    integer(value: number): SparkIntegerExpression { return { kind: "SparkIntegerExpression", value } },
    float(value: number): SparkFloatExpression { return { kind: "SparkFloatExpression", value } },
    boolean(value: boolean): SparkBooleanExpression { return { kind: "SparkBooleanExpression", value } },
    string(value: string): SparkStringExpression { return { kind: "SparkStringExpression", value } },
    binOp<L extends SparkExpression, O extends SparkBinaryOp, R extends SparkExpression>(left: L, op: O, right: R): SparkBinaryOperatorExpression<L, O, R> { return { kind: "SparkBinaryOperatorExpression", left, op, right } },
    leftUnaryOp<L extends SparkExpression, O extends SparkLeftUnaryOp>(left: L, op: O): SparkLeftUnaryOperatorExpression<L, O> { return { kind: "SparkLeftUnaryOperatorExpression", left, op } },
    rightUnaryOp<O extends SparkRightUnaryOp, R extends SparkExpression>(op: O, right: R): SparkRightUnaryOperatorExpression<O, R> { return { kind: "SparkRightUnaryOperatorExpression", op, right } },
    identifier<I extends string>(name: I): SparkIdentifierExpression<I> { return { kind: "SparkIdentifierExpression", name } },
    app<F extends SparkExpression, A extends SparkExpression[]>(func: F, ...args: A): SparkApplicationExpression<F, A> { return { kind: "SparkApplicationExpression", func, args } },
    dsCol(name: string, col: string): SparkDatasetColumnExpression { return this.app(this.binOp(this.identifier(name), '.', this.identifier('col')), this.string(col)) },
    sort(name: string, col: string, order: 'asc' | 'desc', nulls: 'first' | 'last'): SparkSortExpression { return this.binOp(this.dsCol(name, col), '.', this.identifier(`${order}_nulls_${nulls}`)) },
  }
}
