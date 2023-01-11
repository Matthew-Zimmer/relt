
export interface SparkProject {
  kind: "SparkProject";
  name: string;
  package: string;
  imports: ScalaImport[];
  datasets: SparkDatasetHandler[];
  types: ScalaCaseClass[];
}

export interface ScalaImport {
  kind: "ScalaImport";
  package: string;
  names: string[];
}

export type DatasetId = { name: string, idx: number };

export interface SparkDatasetHandler {
  kind: "SparkDatasetHandler";
  inputs: DatasetId[];
  output: DatasetId;
  construct: ScalaExpression;
}

export interface ScalaCaseClass {
  kind: "ScalaCaseClass";
  name: string;
  properties: { name: string, type: ScalaType }[];
}

export type ScalaExpression =
  | ScalaIntegerExpression
  | ScalaFloatExpression
  | ScalaBooleanExpression
  | ScalaStringExpression
  | ScalaBlockExpression
  | ScalaApplicationExpression
  | ScalaBinaryExpression
  | ScalaLUnaryExpression
  | ScalaRUnaryExpression
  | ScalaVarExpression

export interface ScalaIntegerExpression {
  kind: "ScalaIntegerExpression";
  value: number;
}

export interface ScalaFloatExpression {
  kind: "ScalaFloatExpression";
  value: string;
}

export interface ScalaBooleanExpression {
  kind: "ScalaBooleanExpression";
  value: boolean;
}

export interface ScalaStringExpression {
  kind: "ScalaStringExpression";
  value: string;
}

export interface ScalaBlockExpression {
  kind: "ScalaBlockExpression";
  expressions: ScalaExpression[];
}

export interface ScalaApplicationExpression {
  kind: "ScalaApplicationExpression";
  func: ScalaExpression;
  args: ScalaExpression[];
}

export interface ScalaBinaryExpression {
  kind: "ScalaBinaryExpression";
  left: ScalaExpression;
  op: string;
  right: ScalaExpression;
}

export interface ScalaLUnaryExpression {
  kind: "ScalaLUnaryExpression";
  left: ScalaExpression;
  op: string;
}

export interface ScalaRUnaryExpression {
  kind: "ScalaRUnaryExpression";
  op: string;
  right: ScalaExpression;
}

export interface ScalaVarExpression {
  kind: "ScalaVarExpression";
  name: string;
  value: ScalaExpression;
}

export type ScalaType =
  | ScalaIntegerType
  | ScalaDoubleType
  | ScalaBooleanType
  | ScalaStringType
  | ScalaApplicationType
  | ScalaIdentifierType

export interface ScalaIntegerType {
  kind: "ScalaIntegerType";
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

export interface ScalaApplicationType {
  kind: "ScalaApplicationType";
  of: ScalaType;
  args: ScalaType[];
}

export interface ScalaIdentifierType {
  kind: "ScalaIdentifierType";
  name: string;
}
