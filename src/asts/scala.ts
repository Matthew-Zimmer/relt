
export interface SparkType {
  kind: "SparkType";
  caseClass: ScalaCaseClass;
  datasetHandler: DatasetHandler;
}

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

export type DatasetHandler =
  | SourceDatasetHandler
  | DerivedDatasetHandler

export interface SourceDatasetHandler {
  kind: "SourceDatasetHandler";
  typeName: string;
  datasetIndex: number;
  datasetCount: number;
}

export interface DerivedDatasetHandler {
  kind: "DerivedDatasetHandler";
  typeName: string;
  datasetIndex: number;
  datasetCount: number;
  parentDatasets: { name: string, index: number }[];
  rules: SparkRule[];
}

export type SparkRule =
  | SparkJoinRule
  | SparkMapRule
  | SparkAsRule
  | SparkIdentityRule
  | SparkReturnRule

export interface SparkJoinRule {
  kind: "SparkJoinRule";
  name: string;
  left: string;
  right: string;
  type: "inner" | "outer" | "left" | "right";
  leftColumn: string;
  rightColumn: string;
}

export interface SparkMapRule {
  kind: "SparkMapRule";
  name: string;
  dataset: string;
  transformations: SparkMapTransformation[];
}

export interface SparkAsRule {
  kind: "SparkAsRule";
  name: string;
  dataset: string;
  type: string;
}

export interface SparkIdentityRule {
  kind: "SparkIdentityRule";
  name: string;
}

export interface SparkReturnRule {
  kind: "SparkReturnRule";
  name: string;
}

export type SparkMapTransformation =
  | SparkRowExtractTransformation
  | SparkApplicationTransformation
  | SparkReturnTransformation

export interface SparkRowExtractTransformation {
  kind: "SparkRowExtractTransformation";
  name: string;
  property: string;
}

export interface SparkApplicationTransformation {
  kind: "SparkApplicationTransformation";
  name: string;
  func: string;
  args: string[];
}

export interface SparkReturnTransformation {
  kind: "SparkReturnTransformation";
  name: string;
}

export interface SparkProject {
  kind: "SparkProject";
  types: SparkType[];
  name: string;
}
