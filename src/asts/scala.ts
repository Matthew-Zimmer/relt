
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

// export type DatasetHandler =
//   | SourceDatasetHandler
//   | DerivedDatasetHandler

// export interface SourceDatasetHandler {
//   kind: "SourceDatasetHandler";
//   typeName: string;
//   datasetIndex: number;
//   datasetCount: number;
//   connectionInfo: SparkConnectionInfo;
// }

// export type SparkConnectionInfo =
//   | SparkDBConnectionInfo

// export interface SparkDBConnectionInfo {
//   kind: "SparkDBConnectionInfo";
//   host: string;
//   port: number;
//   user: string;
//   password: string;
//   table: string;
//   // TODO
//   // columns: string[]; 
// }

// export interface DerivedDatasetHandler {
//   kind: "DerivedDatasetHandler";
//   typeName: string;
//   datasetIndex: number;
//   datasetCount: number;
//   parentDatasets: { name: string, index: number }[];
//   rules: SparkRule[];
// }

// export type SparkRule =
//   | SparkJoinRule
//   | SparkMapRule
//   | SparkAsRule
//   | SparkIdentityRule
//   | SparkReturnRule
//   | SparkGroupAggRule

// export interface SparkJoinRule {
//   kind: "SparkJoinRule";
//   name: string;
//   left: string;
//   right: string;
//   type: "inner" | "outer" | "left" | "right";
//   leftColumn: string;
//   rightColumn: string;
// }

// export interface SparkMapRule {
//   kind: "SparkMapRule";
//   name: string;
//   dataset: string;
//   transformations: SparkMapTransformation[];
// }

// export interface SparkAsRule {
//   kind: "SparkAsRule";
//   name: string;
//   dataset: string;
//   type: string;
// }

// export interface SparkIdentityRule {
//   kind: "SparkIdentityRule";
//   name: string;
// }

// export interface SparkReturnRule {
//   kind: "SparkReturnRule";
//   name: string;
// }

// export interface SparkGroupAggRule {
//   kind: "SparkGroupAggRule";
//   name: string;
//   dataset: string;
//   groupColumn: string;
//   aggregations: SparkAggregation[];
// }

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
  | SparkRowExtractRule
  | SparkApplicationRule
  | SparkIdentityRule
  | SparkBinaryOperationRule
  | SparkGetOrElseRule

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
  op: "+";
  right: string;
}

export interface SparkGetOrElseRule {
  kind: "SparkGetOrElseRule";
  name: string;
  left: string;
  right: string;
}

// export interface SparkDependencyVertex {
//   kind: "SparkDependencyVertex";
//   id: number;
//   name: string;
//   incoming: number[];
//   outgoing: number[];
// }

// export interface SparkProject {
//   kind: "SparkProject";
//   implicitCaseClasses: ScalaCaseClass[];
//   types: SparkType[];
//   vertices: SparkDependencyVertex[];
//   name: string;
//   package: string;
// }

export type SparkDatasetHandler =
  | SparkDBSourceDatasetHandler
  | SparkJoinDatasetHandler
  | SparkDropDatasetHandler
  | SparkMapDatasetHandler
  | SparkGroupDatasetHandler
  | SparkUnionDatasetHandler

export interface DatasetId {
  name: string;
  idx: number;
}

export interface SparkDBSourceDatasetHandler {
  kind: "SparkDBSourceDatasetHandler";
  output: DatasetId;
  host: string;
  port: number;
  user: string;
  password: string;
  table: string;
  columns: string[];
}

export interface SparkJoinDatasetHandler {
  kind: "SparkJoinDatasetHandler";
  leftInput: DatasetId;
  rightInput: DatasetId;
  output: DatasetId;
  leftColumn: string;
  rightColumn: string;
  method: string;
}

export interface SparkUnionDatasetHandler {
  kind: "SparkUnionDatasetHandler";
  leftInput: DatasetId;
  rightInput: DatasetId;
  output: DatasetId;
}

export interface SparkDropDatasetHandler {
  kind: "SparkDropDatasetHandler";
  input: DatasetId;
  output: DatasetId;
  properties: string[];
}

export interface SparkMapDatasetHandler {
  kind: "SparkMapDatasetHandler";
  input: DatasetId;
  output: DatasetId;
  rules: SparkMapRule[];
}

export interface SparkGroupDatasetHandler {
  kind: "SparkGroupDatasetHandler";
  input: DatasetId;
  output: DatasetId;
  column: string;
  aggregations: SparkAggregation[];
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
}
