// import { TableType, Type } from "../ast/relt/type";
// import { typeName } from "../ast/relt/type/utils";
// import { TypedExpression, TypedTableExpression } from "../ast/relt/typed";
// import { visitMap, visitVoid } from "../ast/relt/typed/utils";
// import { ScalaCaseClass, ScalaType, SparkDatasetHandler, SparkProject } from "../ast/scala";
// import { TableInfo } from "./derviveTableInfo";

// export type TableGenerativeExpression =
//   | TypedTableExpression

// export function gatherExpressionsWhichGenerateTables(e: TypedExpression): TableGenerativeExpression[] {
//   const types: TableGenerativeExpression[] = [];
//   visitVoid(e, {
//     TypedTableExpression: e => {
//       types.push(e);
//     },
//   });
//   return types;
// }

// export function createDatasetHandlers(e: TableGenerativeExpression[], info: TableInfo[]): SparkDatasetHandler[] {

//   const createDatasetHandler = (e: TableGenerativeExpression, info: TableInfo, idx: number): SparkDatasetHandler => {
//     const empty: SparkDatasetHandler = {
//       kind: "SparkDatasetHandler",
//       construct: { kind: "ScalaBooleanExpression", value: false },
//       inputs: [],
//       output: { name: info.name, idx },
//     }
//     switch (e.kind) {
//       case "TypedTableExpression": {
//         return info.data.reduce((p, c) => c.handler(p, info, e.type), empty);
//       }
//     }
//   };

//   return e.map((x, i) => {
//     const tableInfo = info.find(i => i.name === x.type.name);
//     if (tableInfo === undefined)
//       throw new Error(``);
//     return createDatasetHandler(x, tableInfo, i);
//   });
// }

// export function convertToScalaType(t: Type): ScalaType {
//   switch (t.kind) {
//     case "IntegerType": return { kind: "ScalaIntegerType" };
//     case "StringType": return { kind: "ScalaStringType" };
//     case "FloatType": return { kind: "ScalaDoubleType" };
//     case "BooleanType": return { kind: "ScalaBooleanType" };
//     case "TableType": return { kind: "ScalaIdentifierType", name: t.name };
//     case "ArrayType": return { kind: "ScalaApplicationType", of: { kind: "ScalaIdentifierType", name: "Array" }, args: [convertToScalaType(t.of)] };
//     case "OptionalType": return { kind: "ScalaApplicationType", of: { kind: "ScalaIdentifierType", name: "Option" }, args: [convertToScalaType(t.of)] };
//     case "FunctionType":
//     case "AnyType":
//     case "NullType":
//     case "ObjectType":
//       throw new Error(`Cannot convert type: ${typeName(t)} to scala`);
//   }
// }

// export function createCaseClasses(e: TableGenerativeExpression[]): ScalaCaseClass[] {
//   const createCaseClass = (e: TableGenerativeExpression): ScalaCaseClass => {
//     return {
//       kind: "ScalaCaseClass",
//       name: e.type.name,
//       properties: e.type.columns.map(x => ({ ...x, type: convertToScalaType(x.type) })),
//     }
//   };

//   return e.map(createCaseClass);
// }

// export function toSpark(e: TypedExpression, info: TableInfo[]): SparkProject {
//   const genTableExprs = gatherExpressionsWhichGenerateTables(e);
//   // need some de-duping

//   const datasets = createDatasetHandlers(genTableExprs, info);
//   const types = createCaseClasses(genTableExprs);

//   return {
//     kind: "SparkProject",
//     name: "",
//     package: "",
//     imports: [],
//     datasets,
//     types,
//   };
// }
