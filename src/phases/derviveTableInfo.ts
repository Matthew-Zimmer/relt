// import { Expression } from "../ast/relt/source";
// import { normalize, rewrite, visit, visitMap } from "../ast/relt/source/vistor";
// import z from 'zod';
// import { relt } from "../ast/builder";

// const tableInfoSchema = z.object({
//   name: z.string(),
//   data: z.array(z.object({
//     name: z.string(),
//     handler: z.function(z.tuple([]),)
//   })),
// });

// export type TableInfo = z.TypeOf<typeof tableInfoSchema>;

// export function toJSUnsafe(e: Expression): unknown {
//   switch (e.kind) {
//     case "IntegerExpression":
//     case "StringExpression":
//     case "BooleanExpression":
//       return e.value;
//     case "FloatExpression":
//       return Number(e.value);
//     case "ObjectExpression": {
//       const entries: [string, unknown][] = [];

//       for (const prop of e.properties) {
//         switch (prop.kind) {
//           default:
//             throw new Error(`Cannot add property of kind ${prop.kind} to JS object`);
//         }
//       }

//       return Object.fromEntries(entries);
//     }
//     case "ArrayExpression":
//       return e.values.map(toJSUnsafe);

//     case "IdentifierExpression":
//     case "ApplicationExpression":
//     case "LetExpression":
//     case "PlaceholderExpression":
//     case "EvalExpression":
//     case "TableExpression":
//     case "DeclareExpression":
//       throw new Error(`Cannot convert ${e.kind} to JS`);
//   }
// }

// export function toJS<T>(schema: z.Schema<T>, e: Expression): T {
//   return schema.parse(toJSUnsafe(e));
// }

// const emptyInfo: Expression = relt.source.object([
//   relt.source.assign(
//     relt.source.identifier('name'),
//     relt.source.null,
//   ),
//   relt.source.assign(
//     relt.source.identifier('data'),
//     relt.source.object([]),
//   ),
// ]);

// export function deriveTableInfo(e: Expression): [Expression, TableInfo[]] {
//   let infos: TableInfo[] = [];

//   const e1 = visitMap(e, {
//     LetExpression: e => {
//       return visitMap(e, {
//         TableExpression: t => {
//           const [x, info] = normalize(rewrite(e, t, relt.source.application(relt.source.identifier('deriveKeys'), relt.source.array([t, emptyInfo]))), {});
//           infos.push(toJS(tableInfoSchema, info[e.name]));
//           return t;
//         }
//       })
//     }
//   });

//   return [e1, infos];
// }
