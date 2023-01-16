// import { Expression } from "../ast/relt/source";
// import { visitMap } from "../ast/relt/source/vistor";

// export function deriveTables(e: Expression): Expression {
//   return visitMap(e, {
//     LetExpression: e => {
//       return visitMap(e, {
//         ApplicationExpression: x => {
//           if (x.left.kind !== 'IdentifierExpression') return x;
//           if (x.left.name !== 'table') return x;
//           if (x.right.kind !== 'ObjectExpression') return x;
//           if (!x.right.properties.every(x => x.kind === 'DeclareExpression')) {
//             throw new Error(`Error: A table + object combination could not be converted to a table expression since there non declare expressions on thee object: (<expr>: <type>)`);
//           }
//           return {
//             kind: "TableExpression",
//             name: e.name,
//             columns: x.right.properties,
//           };
//         }
//       });
//     }
//   });
// }
