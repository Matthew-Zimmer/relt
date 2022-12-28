// dummy export so the module system does not complain
export const dummy = 0;

// unused code but might be used in the future

// function typeCheck(e: Expression, ctx: Context): [TypedExpression, Context] {
//   switch (e.kind) {
//     case "IntegerExpression": {
//       return [{ kind: "TypedIntegerExpression", value: e.value, type: { kind: "IntegerType" } }, ctx];
//     }
//     case "FloatExpression": {
//       return [{ kind: "TypedFloatExpression", value: e.value, type: { kind: "FloatType" } }, ctx];
//     }
//     case "BooleanExpression": {
//       return [{ kind: "TypedBooleanExpression", value: e.value, type: { kind: "BooleanType" } }, ctx];
//     }
//     case "StringExpression": {
//       return [{ kind: "TypedStringExpression", value: e.value, type: { kind: "StringType" } }, ctx];
//     }
//     case "IdentifierExpression": {
//       if (!(e.name in ctx))
//         throws(`${e.name} is not defined`);
//       return [{ kind: "TypedIdentifierExpression", name: e.name, type: ctx[e.name] }, ctx];
//     }
//     case "LetExpression": {
//       if (e.name in ctx)
//         throws(`${e.name} is already defined`);
//       const [value] = typeCheck(e.value, ctx);
//       return [{ kind: "TypedLetExpression", name: e.name, value, type: value.type }, { ...ctx, [e.name]: value.type }]
//     }
//     case "ObjectExpression": {
//       const properties = e.properties.map(x => ({ name: x.name, value: typeCheck(x.value, ctx)[0] }));
//       return [{ kind: "TypedObjectExpression", properties, type: { kind: "ObjectType", properties: properties.map(x => ({ name: x.name, type: x.value.type })) } }, ctx];
//     }
//     case "FunctionExpression": {
//       const namedParameters = e.parameters.filter(x => x.kind === 'UnboundedParameter').map<NamedType>(x => {
//         switch (x.kind) {
//           case 'BoundedTypeParameter':
//             throw '';
//           case 'UnboundedParameter':
//             return { name: x.name, type: normalize(x.type, ctx)[0] };
//         }
//       });

//       const allParameters = e.parameters.map<Type>(x => {
//         switch (x.kind) {
//           case 'BoundedTypeParameter':
//             return { kind: "TypeType" };
//           case 'UnboundedParameter':
//             return normalize(x.type, ctx)[0];
//         }
//       });

//       const [value] = typeCheck(e.value, { ...ctx, ...Object.fromEntries(namedParameters.map(x => [x.name, x.type])) });
//       const type: FunctionType = {
//         kind: "FunctionType",
//         from: allParameters,
//         to: value.type,
//       };

//       return [{ kind: 'TypedFunctionExpression', name: e.name, parameters: e.parameters, value: value as TypedBlockExpression, type }, { ...ctx, [e.name]: type }];
//     }
//     case "BlockExpression": {
//       const [values] = e.values.reduce<[TypedExpression[], Context]>(([a, c], x) => {
//         const [u, s] = typeCheck(x, c);
//         return [[...a, u], s];
//       }, [[], ctx]);
//       const type: Type = values.length === 0 ? { kind: "UnitType" } : values[values.length - 1].type;
//       return [{ kind: "TypedBlockExpression", values, type }, ctx];
//     }
//   }
// }
