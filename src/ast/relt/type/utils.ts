import { Type } from ".";

export function typeEquals(l: Type, r: Type): boolean {
  if (l.kind === "AnyType" || r.kind === "AnyType") return true;
  if (l.kind === "NeverType" || r.kind === "NeverType") return false;

  switch (l.kind) {
    case "IntegerType": return r.kind === "IntegerType";
    case "StringType": return r.kind === "StringType";
    case "FloatType": return r.kind === "FloatType";
    case "BooleanType": return r.kind === "BooleanType";
    case "TableType": return r.kind === "TableType" && l.name === r.name;
    case "ObjectType": return r.kind === "ObjectType" && l.properties.length === r.properties.length && l.properties.every(x => r.properties.some(y => x.name === y.name && typeEquals(x.type, y.type)));
    case "ArrayType": return r.kind === "ArrayType" && typeEquals(l.of, r.of);
    case "NullType": return r.kind === "NullType";
    case "OptionalType": return r.kind === "OptionalType" && typeEquals(l.of, r.of);
    case "FunctionType": return r.kind === "FunctionType" && typeEquals(l.from, r.from) && typeEquals(l.to, r.to);
    case "TupleType": return r.kind === "TupleType" && l.types.length === r.types.length && l.types.every((_, i) => typeEquals(l.types[i], r.types[i]));
    case "IdentifierType": return r.kind === "IdentifierType" && l.name === r.name;
  }
}

export function typeName(t: Type): string {
  switch (t.kind) {
    case "IntegerType": return `int`;
    case "StringType": return `string`;
    case "FloatType": return `float`;
    case "BooleanType": return `bool`;
    case "NullType": return `null`;
    case "AnyType": return `any`;
    case "NeverType": return `never`;
    case "TableType": return `table ${t.name}`;
    case "ObjectType": return `{ ${t.properties.map(p => `${p.name}: ${typeName(p.type)}`).join(', ')} }`;
    case "ArrayType": return `${typeName(t.of)}[]`;
    case "OptionalType": return `${typeName(t.of)}?`;
    case "FunctionType": return `${typeName(t.from)} => ${typeName(t.to)}`;
    case "TupleType": return `[${t.types.map(typeName).join(', ')}]`;
    case "IdentifierType": return `${t.name}`;
  }
}
