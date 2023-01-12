import { ObjectType, Type } from ".";

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
    case "UnitType": return r.kind === "UnitType";
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
    case "UnitType": return `unit`;
    case "TableType": return `table ${t.name}`;
    case "ObjectType": return `{ ${t.properties.map(p => `${p.name}: ${typeName(p.type)}`).join(', ')} }`;
    case "ArrayType": return `${typeName(t.of)}[]`;
    case "OptionalType": return `${typeName(t.of)}?`;
    case "FunctionType": return `${typeName(t.from)} => ${typeName(t.to)}`;
    case "TupleType": return `[${t.types.map(typeName).join(', ')}]`;
    case "IdentifierType": return `${t.name}`;
  }
}

export function unifyTypes(l: Type, r: Type): Type | undefined {
  switch (l.kind) {
    case "IntegerType":
      switch (r.kind) {
        case "AnyType":
        case "IntegerType": return { kind: "IntegerType" };
        case "OptionalType": { const of = unifyTypes(l, r.of); return of === undefined ? undefined : { kind: "OptionalType", of }; }
        default: return undefined;
      }
    case "StringType":
      switch (r.kind) {
        case "AnyType":
        case "StringType": return { kind: "StringType" };
        case "OptionalType": { const of = unifyTypes(l, r.of); return of === undefined ? undefined : { kind: "OptionalType", of }; }
        default: return undefined;
      }
    case "FloatType":
      switch (r.kind) {
        case "AnyType":
        case "FloatType": return { kind: "FloatType" };
        case "OptionalType": { const of = unifyTypes(l, r.of); return of === undefined ? undefined : { kind: "OptionalType", of }; }
        default: return undefined;
      }
    case "BooleanType":
      switch (r.kind) {
        case "AnyType":
        case "StringType": return { kind: "StringType" };
        case "OptionalType": { const of = unifyTypes(l, r.of); return of === undefined ? undefined : { kind: "OptionalType", of }; }
        default: return undefined;
      }
    case "NullType":
      switch (r.kind) {
        case "AnyType":
        case "NullType": return { kind: "NullType" };
        case "OptionalType": { const of = unifyTypes(l, r.of); return of === undefined ? undefined : { kind: "OptionalType", of }; }
        default: return undefined;
      }
    case "AnyType":
      return r;
    case "NeverType":
      return undefined;
    case "UnitType":
      switch (r.kind) {
        case "AnyType":
        case "UnitType": return { kind: "UnitType" };
        case "OptionalType": { const of = unifyTypes(l, r.of); return of === undefined ? undefined : { kind: "OptionalType", of }; }
        default: return undefined;
      }
    case "TableType":
      switch (r.kind) {
        case "AnyType": return l;
        case "TableType": return l.name === r.name ? l : undefined;
        case "OptionalType": { const of = unifyTypes(l, r.of); return of === undefined ? undefined : { kind: "OptionalType", of }; }
        default: return undefined;
      }
    case "ObjectType":
      switch (r.kind) {
        case "AnyType": return l;
        case "ObjectType": {
          const rProps = Object.fromEntries(r.properties.map(x => [x.name, x.type]));
          const props: ObjectType['properties'] = [];
          for (const p of l.properties) {
            if (!(p.name in rProps))
              return undefined;
            const t = unifyTypes(p.type, rProps[p.name]);
            if (t === undefined)
              return undefined;
            props.push({ name: p.name, type: t });
          }
          return { kind: "ObjectType", properties: props };
        }
        case "OptionalType": { const of = unifyTypes(l, r.of); return of === undefined ? undefined : { kind: "OptionalType", of }; }
        default: return undefined;
      }
    case "ArrayType":
      switch (r.kind) {
        case "AnyType": return l;
        case "ArrayType": { const of = unifyTypes(l.of, r.of); return of === undefined ? undefined : { kind: "ArrayType", of }; };
        case "OptionalType": { const of = unifyTypes(l, r.of); return of === undefined ? undefined : { kind: "OptionalType", of }; }
        default: return undefined;
      }
    case "OptionalType":
      switch (r.kind) {
        case "AnyType": return l;
        case "NeverType": return undefined;
        case "NullType":
        case "IntegerType":
        case "StringType":
        case "FloatType":
        case "BooleanType":
        case "UnitType":
        case "TableType":
        case "ObjectType":
        case "ArrayType":
        case "OptionalType":
        case "FunctionType":
        case "TupleType":
        case "IdentifierType":
          { const of = unifyTypes(l.of, r); return of === undefined ? undefined : { kind: "OptionalType", of }; }
      }
    case "FunctionType":
      switch (r.kind) {
        case "AnyType": return l;
        case "FunctionType": {
          const from = unifyTypes(l.from, r.from);
          const to = unifyTypes(l.to, r.to);
          return from === undefined || to === undefined ? undefined : { kind: "FunctionType", from, to };
        };
        case "OptionalType": { const of = unifyTypes(l, r.of); return of === undefined ? undefined : { kind: "OptionalType", of }; }
        default: return undefined;
      }
    case "TupleType":
      switch (r.kind) {
        case "AnyType": return l;
        case "TupleType": {
          if (r.types.length < l.types.length) return undefined;
          const types = l.types.map((x, i) => unifyTypes(l.types[i], r.types[i]));
          if (types.some(x => x === undefined))
            return undefined;
          return { kind: "TupleType", types: types as Type[] };
        }
        case "OptionalType": { const of = unifyTypes(l, r.of); return of === undefined ? undefined : { kind: "OptionalType", of }; }
        default: return undefined;
      }
    case "IdentifierType":
      switch (r.kind) {
        case "AnyType": return l;
        case "IdentifierType": return l.name === r.name ? l : undefined;
        case "OptionalType": { const of = unifyTypes(l, r.of); return of === undefined ? undefined : { kind: "OptionalType", of }; }
        default: return undefined;
      }
  }
}
