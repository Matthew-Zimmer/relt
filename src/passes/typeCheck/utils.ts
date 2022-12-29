import { Type } from "../../asts/type";

export type Context = Record<string, Type>;

export function typeEquals(l: Type, r: Type): boolean {
  switch (l.kind) {
    case "ObjectType":
      switch (r.kind) {
        case "ObjectType": {
          const lObj = Object.fromEntries(l.properties.map(x => [x.name, x.type]));
          const lKeys = Object.keys(lObj);
          const rObj = Object.fromEntries(r.properties.map(x => [x.name, x.type]));
          const rKeys = Object.keys(rObj);
          return lKeys.length === rKeys.length && lKeys.every(k => k in rObj) && lKeys.every(k => typeEquals(lObj[k], rObj[k]));
        }
        case "IntegerType":
        case "FloatType":
        case "BooleanType":
        case "StringType":
        case "FunctionType":
        case "IdentifierType":
        case "TypeType":
        case "UnitType":
        case "UnionType":
          return false;
      }
    case "IntegerType":
      switch (r.kind) {
        case "IntegerType":
          return true;
        case "ObjectType":
        case "FloatType":
        case "BooleanType":
        case "StringType":
        case "FunctionType":
        case "IdentifierType":
        case "TypeType":
        case "UnitType":
        case "UnionType":
          return false;
      }
    case "FloatType":
      switch (r.kind) {
        case "FloatType":
          return true;
        case "ObjectType":
        case "IntegerType":
        case "BooleanType":
        case "StringType":
        case "FunctionType":
        case "IdentifierType":
        case "TypeType":
        case "UnitType":
        case "UnionType":
          return false;
      }
    case "BooleanType":
      switch (r.kind) {
        case "BooleanType":
          return true;
        case "ObjectType":
        case "IntegerType":
        case "FloatType":
        case "StringType":
        case "FunctionType":
        case "IdentifierType":
        case "TypeType":
        case "UnitType":
        case "UnionType":
          return false;
      }
    case "StringType":
      switch (r.kind) {
        case "StringType":
          return true;
        case "ObjectType":
        case "IntegerType":
        case "FloatType":
        case "BooleanType":
        case "FunctionType":
        case "IdentifierType":
        case "TypeType":
        case "UnitType":
        case "UnionType":
          return false;
      }
    case "FunctionType":
      switch (r.kind) {
        case "FunctionType":
          return typeEquals(l.to, r.to) && l.from.length === r.from.length && l.from.every((_, i) => typeEquals(l.from[i], r.from[i]));
        case "ObjectType":
        case "IntegerType":
        case "FloatType":
        case "BooleanType":
        case "StringType":
        case "IdentifierType":
        case "TypeType":
        case "UnitType":
        case "UnionType":
          return false;
      }
    case "IdentifierType":
      switch (r.kind) {
        case "IdentifierType":
          return l.name === r.name;
        case "ObjectType":
        case "IntegerType":
        case "FloatType":
        case "BooleanType":
        case "StringType":
        case "FunctionType":
        case "TypeType":
        case "UnitType":
        case "UnionType":
          return false;
      }
    case "TypeType":
      return false;
    case "UnitType":
      switch (r.kind) {
        case "UnitType":
          return true;
        case "ObjectType":
        case "IntegerType":
        case "FloatType":
        case "BooleanType":
        case "StringType":
        case "FunctionType":
        case "IdentifierType":
        case "TypeType":
        case "UnionType":
          return false;
      }
    case "UnionType":
      switch (r.kind) {
        case "UnionType":
          // TODO this is technically too strict
          // as it has a false negative for this pair
          // (int | string, string | int)
          return l.types.length === r.types.length && l.types.every((_, i) => typeEquals(l.types[i], r.types[i]));
        case "UnitType":
        case "ObjectType":
        case "IntegerType":
        case "FloatType":
        case "BooleanType":
        case "StringType":
        case "FunctionType":
        case "IdentifierType":
        case "TypeType":
          return false;
      }
  }
}
