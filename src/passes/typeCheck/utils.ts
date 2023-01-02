import { Type } from "../../asts/type";

export type Context = Record<string, Type>;

export function typeEquals(l: Type, r: Type): boolean {
  switch (l.kind) {
    case "StructType":
      switch (r.kind) {
        case "StructType": {
          if (l.name === '' && r.name === '') {
            const lObj = Object.fromEntries(l.properties.map(x => [x.name, x.type]));
            const lKeys = Object.keys(lObj);
            const rObj = Object.fromEntries(r.properties.map(x => [x.name, x.type]));
            const rKeys = Object.keys(rObj);
            return lKeys.length === rKeys.length && lKeys.every(k => k in rObj) && lKeys.every(k => typeEquals(lObj[k], rObj[k]));
          }
          return l.name === r.name;
        }
        case "IntegerType":
        case "FloatType":
        case "BooleanType":
        case "StringType":
        case "FunctionType":
        case "UnitType":
        case "UnionType":
        case "ForeignKeyType":
        case "PrimaryKeyType":
        case "ArrayType":
        case "OptionalType":
          return false;
      }
    case "IntegerType":
      switch (r.kind) {
        case "IntegerType":
          return true;
        case "StructType":
        case "FloatType":
        case "BooleanType":
        case "StringType":
        case "FunctionType":
        case "UnitType":
        case "UnionType":
        case "ForeignKeyType":
        case "PrimaryKeyType":
        case "ArrayType":
        case "OptionalType":
          return false;
      }
    case "FloatType":
      switch (r.kind) {
        case "FloatType":
          return true;
        case "StructType":
        case "IntegerType":
        case "BooleanType":
        case "StringType":
        case "FunctionType":
        case "UnitType":
        case "UnionType":
        case "ForeignKeyType":
        case "PrimaryKeyType":
        case "ArrayType":
        case "OptionalType":
          return false;
      }
    case "BooleanType":
      switch (r.kind) {
        case "BooleanType":
          return true;
        case "StructType":
        case "IntegerType":
        case "FloatType":
        case "StringType":
        case "FunctionType":
        case "UnitType":
        case "UnionType":
        case "ForeignKeyType":
        case "PrimaryKeyType":
        case "ArrayType":
        case "OptionalType":
          return false;
      }
    case "StringType":
      switch (r.kind) {
        case "StringType":
          return true;
        case "StructType":
        case "IntegerType":
        case "FloatType":
        case "BooleanType":
        case "FunctionType":
        case "UnitType":
        case "UnionType":
        case "ForeignKeyType":
        case "PrimaryKeyType":
        case "ArrayType":
        case "OptionalType":
          return false;
      }
    case "FunctionType":
      switch (r.kind) {
        case "FunctionType":
          return typeEquals(l.to, r.to) && l.from.length === r.from.length && l.from.every((_, i) => typeEquals(l.from[i], r.from[i]));
        case "StructType":
        case "IntegerType":
        case "FloatType":
        case "BooleanType":
        case "StringType":
        case "UnitType":
        case "UnionType":
        case "ForeignKeyType":
        case "PrimaryKeyType":
        case "ArrayType":
        case "OptionalType":
          return false;
      }
    case "UnitType":
      switch (r.kind) {
        case "UnitType":
          return true;
        case "StructType":
        case "IntegerType":
        case "FloatType":
        case "BooleanType":
        case "StringType":
        case "FunctionType":
        case "UnionType":
        case "ForeignKeyType":
        case "PrimaryKeyType":
        case "ArrayType":
        case "OptionalType":
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
        case "StructType":
        case "IntegerType":
        case "FloatType":
        case "BooleanType":
        case "StringType":
        case "FunctionType":
        case "ForeignKeyType":
        case "PrimaryKeyType":
        case "ArrayType":
        case "OptionalType":
          return false;
      }
    case "PrimaryKeyType":
      switch (r.kind) {
        case "PrimaryKeyType":
          return typeEquals(l.of, r.of);
        case "StructType":
        case "IntegerType":
        case "FloatType":
        case "BooleanType":
        case "StringType":
        case "FunctionType":
        case "UnitType":
        case "UnionType":
        case "ForeignKeyType":
        case "ArrayType":
        case "OptionalType":
          return false;
      }
    case "ForeignKeyType":
      switch (r.kind) {
        case "ForeignKeyType":
          return l.table === r.table && l.column === r.column;
        case "StructType":
        case "IntegerType":
        case "FloatType":
        case "BooleanType":
        case "StringType":
        case "FunctionType":
        case "UnitType":
        case "UnionType":
        case "PrimaryKeyType":
        case "ArrayType":
        case "OptionalType":
          return false;
      }
    case "ArrayType":
      switch (r.kind) {
        case "ArrayType":
          return typeEquals(l.of, r.of);
        case "StructType":
        case "IntegerType":
        case "FloatType":
        case "BooleanType":
        case "StringType":
        case "FunctionType":
        case "UnitType":
        case "UnionType":
        case "PrimaryKeyType":
        case "ForeignKeyType":
        case "OptionalType":
          return false;
      }
    case "OptionalType":
      switch (r.kind) {
        case "OptionalType":
          return typeEquals(l.of, r.of);
        case "StructType":
        case "IntegerType":
        case "FloatType":
        case "BooleanType":
        case "StringType":
        case "FunctionType":
        case "UnitType":
        case "UnionType":
        case "PrimaryKeyType":
        case "ForeignKeyType":
        case "ArrayType":
          return false;
      }
  }
}
