import { TypedExpression } from "../asts/expression/typed";
import { Expression } from "../asts/expression/untyped";
import { block, line, Line, nl, prefix } from "../asts/line";
import { TopLevelExpression } from "../asts/topLevel";
import { Type } from "../asts/type";
import { TypedTypeExpression } from "../asts/typeExpression/typed";
import { TypeExpression } from "../asts/typeExpression/untyped";

export function generateType(t: Type): string {
  switch (t.kind) {
    case "StructType":
      return `(${t.name}) { ${t.properties.map(p => `${p.name}: ${generateType(p.type)}`).join(', ')} }`;
    case "IntegerType":
      return `int`;
    case "FloatType":
      return `float`;
    case "BooleanType":
      return `bool`;
    case "StringType":
      return `string`;
    case "FunctionType":
      return `(${t.from.map(generateType).join(', ')}) => ${generateType(t.to)}`;
    case "UnitType":
      return `unit`;
    case "UnionType":
      return `(${t.types.map(generateType).join(' | ')})`;
    case "PrimaryKeyType":
      return `pk ${generateType(t.of)}`;
    case "ForeignKeyType":
      return `fk ${t.table}.${t.column}`;
    case "ArrayType":
      return `${generateType(t.of)}[]`;
    case "OptionalType":
      return `${generateType(t.of)}?`;
  }
}

export function generateExpressionUntyped(e: Expression): Line[] {
  switch (e.kind) {
    case "LetExpression":
      return [
        line(`let ${e.name} =`),
        block(
          ...generateExpressionUntyped(e.value),
        ),
      ];
    case "IntegerExpression":
      return [line(`${e.value}`)];
    case "FloatExpression":
      return [line(`${e.value}`)];
    case "BooleanExpression":
      return [line(`${e.value}`)];
    case "StringExpression":
      return [line(`"${e.value}"`)];
    case "IdentifierExpression":
      return [line(`${e.name}`)];
    case "ObjectExpression":
      return [
        line(`{`),
        block(
          ...e.properties.map(p => prefix(`${p.name}: `, generateExpressionUntyped(p.value))),
        ),
        line(`}`),
      ];
    case "FunctionExpression":
      return [
        line(`func ${e.name}(${e.parameters.map(x => `${x.name}: ${generateType(x.type)}`).join(', ')}) {`),
        block(
          ...generateExpressionUntyped(e.value),
        ),
        line('}')
      ];
    case "BlockExpression":
      return [
        line('{'),
        block(
          ...e.values.flatMap(generateExpressionUntyped),
        ),
        line('}')
      ];
    case "ApplicationExpression":
      return [
        ...generateExpressionUntyped(e.func),
        line('('),
        block(
          ...e.args.flatMap(generateExpressionUntyped),
        ),
        line(')'),
      ];
    case "AddExpression":
    case "DefaultExpression":
    case "CmpExpression":
      return [
        ...generateExpressionUntyped(e.left),
        block(
          line(e.op)
        ),
        ...generateExpressionUntyped(e.right),
      ];
    case "ArrayExpression":
      return [
        line(`[`),
        block(
          ...e.values.flatMap(generateExpressionUntyped),
        ),
        line(`]`)
      ];
    case "DotExpression":
      return [
        ...generateExpressionUntyped(e.left),
        block(
          line('.'),
        ),
        ...generateExpressionUntyped(e.right),
      ];
  }
}

export function generateTypeExpressionUntyped(e: TypeExpression): Line[] {
  switch (e.kind) {
    case "ObjectTypeExpression":
      return [
        line(`{`),
        block(
          ...e.properties.map(p => prefix(`${p.name}: `, generateTypeExpressionUntyped(p.value))),
        ),
        line(`}`),
      ];
    case "IntegerTypeExpression":
      return [line(`int`)];
    case "FloatTypeExpression":
      return [line(`float`)];
    case "BooleanTypeExpression":
      return [line(`bool`)];
    case "StringTypeExpression":
      return [line(`string`)];
    case "IdentifierTypeExpression":
      return [line(`${e.name}`)];
    case "JoinTypeExpression":
      return [
        ...generateTypeExpressionUntyped(e.left),
        line(`${e.method} join`),
        ...generateTypeExpressionUntyped(e.right),
        ...(e.leftColumn && e.rightColumn ? [
          line(`on ${e.leftColumn} == ${e.rightColumn}`)
        ] : [])
      ];
    case "DropTypeExpression":
      return [
        line('drop'),
        ...generateTypeExpressionUntyped(e.left),
        block(
          ...e.properties.map(line),
        ),
      ];
    case "WithTypeExpression":
      return [
        ...generateTypeExpressionUntyped(e.left),
        line('with {'),
        block(
          ...e.rules.flatMap(r => {
            switch (r.kind) {
              case "RuleTypeProperty":
                return [
                  line(`${r.name}:`),
                  block(
                    ...generateTypeExpressionUntyped(r.value),
                  )
                ];
              case "RuleValueProperty":
                return [
                  line(`${r.name} =`),
                  block(
                    ...generateExpressionUntyped(r.value),
                  )
                ];
            }
          })
        ),
        line('}')
      ];
    case "UnionTypeExpression":
      return [
        ...generateTypeExpressionUntyped(e.left),
        line(`union`),
        ...generateTypeExpressionUntyped(e.right),
      ];
    case "TypeIntroExpression":
      return [
        line(`type ${e.name} = `),
        block(
          ...generateTypeExpressionUntyped(e.value)
        )
      ];
    case "ForeignKeyTypeExpression":
      return [line(`fk ${e.table}.${e.column}`)];
    case "PrimaryKeyTypeExpression":
      return [prefix('pk ', generateTypeExpressionUntyped(e.of))];
    case "ArrayTypeExpression":
      return [
        ...generateTypeExpressionUntyped(e.of),
        line('[]')
      ];
    case "GroupByTypeExpression":
      return [
        ...generateTypeExpressionUntyped(e.left),
        line(`group by ${e.column} agg {`),
        block(
          ...e.aggregations.flatMap(p => {
            switch (p.kind) {
              case "AggProperty":
                return [
                  line(`${p.name} =`),
                  block(
                    ...generateExpressionUntyped(p.value),
                  )
                ];
            }
          })
        ),
        line('}')
      ];
    case "SortTypeExpression":
      return [
        line(`sort`),
        ...generateTypeExpressionUntyped(e.left),
        line(`by ${e.columns.map(x => `${x.name} ${x.order} ${x.nulls ?? 'last'}`)}`),
      ];
    case "WhereTypeExpression":
      return [
        ...generateTypeExpressionUntyped(e.left),
        line(`where (`),
        block(
          ...generateExpressionUntyped(e.condition),
        ),
        line(`)`)
      ];
    case "DistinctTypeExpression":
      return [
        line(`distinct`),
        ...generateTypeExpressionUntyped(e.left),
        line(`on (`),
        block(
          ...e.columns.map(x => line(`${x},`))
        ),
        line(')')
      ];
  }
}

export function generateSourceCodeUntyped(e: TopLevelExpression): Line[] {
  switch (e.kind) {
    case "LetExpression":
    case "IntegerExpression":
    case "FloatExpression":
    case "BooleanExpression":
    case "StringExpression":
    case "IdentifierExpression":
    case "ObjectExpression":
    case "FunctionExpression":
    case "BlockExpression":
    case "ApplicationExpression":
    case "AddExpression":
    case "CmpExpression":
    case "DefaultExpression":
    case "ArrayExpression":
    case "DotExpression":
      return generateExpressionUntyped(e);
    case "TypeIntroExpression":
      return generateTypeExpressionUntyped(e);
    case "LibraryDeclaration":
      return [
        line(`declare library ${e.name} package ${e.package} version ${e.version} = {`),
        block(
          ...e.members.map(x => line(`${x.name}: ${generateType(x.type)}`)),
        ),
        line('}')
      ];
  }
}

export function generateExpressionTyped(e: TypedExpression): Line[] {
  switch (e.kind) {
    case "TypedLetExpression":
      return [
        line(`let ${e.name} : ${generateType(e.type)} =`),
        block(
          ...generateExpressionTyped(e.value),
        ),
      ];
    case "TypedIntegerExpression":
      return [line(`${e.value} : ${generateType(e.type)}`)];
    case "TypedFloatExpression":
      return [line(`${e.value} : ${generateType(e.type)}`)];
    case "TypedBooleanExpression":
      return [line(`${e.value} : ${generateType(e.type)}`)];
    case "TypedStringExpression":
      return [line(`"${e.value}" : ${generateType(e.type)}`)];
    case "TypedIdentifierExpression":
      return [line(`${e.name} : ${generateType(e.type)}`)];
    case "TypedObjectExpression":
      return [
        line(`{`),
        block(
          ...e.properties.map(p => prefix(`${p.name}: `, generateExpressionTyped(p.value))),
        ),
        line(`} : ${generateType(e.type)}`),
      ];
    case "TypedFunctionExpression":
      return [
        line(`func ${e.name}: ${generateType(e.type)}(${e.parameters.map(x => `${x.name}: ${generateType(x.type)}`).join(', ')}) {`),
        block(
          ...generateExpressionTyped(e.value),
        ),
        line('}')
      ];
    case "TypedBlockExpression":
      return [
        line('{'),
        block(
          ...e.values.flatMap(generateExpressionTyped),
        ),
        line(`} : ${generateType(e.type)}`)
      ];
    case "TypedApplicationExpression":
      return [
        ...generateExpressionTyped(e.func),
        line('('),
        block(
          ...e.args.flatMap(generateExpressionTyped),
        ),
        line(`) : ${generateType(e.type)}`),
      ];
    case "TypedAddExpression":
    case "TypedCmpExpression":
    case "TypedDefaultExpression":
      return [
        ...generateExpressionTyped(e.left),
        block(
          line(`${e.op} : ${generateType(e.type)}`)
        ),
        ...generateExpressionTyped(e.right),
      ];
    case "TypedArrayExpression":
      return [
        line(`[`),
        block(
          ...e.values.flatMap(generateExpressionTyped),
        ),
        line(`] : ${generateType(e.type)}`)
      ];
    case "TypedDotExpression":
      return [
        ...generateExpressionTyped(e.left),
        block(
          line(`. : ${generateType(e.type)}`),
        ),
        ...generateExpressionTyped(e.right),
      ];
  }
}

export function generateTypeExpressionTyped(e: TypedTypeExpression): Line[] {
  switch (e.kind) {
    case "TypedObjectTypeExpression":
      return [
        line(`{`),
        block(
          ...e.properties.map(p => prefix(`${p.name}: `, generateTypeExpressionTyped(p.value))),
        ),
        line(`}`),
        block(
          line(`:type ${generateType(e.type)}`),
        ),
      ];
    case "TypedIntegerTypeExpression":
      return [line(`int`)];
    case "TypedFloatTypeExpression":
      return [line(`float`)];
    case "TypedBooleanTypeExpression":
      return [line(`bool`)];
    case "TypedStringTypeExpression":
      return [line(`string`)];
    case "TypedIdentifierTypeExpression":
      return [line(`${e.name} :type ${generateType(e.type)}`)];
    case "TypedJoinTypeExpression":
      return [
        ...generateTypeExpressionTyped(e.left),
        line(`${e.method} join`),
        block(
          line(`:type ${generateType(e.type)}`),
        ),
        ...generateTypeExpressionTyped(e.right),
        ...(e.leftColumn && e.rightColumn ? [
          line(`on ${e.leftColumn} == ${e.rightColumn}`)
        ] : [])
      ];
    case "TypedDropTypeExpression":
      return [
        line(`drop`),
        block(
          line(`:type ${generateType(e.type)}`),
        ),
        ...generateTypeExpressionTyped(e.left),
        block(
          ...e.properties.map(line),
        ),
      ];
    case "TypedWithTypeExpression":
      return [
        ...generateTypeExpressionTyped(e.left),
        line(`with {`),
        block(
          line(`:type ${generateType(e.type)}`),
        ),
        block(
          ...e.rules.flatMap(r => {
            switch (r.kind) {
              case "TypedRuleTypeProperty":
                return [
                  line(`${r.name} :`),
                  block(
                    ...generateTypeExpressionTyped(r.value),
                  )
                ];
              case "TypedRuleValueProperty":
                return [
                  line(`${r.name} =`),
                  block(
                    ...generateExpressionTyped(r.value),
                  )
                ];
            }
          })
        ),
        line('}')
      ];
    case "TypedUnionTypeExpression":
      return [
        ...generateTypeExpressionTyped(e.left),
        line(`union`),
        ...generateTypeExpressionTyped(e.right),
      ];
    case "TypedTypeIntroExpression":
      return [
        line(`type ${e.name} = `),
        block(
          line(`:type ${generateType(e.type)}`),
        ),
        block(
          ...generateTypeExpressionTyped(e.value)
        )
      ];
    case "TypedForeignKeyTypeExpression":
      return [line(`fk ${e.table}.${e.column} :type ${generateType(e.type)}`)];
    case "TypedPrimaryKeyTypeExpression":
      return [prefix(`pk :type ${generateType(e.type)} `, generateTypeExpressionTyped(e.of))];
    case "TypedArrayTypeExpression":
      return [
        ...generateTypeExpressionTyped(e.of),
        line(`[]`),
        block(
          line(`:type ${generateType(e.type)}`),
        ),
      ];
    case "TypedGroupByTypeExpression":
      return [
        line(`group`),
        ...generateTypeExpressionTyped(e.left),
        line(`by ${e.column} agg {`),
        block(
          line(`:type ${generateType(e.type)}`),
        ),
        block(
          ...e.aggregations.flatMap(p => {
            return [
              line(`${p.name} =`),
              block(
                ...generateExpressionTyped(p.value),
              )
            ];
          })
        ),
        line('}')
      ];
    case "TypedSortTypeExpression":
      return [
        line(`sort`),
        ...generateTypeExpressionTyped(e.left),
        line(`by ${e.columns.map(x => `${x.name} ${x.order} ${x.nulls ?? 'last'}`)}`),
      ];
    case "TypedWhereTypeExpression":
      return [
        ...generateTypeExpressionTyped(e.left),
        line(`where (`),
        block(
          ...generateExpressionTyped(e.condition),
        ),
        line(`)`)
      ];
    case "TypedDistinctTypeExpression":
      return [
        line(`distinct`),
        ...generateTypeExpressionTyped(e.left),
        line(`on (`),
        block(
          ...e.columns.map(x => line(`${x},`))
        ),
        line(')')
      ];
  }
}

export function generateSourceCodeTyped(e: TypedExpression | TypedTypeExpression): Line[] {
  switch (e.kind) {
    case "TypedLetExpression":
    case "TypedIntegerExpression":
    case "TypedFloatExpression":
    case "TypedBooleanExpression":
    case "TypedStringExpression":
    case "TypedIdentifierExpression":
    case "TypedObjectExpression":
    case "TypedFunctionExpression":
    case "TypedBlockExpression":
    case "TypedApplicationExpression":
    case "TypedAddExpression":
    case "TypedDefaultExpression":
    case "TypedCmpExpression":
    case "TypedArrayExpression":
    case "TypedDotExpression":
      return generateExpressionTyped(e);
    case "TypedObjectTypeExpression":
    case "TypedIntegerTypeExpression":
    case "TypedFloatTypeExpression":
    case "TypedBooleanTypeExpression":
    case "TypedStringTypeExpression":
    case "TypedIdentifierTypeExpression":
    case "TypedJoinTypeExpression":
    case "TypedDropTypeExpression":
    case "TypedWithTypeExpression":
    case "TypedUnionTypeExpression":
    case "TypedTypeIntroExpression":
    case "TypedForeignKeyTypeExpression":
    case "TypedPrimaryKeyTypeExpression":
    case "TypedArrayTypeExpression":
    case "TypedGroupByTypeExpression":
    case "TypedSortTypeExpression":
    case "TypedWhereTypeExpression":
    case "TypedDistinctTypeExpression":
      return generateTypeExpressionTyped(e);
  }
}

export function generateAllSourceCodeUntyped(l: TopLevelExpression[]): Line[] {
  return l.flatMap(x => [...generateSourceCodeUntyped(x), nl]);
}

export function generateAllSourceCodeTyped(l: (TypedExpression | TypedTypeExpression)[]): Line[] {
  return l.flatMap(x => [...generateSourceCodeTyped(x), nl]);
}
