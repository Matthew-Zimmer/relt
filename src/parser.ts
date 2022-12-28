import { generate } from 'peggy';

export const parser = generate(`
  module 
    = _ expressions: (@top_level_expression _)*
    { return { kind: "Module", expressions } }

  // optional whitespace
  _  = [ \\t\\r\\n]*

  // mandatory whitespace
  __ = [ \\t\\r\\n]+

  identifier 
    = chars: ([a-zA-Z][a-zA-Z0-9_]*)
    ! { return ["type", "let", "func"].includes(chars[0] + chars[1].join('')) }
    { return chars[0] + chars[1].join('') }

  top_level_expression
    = type_intro_expression
    / expression

  expression
    = let_expression
    / literal_expression
    / func_expression

  type_intro_expression
    = "type" __ name: identifier _ "=" _ value: type_expression
    { return { kind: "TypeIntroExpression", name, value } }

  type_expression
    = with_type_expression

  with_type_expression
    = head:drop_type_expression tail:(_ "with" _ right:drop_type_expression _ { return {
      kind: 'WithTypeExpression',
      right,
  }})*
  { return tail.reduce((t, h) => ({ ...h, left: t }), head) }

  drop_type_expression
    = left: join_type_expression _ "drop" _ properties: (head: identifier tail: (_ "," _ @identifier)* (_ ",")? { return [head, ...tail] })
    { return { kind: "DropTypeExpression", left, properties } }
    / join_type_expression

  join_type_expression
    = head:type_expression_2 tail:(_ type: (@("inner" / "outer" / "left" / "right") __)? "join" _ right:type_expression_2 _ "on" _ columns: (leftColumn: identifier _ "==" _ rightColumn: identifier { return [leftColumn, rightColumn] } / column: identifier { return [column, column] } ) { return {
        kind: 'JoinTypeExpression',
        right,
        type: type ?? "inner",
        leftColumn: columns[0],
        rightColumn: columns[1],
    }})*
    { return tail.reduce((t, h) => ({ ...h, left: t }), head) }

  type_expression_2
    = literal_type_expression
    / type_intro_expression

  literal_type_expression
    = integer_type_expression
    / float_type_expression
    / boolean_type_expression
    / string_type_expression
    / identifier_type_expression
    / object_type_expression
    / group_type_expression

  integer_type_expression
    = "int"
    { return { kind: "IntegerTypeExpression" } }

  float_type_expression
    = "float"
    { return { kind: "FloatTypeExpression" } }

  boolean_type_expression
    = "bool"
    { return { kind: "BooleanTypeExpression" } }

  string_type_expression
    = "string"
    { return { kind: "StringTypeExpression" } }

  identifier_type_expression
    = name: identifier
    { return { kind: "IdentifierTypeExpression", name } }

  type 
    = expr: (integer_type_expression / float_type_expression / boolean_type_expression / string_type_expression / identifier_type_expression)
    { return { ...expr, kind: expr.kind.slice(0, -10) } }

  object_type_expression
    = "{" _ head: object_type_property tail: (_ "," _ @object_type_property)* _ ("," _)? "}"
    { return { kind: "ObjectTypeExpression", properties: [head, ...tail] } }

  object_type_property
    = name: identifier _ ":" _ value: type_expression
    { return { name, value } }

  group_type_expression
    = "(" _ value: type_expression _ ")"
    { return value }

  let_expression
    = "let" __ name: identifier _ "=" _ value: expression
    { return { kind: "LetExpression", name, value } }

  func_expression
    = "func" __ name: identifier _ parameters: parameters _ value: block_expression
    { return { kind: "FunctionExpression", name, parameters, value } }

  parameters
    = "(" _ values: (head: parameter tail: (_ "," _ @parameter)* (_ ",")? _ { return [head, ...tail] })? ")"
    { return values ?? [] }

  parameter
    = name: identifier _ ":" _ type: type
    { return { kind: "Parameter", name, type } }

  block_expression
    = "{" values: (_ @expression)* _ "}"
    { return { kind: "BlockExpression", values } }

  literal_expression
    = integer_expression
    / float_expression
    / boolean_expression
    / string_expression
    / identifier_expression
    / object_expression
    / group_expression

  group_expression
    = "(" _ value: expression _ ")"
    { return value }
  
  boolean_expression
    = value: ("true" / "false")
    { return { kind: "BooleanExpression", value: value === "true" } }

  integer_expression
    = value: ("0" / parts: [1-9][0-9]* { return parts.length === 1 ? parts[0] : parts[0] + parts[1].join("") })
    { return { kind: "IntegerExpression", value: Number(value) } }

  float_expression
    = integer_part: ("0" / parts: [1-9][0-9]* { return parts[0] + parts[1].join("") }) "." decimal_part: [0-9]+
    { return { kind: "IntegerExpression", value: Number(integer_part + "." + decimal_part) } }
  
  string_expression
    = "\\"" chars: [^\\"]* "\\""
    { return { kind: "StringExpression", value: chars.join("") } }
  
  identifier_expression
    = name: identifier
    { return { kind: "IdentifierExpression", name } }

  object_expression
    = "{" _ properties: (@(head: object_property tail: (_ "," _ @object_property)* { return [head, ...tail] }) ","? _)? "}"
    { return { kind: "ObjectExpression", properties: properties ?? [] } }

  object_property
    = name: identifier _ ":" _ value: expression
    { return { name, value } }
`);