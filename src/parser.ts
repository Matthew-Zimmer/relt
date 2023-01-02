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
    ! { return ["type", "let", "func", "fk", "pk"].includes(chars[0] + chars[1].join('')) }
    { return chars[0] + chars[1].join('') }

  top_level_expression
    = type_intro_expression
    / expression

  expression
    = let_expression
    / func_expression
    / default_expression

  type_intro_expression
    = "type" __ name: identifier _ "=" _ value: type_expression
    { return { kind: "TypeIntroExpression", name, value } }

  type_expression
    = with_type_expression

  with_type_expression
    = head:drop_type_expression tail:(_ "with" _ rules: rule_properties _ { return {
      kind: 'WithTypeExpression',
      rules,
  }})*
  { return tail.reduce((t, h) => ({ ...h, left: t }), head) }

  rule_properties
    = "{" _ head: rule_property tail: (_ "," _ @rule_property)* _ ("," _)? "}"
    { return [head, ...tail] }

  rule_property
    = rule_value_property
    / rule_type_property

  rule_value_property
    = name: identifier _ "=" _ value: expression
    { return { kind: "RuleValueProperty", name, value } }

  rule_type_property
    = name: identifier _ ":" _ value: type_expression
    { return { kind: "RuleTypeProperty", name, value } }

  drop_type_expression
    = left: group_by_type_expression _ "drop" _ properties: (head: identifier tail: (_ "," _ @identifier)* (_ ",")? { return [head, ...tail] })
    { return { kind: "DropTypeExpression", left, properties } }
    / group_by_type_expression

  group_by_type_expression
    = left: join_type_expression _ "group" __ "by" __ column: identifier __ "agg" _ aggregations: agg_properties
    { return { kind: "GroupByTypeExpression", left, column, aggregations } }
    / join_type_expression

  agg_properties
    = "{" _ head: agg_property tail: (_ "," _ @agg_property)* _ ("," _)? "}"
    { return [head, ...tail] }

  agg_property
    = name: identifier _ "=" _ value: expression
    { return { kind: "AggProperty", name, value } }

  join_type_expression
    = head:type_expression_2 tail:(_ type: (@("inner" / "outer" / "left" / "right") __)? "join" _ right:type_expression_2 columns: (_ "on" _ @(leftColumn: identifier _ "==" _ rightColumn: identifier { return [leftColumn, rightColumn] } / column: identifier { return [column, column] } ))? { return {
        kind: 'JoinTypeExpression',
        right,
        method: type ?? "inner",
        leftColumn: columns?.[0],
        rightColumn: columns?.[1],
    }})*
    { return tail.reduce((t, h) => ({ ...h, left: t }), head) }

  type_expression_2
    = array_type_expression
    / type_intro_expression

  array_type_expression
    = head:literal_type_expression tail:(_ "[" _ "]" { return {
      kind: 'ArrayTypeExpression',
  }})*
  { return tail.reduce((t, h) => ({ ...h, of: t }), head) }

  literal_type_expression
    = integer_type_expression
    / float_type_expression
    / boolean_type_expression
    / string_type_expression
    / identifier_type_expression
    / object_type_expression
    / group_type_expression
    / foreign_key_type_expression
    / primary_key_type_expression

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

  foreign_key_type_expression
    = "fk" __ table: identifier _ "." _ column: identifier
    { return { kind: "ForeignKeyTypeExpression", table, column } }

  primary_key_type_expression
    = "pk" __ of: (integer_type_expression / string_type_expression)
    { return { kind: "PrimaryKeyTypeExpression", of } }

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

  default_expression
    = head:add_expression tail:(_ op: ("??") _ right:add_expression _ { return {
      kind: 'DefaultExpression',
      op,
      right,
  }})*
  { return tail.reduce((t, h) => ({ ...h, left: t }), head) }

  add_expression
    = head:application_expression tail:(_ op: ("+") _ right:application_expression _ { return {
      kind: 'AddExpression',
      op,
      right,
  }})*
  { return tail.reduce((t, h) => ({ ...h, left: t }), head) }

  application_expression
    = head:literal_expression tail:(_ args: application_args _ { return {
      kind: 'ApplicationExpression',
      args,
  }})*
  { return tail.reduce((t, h) => ({ ...h, func: t }), head) }

  application_args
    = "(" _ args: (h: expression t: (_ "," _ @expression)* _ ("," _)? { return [h, ...t] } )? ")"
    { return args ?? [] }

  literal_expression
    = float_expression
    / integer_expression
    / boolean_expression
    / string_expression
    / identifier_expression
    / object_expression
    / group_expression
    / array_expression

  group_expression
    = "(" _ value: expression _ ")"
    { return value }
  
  boolean_expression
    = value: ("true" / "false")
    { return { kind: "BooleanExpression", value: value === "true" } }

  integer_expression
    = value: ("0" / head: [1-9] tail: [0-9]* { return head + tail.join("") })
    { return { kind: "IntegerExpression", value: Number(value) } }

  float_expression
    = integer_part: ("0" / head: [1-9] tail: [0-9]* { return head + tail.join("") }) "." decimal_part: [0-9]+
    { return { kind: "FloatExpression", value: Number(integer_part + "." + decimal_part) } }
  
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

  array_expression
    = "[" _ values: (h: expression t: (_ "," _ @expression)* _ ("," _)? { return [h, ...t] } )? "]"
    { return { kind: "ArrayExpression", values: values ?? [] } }
`);