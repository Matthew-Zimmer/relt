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
    ! { return [
      "type", "let", "func", "fk", "pk", "declare", "sort", 
      "by", "distinct", "on", "where", "using", "sugar", "then", 
      "union", "bool", "int", "string", "null", "float", "any", 
      "never", "unit", "do", "false", "true", "end",
      ].includes(chars[0] + chars[1].join('')) }
    { return chars[0] + chars[1].join('') }

  free_identifier 
    = chars: ([^,~!:. \\n\\t\\r()]+)
    { return chars.join('') }

  top_level_expression
    = type_intro_expression
    / expression
    / sugar_definition
    / sugar_directive

  sugar_definition
    = "sugar" __ name: identifier __ phase: (@[0-9] __)? "on" __ pattern: expression __ "then" __ replacement: expression
    { return { kind: "SugarDefinition", name, phase: Number(phase ?? "1"), pattern, replacement, loc: location() } }

  sugar_directive
    = "sugar" __ command: ("enable" / "disable")  __ name: identifier
    { return { kind: "SugarDirective", command, name, loc: location() } }

  type_intro_expression
    = "type" __ name: identifier _ "=" _ type: type
    { return { kind: "TypeIntroductionExpression", name, type, loc: location() } }

  type
    = function_type
  
  function_type
    = "(" _ from: (@type _)? ")" _ "=>" _ to: type
    { return { kind: "FunctionType", from: from ?? undefined, to, loc: location() } }
    / postfix_type

  postfix_type
    = head: literal_type tail: (_ op: ("[]" / "?") { return {
      kind: op === "[]" ? "ArrayType" : "OptionalType",
      loc: location()
    }})*
    { return tail.reduce((t, h) => ({ ...h, of: t }), head) }

  literal_type
    = integer_type
    / any_type
    / never_type
    / float_type
    / null_type
    / string_type
    / boolean_type
    / object_type
    / tuple_type
    / group_type
    / unit_type
    / identifier_type

  integer_type
    = "int"
    { return { kind: "IntegerType", loc: location() } }

  unit_type
    = "unit"
    { return { kind: "UnitType", loc: location() } }

  group_type
    = "(" _ value: type _ ")"
    { return value }

  any_type
    = "any"
    { return { kind: "AnyType", loc: location() } }

  never_type
    = "never"
    { return { kind: "NeverType", loc: location() } }

  float_type
    = "float"
    { return { kind: "FloatType", loc: location() } }
    
  null_type
    = "null"
    { return { kind: "NullType", loc: location() } }

  string_type
    = "string"
    { return { kind: "StringType", loc: location() } }

  boolean_type
    = "bool"
    { return { kind: "BooleanType", loc: location() } }

  identifier_type
    = name: identifier
    { return { kind: "IdentifierType", name, loc: location() } }

  tuple_type
    = "[" _ types: (h: type t: (_ "," _ @type)* _ ("," _)? { return [h, ...t] } )? "]"
    { return { kind: "TupleType", types: types ?? [], loc: location() } }

  object_type
    = "{" _ properties: (h: object_type_property t: (_ "," _ @object_type_property)* _ ("," _)? { return [h, ...t] } )? "}"
    { return { kind: "ObjectType", properties: properties ?? [], loc: location() } }

  object_type_property
    = name: identifier _ ":" _ type: type
    { return { name, type, loc: location() } }

  expression
    = let_expression
    / function_expression
    / table_expression
    / eval_expression

  let_expression
    = "let" _ value: expression
    { return { kind: "LetExpression", value, loc: location() } }

  table_expression
    = "table" _ value: expression
    { return { kind: "TableExpression", value, loc: location() } }

  function_expression
    = "func" _ name: (@identifier _)? args: (@function_argument+ _)? "=>" _ value: expression
    { return { kind: "FunctionExpression", name: name ?? undefined, args: args ?? [], value, loc: location() } }

  function_argument
    = "(" _ arg: expression _ ")"
    { return arg }

  eval_expression
    = "$$" _ node: expression
    { return { kind: "EvalExpression", node, loc: location() } }
    / declare_expression

  declare_expression
    = value: assign_expression _ ":" _ type: type
    { return { kind: "DeclareExpression", value, type, loc: location() } }
    / assign_expression

  assign_expression
    = head: conditional_expression tail:(_ op: ("=") _ right: conditional_expression { return {
      kind: 'AssignExpression',
      op,
      right,
      loc: location()
    }})*
    { return tail.reduce((t, h) => ({ ...h, left: t }), head) }

  conditional_expression
    = head: or_expression tail:(_ op: ("??" / "!?") _ right: or_expression { return {
      kind: 'ConditionalExpression',
      op,
      right,
      loc: location()
    }})*
    { return tail.reduce((t, h) => ({ ...h, left: t }), head) }

  or_expression
    = head: and_expression tail:(_ op: ("||") _ right: and_expression { return {
      kind: 'OrExpression',
      op,
      right,
      loc: location()
    }})*
    { return tail.reduce((t, h) => ({ ...h, left: t }), head) }

  and_expression
    = head: cmp_expression tail:(_ op: ("&&") _ right: cmp_expression { return {
      kind: 'AndExpression',
      op,
      right,
      loc: location()
    }})*
    { return tail.reduce((t, h) => ({ ...h, left: t }), head) }

  cmp_expression
    = head: add_expression tail:(_ op: ("==" / "!=" / "<=" / ">=" / "<" / ">") _ right: add_expression { return {
      kind: 'CmpExpression',
      op,
      right,
      loc: location()
    }})*
    { return tail.reduce((t, h) => ({ ...h, left: t }), head) }

  add_expression
    = head: mul_expression tail:(_ op: ("+" / "-") _ right: mul_expression { return {
      kind: 'AddExpression',
      op,
      right,
      loc: location()
    }})*
    { return tail.reduce((t, h) => ({ ...h, left: t }), head) }

  mul_expression
    = head: union_expression tail:(_ op: ("*" / "/" / "%") _ right: union_expression { return {
      kind: 'MulExpression',
      op,
      right,
      loc: location()
    }})*
    { return tail.reduce((t, h) => ({ ...h, left: t }), head) }

  union_expression
    = head: join_expression tail:(_ "union" _ right: join_expression { return {
      kind: 'UnionExpression',
      right,
      loc: location()
    }})*
    { return tail.reduce((t, h) => ({ ...h, left: t }), head) }

  join_expression
    = head: group_by_expression tail:(_ method: (@("inner" / "left" /  "right") __)?  "join" _ right: group_by_expression on: (_ "on" @expression)? { return {
      kind: 'JoinExpression',
      method: method ?? "inner",
      right,
      on: on ?? undefined,
      loc: location()
    }})*
    { return tail.reduce((t, h) => ({ ...h, left: t }), head) }
  
  group_by_expression
    = "group" __ left: expression _ "by" _ by: expression _ "agg" _ agg: expression
    { return { kind: "GroupExpression", left, by, agg, loc: location() } }
    / table_op_expression

  table_op_expression
    = "distinct" __ value: expression on: (_ "on" _ @expression)?
    { return { kind: "DistinctExpression", value, on: on ?? undefined, loc: location() } }
    / head: application_expression tail:(_ op: ("where" / "with" / "drop" / "select") _ right: application_expression { return {
      kind: { where: "WhereExpression", with: "WithExpression", drop: "DropExpression", select: "SelectExpression" }[op],
      right,
      loc: location()
    }})*
    { return tail.reduce((t, h) => ({ ...h, left: t }), head) }

  application_expression
    = head: index_expression tail:([ \\t]+ right: index_expression { return { kind: "ApplicationExpression", right, loc: location() }})*
    { return tail.reduce((t, h) => ({ ...h, left: t }), head) }

  index_expression
    = head: dot_expression tail:("[" _ index: expression _ "]" { return { kind: "IndexExpression", index, loc: location() }})*
    { return tail.reduce((t, h) => ({ ...h, left: t }), head) }
    
  dot_expression
    = head: literal_expression tail:(_ op: (".") _ right: literal_expression { return {
      kind: 'DotExpression',
      op,
      right,
      loc: location()
    }})*
    { return tail.reduce((t, h) => ({ ...h, left: t }), head) }

  literal_expression
    = placeholder_expression
    / float_expression
    / integer_expression
    / string_expression
    / env_expression
    / boolean_expression
    / null_expression
    / group_expression
    / block_expression
    / array_expression
    / object_expression
    / spread_expression
    / identifier_expression

  identifier_expression
    = name: identifier
    { return { kind: "IdentifierExpression", name, loc: location()  } }

  placeholder_expression
    = "$" name: identifier extra: (
        "~" name0: free_identifier ":" name1: identifier { return { extract: undefined, kindCondition: name0, typeCondition: name1, spread: undefined } }
      / ":" name0: identifier "~" name1: free_identifier { return { extract: undefined, kindCondition: name1, typeCondition: name0, spread: undefined } }
      / "~" name0: free_identifier "..." method: ("rl" / "lr") overrides: (idx: [0-9]+ "=" value: expression { return { index: Number(idx), value } })* { return {  extract: undefined, kindCondition: name0, spread: { method, overrides }, typeCondition: undefined } }
      / "." name0: free_identifier { return { extract: name0, kindCondition: undefined, typeCondition: undefined, spread: undefined } }
      / "~" name0: free_identifier { return { extract: undefined, kindCondition: name0, typeCondition: undefined, spread: undefined } }
      / ":" name0: identifier { return { extract: undefined, kindCondition: undefined, typeCondition: name0, spread: undefined } }
    )?
    { return { kind: "PlaceholderExpression", name, ...(extra ?? { extract: undefined, kindCondition: undefined, typeCondition: undefined, spread: undefined }), loc: location()  } }

  integer_expression
    = value: ("0" / head: [1-9] tail: [0-9]* { return head + tail.join("") })
    { return { kind: "IntegerExpression", value: Number(value), loc: location() } }

  float_expression
    = integer_part: ("0" / head: [1-9] tail: [0-9]* { return head + tail.join("") }) "." decimal_part: [0-9]+
    { return { kind: "FloatExpression", value: integer_part + "." + decimal_part, loc: location()  } }

  string_expression
    = "\\"" chars: [^\\"]* "\\""
    { return { kind: "StringExpression", value: chars.join(""), loc: location()  } }

  env_expression
    = "$\\"" chars: [^\\"]* "\\""
    { return { kind: "EnvExpression", value: chars.join(""), loc: location()  } }

  boolean_expression
    = value: ("true" / "false")
    { return { kind: "BooleanExpression", value: value === "true", loc: location()  } }

  null_expression
    = "null"
    { return { kind: "NullExpression", loc: location()  } }

  group_expression
    = "(" _ value: expression _ ")"
    { return value }

  block_expression
    = "do" _ expressions: (@expression [\\n\\r;] _)* _ "end"
    { return { kind: "BlockExpression", expressions, loc: location()  } }

  object_expression
    = "{" _ properties: (h: expression t: (_ "," _ @expression)* _ ("," _)? { return [h, ...t] } )? "}"
    { return { kind: "ObjectExpression", properties: properties ?? [], loc: location()  } }

  array_expression
    = "[" _ values: (h: expression t: (_ "," _ @expression)* _ ("," _)? { return [h, ...t] } )? "]"
    { return { kind: "ArrayExpression", values: values ?? [], loc: location()  } }

  spread_expression
    = "..." _ value: expression
    { return { kind: "SpreadExpression", value, loc: location()  } }
`);