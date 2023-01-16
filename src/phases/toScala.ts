import { readFileSync } from "fs";
import { readFile } from "fs/promises";
import { normalize } from "../ast/relt/typed/utils";
import { inspect } from "util";
import { toTypedExpression } from "..";
import { TypedTopLevelExpression } from "../ast/relt/topLevel";
import { AnyType, TableType, Type } from "../ast/relt/type";
import { TypedApplicationExpression, TypedAssignExpression, TypedDropExpression, TypedExpression, TypedGroupByExpression, TypedIdentifierExpression, TypedJoinExpression, TypedObjectExpression, TypedObjectExpressionProperty, TypedSelectExpression, TypedTableExpression, TypedUnionExpression, TypedWhereExpression, TypedWithExpression } from "../ast/relt/typed";
import { visitVoid } from "../ast/relt/typed/utils";
import { InternalError, reportInternalError, reportUserError, UserError } from "../errors";
import { Scope } from "./typecheck";
import { relt } from "../ast/relt/builder";

export function fetchPath(path: string): string {
  while (path.includes("$")) {
    const idx = path.indexOf("$");
    const end = path.indexOf("/", idx);
    const env = path.slice(idx + 1, end);
    const value = process.env[env];
    if (value === undefined)
      reportUserError(`Environment variable ${env} is not defined`);
    path = path.slice(0, idx) + value + path.slice(end);
  }
  if (path.startsWith('file://'))
    return readFileSync(path.slice(7)).toString();
  else
    reportUserError(`Trying to fetch unknown path: ${path}`);
}

export interface Template {
  value: string;
  emit: () => void;
  emitOnce: () => void;
  replace: (pat: string) => (rep: string) => void;
}

export interface DatasetHandler {
  name: string;
}

export interface TableHook {
  emitDataset?: (ctx: EmitDatasetContext) => DatasetHandler;
}

export interface EmitDatasetContext {
  toDss: string;
  toDs: string;
  name: string;
  typeName: string;
  load: (name: string) => (path: string) => Template;
  project: string;
}

export interface Hooks {
  table: Record<string, TableHook>;
}

export interface TableInfo {
  id: number;
  value: TypedExpression<TableType>;
  incoming: number[];
  outgoing: number[];
  datasetHandler: string;
  isSource: boolean;
}

export type TableInfos = Record<string, TableInfo>;

export function gatherTableInfos(e: TypedExpression[]): TableInfos {
  let c = 0;
  const tables: TableInfos = {};
  const addTable = (x: TypedExpression & { type: TableType }) => {
    if (x.type.name in tables)
      reportInternalError(`While gathering tables ${x.type.name} is already found`);
    tables[x.type.name] = { id: c++, value: x, incoming: [], outgoing: [], datasetHandler: `${x.type.name}DatasetHandler`, isSource: false };
  };
  e.forEach(x => visitVoid(x, {
    TypedTableExpression: (x: TypedTableExpression) => {
      if (x.value.kind === "TypedAssignExpression" && x.value.right.kind !== "TypedObjectExpression") return;
      if (x.type.name in tables)
        reportInternalError(`While gathering tables ${x.type.name} is already found`);
      tables[x.type.name] = { id: c++, value: x, incoming: [], outgoing: [], datasetHandler: `${x.type.name}DatasetHandler`, isSource: false };
    },
    TypedJoinExpression: addTable,
    TypedUnionExpression: addTable,
    TypedGroupByExpression: addTable,
    TypedWhereExpression: addTable,
    TypedWithExpression: addTable,
    TypedDropExpression: addTable,
    TypedSelectExpression: addTable,
  }));
  const binAddDeps = (x: TypedExpression & { left: TypedExpression, right: TypedExpression, type: TableType }) => {
    const l = tables[(x.left.type as TableType).name];
    const r = tables[(x.right.type as TableType).name];
    const s = tables[x.type.name];
    s.incoming.push(l.id, r.id);
    l.outgoing.push(s.id);
    r.outgoing.push(s.id);
  };
  const unaryAddDeps = (x: TypedExpression & { left: TypedExpression, type: TableType }) => {
    const l = tables[(x.left.type as TableType).name];
    const s = tables[x.type.name];
    s.incoming.push(l.id);
    l.outgoing.push(s.id);
  };
  e.forEach(x => visitVoid(x, {
    TypedJoinExpression: binAddDeps,
    TypedUnionExpression: binAddDeps,
    TypedGroupByExpression: x => {
      const v = tables[(x.value.type as TableType).name];
      const s = tables[x.type.name];
      s.incoming.push(v.id);
      v.outgoing.push(s.id);
    },
    TypedWhereExpression: unaryAddDeps,
    TypedWithExpression: x => {
      const l = tables[(x.left.type as TableType).name];
      const s = tables[x.type.name];
      s.incoming.push(l.id);
      l.outgoing.push(s.id);
    },
    TypedDropExpression: unaryAddDeps,
    TypedSelectExpression: unaryAddDeps,
  }));
  for (const k of Object.keys(tables)) {
    tables[k].incoming = [...new Set(tables[k].incoming)];
    tables[k].outgoing = [...new Set(tables[k].outgoing)];
  }

  e.forEach(x => visitVoid(x, {
    TypedTableExpression: (x: TypedTableExpression) => {
      if (x.value.kind === "TypedAssignExpression" && x.value.right.kind !== "TypedObjectExpression") {
        const sub = (x.value.right.type as TableType).name;
        tables[x.type.name] = tables[sub];
        delete tables[sub];
      }
    },
  }));

  return tables;
}

export function toSpark(e: TypedExpression): string {
  switch (e.kind) {
    case "TypedInternalExpression":
    case "TypedEvalExpression":
    case "TypedPlaceholderExpression":
    case "TypedDeclareExpression":
    case "TypedFunctionExpression":
    case "TypedSpreadExpression":
    case "TypedTableExpression":
    case "TypedUnionExpression":
    case "TypedJoinExpression":
    case "TypedGroupByExpression":
    case "TypedWhereExpression":
    case "TypedWithExpression":
    case "TypedDropExpression":
    case "TypedSelectExpression":
    case "TypedObjectExpression":
      reportInternalError(`Cannot convert ${e.kind} to spark`);
    case "TypedLetExpression":
      return `val ${toSpark(e.value)}`;
    case "TypedAssignExpression":
      return `${toSpark(e.left)} = ${toSpark(e.right)}`;
    case "TypedConditionalExpression":
      if (e.op === "!?") {
        reportInternalError(`TODO`)
      }
      return `${toSpark(e.left)}.getOrElse(${e.right})`;
    case "TypedOrExpression":
      return `(${toSpark(e.left)} or ${toSpark(e.right)})`;
    case "TypedAndExpression":
      return `(${toSpark(e.left)} and ${toSpark(e.right)})`;
    case "TypedCmpExpression":
      return `(${toSpark(e.left)} ${{ '==': '===', '!=': "=!=", '<=': '<=', '>=': '>=', '<': '<', '>': '>' }[e.op]} ${toSpark(e.right)})`;
    case "TypedAddExpression":
      return `(${toSpark(e.left)} ${e.op} ${toSpark(e.right)})`;
    case "TypedMulExpression":
      return `(${toSpark(e.left)} ${e.op} ${toSpark(e.right)})`;
    case "TypedDotExpression":
      return `(${toSpark(e.left)}.col("${toSpark(e.right)}"))`;
    case "TypedApplicationExpression":
      return `(${toSpark(e.left)}(${toSpark(e.right)}))`;
    case "TypedIdentifierExpression":
      return e.name;
    case "TypedIntegerExpression":
    case "TypedFloatExpression":
    case "TypedStringExpression":
    case "TypedEnvExpression":
    case "TypedBooleanExpression":
      return `${e.value}`;
    case "TypedNullExpression":
      return `null`;
    case "TypedBlockExpression":
      return `{ ${e.expressions.map(toSpark).join('\n')} }`;
    case "TypedArrayExpression":
      if (e.type.kind === "TupleType")
        return `(${e.values.map(toSpark).join(', ')})`;
      return `Array${e.values.length === 0 ? `[Any]` : ``}(${e.values.map(toSpark).join(', ')})`;
    case "TypedIndexExpression":
      return `(${toSpark(e.left)}(${toSpark(e.index)}))`;
  }
}

const addOps = {
  "StringType": {
    "StringType": {
      "+": ["func", "concat"],
    },
  },
  "FloatType": {
    "FloatType": {
      "+": ["infix", "+"],
      "-": ["infix", "-"],
    },
  },
  "IntType": {
    "IntType": {
      "+": ["infix", "+"],
      "-": ["infix", "-"],
    },
  },
};

const mulOps = {
  "FloatType": {
    "FloatType": {
      "*": ["infix", "*"],
      "/": ["infix", "/"],
      "%": ["infix", "%"],
    },
  },
  "IntType": {
    "IntType": {
      "*": ["infix", "*"],
      "/": ["infix", "/"],
      "%": ["infix", "%"],
    },
  },
};

export function deriveSparkExpression(e: TypedExpression): string {
  switch (e.kind) {
    case "TypedInternalExpression":
    case "TypedEvalExpression":
    case "TypedPlaceholderExpression":
    case "TypedDeclareExpression":
    case "TypedFunctionExpression":
    case "TypedSpreadExpression":
    case "TypedTableExpression":
    case "TypedUnionExpression":
    case "TypedJoinExpression":
    case "TypedGroupByExpression":
    case "TypedWhereExpression":
    case "TypedWithExpression":
    case "TypedDropExpression":
    case "TypedSelectExpression":
    case "TypedObjectExpression":
    case "TypedLetExpression":
    case "TypedAssignExpression":
    case "TypedDotExpression":
    case "TypedApplicationExpression":
    case "TypedBlockExpression":
    case "TypedArrayExpression":
    case "TypedIndexExpression":
      reportInternalError(`Cannot convert ${e.kind} to spark`);
    case "TypedConditionalExpression": {
      const left = deriveSparkExpression(e.left);
      switch (e.op) {
        case "!?":
          return `when(isNotNull(${left}), ${deriveSparkExpression(e.right)}).otherwise(${left})`;
        case "??":
          return `when(isNull(${left}), ${deriveSparkExpression(e.right)}).otherwise(${left})`;
      }
    }
    case "TypedOrExpression":
      return `(${toSpark(e.left)} or ${toSpark(e.right)})`;
    case "TypedAndExpression":
      return `(${toSpark(e.left)} and ${toSpark(e.right)})`;
    case "TypedCmpExpression":
      return `(${toSpark(e.left)} ${{ '==': '===', '!=': "=!=", '<=': '<=', '>=': '>=', '<': '<', '>': '>' }[e.op]} ${toSpark(e.right)})`;
    case "TypedAddExpression": {
      const res = (addOps as any)[e.left.type.kind][e.right.type.kind][e.op];
      if (res === undefined)
        reportInternalError(`Spark add op missing??`);
      return res[0] === "func" ? `${res[1]}(${deriveSparkExpression(e.left)}, ${deriveSparkExpression(e.right)})` : `(${deriveSparkExpression(e.left)} ${res[1]} ${deriveSparkExpression(e.right)})`;
    }
    case "TypedMulExpression": {
      const res = (mulOps as any)[e.left.type.kind][e.right.type.kind][e.op];
      if (res === undefined)
        reportInternalError(`Spark mul op missing??`);
      return res[0] === "func" ? `${res[1]}(${deriveSparkExpression(e.left)}, ${deriveSparkExpression(e.right)})` : `(${deriveSparkExpression(e.left)} ${res[1]} ${deriveSparkExpression(e.right)})`;
    }
    case "TypedIdentifierExpression":
      return `col("${e.name}")`;
    case "TypedIntegerExpression":
      return `lit(${e.value})`;
    case "TypedFloatExpression":
      return `lit(${Number(e.value)})`;
    case "TypedStringExpression":
      return `lit("${e.value}")`;
    case "TypedEnvExpression":
      return `lit(sys.env("${e.value}"))`;
    case "TypedBooleanExpression":
      return `lit(${e.value})`;
    case "TypedNullExpression":
      return `null`;
  }
}

export function deriveSparkExpressions(e: TypedObjectExpressionProperty[]) {
  return e.map(x => {
    switch (x.kind) {
      case "TypedDeclareExpression":
      case "TypedIdentifierExpression":
      case "TypedSpreadExpression":
        reportInternalError(`TODO`);
      case "TypedAssignExpression":
        switch (x.left.kind) {
          case "TypedArrayExpression":
          case "TypedObjectExpression":
            reportInternalError(`TODO`);
          case "TypedIdentifierExpression":
            return `.withColumn("${x.left.name}", ${deriveSparkExpression(x.right)})`;
        }
    }
  }).join('');
}

function unionDatasetHandler(info: TableInfo) {
  const rt = relt.typed;
  const value = info.value as TypedUnionExpression;
  return (
    rt.object([
      rt.assign(
        rt.id("emitDataset"),
        rt.lambda(
          [
            rt.declare(
              rt.id("ctx"),
              relt.type.id("EmitDatasetContext")
            ),
          ],
          rt.block([
            rt.app(
              rt.dot(
                rt.app(
                  rt.app(
                    rt.dot(rt.id("ctx"), rt.id("load")),
                    rt.string("UnionDatasetHandlerClass")
                  ),
                  rt.string(
                    "file://$RELT_HOME/templates/utils/unionDatasetHandler.scala"
                  )
                ),
                rt.id("emitOnce")
              ),
              rt.block([])
            ),
            rt.let(
              rt.assign(
                rt.id("t"),
                rt.app(
                  rt.app(
                    rt.dot(rt.id("ctx"), rt.id("load")),
                    rt.string("UnionDatasetHandler")
                  ),
                  rt.string(
                    "file://$RELT_HOME/templates/unionDatasetHandler.scala"
                  )
                )
              )
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_NAME_")
              ),
              rt.dot(rt.id("ctx"), rt.id("name"))
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_PROJECT_")
              ),
              rt.dot(rt.id("ctx"), rt.id("project"))
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_TYPE_")
              ),
              rt.dot(rt.id("ctx"), rt.id("typeName"))
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_L_TYPE_")
              ),
              rt.string((value.left.type as TableType).name)
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_R_TYPE_")
              ),
              rt.string((value.right.type as TableType).name)
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_TO_DS_")
              ),
              rt.dot(rt.id("ctx"), rt.id("toDs"))
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_TO_DSS_")
              ),
              rt.dot(rt.id("ctx"), rt.id("toDss"))
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_TO_L_DS_")
              ),
              rt.string(`(dss) => dss._${info.incoming[0] + 1}`)
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_TO_R_DS_")
              ),
              rt.string(`(dss) => dss._${info.incoming[info.incoming.length - 1] + 1}`)
            ),
            rt.app(
              rt.dot(rt.id("t"), rt.id("emit")),
              rt.block([])
            ),
            rt.object([
              rt.assign(
                rt.id("name"),
                rt.add(
                  rt.dot(rt.id("ctx"), rt.id("name")),
                  "+",
                  rt.string("UnionDatasetHandler")
                )
              ),
              rt.assign(
                rt.id("isSource"),
                rt.bool(false),
              ),
            ]),
          ])
        )
      ),
    ])
  );
}

function withDatasetHandler(info: TableInfo) {
  const rt = relt.typed;
  const value = info.value as TypedWithExpression;
  return (
    rt.object([
      rt.assign(
        rt.id("emitDataset"),
        rt.lambda(
          [
            rt.declare(
              rt.id("ctx"),
              relt.type.id("EmitDatasetContext")
            ),
          ],
          rt.block([
            rt.let(
              rt.assign(
                rt.id("t"),
                rt.app(
                  rt.app(
                    rt.dot(rt.id("ctx"), rt.id("load")),
                    rt.string("WithDatasetHandler")
                  ),
                  rt.string(
                    "file://$RELT_HOME/templates/withDatasetHandler.scala"
                  )
                )
              )
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_NAME_")
              ),
              rt.dot(rt.id("ctx"), rt.id("name"))
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_PROJECT_")
              ),
              rt.dot(rt.id("ctx"), rt.id("project"))
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_TYPE_")
              ),
              rt.dot(rt.id("ctx"), rt.id("typeName"))
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_TO_DSS_")
              ),
              rt.dot(rt.id("ctx"), rt.id("toDss"))
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_TO_L_DS_")
              ),
              rt.string(`(dss: Test.Datasets) => dss._${info.incoming[0] + 1}`)
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_EXPRESSIONS_")
              ),
              rt.string(deriveSparkExpressions(value.right.properties))
            ),
            rt.app(
              rt.dot(rt.id("t"), rt.id("emit")),
              rt.block([])
            ),
            rt.object([
              rt.assign(
                rt.id("name"),
                rt.add(
                  rt.dot(rt.id("ctx"), rt.id("name")),
                  "+",
                  rt.string("WithDatasetHandler")
                )
              ),
              rt.assign(
                rt.id("isSource"),
                rt.bool(false),
              ),
            ]),
          ])
        )
      ),
    ])
  );
}

function joinDatasetHandler(info: TableInfo) {
  const rt = relt.typed;
  const value = info.value as TypedJoinExpression;
  return (
    rt.object([
      rt.assign(
        rt.id("emitDataset"),
        rt.lambda(
          [
            rt.declare(
              rt.id("ctx"),
              relt.type.id("EmitDatasetContext")
            ),
          ],
          rt.block([
            rt.app(
              rt.dot(
                rt.app(
                  rt.app(
                    rt.dot(rt.id("ctx"), rt.id("load")),
                    rt.string("JoinDatasetHandlerClass")
                  ),
                  rt.string(
                    "file://$RELT_HOME/templates/utils/joinDatasetHandler.scala"
                  )
                ),
                rt.id("emitOnce")
              ),
              rt.block([])
            ),
            rt.let(
              rt.assign(
                rt.id("t"),
                rt.app(
                  rt.app(
                    rt.dot(rt.id("ctx"), rt.id("load")),
                    rt.string("JoinDatasetHandler")
                  ),
                  rt.string(
                    "file://$RELT_HOME/templates/joinDatasetHandler.scala"
                  )
                )
              )
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_NAME_")
              ),
              rt.dot(rt.id("ctx"), rt.id("name"))
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_PROJECT_")
              ),
              rt.dot(rt.id("ctx"), rt.id("project"))
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_TYPE_")
              ),
              rt.dot(rt.id("ctx"), rt.id("typeName"))
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_L_TYPE_")
              ),
              rt.string((value.left.type as TableType).name)
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_R_TYPE_")
              ),
              rt.string((value.right.type as TableType).name)
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_TO_DS_")
              ),
              rt.dot(rt.id("ctx"), rt.id("toDs"))
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_TO_DSS_")
              ),
              rt.dot(rt.id("ctx"), rt.id("toDss"))
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_TO_L_DS_")
              ),
              rt.string(`(dss) => dss._${info.incoming[0] + 1}`)
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_TO_R_DS_")
              ),
              rt.string(`(dss) => dss._${info.incoming[info.incoming.length - 1] + 1}`)
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_TO_CONDITION_")
              ),
              rt.string(`(l, r) => ${deriveSparkExpression(value.on)}`)
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_METHOD_")
              ),
              rt.string(`${value.method}`)
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_DROPS_")
              ),
              rt.string(`Seq[String]()`)
            ),
            rt.app(
              rt.dot(rt.id("t"), rt.id("emit")),
              rt.block([])
            ),
            rt.object([
              rt.assign(
                rt.id("name"),
                rt.add(
                  rt.dot(rt.id("ctx"), rt.id("name")),
                  "+",
                  rt.string("JoinDatasetHandler")
                )
              ),
              rt.assign(
                rt.id("isSource"),
                rt.bool(false),
              ),
            ]),
          ])
        )
      ),
    ])
  );
}

function groupByDatasetHandler(info: TableInfo) {
  const rt = relt.typed;
  const value = info.value as TypedGroupByExpression;
  return (
    rt.object([
      rt.assign(
        rt.id("emitDataset"),
        rt.lambda(
          [
            rt.declare(
              rt.id("ctx"),
              relt.type.id("EmitDatasetContext")
            ),
          ],
          rt.block([
            rt.app(
              rt.dot(
                rt.app(
                  rt.app(
                    rt.dot(rt.id("ctx"), rt.id("load")),
                    rt.string("GroupByDatasetHandlerClass")
                  ),
                  rt.string(
                    "file://$RELT_HOME/templates/utils/groupByDatasetHandler.scala"
                  )
                ),
                rt.id("emitOnce")
              ),
              rt.block([])
            ),
            rt.let(
              rt.assign(
                rt.id("t"),
                rt.app(
                  rt.app(
                    rt.dot(rt.id("ctx"), rt.id("load")),
                    rt.string("GroupByDatasetHandler")
                  ),
                  rt.string(
                    "file://$RELT_HOME/templates/groupByDatasetHandler.scala"
                  )
                )
              )
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_NAME_")
              ),
              rt.dot(rt.id("ctx"), rt.id("name"))
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_PROJECT_")
              ),
              rt.dot(rt.id("ctx"), rt.id("project"))
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_TYPE_")
              ),
              rt.dot(rt.id("ctx"), rt.id("typeName"))
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_T_TYPE_")
              ),
              rt.string((value.value.type as TableType).name)
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_TO_DS_")
              ),
              rt.dot(rt.id("ctx"), rt.id("toDs"))
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_TO_DSS_")
              ),
              rt.dot(rt.id("ctx"), rt.id("toDss"))
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_TO_T_DS_")
              ),
              rt.string(`(dss) => dss._${info.incoming[0] + 1}`)
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_COLS_")
              ),
              rt.string(`(ds) => Seq[Column]()`)
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_AGGS_")
              ),
              rt.string(`(ds) => Seq[Column]()`)
            ),
            rt.app(
              rt.dot(rt.id("t"), rt.id("emit")),
              rt.block([])
            ),
            rt.object([
              rt.assign(
                rt.id("name"),
                rt.add(
                  rt.dot(rt.id("ctx"), rt.id("name")),
                  "+",
                  rt.string("UnionDatasetHandler")
                )
              ),
              rt.assign(
                rt.id("isSource"),
                rt.bool(false),
              ),
            ]),
          ])
        )
      ),
    ])
  );
}

function whereDatasetHandler(info: TableInfo) {
  const rt = relt.typed;
  const value = info.value as TypedWhereExpression;
  return (
    rt.object([
      rt.assign(
        rt.id("emitDataset"),
        rt.lambda(
          [
            rt.declare(
              rt.id("ctx"),
              relt.type.id("EmitDatasetContext")
            ),
          ],
          rt.block([
            rt.app(
              rt.dot(
                rt.app(
                  rt.app(
                    rt.dot(rt.id("ctx"), rt.id("load")),
                    rt.string("WhereDatasetHandlerClass")
                  ),
                  rt.string(
                    "file://$RELT_HOME/templates/utils/whereDatasetHandler.scala"
                  )
                ),
                rt.id("emitOnce")
              ),
              rt.block([])
            ),
            rt.let(
              rt.assign(
                rt.id("t"),
                rt.app(
                  rt.app(
                    rt.dot(rt.id("ctx"), rt.id("load")),
                    rt.string("WhereDatasetHandler")
                  ),
                  rt.string(
                    "file://$RELT_HOME/templates/whereDatasetHandler.scala"
                  )
                )
              )
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_NAME_")
              ),
              rt.dot(rt.id("ctx"), rt.id("name"))
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_PROJECT_")
              ),
              rt.dot(rt.id("ctx"), rt.id("project"))
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_TYPE_")
              ),
              rt.dot(rt.id("ctx"), rt.id("typeName"))
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_TO_DS_")
              ),
              rt.dot(rt.id("ctx"), rt.id("toDs"))
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_TO_DSS_")
              ),
              rt.dot(rt.id("ctx"), rt.id("toDss"))
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_TO_VALUE_DS_")
              ),
              rt.string(`(dss) => dss._${info.incoming[0] + 1}`)
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_TO_CONDITION_")
              ),
              rt.string(`(ds) => ${deriveSparkExpression(value.right)}`)
            ),
            rt.app(
              rt.dot(rt.id("t"), rt.id("emit")),
              rt.block([])
            ),
            rt.object([
              rt.assign(
                rt.id("name"),
                rt.add(
                  rt.dot(rt.id("ctx"), rt.id("name")),
                  "+",
                  rt.string("WhereDatasetHandler")
                )
              ),
              rt.assign(
                rt.id("isSource"),
                rt.bool(false),
              ),
            ]),
          ])
        )
      ),
    ])
  );
}

function dropDatasetHandler(info: TableInfo) {
  const rt = relt.typed;
  const value = info.value as TypedDropExpression;
  const columns = value.right.kind === "TypedIdentifierExpression" ? [value.right.name] : value.right.values.map(x => x.name);
  return (
    rt.object([
      rt.assign(
        rt.id("emitDataset"),
        rt.lambda(
          [
            rt.declare(
              rt.id("ctx"),
              relt.type.id("EmitDatasetContext")
            ),
          ],
          rt.block([
            rt.app(
              rt.dot(
                rt.app(
                  rt.app(
                    rt.dot(rt.id("ctx"), rt.id("load")),
                    rt.string("DropDatasetHandlerClass")
                  ),
                  rt.string(
                    "file://$RELT_HOME/templates/utils/dropDatasetHandler.scala"
                  )
                ),
                rt.id("emitOnce")
              ),
              rt.block([])
            ),
            rt.let(
              rt.assign(
                rt.id("t"),
                rt.app(
                  rt.app(
                    rt.dot(rt.id("ctx"), rt.id("load")),
                    rt.string("DropDatasetHandler")
                  ),
                  rt.string(
                    "file://$RELT_HOME/templates/dropDatasetHandler.scala"
                  )
                )
              )
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_NAME_")
              ),
              rt.dot(rt.id("ctx"), rt.id("name"))
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_PROJECT_")
              ),
              rt.dot(rt.id("ctx"), rt.id("project"))
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_TYPE_")
              ),
              rt.dot(rt.id("ctx"), rt.id("typeName"))
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_T_TYPE_")
              ),
              rt.string((value.left.type as TableType).name)
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_TO_DS_")
              ),
              rt.dot(rt.id("ctx"), rt.id("toDs"))
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_TO_DSS_")
              ),
              rt.dot(rt.id("ctx"), rt.id("toDss"))
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_TO_T_DS_")
              ),
              rt.string(`(dss) => dss._${info.incoming[0] + 1}`)
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_ON_")
              ),
              rt.string(`(ds) => Seq(${columns.map(x => `"${x}"`).join(", ")})`)
            ),
            rt.app(
              rt.dot(rt.id("t"), rt.id("emit")),
              rt.block([])
            ),
            rt.object([
              rt.assign(
                rt.id("name"),
                rt.add(
                  rt.dot(rt.id("ctx"), rt.id("name")),
                  "+",
                  rt.string("DropDatasetHandler")
                )
              ),
              rt.assign(
                rt.id("isSource"),
                rt.bool(false),
              ),
            ]),
          ])
        )
      ),
    ])
  );
}

function selectDatasetHandler(info: TableInfo) {
  const rt = relt.typed;
  const value = info.value as TypedSelectExpression;
  const columns = value.right.kind === "TypedIdentifierExpression" ? [value.right.name] : value.right.values.map(x => x.name);
  return (
    rt.object([
      rt.assign(
        rt.id("emitDataset"),
        rt.lambda(
          [
            rt.declare(
              rt.id("ctx"),
              relt.type.id("EmitDatasetContext")
            ),
          ],
          rt.block([
            rt.app(
              rt.dot(
                rt.app(
                  rt.app(
                    rt.dot(rt.id("ctx"), rt.id("load")),
                    rt.string("SelectDatasetHandlerClass")
                  ),
                  rt.string(
                    "file://$RELT_HOME/templates/utils/selectDatasetHandler.scala"
                  )
                ),
                rt.id("emitOnce")
              ),
              rt.block([])
            ),
            rt.let(
              rt.assign(
                rt.id("t"),
                rt.app(
                  rt.app(
                    rt.dot(rt.id("ctx"), rt.id("load")),
                    rt.string("SelectDatasetHandler")
                  ),
                  rt.string(
                    "file://$RELT_HOME/templates/selectDatasetHandler.scala"
                  )
                )
              )
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_NAME_")
              ),
              rt.dot(rt.id("ctx"), rt.id("name"))
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_PROJECT_")
              ),
              rt.dot(rt.id("ctx"), rt.id("project"))
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_TYPE_")
              ),
              rt.dot(rt.id("ctx"), rt.id("typeName"))
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_T_TYPE_")
              ),
              rt.string((value.left.type as TableType).name)
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_TO_DS_")
              ),
              rt.dot(rt.id("ctx"), rt.id("toDs"))
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_TO_DSS_")
              ),
              rt.dot(rt.id("ctx"), rt.id("toDss"))
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_TO_T_DS_")
              ),
              rt.string(`(dss) => dss._${info.incoming[0] + 1}`)
            ),
            rt.app(
              rt.app(
                rt.dot(rt.id("t"), rt.id("replace")),
                rt.string("_ON_")
              ),
              rt.string(`(ds) => Seq(${columns.map(x => `"${x}"`).join(', ')})`)
            ),
            rt.app(
              rt.dot(rt.id("t"), rt.id("emit")),
              rt.block([])
            ),
            rt.object([
              rt.assign(
                rt.id("name"),
                rt.add(
                  rt.dot(rt.id("ctx"), rt.id("name")),
                  "+",
                  rt.string("SelectDatasetHandler")
                )
              ),
              rt.assign(
                rt.id("isSource"),
                rt.bool(false),
              ),
            ]),
          ])
        )
      ),
    ])
  );
}

export function deriveDefaultTableHook(name: string, info: TableInfo): TypedObjectExpression<TypedAssignExpression<TypedIdentifierExpression>> {
  const value = info.value;

  switch (value.kind) {
    case "TypedTableExpression":
      return {
        kind: "TypedObjectExpression",
        properties: [],
        type: { kind: "ObjectType", properties: [] }
      };
    case "TypedUnionExpression":
      return unionDatasetHandler(info) as TypedObjectExpression<TypedAssignExpression<TypedIdentifierExpression>>;
    case "TypedWithExpression":
      return withDatasetHandler(info) as TypedObjectExpression<TypedAssignExpression<TypedIdentifierExpression>>;
    case "TypedWhereExpression":
      return whereDatasetHandler(info) as TypedObjectExpression<TypedAssignExpression<TypedIdentifierExpression>>;
    case "TypedDropExpression":
      return dropDatasetHandler(info) as TypedObjectExpression<TypedAssignExpression<TypedIdentifierExpression>>;
    case "TypedSelectExpression":
      return selectDatasetHandler(info) as TypedObjectExpression<TypedAssignExpression<TypedIdentifierExpression>>;
    case "TypedJoinExpression":
      return joinDatasetHandler(info) as TypedObjectExpression<TypedAssignExpression<TypedIdentifierExpression>>;
    case "TypedGroupByExpression":
      return groupByDatasetHandler(info) as TypedObjectExpression<TypedAssignExpression<TypedIdentifierExpression>>;
    default:
      reportInternalError(`Cannot derive default table hook for ${value.kind}`);
  }
}

export function tryDot(e: TypedExpression, name: string, scope: Scope, errorMsg: string): [TypedExpression, Scope] {
  try {
    return normalize({ kind: "TypedDotExpression", left: e, right: { kind: "TypedIdentifierExpression", name, type: { kind: "AnyType" } }, type: { kind: "AnyType" } }, scope);
  }
  catch (e) {
    if (e instanceof InternalError)
      reportUserError(errorMsg);
    else
      throw e;
  }
}

const any_t: AnyType = { kind: "AnyType" };

export function generateScalaType(name: string, type: Type): [string[], string] {
  switch (type.kind) {
    case "IntegerType":
      return [[], `Int`];
    case "StringType":
      return [[], `String`];
    case "AnyType":
      return [[], `Any`];
    case "ArrayType": {
      const [extra, of] = generateScalaType(name, type.of);
      return [extra, `Array[${of}]`];
    }
    case "BooleanType":
      return [[], `Boolean`];
    case "FloatType":
      return [[], `Double`];
    case "TableType":
      return [generateCaseClass(name, type.columns), name];
    case "ObjectType":
      return [generateCaseClass(name, type.properties), name];
    case "NullType":
      return [[], `Null`];
    case "OptionalType": {
      const [extra, of] = generateScalaType(name, type.of);
      return [extra, `Option[${of}]`];
    }
    case "FunctionType": {
      const [extra1, from] = generateScalaType(name, type.from);
      const [extra2, to] = generateScalaType(name, type.to);
      return [extra1.concat(extra2), `(${from}) => ${to}`];
    }
    case "TupleType": {
      const data = type.types.map((t, i) => generateScalaType(`${name}${i}`, t));
      return [data.reduce<string[]>((p, c) => [...p, ...c[0]], []), type.types.length === 0 ? `Array[Nothing]` : type.types.length === 1 ? `Tuple1[${data[0][1]}]` : `(${data.map(x => x[1]).join(', ')})`];
    }
    case "NeverType":
      return [[], `Nothing`];
    case "IdentifierType":
      return [[], type.name];
    case "UnitType":
      return [[], `Unit`];
  }
}


export function generateCaseClass(name: string, props: { name: string, type: Type }[]): string[] {
  const [extra, properties] = props.reduce<[string[], string[]]>((p, c) => {
    const x = generateScalaType(c.name, c.type);
    return [p[0].concat(x[0]), p[1].concat(`${c.name}: ${x[1]}`)];
  }, [[], []]);

  return extra.concat(`case class ${name} (${properties.map(x => `\n\t${x},`).join('')}\n)\n\n`);
}

export async function toScala(tast: TypedTopLevelExpression[], scope: Scope): Promise<[string]> {
  let scalaSourceCode = "";

  const templates: Record<string, Template> = {};
  const loadTemplate = (name: string, path: string) => {
    let hasEmitted = false;
    if (!(name in templates))
      templates[name] = {
        value: fetchPath(path),
        emit() {
          scalaSourceCode += this.value;
        },
        emitOnce() {
          if (hasEmitted) return;
          this.emit();
          hasEmitted = true;
        },
        replace(pat) {
          return (rep) => this.value = this.value.replace(RegExp(pat, 'g'), rep);
        }
      };
    return { ...templates[name], value: templates[name].value.slice(0) };
  };

  const header = loadTemplate("header", "file://$RELT_HOME/templates/utils/header.scala");
  header.replace("_PACKAGE_")("Test");
  header.emit();

  const typedExprs: TypedExpression[] = tast.filter(x => x.kind !== "SugarDirective" && x.kind !== "TypedSugarDefinition" && x.kind !== "TypeIntroductionExpression") as TypedExpression[];

  const infos = gatherTableInfos(typedExprs);

  const tables = Object.entries(infos).sort(([, a], [, b]) => a.id - b.id);

  if (tables.length === 0)
    reportUserError(`Need at least one table defined`);

  scalaSourceCode += `package object ${"Test"} {\n\ttype Datasets = ${generateScalaType('', relt.type.tuple(tables.map(x => relt.type.id(`Dataset[${x[0]}]`))))[1]}\n}\n\n`;


  for (const [k, v] of tables) {

    scalaSourceCode += generateCaseClass(k, (v.value.type as TableType).columns);

    const userHooks = v.value.kind === "TypedTableExpression" ? v.value.hooks : [];
    const [hookExpr] = normalize(userHooks.reduceRight<TypedExpression>((p, c) => ({ kind: "TypedApplicationExpression", left: c, right: p, type: { kind: "IdentifierType", name: "ReltTableHook" } }), deriveDefaultTableHook(k, v)), scope);

    const [hook] = tryDot(hookExpr, "emitDataset", scope, `No emit dataset hook for table: ${k}`);

    const emitDatasetCtx: TypedExpression = {
      kind: "TypedObjectExpression",
      properties: [
        {
          kind: "TypedAssignExpression", type: any_t, left: { kind: "TypedIdentifierExpression", name: "name", type: any_t }, op: "=", right: {
            kind: "TypedStringExpression", value: `${k}`, type: { kind: "StringType" },
          }
        },
        {
          kind: "TypedAssignExpression", type: any_t, left: { kind: "TypedIdentifierExpression", name: "toDs", type: any_t }, op: "=", right: {
            kind: "TypedStringExpression", value: `(dss: Test.Datasets) => dss._${v.id + 1}`, type: { kind: "StringType" },
          }
        },
        {
          kind: "TypedAssignExpression", type: any_t, left: { kind: "TypedIdentifierExpression", name: "toDss", type: any_t }, op: "=", right: {
            kind: "TypedStringExpression", value: `(dss: Test.Datasets, ds: Dataset[${k}]) => ${tables.length === 1 ? `Tuple1` : ``}(${tables.map(x => x[1].id === v.id ? 'ds' : `dss._${x[1].id + 1}`)})`, type: { kind: "StringType" },
          }
        },
        {
          kind: "TypedAssignExpression", type: any_t, left: { kind: "TypedIdentifierExpression", name: "project", type: any_t }, op: "=", right: {
            kind: "TypedStringExpression", value: "Test", type: { kind: "StringType" },
          }
        },
        {
          kind: "TypedAssignExpression", type: any_t, left: { kind: "TypedIdentifierExpression", name: "typeName", type: any_t }, op: "=", right: {
            kind: "TypedStringExpression", value: (v.value.type as TableType).name, type: { kind: "StringType" },
          }
        },
        {
          kind: "TypedAssignExpression", type: any_t, left: { kind: "TypedIdentifierExpression", name: "load", type: any_t }, op: "=", right: {
            kind: "TypedInternalExpression", type: { kind: "FunctionType", from: any_t, to: any_t }, value: name => (
              {
                kind: "TypedInternalExpression", type: { kind: "FunctionType", from: any_t, to: any_t }, value: path => {
                  if (name.kind !== "TypedStringExpression" || path.kind !== "TypedStringExpression")
                    reportInternalError(`Called load with ${name.kind} and ${path.kind} This should have got caught in typechecking or some bug occurred in normalization`);
                  const t = loadTemplate(name.value, path.value);
                  return {
                    kind: "TypedObjectExpression",
                    properties: [
                      {
                        kind: "TypedAssignExpression", type: any_t, left: { kind: "TypedIdentifierExpression", name: "replace", type: any_t }, op: "=", right: {
                          kind: "TypedInternalExpression", type: { kind: "FunctionType", from: any_t, to: any_t }, value: find => ({
                            kind: "TypedInternalExpression", type: { kind: "FunctionType", from: any_t, to: any_t }, value: replace => {
                              if (find.kind !== "TypedStringExpression" || replace.kind !== "TypedStringExpression")
                                reportInternalError(`Called load with ${name.kind} and ${path.kind} This should have got caught in typechecking or some bug occurred in normalization`);
                              t.replace(find.value)(replace.value);
                              return { kind: "TypedBlockExpression", expressions: [], type: { kind: "UnitType" } };
                            }
                          })
                        }
                      },
                      {
                        kind: "TypedAssignExpression", type: any_t, left: { kind: "TypedIdentifierExpression", name: "emitOnce", type: any_t }, op: "=", right: {
                          kind: "TypedInternalExpression", type: { kind: "FunctionType", from: any_t, to: any_t }, value: _ => {
                            t.emitOnce();
                            return { kind: "TypedBlockExpression", expressions: [], type: { kind: "UnitType" } };
                          }
                        }
                      },
                      {
                        kind: "TypedAssignExpression", type: any_t, left: { kind: "TypedIdentifierExpression", name: "emit", type: any_t }, op: "=", right: {
                          kind: "TypedInternalExpression", type: { kind: "FunctionType", from: any_t, to: any_t }, value: _ => {
                            t.emit();
                            return { kind: "TypedBlockExpression", expressions: [], type: { kind: "UnitType" } };
                          }
                        }
                      }
                    ],
                    type: { kind: "ObjectType", properties: [], }
                  }
                }
              }
            )
          },
        },
      ],
      type: { kind: "ObjectType", properties: [] }
    };

    const [x] = normalize({ kind: "TypedApplicationExpression", left: hook, right: emitDatasetCtx, type: { kind: "AnyType" } }, scope);
    const [t] = tryDot(x, 'name', scope, `No name on table hook`);
    if (t.kind !== "TypedStringExpression")
      reportInternalError(``);
    const [t1] = tryDot(x, 'isSource', scope, `No name on table hook`);
    if (t1.kind !== "TypedBooleanExpression")
      reportInternalError(``);

    infos[k].datasetHandler = t.value;
    infos[k].isSource = t1.value;
  }

  scalaSourceCode += mainClass("Test", "Project", infos);

  return [scalaSourceCode];
}

export function mainClass(packageName: string, projectName: string, infos: TableInfos) {
  const tables = Object.values(infos).sort((a, b) => a.id - b.id);
  return `\
object ${projectName} {
  private val dg = new DependencyGraph[DatasetHandler[${packageName}.Datasets]](Map(
${tables.map(x => `\t\t${x.id} -> new Vertex(${x.id}, Array(${x.incoming.join(', ')}), Array(${x.outgoing.join(', ')}), ${x.datasetHandler}),`).join('\n')}
  ))
  private val sourceTables = Map[String, Int](
${tables.filter(x => x.isSource).map(x => `\t\t"${(x.value.type as TableType).name}" -> ${x.id},`).join('\n')}
  )
  private val sourceTableRelations = Map[Int, Map[Int, Column]]()
  private val planParsers = Seq[PlanParser](
    RefreshPlanParser,
    DeltaPlanParser,
  )

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("${projectName}")
      .master("local") // LOCAL
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    var requestedPlans = Seq[Plan]()
    var idx = 0
    val stop = new Breaks

    while (idx < args.size) {
      stop.breakable {
        for (parser <- this.planParsers) {
          val res = parser.maybeParse(args, idx, this.sourceTables)
          res match {
            case Some((plan, i)) => {
              idx = i
              requestedPlans :+= plan
              stop.break
            }
            case None => {}
          }
        }
        System.err.println(s"\${args.slice(idx, -1).mkString(" ")} bad request")
        System.exit(1)
      }
    }

    if (requestedPlans.size == 0) {
      println("Nothing todo")
      System.exit(0)
    }
    if (requestedPlans.size > 1) {
      println("More than one source table being requested is not allowed as of now")
      System.exit(1)
    }

    val id = requestedPlans(0).id

    val dg = this.dg.addEdges(this.sourceTables.values.toSeq.map(x => (id, x))).familyOf(requestedPlans.map(_.id).toArray)

    val executionPlans = dg.topologicalSort.foldLeft(LinkedHashMap(requestedPlans.map(x => (x.id -> x)): _*))((p, c) => c.data.constructPlan(c.id, id, this.sourceTableRelations, dg, p))

    var dss = ${tables.length === 1 ? `Tuple1` : ``}(${tables.map(x => `spark.emptyDataset[${(x.value.type as TableType).name}]`).join(', ')})

    for ((id, plan) <- executionPlans) {
      dss = this.dg.get(id).construct(spark, plan, dss)
    }

    println("DONE!")
  }
}`;
}
