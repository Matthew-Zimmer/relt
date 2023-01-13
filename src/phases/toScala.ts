import { readFile } from "fs/promises";
import { toTypedExpression } from "..";
import { TypedTopLevelExpression } from "../ast/relt/topLevel";
import { TableType } from "../ast/relt/type";
import { TypedExpression } from "../ast/relt/typed";
import { visitVoid } from "../ast/relt/typed/utils";
import { InternalError, reportInternalError, reportUserError } from "../errors";

export async function fetchPath(path: string): Promise<string> {
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
    return (await readFile(path.slice(7))).toString();
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
  emitDataset: (ctx: EmitDatasetContext) => Promise<DatasetHandler>;
}

export interface EmitDatasetContext {
  toDss: string;
  toDs: string;
  name: string;
  typeName: string;
  load: (name: string) => (path: string) => Promise<Template>;
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
}

export type TableInfos = Record<string, TableInfo>;

export function gatherTableInfos(e: TypedExpression[]): TableInfos {
  let c = 0;
  const tables: TableInfos = {};
  const addTable = (x: TypedExpression & { type: TableType }) => {
    if (x.type.name in tables)
      reportInternalError(`While gathering tables ${x.type.name} is already found`);
    tables[x.type.name] = { id: c++, value: x, incoming: [], outgoing: [] };
  };
  e.forEach(x => visitVoid(x, {
    TypedTableExpression: addTable,
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
    TypedWithExpression: binAddDeps,
    TypedDropExpression: unaryAddDeps,
    TypedSelectExpression: unaryAddDeps,
  }));
  for (const k of Object.keys(tables)) {
    tables[k].incoming = [...new Set(tables[k].incoming)];
    tables[k].outgoing = [...new Set(tables[k].outgoing)];
  }
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

export function deriveDefaultTableHook(name: string, info: TableInfo): TableHook {
  const value = info.value;
  return {
    async emitDataset(ctx) {
      switch (value.kind) {
        case "TypedJoinExpression": {
          (await ctx.load("JoinDatasetHandlerClass")("file://$RELT_HOME/templates/utils/joinDatasetHandler.scala")).emitOnce();
          const t = await ctx.load("JoinDatasetHandler")("file://$RELT_HOME/templates/joinDatasetHandler.scala");
          t.replace("_NAME_")(ctx.name);
          t.replace("_PROJECT_")(ctx.project);
          t.replace("_TYPE_")(ctx.typeName);
          t.replace("_L_TYPE_")((value.left.type as TableType).name);
          t.replace("_R_TYPE_")((value.right.type as TableType).name);
          t.replace("_TO_DS_")(ctx.toDs);
          t.replace("_TO_DSS_")(ctx.toDss);
          t.replace("_TO_L_DS_")(`(dss) => dss._${info.incoming[0] + 1}`);
          t.replace("_TO_R_DS_")(`(dss) => dss._${info.incoming[1] + 1}`);
          t.replace("_TO_CONDITION_")(`(l, r) => ${toSpark(value.on)}`);
          t.replace("_METHOD_")(value.method);
          t.replace("_DROPS_")(`Seq()`); // TODO empty list for now
          t.emit();
          return { name: `${ctx.name}DatasetHandler` };
        }
        case "TypedUnionExpression": {
          (await ctx.load("UnionDatasetHandlerClass")("file://$RELT_HOME/templates/utils/unionDatasetHandler.scala")).emitOnce();
          const t = await ctx.load("UnionDatasetHandler")("file://$RELT_HOME/templates/unionDatasetHandler.scala");
          t.replace("_NAME_")(ctx.name);
          t.replace("_PROJECT_")(ctx.project);
          t.replace("_TYPE_")(ctx.typeName);
          t.replace("_L_TYPE_")((value.left.type as TableType).name);
          t.replace("_R_TYPE_")((value.right.type as TableType).name);
          t.replace("_TO_DS_")(ctx.toDs);
          t.replace("_TO_DSS_")(ctx.toDss);
          t.replace("_TO_L_DS_")(`(dss) => dss._${info.incoming[0] + 1}`);
          t.replace("_TO_R_DS_")(`(dss) => dss._${info.incoming[1] + 1}`);
          t.emit();
          return { name: `${ctx.name}DatasetHandler` };
        }
        case "TypedGroupByExpression": {
          reportInternalError(`TODO`);
        }
        case "TypedWhereExpression": {
          (await ctx.load("JoinDatasetHandlerClass")("file://$RELT_HOME/templates/utils/whereDatasetHandler.scala")).emitOnce();
          const t = await ctx.load("JoinDatasetHandler")("file://$RELT_HOME/templates/whereDatasetHandler.scala");
          t.replace("_NAME_")(ctx.name);
          t.replace("_PROJECT_")(ctx.project);
          t.replace("_TYPE_")(ctx.typeName);
          t.replace("_TO_DS_")(ctx.toDs);
          t.replace("_TO_DSS_")(ctx.toDss);
          t.replace("_TO_VALUE_DS_")(`(dss) => dss._${info.incoming[0] + 1}`);
          t.replace("_TO_CONDITION_")(`(l, r) => ${toSpark(value.right)}`);
          t.emit();
          return { name: `${ctx.name}DatasetHandler` };
        }
        case "TypedWithExpression": {
          reportInternalError(`TODO`);
        }
        case "TypedDropExpression": {
          (await ctx.load("JoinDatasetHandlerClass")("file://$RELT_HOME/templates/utils/dropDatasetHandler.scala")).emitOnce();
          const t = await ctx.load("JoinDatasetHandler")("file://$RELT_HOME/templates/dropDatasetHandler.scala");
          t.replace("_NAME_")(ctx.name);
          t.replace("_PROJECT_")(ctx.project);
          t.replace("_TYPE_")(ctx.typeName);
          t.replace("_T_TYPE_")((value.left.type as TableType).name);
          t.replace("_TO_DS_")(ctx.toDs);
          t.replace("_TO_DSS_")(ctx.toDss);
          t.replace("_TO_T_DS_")(`(dss) => dss._${info.incoming[0] + 1}`);
          t.replace("_ON_")(`Seq()`); // TODO empty list for now
          t.emit();
          return { name: `${ctx.name}DatasetHandler` };
        }
        case "TypedSelectExpression": {
          (await ctx.load("JoinDatasetHandlerClass")("file://$RELT_HOME/templates/utils/selectDatasetHandler.scala")).emitOnce();
          const t = await ctx.load("JoinDatasetHandler")("file://$RELT_HOME/templates/selectDatasetHandler.scala");
          t.replace("_NAME_")(ctx.name);
          t.replace("_PROJECT_")(ctx.project);
          t.replace("_TYPE_")(ctx.typeName);
          t.replace("_T_TYPE_")((value.left.type as TableType).name);
          t.replace("_TO_DS_")(ctx.toDs);
          t.replace("_TO_DSS_")(ctx.toDss);
          t.replace("_TO_T_DS_")(`(dss) => dss._${info.incoming[0] + 1}`);
          t.replace("_ON_")(`Seq()`); // TODO empty list for now
          t.emit();
          return { name: `${ctx.name}DatasetHandler` };
        }
        default:
          reportUserError(`Cannot derive the default table hook for ${info.value.kind}`);
      }
    }
  };
}

export async function toScala(tast: TypedTopLevelExpression[], hooks: Hooks) {
  let scalaSourceCode = "";

  const templates: Record<string, Template> = {};
  const loadTemplate = async (name: string, path: string) => {
    let hasEmitted = false;
    if (!(name in templates))
      templates[name] = {
        value: await fetchPath(path),
        emit() {
          scalaSourceCode += this.value;
        },
        emitOnce() {
          if (hasEmitted) return;
          this.emit();
          hasEmitted = true;
        },
        replace(pat) {
          return (rep) => this.value = this.value.replace(pat, rep);
        }
      };
    return { ...templates[name], value: templates[name].value.slice(0) };
  };

  const header = await loadTemplate("header", "file://$RELT_HOME/templates/utils/header.scala");
  header.replace("_PACKAGE_")("TEST");
  header.emit();

  const typedExprs: TypedExpression[] = tast.filter(x => x.kind !== "SugarDirective" && x.kind !== "TypedSugarDefinition" && x.kind !== "TypeIntroductionExpression") as TypedExpression[];

  const infos = gatherTableInfos(typedExprs);

  const tables = Object.entries(infos).sort(([, a], [, b]) => a.id - b.id);

  for (const [k, v] of tables) {
    const hook = hooks.table[k] ?? deriveDefaultTableHook(k, v);
    await hook.emitDataset({
      name: `${k}`,
      toDs: `(dss) => dss._${v.id + 1}`,
      toDss: `(dss, ds) => (${tables.map(x => x[1].id === v.id ? 'ds' : `dss._${x[1].id + 1}`)})`,
      project: "Test",
      typeName: (v.value.type as TableType).name,
      load: (name) => {
        return async (path) => {
          return await loadTemplate(name, path);
        }
      }
    });
  }

  scalaSourceCode += mainClass("Test", "Project", infos);

  return scalaSourceCode;
}

export function mainClass(packageName: string, projectName: string, infos: TableInfos) {
  const tables = Object.values(infos).sort((a, b) => a.id - b.id);
  return `\
object ${projectName} {
  private val dg = new DependencyGraph[DatasetHandler[${packageName}.Datasets]](Map(
${tables.map(x => `\t\t${x.id} -> new Vertex(${x.id}, Array(${x.incoming.join(', ')}), Array(${x.outgoing.join(', ')}), ${(x.value.type as TableType).name}DatasetHandler),`).join('\n')}
  ))
  private val sourceTables = Map[String, Int](
    "A" -> 0,
    "B" -> 1,
    "C" -> 2,
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

    var dss = (${tables.map(x => `spark.emptyDataset[${(x.value.type as TableType).name}]`).join(', ')})

    for ((id, plan) <- executionPlans) {
      dss = this.dg.get(id).construct(spark, plan, dss)
    }

    println("DONE!")
  }
}`;
}