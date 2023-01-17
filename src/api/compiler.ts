import { readFileSync } from "fs";
import { writeFile } from "fs/promises";
import { Expression } from "../compiler/ast/relt/source";
import { visitMap as visitMapSource, Visitor as SourceVisitor, visitVoid as visitVoidSource } from "../compiler/ast/relt/source/utils";
import { SugarDefinition, SugarDirective, TopLevelExpression, TypedTopLevelExpression } from "../compiler/ast/relt/topLevel";
import { TypedAssignExpression, TypedExpression, TypedIdentifierExpression } from "../compiler/ast/relt/typed";
import { visitMap as visitMapTyped, Visitor as TypedVisitor, visitVoid as visitVoidTyped } from "../compiler/ast/relt/typed/utils";
import { SparkProject } from "../compiler/ast/scala";
import { reportInternalError } from "../compiler/errors";
import { checkSugars } from "../compiler/phases/checkSugar";
import { desugar } from "../compiler/phases/desugar";
import { parse } from "../compiler/phases/parse";
import { preTypeCheckDesugar } from "../compiler/phases/preTypeCheckDesugar";
import { toScala } from "../compiler/phases/toScala";
import { Context, Scope, typeCheck } from "../compiler/phases/typecheck";
import { writeSparkProject } from "../compiler/phases/writeSparkProject";
import { ReltProject } from "./project";

export function toTypedExpression(x: any): TypedExpression {
  switch (typeof x) {
    case "bigint":
    case "symbol":
    case "undefined":
    case "function":
      reportInternalError(`Cannot convert ${typeof x} to typed expression`);
    case "boolean":
      return { kind: "TypedBooleanExpression", value: x, type: { kind: "BooleanType" } };
    case "number":
      if (Number.isInteger(x))
        return { kind: "TypedIntegerExpression", value: x, type: { kind: "IntegerType" } };
      else
        return { kind: "TypedFloatExpression", value: `${x}`, type: { kind: "FloatType" } };
    case "object":
      if (x === null)
        return { kind: "TypedNullExpression", type: { kind: "NullType" } };
      else if (Array.isArray(x))
        return { kind: "TypedArrayExpression", values: x.map(toTypedExpression), type: { kind: "ArrayType", of: { kind: "AnyType" } } };
      else
        return { kind: "TypedObjectExpression", properties: Object.entries(x).map<TypedAssignExpression<TypedIdentifierExpression>>(x => ({ kind: "TypedAssignExpression", left: { kind: "TypedIdentifierExpression", name: x[0], type: { kind: "AnyType" } }, op: "=", right: toTypedExpression(x[1]), type: { kind: "AnyType" } })), type: { kind: "ObjectType", properties: [] } };
    case "string":
      return { kind: "TypedStringExpression", value: x, type: { kind: "StringType" } };
  }
}

export interface AstStep {
  data: TopLevelExpression[];
  insert: (e: TopLevelExpression, idx: number) => void;
  next: () => DesugaredAstStep;
  finish: () => Promise<void>;
  visit: (visitor: SourceVisitor<Expression, void>) => void;
  transform: (visitor: SourceVisitor<Expression, Expression>) => TopLevelExpression[];
}

export interface DesugaredAstStep {
  data: TopLevelExpression[];
  insert: (e: TopLevelExpression, idx: number) => void;
  next: () => TypedAstStep;
  finish: () => Promise<void>;
  visit: (visitor: SourceVisitor<Expression, void>) => void;
  transform: (visitor: SourceVisitor<Expression, Expression>) => TopLevelExpression[];
}

export interface TypedAstStep {
  data: TypedTopLevelExpression[];
  context: Context;
  scope: Scope;
  typeContext: Context;
  insert: (e: TypedTopLevelExpression, idx: number) => void;
  next: () => DesugaredTypedAstStep;
  finish: () => Promise<void>;
  visit: (visitor: TypedVisitor<TypedExpression, void>) => void;
  transform: (visitor: TypedVisitor<TypedExpression, TypedExpression>) => TypedTopLevelExpression[];
}

export interface DesugaredTypedAstStep {
  data: TypedTopLevelExpression[];
  insert: (e: TypedTopLevelExpression, idx: number) => void;
  next: (scope?: Scope) => ScalaStep;
  finish: () => Promise<void>;
  visit: (visitor: TypedVisitor<TypedExpression, void>) => void;
  transform: (visitor: TypedVisitor<TypedExpression, TypedExpression>) => TypedTopLevelExpression[];
}

export interface ScalaStep {
  data: string;
  write: () => Promise<void>;
  finish: () => Promise<void>;
}

export interface CompilerStepMap {
  "ast": AstStep;
  "desugared-ast": DesugaredAstStep;
  "typed-ast": TypedAstStep;
  "desugared-typed-ast": DesugaredTypedAstStep;
  "scala": ScalaStep;
}

export type CompilerStep = keyof CompilerStepMap;

export class CompilerApi {

  constructor(private project: ReltProject) { }


  private makeSourceVisit(ast: TopLevelExpression[]) {
    return (visitor: SourceVisitor<Expression, void>) => {
      ast.forEach(x => {
        switch (x.kind) {
          case "SugarDefinition":
          case "TypeIntroductionExpression":
          case "SugarDirective":
            return;
          default:
            visitVoidSource(x, visitor);
        }
      });
    }
  }

  private makeSourceTransform(ast: TopLevelExpression[]) {
    return (visitor: SourceVisitor<Expression, Expression>) => {
      return ast = ast.map<TopLevelExpression>(x => {
        switch (x.kind) {
          case "SugarDefinition":
          case "TypeIntroductionExpression":
          case "SugarDirective":
            return x;
          default:
            return visitMapSource(x, visitor);
        }
      });
    };
  }

  private makeTypedVisit(tast: TypedTopLevelExpression[]) {
    return (visitor: TypedVisitor<TypedExpression, void>) => {
      tast.forEach(x => {
        switch (x.kind) {
          case "TypedSugarDefinition":
          case "TypeIntroductionExpression":
          case "SugarDirective":
            return;
          default:
            visitVoidTyped(x, visitor);
        }
      });
    }
  }

  private makeTypedTransform(tast: TypedTopLevelExpression[]) {
    return (visitor: TypedVisitor<TypedExpression, TypedExpression>) => {
      return tast = tast.map<TypedTopLevelExpression>(x => {
        switch (x.kind) {
          case "TypedSugarDefinition":
          case "TypeIntroductionExpression":
          case "SugarDirective":
            return x;
          default:
            return visitMapTyped(x, visitor);
        }
      });
    };
  }

  private toAst(fileName: string): AstStep {
    const fileContent = readFileSync(fileName).toString();
    const fileContentWithoutComments = fileContent.split('\n').map(x => {
      const idx = x.indexOf("#");
      return idx === -1 ? x : x.slice(0, idx);
    }).join('\n');
    const ast = parse(fileContentWithoutComments);
    const sugars = ast.filter(x => x.kind === "SugarDefinition" || x.kind === "SugarDirective") as (SugarDefinition | SugarDirective)[];
    checkSugars(sugars);
    return {
      data: ast,
      insert: (e: TopLevelExpression, idx: number) => { ast.splice(idx, 0, e); },
      next: () => this.toDesugaredAst(ast),
      finish() { return this.next().finish() },
      visit: this.makeSourceVisit(ast),
      transform: this.makeSourceTransform(ast),
    };
  }

  private toDesugaredAst(ast: TopLevelExpression[]): DesugaredAstStep {
    const dast = preTypeCheckDesugar(ast);
    return {
      data: dast,
      insert: (e: TopLevelExpression, idx: number) => { ast.splice(idx, 0, e); },
      next: () => this.toTypedAst(ast),
      finish() { return this.next().finish() },
      visit: this.makeSourceVisit(ast),
      transform: this.makeSourceTransform(ast),
    };
  }

  private toTypedAst(ast: TopLevelExpression[]): TypedAstStep {
    let ctx: Context = {
      '+': { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "IntegerType" } } },
      '-': { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "IntegerType" } } },
      '*': { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "IntegerType" } } },
      '/': { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "IntegerType" } } },
      '%': { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "IntegerType" } } },
      '==': { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "BooleanType" } } },
      '!=': { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "BooleanType" } } },
      '<=': { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "BooleanType" } } },
      '>=': { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "BooleanType" } } },
      '<': { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "BooleanType" } } },
      '>': { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "BooleanType" } } },
      '||': { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "BooleanType" } } },
      '&&': { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "BooleanType" } } },
    };

    let scope: Scope = {
      relt: {
        kind: "TypedObjectExpression",
        properties: [],
        type: { kind: "ObjectType", properties: [] },
      }
    };
    let tCtx: Context = {};
    const [tast, ctx1, scope1] = typeCheck(ast, ctx, scope, tCtx);
    return {
      data: tast,
      context: ctx1,
      scope: scope1,
      typeContext: tCtx,
      insert: (e: TypedTopLevelExpression, idx: number) => { tast.splice(idx, 0, e); },
      next: () => this.toDesugaredTypedAst(tast, scope1),
      finish() { return this.next().finish() },
      visit: this.makeTypedVisit(tast),
      transform: this.makeTypedTransform(tast),
    };
  }

  private toDesugaredTypedAst(tast: TypedTopLevelExpression[], scope_: Scope): DesugaredTypedAstStep {
    const dtast = desugar(tast);
    return {
      data: dtast,
      insert: (e: TypedTopLevelExpression, idx: number) => { dtast.splice(idx, 0, e); },
      next: (scope?: Scope) => this.toScala(dtast, { ...scope_, ...scope }),
      finish() { return this.next().finish() },
      visit: this.makeTypedVisit(dtast),
      transform: this.makeTypedTransform(dtast),
    };
  }

  private toScala(tast: TypedTopLevelExpression[], scope: Scope): ScalaStep {
    const scalaSourceCode = toScala(tast, scope);

    const sparkProject: SparkProject = {
      kind: "SparkProject",
      imports: [],
      name: "Test",
      package: "",
      sourceCode: scalaSourceCode,
      types: [],
    };

    return {
      data: scalaSourceCode,
      write: () => writeSparkProject(this.project, sparkProject),
      finish() { return this.write(); },
    };
  }

  async compile<Step extends CompilerStep | undefined = undefined>(to?: Step): Promise<Step extends CompilerStep ? CompilerStepMap[Step] : undefined> {
    const fileName = this.project.mainFile;

    const step0 = this.toAst(fileName);
    if (to === "ast") return step0 as Step extends CompilerStep ? CompilerStepMap[Step] : undefined;
    const step1 = step0.next();
    if (to === "desugared-ast") return step1 as Step extends CompilerStep ? CompilerStepMap[Step] : undefined;
    const step2 = step1.next();
    if (to === "typed-ast") return step2 as Step extends CompilerStep ? CompilerStepMap[Step] : undefined;
    const step3 = step2.next();
    if (to === "desugared-typed-ast") return step3 as Step extends CompilerStep ? CompilerStepMap[Step] : undefined;
    const step4 = step3.next();
    if (to === "scala") return step4 as Step extends CompilerStep ? CompilerStepMap[Step] : undefined;

    await step4.write();

    return undefined as Step extends CompilerStep ? CompilerStepMap[Step] : undefined;
  }
}
