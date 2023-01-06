import { TypedTypeExpression } from "../asts/typeExpression/typed";
import { throws } from "../utils";

interface Vertex {
  id: number;
  value: string;
}

interface Edge {
  from: number;
  to: number;
}

export class DependencyGraph {
  childrenEdges = new Map<number, number[]>();
  private parentEdges = new Map<number, number[]>();
  private vertexLookup = new Map<string, number>();
  vertices = new Map<number, Vertex>();

  constructor(vertices: Vertex[], private edges: Edge[]) {
    edges.forEach(e => {
      const val = this.childrenEdges.get(e.from) ?? [];
      this.childrenEdges.set(e.from, [...val, e.to]);
    });
    edges.forEach(e => {
      const val = this.parentEdges.get(e.to) ?? [];
      this.parentEdges.set(e.to, [...val, e.from]);
    });
    vertices.forEach(v => {
      this.vertexLookup.set(v.value, v.id);
    });
    this.vertices = new Map(vertices.map(v => [v.id, v]));
  }

  private vertex(v: string): number {
    return this.vertexLookup.get(v) ?? throws(`Vertex ${v} is not defined`);
  }

  parents(id: number): number[] {
    return this.parentEdges.get(id) ?? [];
  }

  children(id: number): number[] {
    return this.childrenEdges.get(id) ?? [];
  }

  remove(name: string): DependencyGraph {
    const id = this.vertex(name);
    const parents = this.parents(id);
    const children = this.children(id);

    const vertices = [...this.vertices.values()].filter(v => v.id !== id);

    const edges = [
      ...this.edges.filter(x => x.from !== id && x.to !== id),
      ...parents.flatMap(x => children.map(y => ({ from: x, to: y }))),
    ];

    return new DependencyGraph(vertices, edges);
  }

  private valueOf(id: number): string {
    return this.vertices.get(id)!.value;
  }

  private valuesOf(ids: number[]): string[] {
    return ids.map(x => this.valueOf(x));
  }

  isCyclic(): boolean {
    return false;
  }

  topologicalSort(): string[] {
    const visited = new Map<number, boolean>([...this.vertices.keys()].map(k => [k, false]));
    const stack: number[] = [];

    const imp = (id: number) => {
      visited.set(id, true);

      for (const i of this.children(id))
        if (!visited.get(i))
          imp(i);

      stack.push(id);
    }

    for (const id of this.vertices.keys())
      if (!visited.get(id))
        imp(id);

    return this.valuesOf(stack);
  }

  parentsOf(v: string): string[] {
    return this.valuesOf(this.parents(this.vertex(v)));
  }

  ancestorsOf(v: string): string[] {
    const seen = new Set<number>();
    const imp = (id: number): number[] => {
      if (seen.has(id)) return [];
      const parents = this.parents(id);
      seen.add(id);
      return [id, ...parents.flatMap(imp)];
    }
    return this.valuesOf(imp(this.vertex(v)));
  }

  childrenOf(v: string): string[] {
    return this.valuesOf(this.children(this.vertex(v)));
  }

  descendantsOf(v: string): string[] {
    const seen = new Set<number>();
    const imp = (id: number): number[] => {
      if (seen.has(id)) return [];
      const children = this.children(id);
      seen.add(id);
      return [id, ...children.flatMap(imp)];
    }
    return this.valuesOf(imp(this.vertex(v)));
  }
}

/**
 * Assuming typeExpressions have be desugared
 * TODO need a type for desugared type expressions
 * @param typeExpressions 
 * @returns 
 */
export function namedTypeDependencyGraph(typeExpressions: TypedTypeExpression[]): DependencyGraph {
  const graph: Record<string, string[]> = {};

  const walk = (e: TypedTypeExpression): boolean => {
    switch (e.kind) {
      case "TypedIntegerTypeExpression":
      case "TypedFloatTypeExpression":
      case "TypedBooleanTypeExpression":
      case "TypedStringTypeExpression":
      case "TypedForeignKeyTypeExpression":
      case "TypedPrimaryKeyTypeExpression":
      case "TypedIdentifierTypeExpression":
        return false;
      case "TypedObjectTypeExpression":
        e.properties.forEach(x => walk(x.value));
        return true;
      case "TypedArrayTypeExpression":
        walk(e.of);
        return true;

      case "TypedTypeIntroExpression": {
        walk(e.value);
        if (e.type.kind === 'StructType' && !(e.type.name in graph))
          graph[e.type.name] = [];
        return true;
      }
      case "TypedUnionTypeExpression":
      case "TypedJoinTypeExpression": {
        walk(e.left);
        walk(e.right);
        graph[e.type.name] = [e.left.type.name, e.right.type.name];
        return true;
      }
      case "TypedDropTypeExpression":
      case "TypedWithTypeExpression":
      case "TypedGroupByTypeExpression":
      case "TypedSortTypeExpression":
      case "TypedDistinctTypeExpression":
      case "TypedWhereTypeExpression": {
        walk(e.left);
        graph[e.type.name] = [e.left.type.name];
        return true;
      }
    }
  }

  typeExpressions.forEach(walk);

  const idMap = new Map(Object.keys(graph).map((k, i) => [k, i]));

  const vertices: Vertex[] = [...idMap.entries()].map(([value, id]) => ({ id, value }));
  const edges: Edge[] = [...idMap.entries()].flatMap(([value, id]) => graph[value].map(x => ({ from: idMap.get(x)!, to: id })));

  return new DependencyGraph(vertices, edges);
}
