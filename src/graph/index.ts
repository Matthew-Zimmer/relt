import { FlatTypeExpression, FlatTypeIntroExpression } from "../asts/typeExpression/flat";
import { throws, dedup } from "../utils";

interface Vertex {
  id: number;
  value: string;
}

interface Edge {
  from: number;
  to: number;
}

export class DependencyGraph {
  private childrenEdges = new Map<number, number[]>();
  private parentEdges = new Map<number, number[]>();
  private vertexLookup = new Map<string, number>();

  constructor(public vertices: Vertex[], edges: Edge[]) {
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

  private valueOf(id: number): string {
    return this.vertices[id].value;
  }

  private valuesOf(ids: number[]): string[] {
    return ids.map(this.valueOf.bind(this));
  }

  isCyclic(): boolean {
    return false;
  }

  topologicalSort(): string[] {
    return [];
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

function typeDependenciesOf(type: FlatTypeExpression): string[] {
  switch (type.kind) {
    case 'FlatBooleanTypeExpression':
    case 'FlatFloatTypeExpression':
    case 'FlatIntegerTypeExpression':
    case 'FlatStringTypeExpression':
    case 'FlatPrimaryKeyTypeExpression':
    case 'FlatForeignKeyTypeExpression': // maybe questionable? I don't think so since its not a data dep its just a relational concept 
      return [];
    case 'FlatIdentifierTypeExpression':
      return [type.name];
    case 'FlatObjectTypeExpression':
      return type.properties.flatMap(x => typeDependenciesOf(x.value));
    case 'FlatDropTypeExpression':
    case 'FlatWithTypeExpression':
    case "FlatGroupByTypeExpression":
      return typeDependenciesOf(type.left);
    case 'FlatJoinTypeExpression':
    case 'FlatUnionTypeExpression':
      return [typeDependenciesOf(type.left), typeDependenciesOf(type.right)].flat();
    case 'FlatTypeIntroExpression':
      return [type.name, typeDependenciesOf(type.value)].flat();
    case 'FlatArrayTypeExpression':
      return typeDependenciesOf(type.of);
  }
}

export function namedTypeDependencyGraph(types: FlatTypeIntroExpression[]): DependencyGraph {
  const graph: Record<string, string[]> = {};

  for (const { name, value } of types) {
    const deps = dedup(typeDependenciesOf(value));
    if (name in graph)
      throws(`ReEntry of ${name} into dep graph this might be ok. need to think about it`);
    graph[name] = deps;
  }

  const idMap = new Map(Object.keys(graph).map((k, i) => [k, i]));

  const vertices: Vertex[] = [...idMap.entries()].map(([value, id]) => ({ id, value }));
  const edges: Edge[] = [...idMap.entries()].flatMap(([value, id]) => graph[value].map(x => ({ from: idMap.get(x)!, to: id })));

  return new DependencyGraph(vertices, edges);
}
