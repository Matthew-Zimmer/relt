import { TypedTypeExpression } from "../asts/typeExpression/typed";
import { TypeExpression, TypeIntroExpression } from "../asts/typeExpression/untyped";
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

// function typeDependenciesOf(type: TypeExpression): string[] {
//   switch (type.kind) {
//     case 'BooleanTypeExpression':
//     case 'FloatTypeExpression':
//     case 'IntegerTypeExpression':
//     case 'StringTypeExpression':
//     case 'PrimaryKeyTypeExpression':
//     case 'ForeignKeyTypeExpression': // maybe questionable? I don't think so since its not a data dep its just a relational concept 
//       return [];
//     case 'IdentifierTypeExpression':
//       return [type.name];
//     case 'ObjectTypeExpression':
//       return type.properties.flatMap(x => typeDependenciesOf(x.value));
//     case 'DropTypeExpression':
//     case "GroupByTypeExpression":
//       return typeDependenciesOf(type.left);
//     case 'WithTypeExpression':
//       return [
//         ...typeDependenciesOf(type.left),
//         ...type.rules.flatMap(x => x.kind === 'RuleTypeProperty' ? typeDependenciesOf(x.value) : []),
//       ];
//     case 'JoinTypeExpression':
//     case 'UnionTypeExpression':
//       return [typeDependenciesOf(type.left), typeDependenciesOf(type.right)].flat();
//     case 'TypeIntroExpression':
//       return [type.name, typeDependenciesOf(type.value)].flat();
//     case 'ArrayTypeExpression':
//       return typeDependenciesOf(type.of);
//   }
// }

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
      case "TypedJoinTypeExpression": {
        walk(e.left);
        walk(e.right);
        graph[e.type.name] = [e.left.type.name, e.right.type.name];
        return true;
      }
      case "TypedDropTypeExpression": {
        walk(e.left);
        graph[e.type.name] = [e.left.type.name];
        return true;
      }
      case "TypedWithTypeExpression": {
        walk(e.left);
        graph[e.type.name] = [e.left.type.name];
        return true;
      }
      case "TypedUnionTypeExpression": {
        walk(e.left);
        walk(e.right);
        graph[e.type.name] = [e.left.type.name, e.right.type.name];
        return true;
      }
      case "TypedGroupByTypeExpression": {
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
