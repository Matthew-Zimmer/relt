
export type Value =
  | number
  | string
  | boolean
  | ValueObject
  | Value[]
  | undefined
  | ((...args: Value[]) => Value)

export interface ValueObject {
  [x: string]: Value
}
