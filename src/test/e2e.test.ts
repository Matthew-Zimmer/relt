import { compile } from "..";

it('should parse and type check integers', async () => {
  const source = `1`;
  const result = compile(source);
  expect(result).toMatchSnapshot();
});

it('should parse and type check floats', async () => {
  const source = `1.02`;
  const result = compile(source);
  expect(result).toMatchSnapshot();
});

it('should parse and type check booleans', async () => {
  const source = `true`;
  const result = compile(source);
  expect(result).toMatchSnapshot();
});

it('should parse and type check booleans', async () => {
  const source = `false`;
  const result = compile(source);
  expect(result).toMatchSnapshot();
});

it('should parse and type check empty strings', async () => {
  const source = `""`;
  const result = compile(source);
  expect(result).toMatchSnapshot();
});

it('should parse and type check non empty strings', async () => {
  const source = `"s"`;
  const result = compile(source);
  expect(result).toMatchSnapshot();
});

it('should parse and type check non empty strings', async () => {
  const source = `"sasdkljaskl;jdsa;lmd;aklsdjnmklsadnmkals;'dnas;kldns;akond;"`;
  const result = compile(source);
  expect(result).toMatchSnapshot();
});

it('should parse and type check null', async () => {
  const source = `null`;
  const result = compile(source);
  expect(result).toMatchSnapshot();
});

it('should parse and type check env string', async () => {
  const source = `$"ENV_VAR"`;
  const result = compile(source);
  expect(result).toMatchSnapshot();
});

it('should parse and type check tuples (empty)', async () => {
  const source = `[]`;
  const result = compile(source);
  expect(result).toMatchSnapshot();
});

it('should parse and type check tuples (singular)', async () => {
  const source = `[0]`;
  const result = compile(source);
  expect(result).toMatchSnapshot();
});

it('should parse and type check tuples (multi)', async () => {
  const source = `[0, 0.4]`;
  const result = compile(source);
  expect(result).toMatchSnapshot();
});

it('should parse and type check placeholders', async () => {
  const source = `$A`;
  const result = compile(source);
  expect(result).toMatchSnapshot();
});

it('should parse and type check placeholders (kind)', async () => {
  const source = `$A~@`;
  const result = compile(source);
  expect(result).toMatchSnapshot();
});

it('should parse and type check placeholders (type)', async () => {
  const source = `$A:table`;
  const result = compile(source);
  expect(result).toMatchSnapshot();
});

it('should parse and type check placeholders (spread)', async () => {
  const source = `$A~@...rl`;
  const result = compile(source);
  expect(result).toMatchSnapshot();
});

it('should parse and type check placeholders (spread + override)', async () => {
  const source = `$A~@...rl0=[x,9]`;
  const result = compile(source);
  expect(result).toMatchSnapshot();
});

it('should parse and type check do end (empty)', async () => {
  const source = `do end`;
  const result = compile(source);
  expect(result).toMatchSnapshot();
});

it('should parse and type check do end (singular)', async () => {
  const source = `do 1; end`;
  const result = compile(source);
  expect(result).toMatchSnapshot();
});

it('should parse and type check do end (multi)', async () => {
  const source = `do 1; "aslkdjhsla"; end`;
  const result = compile(source);
  expect(result).toMatchSnapshot();
});

it('should parse and type check identifier (unknown)', async () => {
  const source = `x`;
  const result = compile(source);
  expect(result).toMatchSnapshot();
});

it('should parse and type check identifier (known)', async () => {
  const source = `let x = 0\nx`;
  const result = compile(source);
  expect(result).toMatchSnapshot();
});

it('should parse and type check function (0 args)', async () => {
  const source = `func f => 1`;
  const result = compile(source);
  expect(result).toMatchSnapshot();
});

it('should parse and type check function (1 args)', async () => {
  const source = `func f(x: string) => x`;
  const result = compile(source);
  expect(result).toMatchSnapshot();
});

it('should parse and type check function (2 args)', async () => {
  const source = `func f(x: string)(y: string) => x`;
  const result = compile(source);
  expect(result).toMatchSnapshot();
});

it('should parse and type check table', async () => {
  const source = `table t = { x: int }`;
  const result = compile(source);
  expect(result).toMatchSnapshot();
});

