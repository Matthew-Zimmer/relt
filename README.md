# RELT

Reactive Extract Transform Load

Language for defining spark etls

## Installation

```sh
git clone https://github.com/Matthew-Zimmer/relt.git
cd relt
npm ci
tsc -p .
```
### Link executable to PATH directory

```sh
ln relt /some/directory/in/path/relt
```

### Add RELT_HOME to Shell file

(for bash)
```sh
echo "export RELT_HOME='$(pwd)'" >> ~/.bashrc
```

## Usage

```sh
relt --version # shows the version of relt
relt --help # shows the help screen
relt init # Creates a new relt project
relt compile # Compiles the current relt project to the scala project project
```

## Configuring a relt project

### name

The name of the relt project

- Required

### package

The package prefix of the scala project

- Default ("")

### srcDir

The directory which contains the main relt file

- Default ("src")

### outDir

The directory which contains the generated scala project(s)

- Default ("out")

### mainFile

The name of the main relt file which the compiler starts with

- Default ("main.relt")

## Language

TODO
