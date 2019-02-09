# ETL framework for Clojure

A Clojure ETL based on Singer open source standard [docs](https://github.com/singer-io/getting-started)

> Singer is an open source standard for moving data between databases, web APIs, files, queues, and just about anything else you can think of. The Singer spec describes how data extraction scripts — called “Taps” — and data loading scripts — called “Targets” — should communicate using a standard JSON-based data format over stdout. By conforming to this spec, Taps and Targets can be used in any combination to move data from any source to any destination.

## Goals

- [ ] support Singer specification
- [x] written in Clojure :)
- [x] single executable

## TODO

- [x] CLI executable
- [ ] Docker image
- [ ] Travis
- [ ] [Singer Specification](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md#singer-specification)
  - [x] [Synopsis](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md#synopsis)
  - [x] [Input](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md#input)
    - [x] Config
    - [x] State
    - [x] Catalog
  - [ ] [Output](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md#output)
    - [x] RECORD Message
    - [ ] SCHEMA Message
    - [x] STATE Message
- [x] CSV tap
- [ ] CSV target
- [ ] Google PubSub tap
- [ ] Google PubSub target

## Usage

### Build

``` shell
lein test && lein bin
```

### Run

Example CSV tap

``` shell
# Discovery - will create catalog.json
target/etl --config resources/tap-csv-config.json discover --type csv > catalog.json
# Run tap - will send data to *out*
target/etl --config resources/tap-csv-config.json tap --type csv
```


## License

Copyright © 2019 NativeKloud

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
