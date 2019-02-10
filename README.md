# ETL framework for Clojure [![Build Status](https://travis-ci.org/nativekloud/etl.svg?branch=master)](https://travis-ci.org/nativekloud/etl)

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
  - [ ] Metrics
- [x] CSV tap
- [ ] CSV target
- [ ] Google PubSub tap
- [ ] Google PubSub target

## Usage

### Synopsis

Since we have single executable for tap and target synopsis is little different.

``` shell
etl --config CONFIG [--state STATE] [--catalog CATALOG] tap|target|discovery --type TYPE

CONFIG is a required argument that points to a JSON file containing any
configuration parameters the Tap|Target needs.

STATE is an optional argument pointing to a JSON file that the
Tap|Target can use to remember information from the previous invocation,
like, for example, the point where it left off.

CATALOG is an optional argument pointing to a JSON file that the
Tap|Target can use to filter which streams should be synced.

tap|target|discovey is required argument to choose if we running tap, target or discovery

TYPE is required argument to specify type of tap or target ie. csv

```

### Build

``` shell
lein test && lein bin
```

### Run

Example CSV tap

``` shell
# Discovery - will create catalog.json
target/etl --config resources/tap-csv-config.json discover --type csv > catalog.json
# Run tap - will send data to *out* using discovered streams in catalog.json
target/etl --config resources/tap-csv-config.json tap --type csv
```


## License

Copyright © 2019 NativeKloud

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
