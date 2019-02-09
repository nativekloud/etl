# ETL framework for Clojure

A Clojure ETL based on Singer open source standard [docs](https://github.com/singer-io/getting-started)

> Singer is an open source standard for moving data between databases, web APIs, files, queues, and just about anything else you can think of. The Singer spec describes how data extraction scripts — called “Taps” — and data loading scripts — called “Targets” — should communicate using a standard JSON-based data format over stdout. By conforming to this spec, Taps and Targets can be used in any combination to move data from any source to any destination.

## Goals

- support Singer standard
- written in Clojure :)
- single executable

## TODO

- [x] CLI executable
- [ ] Docker image
- [ ] Travis
- [ ] Singer standard implementation
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
target/etl-0.1.0-SNAPSHOT --config resources/tap-csv-config.json discover --type csv
# Run tap - will send data to *out*
target/etl-0.1.0-SNAPSHOT --config resources/tap-csv-config.json tap --type csv
```


## License

Copyright © 2019

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
