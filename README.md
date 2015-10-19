# anon-valid

Anonimization validator is a program to check if database contains sensitive data. It is designed to easily scan and analize large databases. 

anon-valid scans the database via a JDBC connection. Currently it supports oracle, mssql and mysql databases.

## Installation

Download from https://github.com/jozseftiborcz/anonimization-validator

## Usage

Program can be started with the following options. It requires java 1.6 or greater (as Clojure). 

    $ java -jar anon-valid-0.1.0-standalone.jar [args]

## Options

FIXME: listing of options this app accepts.

## Features
* Detection of sensitive field candiates in Hungarian and English based on field names.
* Sensitive field discovery based on column content.

## License

Copyright © 2015 József Tiborcz 

Distributed under the Eclipse Public License, the same as Clojure uses. See the file LICENSE.
