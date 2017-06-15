# Webhook Logger

This simple server accepts incoming JSON objects over HTTP, stores them in a
database, and makes them queryable by index via HTTP.

## Build

    go build

## Run

    ./webhook-logger

## Send JSON objects

    curl -v -XPOST -d '{"foo": "bar"}' http://localhost:9099/append

## Retrieve objects

Retrieve all objects:

    curl http://localhost:9099/get

Retrieve all objects with entry index `>= 3` for generation ID `3f8e1781-b755-4f6a-8855-94eb20b00dc6`:

    curl 'http://localhost:9099/get?generationID=3f8e1781-b755-4f6a-8855-94eb20b00dc6&fromIndex=3'

The `generationID` query parameter is expected to match the `generationID`
value returned from any `/get` requests. If it does not match, all entries are
returned instead of just the ones starting from `fromIndex`. The generation ID
is created when the tool's database is first initialized.
