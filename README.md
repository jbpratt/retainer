# Elasticsearch Retainer

*WARNING*: Currently under development, please do not risk logs running this.

## Installation
```
$ go get -u github.com/jbpratt78/retainer
```

## Usage
```
$ retainer  -days=10 -index="nginx-logs"
```

This will allow you to easily retain a range of dates for an index that is time based. While it is perfectly possible to store documents of a type in one large index, we would run out of space. With indices based on time, it becomes much more performant to retain, roll over, and delete of indices.

# License

This software is released under the MIT License, see LICENSE.
