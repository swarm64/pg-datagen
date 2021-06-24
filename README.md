# PG Datagen - PostgreSQL Random Data Generator

A schema-driven Python random data generator for PostgreSQL.

[![Tests](https://github.com/swarm64/pg-datagen/actions/workflows/ci.yml/badge.svg)](https://github.com/swarm64/pg-datagen/actions/workflows/ci.yml)

| ⚠️  PG Datagen is currently very much alpha, any definition may change. Consider it having a non-stable interface. |
| --- |


ℹ️ Name suggestions welcome 😊


The purpose of this tool is to provide a random data generator for each table in
an (annotated) PostgreSQL schema. For example, when loading the schema:

```sql
CREATE TABLE a(
  id CHAR(32) PRIMARY KEY, -- gen: md5
  value TEXT
);
```

The tool would automatically generate (and ingest) values for the columns `id`
and `value` in table `a`. Dependencies between tables are supported as well (see
[this example](./examples/dependency)).


## Current Limitations

* Annotations cannot be mixed
* Each table has to be defined in a single file
* IDs based on `INT`/`BIGINT` can have collisions
* No automated tests


## Installation

1. Clone this repository
2. Install dependencies: `pip3 install -r requirements.txt`


## Example Usage

We'll be using [./examples/simple](./examples/simple) for
a demonstration:

1. Create a database and load `examples/simple/{a,b}.sql`
2. Run the generator:

    ```bash
    ./generator.py \
      --dsn postgresql://postgres@localhost/pydatagen \
      --batch-size 100 \
      --rows 1000 \
      --truncate \
      --target examples/simple/two-tables.py
    ```

## Details

### Python Control File

You will have to provide a Python file as entrypoint. The minimum definition
of such file for two tables `a` and `b` would look like this:

```python
from lib.base_object import Table

TABLES = {
  'public.a': Table(schema_path='...', scaler=1),
  'public.b': Table(schema_path='...', scaler=2)
}

GRAPH = {
  'public.a': [],
  'public.b': []
}

ENTRYPOINT = 'public.a'
```

Here, `a` and `b` are independent and `b` has twice as many rows as `a`. That
is, if you declare `--rows=100`, then 100 rows will be generated for `a` and
200 rows will be generated for `b`.

### Supported Annotations

Currently, there can be only one annotation per column. An annotation must be
placed on the same line as the column. Two examples below. This will work:

```sql
CREATE TABLE a(
    foo BIGINT -- none_prob: 0.9
  , bar BIGINT
)
```

This won't work:


```sql
CREATE TABLE a(
    foo BIGINT
    -- none_prob: 0.9
  , bar BIGINT
)
```

| Annotation | Description |
| ---------- | ----------- |
| none_prob: <0.0..1.0> | Sets probability of generating a `NULL` value (if allowed) |
| gen: <method name> | Hardcodes the generator to use. Methods in [./lib/random.py](./lib/random.py) are supported (not all!). For example: `-- gen: md5` would use the `md5` method. There is a special generator `choose_from_list` to inject dependencies (see below). |
    
### Inject Dependencies

If you have tables `a` and `b` and they are defined like:
    
```sql
-- a.sql
CREATE TABLE a(
    id CHAR(32) PRIMARY KEY -- gen: md5
  , value BIGINT
);
    
-- b.sql
CREATE TABLE b(
    id CHAR(32) PRIMARY KEY -- gen: md5
  , id_a CHAR(32) NOT NULL -- gen: choose_from_list public.a.id
  , value BIGINT
)
```

Then, the data generator will take random values from `a` and use them for
the `id_a` column of table `b`. Always supply a complete "path",
that is, the path must be of format: `<schema>.<table>.<column>`. Use `public`
as `<schema>` if you do not use custom schemas.

Modify the Python target file to ensure, that `a` is generated before `b`:

```python
GRAPH = {
  'public.a': ['public.b'],
  'public.b': []
}
```
