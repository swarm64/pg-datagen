# Python PostgreSQL Random Data Generator

This tool takes PostgreSQL schema definitions as input and produces random 
data according to it.

# Brief Usage

1. Install deps: `pip3 install -r requirements.txt`
2. Check out how `examples/simple/two-tables.py` is built
3. Create a database and load `examples/simple/{a,b}.sql`
4. Run the generator, e.g.

    ```bash
    ./generator.py \
      --dsn postgresql://postgres@localhost/pydatagen \
      --batch-size 100 \
      --total-rows 1000 \
      --truncate \
      --target examples/simple/two-tables.py
    ```
