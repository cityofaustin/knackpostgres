# Knack-to-Postgres (`knackpostgres`)

Convert Knack applications to a PostgreSQL database.

## Installation

*If you want to use the `Loader` class to actually load data to a Postgres database, you'll need to install [`psycopg2`](https://pypi.org/project/psycopg2/) manually. Because of installation headaches, it is not installed automatically.*

1. Clone this repo

```bash
$ git clone http://github.com/cityofaustin/knack-to-postgres
```

2. Install the library
```bash
$ pip install knack-to-postgres
```

## Usage

*If you're new to Knack + Python, [Knackpy](https://github.com/cityofaustin/knackpy) is a good way to explore the API.*

### Convert your App to PostgreSQL

`knackpostgres` will generate a series of Postgres-compliant SQL commands which can be executued to create your database:

```python
>>> from knackpostgres import App

>>> app = App("myappidstring")

>>> app.to_sql() # writes statements to /sql directory
```

The SQL commands are co-dependent and must be executed in the proper order. TODO: make this easy and document it.