# `knackpostgres`

### *** This Library is Under Construction ****

Convert Knack applications to a PostgreSQL database.

## Installation

*If you want to use the `Loader` class to load data to a Postgres database, you'll need to install [`psycopg2`](https://pypi.org/project/psycopg2/) manually. Because of installation headaches, it is not automatically.*

1. Clone this repo

```bash
$ git clone http://github.com/cityofaustin/knack-to-postgres
```

2. Install the library

```bash
$ pip install knackpostgres
```

## Usage

*If you're new to Knack + Python, consider learning via [Knackpy](https://github.com/cityofaustin/knackpy).*

### Convert your App to PostgreSQL

`knackpostgres` will generate a series of Postgres-compliant SQL commands which can be executued to create your database.

```python
>>> from knackpostgres import App

>>> app = App("myappidstring")

>>> app.to_sql() # writes statements to /sql directory
```

The SQL commands are co-dependent and must be executed in the proper order. TODO: make this easy and document it.

### Quick Create PostgreSQL databse

If you're in need of a postgresql database for development. Consider the [official docker images](https://hub.docker.com/_/postgres).

To start your database with the default username (`postgres`), start the database in a new container:

```bash
$ docker run -p 5432:5432 --name my-db-name -e POSTGRES_PASSWORD=my_password -d postgres
```

If you want to explore the database with `psql`, open a separate terminal window and run

```bash
docker run -it --rm --network host my-db-name psql -h localhost -U postgres
```

### Implement Knack Database Schema in PostgreSQL

* Install [`psycopg2`](https://pypi.org/project/psycopg2/) in your Python environment.

Pass your `App` to a new `Loader` instance.

```python
>>> from knackpostgres import Loader

# ! This will overwrite the destination DB schema !
>>> loader = Loader(app, overwrite=True)
```

Connect to your database:

```python
>>> loader.connect(
    host="localhost",
    dbname="my-db-name",
    user="postgres",
    password="myunguessabledatabasepassword" )
```

Execute your `App`'s' sql commands:

```python
>>> loader.create_tables()

>>> loader.create_relationships()
```