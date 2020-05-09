# `knackpostgres`

### *** This Library is Under Construction ****

Convert Knack applications to a PostgreSQL database.

## Installation

*If you want to use the `Loader` class to load data to a Postgres database, you'll need to install [`psycopg2`](https://pypi.org/project/psycopg2/) manually. Because of installation headaches, it is not installed automatically.*

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

`knackpostgres` will generate a series of Postgres-compliant SQL commands which translate your Knack app's schema to a PostgreSQL schema. 

```python
>>> from knackpostgres import App

# find your app id: https://www.knack.com/developer-documentation/#find-your-api-key-amp-application-id
>>> app = App("myappidstring")
```

If you want to execute the SQL commandsd manually, you can write the App's SQL commands to files:

```python
>>> app.to_sql(path="mypath") # writes statements to mypath/sql directory
```

Alternatively, you can use the `Loader` class to execute your app's SQL. Read on...

### Quick Create PostgreSQL databse

If you're in need of a postgresql database for development. Consider the [official docker images](https://hub.docker.com/_/postgres).

To start your database with the default username (`postgres`), start the database in a new container:

```bash
$ docker run -p 5432:5432 --name postgres -e POSTGRES_PASSWORD=my_password -d my-db-name
```

If you want to explore the database with `psql`, open a separate terminal window and run

```bash
docker run -it --rm --network host my-db-name psql -h localhost -U postgres
```

### Deploy Knack Database Schema to PostgreSQL

* Install [`psycopg2`](https://pypi.org/project/psycopg2/) in your Python environment.

Pass your `App` to a new `Loader` instance.

```python
>>> from knackpostgres import App, Loader

>>> app = App("myappidstring")

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

Execute your `App`'s sql commands:

```python
>>> loader.create_tables()

# read-only fields (formule & equations) are only available in views
>>> loader.create_tables()

```

### Knack Feature Coverage

This is a work in progress. Currently supported Knack features include:

All **connection field** types are supported, although self-connections are not well tested.

**Address** and **Name** fields are supported and stored as `JSON` types.

Standard **[formula fields](https://support.knack.com/hc/en-us/articles/226583008-Formulas)** are supported

**Equation** fields are not yet supported.

For **Concatentation (aka Text Formula)** fields, all **[Text Functions](https://support.knack.com/hc/en-us/articles/115005002328-Text-Formula-Functions)** are supported.

The only supported **text formula** date functions are `getDateDayOfWeekName` and `getDateMonthOfYearName`. You can add support for others by writing your own [`MethodHandler`](https://github.com/cityofaustin/knackpostgres/blob/master/knackpostgres/method_handler.py) method.