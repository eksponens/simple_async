from flask import Flask
from flask import jsonify
from flask import request
from flask import abort
from sqlalchemy import text
from sqlalchemy import select
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.ext.asyncio import create_async_engine
import asyncpg
import asyncio
import platform


# Creating and configuring Flask application
app = Flask(__name__)

app.config['DATABASE_URI'] = 'postgresql+asyncpg://postgres:Pa$$w0rd@localhost:5432/postgres'

# List of IDs if JSON in get_data is empty (see line # 100)
id_list = [*range(1, 61)]


################################################################

# Database configuration
metadata = MetaData()

datasource1 = Table('datasource1', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('text', String))

datasource2 = Table('datasource2', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('text', String))

datasource3 = Table('datasource3', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('text', String))

tables = [datasource1, datasource2, datasource3]


################################################################

# Functions for accessing data

# Executes a raw SQL query to retrieve all data from all tables in a specific order
async def get_all_data_raw():
    engine = create_async_engine(app.config['DATABASE_URI'])
    async with engine.connect() as conn:
        result = await conn.execute(text('''SELECT * FROM (
                (SELECT * FROM public."datasource1") UNION (SELECT * FROM public."datasource2") 
                UNION (SELECT * FROM public."datasource3")) AS x ORDER BY id'''))

        return result


# Executes a query using SQLAlchemy's ORM to retrieve data from a
# specific table based on a list of IDs
async def get_data_orm(conn, table, ids):
    try:
        query = select(table).where(table.c.id.in_(ids))
        result = await conn.execute(query)

        return result

    except (asyncpg.PostgresError, asyncio.TimeoutError):
        return []


################################################################

# Error message function
def err(code=500, message=None):
    abort(code, {'message': message} if message else None)


################################################################

# Route handlers

# Returns a JSON response with a greeting and status message
@app.route('/greeting', methods=['GET'])
def hello():
    return jsonify({'title': 'Welcome aboard, captain! All systems online! To init the database go to /init'})


# Retrieves all data from the database and returns it in a JSON response
@app.route('/', methods=['GET'])
async def get_all_data():
    try:
        result = await asyncio.wait_for(get_all_data_raw(), timeout=2)
        # sorted_data = sorted([*result], key=lambda x: x.id)

    except asyncio.TimeoutError:
        result = []

    # response = [{'id': row.id, 'text': row.text} for row in result]
    if result:
        data = dict((k, v) for k, v in result)

        response = [{'id': k, 'text': v} for k, v in data.items()]

        return jsonify(response)

    else:
        return result


# Retrieves data from the database based on the provided IDs
# (or a default list if none is provided) and returns it in a JSON response
@app.route('/', methods=['POST'])
async def get_data():
    data = request.get_json(force=True, silent=True)

    if data and data['ids']:
        ids = data['ids']
    else:
        ids = id_list

    engine = create_async_engine(app.config['DATABASE_URI'])
    async with engine.connect() as conn:
        coroutines = []

        # Create coroutines for executing queries asynchronously
        for table in tables:
            coroutine = get_data_orm(conn, table, ids)
            coroutines.append(coroutine)

        # Execute coroutines
        results = await asyncio.gather(*coroutines)

    data = []
    for result in results:
        data.extend(result)

    data = dict(sorted((k, v) for k, v in data))

    response = [{'id': k, 'text': v} for k, v in data.items()]

    return jsonify(response)


# Initializes the database by creating the tables and inserting test data.
# To initialize send JSON {'init': 1}
@app.route('/init', methods=['POST'])
async def init():
    data = request.get_json(force=True, silent=True)

    if data and 'init' in data and data['init'] == 1:

        engine = create_async_engine(app.config['DATABASE_URI'])
        async with engine.begin() as conn:
            await conn.run_sync(metadata.create_all)

            for i in [*range(1, 11), *range(31, 41)]:
                query = datasource1.insert().values(id=i, text=f"Test {i}")
                await conn.execute(query)

            for i in [*range(11, 21), *range(41, 51)]:
                query = datasource2.insert().values(id=i, text=f"Test {i}")
                await conn.execute(query)

            for i in [*range(21, 31), *range(51, 61)]:
                query = datasource3.insert().values(id=i, text=f"Test {i}")
                await conn.execute(query)

        return 'The database was initialized', 200

    else:
        err(400, 'Invalid request: init not 1')


# Handler for the favicon request (returns an empty response with status code 204)
@app.route('/favicon.ico')
def favicon():
    return '', 204


################################################################

# Starting the application
if __name__ == '__main__':

    # Fixing windows problem "loop is closed"
    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # Creating loop and starting app
    loop = asyncio.get_event_loop()

    loop.run_until_complete(app.run(debug=True))

    # Gently put asyncio in sleep
    loop.run_until_complete(asyncio.sleep(0))

    loop.close()
