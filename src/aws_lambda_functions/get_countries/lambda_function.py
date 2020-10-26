import databases
import utils
import sys
import logging
from psycopg2.extras import RealDictCursor

"""
Define the connection to the database outside of the "lambda_handler" function.
The connection to the database will be created the first time the function is called.
Any subsequent function call will use the same database connection.
"""
postgresql_connection = None

logger = logging.getLogger(__name__)  # Create the logger with the specified name.
logger.setLevel(logging.WARNING)  # Set the logging level of the logger.


def lambda_handler(event, context):
    """
    :argument event: The AWS Lambda uses this parameter to pass in event data to the handler.
    :argument context: The AWS Lambda uses this parameter to provide runtime information to your handler.
    """
    # Since the connection with the database were defined outside of the function, we create global variable.
    global postgresql_connection
    if not postgresql_connection:
        try:
            postgresql_connection = databases.create_postgresql_connection()
        except Exception as error:
            logger.error(error)
            sys.exit(1)

    # With a dictionary cursor, the data is sent in a form of Python dictionaries.
    cursor = postgresql_connection.cursor(cursor_factory=RealDictCursor)

    # Prepare the SQL request that returns the list of countries.
    statement = """
    select
        country_id,
        country_short_name,
        country_official_name,
        country_alpha_2_code,
        country_alpha_3_code,
        country_numeric_code,
        country_code_top_level_domain
    from
        countries;
    """

    # Execute a previously prepared SQL query.
    try:
        cursor.execute(statement)
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # After the successful execution of the query commit your changes to the database.
    postgresql_connection.commit()

    # Fetch the next row of a query result set.
    countries_entries = cursor.fetchall()

    # The cursor will be unusable from this point forward.
    cursor.close()

    # Analyze the data about countries received from the database.
    countries = list()
    if countries_entries is not None:
        for entry in countries_entries:
            country = dict()
            for key, value in entry.items():
                if "_id" in key and value is not None:
                    value = str(value)
                country[utils.camel_case(key)] = value
            countries.append(country)

    # Return the list of countries as the response.
    return countries
