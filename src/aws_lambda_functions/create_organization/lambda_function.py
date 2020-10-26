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

    # Define values of the data passed to the function.
    organization_name = event["arguments"]["input"]["organizationName"]
    try:
        organization_description = event["arguments"]["input"]["organizationDescription"]
    except KeyError:
        organization_description = None
    try:
        parent_organization_id = event["arguments"]["input"]["parentOrganizationId"]
    except KeyError:
        parent_organization_id = None
    try:
        parent_organization_name = event["arguments"]["input"]["parentOrganizationName"]
    except KeyError:
        parent_organization_name = None
    try:
        parent_organization_description = event["arguments"]["input"]["parentOrganizationDescription"]
    except KeyError:
        parent_organization_description = None
    try:
        root_organization_id = event["arguments"]["input"]["rootOrganizationId"]
    except KeyError:
        root_organization_id = None
    try:
        root_organization_name = event["arguments"]["input"]["rootOrganizationName"]
    except KeyError:
        root_organization_name = None
    try:
        root_organization_description = event["arguments"]["input"]["rootOrganizationDescription"]
    except KeyError:
        root_organization_description = None

    # With a dictionary cursor, the data is sent in a form of Python dictionaries.
    cursor = postgresql_connection.cursor(cursor_factory=RealDictCursor)

    # Prepare the SQL request that creates new organization.
    statement = """
    insert into organizations (
        organization_name,
        organization_description,
        parent_organization_id,
        parent_organization_name,
        parent_organization_description,
        root_organization_id,
        root_organization_name,
        root_organization_description
    ) values (
        {0},
        {1},
        {2},
        {3},
        {4},
        {5},
        {6},
        {7}
    ) returning
        organization_id,
        organization_name,
        organization_description,
        parent_organization_id,
        parent_organization_name,
        parent_organization_description,
        root_organization_id,
        root_organization_name,
        root_organization_description;
    """.format(
        "'{0}'".format(organization_name),
        'null' if organization_description is None or len(organization_description) == 0
        else "'{0}'".format(organization_description),
        'null' if parent_organization_id is None or len(parent_organization_id) == 0
        else "'{0}'".format(parent_organization_id),
        'null' if parent_organization_name is None or len(parent_organization_name) == 0
        else "'{0}'".format(parent_organization_name),
        'null' if parent_organization_description is None or len(parent_organization_description) == 0
        else "'{0}'".format(parent_organization_description),
        'null' if root_organization_id is None or len(root_organization_id) == 0
        else "'{0}'".format(root_organization_id),
        'null' if root_organization_name is None or len(root_organization_name) == 0
        else "'{0}'".format(root_organization_name),
        'null' if root_organization_description is None or len(root_organization_description) == 0
        else "'{0}'".format(root_organization_description)
    )

    # Execute a previously prepared SQL query.
    try:
        cursor.execute(statement)
    except Exception as error:
        logger.error(error)
        sys.exit(1)

    # After the successful execution of the query commit your changes to the database.
    postgresql_connection.commit()

    # Fetch the next row of a query result set.
    organization_entry = cursor.fetchone()

    # The cursor will be unusable from this point forward.
    cursor.close()

    # Analyze the data about organization received from the database.
    organization = dict()
    if organization_entry is not None:
        for key, value in organization_entry.items():
            if "_id" in key and value is not None:
                value = str(value)
            organization[utils.camel_case(key)] = value

    # Return the information about the organization.
    return organization
