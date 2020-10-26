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

    # Define the value of the data passed to the function.
    root_organization_id = event["arguments"]["rootOrganizationId"]
    offset = event["arguments"]["currentPageNumber"]
    limit = event["arguments"]["recordsNumber"]
    offset = (offset - 1) * limit

    # Prepare the SQL request that returns all detailed information about internal users.
    statement = """
    select
        internal_users.auth0_user_id,
        internal_users.auth0_metadata::text,
        users.user_id,
        internal_users.internal_user_first_name as user_first_name,
        internal_users.internal_user_last_name as user_last_name,
        internal_users.internal_user_middle_name as user_middle_name,
        internal_users.internal_user_primary_email as user_primary_email,
        internal_users.internal_user_secondary_email as user_secondary_email,
        internal_users.internal_user_primary_phone_number as user_primary_phone_number,
        internal_users.internal_user_secondary_phone_number as user_secondary_phone_number,
        internal_users.internal_user_profile_photo_url as user_profile_photo_url,
        internal_users.internal_user_position_name as user_position_name,
        genders.gender_id,
        genders.gender_technical_name,
        genders.gender_public_name,
        countries.country_id,
        countries.country_short_name,
        countries.country_official_name,
        countries.country_alpha_2_code,
        countries.country_alpha_3_code,
        countries.country_numeric_code,
        countries.country_code_top_level_domain,
        roles.role_id,
        roles.role_technical_name,
        roles.role_public_name,
        roles.role_description,
        organizations.organization_id,
        organizations.organization_name,
        organizations.organization_description,
        organizations.parent_organization_id,
        organizations.parent_organization_name,
        organizations.parent_organization_description,
        organizations.root_organization_id,
        organizations.root_organization_name,
        organizations.root_organization_description
    from
        users
    left join internal_users on
        users.internal_user_id = internal_users.internal_user_id
    left join genders on
        internal_users.gender_id = genders.gender_id
    left join countries on
        internal_users.country_id = countries.country_id
    left join roles on
        internal_users.role_id = roles.role_id
    left join organizations on
        internal_users.organization_id = organizations.organization_id
    where
        users.internal_user_id is not null
    and
        organizations.root_organization_id = '{0}'
    offset {1} limit {2};
    """.format(
        root_organization_id,
        offset,
        limit
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
    internal_users_entries = cursor.fetchall()

    # The cursor will be unusable from this point forward.
    cursor.close()

    # Analyze the data about internal users received from the database.
    internal_users = list()
    if internal_users_entries is not None:
        for entry in internal_users_entries:
            internal_user = dict()
            gender = dict()
            country = dict()
            role = dict()
            organization = dict()
            for key, value in entry.items():
                if "_id" in key and value is not None:
                    value = str(value)
                if "gender_" in key:
                    gender[utils.camel_case(key)] = value
                elif "country_" in key:
                    country[utils.camel_case(key)] = value
                elif "role_" in key:
                    role[utils.camel_case(key)] = value
                elif "organization_" in key:
                    organization[utils.camel_case(key)] = value
                else:
                    internal_user[utils.camel_case(key)] = value
            internal_user["gender"] = gender
            internal_user["country"] = country
            internal_user["role"] = role
            internal_user["organization"] = organization
            internal_users.append(internal_user)

    # Return the list of internal users as the response.
    return internal_users
