import databases
import utils
import sys
import logging
import itertools
import copy
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
    # Since the connection with the database were defined outside of the function, we create global variables.
    global postgresql_connection
    if not postgresql_connection:
        try:
            postgresql_connection = databases.create_postgresql_connection()
        except Exception as error:
            logger.error(error)
            sys.exit(1)

    # With a dictionary cursor, the data is sent in a form of Python dictionaries.
    cursor = postgresql_connection.cursor(cursor_factory=RealDictCursor)

    # Prepare the SQL request that returns the list of internal users.
    statement = """
    select
        users.user_id,
        internal_users.internal_user_id,
        internal_users.internal_user_first_name as user_first_name,
        internal_users.internal_user_last_name as user_last_name,
        internal_users.internal_user_middle_name as user_middle_name,
        internal_users.internal_user_primary_email as user_primary_email,
        internal_users.internal_user_secondary_email as user_secondary_email,
        internal_users.internal_user_primary_phone_number as user_primary_phone_number,
        internal_users.internal_user_secondary_phone_number as user_secondary_phone_number,
        internal_users.internal_user_profile_photo_url as user_profile_photo_url,
        genders.gender_id,
        genders.gender_technical_name,
        genders.gender_public_name,
        countries.country_id,
        countries.country_short_name,
        countries.country_official_name,
        countries.country_alpha_2_code,
        countries.country_alpha_3_code,
        countries.country_numeric_code,
        countries.country_code_top_level_domain
    from
        users
    left join internal_users on
        users.internal_user_id = internal_users.internal_user_id
    left join genders on
        internal_users.gender_id = genders.gender_id
    left join countries on
        internal_users.country_id = countries.country_id
    where
        users.internal_user_id is not null;
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
    user_entries = cursor.fetchall()

    # Prepare the SQL request that returns information about organizations of internal users.
    statement = """
    select
        internal_users_organizations_relationship.internal_user_id,
        organizations.organization_id,
        organizations.organization_name,
        organizations.organization_description,
        organizations.parent_organization_id,
        organizations.parent_organization_name,
        organizations.parent_organization_description
    from
        internal_users_organizations_relationship
    left join organizations on
        internal_users_organizations_relationship.organization_id = organizations.organization_id
    where
        internal_users_organizations_relationship.internal_user_id = (
            select
                internal_user_id
            from
                users
            where
                internal_user_id is not null
        );
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
    organizations_entries = cursor.fetchall()

    # The cursor will be unusable from this point forward.
    cursor.close()

    # Pre-sort the data to group it later.
    organizations_entries = sorted(
        organizations_entries,
        key=lambda x: x['internal_user_id']
    )

    """
    The "groupby" function makes grouping objects in an iterable a snap. It takes an iterable inputs and a key to group
    by, and returns an object containing iterators over the elements of inputs grouped by the key.
    """
    grouped_organizations = itertools.groupby(
        copy.deepcopy(organizations_entries),
        key=lambda x: x['internal_user_id']
    )

    # Store grouped organizations in a dictionary for quick search and matching.
    organizations_storage = dict()

    # Analyze data, clean the dictionaries of certain keys and convert the key names in the dictionaries.
    for key, records in grouped_organizations:
        cleaned_records = list()
        for record in list(records):
            cleaned_record = dict()
            [record.pop(item) for item in ['internal_user_id']]
            for _key, _value in record.items():
                cleaned_record[utils.camel_case(_key)] = _value
            cleaned_records.append(cleaned_record)
        organizations_storage[key] = cleaned_records

    # Analyze the data about internal users received from the database.
    users = list()
    for entry in user_entries:
        user = dict()
        gender = dict()
        country = dict()
        for key, value in entry.items():
            # Convert the UUID data type to the string.
            if "_id" in key and value is not None:
                value = str(value)
            if "gender_" in key:
                gender[utils.camel_case(key)] = value
            elif "country_" in key:
                country[utils.camel_case(key)] = value
            elif key != "internal_user_id":
                continue
            else:
                user[utils.camel_case(key)] = value
        user["gender"] = gender
        user["country"] = country
        user["organizations"] = organizations_storage[entry["internal_user_id"]]
        users.append(user)

    # Return the list of internal users as the response.
    return users
