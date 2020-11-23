import databases
import utils
import logging
import sys
import os
from psycopg2.extras import RealDictCursor


"""
Define the connection to the database outside of the "lambda_handler" function.
The connection to the database will be created the first time the function is called.
Any subsequent function call will use the same database connection.
"""
postgresql_connection = None

# Define databases settings parameters.
POSTGRESQL_USERNAME = os.environ["POSTGRESQL_USERNAME"]
POSTGRESQL_PASSWORD = os.environ["POSTGRESQL_PASSWORD"]
POSTGRESQL_HOST = os.environ["POSTGRESQL_HOST"]
POSTGRESQL_PORT = int(os.environ["POSTGRESQL_PORT"])
POSTGRESQL_DB_NAME = os.environ["POSTGRESQL_DB_NAME"]

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
            postgresql_connection = databases.create_postgresql_connection(
                POSTGRESQL_USERNAME,
                POSTGRESQL_PASSWORD,
                POSTGRESQL_HOST,
                POSTGRESQL_PORT,
                POSTGRESQL_DB_NAME
            )
        except Exception as error:
            logger.error(error)
            sys.exit(1)

    # Define the value of the data passed to the function.
    root_organization_id = event["arguments"]["rootOrganizationId"]
    offset = event["arguments"]["currentPageNumber"]
    limit = event["arguments"]["recordsNumber"]
    offset = (offset - 1) * limit

    # With a dictionary cursor, the data is sent in a form of Python dictionaries.
    cursor = postgresql_connection.cursor(cursor_factory=RealDictCursor)

    # Prepare the SQL request that returns the information of the specific client.
    statement = """
    select
        count(*) over() as total_number_of_users,
        aggregated_data.*
    from (
        select
            distinct users.user_id,
            users.entry_created_date_time as created_date_time,
            case
                when users.identified_user_id is not null
                and users.unidentified_user_id is null then identified_users.identified_user_first_name
                else null
            end as user_first_name,
            case
                when users.identified_user_id is not null
                and users.unidentified_user_id is null then identified_users.identified_user_last_name
                else null
            end as user_last_name,
            case
                when users.identified_user_id is not null
                and users.unidentified_user_id is null then identified_users.identified_user_middle_name
                else null
            end as user_middle_name,
            case
                when users.identified_user_id is not null
                and users.unidentified_user_id is null then identified_users.identified_user_primary_email
                else null
            end as user_primary_email,
            case
                when users.identified_user_id is not null
                and users.unidentified_user_id is null then identified_users.identified_user_secondary_email
                else null
            end as user_secondary_email,
            case
                when users.identified_user_id is not null
                and users.unidentified_user_id is null then identified_users.identified_user_primary_phone_number
                else null
            end as user_primary_phone_number,
            case
                when users.identified_user_id is not null
                and users.unidentified_user_id is null then identified_users.identified_user_secondary_phone_number
                else null
            end as user_secondary_phone_number,
            case
                when users.identified_user_id is not null
                and users.unidentified_user_id is null then identified_users.identified_user_profile_photo_url
                else null
            end as user_profile_photo_url,
            case
                when users.identified_user_id is not null
                and users.unidentified_user_id is null then 'identified_user'
                else 'unidentified_user'
            end as user_type,
            case
                when users.identified_user_id is not null
                and users.unidentified_user_id is null then identified_users.metadata::text
                else unidentified_users.metadata::text
            end as metadata,
            case
                when users.identified_user_id is not null
                and users.unidentified_user_id is null then identified_users.telegram_username
                else null
            end as telegram_username,
            case
                when users.identified_user_id is not null
                and users.unidentified_user_id is null then identified_users.whatsapp_profile
                else null
            end as whatsapp_profile,
            case
                when users.identified_user_id is not null
                and users.unidentified_user_id is null then identified_users.whatsapp_username
                else null
            end as whatsapp_username,
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
            chat_rooms_users_relationship
        left join users on
            chat_rooms_users_relationship.user_id = users.user_id
        left join identified_users on
            users.identified_user_id = identified_users.identified_user_id
        left join unidentified_users on
            users.unidentified_user_id = unidentified_users.unidentified_user_id
        left join genders on
            identified_users.gender_id = genders.gender_id
        left join countries on
            identified_users.country_id = countries.country_id
        where
            users.entry_deleted_date_time is not null
        and
            users.internal_user_id is null
        and 
            (users.unidentified_user_id is not null or users.identified_user_id is not null)
        and
            chat_rooms_users_relationship.chat_room_id in (
                select
                    chat_rooms.chat_room_id
                from
                    chat_rooms
                where
                    chat_rooms.channel_id in (
                        select
                            channels_organizations_relationship.channel_id
                        from
                            channels_organizations_relationship
                        left join organizations on
                            channels_organizations_relationship.organization_id = organizations.organization_id
                        where
                            organizations.organization_id = '{0}'
                        or
                            organizations.root_organization_id = '{0}'
                    )
            )
        order by
            users.user_id
    ) as aggregated_data offset {1} limit {2};
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
    clients_entries = cursor.fetchall()

    # The cursor will be unusable from this point forward.
    cursor.close()

    # Analyze the data about clients received from the database.
    clients = list()
    total_number_of_users = 0
    if clients_entries is not None:
        for index, entry in enumerate(clients_entries):
            client = dict()
            gender = dict()
            country = dict()
            for key, value in entry.items():
                if ("_id" in key or "_date_time" in key) and value is not None:
                    value = str(value)
                if "gender_" in key:
                    gender[utils.camel_case(key)] = value
                elif "country_" in key:
                    country[utils.camel_case(key)] = value
                else:
                    client[utils.camel_case(key)] = value
            client["gender"] = gender
            client["country"] = country
            clients.append(client)
            if index == 0:
                total_number_of_users = entry["total_number_of_users"]

    # Return the full information about the clients as the response.
    response = {
        "totalNumberOfUsers": total_number_of_users,
        "clients": clients
    }
    return response
