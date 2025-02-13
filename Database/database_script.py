import requests
from datetime import datetime
from mariadb import connect
from mariadb.connections import Connection
from dotenv import load_dotenv
import os
import json
from typing import List
from utils import logger

load_dotenv()


def database_connection(db_created=False) -> Connection:
    args = {
        'host': os.getenv('DB_HOST'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PW'),
    }
    if db_created:
        args['database'] = 'ram'
    connection = connect(**args)
    return connection


def create_database(connection: Connection):
    cursor = connection.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS ram")


def create_tables(connection: Connection):
    cursor = connection.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS episodes(
            id INT NOT NULL,
            name VARCHAR(70) NOT NULL,
            air_date VARCHAR(30) NOT NULL,
            episode VARCHAR(6) NOT NULL,
            url VARCHAR(50) NOT NULL,
            created DATETIME NOT NULL,
            PRIMARY KEY (id)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS locations(
            id INT NOT NULL,
            name VARCHAR(50) NOT NULL,
            type VARCHAR(30),
            dimension VARCHAR(30),
            url VARCHAR(50) NOT NULL,
            created DATETIME NOT NULL,
            PRIMARY KEY (id)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS characters(
            id INT NOT NULL,
            name VARCHAR(60) NOT NULL,
            status VARCHAR(15) NOT NULL,
            species VARCHAR(30) NOT NULL,
            type VARCHAR(20),
            gender VARCHAR(15) NOT NULL,
            image VARCHAR(50) NOT NULL,
            url VARCHAR(50) NOT NULL,
            created DATETIME NOT NULL,
            origin_id INT,
            location_id INT,
            PRIMARY KEY (id),
            FOREIGN KEY (origin_id) REFERENCES locations(id) ON DELETE CASCADE,
            FOREIGN KEY (location_id) REFERENCES locations(id) ON DELETE CASCADE
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS episodes_characters(
            id INT NOT NULL,
            episode_id INT NOT NULL,
            character_id INT NOT NULL,
            FOREIGN KEY (episode_id) REFERENCES episodes(id) ON DELETE CASCADE,
            FOREIGN KEY (character_id) REFERENCES characters(id) ON DELETE CASCADE
        )
    """
    )


def extract_data() -> List[List]:
    """Extracts all the data of the characters, locations and episodes from the ramapi"""

    # Since all the endpoints got the same structure, we're going to cycle through the
    # endpoint names to save code.
    endpoint_names = ["character", "location", "episode"]
    main_list = []

    for endpoint in endpoint_names:
        current_list = []
        next_page = 'https://rickandmortyapi.com/api/{page}'.format(page=endpoint)

        while next_page:
            endpoint_request = requests.get(next_page)
            request_parsed = json.loads(endpoint_request.text)
            current_list.extend(request_parsed['results'])
            next_page = request_parsed['info']['next']

        main_list.append(current_list)

    return main_list


def insert_episodes(episode_list: List):
    pass


def insert_characters(character_list: List):
    pass


def insert_locations(location_list: List):
    pass


def episode_characters_table(episode_list: List):
    pass


def main():
    # Creating the database
    con = database_connection()
    create_database(con)
    con.close()
    # Extracting the data from the API
    character_list, location_list, episode_list = extract_data()
    # Creating a new connection and the tables
    con = database_connection(db_created=True)
    create_tables(con)
    # Inserting all the data into the tables
    # insert_episodes(episode_list)
    # insert_characters(character_list)
    # insert_locations(location_list)
    # episode_characters_table(episode_list)
    con.close()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(e)
