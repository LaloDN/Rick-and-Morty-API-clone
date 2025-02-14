import requests
from datetime import datetime
from mariadb import connect
from mariadb.connections import Connection
from dotenv import load_dotenv
import os
import json
from typing import List
from utils import logger
from pprint import pprint as pp

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
            type VARCHAR(50),
            dimension VARCHAR(50),
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
            type VARCHAR(50),
            gender VARCHAR(15) NOT NULL,
            image VARCHAR(70) NOT NULL,
            url VARCHAR(70) NOT NULL,
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
            id INT AUTO_INCREMENT NOT NULL,
            episode_id INT NOT NULL,
            character_id INT NOT NULL,
            PRIMARY KEY (id),
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


def insert_episodes(episode_list: List, con: Connection):
    rows_to_insert = [(episode['id'], episode['name'], episode['air_date'],
                       episode['episode'], episode['url'],
                       datetime.strptime(episode['created'], '%Y-%m-%dT%H:%M:%S.%fZ'))
                      for episode in episode_list]
    cursor = con.cursor()
    cursor.executemany("""
        INSERT INTO episodes (id, name, air_date, episode, url, created)
        VALUES (?,?,?,?,?,?)
    """, rows_to_insert)
    con.commit()


def insert_locations(location_list: List, con: Connection):
    rows_to_insert = [(location['id'], location['name'], location['type'],
                       None if location['dimension'] == '' else location['dimension'], location['url'],
                       datetime.strptime(location['created'], '%Y-%m-%dT%H:%M:%S.%fZ'))
                      for location in location_list]
    cursor = con.cursor()
    cursor.executemany("""
        INSERT INTO locations (id, name, type, dimension, url, created)
        VALUES (?,?,?,?,?,?)
    """, rows_to_insert)
    con.commit()


def insert_characters(character_list: List, con: Connection):
    rows_to_insert = [(character['id'], character['name'], character['status'],
                       character['species'], None if character['type'] == '' else character['type'],
                       character['gender'], character['image'], character['url'],
                       datetime.strptime(character['created'], '%Y-%m-%dT%H:%M:%S.%fZ'),
                       None if character['location']['url'] == '' else int(character['location']['url'].split('/')[-1]),
                       None if character['origin']['url'] == '' else int(character['origin']['url'].split('/')[-1])
                       ) for character in character_list]

    cursor = con.cursor()
    cursor.executemany("""
        INSERT INTO characters (id, name, status, species, type, gender, 
                                image, url, created, location_id, origin_id)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, rows_to_insert)
    con.commit()


def episode_characters_table(episode_list: List, con: Connection):
    """Creates the table episode_characters in the database"""
    cursor = con.cursor()
    rows_to_insert = []
    for episode in episode_list:
        characters_ids = [character.split('/')[-1] for character in episode['characters']]
        episode_character_ids = [(episode['id'], int(character_id)) for character_id in characters_ids]
        rows_to_insert.extend(episode_character_ids)
    cursor.executemany('''
               INSERT INTO episodes_characters (episode_id, character_id)
                VALUES (?, ?)
            ''', rows_to_insert)
    con.commit()


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
    insert_episodes(episode_list,con)
    insert_locations(location_list,con)
    insert_characters(character_list, con)
    episode_characters_table(episode_list, con)
    con.close()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(e)
