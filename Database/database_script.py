import requests
from datetime import datetime
from pprint import pprint as pp
import logging
import json
from typing import List
import sqlite3
from utils import logger

con = sqlite3.connect('ram.db')
cursor = con.cursor()


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


@logger
def create_database():
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS episodes(
            id INTEGER PRIMARY KEY,
            name STRING,
            air_date STRING,
            episode STRING,
            url STRING,
            created DATETIME
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS characters(
            id INTEGER PRIMARY KEY,
            name STRING,
            status STRING,
            species STRING,
            type STRING,
            gender STRING,
            image STRING,
            url STRING,
            created DATETIME,
            origin_id INTEGER,
            location_id INTEGER
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS locations(
            id INTEGER PRIMARY KEY,
            name STRING,
            type STRING,
            dimension STRING,
            url STRING,
            created DATETIME
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS episodes_characters(
            id INTEGER PRIMARY KEY,
            episode_id INTEGER,
            character_id INTEGER
        )
    ''')


def insert_episodes(episode_list: List):
    rows_to_insert = [(episode['id'], episode['name'], episode['air_date'], episode['episode'], episode['url'],
                       datetime.strptime(episode['created'], '%Y-%m-%dT%H:%M:%S.%fZ'))
                      for episode in episode_list]
    cursor.executemany('''
        INSERT INTO episodes (id, name, air_date, episode, url, created)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', rows_to_insert)
    con.commit()


def insert_characters(character_list: List):
    # In the original format, the origin and the location fields are objects, not a single number, this
    # comprehension list extracts the id from the url in the original format.
    character_list = [{**row, 'origin_id': row['origin']['url'].split('/')[-1],
                       'location_id': row['location']['url'].split('/')[-1]} for row in character_list]
    for character in character_list:
        character['origin_id'] = int(character['origin_id']) if character['origin_id'] else 0
        character['location_id'] = int(character['location_id']) if character['location_id'] else 0

    rows_to_insert = [(character['id'], character['name'], character['status'], character['species'],
                       character['type'], character['gender'], character['image'], character['url'],
                       datetime.strptime(character['created'], '%Y-%m-%dT%H:%M:%S.%fZ'),
                       int(character['origin_id']), int(character['location_id'])) for character in character_list]
    cursor.executemany('''
        INSERT INTO characters (id, name, status, species, type, gender,
        image, url, created, origin_id, location_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', rows_to_insert)
    con.commit()


def insert_locations(location_list: List):
    rows_to_insert = [(location['id'], location['name'], location['type'], location['dimension'],
                       location['url'], datetime.strptime(location['created'], '%Y-%m-%dT%H:%M:%S.%fZ'))
                      for location in location_list]
    cursor.executemany('''
        INSERT INTO locations (id, name, type, dimension, url, created)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', rows_to_insert)
    con.commit()


def episode_characters_table(episode_list: List):
    """Creates the table episode_characters in the database"""
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
    character_list, location_list, episode_list = extract_data()

    create_database()
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
    except Exception:
        pass
