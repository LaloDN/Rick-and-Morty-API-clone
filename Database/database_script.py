import requests
from pprint import pprint as pp
import json
from typing import List, Dict


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




def main():
    character_list, location_list, episode_list = extract_data()
    print(len(episode_list))

if __name__ == '__main__':
    main()