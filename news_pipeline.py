import requests
import json
import os


def run(api_url):
    response = requests.get(api_url)
    data = response.json()

    if "articles" not in data:
        raise Exception("Invalid News API response")

    if not os.path.exists("outputs"):
        os.makedirs("outputs")

    output_file = "outputs/news_output.json"

    with open(output_file, "w") as f:
        json.dump(data, f, indent=4)

    return output_file