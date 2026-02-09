import os
import time
import random
import requests
from kafka import KafkaProducer
import json

broker = os.getenv("KAFKA_BROKER", "localhost:9092")
topic = "pokemon"

producer = KafkaProducer(
    bootstrap_servers=broker,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

max_pokemon_id = 1025

while True:
    pokemon_id = random.randint(1, max_pokemon_id)

    # Pokémon base
    p_resp = requests.get(f"https://pokeapi.co/api/v2/pokemon/{pokemon_id}")
    if p_resp.status_code != 200:
        time.sleep(5)
        continue

    p = p_resp.json()

    # Species → generación
    s_resp = requests.get(p["species"]["url"])
    generation = None
    if s_resp.status_code == 200:
        generation = s_resp.json()["generation"]["name"]

    # Habilidades
    ability_main = None
    ability_hidden = None
    for a in p["abilities"]:
        if a["is_hidden"]:
            ability_hidden = a["ability"]["name"]
        else:
            ability_main = a["ability"]["name"]

    types = [t["type"]["name"] for t in p["types"]]

    data = {
        "id": p["id"],
        "name": p["name"],
        "height": p["height"],
        "weight": p["weight"],
        "base_experience": p["base_experience"],
        "primary_type": types[0] if len(types) > 0 else None,
        "secondary_type": types[1] if len(types) > 1 else None,
        "ability_main": ability_main,
        "ability_hidden": ability_hidden,
        "generation": generation
    }

    producer.send(topic, data)
    print("Sent:", data)
    time.sleep(10)
