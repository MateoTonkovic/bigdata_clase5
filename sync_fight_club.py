import os
import sys
import json
import re
from typing import Dict, Any, Optional, List

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.collection import Collection
import requests

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB = os.getenv("MONGO_DB", "bigdata")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "ibdm")
TMDB_API_KEY = os.getenv("TMDB_API_KEY", "")
TMDB_REGION = os.getenv("TMDB_REGION", "US").upper()

if not TMDB_API_KEY:
    print("ERROR: TMDB_API_KEY no configurada en .env")
    sys.exit(1)

def get_collection() -> Collection:
    client = MongoClient(MONGO_URI, uuidRepresentation="standard")
    db = client[MONGO_DB]
    return db[MONGO_COLLECTION]

def to_int_or_none(v):
    try:
        if v in (None, "", "\\N"):
            return None
        return int(v)
    except Exception:
        return None

def find_film(collection: Collection, title: str, year: int) -> Optional[Dict[str, Any]]:
    query = {
        "titleType": "movie",
        "primaryTitle": {"$regex": f"^{re.escape(title)}$", "$options": "i"},
        "startYear": year
    }
    doc = collection.find_one(query)
    return doc

def tmdb_search_movie(title: str, year: int) -> Optional[Dict[str, Any]]:
    url = "https://api.themoviedb.org/3/search/movie"
    params = {"api_key": TMDB_API_KEY, "query": title, "year": year, "include_adult": False}
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()
    results = data.get("results", [])
    return results[0] if results else None

def tmdb_get_providers(movie_id: int, region: str = "US") -> Dict[str, Any]:
    url = f"https://api.themoviedb.org/3/movie/{movie_id}/watch/providers"
    params = {"api_key": TMDB_API_KEY}
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    data = r.json() or {}
    results = data.get("results", {})
    return results.get(region.upper(), {})  # keys: flatrate, rent, buy, link

def format_providers_entry(p: Dict[str, Any]) -> Dict[str, Any]:
    # normaliza a listas de nombres y guarda el link canónico de TMDB
    def names(kind: str) -> List[str]:
        return [x.get("provider_name") for x in p.get(kind, []) if isinstance(x, dict) and x.get("provider_name")]
    return {
        "region": TMDB_REGION,
        "link": p.get("link"),
        "flatrate": names("flatrate"),
        "rent": names("rent"),
        "buy": names("buy"),
    }

def update_movie_with_tmdb(collection: Collection, mongo_id, tmdb_id: int, providers: Dict[str, Any]) -> None:
    # Estructura sugerida en Mongo:
    # tmdb: { id: 550, providers: { US: { flatrate: [], rent: [], buy: [], link: "" } } }
    update = {
        "$set": {
            "tmdb.id": tmdb_id,
            f"tmdb.providers.{TMDB_REGION}": providers
        }
    }
    collection.update_one({"_id": mongo_id}, update)

def update_by_tmdb_id(collection: Collection, tmdb_id: int, providers: Dict[str, Any]) -> None:
    collection.update_many(
        {"tmdb.id": tmdb_id},
        {"$set": {f"tmdb.providers.{TMDB_REGION}": providers}}
    )

def main():
    col = get_collection()

    print("Colección Mongo:", col._name)

    # 1) recuperar Fight Club (1999) y mostrar datos
    doc = find_film(col, "Fight Club", 1999)

    if not doc:
        print("No se encontró Fight Club (1999) en Mongo.")
        sys.exit(0)

    print("=== Documento en Mongo (Fight Club) ===")
    print(json.dumps({
        "id": str(doc.get("_id")),
        "tconst": doc.get("tconst"),
        "titleType": doc.get("titleType"),
        "primaryTitle": doc.get("primaryTitle"),
        "originalTitle": doc.get("originalTitle"),
        "startYear": to_int_or_none(doc.get("startYear")),
        "endYear": to_int_or_none(doc.get("endYear")),
        "genres": doc.get("genres"),
        "tmdb": doc.get("tmdb", {})
    }, indent=2, ensure_ascii=False))

    # 2) TMDB: buscar ID y obtener plataformas de streaming
    hit = tmdb_search_movie("Fight Club", 1999)
    if not hit:
        print("TMDB: no se encontró la película.")
        sys.exit(0)

    tmdb_id = int(hit["id"])
    print(f"\nTMDB match: id={tmdb_id}, title={hit.get('title')} ({hit.get('release_date', '')})")

    providers_raw = tmdb_get_providers(tmdb_id, TMDB_REGION)
    providers_fmt = format_providers_entry(providers_raw)

    print("\n=== Plataformas de streaming (TMDB) ===")
    print(json.dumps(providers_fmt, indent=2, ensure_ascii=False))

    # 3) Actualizar en Mongo: guardar tmdb.id y providers
    update_movie_with_tmdb(col, doc["_id"], tmdb_id, providers_fmt)
    print("\nMongo actualizado con tmdb.id y providers.")

    # 4) Ejemplo: actualizar por ID (si más tarde querés refrescar solo por tmdb_id)
    #    (descomentar para usar)
    # update_by_tmdb_id(col, tmdb_id, providers_fmt)
    # print("Mongo refrescado por tmdb_id.")

if __name__ == "__main__":
    main()
