import os
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB = os.getenv("MONGO_DB", "bigdata")
TITLES_COLLECTION = os.getenv("TITLES_COLLECTION", os.getenv("MONGO_COLLECTION", "ibdm"))
RATINGS_COLLECTION = os.getenv("RATINGS_COLLECTION", "ratings")
TMDB_REGION = os.getenv("TMDB_REGION", "US").upper()

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
col_titles = db[TITLES_COLLECTION]
col_ratings = db[RATINGS_COLLECTION]

def providers_to_list(pregion) -> list:
    """
    Acepta:
    - dict estilo TMDB: {"flatrate": [ {provider_name: ...}, ...], "rent": [...], "buy": [...]}
    - dict normalizado: {"flatrate": ["HBO Max", ...], "rent": [...], "buy": [...]}
    - lista plana: ["HBO Max", "Apple TV"]  (fallback)
    - None / tipos raros -> []
    Devuelve lista única ordenada alfabéticamente.
    """
    if pregion is None:
        return []

    # Caso lista plana
    if isinstance(pregion, list):
        names = []
        for x in pregion:
            if isinstance(x, str):
                names.append(x.strip())
            elif isinstance(x, dict):
                name = x.get("provider_name") or x.get("name")
                if name:
                    names.append(str(name).strip())
        return sorted(list(dict.fromkeys([n for n in names if n])))

    # Caso dict con llaves flatrate/rent/buy
    if isinstance(pregion, dict):
        names = []
        for kind in ("flatrate", "rent", "buy"):
            arr = pregion.get(kind) or []
            if isinstance(arr, list):
                for x in arr:
                    if isinstance(x, str):
                        names.append(x.strip())
                    elif isinstance(x, dict):
                        name = x.get("provider_name") or x.get("name")
                        if name:
                            names.append(str(name).strip())
        return sorted(list(dict.fromkeys([n for n in names if n])))

    # Otro tipo
    return []


def coerce_year(v):
    if v in (None, "", "\\N"):
        return None
    try:
        return int(v)
    except Exception:
        return None

# -------------------------------
# A) FIGHT CLUB → info + providers + avgRating
# -------------------------------
def show_fight_club_row(title="Fight Club", year=1999, region=TMDB_REGION):
    # Traer título
    doc = col_titles.find_one({
        "titleType": "movie",
        "primaryTitle": {"$regex": f"^{title}$", "$options": "i"},
        "startYear": year
    })
    if not doc:
        print(f"No se encontró {title} ({year}).")
        return

    # Traer rating por tconst (si existe la colección)
    rating = None
    try:
        if col_ratings.count_documents({}, limit=1) > 0 and doc.get("tconst"):
            r = col_ratings.find_one({"tconst": doc["tconst"]}, {"_id": 0, "averageRating": 1, "numVotes": 1})
            rating = (r or {}).get("averageRating")
    except Exception:
        # si no existe la colección ratings, seguimos sin rating
        pass

    # Providers desde tmdb.providers.<REGION>
    tmdb = doc.get("tmdb", {})
    pregion = (tmdb.get("providers") or {}).get(region)
    providers = providers_to_list(pregion)

    row = {
        "primaryTitle": doc.get("primaryTitle"),
        "startYear": coerce_year(doc.get("startYear")),
        "genres": doc.get("genres"),
        "avgRating": rating,
        "streamingProviders": providers
    }

    df = pd.DataFrame([row])
    print("\n=== Fight Club (resumen) ===")
    print(df.to_string(index=False))

# -------------------------------
# B) Rating promedio por género (últimos 5 años)
# -------------------------------
def avg_rating_by_genre_last5():
    current_year = datetime.now().year
    min_year = current_year - 4  # últimos 5 años inclusive (ej: 2021..2025)

    # Traemos solo películas en ese rango con campos relevantes
    cursor = col_titles.find(
        {"titleType": "movie", "startYear": {"$gte": min_year}},
        {"_id": 0, "tconst": 1, "startYear": 1, "genres": 1}
    )
    titles = list(cursor)
    if not titles:
        print(f"No hay películas desde {min_year}.")
        return

    # DataFrame de títulos
    df_t = pd.DataFrame(titles)
    # coerce de startYear
    df_t["startYear"] = df_t["startYear"].apply(coerce_year)

    # Traemos ratings (si existe)
    try:
        ratings = list(col_ratings.find({}, {"_id": 0, "tconst": 1, "averageRating": 1}))
        df_r = pd.DataFrame(ratings)
        # Join por tconst
        df = df_t.merge(df_r, on="tconst", how="left")
    except Exception:
        # si no hay ratings, seguimos con df_t y columna NaN
        df = df_t.copy()
        df["averageRating"] = pd.NA

    # Limpiamos géneros
    df["genres"] = df["genres"].fillna("")
    # dividir string "A,B,C" → listas; explotar en filas
    df["genres_list"] = df["genres"].apply(lambda s: [] if s in ("", "\\N") else [g.strip() for g in s.split(",") if g.strip()])
    df = df.explode("genres_list")

    # filtrar filas sin género o sin rating
    df = df[(df["genres_list"].notna()) & (df["genres_list"] != "")]
    df = df[df["averageRating"].notna()]

    # agrupar por género y calcular promedio
    out = (
        df.groupby("genres_list", as_index=False)["averageRating"]
          .mean()
          .rename(columns={"genres_list": "genre", "averageRating": "avgRating"})
          .sort_values(by=["avgRating", "genre"], ascending=[False, True])
    )

    print(f"\n=== Rating promedio por género (películas desde {min_year}) ===")
    if out.empty:
        print("No hay ratings para calcular promedios.")
    else:
        # formato 2 decimales
        out["avgRating"] = out["avgRating"].round(2)
        print(out.to_string(index=False))

if __name__ == "__main__":
    show_fight_club_row(title="Fight Club", year=1999, region=TMDB_REGION)
    avg_rating_by_genre_last5()
