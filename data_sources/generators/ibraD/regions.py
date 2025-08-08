from pymongo import MongoClient

# Connexion à MongoDB (ajuste l'URI si nécessaire)
client = MongoClient("mongodb://localhost:27017/")
db = client["datasetMongo"]
regions_collection = db["regions"]

# Données des régions avec leurs villes
regions_data = [
    {
        "id_region": 1,
        "nom_region": "Diourbel",
        "villes": ["Diourbel", "Mbacké", "Bambey"]
    },
    {
        "id_region": 2,
        "nom_region": "Thiès",
        "villes": ["Mbour1", "Mbour2", "Thiès Département", "Thiès Ville", "Tivaouane"]
    }
]

# Insertion dans la collection
regions_collection.insert_many(regions_data)

print("Régions insérées avec succès dans MongoDB.")
