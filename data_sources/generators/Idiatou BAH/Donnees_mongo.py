from pymongo import MongoClient
from faker import Faker
import random
from datetime import datetime

# Connexion MongoDB
client = MongoClient("mongodb://root:root@localhost:27017/")
db = client["education"]

# Initialisation Faker
fake = Faker('fr_FR')
random.seed(42)
Faker.seed(42)

# Régions et leurs villes (seulement 3 régions)
regions_sn = [
    {"nom": "Sédhiou", "villes": ["Sédhiou", "Bounkiling", "Goudomp"]},
    {"nom": "Ziguinchor", "villes": ["Ziguinchor", "Oussouye", "Bignona"]},
    {"nom": "Kolda", "villes": ["Kolda", "Vélingara", "Médina Yoro Foulah"]}
]

types_etablissement = ["CEM", "Collège", "Lycée"]
statuts = ["Public", "Privé", "Franco-arabe"]
niveaux = ["6e", "5e", "4e", "3e", "2nde", "1ère", "Tle"]
matieres_sn = [
    "Mathématiques", "Français", "Anglais", "SVT", "Histoire-Géo",
    "Philosophie", "Physique-Chimie", "Arabe", "Éducation Islamique"
]

# Nettoyer les anciennes données
# db.regions.drop()
# db.etablissements.drop()
# db.eleves.drop()
# db.matieres.drop()
# db.cours.drop()

# 1. Régions
regions = []
for i, region_info in enumerate(regions_sn):
    regions.append({
        "_id": i + 1,
        "nom": region_info["nom"],
        "directeur_region": fake.name(),
        "villes": region_info["villes"]
    })
db.regions.insert_many(regions)

# 2. Matières
matieres = []
for i, nom in enumerate(matieres_sn):
    matieres.append({
        "_id": i + 1,
        "nom": nom,
        "filiere": random.choice(["Littéraire", "Scientifique", "Arabe"])
    })
db.matieres.insert_many(matieres)

# 3. Établissements
etablissements = []
for i in range(1, 11):  # 10 établissements
    region = random.choice(regions)
    ville = random.choice(region["villes"])
    etablissements.append({
        "_id": i,
        "nom": f"{random.choice(types_etablissement)} {fake.last_name()}",
        "type": random.choice(types_etablissement),
        "statut": random.choice(statuts),
        "ville": ville,
        "region": {
            "id": region["_id"],
            "nom": region["nom"]
        }
    })
db.etablissements.insert_many(etablissements)

# Recharger les établissements depuis MongoDB
etablissements_from_db = list(db.etablissements.find())

# 4. Élèves
eleves = []
for i in range(1, 301):  # 100 élèves
    etab = random.choice(etablissements_from_db)
    eleves.append({
        "_id": i,
        "prenom": fake.first_name(),
        "nom": fake.last_name(),
        "sexe": random.choice(["M", "F"]),
        "date_naissance": datetime.combine(fake.date_of_birth(minimum_age=12, maximum_age=20), datetime.min.time()),
        "niveau": random.choice(niveaux),
        "cycle": etab["type"],
        "etablissement": {
            "id": etab["_id"],
            "nom": etab["nom"],
            "ville": etab["ville"],
            "region": etab["region"]
        },
        "resultats": {
            "annee_scolaire": random.choice(["2021-2022", "2022-2023", "2023-2024"]),
            "moyenne": round(random.uniform(5, 18), 2),
            "decision": random.choice(["Passé", "Redoublé", "Exclu"]),
            "redoublant": random.choice([True, False]),
            "observation": fake.sentence()
        }
    })
db.eleves.insert_many(eleves)

# 5. Cours
cours = []
for i in range(1, 51):  # 50 cours
    matiere = random.choice(matieres)
    etab = random.choice(etablissements_from_db)
    cours.append({
        "_id": i,
        "matiere": {
            "id": matiere["_id"],
            "nom": matiere["nom"],
            "filiere": matiere["filiere"]
        },
        "niveau": random.choice(niveaux),
        "etablissement": {
            "id": etab["_id"],
            "nom": etab["nom"],
            "ville": etab["ville"],
            "region": etab["region"]
        },
        "volume_horaire": random.randint(2, 6)
    })
db.cours.insert_many(cours)

print("✅ Données simplifiées insérées avec succès dans MongoDB")
