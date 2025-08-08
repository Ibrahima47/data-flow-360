from pymongo import MongoClient
from faker import Faker
from datetime import datetime, timedelta
import random

fake = Faker("fr_FR")


# Connexion MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["datasetMongo"]

# Chargement élèves & cours
eleves = list(db["eleves"].find())
cours = list(db["cours"].find())

#  Préparer la collection notes
notes_collectioncorec = db["notes"]
notes_collectioncorec.drop()

# Types de note possibles
types_notes = ["Composition", "Devoir"]

start_date = datetime(2022, 10, 1)
end_date = datetime(2024, 6, 30)

# gérer les dates pour une période précis
def random_date(start, end):
    delta = end - start # nombre total de jours entre start et end
    random_days = random.randint(0, delta.days) # choisir un nombre aléatoire de jours
    return (start + timedelta(days=random_days)).strftime("%Y-%m-%d")

#  Nombre de notes à générer
NB_NOTES = 4550 * 4
id_note =1
notes = []
for i in range(1, NB_NOTES + 1):
    cours_choisi = random.choice(cours)
    niveau_cours = cours_choisi["niveau"]

    # Filtrer les élèves par niveau d'âge approximatif (on ne peut pas vérifier le vrai niveau ici)
    eleve = random.choice(eleves)

    notes.append({
        "id_note": id_note,
        "eleve": {
            "id_eleve": eleve["id_eleve"],
            "nom_eleve": eleve["nom_eleve"],
            "prenom_eleve": eleve["prenom_eleve"]
        },
        "cours": {
            "id_cours": cours_choisi["id_cours"],
            "nom_matiere": cours_choisi["matiere"]["nom_matiere"],
            "niveau": cours_choisi["niveau"]
        },
        "note": round(random.uniform(4, 20), 2),
        "type_note": random.choice(types_notes),
        "date": random_date(start_date, end_date),

    })
    id_note += 1

# Insertion dans MongoDB
notes_collectioncorec.insert_many(notes)

print(f"{len(notes)} notes insérées dans la collection 'notes'.")
