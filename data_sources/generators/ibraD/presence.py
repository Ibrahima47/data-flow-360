from pymongo import MongoClient
from faker import Faker
from datetime import datetime, timedelta
import random

fake = Faker("fr_FR")

# Connexion MongoDB
client = MongoClient("mongodb://root:root@localhost:27017/")
db = client["datasetMongo"]

# Chargement élèves & cours
eleves = list(db["eleves"].find())
cours = list(db["cours"].find())

# Préparer la collection
presences_collection = db["presences"]
presences_collection.drop()

# Fonction pour générer les présences
def generate_presences(eleves, cours):
    presences = []
    id_presence = 1

    start_date = datetime(2022, 10, 1)
    end_date = datetime(2024, 6, 30)
    current = start_date

    while current <= end_date:
        # Exclure week-end et vacances (juillet, août, septembre)
        if current.weekday() < 5 and current.month not in [7, 8, 9]:
            # Tirer jusqu’à 2 cours aléatoires pour la journée
            selected_courses = random.sample(cours, min(2, len(cours)))
            for cour in selected_courses:
                for eleve in eleves:
                    is_present = random.random() < 0.9  # 90% présent
                    presence = {
                        "id_presence": id_presence,
                        "eleve": {
                            "id_eleve": eleve["id_eleve"],
                            "nom_eleve": eleve["nom_eleve"],
                            "prenom_eleve": eleve["prenom_eleve"]
                        },
                        "cours": {
                            "id_cours": cour["id_cours"],
                            "nom_matiere": cour["matiere"]["nom_matiere"],
                            "niveau": cour["niveau"]
                        },
                        "date_cours": current.strftime("%Y-%m-%d"),
                        "heure_cours": fake.time(pattern="%H:%M"),
                        "present": is_present,
                        "justifie": None if is_present else random.choice([True, False])
                    }
                    presences.append(presence)
                    id_presence += 1
        current += timedelta(days=1)

    return presences

# Générer les présences
presences = generate_presences(eleves, cours)

# Insérer dans MongoDB
presences_collection.insert_many(presences)

print(f"{len(presences)} présences insérées dans la collection 'presencesCorec'.")
