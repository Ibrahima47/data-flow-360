from pymongo import MongoClient
import random

#  Connexion MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["datasetMongo"]

# Charger enseignants et matières
enseignants = list(db["enseignants"].find())
matieres = list(db["matieres"].find())

# Préparer la collection cours
cours_collection = db["cours"]
cours_collection.drop()

# Niveaux scolaires
niveaux = ["6e", "5e", "4e", "3e", "2nde", "1ère", "Terminale"]

cours = []
cours_id = 1

for niveau in niveaux:
    for matiere in matieres:
        enseignant = random.choice(enseignants)
        cours.append({
            "id_cours": cours_id,
            "niveau": niveau,
            "duree": matiere["volume_horaire"],
            "annee": "2024-2025",
            "enseignant": {
                "id_enseignant": enseignant["id_enseignant"],
                "nom_enseignant": enseignant["nom_enseignant"],
                "prenom_enseignant": enseignant["prenom_enseignant"],
                "email": enseignant["email"]
            },
            "matiere": {
                "id_matiere": matiere["id_matiere"],
                "nom_matiere": matiere["nom_matiere"],
                "volume_horaire": matiere["volume_horaire"]
            }
        })
        cours_id += 1

#Insertion
cours_collection.insert_many(cours).

print(f"{len(cours)} cours générés et insérés dans la collection 'cours'.")
