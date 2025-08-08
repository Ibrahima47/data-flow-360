from pymongo import MongoClient

# Connexion MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["datasetMongo"]

# Préparation collection matières
matieres_collection = db["matieres"]
matieres_collection.drop()

# Liste des matières avec volume horaire
matiere_data = [
    ("Mathématiques", 5),
    ("Français", 4),
    ("SVT", 2),
    ("Histoire", 2),
    ("Géographie", 2),
    ("Physique", 3),
    ("Chimie", 2),
    ("Anglais", 3),
    ("Espagnol", 2),
    ("Informatique", 2),
    ("Philosophie", 2),
    ("Éducation civique", 1)
]

id_matiere = 1
# Génération des documents
matieres = []
for i, (nom, volume) in enumerate(matiere_data, start=1):
    matieres.append({
        "id_matiere": id_matiere,
        "nom_matiere": nom,
        "volume_horaire": volume,
        "annee": "2024-2025"
    })
    id_matiere +=1
# Insertion dans MongoDB
matieres_collection.insert_many(matieres)

print("Insertion de", len(matieres), "matières dans la collection 'matieres'.")
