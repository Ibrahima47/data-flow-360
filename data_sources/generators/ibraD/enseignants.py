from pymongo import MongoClient
from faker import Faker
import random

fake = Faker("fr_FR")

# Connexion MongoDB
client = MongoClient("mongodb://root:root@localhost:27017/")
db = client["datasetMongo"]

# Récupération des établissements
etabs_collection = db["etablissements"]
etablissements = list(etabs_collection.find())

# Préparer la collection enseignants
enseignants_collection = db["enseignants"]
enseignants_collection.drop()

# Liste de domaines/matières
domaines = ["Mathématiques", "Français", "Physique", "SVT", "Histoire", "Géographie", "Anglais", "Espagnol", "Philosophie", "Informatique"]

# Noms et prénoms typiques Sénégalais
noms = ["Diop", "Ndiaye", "Fall", "Ba", "Faye", "Sarr", "Sow", "Gueye", "Sy", "Mbaye", "Camara", "Diallo", "Ndoye", "Kane", "Cissé", "Toure", "Sané", "Coly", "Seck", "Niane"]
prenoms_femmes = ["Awa", "Aminata", "Fatou", "Adama", "Coumba", "Mariama", "Bineta", "Khadija", "Astou", "Ndeye"]
prenoms_hommes = ["Mamadou", "Cheikh", "Ousmane", "Moussa", "Abdou", "Aliou", "Ibrahima", "Boubacar", "Serigne", "Elhadji"]

id_enseignant = 1
enseignants = []
# 728 enseignats contre 18200 élèves, soit 25 élèves par enseigant. 
for i in range(1, 729):
    sexe = random.choice(["M", "F"])
    prenom = random.choice(prenoms_femmes if sexe == "F" else prenoms_hommes)
    nom = random.choice(noms)
    domaine = random.choice(domaines)
    email = f"{prenom.lower()}.{nom.lower()}@edu.sn"
    etab = random.choice(etablissements)

    enseignant = {
        "id_enseignant": id_enseignant,
        "nom_enseignant": nom,
        "prenom_enseignant": prenom,
        "domaine": domaine,
        "email": email,
        "date_naissance": fake.date_between(start_date="-55y", end_date="-25y").isoformat(),
        "sexe": sexe,
        "adresse": fake.address().replace("\n", ", "),
        "etablissement": {
            "id_etablissement": etab["id_etablissement"],
            "nom_etablissement": etab["nom"],
            "type": etab["type"],
            "statut": etab["statut"],
            "region": etab["region"]
        }
    }
    
    enseignants.append(enseignant)
    id_enseignant += 1
# Insertion dans MongoDB
enseignants_collection.insert_many(enseignants)

print("728 enseignants insérés dans la collection 'enseignants' de la DB 'datasetMongo'.")
