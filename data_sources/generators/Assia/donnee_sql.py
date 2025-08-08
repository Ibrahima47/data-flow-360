from faker import Faker
import random
import mysql.connector

# Initialiser Faker
fake = Faker('fr_FR')

# Connexion MySQL
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="    ",  # Remplace par ton mot de passe
    database="dataflow360"
)
cursor = conn.cursor()

# Désactiver vérif FK pour reset propre
cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
tables = ["Presence", "Note", "Eleve", "Enseignant", "Cours", "Matiere", "Etablissement", "Region"]
for table in tables:
    cursor.execute(f"TRUNCATE TABLE {table}")
cursor.execute("SET FOREIGN_KEY_CHECKS = 1")

# -----------------------
# 1. Régions et villes
# -----------------------
regions_villes = {
    "Dakar": ["Dakar", "Pikine", "Rufisque", "Guédiawaye"],
    "Saint-Louis": ["Saint-Louis", "Dagana", "Podor"]
}

for region, villes in regions_villes.items():
    for ville in villes:
        cursor.execute(
            "INSERT INTO Region (nom_region, nom_ville) VALUES (%s, %s)",
            (region, ville)
        )
conn.commit()

# Récupérer ID régions
cursor.execute("SELECT id_region FROM Region")
region_ids = [row[0] for row in cursor.fetchall()]

# -----------------------
# 2. Établissements (lycées réels + collèges/élémentaires génériques)
# -----------------------
lycees_dakar = [
    "Lycée Amath Dansokho", "Lycée Galandou Diouf", "Lycée Ousmane Sembène",
    "Lycée Blaise Diagne", "Lycée Franco Arabe Cheikh M. Fadilou Mbacké",
    "Lycée John Fitzgerald Kennedy", "Lycée Lamine Gueye",
    "Lycée Mixte Maurice Delafosse", "Maison d’éducation Mariama Bâ de Gorée",
    "Lycée Ex CEM de Hann", "Lycée Moderne de Grand Dakar",
    "Lycée Aminata Sow Fall", "Lycée Ex CEM Talibou Dabou",
    "Lycée P. Ass Unité 13"
]

lycees_saint_louis = [
    "Lycée Faidherbe", "Lycée Ameth Fall", "Lycée Charles De Gaulle",
    "Lycée Richard Toll", "Lycée de Dagana", "Lycée de Podor"
]

colleges_elementaires = [
    "Collège Sacré-Cœur", "Collège Hyacinthe Thiandoum", "Collège Thiaroye",
    "École élémentaire Pikine", "École élémentaire Guédiawaye",
    "École élémentaire Saint-Louis Sud"
]

etablissements = lycees_dakar + lycees_saint_louis + colleges_elementaires
statuts = ["Public", "Privé"]

for nom in etablissements:
    if "Lycée" in nom:
        type_etab = "Lycée"
    elif "Collège" in nom:
        type_etab = "Collège"
    else:
        type_etab = "Élémentaire"

    statut = random.choice(statuts)
    id_region = random.choice(region_ids)
    cursor.execute(
        "INSERT INTO Etablissement (nom, type, statut, id_region) VALUES (%s, %s, %s, %s)",
        (nom, type_etab, statut, id_region)
    )
conn.commit()

# Récupérer ID établissements
cursor.execute("SELECT id_etab FROM Etablissement")
etab_ids = [row[0] for row in cursor.fetchall()]

# -----------------------
# 3. Prénoms et noms sénégalais
# -----------------------
prenoms_senegalais = ["Awa", "Fatou", "Mariama", "Astou", "Aminata", "Adama",
                      "Khady", "Ndeye", "Ousmane", "Cheikh", "Ibrahima",
                      "Serigne", "Mamadou", "Abdou", "Babacar"]
noms_senegalais = ["Ndiaye", "Diop", "Fall", "Sow", "Ba", "Seck",
                   "Gaye", "Faye", "Diallo", "Cissé"]

def nom_complet():
    return random.choice(prenoms_senegalais), random.choice(noms_senegalais)

# -----------------------
# 4. Génération élèves (≥100 par établissement)
# -----------------------
for etab_id in etab_ids:
    for _ in range(random.randint(100, 200)):  # au moins 100 élèves
        prenom, nom = nom_complet()
        date_naissance = fake.date_of_birth(minimum_age=6, maximum_age=20)
        sexe = "F" if prenom in ["Awa", "Fatou", "Mariama", "Astou", "Aminata", "Khady", "Ndeye"] else "M"
        redouble = random.choice([0, 1])
        adresse = fake.address()
        cursor.execute(
            "INSERT INTO Eleve (nom_eleve, prenom_eleve, date_naissance, sexe, redouble, adresse, id_etab) VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (nom, prenom, date_naissance, sexe, redouble, adresse, etab_id)
        )
conn.commit()

# Récupérer ID élèves
cursor.execute("SELECT id_eleve FROM Eleve")
eleve_ids = [row[0] for row in cursor.fetchall()]

# -----------------------
# 5. Enseignants
# -----------------------
domaines = ["Mathématiques", "Physique", "Informatique", "Histoire", "Lettres", "SVT"]

for _ in range(50):
    prenom, nom = nom_complet()
    domaine = random.choice(domaines)
    email = f"{prenom.lower()}.{nom.lower()}@ecole.sn"
    sexe = "F" if prenom in ["Awa", "Fatou", "Mariama", "Astou", "Aminata", "Khady", "Ndeye"] else "M"
    cursor.execute(
        "INSERT INTO Enseignant (nom_enseignant, domaine, email, sexe) VALUES (%s,%s,%s,%s)",
        (f"{prenom} {nom}", domaine, email, sexe)
    )
conn.commit()

# -----------------------
# 6. Cours
# -----------------------
niveaux = ["Élémentaire", "Collège", "Lycée"]

for _ in range(20):
    niveau = random.choice(niveaux)
    duree = f"{random.randint(1,3)}h"
    cursor.execute(
        "INSERT INTO Cours (niveau, duree) VALUES (%s,%s)",
        (niveau, duree)
    )
conn.commit()

# Récupérer ID cours
cursor.execute("SELECT id_cours FROM Cours")
cours_ids = [row[0] for row in cursor.fetchall()]

# -----------------------
# 7. Matières
# -----------------------
matieres = ["Mathématiques", "Physique", "SVT", "Français", "Anglais"]
for matiere in matieres:
    cursor.execute("INSERT INTO Matiere (nom_matiere) VALUES (%s)", (matiere,))
conn.commit()

# Récupérer ID matières
cursor.execute("SELECT id_matiere FROM Matiere")
matiere_ids = [row[0] for row in cursor.fetchall()]

# -----------------------
# 8. Notes (0-20) pour chaque élève
# -----------------------
types_notes = ["Devoir", "Composition", "Examen"]

for eleve_id in eleve_ids:
    for _ in range(5):  # 5 notes par élève
        matiere_id = random.choice(matiere_ids)
        type_note = random.choice(types_notes)
        valeur = round(random.uniform(0, 20), 2)
        cursor.execute(
            "INSERT INTO Note (id_eleve, id_matiere, type_note, valeur_note) VALUES (%s,%s,%s,%s)",
            (eleve_id, matiere_id, type_note, valeur)
        )
conn.commit()

# -----------------------
# 9. Présence (aléatoire) pour chaque élève dans des cours
# -----------------------
for eleve_id in eleve_ids:
    for _ in range(3):  # 3 enregistrements de présence par élève
        cours_id = random.choice(cours_ids)
        is_present = random.choice([0, 1])
        cursor.execute(
            "INSERT INTO Presence (id_eleve, id_cours, is_present) VALUES (%s,%s,%s)",
            (eleve_id, cours_id, is_present)
        )
conn.commit()

# Fermeture
cursor.close()
conn.close()

print("Toutes les données sénégalaises insérées avec succès !")
