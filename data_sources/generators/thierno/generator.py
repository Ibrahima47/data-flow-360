from faker import Faker
import random
from datetime import datetime,timedelta
import json
from dateutil.relativedelta import relativedelta
from pymysql import connect
from pymysql.cursors import DictCursor

faker = Faker('fr_FR')  # localisation francophone pour style sénégalais

REGIONS_SN = [
    {"id_region": 1, "nom_region": "Tambacounda", "ville": "Bakel"},
    {"id_region": 2, "nom_region": "Tambacounda", "ville": "Goudiri"},
    {"id_region": 3, "nom_region": "Tambacounda", "ville": "Tambacounda"},
    {"id_region": 4, "nom_region": "Tambacounda", "ville": "Koumpentoum"},
    {"id_region": 5, "nom_region": "Tambacounda", "ville": "Kidira"},
    {"id_region": 6, "nom_region": "Louga", "ville": "Louga"},
    {"id_region": 7, "nom_region": "Louga", "ville": "Kebemer"},
    {"id_region": 8, "nom_region": "Louga", "ville": "Linguere"},
    {"id_region": 9, "nom_region": "Louga", "ville": "Darou Mousty"},
    {"id_region": 10, "nom_region": "Matam", "ville": "Matam"},
]


ETABLISSEMENTS=[
    {"id_etablissement":2,"id_region":2,"nom_etablissement":"lycee goudiry","status":"Lycee"},
    {"id_etablissement":1,"id_region":1,"nom_etablissement":"Lycée waoundé ndiaye bakel","status":"Lycee"},
    {"id_etablissement":3,"id_region":3,"nom_etablissement":"Lycée Excellence De Tambacounda","status":"Lycee"},
    {"id_etablissement":4,"id_region":3,"nom_etablissement":" Lycée Tamba Commune","status":"Lycee"},
    {"id_etablissement":5,"id_region":3,"nom_etablissement":"Lycée Mame Cheikh Mbaye","status":"Lycee"},
    {"id_etablissement":6,"id_region":4,"nom_etablissement":"Lycée Bouna Sémou Niang de Koumpentoum","status":"Lycee"},
    {"id_etablissement":7,"id_region":5,"nom_etablissement":"Lycée de Kidira","status":"Lycee"},
    {"id_etablissement":8,"id_region":6,"nom_etablissement":"lycee Malick SALL","status":"Lycee"},
    {"id_etablissement":9,"id_region":6,"nom_etablissement":"Nouveau lycee de louga","status":"Lycee"},
    {"id_etablissement":10,"id_region":7,"nom_etablissement":"Lycée Macodou Kanghé Sall de Kébémer","status":"Lycee"},
    {"id_etablissement":11,"id_region":8,"nom_etablissement":"Lycée Bouna Sémou Niang de Koumpentoum","status":"Lycee"},
    {"id_etablissement":12,"id_region":9,"nom_etablissement":"lycee Darou Mousty","status":"Lycee"},
    {"id_etablissement":13,"id_region":10,"nom_etablissement":"Lycée de Ourossogui","status":"Lycee"},
    {"id_etablissement":14,"id_region":10,"nom_etablissement":"Lycée de Thilogne","status":"Lycee"},
    {"id_etablissement":15,"id_region":10,"nom_etablissement":"Lycée de Nguidjilone","status":"Lycee"},
    {"id_etablissement":16,"id_region":10,"nom_etablissement":"Lycée de Bokidiawé","status":"Lycee"},
    {"id_etablissement":17,"id_region":10,"nom_etablissement":"Lycée d'Agnam","status":"Lycee"}
]


# Listes de noms sénégalais (peuvent être étendues)
prenoms_masculins = ['Moussa', 'Cheikh', 'Ibrahima', 'Abdoulaye', 'Serigne', 'Mamadou',"Thierno","Mor","Babacar","Mouhamed","Ousseynou","Assane"]
prenoms_feminins = ['Fatou', 'Aminata', 'Mame', 'Adama', 'Awa', 'Astou',"Idia","Mariama","Aminata","Mareme","Fatima","Fatoumata","Anta","Fanta"]
noms_famille = ['Diallo', 'Diop', 'Ndiaye', 'Sow', 'Ba', 'Faye', 'Fall', 'Gueye',"Fall","Diakhoumpa"]




MATIERES_SN = [
    "Mathématiques", "Français", "Histoire-Géographie", "Physique-Chimie",
    "SVT", "Anglais", "Arabe", "Philosophie", "Informatique", "EPS"
]


NIVEAUX = ["2nde", "1ere", "Terminale"]

STATUTS = ["Public"]
TYPES_ETABLISSEMENT = ["Lycée"]

SEXE = ["M", "F"]
TYPES_NOTE = ["Devoir", "Examen"]
ANNEES = [f"{y}-{y+1}" for y in range(2022, 2024)]

def generate_enseignants(n=10):
    domaines = MATIERES_SN
    return [{
        "id_enseignant": i + 1,
        "nom_enseignant": faker.name(),
        "email": faker.email(),
        "sexe": random.choice(SEXE),
        "domaine": random.choice(domaines)
    } for i in range(n)]

def generate_matieres():
    return [{"id_matiere": i + 1, "nom_matiere": name} for i, name in enumerate(MATIERES_SN)]



def generate_eleves():
    eleves = []
    id_counter = 1

    for etab in ETABLISSEMENTS:
        n = random.randint(70,150)
        for _ in range(n):
            sexe = random.choice(['M', 'F'])
            prenom = random.choice(prenoms_masculins if sexe == 'M' else prenoms_feminins)
            nom = random.choice(noms_famille)
            date_naissance = faker.date_of_birth(minimum_age=18, maximum_age=23).strftime("%Y-%m-%d")
            redouble = random.choice([True, False])

            eleve = {
                "id_eleve": id_counter,
                "nom_eleve": nom,
                "prenom_eleve": prenom,
                "date_naissance": date_naissance,
                "sexe": sexe,
                "redouble": redouble,
                "id_etablissement": etab["id_etablissement"]
            }

            eleves.append(eleve)
            id_counter += 1
    
    return eleves

def generate_cours(n, matieres, enseignants):
    cours = []

    for i in range(n):
        matiere = random.choice(matieres)
        enseignant = random.choice(enseignants)

        cours.append({
            "id_cours": i + 1,
            "niveau": random.choice(NIVEAUX),
            "duree": random.randint(30, 120),
            "id_matiere": matiere["id_matiere"],
            "id_enseignant": enseignant["id_enseignant"]
        })

    return cours

def generate_presences(eleves, cours):
    presences = []
    id_presence = 1

    start_date = datetime(2022, 10, 1)
    end_date = datetime(2024,6,30)
    current = start_date

    while current <= end_date:
        # Exclure les samedis, dimanches et les mois 7, 8, 9
        if current.weekday() < 5 and current.month not in [7, 8, 9]:
            # Choisir jusqu’à 3 cours différents aléatoirement
            selected_courses = random.sample(cours, min(3, len(cours)))
            for cour in selected_courses:
                for eleve in eleves:
                    presences.append({
                        "id_presence": id_presence,
                        "id_eleve": eleve["id_eleve"],
                        "id_cours": cour["id_cours"],
                        "date_cours": current.strftime("%Y-%m-%d"),
                        "present": random.random() < 0.9
                    })
                    id_presence += 1
        current += timedelta(days=1)

    return presences

def generate_notes(eleves, matieres, annees):
    notes = []
    id_note = 1

    for eleve in eleves:
        for matiere in matieres:
            for annee in annees:
                for type_note in ["devoir", "examen"]:
                    notes.append({
                        "id_note": id_note,
                        "id_eleve": eleve["id_eleve"],
                        "id_matiere": matiere["id_matiere"],
                        "annee": annee,
                        "type": type_note,
                        "note": round(random.uniform(5, 20), 2)  # note réaliste entre 5 et 20
                    })
                    id_note += 1
    return notes

def generate_enseignement(n=20, enseignant_ids=[], matiere_ids=[]):
    return [{
        "id_enseignement": i + 1,
        "id_enseignant": random.choice(enseignant_ids),
        "id_matiere": random.choice(matiere_ids),
        "annee": random.choice(ANNEES)
    } for i in range(n)]


def insert_regions(cursor, regions):
    for region in regions:
        cursor.execute(
            "INSERT INTO regions (id_region, nom_region,ville) VALUES (%s, %s,%s)",
            (region["id_region"], region["nom_region"],region["ville"])
        )

def insert_etablissements(cursor, etablissements):
    for etab in etablissements:
        cursor.execute(
            "INSERT INTO etablissements (id_etablissement, id_region, nom_etablissement,status) VALUES (%s, %s, %s,%s)",
            (etab["id_etablissement"], etab["id_region"], etab["nom_etablissement"],etab["status"])
        )

def insert_enseignants(cursor, enseignants):
    for enseignant in enseignants:
        cursor.execute(
            "INSERT INTO enseignants (id_enseignant, nom_enseignant, email, domaine,sexe) VALUES (%s, %s, %s, %s,%s)",
            (enseignant["id_enseignant"], enseignant["nom_enseignant"], enseignant["email"], enseignant["domaine"],enseignant["sexe"])
        )
def insert_matieres(cursor, matieres):
    for matiere in matieres:
        cursor.execute(
            "INSERT INTO matieres (id_matiere, nom_matiere) VALUES (%s, %s)",
            (matiere["id_matiere"], matiere["nom_matiere"])
        )

def insert_eleves(cursor, eleves):
    for eleve in eleves:
        cursor.execute(
            "INSERT INTO eleves (id_eleve, nom_eleve, prenom_eleve, date_naissance, sexe, redouble, id_etablissement) VALUES ( %s, %s, %s, %s, %s, %s, %s)",
            (
                eleve["id_eleve"], eleve["nom_eleve"], eleve["prenom_eleve"],
                eleve["date_naissance"], eleve["sexe"], eleve["redouble"],
                 eleve["id_etablissement"]
            )
        )

def insert_cours(cursor, cours):
    for cour in cours:
        cursor.execute(
            "INSERT INTO cours (id_cours, niveau, duree, id_matiere, id_enseignant) VALUES (%s, %s, %s, %s, %s)",
            (cour["id_cours"], cour["niveau"], cour["duree"], cour["id_matiere"], cour["id_enseignant"])
        )

def insert_presences(cursor, presences):
    for p in presences:
        cursor.execute(
            "INSERT INTO presence (id_presence, id_eleve, id_cours, date_cours, present) VALUES (%s, %s, %s, %s, %s)",
            (p["id_presence"], p["id_eleve"], p["id_cours"], p["date_cours"], p["present"])
        )

def insert_notes(cursor, notes):
    for note in notes:
        cursor.execute(
            "INSERT INTO noter (id_note, id_eleve, id_matiere, annee, type, note) VALUES (%s, %s, %s, %s, %s, %s)",
            (
                note["id_note"], note["id_eleve"], note["id_matiere"],
                note["annee"], note["type"], note["note"]
            )
        )

if __name__ == "__main__":
    # Générer les ID nécessaires pour les relations
    regions = REGIONS_SN
    etablissements = ETABLISSEMENTS
    enseignants = generate_enseignants(40)
    matieres = generate_matieres()
    eleves = generate_eleves()
    cours = generate_cours(10, matieres,enseignants)
    presence = generate_presences(eleves,cours)
    notes = generate_notes(eleves,matieres,[2020,2021,2022,2023,2024])
    conn = connect(database="education",port=3308,user="root",password="1234",host="localhost",cursorclass=DictCursor)

    try:
        with conn.cursor() as cursor:
            insert_regions(cursor, REGIONS_SN)
            insert_etablissements(cursor, ETABLISSEMENTS)
            insert_enseignants(cursor, enseignants)
            insert_matieres(cursor, matieres)
            insert_eleves(cursor, eleves)
            insert_cours(cursor, cours)
            insert_presences(cursor, presence)
            insert_notes(cursor, notes)
        conn.commit()
        print("Toutes les données ont été insérées avec succès.")   
    except Exception as e:
        conn.rollback()
        print("Erreur lors de l'insertion :", e)
    finally:
        conn.close()

