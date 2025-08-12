import uuid
from cassandra.cluster import Cluster

def creer_table(session, bd, nom_table, *attributs):
    session.set_keyspace(bd)
    
    # Définition des colonnes 
    # colonnes = ["id UUID PRIMARY KEY"]
    colonnes = []
    for attr in attributs:
        if isinstance(attr, tuple) and len(attr) == 2:
            colonnes.append(f"{attr[0]} {attr[1]}")
        else:
            raise ValueError
        
    requete = ", \n".join(colonnes)
    
    session.execute(f"""
        CREATE TABLE IF NOT EXISTS {nom_table} (
            {requete}
        )
    """)

# ===============================
# CRÉATION DES TABLES AVEC RELATIONS (selon le schéma ER)
# ===============================

def creer_tables_systeme_educatif(session, bd):
    """Crée toutes les tables du système éducatif sénégalais avec les relations"""
    
    # Table Region (entité indépendante)
    creer_table(session, bd, "region",
        ("id_region","INT PRIMARY KEY"),
        ("nom_region", "TEXT"),
        ("nom_ville", "TEXT")
    )
    
    # Table Etablissement (lié à Region)
    creer_table(session, bd, "etablissement",
        ("id_etablissement","INT PRIMARY KEY"),
        ("nom_etablissement", "TEXT"),
        ("type", "TEXT"),
        ("statut", "TEXT"),
        ("id_region", "UUID")  # Clé étrangère vers Region
    )
    
    # Table Enseignant (peut être lié à Etablissement)
    creer_table(session, bd, "enseignant",
        ("id_enseignant","INT PRIMARY KEY"),
        ("nom_enseignant", "TEXT"),
        ("domaine", "TEXT"),
        ("email", "TEXT"),
        ("sexe", "TEXT"),
        ("id_etablissement", "UUID")  # Clé étrangère vers Etablissement
    )
    
    # Table Eleve (lié à Etablissement)
    creer_table(session, bd, "eleve",
        ("id_eleve","INT PRIMARY KEY"),
        ("nom_eleve", "TEXT"),
        ("prenom_eleve", "TEXT"),
        ("date_naissance", "DATE"),
        ("sexe", "TEXT"),
        ("redouble", "BOOLEAN"),
        ("adresse", "TEXT"),
        ("id_etablissement", "UUID")  # Clé étrangère vers Etablissement
    )
    
    # Table Matiere (entité indépendante)
    creer_table(session, bd, "matiere",
        ("id_matiere","INT PRIMARY KEY"),
        ("nom_matiere", "TEXT")
    )
    
    # Table Cours (lié à Matiere et Enseignant)
    creer_table(session, bd, "cours",
        ("id_cours","INT PRIMARY KEY"),
        ("niveau", "TEXT"),
        ("duree", "INT"),
        ("id_matiere", "UUID"),  # Clé étrangère vers Matiere
        ("id_enseignant", "UUID")  # Clé étrangère vers Enseignant
    )
    
    # Table Noter (relation entre Eleve et Matiere)
    creer_table(session, bd, "noter",
        ("id_note","INT PRIMARY KEY"),
        ("note", "DECIMAL"),
        ("type_note", "TEXT"),
        ("id_eleve", "UUID"),  # Clé étrangère vers Eleve
        ("id_matiere", "UUID")  # Clé étrangère vers Matiere
    )
    
    # Table Presence (lié à Eleve et Cours)
    creer_table(session, bd, "presence",
        ("id_presence","INT PRIMARY KEY"),
        ("is_present", "BOOLEAN"),
        ("date_presence", "DATE"),
        ("id_eleve", "UUID"),  # Clé étrangère vers Eleve
        ("id_cours", "UUID")  # Clé étrangère vers Cours
    )
    
    # Table Enseignement (relation entre Enseignant et Matiere/Cours)
    creer_table(session, bd, "enseignement",
        ("id_enseignement","INT PRIMARY KEY"),
        ("annee", "TEXT"),
        ("id_enseignant", "UUID"),  # Clé étrangère vers Enseignant
        ("id_matiere", "UUID")  # Clé étrangère vers Matiere
    )

# ===============================
# FONCTIONS D'INSERTION AVEC RELATIONS
# ===============================

def inserer_region(session, bd, region_id, nom_region, nom_ville):
    """Insère une région dans la table region"""
    session.set_keyspace(bd)
    
    
    session.execute("""
        INSERT INTO region (id, nom_region, nom_ville)
        VALUES (%s, %s, %s)
    """, (region_id, nom_region, nom_ville))
    
    return region_id

def inserer_etablissement(session, bd, etablissement_id, nom_etablissement, type_etab, statut, id_region):
    """Insère un établissement dans la table etablissement"""
    session.set_keyspace(bd)
    
    
    session.execute("""
        INSERT INTO etablissement (id, nom_etablissement, type, statut, id_region)
        VALUES (%s, %s, %s, %s, %s)
    """, (etablissement_id, nom_etablissement, type_etab, statut, id_region))
    
    return etablissement_id

def inserer_enseignant(session, bd, enseignant_id, nom_enseignant, domaine, email, sexe, id_etablissement):
    """Insère un enseignant dans la table enseignant"""
    session.set_keyspace(bd)
    
    
    session.execute("""
        INSERT INTO enseignant (id, nom_enseignant, domaine, email, sexe, id_etablissement)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (enseignant_id, nom_enseignant, domaine, email, sexe, id_etablissement))
    
    return enseignant_id

def inserer_eleve(session, bd, eleve_id, nom_eleve, prenom_eleve, date_naissance, sexe, redouble, adresse, id_etablissement):
    """Insère un élève dans la table eleve"""
    session.set_keyspace(bd)
    
    
    session.execute("""
        INSERT INTO eleve (id, nom_eleve, prenom_eleve, date_naissance, sexe, redouble, adresse, id_etablissement)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (eleve_id, nom_eleve, prenom_eleve, date_naissance, sexe, redouble, adresse, id_etablissement))
    
    return eleve_id

def inserer_matiere(session, bd, matiere_id, nom_matiere):
    """Insère une matière dans la table matiere"""
    session.set_keyspace(bd)
    
    
    session.execute("""
        INSERT INTO matiere (id, nom_matiere)
        VALUES (%s, %s)
    """, (matiere_id, nom_matiere))
    
    return matiere_id

def inserer_cours(session, bd, cours_id, niveau, duree, id_matiere, id_enseignant):
    """Insère un cours dans la table cours"""
    session.set_keyspace(bd)
    
    
    session.execute("""
        INSERT INTO cours (id, niveau, duree, id_matiere, id_enseignant)
        VALUES (%s, %s, %s, %s, %s)
    """, (cours_id, niveau, duree, id_matiere, id_enseignant))
    
    return cours_id

def inserer_noter(session, bd, noter_id, note, type_note, id_eleve, id_matiere):
    """Insère une note dans la table noter"""
    session.set_keyspace(bd)
    
    
    session.execute("""
        INSERT INTO noter (id, note, type_note, id_eleve, id_matiere)
        VALUES (%s, %s, %s, %s, %s)
    """, (noter_id, note, type_note, id_eleve, id_matiere))
    
    return noter_id

def inserer_presence(session, bd, presence_id, is_present, date_presence, id_eleve, id_cours):
    """Insère une présence dans la table presence"""
    session.set_keyspace(bd)
    
    
    session.execute("""
        INSERT INTO presence (id, is_present, date_presence, id_eleve, id_cours)
        VALUES (%s, %s, %s, %s, %s)
    """, (presence_id, is_present, date_presence, id_eleve, id_cours))
    
    return presence_id

def inserer_enseignement(session, bd, enseignement_id, annee, id_enseignant, id_matiere):
    """Insère un enseignement dans la table enseignement"""
    session.set_keyspace(bd)
    
    
    session.execute("""
        INSERT INTO enseignement (id, annee, id_enseignant, id_matiere)
        VALUES (%s, %s, %s, %s)
    """, (enseignement_id, annee, id_enseignant, id_matiere))
    
    return enseignement_id

# ===============================
# EXEMPLE D'UTILISATION AVEC RELATIONS
# ===============================

def exemple_utilisation_avec_relations():
    """Exemple d'utilisation en respectant les relations du schéma ER"""
    
    # Connexion à Cassandra
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    
    # Nom de la base de données
    bd = "systeme_educatif_senegal"
    
    # Créer le keyspace si nécessaire
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {bd}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
    """)
    
    # Créer toutes les tables
    creer_tables_systeme_educatif(session, bd)
    
    # Insertion en respectant l'ordre des dépendances
    from datetime import date
    
    # 1. Insérer une région (indépendante)
    region_id = inserer_region(session, bd, "Dakar", "Dakar")
    print(f"Région créée avec ID: {region_id}")
    
    # 2. Insérer un établissement (dépend de région)
    etablissement_id = inserer_etablissement(session, bd, 
        "Institut Cheikh Anta Diop", "Lycée", "Public", region_id)
    print(f"Établissement créé avec ID: {etablissement_id}")
    
    # 3. Insérer un enseignant (dépend d'établissement)
    enseignant_id = inserer_enseignant(session, bd,
        "Mariam Diop", "Sciences Physiques", "mariam.diop@college.sn", "F", etablissement_id)
    print(f"Enseignant créé avec ID: {enseignant_id}")
    
    # 4. Insérer un élève (dépend d'établissement)
    eleve_id = inserer_eleve(session, bd,
        "Seck", "Khady", date(2009, 4, 9), "F", False, "Rue 10 x 15, Médina, Dakar", etablissement_id)
    print(f"Élève créé avec ID: {eleve_id}")
    
    # 5. Insérer une matière (indépendante)
    matiere_id = inserer_matiere(session, bd, "Sciences Physiques")
    print(f"Matière créée avec ID: {matiere_id}")
    
    # 6. Insérer un cours (dépend de matière et enseignant)
    cours_id = inserer_cours(session, bd, "Technique", 55, matiere_id, enseignant_id)
    print(f"Cours créé avec ID: {cours_id}")
    
    # 7. Insérer une note (dépend d'élève et matière)
    noter_id = inserer_noter(session, bd, 15.0, "Devoir", eleve_id, matiere_id)
    print(f"Note créée avec ID: {noter_id}")
    
    # 8. Insérer une présence (dépend d'élève et cours)
    presence_id = inserer_presence(session, bd, True, date(2024, 9, 24), eleve_id, cours_id)
    print(f"Présence créée avec ID: {presence_id}")
    
    # 9. Insérer un enseignement (dépend d'enseignant et matière)
    enseignement_id = inserer_enseignement(session, bd, "2024-2025", enseignant_id, matiere_id)
    print(f"Enseignement créé avec ID: {enseignement_id}")
    
    cluster.shutdown()
    print("\n✅ Toutes les tables ont été créées avec les relations!")
    print("✅ Exemple de données insérés en respectant les dépendances!")

# ===============================
# RELATIONS IDENTIFIÉES DANS LE SCHÉMA
# ===============================
"""
RELATIONS DU SCHÉMA ER:
- Region -> Etablissement (1:N)
- Etablissement -> Eleve (1:N)
- Etablissement -> Enseignant (1:N) 
- Enseignant -> Cours (1:N)
- Matiere -> Cours (1:N)
- Eleve -> Noter (1:N)
- Matiere -> Noter (1:N)
- Eleve -> Presence (1:N)
- Cours -> Presence (1:N)
- Enseignant -> Enseignement (1:N)
- Matiere -> Enseignement (1:N)
"""

# ===============================
# FONCTIONS D'EXTRACTION/LECTURE DES DONNÉES
# ===============================

def extraire_toutes_regions(session, bd):
    """Extrait toutes les régions"""
    session.set_keyspace(bd)
    
    rows = session.execute("SELECT * FROM region")
    regions = []
    for row in rows:
        regions.append({
            'id': row.id,
            'nom_region': row.nom_region,
            'nom_ville': row.nom_ville
        })
    return regions

def extraire_region_par_id(session, bd, region_id):
    """Extrait une région par son ID"""
    session.set_keyspace(bd)
    
    row = session.execute("SELECT * FROM region WHERE id = %s", (region_id,)).one()
    if row:
        return {
            'id': row.id,
            'nom_region': row.nom_region,
            'nom_ville': row.nom_ville
        }
    return None

def extraire_tous_etablissements(session, bd):
    """Extrait tous les établissements"""
    session.set_keyspace(bd)
    
    rows = session.execute("SELECT * FROM etablissement")
    etablissements = []
    for row in rows:
        etablissements.append({
            'id': row.id,
            'nom_etablissement': row.nom_etablissement,
            'type': row.type,
            'statut': row.statut,
            'id_region': row.id_region
        })
    return etablissements

def extraire_etablissement_par_id(session, bd, etablissement_id):
    """Extrait un établissement par son ID"""
    session.set_keyspace(bd)
    
    row = session.execute("SELECT * FROM etablissement WHERE id = %s", (etablissement_id,)).one()
    if row:
        return {
            'id': row.id,
            'nom_etablissement': row.nom_etablissement,
            'type': row.type,
            'statut': row.statut,
            'id_region': row.id_region
        }
    return None

def extraire_etablissements_par_region(session, bd, region_id):
    """Extrait tous les établissements d'une région (nécessite un index)"""
    session.set_keyspace(bd)
    
    # Note: Cette requête nécessite un index sur id_region
    try:
        rows = session.execute("SELECT * FROM etablissement WHERE id_region = %s ALLOW FILTERING", (region_id,))
        etablissements = []
        for row in rows:
            etablissements.append({
                'id': row.id,
                'nom_etablissement': row.nom_etablissement,
                'type': row.type,
                'statut': row.statut,
                'id_region': row.id_region
            })
        return etablissements
    except Exception as e:
        print(f"Erreur: {e}. Créez un index sur id_region si nécessaire.")
        return []

def extraire_tous_enseignants(session, bd):
    """Extrait tous les enseignants"""
    session.set_keyspace(bd)
    
    rows = session.execute("SELECT * FROM enseignant")
    enseignants = []
    for row in rows:
        enseignants.append({
            'id': row.id,
            'nom_enseignant': row.nom_enseignant,
            'domaine': row.domaine,
            'email': row.email,
            'sexe': row.sexe,
            'id_etablissement': row.id_etablissement
        })
    return enseignants

def extraire_enseignant_par_id(session, bd, enseignant_id):
    """Extrait un enseignant par son ID"""
    session.set_keyspace(bd)
    
    row = session.execute("SELECT * FROM enseignant WHERE id = %s", (enseignant_id,)).one()
    if row:
        return {
            'id': row.id,
            'nom_enseignant': row.nom_enseignant,
            'domaine': row.domaine,
            'email': row.email,
            'sexe': row.sexe,
            'id_etablissement': row.id_etablissement
        }
    return None

def extraire_enseignants_par_etablissement(session, bd, etablissement_id):
    """Extrait tous les enseignants d'un établissement"""
    session.set_keyspace(bd)
    
    try:
        rows = session.execute("SELECT * FROM enseignant WHERE id_etablissement = %s ALLOW FILTERING", (etablissement_id,))
        enseignants = []
        for row in rows:
            enseignants.append({
                'id': row.id,
                'nom_enseignant': row.nom_enseignant,
                'domaine': row.domaine,
                'email': row.email,
                'sexe': row.sexe,
                'id_etablissement': row.id_etablissement
            })
        return enseignants
    except Exception as e:
        print(f"Erreur: {e}")
        return []

def extraire_tous_eleves(session, bd):
    """Extrait tous les élèves"""
    session.set_keyspace(bd)
    
    rows = session.execute("SELECT * FROM eleve")
    eleves = []
    for row in rows:
        eleves.append({
            'id': row.id,
            'nom_eleve': row.nom_eleve,
            'prenom_eleve': row.prenom_eleve,
            'date_naissance': row.date_naissance,
            'sexe': row.sexe,
            'redouble': row.redouble,
            'adresse': row.adresse,
            'id_etablissement': row.id_etablissement
        })
    return eleves

def extraire_eleve_par_id(session, bd, eleve_id):
    """Extrait un élève par son ID"""
    session.set_keyspace(bd)
    
    row = session.execute("SELECT * FROM eleve WHERE id = %s", (eleve_id,)).one()
    if row:
        return {
            'id': row.id,
            'nom_eleve': row.nom_eleve,
            'prenom_eleve': row.prenom_eleve,
            'date_naissance': row.date_naissance,
            'sexe': row.sexe,
            'redouble': row.redouble,
            'adresse': row.adresse,
            'id_etablissement': row.id_etablissement
        }
    return None

def extraire_eleves_par_etablissement(session, bd, etablissement_id):
    """Extrait tous les élèves d'un établissement"""
    session.set_keyspace(bd)
    
    try:
        rows = session.execute("SELECT * FROM eleve WHERE id_etablissement = %s ALLOW FILTERING", (etablissement_id,))
        eleves = []
        for row in rows:
            eleves.append({
                'id': row.id,
                'nom_eleve': row.nom_eleve,
                'prenom_eleve': row.prenom_eleve,
                'date_naissance': row.date_naissance,
                'sexe': row.sexe,
                'redouble': row.redouble,
                'adresse': row.adresse,
                'id_etablissement': row.id_etablissement
            })
        return eleves
    except Exception as e:
        print(f"Erreur: {e}")
        return []

def extraire_toutes_matieres(session, bd):
    """Extrait toutes les matières"""
    session.set_keyspace(bd)
    
    rows = session.execute("SELECT * FROM matiere")
    matieres = []
    for row in rows:
        matieres.append({
            'id': row.id,
            'nom_matiere': row.nom_matiere
        })
    return matieres

def extraire_matiere_par_id(session, bd, matiere_id):
    """Extrait une matière par son ID"""
    session.set_keyspace(bd)
    
    row = session.execute("SELECT * FROM matiere WHERE id = %s", (matiere_id,)).one()
    if row:
        return {
            'id': row.id,
            'nom_matiere': row.nom_matiere
        }
    return None

def extraire_tous_cours(session, bd):
    """Extrait tous les cours"""
    session.set_keyspace(bd)
    
    rows = session.execute("SELECT * FROM cours")
    cours = []
    for row in rows:
        cours.append({
            'id': row.id,
            'niveau': row.niveau,
            'duree': row.duree,
            'id_matiere': row.id_matiere,
            'id_enseignant': row.id_enseignant
        })
    return cours

def extraire_cours_par_id(session, bd, cours_id):
    """Extrait un cours par son ID"""
    session.set_keyspace(bd)
    
    row = session.execute("SELECT * FROM cours WHERE id = %s", (cours_id,)).one()
    if row:
        return {
            'id': row.id,
            'niveau': row.niveau,
            'duree': row.duree,
            'id_matiere': row.id_matiere,
            'id_enseignant': row.id_enseignant
        }
    return None

def extraire_cours_par_enseignant(session, bd, enseignant_id):
    """Extrait tous les cours d'un enseignant"""
    session.set_keyspace(bd)
    
    try:
        rows = session.execute("SELECT * FROM cours WHERE id_enseignant = %s ALLOW FILTERING", (enseignant_id,))
        cours = []
        for row in rows:
            cours.append({
                'id': row.id,
                'niveau': row.niveau,
                'duree': row.duree,
                'id_matiere': row.id_matiere,
                'id_enseignant': row.id_enseignant
            })
        return cours
    except Exception as e:
        print(f"Erreur: {e}")
        return []

def extraire_toutes_notes(session, bd):
    """Extrait toutes les notes"""
    session.set_keyspace(bd)
    
    rows = session.execute("SELECT * FROM noter")
    notes = []
    for row in rows:
        notes.append({
            'id': row.id,
            'note': row.note,
            'type_note': row.type_note,
            'id_eleve': row.id_eleve,
            'id_matiere': row.id_matiere
        })
    return notes

def extraire_note_par_id(session, bd, note_id):
    """Extrait une note par son ID"""
    session.set_keyspace(bd)
    
    row = session.execute("SELECT * FROM noter WHERE id = %s", (note_id,)).one()
    if row:
        return {
            'id': row.id,
            'note': row.note,
            'type_note': row.type_note,
            'id_eleve': row.id_eleve,
            'id_matiere': row.id_matiere
        }
    return None

def extraire_notes_par_eleve(session, bd, eleve_id):
    """Extrait toutes les notes d'un élève"""
    session.set_keyspace(bd)
    
    try:
        rows = session.execute("SELECT * FROM noter WHERE id_eleve = %s ALLOW FILTERING", (eleve_id,))
        notes = []
        for row in rows:
            notes.append({
                'id': row.id,
                'note': row.note,
                'type_note': row.type_note,
                'id_eleve': row.id_eleve,
                'id_matiere': row.id_matiere
            })
        return notes
    except Exception as e:
        print(f"Erreur: {e}")
        return []

def extraire_toutes_presences(session, bd):
    """Extrait toutes les présences"""
    session.set_keyspace(bd)
    
    rows = session.execute("SELECT * FROM presence")
    presences = []
    for row in rows:
        presences.append({
            'id': row.id,
            'is_present': row.is_present,
            'date_presence': row.date_presence,
            'id_eleve': row.id_eleve,
            'id_cours': row.id_cours
        })
    return presences

def extraire_presence_par_id(session, bd, presence_id):
    """Extrait une présence par son ID"""
    session.set_keyspace(bd)
    
    row = session.execute("SELECT * FROM presence WHERE id = %s", (presence_id,)).one()
    if row:
        return {
            'id': row.id,
            'is_present': row.is_present,
            'date_presence': row.date_presence,
            'id_eleve': row.id_eleve,
            'id_cours': row.id_cours
        }
    return None

def extraire_presences_par_eleve(session, bd, eleve_id):
    """Extrait toutes les présences d'un élève"""
    session.set_keyspace(bd)
    
    try:
        rows = session.execute("SELECT * FROM presence WHERE id_eleve = %s ALLOW FILTERING", (eleve_id,))
        presences = []
        for row in rows:
            presences.append({
                'id': row.id,
                'is_present': row.is_present,
                'date_presence': row.date_presence,
                'id_eleve': row.id_eleve,
                'id_cours': row.id_cours
            })
        return presences
    except Exception as e:
        print(f"Erreur: {e}")
        return []

def extraire_tous_enseignements(session, bd):
    """Extrait tous les enseignements"""
    session.set_keyspace(bd)
    
    rows = session.execute("SELECT * FROM enseignement")
    enseignements = []
    for row in rows:
        enseignements.append({
            'id': row.id,
            'annee': row.annee,
            'id_enseignant': row.id_enseignant,
            'id_matiere': row.id_matiere
        })
    return enseignements

def extraire_enseignement_par_id(session, bd, enseignement_id):
    """Extrait un enseignement par son ID"""
    session.set_keyspace(bd)
    
    row = session.execute("SELECT * FROM enseignement WHERE id = %s", (enseignement_id,)).one()
    if row:
        return {
            'id': row.id,
            'annee': row.annee,
            'id_enseignant': row.id_enseignant,
            'id_matiere': row.id_matiere
        }
    return None

def extraire_enseignements_par_enseignant(session, bd, enseignant_id):
    """Extrait tous les enseignements d'un enseignant"""
    session.set_keyspace(bd)
    
    try:
        rows = session.execute("SELECT * FROM enseignement WHERE id_enseignant = %s ALLOW FILTERING", (enseignant_id,))
        enseignements = []
        for row in rows:
            enseignements.append({
                'id': row.id,
                'annee': row.annee,
                'id_enseignant': row.id_enseignant,
                'id_matiere': row.id_matiere
            })
        return enseignements
    except Exception as e:
        print(f"Erreur: {e}")
        return []

# ===============================
# FONCTION D'EXEMPLE POUR TESTER LES EXTRACTIONS
# ===============================

def exemple_extractions():
    """Exemple d'utilisation des fonctions d'extraction - Affiche ET retourne les données"""
    
    # Connexion à Cassandra
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    bd = "systeme_educatif_senegal"
    
    print("=== EXEMPLES D'EXTRACTION DE DONNÉES ===\n")
    
    # Dictionnaire pour stocker toutes les données extraites
    donnees_extraites = {}
    
    # Extraire toutes les régions
    regions = extraire_toutes_regions(session, bd)
    donnees_extraites['regions'] = regions
    print(f"📍 Nombre de régions: {len(regions)}")
    for region in regions[:2]:  # Afficher les 2 premières
        print(f"   - {region['nom_region']} ({region['nom_ville']})")
    
    # Extraire tous les établissements
    etablissements = extraire_tous_etablissements(session, bd)
    donnees_extraites['etablissements'] = etablissements
    print(f"\n🏫 Nombre d'établissements: {len(etablissements)}")
    for etab in etablissements[:2]:  # Afficher les 2 premiers
        print(f"   - {etab['nom_etablissement']} ({etab['type']}, {etab['statut']})")
    
    # Extraire tous les enseignants
    enseignants = extraire_tous_enseignants(session, bd)
    donnees_extraites['enseignants'] = enseignants
    print(f"\n👨‍🏫 Nombre d'enseignants: {len(enseignants)}")
    for ens in enseignants[:2]:  # Afficher les 2 premiers
        print(f"   - {ens['nom_enseignant']} ({ens['domaine']})")
    
    # Extraire tous les élèves
    eleves = extraire_tous_eleves(session, bd)
    donnees_extraites['eleves'] = eleves
    print(f"\n👨‍🎓 Nombre d'élèves: {len(eleves)}")
    for eleve in eleves[:2]:  # Afficher les 2 premiers
        print(f"   - {eleve['nom_eleve']} {eleve['prenom_eleve']} ({eleve['sexe']})")
    
    # Extraire toutes les matières
    matieres = extraire_toutes_matieres(session, bd)
    donnees_extraites['matieres'] = matieres
    print(f"\n📚 Nombre de matières: {len(matieres)}")
    for matiere in matieres[:2]:  # Afficher les 2 premières
        print(f"   - {matiere['nom_matiere']}")
    
    # Extraire tous les cours
    cours = extraire_tous_cours(session, bd)
    donnees_extraites['cours'] = cours
    print(f"\n📖 Nombre de cours: {len(cours)}")
    for c in cours[:2]:  # Afficher les 2 premiers
        print(f"   - Niveau: {c['niveau']}, Durée: {c['duree']}min")
    
    # Extraire toutes les notes
    notes = extraire_toutes_notes(session, bd)
    donnees_extraites['notes'] = notes
    print(f"\n📝 Nombre de notes: {len(notes)}")
    for note in notes[:2]:  # Afficher les 2 premières
        print(f"   - Note: {note['note']}, Type: {note['type_note']}")
    
    # Extraire toutes les présences
    presences = extraire_toutes_presences(session, bd)
    donnees_extraites['presences'] = presences
    print(f"\n✅ Nombre de présences: {len(presences)}")
    for presence in presences[:2]:  # Afficher les 2 premières
        status = "Présent" if presence['is_present'] else "Absent"
        print(f"   - {status} le {presence['date_presence']}")
    
    # Extraire tous les enseignements
    enseignements = extraire_tous_enseignements(session, bd)
    donnees_extraites['enseignements'] = enseignements
    print(f"\n🎓 Nombre d'enseignements: {len(enseignements)}")
    for ens in enseignements[:2]:  # Afficher les 2 premiers
        print(f"   - Année: {ens['annee']}")
    
    cluster.shutdown()
    print("\n✅ Extraction des données terminée!")
    print(f"\n📊 Résumé des données extraites:")
    print(f"   - Régions: {len(donnees_extraites.get('regions', []))}")
    print(f"   - Établissements: {len(donnees_extraites.get('etablissements', []))}")
    print(f"   - Enseignants: {len(donnees_extraites.get('enseignants', []))}")
    print(f"   - Élèves: {len(donnees_extraites.get('eleves', []))}")
    print(f"   - Matières: {len(donnees_extraites.get('matieres', []))}")
    print(f"   - Cours: {len(donnees_extraites.get('cours', []))}")
    print(f"   - Notes: {len(donnees_extraites.get('notes', []))}")
    print(f"   - Présences: {len(donnees_extraites.get('presences', []))}")
    print(f"   - Enseignements: {len(donnees_extraites.get('enseignements', []))}")
    
    # Retourner toutes les données extraites
    return donnees_extraites

def extraire_toutes_donnees_systeme(session, bd, affichage=True):
    """
    Fonction complète pour extraire toutes les données du système
    
    Args:
        session: Session Cassandra
        bd: Nom de la base de données
        affichage: Boolean - Afficher les résultats ou non
    
    Returns:
        dict: Dictionnaire contenant toutes les données extraites
    """
    session.set_keyspace(bd)
    
    donnees_completes = {}
    
    if affichage:
        print("=== EXTRACTION COMPLÈTE DU SYSTÈME ÉDUCATIF ===\n")
    
    # Extraire toutes les données de chaque table
    tables_et_fonctions = [
        ('regions', extraire_toutes_regions),
        ('etablissements', extraire_tous_etablissements),
        ('enseignants', extraire_tous_enseignants),
        ('eleves', extraire_tous_eleves),
        ('matieres', extraire_toutes_matieres),
        ('cours', extraire_tous_cours),
        ('notes', extraire_toutes_notes),
        ('presences', extraire_toutes_presences),
        ('enseignements', extraire_tous_enseignements)
    ]
    
    for nom_table, fonction_extraction in tables_et_fonctions:
        try:
            donnees = fonction_extraction(session, bd)
            donnees_completes[nom_table] = donnees
            
            if affichage:
                print(f"✅ {nom_table.capitalize()}: {len(donnees)} enregistrements extraits")
                
        except Exception as e:
            if affichage:
                print(f"❌ Erreur lors de l'extraction de {nom_table}: {e}")
            donnees_completes[nom_table] = []
    
    if affichage:
        total_enregistrements = sum(len(donnees) for donnees in donnees_completes.values())
        print(f"\n📊 TOTAL: {total_enregistrements} enregistrements extraits de {len(donnees_completes)} tables")
    
    return donnees_completes

def extraire_donnees_avec_relations(session, bd, affichage=True):
    """
    Extrait les données en montrant les relations entre les entités
    
    Returns:
        dict: Données avec informations relationnelles
    """
    session.set_keyspace(bd)
    
    if affichage:
        print("=== EXTRACTION AVEC RELATIONS ===\n")
    
    # Extraire les données de base
    donnees = extraire_toutes_donnees_systeme(session, bd, affichage=False)
    
    # Analyser les relations
    relations_trouvees = {}
    
    # Analyser établissements par région
    if donnees['regions'] and donnees['etablissements']:
        relations_trouvees['etablissements_par_region'] = {}
        for region in donnees['regions']:
            etabs_region = [e for e in donnees['etablissements'] if e['id_region'] == region['id']]
            relations_trouvees['etablissements_par_region'][region['nom_region']] = len(etabs_region)
    
    # Analyser élèves par établissement
    if donnees['etablissements'] and donnees['eleves']:
        relations_trouvees['eleves_par_etablissement'] = {}
        for etab in donnees['etablissements']:
            eleves_etab = [e for e in donnees['eleves'] if e['id_etablissement'] == etab['id']]
            relations_trouvees['eleves_par_etablissement'][etab['nom_etablissement']] = len(eleves_etab)
    
    # Analyser enseignants par établissement
    if donnees['etablissements'] and donnees['enseignants']:
        relations_trouvees['enseignants_par_etablissement'] = {}
        for etab in donnees['etablissements']:
            ens_etab = [e for e in donnees['enseignants'] if e['id_etablissement'] == etab['id']]
            relations_trouvees['enseignants_par_etablissement'][etab['nom_etablissement']] = len(ens_etab)
    
    if affichage:
        print("🔗 ANALYSE DES RELATIONS:")
        if 'etablissements_par_region' in relations_trouvees:
            print("\n📍 Établissements par région:")
            for region, count in relations_trouvees['etablissements_par_region'].items():
                print(f"   - {region}: {count} établissements")
        
        if 'eleves_par_etablissement' in relations_trouvees:
            print("\n👨‍🎓 Élèves par établissement (top 3):")
            top_eleves = sorted(relations_trouvees['eleves_par_etablissement'].items(), 
                              key=lambda x: x[1], reverse=True)[:3]
            for etab, count in top_eleves:
                print(f"   - {etab}: {count} élèves")
    
    return {
        'donnees': donnees,
        'relations': relations_trouvees
    }

# Appeler la fonction d'exemple si ce script est exécuté directement
if __name__ == "__main__":
    # exemple_utilisation_avec_relations()  # Créer les données d'abord
    
    # Extraire et afficher toutes les données
    # donnees = exemple_extractions()  # Retourne maintenant les données
    # print(f"\n🔄 Les données ont été retournées dans la variable 'donnees'")
    # print(f"   Clés disponibles: {list(donnees.keys())}")
    
    # # Exemple d'utilisation des données retournées
    # if 'eleves' in donnees and donnees['eleves']:
    #     print(f"\n📋 Exemple d'accès aux données retournées:")
    #     print(f"   Premier élève: {donnees['eleves'][0]}")
    
    # # Utiliser la fonction complète d'extraction
    # print("\n" + "="*50)
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    
    # donnees_completes = extraire_toutes_donnees_systeme(
    #     session, "systeme_educatif_senegal", affichage=True
    # )
    
    # # Utiliser la fonction avec analyse des relations
    # print("\n" + "="*50)
    # donnees_avec_relations = extraire_donnees_avec_relations(
    #     session, "systeme_educatif_senegal", affichage=True
    # )
    creer_tables_systeme_educatif(session,"education")
    
    cluster.shutdown()