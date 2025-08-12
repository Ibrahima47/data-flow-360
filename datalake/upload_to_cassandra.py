import os
import csv
import uuid
from datetime import datetime
from cassandra.cluster import Cluster

def deviner_type(valeurs):
    """
    Devine le type Cassandra d'une colonne à partir d'un échantillon de valeurs.
    Types gérés : int, boolean, date, text (par défaut).
    """
    for val in valeurs:
        if val == '' or val is None:
            continue
        # booléen
        if val.lower() in ('true', 'false'):
            return 'boolean'
        # int
        try:
            int(val)
            continue
        except:
            pass
        # date
        try:
            datetime.strptime(val, '%Y-%m-%d')
            return 'date'
        except:
            pass
        # sinon texte
        return 'text'
    return 'text'

def creer_table_dyn(session, keyspace, table_name, colonnes_types):
    """
    Crée une table Cassandra avec colonnes et types donnés.
    La première colonne est int PRIMARY KEY.
    """
    cle_primaire = list(colonnes_types.keys())[0]
    
    # Forcer le type de la première colonne en int PRIMARY KEY
    colonnes_types[cle_primaire] = "int"
    
    colonnes_str = []
    for nom, typ in colonnes_types.items():
        if nom == cle_primaire:
            colonnes_str.append(f"{nom} {typ} PRIMARY KEY")
        else:
            colonnes_str.append(f"{nom} {typ}")
    colonnes_str = ", ".join(colonnes_str)
    
    cql = f"""
    CREATE TABLE IF NOT EXISTS {keyspace}.{table_name} (
        {colonnes_str}
    )
    """
    session.execute(cql)
    print(f"Table {table_name} créée avec colonnes {colonnes_types}")

def inserer_csv(session, keyspace, table_name, colonnes, fichier_csv):
    """
    Insère les données du CSV dans la table Cassandra.
    """
    placeholders = ", ".join(["?"] * len(colonnes))
    cols_str = ", ".join(colonnes)
    query = f"INSERT INTO {keyspace}.{table_name} ({cols_str}) VALUES ({placeholders})"
    prepared = session.prepare(query)

    with open(fichier_csv, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            valeurs = []
            for col in colonnes:
                val = row[col]
                # conversion simple (ajoute si nécessaire)
                if val == '':
                    val = None
                elif val.lower() in ('true', 'false'):
                    val = val.lower() == 'true'
                elif val.isdigit():
                    val = int(val)
                elif '-' in val and len(val) == 10:
                    try:
                        val = datetime.strptime(val, "%Y-%m-%d").date()
                    except:
                        pass
                valeurs.append(val)
            session.execute(prepared, valeurs)

def traiter_repertoire(session, keyspace, dossier):
    """
    Lit tous les CSV du dossier, crée une table par CSV et insère les données.
    """
    fichiers = [f for f in os.listdir(dossier) if f.endswith('.csv')]
    for fichier in fichiers:
        path = os.path.join(dossier, fichier)
        table_name = os.path.splitext(fichier)[0]
        print(f"\nTraitement du fichier {fichier} pour créer la table {table_name} ...")

        # Lire les colonnes et un échantillon de valeurs pour deviner les types
        with open(path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            colonnes = reader.fieldnames
            echantillon = {col: [] for col in colonnes}
            for i, row in enumerate(reader):
                if i >= 10:
                    break
                for col in colonnes:
                    echantillon[col].append(row[col])

        colonnes_types = {}
        for col in colonnes:
            colonnes_types[col] = deviner_type(echantillon[col])

        # Créer la table dans Cassandra
        creer_table_dyn(session, keyspace, table_name, colonnes_types)

        # Insérer les données du CSV
        inserer_csv(session, keyspace, table_name, colonnes, path)

        print(f"Table {table_name} créée et données insérées.")

# Exemple d'utilisation :
if __name__ == "__main__":
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('education')  # keyspace existant

    dossier_csv = '/home/dev47/Bureau/data_flow_360/data_sources/generators/Ibrahima47/scraped_data'  # à modifier selon ton environnement
    traiter_repertoire(session, 'education', dossier_csv)
