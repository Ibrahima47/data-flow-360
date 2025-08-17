import requests
from hdfs import InsecureClient
import csv

def recuperer_donnees(endpoint, token):

    donnees =  requests.get(f"{url}/{endpoint}", headers={"Authorization":f"Bearer {token}"})

    return donnees.json()

def stocke_data_datalake(client, chemin, contenue):
    if not contenue:
        print(f"[WARN] Aucune donnée à écrire dans {chemin}")
        return

    try:
        with client.write(chemin, overwrite=True, encoding="utf-8") as writer:
            writer_csv = csv.DictWriter(writer, fieldnames=contenue[0].keys())
            writer_csv.writeheader()
            writer_csv.writerows(contenue)
        print(f"[INFO] Données écrites dans {chemin}")
    except Exception as e:
        print(f"[ERROR] Erreur écriture HDFS : {e}")

if __name__=="__main__":
    url = "http://localhost:8000"

    client = InsecureClient("http://localhost:9870", user = "hadoop")

    reponse_login = requests.post(f"{url}/utilisateurs/login",params={"username":"Ibrahima_47", "password":"#Passer123"})
    
    token = reponse_login.json()["access_token"]

    # cours = recuperer_donnees("cours",token)
    # eleves = recuperer_donnees("eleves",token)
    # etablissement = recuperer_donnees("etablissements",token)
    # presence = recuperer_donnees("presence",token)
    # matiere = recuperer_donnees("matiere",token)
    # enseignant = recuperer_donnees("enseignats",token)
    # note = recuperer_donnees("notes",token)
    # region = recuperer_donnees("regions",token)

    endpoints = ["eleves", "enseignants", "notes", "presences", "matiere", "regions", "etablissements", "cours"]
    data_map = {}

    for endpoint in endpoints:
        data_map[endpoint] = recuperer_donnees(endpoint, token)

    chemin_principal = "/data/datalake/API_thierno/"

    for key, data in data_map.items():
        stocke_data_datalake(client, chemin_principal + f"{key}_api.csv", data)

    print("[INFO] Terminé.")