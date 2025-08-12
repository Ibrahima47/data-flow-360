from pymongo import MongoClient

# Connexion MongoDB
client = MongoClient("mongodb://root:root@localhost:27017/")
db = client["datasetMongo"]
etabs_collection = db["etablissements"]

# Nettoyage de la collection avant insertion
etabs_collection.delete_many({})

# Données des établissements (région → ville → établissements)
etablissements_data = {
    "Diourbel": {
        "Diourbel": [
            "LYCEE DE NDINDI",
            "LYCEE DE NDOULO",
            "LYCEE DE NGOHE",
            "LYCEE ENSEIGNEMENT GENERAL DE DIOURBEL",
            "LYCEE EX CEM CHEIKH M. MOUSTAPHA. MBACKE",
            "LYCEE SCIENTIFIQUE D'EXCELLENCE DE DIOURBEL",
            "LYCEE TAIBA MOUTOUFA"
        ],
        "Mbacké": [
            "LYCEE DE MBACKE",
            "LYCEE EX CEM KERE MBAYE",
            "LYCEE EX CEM MBACKE",
            "LYCEE EX CFA NDINDI",
            "LYCEE S.M.M.M EX CEM TAÏF"
        ],
        "Bambey": [
            "LYCEE BABA GARAGE",
            "LYCEE BAMBEY SERERES",
            "LYCEE DE BAMBEY",
            "LYCEE DE KEUR SAMBA KANE",
            "LYCEE EX CEM DE DANGALMA",
            "LYCEE EX CEM DE GATE",
            "LYCEE EX CEM DE LAMBAYE",
            "LYCEE EX CEM DE NDONDOL",
            "LYCEE EX CEM DE NGASCOPE",
            "LYCEE EX CEM DE NGOYE",
            "LYCEE EX CEM DE REFANE"
        ]
    },
    "Thiès": {
        "Mbour1": [
            "LYCEE BOUKHOU",
            "LYCÉE DE GUEREO",
            "LYCEE DE NGUEKOKH",
            "LYCEE DE TCHIKY",
            "LYCEE DE TENE TOUBAB",
            "LYCEE DEMBA DIOP",
            "LYCEE DIASS (EX CEM)",
            "LYCEE EX CEM CHEIKH AMADOU LAMINE DABO",
            "LYCEE EX CEM DE NGAPAROU/SOMONE",
            "LYCEE EX CEM MALICOUNDA II",
            "LYCEE EX CEM SALY",
            "LYCEE EX CEM SINDIA",
            "LYCEE EX CEM TOGLOU",
            "LYCEE KIRENE",
            "LYCEE POPENGUINE (EX CEM)",
            "NOUVEAU LYCEE DE MBOUR",
            "NOUVEAU LYCEE DE SALY"
        ],
        "Mbour2": [
            "LYCEE DE KOULOUCK",
            "LYCEE DE NDIEMANE",
            "LYCEE DE THIADIAYE",
            "LYCEE DE TOCOMACK",
            "LYCEE DIOUMA COR FAYE (NDIAGANIAO)",
            "LYCEE FISSEL MBADANE",
            "LYCEE LEOPOLD SEDAR SENGHOR",
            "LYCEE LOULY BENTEGNE",
            "LYCEE NDIARAO",
            "LYCEE NGUENIENE",
            "LYCEE SANDIARA"
        ],
        "Thiès Département": [
            "LYCEE BABACK SERERES",
            "LYCEE COUMBA DIACK GUEYE (KHOMBOLE)",
            "LYCEE DE BAYAKH (EX CEM)",
            "LYCEE DE SANGHE (EX CEM)",
            "LYCEE DE TOUBA PEYCOUCK",
            "LYCEE DIACK BODOKHANE",
            "LYCEE DJENDER",
            "LYCEE EX CEM KAYAR",
            "LYCEE EX CEM KEUR MOUSSA",
            "LYCEE EX CEM NDIEYENE SIRAKH",
            "LYCEE EX CEM NOTTO DIOBASS",
            "LYCEE EX CEM TASSETTE",
            "LYCEE EX CEM THIENABA",
            "LYCEE FANDENE EX CEM",
            "LYCEE NGOUNDIANE (EX CEM)"
        ],
        "Thiès Ville": [
            "LYCEE AHMADOU NDACK SECK (NOUVEAU LYCEE)",
            "LYCEE EX CEM FAHU",
            "LYCEE EX CEM JULES SAGNA",
            "LYCEE EX CEM MEDINA FALL-THIES",
            "LYCEE MALICK SY"
        ],
        "Tivaouane": [
            "LYCEE DE DIOGO",
            "LYCEE DE KOUL (TIVAOUANE)",
            "LYCEE DE TIVAOUANE",
            "LYCEE EL HADJI AMADOU CISSE DE PIRE",
            "LYCEE EX CEM BRAVE HIPPOLYTE DE MONT- ROLLAND",
            "LYCEE EX CEM DAROU KHOUDOSS",
            "LYCEE EX CEM KELLE",
            "LYCEE EX CEM LAMLAM GARE",
            "LYCEE EX CEM MEOUANE",
            "LYCEE EX CEM MERINA DAKHAR",
            "LYCEE EX CEM NOTTO GOUYE DIAMA",
            "LYCEE EX CEM PAMBAL",
            "LYCEE EX CEM PEKESSE",
            "LYCEE MEKHE VILLAGE",
            "YCEE MOURATH NDAO",
            "LYCEE NIAKHENE",
            "LYCEE TAIBA ICS",
            "LYCEE TAIBA NDIAYE",
            "LYCEE THILMAKHA (EX CEM)",
            "NOUVEAU LYCEE DE TIVAOUANE"
        ]
    }
}
# Génération des établissements avec ID entier
id_counter = 1

for region, villes in etablissements_data.items():
    for ville, etabs in villes.items():
        for nom in etabs:
            doc = {
                "id_etablissement": id_counter,  # ID entier
                "nom": nom.strip(),
                "ville": ville,
                "region": region,
                "statut": "Lycée",
                "type": "Public"
            }
            etabs_collection.insert_one(doc)
            id_counter += 1

print("Établissements insérés avec 'id_etablissement' entier.")
