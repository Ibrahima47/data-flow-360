import requests
import json



def imprimer_json(chemin, donnees) : 
    with open(chemin, 'a', encoding='utf-8') as f:
        f.write(json.dumps(donnees, ensure_ascii=False, indent=4))


