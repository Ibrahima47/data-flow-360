from utilitaires import imprimer_json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

url = "https://ibrahima47.github.io/education-dataFlow360/"

driver = webdriver.Chrome()

driver.get(url)
wait = WebDriverWait(driver, 5)
wait.until(EC.presence_of_element_located((By.CLASS_NAME, "nav-tab")))
boutons = driver.find_elements(By.CLASS_NAME, "nav-tab")
for i, bouton in enumerate(boutons): 
    if bouton.text in ["API", "Dashboard"]: continue
    else : 
        print(bouton.text)
        document = []
        bouton.click()
        # wait.until(EC.presence_of_element_located((By.CLASS_NAME, "section active")))
        # boutons = driver.find_elements(By.CLASS_NAME, "nav-tab")÷÷÷
        entite = driver.find_element(By.CSS_SELECTOR, ".section.active")
        lignes = entite.find_elements(By.TAG_NAME, "tr")
        enTetes = lignes[0].find_elements(By.CSS_SELECTOR, "th")
        for l,ligne in enumerate(lignes) : 
            if l == 0 : continue
            enregistrement = {}
            cellules = ligne.find_elements(By.CSS_SELECTOR, "td")
            for k in range(len(enTetes)) : 
                # if l== 0 : continue
                enregistrement[enTetes[k].text]=cellules[k].text
            document.append(enregistrement)
        long = len(lignes)-1
        try : 
            page_actuelle = entite.find_element(By.CSS_SELECTOR, "span.page-info")
            print(f" page_actuelle :: {page_actuelle.text}")
            if page_actuelle : 
                nb_page = int(page_actuelle.text.split()[3])
                for i in range(1, nb_page) : 
                    try : 
                        print(f"Tentative d'acès à la page {i+1}")
                        suivant = entite.find_elements(By.CSS_SELECTOR, 'button.page-btn')#[name="Suivant] ›
                        suivant[len(suivant)-1].click()
                        entite = driver.find_element(By.CSS_SELECTOR, ".section.active")
                        lignes = entite.find_elements(By.TAG_NAME, "tr")
                        enTetes = lignes[0].find_elements(By.CSS_SELECTOR, "th")
                        print(f"le bouton suivant a été cliqué :: Acces granted")
                        for o,ligne in enumerate(lignes) : 
                            if o == 0 : continue
                            enregistrement = {}
                            cellules = ligne.find_elements(By.CSS_SELECTOR, "td")
                            for k in range(len(enTetes)) : 
                                enregistrement[enTetes[k].text]=cellules[k].text
                            document.append(enregistrement)
                        long += len(lignes)-1
                        print(f"nombre de ligne scrapés dans {page_actuelle.text} pour la page {i+1}{len(lignes)-1}")
                    except Exception as e : 
                        print(f"erreur d'accès à la page {i+1} de {bouton.text} \n\tCause: \t{e}")
        except Exception as e : print(f"Cette section n'a pas de pagination  : Poursuivons vers la section suivante !")
        finally : 
            print(long)
            imprimer_json(f"./data_sources/generators/Ibrahima47/scraped_data/{bouton.text.split()[-1]}.json", document)
        # for entite in entites:
        #     print(entite.text)

# html_content = driver.page_source
# soup = BeautifulSoup(html_content, 'html.parser')

# print(len(entites))
