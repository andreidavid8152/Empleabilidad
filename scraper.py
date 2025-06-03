import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# Configurar Selenium
options = Options()
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("start-maximized")
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36")

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)


driver = webdriver.Chrome(
    service=Service(ChromeDriverManager().install()), options=options
)

# Leer datos del Excel
df = pd.read_excel("data/data.xlsx", sheet_name="Empresas")
df["UBICACION"] = df["UBICACION"].astype("object")  # 💡 Esto resuelve el warning

# Procesar TODAS LAS EMPRESAS
procesadas = 0
max_empresas = 10

for idx, row in df.iterrows():
    if procesadas >= max_empresas:
        break

    empresa = row["EMPRESA"]
    ubicacion = row.get("UBICACION", "")

    if pd.isna(ubicacion) or ubicacion == "":
        print(f"Procesando empresa: {empresa}")
        try:
            # Búsqueda en Google
            query = f"{empresa} site:https://www.emis.com/php/company-profile/EC"
            driver.get("https://www.google.com/")
            time.sleep(1)

            search_box = driver.find_element(By.NAME, "q")
            search_box.clear()
            search_box.send_keys(query)
            search_box.send_keys(Keys.RETURN)
            time.sleep(2)

            # Click en el primer resultado
            first_result = driver.find_element(By.CSS_SELECTOR, "div.yuRUbf a.zReHs")
            first_result.click()
            time.sleep(3)

            # Extraer dirección desde EMIS
            divs = driver.find_elements(
                By.CSS_SELECTOR, "div.contact-info-div.d-t.w-100 div.d-tc.w-50.va-t"
            )
            if divs:
                p = divs[0].find_elements(By.TAG_NAME, "p")[0]
                span_text = p.find_element(By.TAG_NAME, "span").text
                full_text = p.text.replace(span_text, "").strip()
                clean_text = " ".join(full_text.split()).rstrip(";")
                df.at[idx, "UBICACION"] = clean_text
            else:
                df.at[idx, "UBICACION"] = "NO ENCONTRADO"
        except Exception as e:
            print(f"❌ Error al procesar {empresa}: {e}")
            df.at[idx, "UBICACION"] = "NO ENCONTRADO"

        procesadas += 1  # ✅ Contador de empresas procesadas

# Guardar el DataFrame completo con todos los datos
df.to_excel("ubicaciones.xlsx", index=False)
driver.quit()
print("✅ Proceso finalizado. Archivo guardado como 'ubicaciones.xlsx'.")
