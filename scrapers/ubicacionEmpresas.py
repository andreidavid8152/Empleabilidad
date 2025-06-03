import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import re
from selenium.common.exceptions import TimeoutException
from urllib.parse import quote_plus

# Configurar Selenium
options = Options()
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("start-maximized")
options.add_argument(
    "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
)
driver = webdriver.Chrome(
    service=Service(ChromeDriverManager().install()), options=options
)

# Leer datos del Excel
df = pd.read_excel("data/ubicacionesEmpresas.xlsx")
df["UBICACION"]   = df["UBICACION"].astype("object")
df["COORDENADAS"] = df.get("COORDENADAS", pd.Series([""] * len(df))).astype("object")
df["MAPA_URL"]    = df.get("MAPA_URL",    pd.Series([""] * len(df))).astype("object")

procesadas   = 0
max_empresas = 2000

try:
    for idx, row in df.iterrows():
        if procesadas >= max_empresas:
            break

        empresa   = row["EMPRESA"]
        ubicacion = row.get("UBICACION", "")

        if pd.isna(ubicacion) or ubicacion == "":
            print(f"🔍 Procesando empresa: {empresa}")
            # -----------------------------------------------------------------
            # 1) Búsqueda en DuckDuckGo (armamos la URL y la cargamos de una vez)
            query = f"{empresa} site:emis.com/php"
            driver.get(f"https://duckduckgo.com/?q={quote_plus(query)}&ia=web")

            try:
                # -----------------------------------------------------------------
                # 2) Esperamos que aparezcan resultados orgánicos
                results = WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located(
                        (
                            By.CSS_SELECTOR,
                            "article[data-testid='result'] a[data-testid='result-title-a']"
                        )
                    )
                )

                # 3) Click en el primer resultado “real”
                first_link = results[0]
                driver.execute_script("arguments[0].scrollIntoView(true);", first_link)
                first_link.click()

            except TimeoutException:
                print(f"❌ DuckDuckGo no devolvió resultados para «{empresa}»")
                df.at[idx, "UBICACION"]   = "NO ENCONTRADO"
                df.at[idx, "COORDENADAS"] = "NO DISPONIBLE"
                df.at[idx, "MAPA_URL"]    = ""
                procesadas += 1
                continue

            if "/company-profile/EC" not in driver.current_url:
                print(f"⚠️ URL no es de EC: {driver.current_url}")
                df.at[idx, "UBICACION"]   = "NO ENCONTRADO"
                df.at[idx, "COORDENADAS"] = "NO DISPONIBLE"
                df.at[idx, "MAPA_URL"]    = ""
                procesadas += 1
                continue

            # 4) Extraer dirección
            divs = driver.find_elements(
                By.CSS_SELECTOR,
                "div.contact-info-div.d-t.w-100 div.d-tc.w-50.va-t"
            )
            if divs:
                p = divs[0].find_elements(By.TAG_NAME, "p")[0]
                span_text  = p.find_element(By.TAG_NAME, "span").text
                full_text  = p.text.replace(span_text, "").strip()
                clean_text = " ".join(full_text.split()).rstrip(";")
                df.at[idx, "UBICACION"] = clean_text
            else:
                df.at[idx, "UBICACION"] = "NO ENCONTRADO"

            # 5) Extraer coordenadas dentro del iframe
            try:
                # 1) Espera hasta que el iframe esté presente en el DOM (hasta 10s)
                iframe = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.div-map iframe"))
                )
                # 2) Scroll hasta el iframe para forzar carga lazy-load
                driver.execute_script("arguments[0].scrollIntoView(true);", iframe)

                # 3) Intenta leer el src directamente
                src = iframe.get_attribute("src")
                if src and "ll=" in src:
                    coords = re.search(r"ll=([^&]+)", src).group(1)
                    lat, lng = [c.strip() for c in coords.split(",")]
                    df.at[idx, "COORDENADAS"] = f"{lat},{lng}"
                    df.at[idx, "MAPA_URL"]    = f"https://www.google.com/maps?q={lat},{lng}"
                    print(f"📍 Coordenadas desde iframe src: {lat},{lng}")
                else:
                    # Fallback: entrar en el frame y hacer click en “Ampliar el mapa”
                    driver.switch_to.frame(iframe)
                    mapa_link = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, "a[aria-label='Ampliar el mapa']"))
                    )
                    href = mapa_link.get_attribute("href")
                    if "maps.google.com/maps?ll=" in href:
                        coords = href.split("ll=")[1].split("&")[0]
                        lat, lng = coords.split(",")
                        df.at[idx, "COORDENADAS"] = f"{lat.strip()},{lng.strip()}"
                        df.at[idx, "MAPA_URL"]    = f"https://www.google.com/maps?q={lat.strip()},{lng.strip()}"
                        print(f"📍 Coordenadas tras click: {lat.strip()},{lng.strip()}")
                    else:
                        df.at[idx, "COORDENADAS"] = "NO DISPONIBLE"
                        df.at[idx, "MAPA_URL"]    = ""
                    driver.switch_to.default_content()

            except TimeoutException:
                print("❌ Timeout: no se encontró el iframe del mapa.")
                df.at[idx, "COORDENADAS"] = "NO DISPONIBLE"
                df.at[idx, "MAPA_URL"]    = ""
            except Exception as e:
                print(f"❌ Error extrayendo coordenadas: {e}")
                df.at[idx, "COORDENADAS"] = "NO DISPONIBLE"
                df.at[idx, "MAPA_URL"]    = ""
            finally:
                # asegúrate de volver al contenido principal si no lo hiciste en el fallback
                if driver.current_url and driver.switch_to:
                    try: driver.switch_to.default_content()
                    except: pass

            procesadas += 1

except KeyboardInterrupt:
    print("\n⏹️ Proceso detenido por el usuario. Guardando datos…")

finally:
    # Guardar en cualquier caso
    df.to_excel("data/ubicacionesEmpresas.xlsx", index=False)
    driver.quit()
    print("✅ Datos guardados en 'ubicacionesEmpresas.xlsx'.")
