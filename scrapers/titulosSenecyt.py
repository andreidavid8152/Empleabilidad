import os
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

def configurar_driver():
    options = Options()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("start-maximized")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
    )
    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()), options=options
    )

def extraer_titulos(driver, tabla_id, nivel, identificacion):
    registros = []
    try:
        tabla = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.ID, tabla_id))
        )
    except:
        return registros
    for row in tabla.find_elements(By.CSS_SELECTOR, "tbody tr"):
        cols = row.find_elements(By.TAG_NAME, "td")
        datos = [c.text.replace("\n", " ").strip() for c in cols]
        registros.append({
            "identificacion": identificacion,
            "nivel": nivel,
            "Título": datos[0],
            "Institución de Educación Superior": datos[1],
            "Tipo": datos[2],
            "Reconocido Por": datos[3],
            "Número de Registro": datos[4],
            "Fecha de Registro": datos[5],
            "Área o Campo de Conocimiento": datos[6],
            "Observación": datos[7],
        })
    return registros

def main():
    ruta = os.path.join("data", "cedulas_resultados.xlsx")
    df = pd.read_excel(ruta, sheet_name="cedulas")

    try:
        df_titulos_old = pd.read_excel(ruta, sheet_name="titulos")
    except:
        df_titulos_old = pd.DataFrame(columns=[
            "identificacion","nivel","Título",
            "Institución de Educación Superior","Tipo",
            "Reconocido Por","Número de Registro",
            "Fecha de Registro","Área o Campo de Conocimiento","Observación"
        ])

    registros_all = []

    driver = configurar_driver()
    wait = WebDriverWait(driver, 30)

    try:
        for idx, row in df[df["PROCESADO"].isna()].iterrows():
            ced = str(row["CEDULA"]).strip()
            print(f"\n🔎 Procesando cédula {ced}…")

            # cargar página de búsqueda
            driver.get(
                "https://www.senescyt.gob.ec/consulta-titulos-web/"
                "faces/vista/consulta/consulta.xhtml"
            )

            # ingresar cédula
            inp = wait.until(EC.element_to_be_clickable((By.ID, "formPrincipal:identificacion")))
            inp.clear()
            inp.send_keys(ced)

            # esperar captcha manual
            input(f"🛑 Ingresa el CAPTCHA para {ced} y presiona Enter…")

            # click "Buscar"
            driver.find_element(By.ID, "formPrincipal:boton-buscar").click()

            # esperar carga de resultados
            wait.until(EC.visibility_of_element_located((By.ID, "formPrincipal:pnlInfoPersonal")))

            # extraer ambos niveles
            nuevos = []
            nuevos += extraer_titulos(driver, "formPrincipal:j_idt45:0:tablaAplicaciones", "Cuarto Nivel", ced)
            nuevos += extraer_titulos(driver, "formPrincipal:j_idt45:1:tablaAplicaciones", "Tercer Nivel", ced)

            if nuevos:
                df.at[idx, "PROCESADO"] = 1
                registros_all.extend(nuevos)
                print(f"   ✅ {len(nuevos)} títulos extraídos.")
            else:
                df.at[idx, "PROCESADO"] = 0
                print("   ⚠️  No se encontraron datos.")

    except KeyboardInterrupt:
        print("\n⏸️  Interrumpido por usuario, guardando avances…")

    finally:
        driver.quit()

        # combinar con los títulos antiguos
        df_nuevos = pd.DataFrame(registros_all)
        df_titulos = pd.concat([df_titulos_old, df_nuevos], ignore_index=True)

        # guardar ambas hojas
        with pd.ExcelWriter(ruta, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="cedulas", index=False)
            df_titulos.to_excel(writer, sheet_name="titulos", index=False)

        print(f"\n📁 Archivo guardado en: {ruta}")

if __name__ == "__main__":
    main()
