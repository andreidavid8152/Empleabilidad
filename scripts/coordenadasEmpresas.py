"""
Geocodificación con “NA” persistente para todo el DataFrame
----------------------------------------------------------
• Procesa SOLO filas cuyo campo COORDENADA esté vacío.
• Limpia y normaliza direcciones.
• Descarte inteligente de registros con poca información.
• Cachea el resultado (coordenadas o "NA") para no repetir trabajo.
• Escribe siempre algo en COORDENADA:
      - "lat,lng" cuando la API responde
      - "NA" cuando la dirección es insuficiente o la API falla
"""

import os
import pandas as pd
import requests
from dotenv import load_dotenv

# ------------- CONFIGURACIÓN -------------
load_dotenv()
API_KEY = os.getenv("api_key")

INVALID = {
    "",
    "--------------",
    "----------------",
    "--------",
    "nan",
    ".",
    "-",
    "--",
    "---",
    "_",
    "*",
    "----------------",
    "___",
    "null",
    "NULL",
    "NA" "ninguna",
    "ninguno",
    "na",
    "n/a",
}


# ------------- UTILIDADES ---------------
def valor_valido(v: str) -> str:
    if v is None:
        return ""
    s = str(v).strip().lower()
    return "" if s in INVALID else s.title().strip()


def construir_api_address(row) -> str:
    """Cadena limpia en minúsculas para enviar a la API y usar como cache key."""
    comps = []
    calle = valor_valido(row.get("CALLE"))
    calle_sec = valor_valido(row.get("CALLE SECUNDARIA"))
    numero = valor_valido(row.get("NUMERO"))
    barrio = valor_valido(row.get("BARRIO"))
    ciudad = valor_valido(row.get("CIUDAD"))
    provincia = valor_valido(row.get("PROVINCIA"))

    if calle and calle_sec:
        comps.append(f"{calle} y {calle_sec}")
    elif calle:
        comps.append(calle)
    elif calle_sec:
        comps.append(calle_sec)

    if numero:
        comps.append(numero)
    if barrio:
        comps.append(barrio)
    if ciudad:
        comps.append(ciudad)
    if provincia:
        comps.append(provincia)

    comps.append("Ecuador")
    return ", ".join(comps).lower()


def construir_fulladdress(row) -> str:
    """Texto para logs que muestra la dirección con capitalización, sin identificador."""
    return construir_api_address(row).title()


def es_direccion_util(row) -> bool:
    """Verifica ciudad+provincia y al menos una parte local (calle/intersec/barrio)."""
    ciudad = valor_valido(row.get("CIUDAD"))
    provincia = valor_valido(row.get("PROVINCIA"))
    calle = valor_valido(row.get("CALLE"))
    calle_sec = valor_valido(row.get("CALLE SECUNDARIA"))
    barrio = valor_valido(row.get("BARRIO"))
    return bool(ciudad and provincia and (calle or calle_sec or barrio))


# ----------- API ---------------
def call_geocode(key: str, address: str):
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {
        "address": address,
        "key": key,
        "components": "country:EC",  # 👈 restricción por país
    }
    r = requests.get(url, params=params, timeout=10)
    if r.status_code == 200 and r.json().get("status") == "OK":
        loc = r.json()["results"][0]["geometry"]["location"]
        return loc["lat"], loc["lng"]
    return None


def safe_geocode(key: str, address: str):
    try:
        return call_geocode(key, address)
    except Exception as e:
        print("⚠️ Excepción:", e, "|", address)
        return None


# ------------- CARGA -------------
print("📥 Cargando archivo Excel completo...")
df = pd.read_excel(
    "data/empleabilidad.xlsx", dtype=str, keep_default_na=False, na_values=[]
)

if "COORDENADA" not in df.columns:
    print("➕ Creando columna 'COORDENADA' vacía")
    df["COORDENADA"] = ""

# Pre-calcular las direcciones
print("🔧 Pre-calculando AddressAPI y FullAddress para cada fila...")
df["AddressAPI"] = df.apply(construir_api_address, axis=1)
df["FullAddress"] = df.apply(construir_fulladdress, axis=1)

# ------------- PROCESAMIENTO -------------
cache = {}
iteration = 0
LIMITE = None

while True:
    mask = df["COORDENADA"].str.strip() == ""
    if not mask.any():
        print("✔️ No quedan filas sin coordenada → proceso terminado.")
        break

    idx = df[mask].index[0]
    addr_key = df.at[idx, "AddressAPI"]
    fulladdr = df.at[idx, "FullAddress"]

    if addr_key in cache:
        coord_str = cache[addr_key]
        fuente = "Cache"
    else:
        row = df.loc[idx]
        if not es_direccion_util(row):
            coord_str = "NA"
            fuente = "Descartado"
        else:
            print(f"📍 Iter {iteration+1}: geocodificando → {fulladdr}")
            result = safe_geocode(API_KEY, addr_key)
            if result is None:
                coord_str = "NA"
                fuente = "Sin coord"
            else:
                lat, lng = result
                coord_str = f"{lat},{lng}"
                fuente = "API"
        cache[addr_key] = coord_str

    coinc = mask & (df["AddressAPI"] == addr_key)
    n = coinc.sum()
    df.loc[coinc, "COORDENADA"] = coord_str
    print(
        f"🔄 Iter {iteration+1}: '{addr_key}' → {coord_str}   actualizado en {n} fila(s)."
    )

    iteration += 1
    if LIMITE is not None and iteration >= LIMITE:
        print(f"⏹️ Límite de iteraciones alcanzado: {LIMITE}")
        break

# ------------- GUARDAR -------------
print("💾 Guardando archivo con coordenadas actualizadas...")
df.to_excel("empleabilidad_geocodificada.xlsx", index=False)
print(f"✅ Proceso completado: {iteration} iteraciones ejecutadas.")