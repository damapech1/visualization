# prepare_data.py  ← VERSIÓN FINAL OFICIAL (10 COLUMNAS LIMPIAS
import pandas as pd

print("Eliminando columna extra vacía y dejando solo las 10 columnas útiles...")

# Lee saltando las 4 filas de encabezado
df = pd.read_csv('../data/raw/sismos_completo.csv', skiprows=4, header=None, encoding='utf-8', on_bad_lines='skip')

print(f"Original: {df.shape[1]} columnas detectadas")

# Nos quedamos SOLO con las primeras 10 columnas (la 11ª está vacía)
df = df.iloc[:, :10]

# Ponemos nombres perfectos
df.columns = [
    'Fecha', 'Hora', 'Magnitud', 'Latitud', 'Longitud',
    'Profundidad', 'Referencia_localizacion',
    'Fecha_UTC', 'Hora_UTC', 'Estatus'
]

# Quitamos filas completamente vacías
df.dropna(how='all', inplace=True)
df.reset_index(drop=True, inplace=True)

# Guardamos la versión 100 % limpia
df.to_csv('../data/raw/sismos_raw.csv', index=False)

print(f"¡HECHO! {len(df):,} registros con exactamente 10 columnas")
print("Archivo final: data/raw/sismos_raw.csv")
print(df.head(2))