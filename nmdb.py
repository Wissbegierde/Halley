#!/usr/bin/env python3
"""
nmdb_to_csv.py

Función principal expuesta:
    nmdb_data(start_date, start_time, end_date, end_time,
              out_csv='nmdb_oulu.csv', base_url=None)

start_date / end_date: "YYYY-MM-DD" (string)
start_time / end_time: "HH:MM"     (string, 24h)

Retorna: ruta del CSV generado (string) y el DataFrame resultante.
"""
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
import requests
import pandas as pd
import re
import matplotlib.pyplot as plt

# URL por defecto (la suministrada originalmente)
DEFAULT_URL = ("https://www.nmdb.eu/nest/draw_graph.php?"
               "formchk=1&stations[]=OULU&tabchoice=revori&dtype=corr_for_efficiency&"
               "tresolution=5&yunits=0&shift=2&date_choice=bydate&"
               "start_day=29&start_month=7&start_year=2025&start_hour=0&start_min=0&"
               "end_day=29&end_month=8&end_year=2025&end_hour=23&end_min=59&"
               "output=plot&ygrid=1&mline=1&transp=0&fontsize=1&text_color=222222&"
               "background_color=FFFFFF&margin_color=FFFFFF")

DATE_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')
TIME_RE = re.compile(r'^\d{2}:\d{2}$')
NUM_RE = re.compile(r'^[+-]?\d+(\.\d+)?([eE][+-]?\d+)?$')

def _date_time_to_query_parts(date_str: str, time_str: str):
    if not DATE_RE.match(date_str):
        raise ValueError("date debe tener formato YYYY-MM-DD")
    if not TIME_RE.match(time_str):
        raise ValueError("time debe tener formato HH:MM")
    y, m, d = date_str.split('-')
    hh, mm = time_str.split(':')
    # el formulario usa días/meses sin 0 a la izquierda en algunos casos; imitamos eso
    return d.lstrip('0') or '0', m.lstrip('0') or '0', y, hh.lstrip('0') or '0', mm.lstrip('0') or '0'

def _build_ascii_url(base_url: str, start_date: str, start_time: str, end_date: str, end_time: str) -> str:
    p = urlparse(base_url)
    qs = parse_qs(p.query)
    s_day, s_month, s_year, s_hour, s_min = _date_time_to_query_parts(start_date, start_time)
    e_day, e_month, e_year, e_hour, e_min = _date_time_to_query_parts(end_date, end_time)

    qs['start_day'] = [s_day]; qs['start_month'] = [s_month]; qs['start_year'] = [s_year]
    qs['start_hour'] = [s_hour]; qs['start_min'] = [s_min]
    qs['end_day'] = [e_day]; qs['end_month'] = [e_month]; qs['end_year'] = [e_year]
    qs['end_hour'] = [e_hour]; qs['end_min'] = [e_min]
    qs['output'] = ['ascii']
    new_q = urlencode(qs, doseq=True)
    return urlunparse((p.scheme, p.netloc, p.path, p.params, new_q, p.fragment))

def _download_text(url: str) -> str:
    headers = {'User-Agent': 'nmdb-to-csv/1.0 (+https://example)'}
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.text

def _parse_ascii(text: str) -> pd.DataFrame:
    rows = []
    for ln in text.splitlines():
        line = ln.strip()
        if not line or line.startswith('#'):
            continue
        tokens = line.split()
        dt = None; cnt = None

        # Formato habitual: YYYY-MM-DD hh:mm:ss count ...
        if len(tokens) >= 3 and re.match(r'^\d{4}-\d{2}-\d{2}$', tokens[0]) and re.match(r'^\d{2}:\d{2}(:\d{2})?$', tokens[1]):
            dt = tokens[0] + ' ' + tokens[1]
            cnt = tokens[2]
        # Caso YYYY-MM-DD count
        elif len(tokens) >= 2 and re.match(r'^\d{4}-\d{2}-\d{2}$', tokens[0]) and NUM_RE.match(tokens[1]):
            dt = tokens[0]
            cnt = tokens[1]
        # Último token numérico -> count, resto datetime libre
        elif NUM_RE.match(tokens[-1]) and len(tokens) >= 2:
            cnt = tokens[-1]
            dt = ' '.join(tokens[:-1])
        else:
            joined = ' '.join(tokens)
            m = re.search(r'(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2})?)', joined)
            n = NUM_RE.search(joined)
            if m and n:
                dt = m.group(1).replace('T', ' ')
                cnt = n.group(0)
            else:
                # no pudo parsear esta línea -> se ignora
                continue

        rows.append((dt, cnt))

    df = pd.DataFrame(rows, columns=['datetime_raw', 'count_raw'])
    df['datetime'] = pd.to_datetime(df['datetime_raw'], errors='coerce', dayfirst=False)
    df['count'] = pd.to_numeric(df['count_raw'], errors='coerce')
    df = df.dropna(subset=['datetime', 'count']).sort_values('datetime').reset_index(drop=True)
    return df[['datetime', 'count']]

def nmdb_data(start_date: str, start_time: str, end_date: str, end_time: str,
              out_csv: str = 'nmdb_oulu.csv', base_url: str = None):
    """
    Descarga los datos entre start_date start_time y end_date end_time y guarda un CSV.
    Ejemplo de uso:
        nmdb_data('2025-07-29', '00:00', '2025-08-29', '23:59')
    Retorna (out_csv, dataframe)
    """
    base = base_url or DEFAULT_URL
    url_ascii = _build_ascii_url(base, start_date, start_time, end_date, end_time)
    print("Descargando:", url_ascii)
    text = _download_text(url_ascii)
    df = _parse_ascii(text)
    if df.empty:
        # guardamos el contenido para depuración
        with open('nmdb_debug.txt', 'w', encoding='utf-8') as f:
            f.write(text)
        raise RuntimeError("No se extrajeron filas válidas. Revisa nmdb_debug.txt")
    df.to_csv(out_csv, index=False, date_format='%Y-%m-%d %H:%M:%S')
    print(f"CSV guardado en: {out_csv}  (filas: {len(df)})")
    return out_csv, df

if __name__ == "__main__":
    #desde 2025-07-29 00:00 hasta 2025-08-29 23:59
    csvpath, df = nmdb_data('2024-07-29', '00:00', '2025-08-29', '23:59')
    # opcional: imprimir las primeras filas
    print(df.head())
