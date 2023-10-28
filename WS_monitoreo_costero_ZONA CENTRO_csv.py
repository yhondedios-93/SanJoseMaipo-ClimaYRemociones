"""
Nombre del Autor: Juan Galaz Amengual
Fecha de Creación: 28-10-2023
Descripción: Script para extraer datos meteorológicos de varias estaciones COSTERAS y guardarlos en un archivo CSV.
Contacto: proyectolluvia@proton.me
"""


import requests
from bs4 import BeautifulSoup
import time
import ntplib
from datetime import datetime
import pytz
import csv

def obtener_hora_precisa():
    """
    Obtiene la hora precisa a través de un servidor NTP.

    Si no se puede establecer la conexión con el servidor NTP, 
    se recurre a la función obtener_hora_respaldo para obtener la hora del sistema.

    Returns:
        datetime: Objeto datetime que representa la hora actual en la zona horaria de Chile/Continental.
    """
    try:
        cliente_ntp = ntplib.NTPClient()
        respuesta = cliente_ntp.request('europe.pool.ntp.org', version=3)
        fecha_hora = datetime.fromtimestamp(respuesta.tx_time, pytz.timezone('Chile/Continental'))
        return fecha_hora
    except:
        return obtener_hora_respaldo()

def obtener_hora_respaldo():
    """
    Obtiene la hora actual del sistema en la zona horaria de Chile/Continental.

    Esta función se utiliza como respaldo en caso de que la conexión al servidor NTP falle.

    Returns:
        datetime: Objeto datetime que representa la hora actual en la zona horaria de Chile/Continental.
    """
    return datetime.now(pytz.timezone('Chile/Continental'))

# Diccionario que mapea las URLs de las estaciones meteorológicas a sus nombres correspondientes
direcciones = {
    "http://web.directemar.cl/met/jturno/estaciones/huasco/index.htm": "HUASCO",
    "http://web.directemar.cl/met/jturno/estaciones/tortuga/index.htm": "FARO PUNTA TORTUGA",
    "http://web.directemar.cl/met/jturno/estaciones/losvilos/index.htm": "LOS VILOS",
    "http://web.directemar.cl/met/jturno/estaciones/quintero/index.htm": "QUINTERO",
    "http://web.directemar.cl/met/jturno/estaciones/valparaiso/index.htm": "VALPARAÍSO",
    "http://web.directemar.cl/met/jturno/estaciones/panul/index.htm": "PANUL",
    "http://web.directemar.cl/met/jturno/estaciones/pichilemu/index.htm": "PICHILEMU"
}

# Nombre del archivo CSV donde se almacenarán los datos
nombre_archivo_csv = 'datos_meteorologicos.csv'

# Encabezados para el archivo CSV
encabezados_csv = [
    'Hora de extracción', 'Estación', 'Temperatura', 'Punto de Rocío', 'Humedad',
    'Lluvia Hoy', 'Velocidad Viento (racha)', 'Velocidad Viento (promedio)', 'Barómetro'
]

# Creación e inicialización del archivo CSV
with open(nombre_archivo_csv, 'w', newline='', encoding='utf-8') as archivo_csv:
    escritor_csv = csv.DictWriter(archivo_csv, fieldnames=encabezados_csv)
    escritor_csv.writeheader()

def extraer_datos(url, contenido_pagina):
    """
    Extrae los datos meteorológicos de la página web y los guarda en un archivo CSV.

    Args:
        url (str): URL de la estación meteorológica.
        contenido_pagina (str): Contenido HTML de la página de la estación meteorológica.
    """
    sopa = BeautifulSoup(contenido_pagina, 'html.parser')
    hora_actual = obtener_hora_precisa()
    nombre_estacion = direcciones.get(url, "Desconocido")  

    print(f"\nHora de extracción: {hora_actual.strftime('%Y-%m-%d %H:%M:%S')}")
    print("Estación: %s" % nombre_estacion)

    # Extracción de los datos de temperatura, punto de rocío y humedad
    datos_temperatura = sopa.select(".td_temperature_data td")
    temperatura = datos_temperatura[1].text.strip()
    punto_rocio = datos_temperatura[3].text.strip()
    humedad = datos_temperatura[7].text.strip()

    # Extracción de los datos de lluvia
    datos_lluvia = sopa.select(".td_rainfall_data td")
    lluvia_hoy = datos_lluvia[1].text.strip()

    # Extracción de los datos de viento
    datos_viento = sopa.select(".td_wind_data td")
    velocidad_viento_racha = datos_viento[1].text.strip()
    velocidad_viento_promedio = datos_viento[3].text.strip()

    # Extracción de los datos de presión
    datos_presion = sopa.select(".td_pressure_data td")
    barometro = datos_presion[1].text.strip()

    # Impresión de los datos extraídos
    print("Temperatura: %s" % temperatura)
    print("Punto de Rocío: %s" % punto_rocio)
    print("Humedad: %s" % humedad)
    print("Lluvia Hoy: %s" % lluvia_hoy)
    print("Velocidad Viento (racha): %s" % velocidad_viento_racha)
    print("Velocidad Viento (promedio): %s" % velocidad_viento_promedio)
    print("Barómetro: %s\n" % barometro)

    # Guardado de los datos en el archivo CSV
    with open(nombre_archivo_csv, 'a', newline='', encoding='utf-8') as archivo_csv:
        escritor_csv = csv.DictWriter(archivo_csv, fieldnames=encabezados_csv)
        escritor_csv.writerow({
            'Hora de extracción': hora_actual.strftime('%Y-%m-%d %H:%M:%S'),
            'Estación': nombre_estacion,
            'Temperatura': temperatura,
            'Punto de Rocío': punto_rocio,
            'Humedad': humedad,
            'Lluvia Hoy': lluvia_hoy,
            'Velocidad Viento (racha)': velocidad_viento_racha,
            'Velocidad Viento (promedio)': velocidad_viento_promedio,
            'Barómetro': barometro
        })

# Bucle principal para realizar las solicitudes cada 10 minutos
while True:
    for url in direcciones:
        try:
            respuesta = requests.get(url)
            respuesta.raise_for_status()
            extraer_datos(url, respuesta.text)
        except requests.RequestException as e:
            print(f"Error al acceder a {url}: {e}")
    time.sleep(600)  # Espera de 10 minutos
