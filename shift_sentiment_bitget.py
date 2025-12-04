#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para desplazar un día atrás la información de sentimiento (vader, finbert, sentiment_ratio, sentiment_combined)
de archivos de criptomonedas de Bitget. 
- Solo procesa los símbolos listados en activos.txt con fuente 'bitget'.
- Mantiene el mismo nombre de archivo.
- Hace un backup de cada archivo antes de modificarlo.
"""

import pandas as pd
import os
import shutil
from datetime import datetime

# ----------------------------
# Configuración
# ----------------------------
activos_file = 'activos.txt'  # Lista de activos con su fuente
sentiment_cols = ['vader', 'finbert', 'sentiment_ratio', 'sentiment_combined']

# Cargar lista de cripto válidos en Bitget
activos = pd.read_csv(activos_file, header=None, names=['symbol', 'source', 'name'], sep=':')
crypto_symbols = activos[activos['source'] == 'bitget']['symbol'].tolist()

# Nombre del folder de backup descriptivo
backup_folder = f"backup_sentiment_shift_bitget_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
if not os.path.exists(backup_folder):
    os.makedirs(backup_folder)

# ----------------------------
# Procesar todos los archivos SYMBOL_datos.txt válidos
# ----------------------------
for file_name in os.listdir('.'):
    if file_name.endswith('_datos.txt'):
        symbol = file_name.split('_')[0]
        if symbol in crypto_symbols:
            print(f"Procesando {file_name} ...")

            # Crear backup del archivo
            backup_file = os.path.join(backup_folder, file_name)
            shutil.copy2(file_name, backup_file)

            # Cargar datos con tabulaciones como separador
            df = pd.read_csv(file_name, sep='\t')
            df.columns = df.columns.str.strip()  # eliminar espacios extra en los nombres de columnas

            # Verificar que las columnas de sentiment existan
            missing_cols = [col for col in sentiment_cols if col not in df.columns]
            if missing_cols:
                print(f"Advertencia: columnas faltantes {missing_cols} en {file_name}, se omite")
                continue

            # Mover columnas de sentiment un día atrás
            df[sentiment_cols] = df[sentiment_cols].shift(-1)

            # Guardar en el mismo archivo, manteniendo tabulaciones
            df.to_csv(file_name, sep='\t', index=False)
            print(f"Archivo actualizado guardado: {file_name}")
