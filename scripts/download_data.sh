#!/usr/bin/env bash
# Télécharge le jeu UCI « Individual household electric power consumption ».
# Si le téléchargement échoue, utilisez le plan B :
#   python scripts/generate_synthetic_data.py
set -e
cd "$(dirname "$0")/../data"
URL="https://archive.ics.uci.edu/static/public/235/individual+household+electric+power+consumption.zip"
echo "Téléchargement depuis $URL"
curl -fL -o household.zip "$URL"
unzip -o household.zip
ls -lh household_power_consumption.txt
