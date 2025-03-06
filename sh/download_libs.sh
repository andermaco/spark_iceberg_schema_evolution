#!/bin/bash

# Ruta absoluta al directorio de descarga
DIR="`pwd`/deploy/jar_libraries"  # Reemplaza /path/to/your/project con la ruta real

URLS=(
  "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.17.161/bundle-2.17.161.jar"  
  "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/1.6.1/iceberg-spark-runtime-3.3_2.12-1.6.1.jar"  
  "https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/2.17.161/url-connection-client-2.17.161.jar"
)

# Asegúrate de que el directorio exista. Si no existe, créalo.
mkdir -p "$DIR" # El flag -p crea directorios padres si es necesario

for URL in "${URLS[@]}"; do
  FILENAME=$(basename "$URL")  
  wget -q "$URL" -O "$DIR/$FILENAME"
  echo "Download: $FILENAME"
done