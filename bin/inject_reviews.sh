#!/bin/bash

SRC_DIR="data/reviews_chunks"
DEST_DIR="data/reviews_stream"

mkdir -p "$DEST_DIR"

for file in "$SRC_DIR"/*.csv
do
    echo "Injecting $file..."
    cp "$file" "$DEST_DIR"
    sleep 3
done
