# Kata Spark

Ce projet implémente une pipeline de traitement de données Airbnb en utilisant PySpark, PostgreSQL et Hive. L'objectif est de traiter les données des annonces et des avis Airbnb, de les nettoyer, de les analyser, et de les exporter pour des usages ultérieurs.

---

## **Ordre d'exécution des scripts**

### **1. Charger les données dans la base de données**
Le script `01_load_listings_to_db.py` charge les données des annonces Airbnb depuis le fichier CSV `listings.csv` dans une base de données PostgreSQL.

```bash
python 01_load_listings_to_db.py
```

### **2. Ingestion des données des annonces dans Bronze**
Le script `02_ingest_listings_from_db.py` lit les données des annonces depuis la base de données PostgreSQL et les stocke dans le répertoire `bronze/listings/` au format Parquet, partitionnées par `year`, `month`, et `day`.

```bash
python 02_ingest_listings_from_db.py
```

### **3. Préparation des fichiers de streaming pour les avis**
Le script `03_prepare_reviews_chunks.py` découpe le fichier CSV `reviews.csv` en plusieurs morceaux de 1000 lignes et les stocke dans le répertoire `data/reviews_chunks/`.

```bash
python 03_prepare_reviews_chunks.py
```

### **4. Simulation du streaming des avis**
Le script Bash `inject_reviews.sh` copie progressivement les fichiers découpés dans le répertoire `data/reviews_stream/`, simulant un flux de données.

```bash
bash inject_reviews.sh
```

### **5. Traitement en streaming des avis**
Le script `04_stream_reviews.py` lit les fichiers dans `data/reviews_stream/` en mode streaming, ajoute des colonnes de partition (`année`, `mois`, `jour`), et écrit les données dans le répertoire `bronze/reviews/`.

```bash
python 04_stream_reviews.py
```

### **6. Nettoyage et jointure des données**
Le script `05_transform_clean.py` :
- Nettoie les données des annonces et des avis.
- Effectue une jointure entre les deux ensembles de données.
- Stocke les données jointes dans une table Hive `silver_joined`.

```bash
python 05_transform_clean.py
```

### **7. Modélisation Machine Learning**
Le script `06_ml_model.py` :
- Charge les données depuis la table Hive `silver_joined`.
- Entraîne un modèle de régression linéaire pour prédire les prix des annonces.
- Sauvegarde les prédictions dans le répertoire `gold/predictions/`.

```bash
python 06_ml_model.py
```

### **8. Export des données vers la base de données**
Le script `07_export_to_api.py` :
- Exporte les prédictions depuis `gold/predictions/` vers la table PostgreSQL `listing_predictions`.
- Exporte les données résumées depuis `gold/listing_summary/` vers la table PostgreSQL `listing_summary`.

```bash
python 07_export_to_api.py
```

---

## **Dépendances**
Installez les dépendances nécessaires avec le fichier `requirements.txt` :

```bash
pip install -r requirements.txt
```

---

## **Structure du projet**

```plaintext
airbnb_pipeline/
├── bin/
│   └── inject_reviews.sh
├── data/
│   ├── listings.csv
│   ├── reviews.csv
│   ├── reviews_chunks/
│   └── reviews_stream/
├── bronze/
│   ├── listings/
│   └── reviews/
├── silver/
│   └── joined/
├── gold/
│   ├── predictions/
│   └── listing_summary/
├── scripts/
│   ├── 01_load_listings_to_db.py
│   ├── 02_ingest_listings_from_db.py
│   ├── 03_prepare_reviews_chunks.py
│   ├── 04_stream_reviews.py
│   ├── 05_transform_clean.py
│   ├── 06_ml_model.py
│   └── 07_export_to_api.py
└── requirements.txt
```

---

## **Notes**
- Assurez-vous que PostgreSQL, Hive et PySpark sont correctement configurés avant d'exécuter les scripts.
- Les chemins des fichiers et répertoires doivent être ajustés en fonction de votre environnement.

