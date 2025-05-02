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
[Regardez la vidéo de simulation du streaming](https://drive.google.com/file/d/1GUI3CaHCAl0RsaW7Ijx7vynJE0YydLE3/view?usp=sharing)

### **5. Traitement en streaming des avis**
Le script `04_stream_reviews.py` lit les fichiers dans `data/reviews_stream/` en mode streaming, ajoute des colonnes de partition (`année`, `mois`, `jour`), et écrit les données dans le répertoire `bronze/reviews/`.

```bash
python 04_stream_reviews.py
```
[Regardez la vidéo de simulation du streaming](https://drive.google.com/file/d/1RITeTYhvpEI6Xbuwk3Z_4ya3Ohn4vKze/view?usp=sharing)

### **6. Nettoyage et jointure des données**
Le script `05_transform_clean.py` :
- Nettoie les données des annonces et des avis.
- Effectue une jointure entre les deux ensembles de données.
- Stocke les données jointes dans une table Hive `silver_joined`.

```bash
python 05_transform_clean.py
```

### **7. Modélisation Machine Learning**
Le script `06_ml_model.py` effectue plusieurs prédictions en utilisant différents modèles de machine learning. Les données sont chargées depuis la table Hive `silver_joined`, et les résultats sont sauvegardés dans le répertoire `gold/`.

#### **1. Prédiction du prix des annonces**
- **Objectif :** Prédire le prix (`price`) des annonces en fonction de leurs caractéristiques.
- **Modèles testés :**
  - Régression linéaire (`LinearRegression`)
  - Arbre de décision (`DecisionTreeRegressor`)
  - Forêt aléatoire (`RandomForestRegressor`)
- **Processus :**
  - Les modèles sont entraînés sur les données, et le modèle avec le plus faible RMSE (Root Mean Squared Error) est sélectionné.
- **Résultat :**
  - Les prédictions du meilleur modèle sont sauvegardées dans `gold/predictions_price/`.

#### **2. Prédiction de la probabilité de réservation**
- **Objectif :** Prédire si une annonce sera réservée ou non (`reserved`).
- **Modèle utilisé :**
  - Régression logistique (`LogisticRegression`)
- **Processus :**
  - Une colonne binaire `reserved` est créée (1 si `reviews_per_month > 0`, sinon 0).
  - Le modèle est entraîné pour prédire cette colonne en fonction des caractéristiques de l'annonce.
- **Résultat :**
  - Les prédictions sont sauvegardées dans `gold/predictions_reserved/`.

#### **3. Prédiction des notes des avis**
- **Objectif :** Prédire la note moyenne des avis (`review_scores_rating`) des annonces.
- **Modèle utilisé :**
  - Arbre de décision (`DecisionTreeRegressor`)
- **Processus :**
  - Le modèle est entraîné pour prédire les notes des avis en fonction des caractéristiques de l'annonce.
- **Résultat :**
  - Les prédictions sont sauvegardées dans `gold/predictions_rating/`.

#### **4. Analyse des sentiments des commentaires**
- **Objectif :** Analyser les sentiments des commentaires (`comments`) pour déterminer s'ils sont positifs ou négatifs.
- **Modèle utilisé :**
  - Naive Bayes (`NaiveBayes`)
- **Processus :**
  - Une colonne binaire `sentiment` est créée (1 si `review_scores_rating > 8`, sinon 0).
  - Les étapes suivantes sont appliquées :
    1. **Tokenization :** Les commentaires sont divisés en mots.
    2. **Suppression des mots inutiles :** Les mots comme "le", "et", "de" sont supprimés.
    3. **Vectorisation :** Les mots sont convertis en vecteurs numériques.
    4. **Classification :** Le modèle Naive Bayes est entraîné pour prédire les sentiments.
- **Résultat :**
  - Les prédictions sont sauvegardées dans `gold/predictions_sentiment/`.

#### **5. Résumé des données**
- **Objectif :** Sauvegarder les données jointes et nettoyées pour des analyses ultérieures.
- **Processus :**
  - Les données de la table Hive `silver_joined` sont sauvegardées dans `gold/listing_summary/`.

---

### **Exemple de structure des fichiers Gold**
Après l'exécution du script, les prédictions et les données enrichies sont sauvegardées dans les répertoires suivants :
- `gold/predictions_price/` : Prédictions des prix des annonces.
- `gold/predictions_reserved/` : Prédictions de la probabilité de réservation.
- `gold/predictions_rating/` : Prédictions des notes des avis.
- `gold/predictions_sentiment/` : Résultats de l'analyse des sentiments.
- `gold/listing_summary/` : Données jointes et nettoyées pour des analyses ultérieures.

```bash
python 06_ml_model.py
```

### **8. Export des données vers la base de données**
Le script `07_export_to_api.py` :
- Exporte les prédictions depuis le répertoire `gold/` vers des tables PostgreSQL :
  - `listing_predictions_price` : Prédictions des prix des annonces.
  - `listing_predictions_reserved` : Prédictions de la probabilité de réservation.
  - `listing_predictions_rating` : Prédictions des notes des avis.
  - `listing_predictions_sentiment` : Résultats de l'analyse des sentiments.
- Exporte également les données résumées depuis `gold/listing_summary/` vers la table PostgreSQL `listing_summary`.

Une fois les données exportées dans PostgreSQL, elles peuvent être utilisées pour des analyses avancées dans Power BI. Une connexion est établie entre Power BI et PostgreSQL pour visualiser et explorer les données exportées.

De plus, les scripts SQL pour créer les tables nécessaires dans PostgreSQL sont disponibles dans le dossier `sql/`.

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
│   └── inject_reviews.sh                # Script Bash pour simuler un flux de données des avis
├── data/
│   ├── listings.csv                     # Fichier source des annonces Airbnb
│   ├── reviews.csv                      # Fichier source des avis Airbnb
│   ├── reviews_chunks/                  # Répertoire contenant les morceaux d'avis découpés
│   └── reviews_stream/                  # Répertoire simulant un flux de données des avis
├── bronze/                              # Zone Bronze : Données brutes
│   ├── listings/                        # Données des annonces au format Parquet
│   └── reviews/                         # Données des avis au format Parquet
├── silver/                              # Zone Silver : Données nettoyées et jointes
│   └── joined/                          # Données jointes des annonces et des avis
├── gold/                                # Zone Gold : Données prêtes pour l'analyse et les prédictions
│   ├── predictions_price/               # Prédictions des prix des annonces
│   ├── predictions_reserved/            # Prédictions de la probabilité de réservation
│   ├── predictions_rating/              # Prédictions des notes des avis
│   ├── predictions_sentiment/           # Résultats de l'analyse des sentiments
│   └── listing_summary/                 # Résumé des données jointes
├── scripts/                             # Scripts Python pour chaque étape de la pipeline
│   ├── 01_load_listings_to_db.py        # Charger les annonces dans PostgreSQL
│   ├── 02_ingest_listings_from_db.py    # Ingestion des annonces dans la zone Bronze
│   ├── 03_prepare_reviews_chunks.py     # Découper les avis en morceaux pour le streaming
│   ├── 04_stream_reviews.py             # Traiter les avis en mode streaming
│   ├── 05_transform_clean.py            # Nettoyer et joindre les données
│   ├── 06_ml_model.py                   # Modélisation Machine Learning
│   └── 07_export_to_api.py              # Exporter les données vers PostgreSQL
└── requirements.txt                     # Liste des dépendances Python
```

---

## **Notes**
- Assurez-vous que PostgreSQL, Hive et PySpark sont correctement configurés avant d'exécuter les scripts.