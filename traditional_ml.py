import pandas as pd
import numpy as np

from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.svm import SVC
from sklearn.ensemble import RandomForestClassifier
from sklearn.cluster import DBSCAN
from sklearn.metrics import (
    classification_report,
    accuracy_score,
    cohen_kappa_score
)

# ==========================================
# 1. LOAD AND PREPARE DATA
# ==========================================

print("Loading dataset...")

df = pd.read_json("C:\\SS2026\\Thesis\\package6_s24_augmented_random1000 (3).jsonl", lines=True)

print("\nColumns:")
print(df.columns)

print("\nLabel distribution:")
print(df["label"].value_counts())

# Remove rows with missing values
df = df.dropna(subset=["text", "label"])

# Remove rows with missing text
df = df.dropna(subset=["text"])

# Remove blank labels
df = df[df["label"].astype(str).str.strip() != ""]

# Map labels to integers
label_mapping = {
    "fake": 0,
    "real": 1,
    "unrelated": 2
}

df["encoded_label"] = df["label"].map(label_mapping)

# Check for unmapped labels
if df["encoded_label"].isna().sum() > 0:
    print("\nWARNING: Unmapped labels found:")
    print(df[df["encoded_label"].isna()]["label"].unique())
    raise ValueError("Some labels were not mapped.")

X = df["text"]
y = df["encoded_label"]

print("\nEncoded label distribution:")
print(y.value_counts())

# ==========================================
# 2. TRAIN / TEST SPLIT
# ==========================================

print("\nSplitting dataset...")

X_train, X_test, y_train, y_test = train_test_split(
    X,
    y,
    test_size=0.20,
    random_state=42,
    stratify=y
)

print(f"Training samples: {len(X_train)}")
print(f"Test samples: {len(X_test)}")

# ==========================================
# 3. TF-IDF FEATURE EXTRACTION
# ==========================================

print("\nExtracting TF-IDF features...")

vectorizer = TfidfVectorizer(
    max_features=5000,
    stop_words="english",
    ngram_range=(1, 2)
)

X_train_tfidf = vectorizer.fit_transform(X_train)
X_test_tfidf = vectorizer.transform(X_test)

print("TF-IDF matrix created.")

# ==========================================
# 4. SUPPORT VECTOR MACHINE
# ==========================================

print("\n==============================")
print("TRAINING LINEAR SVM")
print("==============================")

svm_model = SVC(
    kernel="linear",
    C=1.0,
    random_state=42
)

svm_model.fit(X_train_tfidf, y_train)

svm_preds = svm_model.predict(X_test_tfidf)

print("\nSVM Accuracy:")
print(accuracy_score(y_test, svm_preds))

print("\nSVM Cohen's Kappa:")
print(cohen_kappa_score(y_test, svm_preds))

print("\nSVM Classification Report:")
print(
    classification_report(
        y_test,
        svm_preds,
        target_names=["fake", "real", "unrelated"]
    )
)

# ==========================================
# 5. RANDOM FOREST
# ==========================================

print("\n==============================")
print("TRAINING RANDOM FOREST")
print("==============================")

rf_model = RandomForestClassifier(
    n_estimators=100,
    random_state=42
)

rf_model.fit(X_train_tfidf, y_train)

rf_preds = rf_model.predict(X_test_tfidf)

print("\nRandom Forest Accuracy:")
print(accuracy_score(y_test, rf_preds))

print("\nRandom Forest Cohen's Kappa:")
print(cohen_kappa_score(y_test, rf_preds))

print("\nRandom Forest Classification Report:")
print(
    classification_report(
        y_test,
        rf_preds,
        target_names=["fake", "real", "unrelated"]
    )
)

# ==========================================
# 6. DBSCAN CLUSTERING
# ==========================================

print("\n==============================")
print("RUNNING DBSCAN")
print("==============================")

dbscan = DBSCAN(
    eps=0.5,
    min_samples=3,
    metric="cosine"
)

cluster_labels = dbscan.fit_predict(X_test_tfidf)

unique_clusters = np.unique(cluster_labels)

num_clusters = len(unique_clusters)

if -1 in unique_clusters:
    num_clusters -= 1

print(f"\nClusters found: {num_clusters}")

print("\nCluster distribution:")
print(pd.Series(cluster_labels).value_counts().sort_index())