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
# 1. LOAD DATA
# ==========================================

df = pd.read_json(
    r"C:\SS2026\Thesis\package6_augmented_random1000.jsonl",
    lines=True
)

print("\nColumns:")
print(df.columns)

print("\nRaw label distribution:")
print(df["label"].value_counts(dropna=False))

# ==========================================
# 2. CLEAN DATA
# ==========================================

# Drop missing values
df = df.dropna(subset=["text", "label"])

# Clean label text
df["label"] = (
    df["label"]
    .astype(str)
    .str.lower()
    .str.strip()
)

# Remove empty labels
df = df[df["label"] != ""]

# Remove text inside parentheses (fake (misinformation) -> fake)
df["label"] = df["label"].str.replace(r"\s*\(.*\)", "", regex=True)

# Normalize whitespace
df["label"] = df["label"].str.replace(r"\s+", " ", regex=True).str.strip()

print("\nCleaned label distribution:")
print(df["label"].value_counts())

# ==========================================
# 3. MAP LABELS
# ==========================================

label_mapping = {
    "fake": 0,
    "real": 1,
    "unrelated": 2
}

# Keep only valid labels
df = df[df["label"].isin(label_mapping.keys())]

df["encoded_label"] = df["label"].map(label_mapping)

# Safety check
print("\nEncoded label distribution:")
print(df["encoded_label"].value_counts())

if df["encoded_label"].isna().sum() > 0:
    raise ValueError("Unmapped labels still exist!")

X = df["text"]
y = df["encoded_label"]

# ==========================================
# 4. TRAIN / TEST SPLIT (SAFE)
# ==========================================

# If dataset too small, stratify can fail → fallback
try:
    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.20,
        random_state=42,
        stratify=y
    )
except ValueError:
    print("Stratified split failed → using non-stratified split")
    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.20,
        random_state=42
    )

print(f"Training samples: {len(X_train)}")
print(f"Test samples: {len(X_test)}")

print("\nTest label distribution:")
print(y_test.value_counts())

# ==========================================
# 5. TF-IDF
# ==========================================

vectorizer = TfidfVectorizer(
    max_features=5000,
    stop_words="english",
    ngram_range=(1, 2)
)

X_train_tfidf = vectorizer.fit_transform(X_train)
X_test_tfidf = vectorizer.transform(X_test)

# ==========================================
# 6. SVM
# ==========================================

print("\n==============================")
print("TRAINING SVM")
print("==============================")

svm_model = SVC(kernel="linear", C=1.0, random_state=42)
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
        labels=[0, 1, 2],
        target_names=["fake", "real", "unrelated"],
        zero_division=0
    )
)

# ==========================================
# 7. RANDOM FOREST
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
        labels=[0, 1, 2],
        target_names=["fake", "real", "unrelated"],
        zero_division=0
    )
)

# ==========================================
# 8. DBSCAN (OPTIONAL / EXPERIMENTAL)
# ==========================================

print("\n==============================")
print("DBSCAN")
print("==============================")

dbscan = DBSCAN(
    eps=0.5,
    min_samples=3,
    metric="cosine"
)

cluster_labels = dbscan.fit_predict(X_test_tfidf)

print("\nClusters found:",
      len(set(cluster_labels)) - (1 if -1 in cluster_labels else 0))

print("\nCluster distribution:")
print(pd.Series(cluster_labels).value_counts().sort_index())