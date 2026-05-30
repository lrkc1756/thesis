import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.svm import SVC
from sklearn.ensemble import RandomForestClassifier
from sklearn.cluster import DBSCAN
from sklearn.metrics import classification_report, accuracy_score, cohen_kappa_score

# ==========================================
# 1. LOAD AND PREPARE DATA
# ==========================================
# Replace 'your_dataset.csv' with your actual file path (e.g., Package 06 data)
df = pd.read_csv('your_dataset.csv') 

# Clean dataset: Drop missing text or label rows
df = df.dropna(subset=['review_text', 'human_label'])

# Map text labels to integers if needed (e.g., 'real': 1, 'fake': 0)
# Adjust these strings to match the exact labels in your CSV file
label_mapping = {'real': 1, 'fake': 0}
df['encoded_label'] = df['human_label'].map(label_mapping)

X = df['review_text']
y = df['encoded_label']

# Replicate your exact SetFit data split (80% training, 20% test)
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.20, random_state=42, stratify=y
)

# ==========================================
# 2. FEATURE EXTRACTION (TF-IDF)
# ==========================================
# Converts raw forum text into a mathematical matrix of token weights
print("Extracting TF-IDF features...")
vectorizer = TfidfVectorizer(max_features=5000, stop_words='english', ngram_range=(1, 2))

X_train_tfidf = vectorizer.fit_transform(X_train)
X_test_tfidf = vectorizer.transform(X_test)

# ==========================================
# 3. BASELINE 1: SUPPORT VECTOR MACHINE (SVM)
# ==========================================
print("\n--- Training Linear SVM ---")
svm_model = SVC(kernel='linear', C=1.0, random_state=42)
svm_model.fit(X_train_tfidf, y_train)
svm_preds = svm_model.predict(X_test_tfidf)

print("SVM Accuracy:", accuracy_score(y_test, svm_preds))
print("SVM Cohen's Kappa:", cohen_kappa_score(y_test, svm_preds))
print(classification_report(y_test, svm_preds, target_names=['fake', 'real']))

# ==========================================
# 4. BASELINE 2: RANDOM FOREST (RF)
# ==========================================
print("\n--- Training Random Forest ---")
rf_model = RandomForestClassifier(n_estimators=100, max_depth=None, random_state=42)
rf_model.fit(X_train_tfidf, y_train)
rf_preds = rf_model.predict(X_test_tfidf)

print("Random Forest Accuracy:", accuracy_score(y_test, rf_preds))
print("Random Forest Cohen's Kappa:", cohen_kappa_score(y_test, rf_preds))
print(classification_report(y_test, rf_preds, target_names=['fake', 'real']))

# ==========================================
# 5. BASELINE 3: DBSCAN (UNSUPERVISED CLUSTERING)
# ==========================================
print("\n--- Running DBSCAN Clustering ---")
# Using cosine distance because it is optimal for sparse text vectors
dbscan = DBSCAN(eps=0.5, min_samples=3, metric='cosine')
# Run on the whole dataset or just test set to look for natural clusters
cluster_labels = dbscan.fit_predict(X_test_tfidf)

# Check how many clusters were found (-1 indicates noise points)
unique_clusters = np.unique(cluster_labels)
print(f"DBSCAN found {len(unique_clusters) - (1 if -1 in cluster_labels else 0)} distinct clusters.")
print("Distribution of samples across clusters:", pd.Series(cluster_labels).value_counts())