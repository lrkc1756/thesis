#calculate Cohen's Kappa for agreement between human and LLM Labels in CSV file
import pandas as pd
from sklearn.metrics import cohen_kappa_score

# Load your CSV file
df = pd.read_csv("your_file.csv")

# Check required columns exist
required_cols = {"review_id", "human_label", "LLM_label"}
if not required_cols.issubset(df.columns):
    raise ValueError(f"CSV must contain columns: {required_cols}")

# Drop rows with missing labels (optional but recommended)
df = df.dropna(subset=["human_label", "LLM_label"])

# Extract labels
human = df["human_label"]
llm = df["LLM_label"]

# Compute Cohen's Kappa
kappa = cohen_kappa_score(human, llm)

print(f"Cohen's Kappa: {kappa:.4f}")