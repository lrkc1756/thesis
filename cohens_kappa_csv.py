import pandas as pd
from sklearn.metrics import cohen_kappa_score

# Load files
human_df = pd.read_json("C:\SS2026\Thesis\package6_s24.jsonl", lines=True)
llm_df = pd.read_csv("C:\SS2026\Thesis\package6_s24_llama_test4.csv")

# Keep only needed columns
human_df = human_df[["cid", "label"]]
llm_df = llm_df[["cid", "label"]]

# Rename columns so they don't collide
human_df = human_df.rename(columns={"label": "human_label"})
llm_df = llm_df.rename(columns={"label": "llm_label"})

# Merge on shared review ID
merged = pd.merge(human_df, llm_df, on="cid")

# Remove missing labels
merged = merged.dropna(subset=["human_label", "llm_label"])

# Calculate Cohen's Kappa
kappa = cohen_kappa_score(
    merged["human_label"],
    merged["llm_label"]
)

print(f"Cohen's Kappa: {kappa:.4f}")