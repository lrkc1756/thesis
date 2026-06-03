import pandas as pd
from sklearn.model_selection import train_test_split

# 1. Load your massive M12 dataset
df = pd.read_json(r"C:\SS2026\Thesis\llms\dataset\M12_final_v1_to_dataset.jsonl", lines=True)

# 2. Downsample to 500 reviews while preserving class balance
if len(df) > 500:
    _, df_sampled = train_test_split(
        df, 
        test_size=500, 
        stratify=df['label'], 
        random_state=42
    )
else:
    df_sampled = df

# 3. Split into Train (80%) and Test (20%)
train_df, test_df = train_test_split(
    df_sampled, 
    test_size=0.20, 
    stratify=df_sampled['label'], 
    random_state=42
)

# 4. Save them out
# ONLY send train_df to LLM/SetFit augmentation loop!
train_df.to_json("m12_train_ready_for_aug.jsonl", orient="records", lines=True)
test_df.to_json("m12_test_frozen.jsonl", orient="records", lines=True)
print(f"Done! Train size: {len(train_df)}, Test size: {len(test_df)}")