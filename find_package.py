import json
from collections import Counter, defaultdict

seen = set()

# Full path to your dataset
filename = r"C:\SS2026\Thesis\llms\dataset\M12_final_v1_to_dataset.jsonl"

counts = Counter()
unique_urls = defaultdict(set)

# FIRST PASS: count + collect unique URLs
with open(filename, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue

        try:
            obj = json.loads(line)

            pkg = str(obj["package_id"])
            url = obj.get("url")

            counts[pkg] += 1

            if url:
                unique_urls[pkg].add(url)

        except:
            continue


# SECOND PASS: print summary per package
seen = set()

with open(filename, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue

        try:
            obj = json.loads(line)
            pkg = str(obj["package_id"])

            if pkg not in seen:
                seen.add(pkg)

                print("\n========================")
                print(f"PACKAGE: {pkg}")
                print(f"TOTAL REVIEWS: {counts[pkg]}")
                print(f"UNIQUE URLS: {len(unique_urls[pkg])}")
                print("EXAMPLE URL:", obj.get("url"))
                print("TITLE:", obj.get("title"))

        except:
            continue

#save all reviews from package 6 with the same URL for simple testing
target = "samsung-galaxy-s24-series"
count = 0
missed = 0

with open("package6_s24.jsonl", "w", encoding="utf-8") as out:
    with open(filename, "r", encoding="utf-8") as f:
        for line in f:
            try:
                obj = json.loads(line)
                if str(obj.get("package_id")).lstrip("0") == "6":
                    url = obj.get("url", "")
                    if target in url.lower():  # case-insensitive
                        out.write(json.dumps(obj, ensure_ascii=False) + "\n")
                        count += 1
                    else:
                        if missed < 10:  # print first 10 misses to inspect
                            print("MISSED URL:", url)
                        missed += 1
            except:
                continue

print(f"Saved: {count} | Missed from pkg 6: {missed}")

# Save packages 2, 3, and 6 into separate files
output_files = {
    "2": open("package_2.jsonl", "w", encoding="utf-8"),
    "3": open("package_3.jsonl", "w", encoding="utf-8"),
    "6": open("package_6.jsonl", "w", encoding="utf-8"),
}

with open(filename, "r", encoding="utf-8") as f:
    for line in f:

        line = line.strip()

        if not line:
            continue

        try:
            obj = json.loads(line)

            pkg = str(obj["package_id"]).lstrip("0")

            if pkg in output_files:
                output_files[pkg].write(line + "\n")

        except json.JSONDecodeError:
            print("Skipping invalid JSON line.")

# Close output files
for file in output_files.values():
    file.close()

