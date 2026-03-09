# gestion.py
import requests
import pandas as pd
import os
from datetime import datetime

def update_outages_history(output_file="hydroquebec_history.csv"):
    version_url = "https://pannes.hydroquebec.com/pannes/donnees/v3_0/bisversion.json"
    version = requests.get(version_url).text.strip('"')

    data_url = f"https://pannes.hydroquebec.com/pannes/donnees/v3_0/bismarkers{version}.json"
    data = requests.get(data_url).json()

    rows = []
    for p in data.get("pannes", []):
        unique_id = f"{p[1]}_{p[4]}"
        rows.append({
            "outage_id": unique_id,
            "customers_affected": p[0],
            "start_time": p[1],
            "estimated_restore": p[2],
            "status_code": p[5],
            "cause_code": p[6],
            "municipality_id": p[8],
            "coordinates": p[4],
            "captured_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })

    new_df = pd.DataFrame(rows)
    if new_df.empty:
        print("Aucune nouvelle panne.")
        return

    new_df[["lon","lat"]] = new_df["coordinates"].str.strip("[]").str.split(",", expand=True).astype(float)
    new_df = new_df.drop(columns=["coordinates"])

    # Cause label
    def classify_cause(code):
        try:
            c = int(code)
            if 21 <= c <= 26: return "weather"
            if c == 51: return "vegetation"
            if c in [52,53]: return "animal"
            if 11 <= c <= 15 or c in [58,70,72,73,74,79]: return "equipment"
            if 31 <= c <= 57: return "accident"
            return "other"
        except:
            return "unknown"

    new_df["cause_label"] = new_df["cause_code"].apply(classify_cause)

    # Fusion avec CSV existant
    if os.path.exists(output_file):
        existing_df = pd.read_csv(output_file)
        combined_df = pd.concat([existing_df, new_df]).drop_duplicates(subset=["outage_id"], keep="last")
    else:
        combined_df = new_df

    combined_df.to_csv(output_file, index=False)
    print(f"CSV mis à jour : {len(combined_df)} pannes totales.")

if __name__ == "__main__":
    update_outages_history()
