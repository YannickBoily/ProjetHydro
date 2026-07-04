import requests
import pandas as pd
import os
from datetime import datetime

def safe_get(arr, idx):
    """Retourne arr[idx] ou None si index absent."""
    try:
        return arr[idx]
    except (IndexError, TypeError):
        return None

def classify_cause(code):
    """Classification des causes Hydro-Québec."""
    try:
        if code is None or code == "":
            return "unknown"

        c = int(float(code))

        if 11 <= c <= 15 or c in [58, 70, 72, 73, 74, 79]:
            return "equipment"
        if 21 <= c <= 26:
            return "weather"
        if c == 51:
            return "vegetation"
        if c in [52, 53]:
            return "animal"
        if 31 <= c <= 34 or c in [41,42,43,44,54,55,56,57]:
            return "accident"

        return "other"
    except:
        return "unknown"

def update_outages_history(output_file="data/raw/hydroquebec_history.csv"):
    """Récupère les pannes Hydro-Québec et met à jour un CSV propre."""

    try:
        # 1. Version BIS
        version_url = "https://pannes.hydroquebec.com/pannes/donnees/v3_0/bisversion.json"
        resp = requests.get(version_url, timeout=10)
        resp.raise_for_status()
        version = resp.text.strip('"')

        print(f"[{datetime.now().strftime('%H:%M:%S')}] Version BIS : {version}")

        # 2. Données pannes
        data_url = f"https://pannes.hydroquebec.com/pannes/donnees/v3_0/bismarkers{version}.json"
        resp = requests.get(data_url, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        rows = []
        for p in data.get("pannes", []):

            # Skip lignes suspectes
            if not isinstance(p, list) or len(p) < 9:
                print("⚠️ Ligne ignorée (malformée):", p)
                continue

            lon_lat = safe_get(p, 4)

            rows.append({
                "outage_id": f"{safe_get(p,8)}_{safe_get(p,4)}_{safe_get(p,1)}",
                "customers_affected": safe_get(p,0),
                "start_time": safe_get(p,1),
                "estimated_restore": safe_get(p,2),
                "status_code": safe_get(p,5),
                "cause_code": safe_get(p,7),   # ✅ FIXED
                "municipality_id": safe_get(p,8),
                "coordinates": lon_lat,
                "captured_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })

        if not rows:
            print("Aucune donnée récupérée.")
            return

        df = pd.DataFrame(rows)

        # Nettoyage coordonnées sécurisé
        coords = df["coordinates"].astype(str).str.strip("[]").str.split(",", expand=True)
        df["lon"] = pd.to_numeric(coords[0], errors="coerce")
        df["lat"] = pd.to_numeric(coords[1], errors="coerce")
        df.drop(columns=["coordinates"], inplace=True)

        # Dates
        df["start_time"] = pd.to_datetime(df["start_time"], errors="coerce")
        df["estimated_restore"] = pd.to_datetime(df["estimated_restore"], errors="coerce")

        # Cause
        df["cause_label"] = df["cause_code"].apply(classify_cause)

        # Status mapping (optionnel mais utile)
        status_map = {
            "A": "assigned",
            "L": "working",
            "R": "en_route",
            "N": "new"
        }
        df["status"] = df["status_code"].map(status_map)

        # Colonnes propres
        expected_cols = [
            "outage_id","customers_affected","start_time","estimated_restore",
            "status_code","status","cause_code","cause_label","municipality_id",
            "captured_at","lon","lat"
        ]
        df = df.reindex(columns=expected_cols)

        # Merge avec existant sans casser
        if os.path.exists(output_file):
            existing_df = pd.read_csv(output_file, on_bad_lines="skip")

            combined_df = pd.concat([existing_df, df], ignore_index=True)

            # 🔥 garder historique complet (pas de drop agressif)
            combined_df = combined_df.drop_duplicates(
                subset=["outage_id", "captured_at"],
                keep="last"
            )
        else:
            combined_df = df

        combined_df.to_csv(output_file, index=False)

        print(f"✅ CSV mis à jour : {len(combined_df)} lignes")

    except Exception as e:
        print(f"❌ Erreur : {e}")


if __name__ == "__main__":
    update_outages_history()

