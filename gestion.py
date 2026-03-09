import requests
import pandas as pd
import os
import time
from datetime import datetime

def update_outages_history(output_file="hydroquebec_history.csv"):
    """
    Récupère, nettoie et accumule les pannes d'Hydro-Québec dans un historique CSV.
    """
    try:
        # 1. Récupération de la version actuelle
        version_url = "https://pannes.hydroquebec.com/pannes/donnees/v3_0/bisversion.json"
        response_v = requests.get(version_url, timeout=10)
        version = response_v.text.strip('"')
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Vérification version : {version}")

        # 2. Récupération des données de pannes
        data_url = f"https://pannes.hydroquebec.com/pannes/donnees/v3_0/bismarkers{version}.json"
        data = requests.get(data_url, timeout=10).json()

        if not data.get("pannes"):
            print("Aucune panne en cours actuellement.")
            return

        # 3. Traitement des données
        rows = []
        for p in data["pannes"]:
            # On crée un ID unique combinant début et lieu pour éviter les doublons au fil des heures
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

        # Nettoyage des coordonnées
        new_df[["lon", "lat"]] = new_df["coordinates"].str.strip("[]").str.split(",", expand=True).astype(float)
        new_df = new_df.drop(columns=["coordinates"])

        # Traduction des causes (Logique ML)
        def classify_cause(code):
            try:
                c = int(code)
                if 21 <= c <= 26: return "weather"
                if c == 51: return "vegetation"
                if c in [52, 53]: return "animal"
                if 11 <= c <= 15 or c in [58, 70, 72, 73, 74, 79]: return "equipment"
                if 31 <= c <= 57: return "accident"
                return "other"
            except:
                return "unknown"

        new_df["cause_label"] = new_df["cause_code"].apply(classify_cause)

        # 4. Fusion avec le fichier existant
        if os.path.exists(output_file):
            existing_df = pd.read_csv(output_file)
            # On combine et on garde la version la plus récente de chaque panne (keep='last')
            combined_df = pd.concat([existing_df, new_df]).drop_duplicates(subset=["outage_id"], keep="last")
        else:
            combined_df = new_df

        # 5. Sauvegarde
        combined_df.to_csv(output_file, index=False)
        print(f"--> Succès : {len(combined_df)} pannes totales dans l'historique.")

    except Exception as e:
        print(f"Erreur lors de la mise à jour : {e}")

# --- BOUCLE D'EXÉCUTION ---
if __name__ == "__main__":
    print("Démarrage du collecteur d'historique Hydro-Québec...")
    print("Appuyez sur Ctrl+C pour arrêter.")
    
    while True:
        update_outages_history()
        # On attend 15 minutes (900 secondes) avant la prochaine vérification
        time.sleep(900)