from market_data_db import MarketDataDB
from sqlalchemy import text

def fix_industry_names():
    db = MarketDataDB()
    
    # Mapping: "Wrong Name in DB" -> "Correct Name Displayed"
    corrections = {
        "Semiondutors": "Semiconductors",
        "Softwre - Infrstruture": "Software - Infrastructure",
        "Softwre - Applition": "Software - Application",
        "Biotehnology": "Biotechnology",
        "Agriulturl Inputs": "Agricultural Inputs",
        "Aerospe & Defense": "Aerospace & Defense",
        "Medil Devies": "Medical Devices",
        "Bnks - Regionl": "Banks - Regional",
        "Bnks - Diversified": "Banks - Diversified",
        "Communition Equipment": "Communication Equipment"
    }
    
    with db.engine.begin() as conn:
        print("--- Fixing Industry Names ---")
        for wrong, correct in corrections.items():
            # Check if wrong exists
            res = conn.execute(text("SELECT id FROM industries WHERE name = :w"), {"w": wrong}).fetchone()
            
            if res:
                ind_id = res[0]
                print(f"fixing: '{wrong}' -> '{correct}' (ID: {ind_id})")
                
                # Update
                conn.execute(
                    text("UPDATE industries SET name = :c WHERE id = :id"),
                    {"c": correct, "id": ind_id}
                )
            else:
                print(f"Skipping: '{wrong}' not found in DB.")
                
    print("\n✅ Industry names updated successfully!")
    print("Refresh your home.html to see correct spellings.")

if __name__ == "__main__":
    fix_industry_names()
