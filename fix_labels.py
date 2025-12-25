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
        "Advertising Agenies": "Advertising Agencies",
        "Airports & Air Servies": "Airports & Air Services",
        "Brodsting": "Broadcasting",
        "Bnks - Regionl": "Banks - Regional",
        "Bnks - Diversified": "Banks - Diversified",
        "Communition Equipment": "Communication Equipment",
        "Auto Prts": "Auto Parts",
        "Auto Mnufturers": "Auto Manufacturers",
        "Auto & Truk Delerships": "Auto & Truck Dealerships",
        "Apprel Mnufturing": "Apparel Manufacturing",
        "Apprel Retil": "Apparel Retail",
        "Asset Mngement": "Asset Management",
        "Beverges - Brewers": "Beverages - Brewers",
        "Beverges - Non-Aloholi": "Beverages - Non-Alcoholic",
        "Beverges - Wineries & Distille": "Beverages - Wineries & Distilleries",
        "Building Mterils": "Building Materials",
        "Building Produts & Equipment": "Building Products & Equipment",
        "Business Equipment & Supplies": "Business Equipment & Supplies",
        "Chemils": "Chemicals",
        "Coking Col": "Coking Coal",
        "Computer Hrdwre": "Computer Hardware",
        "Confetioners": "Confectioners",
        "Conglomertes": "Conglomerates",
        "Consulting Servies": "Consulting Services",
        "Consumer Eletronis": "Consumer Electronics",
        "Cpitl Mrkets": "Capital Markets",
        "Credit Servies": "Credit Services",
        "Deprtment Stores": "Department Stores",
        "Dignostis & Reserh": "Diagnostics & Research",
        "Disount Stores": "Discount Stores",
        "Drug Mnufturers - Generl": "Drug Manufacturers - General",
        "Drug Mnufturers - Speilty & Ge": "Drug Manufacturers - Specialty & Generic",
        "Edution & Trining Servies": "Education & Training Services",
        "Eletril Equipment & Prts": "Electrical Equipment & Parts",
        "Eletroni Components": "Electronic Components",
        "Eletroni Gming & Multimedi": "Electronic Gaming & Multimedia",
        "Eletronis & Computer Distriuti": "Electronics & Computer Distribution",
        "Engineering & Constrution": "Engineering & Construction",
        "Entertinment": "Entertainment",
        "Finnil Conglomertes": "Financial Conglomerates",
        "Finnil Dt & Stok Exhnges": "Financial Data & Stock Exchanges",
        "Food Distriution": "Food Distribution",
        "Footwer & Aessories": "Footwear & Accessories",
        "Frm & Hevy Constrution Mhinery": "Farm & Heavy Construction Machinery",
        "Frm Produts": "Farm Products",
        "Furnishings, Fixtures & Applin": "Furnishings, Fixtures & Appliances",
        "Gmling": "Gambling",
        "Groery Stores": "Grocery Stores",
        "Helth Informtion Servies": "Health Information Services",
        "Helthre Plns": "Healthcare Plans",
        "Home Improvement Retil": "Home Improvement Retail",
        "Household & Personl Produts": "Household & Personal Products",
        "Industril Distriution": "Industrial Distribution",
        "Informtion Tehnology Servies": "Information Technology Services",
        "Infrstruture Opertions": "Infrastructure Operations",
        "Insurne - Diversified": "Insurance - Diversified",
        "Insurne - Life": "Insurance - Life",
        "Insurne - Property & Csulty": "Insurance - Property & Casualty",
        "Insurne - Reinsurne": "Insurance - Reinsurance",
        "Insurne - Speilty": "Insurance - Specialty",
        "Insurne Brokers": "Insurance Brokers",
        "Integrted Freight & Logistis": "Integrated Freight & Logistics",
        "Internet Content & Informtion": "Internet Content & Information",
        "Internet Retil": "Internet Retail",
        "Lumer & Wood Prodution": "Lumber & Wood Production",
        "Medil Cre Filities": "Medical Care Facilities",
        "Medil Distriution": "Medical Distribution",
        "Medil Instruments & Supplies": "Medical Instruments & Supplies",
        "Metl Frition": "Metal Fabrication",
        "Mortgge Finne": "Mortgage Finance",
        "Mrine Shipping": "Marine Shipping",
        "Oil & Gs Drilling": "Oil & Gas Drilling",
        "Oil & Gs Equipment & Servies": "Oil & Gas Equipment & Services",
        "Oil & Gs Explortion & Produtio": "Oil & Gas Exploration & Production",
        "Oil & Gs Integrted": "Oil & Gas Integrated",
        "Oil & Gs Midstrem": "Oil & Gas Midstream",
        "Oil & Gs Refining & Mrketing": "Oil & Gas Refining & Marketing",
        "Other Industril Metls & Mining": "Other Industrial Metals & Mining",
        "Other Preious Metls & Mining": "Other Precious Metals & Mining",
        "Personl Servies": "Personal Services",
        "Phrmeutil Retilers": "Pharmaceutical Retailers",
        "Pkged Foods": "Packaged Foods",
        "Pkging & Continers": "Packaging & Containers",
        "Pollution & Tretment Controls": "Pollution & Treatment Controls",
        "Pper & Pper Produts": "Paper & Paper Products",
        "Pulishing": "Publishing",
        "REIT - Diversified": "REIT - Diversified",
        "REIT - Helthre Filities": "REIT - Healthcare Facilities",
        "REIT - Hotel & Motel": "REIT - Hotel & Motel",
        "REIT - Industril": "REIT - Industrial",
        "REIT - Mortgge": "REIT - Mortgage",
        "REIT - Offie": "REIT - Office",
        "REIT - Residentil": "REIT - Residential",
        "REIT - Retil": "REIT - Retail",
        "REIT - Speilty": "REIT - Specialty",
        "Rel Estte - Development": "Real Estate - Development",
        "Rel Estte - Diversified": "Real Estate - Diversified",
        "Rel Estte Servies": "Real Estate Services",
        "Rilrods": "Railroads"
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
