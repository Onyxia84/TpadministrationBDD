Pipeline : RAW (MongoDB) → ENRICHED (MongoDB) → SQL (Supabase/Postgres) → API → Dashboard

- Source : OpenFoodFacts (`/api/v2/search`)
- Stockage RAW : MongoDB `raw_products`
- Format RAW :
  - `source`
  - `fetched_at` (UTC ISO)
  - `raw_hash` (SHA-256 du JSON canonique du payload)
  - `payload` (réponse produit brute, non modifiée)
- Idempotence : index unique Mongo sur `raw_hash`

- Python 3.13+
- MongoDB (service Windows ou installation locale)

bash
cd collector
python -m venv .venv
# Windows: .venv\Scripts\activate
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# éditer OFF_USER_AGENT (email)
python main.py --limit 300 --page-size 100
