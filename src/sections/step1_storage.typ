#import "../utils.typ": screenshot-placeholder

== Step 1: Configurazione Storage (S3)
È stato predisposto lo storage layer creando una struttura di bucket S3 per separare i dati grezzi dai dati trasformati.
Sono stati creati i seguenti bucket (o cartelle):
- `raw-data`: Contenitore per i file CSV originali (Prezzi e Google Trends).
- `silver-data`: Contenitore destinato ai file Parquet processati.
- `scripts`: Repo per i codici Python/PySpark di Glue.

// Screenshot 1
#screenshot-placeholder(
  "Console S3 che mostra i bucket (o le cartelle) raw, silver e scripts creati.",
  "s3_structure.png"
)
