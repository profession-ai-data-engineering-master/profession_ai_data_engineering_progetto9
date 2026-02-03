#import "../utils.typ": screenshot-placeholder

== Step 3: ETL Processing (AWS Glue)
Sono stati sviluppati due job AWS Glue per processare parallelamente BTC e XMR.
Ogni job esegue:
1.  Lettura dal bucket `raw`.
2.  Handling dei nulli (prezzi a -1).
3.  Calcolo media mobile (10 giorni).
4.  Scrittura in formato Parquet nel bucket `silver`.

// Screenshot 3
#screenshot-placeholder(
  "Visual Editor di Glue o lo script PySpark che mostra la logica di trasformazione.",
  "glue_job_logic.png"
)
