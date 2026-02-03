#import "../utils.typ": screenshot-placeholder

== Step 5: Data Warehousing (Redshift)
Per l'analisi finale, è stato configurato un cluster Amazon Redshift (o Serverless workgroup). È stata definita la DDL per le tabelle finali che ospiteranno i dati unificati (`data`, `prezzo`, `trend`).

// Screenshot 2
#screenshot-placeholder(
  "Console Redshift (Query Editor v2) con la query di creazione tabella o la vista del cluster attivo.",
  "redshift_setup.png"
)
