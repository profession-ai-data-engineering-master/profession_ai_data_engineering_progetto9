#import "../utils.typ": screenshot-placeholder

== Step 6: Visualization (Amazon QuickSight)

Amazon QuickSight permette di creare dashboard interattive, serverless, connettendosi direttamente ai dati presenti nel Data Warehouse. In questo step colleghiamo QuickSight a Redshift per visualizzare prezzi, medie mobili e trend di interesse.

=== 1. Configurazione Iniziale e Data Source

1.  Dalla Console AWS, naviga su *QuickSight*. Se è il primo accesso, segui il wizard per la creazione dell'account (l'edizione *Standard* è sufficiente, ma spesso viene proposta la *Enterprise* in trial).
2.  Assicurati che QuickSight abbia i permessi per accedere a Redshift (fondamentale in VPC):
    -   Clicca sull'icona utente in alto a destra -> *Manage QuickSight*.
    -   Vai su *Security & permissions*.
    -   Sotto *QuickSight access to AWS services*, clicca *Manage* (o *Add or remove*).
    -   Spunta *Amazon Redshift*. Se richiesto, seleziona esplicitamente il workgroup Serverless o il Cluster creato nello Step 5.
3.  Torna alla home di QuickSight, vai su *Datasets* -> *New dataset*.
4.  Seleziona *Redshift (Auto-discovered)* (se non appare, usa "Redshift Manual Connect").
    -   *Data source name*: `CryptoRedshiftSource`.
    -   *Instance ID*: Seleziona il workgroup Redshift Serverless dal menu a tendina.
    -   *Database name*: `dev` (o il nome DB scelto nello Step 5).
    -   *Username/Password*: Usa le credenziali admin impostate alla creazione del workgroup.
    -   Clicca *Create data source*.

#screenshot-placeholder(
  "Creazione del Data Source Redshift in QuickSight con connessione validata.",
  "quicksight_dataset_created.png"
)

=== 2. Creazione Dataset e SPICE vs Direct Query

1.  Nella schermata successiva (selezione tabelle):
    -   *Schema*: Seleziona `cryptodata`.
    -   *Table*: Seleziona `crypto_market`.
2.  Clicca *Select*.
3.  Verrà chiesto di scegliere tra *Import to SPICE* e *Query directly*.
    -   *Scegli SPICE*: È il motore in-memory di QuickSight. Per dataset piccoli come il nostro (< 1GB), garantisce performance istantanee senza gravare su Redshift ad ogni click.
    -   *Direct Query* si userebbe per dataset enormi o per dati "live" al secondo (non è il nostro caso).
4.  Clicca *Visualize*.

=== 3. Costruzione della Dashboard (Analysis)

Si apre l'interfaccia di analisi. Creiamo visualizzazioni mirate.

==== Visual 1: Prezzo vs Media Mobile (Time Series)
1.  Nel pannello *Visual types* (in basso a sinistra), seleziona l'icona *Line chart*.
2.  Trascina i campi dal pannello *Fields list* ai *Field wells* (in alto):
    -   *X axis*: `date` (assicurati che sia impostato come *Date* e aggregazione *Day*).
    -   *Value*: Trascina `price` e `moving_avg_10d`.
3.  *Aggiungi un Filtro per Coin*:
    -   Clicca sull'icona *Filter* (imbuto) nella barra sinistra.
    -   Clicca *ADD FILTER* -> seleziona `coin`.
    -   Clicca sui tre puntini accanto al nuovo filtro -> *Add to sheet*.
    -   Nel pannello di controllo apparso nel foglio, clicca l'icona matita ("Format control") e scegli *Style*: "Dropdown" e *Type*: "Single select".
    -   Seleziona "BTC" come default.

#screenshot-placeholder(
  "Analisi QuickSight: Line chart con Prezzo e Media Mobile, filtrato su BTC.",
  "quicksight_analysis_line_chart.png"
)

==== Visual 2: Prezzo vs Trend Google (Dual Axis)
1.  Clicca *+ Add* (in alto a sinistra) -> *Add visual*.
2.  Seleziona *Line chart* (oppure *Dual Axis Line Chart* se specifico).
3.  Configura i Field wells:
    -   *X axis*: `date`.
    -   *Value* (Left Axis): `price`.
    -   *Value* (Right Axis): `trend_score` (se non c'è box dedicato, trascina nello stesso Value e poi imposta "Show on right axis" dalle proprietà della serie).
    -   *Nota*: I valori NULL nel `trend_score` appariranno come interruzioni nella linea, il che è corretto.

==== Visual 3: Correlazione (Scatter Plot)
1.  Aggiungi un nuovo visual -> *Scatter plot*.
2.  Configura:
    -   *X axis*: `trend_score`.
    -   *Y axis*: `price`.
    -   *Group/Color*: `coin`.
3.  Questo grafico mostrerà visivamente se prezzi alti corrispondono a trend alti (nuvole di punti).

=== 4. Pubblicazione e Validazione Rapida

1.  Clicca *Share* (in alto a destra) -> *Publish dashboard*.
2.  Assegna il nome: `CryptoData Dashboard` -> *Publish dashboard*.
3.  Esegui questi 3 controlli pratici sulla dashboard finale:
    -   [ ] *Interattività*: Cambia il filtro da `BTC` a `XMR`. Tutti i grafici devono aggiornarsi.
    -   [ ] *Coerenza Dati*: Nel primo grafico, la linea `moving_avg_10d` deve apparire più "liscia" e meno volatile della linea `price`.
    -   [ ] *Qualità Dati*: Nel secondo grafico, verifica che `trend_score` abbia dei buchi (NULL) dove non avevamo dati sufficienti, confermando che il sistema non ha "inventato" valori.

#screenshot-placeholder(
  "Dashboard finale pubblicata con i 3 grafici e menu di filtro a tendina.",
  "quicksight_dashboard_published.png"
)
