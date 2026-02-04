#import "../utils.typ": screenshot-placeholder

== Step 6: Visualization (Amazon QuickSight)

In questo step finale ho collegato Amazon QuickSight ai dati preparati per visualizzare i risultati dell'analisi.

A causa delle limitazioni di connettività diretta tra QuickSight e Redshift Serverless (che non sempre espone un endpoint JDBC pubblico nativamente compatibile), ho adottato una soluzione architetturale robusta basata su *Amazon Athena*.
Questa scelta mi ha permesso di implementare il pattern: *QuickSight → Athena → S3 Gold (Parquet)*.

In questo scenario, ho configurato Athena per agire come layer di query serverless direttamente sui file Parquet prodotti nello Step 2, mentre ho mantenuto Redshift Serverless come motore dedicato alle query SQL complesse, alle trasformazioni avanzate e alla validazione dei dati. L'integrazione nativa di QuickSight con Athena mi ha garantito un accesso sicuro e performante al Data Lake.

=== 1. Preparazione Layer di Accesso con Amazon Athena

Prima di passare a QuickSight, ho definito lo schema dei dati in Athena per renderli interrogabili come tabelle standard.

In questo caso specifico, i dati nel bucket Gold erano stati salvati in due prefissi distinti (`BTC/final_dataset/` e `XMR/final_dataset/`). Poiché una singola `LOCATION` in una tabella esterna punta a una sola cartella radice, non potevo creare un'unica tabella che leggesse tutto automaticamente.

La strategia che ho adottato prevede:
1.  La creazione di due *External Tables* separate, una per BTC e una per XMR, puntando alle rispettive cartelle.
2.  La creazione di una *View* unificata (`crypto_market_gold`) che esegue una `UNION ALL` tra le due tabelle e aggiunge la colonna `coin` come costante.
3.  L'utilizzo di questa view `crypto_market_gold` in QuickSight come se fosse una singola tabella, permettendo di analizzare tutti i dati insieme.

Ho proceduto quindi con la configurazione:

1.  Dalla Console AWS, ho navigato su *Athena* -> *Query Editor*.
2.  Mi sono assicurato di aver configurato un bucket di output per le query di Athena (Settings -> Manage -> S3 location for query result).
3.  Ho eseguito la seguente query SQL nell'editor per creare il database logico:
    ```sql
    CREATE DATABASE IF NOT EXISTS cryptodata_athena;
    ```
4.  Ho eseguito le query seguenti una alla volta per configurare tabelle e viste, assicurandomi di usare i path `LOCATION` corretti del mio bucket (`cryptodata-insights-gold`).

    *Passo 1: Creazione Tabelle Esterne*

    ```sql

    -- 1) External table BTC
    CREATE EXTERNAL TABLE IF NOT EXISTS cryptodata_athena.crypto_market_gold_btc (
      date date,
      price double,
      moving_avg_10d double,
      trend_score int
    )
    STORED AS PARQUET
    LOCATION 's3://cryptodata-insights-gold/BTC/final_dataset/';

    -- 2) External table XMR
    CREATE EXTERNAL TABLE IF NOT EXISTS cryptodata_athena.crypto_market_gold_xmr (
      date date,
      price double,
      moving_avg_10d double,
      trend_score int
    )
    STORED AS PARQUET
    LOCATION 's3://cryptodata-insights-gold/XMR/final_dataset/';
    ```
    *Nota*: Ho considerato la proprietà `TBLPROPERTIES ("parquet.compression"="SNAPPY")` come opzionale in lettura su Athena.

    *Passo 2: Creazione View Unificata*

    Ho creato successivamente la vista che unisce i due flussi di dati e reintroduce l'identificativo della moneta:

    ```sql
    -- 4) View unificata (aggiunge coin e fa UNION ALL)
    CREATE OR REPLACE VIEW cryptodata_athena.crypto_market_gold AS
    SELECT
      'BTC' AS coin,
      date,
      price,
      moving_avg_10d,
      trend_score
    FROM cryptodata_athena.crypto_market_gold_btc
    UNION ALL
    SELECT
      'XMR' AS coin,
      date,
      price,
      moving_avg_10d,
      trend_score
    FROM cryptodata_athena.crypto_market_gold_xmr;
    ```

    *Passo 3: Validazione*

    Ho eseguito queste query per confermare che Athena leggesse correttamente i dati da entrambe le cartelle S3 attraverso la vista:

    ```sql
    -- preview righe
    SELECT * FROM cryptodata_athena.crypto_market_gold
    ORDER BY date DESC
    LIMIT 20;

    -- conteggio per coin
    SELECT coin, count(*) AS n
    FROM cryptodata_athena.crypto_market_gold
    GROUP BY coin;

    -- range date per coin
    SELECT coin, min(date) AS min_date, max(date) AS max_date
    FROM cryptodata_athena.crypto_market_gold
    GROUP BY coin;
    ```

=== 2. Configurazione Iniziale e Data Source

1.  Dalla Console AWS, ho navigato su *QuickSight*.
2.  Ho verificato che QuickSight avesse i permessi corretti:
    -   Ho cliccato sull'icona utente -> *Manage QuickSight* -> *Security & permissions*.
    -   Sono andato su *manage* in *QuickSight access to AWS services*.
    -   Ho abilitato *Amazon Athena*.
    -   Ho abilitato *Amazon S3* e ho selezionato il bucket `cryptodata-insights-gold` (con permessi di lettura/scrittura `Show buckets` -> Select).
    -   Ho salvato le modifiche.
3.  Tornato alla home di QuickSight, sono andato su *Datasets* -> *New dataset*.
4.  Ho selezionato la scheda *Athena* e inserito i parametri:
    -   *Data source name*: `CryptoAthenaSource`.
    -   *Athena workgroup*: `primary` (default).
    -   Ho cliccato su *Create data source*.
    
    In questa configurazione, non ho stabilito alcuna connessione verso Redshift; l'accesso ai dati avviene direttamente sullo storage S3 attraverso il catalogo di Athena.

=== 3. Creazione Dataset e SPICE vs Direct Query

1.  Nella schermata di selezione tabelle ho impostato:
    -   *Catalog*: `AwsDataCatalog`.
    -   *Database*: `cryptodata_athena`.
    -   *Table*: `crypto_market_gold`.
    -   Ho cliccato su *Select*.
2.  Ho scelto la modalità di query:
    -   Ho selezionato *Import to SPICE*.
    -   *Motivazione*: Ho preferito SPICE (Super-fast, Parallel, In-memory Calculation Engine) perché è ideale per dataset analitici di dimensioni contenute come questo. Mi garantisce performance di rendering elevate e, aspetto cruciale con Athena, *evita i costi per query* ad ogni interazione/filtro sulla dashboard, dato che i dati vengono caricati in memoria una tantum (o via refresh schedulato).
    -   Ho evitato *Direct Query* poiché interrogherebbe Athena in tempo reale ad ogni click, aumentando latenza e costi (5\$ per TB scansionato).
3.  Ho cliccato su *Visualize*.

=== 4. Costruzione della Dashboard (Analysis)

Una volta aperta l'interfaccia di analisi, ho replicato le visualizzazioni previste seguendo questo schema:

==== Visual 1: Prezzo vs Media Mobile (Time Series)
1.  Ho selezionato il tipo *Line chart*.
2.  Ho configurato i *Field wells*:
    -   *X axis*: `date`.
    -   *Value*: Ho trascinato `price` e `moving_avg_10d`.
3.  *Aggiunta Filtro per Coin*:
    -   Dal pannello *Filter* ho aggiunto un filtro su `coin`.
    -   L'ho impostato come controllo a tendina (*Add to sheet*) per permettere all'utente di selezionare "BTC" o "XMR".

==== Visual 2: Prezzo vs Trend Google (Dual Axis)
1.  Ho aggiunto un *Line chart* (o Dual Axis).
2.  Ho configurato:
    -   *X axis*: `date`.
    -   *Value (Left)*: `price`.
    -   *Value (Right)*: `trend_score`.
3.  La visualizzazione risultante mostra il punteggio del trend (0-100) sull'asse destro. I valori `NULL` nel `trend_score` appaiono come interruzioni grafico, indicando assenza di dati sufficienti per quel giorno.

==== Visual 3: Correlazione (Scatter Plot)
1.  Ho aggiunto uno *Scatter plot*.
2.  Ho configurato:
    -   *X axis*: `trend_score`.
    -   *Y axis*: `price`.
    -   *Group/Color*: `coin`.

=== Pubblicazione e Validazione

1.  Ho cliccato su *Share* -> *Publish dashboard*.
2.  Ho assegnato il nome: `CryptoData Dashboard`.

=== Conclusioni Architetturali

L'adozione di Athena come layer di accesso mi ha consentito di mantenere un'architettura completamente serverless, separando il layer di calcolo analitico (Redshift Serverless) dal layer di visualizzazione (QuickSight).
Questa configurazione è pienamente conforme ai principi moderni di *Lakehouse architecture*, dove lo storage S3 agisce come "source of truth" centrale accessibile da molteplici motori specializzati (ETL, Warehousing, BI) senza duplicazione fisica del dato.

=== Galleria Visualizzazioni Finali

Di seguito riporto gli screenshot delle dashboard realizzate in Amazon QuickSight, che confermano il successo dell'integrazione end-to-end e permettono di analizzare i trend di mercato.

#figure(
  image("../assets/sum_of_price_and_sum_of_moving_avg_10d_by_date_btc.png"),
  caption: [
    Analisi temporale di Bitcoin (BTC): confronto tra prezzo giornaliero e media mobile a 10 giorni.
    Il grafico mostra come la media mobile smussi le oscillazioni di breve periodo, evidenziando il trend di fondo del prezzo.
    Le divergenze tra le due curve indicano fasi di elevata volatilità, mentre la convergenza segnala periodi di maggiore stabilità.
  ],
)

#figure(
  image("../assets/sum_of_price_and_sum_of_moving_avg_10d_by_date_xmr.png"),
  caption: [
    Analisi temporale di Monero (XMR): andamento del prezzo confrontato con la media mobile a 10 giorni.
    La visualizzazione evidenzia una dinamica più irregolare rispetto a Bitcoin, con variazioni di prezzo più brusche
    e una media mobile che segue il trend generale attenuando la rumorosità giornaliera.
  ],
)

#figure(
  image("../assets/sum_of_price_and_sum_of_moving_avg_10d_by_date_xmr.png"),
  caption: [
    Analisi temporale di Monero (XMR): andamento del prezzo confrontato con la media mobile a 10 giorni.
    La visualizzazione evidenzia una dinamica più irregolare rispetto a Bitcoin, con variazioni di prezzo più brusche
    e una media mobile che segue il trend generale attenuando la rumorosità giornaliera.
  ],
)

#figure(
  image("../assets/sum_of_price_and_sum_of_trend_score_by_date_xmr.png"),
  caption: [
    Correlazione temporale Monero: confronto tra prezzo e interesse di ricerca su Google.
    A differenza di Bitcoin, il trend score mostra valori mediamente più bassi e meno frequenti,
    riflettendo una minore esposizione mediatica e una correlazione meno marcata con le variazioni di prezzo.
  ],
)

#figure(
  image("../assets/sum_of_trend_score_and_sum_of_price_by_coin.png"),
  caption: [
    Analisi aggregata BTC vs XMR: scatter plot dei valori cumulativi di prezzo e trend score.
    Il grafico evidenzia la forte differenza di scala tra Bitcoin e Monero, con Bitcoin caratterizzato
    da valori di prezzo e interesse di ricerca significativamente superiori.
    La separazione netta dei punti conferma la diversa rilevanza di mercato e mediatica delle due criptovalute.
  ],
)
