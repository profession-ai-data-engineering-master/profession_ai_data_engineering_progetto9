#import "../utils.typ": screenshot-placeholder

== Step 2: ETL Processing (AWS Glue)

Il cuore della pipeline è rappresentato dalla fase di ETL (Extract, Transform, Load), responsabile della trasformazione dei dati grezzi in informazioni analitiche. Per questo progetto, è stato scelto *AWS Glue* come ambiente di esecuzione serverless per processare i flussi dati di Bitcoin e Monero.

=== Analisi Preliminare dei File RAW (CSV)
Prima di sviluppare la logica di trasformazione, è stata condotta un'analisi puntuale su ciascuno dei quattro file CSV sorgente depositati nel *Bronze Bucket*, per identificarne schemi, formati e anomalie.

==== File: `BTC_EUR_Historical_Data.csv`
- *Origine:* Dati storici di mercato per Bitcoin (BTC/EUR).
- *Granularità:* Giornaliera.
- *Analisi delle Colonne:*
  - `Date`: Stringa in formato "mm/dd/yyyy" (es. "03/12/2024"). *Utilizzo:* Chiave primaria temporale. Richiede parsing esplicito.
  - `Price`: Stringa numerica con separatore di migliaia (es. "65,619.5" o "1,234.56"). *Utilizzo:* Metrica principale. Richiede rimozione virgole e cast a `double`.
  - `Open`, `High`, `Low`: Stringhe numeriche. *Stato:* Ignorate (focus dell'analisi sul prezzo medio/chiusura).
  - `Vol.`: Stringa con suffissi "K"/"M" (es. "0.19K"). *Stato:* Ignorata.
  - `Change %`: Stringa percentuale (es. "-0.37%"). *Stato:* Ignorata.
- *Qualità del Dato:* Rilevata presenza di valori sentinel `-1` nella colonna `Price`, indicativi di dati mancanti.

==== File: `XMR_EUR Kraken Historical Data.csv`
- *Origine:* Dati storici di mercato per Monero (XMR/EUR).
- *Granularità:* Giornaliera.
- *Analisi delle Colonne:*
  - Schema identico al file BTC: `Date`, `Price`, `Open`, `High`, `Low`, `Vol.`, `Change %`.
  - Formato `Price`: Numerico EN-US ("133.290"). Richiede la stessa logica di pulizia.
  - Colonne accessorie (`Open`...`Change %`): Presenti ma escluse dall'ETL.

==== File: `google_trend_bitcoin.csv`
- *Origine:* Google Trends (Keyword: "Bitcoin").
- *Granularità:* Settimanale (Indice lunedì-domenica).
- *Analisi delle Colonne:*
  - `Settimana`: Stringa ISO-8601 "yyyy-MM-dd" (es. "2019-03-17"). *Utilizzo:* Chiave temporale. Viene normalizzata a `week_start` per il join.
  - `interesse bitcoin`: Intero (0-100). *Utilizzo:* Metrica di popolarità.
- *Implicazioni ETL:* Il nome della colonna metrica contiene la keyword "bitcoin", richiedendo una logica di selezione dinamica della colonna target.

==== File: `google_trend_monero.csv`
- *Origine:* Google Trends (Keyword: "Monero").
- *Granularità:* Settimanale.
- *Analisi delle Colonne:*
  - `Settimana`: Stringa ISO-8601. *Analisi:* Formato coerente con il dataset Bitcoin.
  - `Monero_interesse`: Intero (0-100). *Analisi:* Si nota una nomenclatura diversa rispetto al file Bitcoin (`Monero_interesse` vs `interesse bitcoin`).
- *Implicazioni ETL:* La diversità nei nomi delle colonne (`interesse bitcoin` vs `Monero_interesse`) ha guidato l'implementazione di una funzione di lettura flessibile basata sulla sottostringa `"interesse"`.

=== Scelta Implementativa: Job Parametrico
Per massimizzare la manutenibilità del codice e ridurre la duplicazione, si è optato per lo sviluppo di un *unico AWS Glue Job parametrico*, anziché creare script distinti per ogni criptovaluta.

Il job accetta in input un parametro `--coin` (es. `BTC` o `XMR`) e adatta dinamicamente i percorsi di lettura e scrittura.
- *Vantaggi:* Unica codebase da mantenere; facilità di onboarding per nuove valute.
- *Logica:* Lo script determina quali file leggere dal Bronze Bucket basandosi sul parametro fornito e indirizza l'output nelle cartelle corrispondenti del Silver e Gold Bucket.
- *Consistenza Temporale:* La chiave di join `week_start` viene calcolata normalizzando le date tramite `date_trunc("week", ...)` sia per i prezzi che per i trend. Questo approccio assicura un allineamento temporale robusto e privo di ambiguità legate al calendario ISO.
- *Integrità del Dato:* Il forward-fill sui prezzi viene applicato solo ai record con date valide, garantendo la correttezza della serie temporale. I valori di trend mancanti sono mantenuti `NULL` per preservare la distinzione tra assenza di dato e valore zero.

=== Procedura Operativa: Creazione Glue Job
Di seguito è riportata la procedura eseguita sulla AWS Console per configurare il job:

1.  *Configurazione IAM (Ruolo `GlueServiceRole-Crypto`):*
    Per rispettare il principio del privilegio minimo, la configurazione è stata divisa in due fasi: creazione di una policy dedicata e successiva associazione al ruolo.

    - *Fase 1: Creazione Policy S3 (Customer Managed)*
      1.  Dalla Console AWS, navigare in *IAM* > *Policies* > *Create policy*.
      2.  Nel tab *JSON*, inserire la seguente policy che limita l'accesso in lettura/scrittura esclusivamente ai bucket del progetto:
      ```json
      {
          "Version": "2012-10-17",
          "Statement": [
              {
                  "Effect": "Allow",
                  "Action": [
                      "s3:ListBucket",
                      "s3:GetObject",
                      "s3:PutObject",
                      "s3:DeleteObject"
                  ],
                  "Resource": [
                      "arn:aws:s3:::cryptodata-insights-bronze",
                      "arn:aws:s3:::cryptodata-insights-bronze/*",
                      "arn:aws:s3:::cryptodata-insights-silver",
                      "arn:aws:s3:::cryptodata-insights-silver/*",
                      "arn:aws:s3:::cryptodata-insights-gold",
                      "arn:aws:s3:::cryptodata-insights-gold/*",
                      "arn:aws:s3:::cryptodata-insights-scripts",
                      "arn:aws:s3:::cryptodata-insights-scripts/*"
                  ]
              }
          ]
      }
      ```
      3.  Salvare la policy con il nome `CryptoData-S3-GlueAccess`.

    - *Fase 2: Creazione Ruolo e Associazione Permessi*
      1.  Navigare in *IAM* > *Roles* > *Create role* (Trusted entity: *AWS Service*, Use case: *Glue*).
      2.  Nella sezione *Add permissions*, allegare le seguenti policy:
          - *Policy Managed (AWS):* `AWSGlueServiceRole` (accesso base Glue e CloudWatch).
          - *Policy Custom:* `CryptoData-S3-GlueAccess` (creata al punto precedente).
      3.  Finalizzare la creazione con il nome `GlueServiceRole-Crypto`.

    - *Trust Relationship:*
      Verificare che il ruolo possieda la relazione di fiducia necessaria per essere assunto da Glue:
      ```json
      {
          "Version": "2012-10-17",
          "Statement": [
              {
                  "Effect": "Allow",
                  "Principal": { "Service": "glue.amazonaws.com" },
                  "Action": "sts:AssumeRole"
              }
          ]
      }
      ```
2.  *Creazione Job:*
    - Servizio: *AWS Glue* > *ETL jobs*.
    - Opzione: *Script editor* (Spark).
    - Engine: Spark (Python/PySpark).
3.  *Job Details:*
    - Name: `CryptoData-ETL-Generic`.
    - IAM Role: `GlueServiceRole-Crypto`.
    - Worker Type: `G.1X` (sufficiente per la mole di dati).
    - Job parameters (valori di default per test):
        - `--coin`: `BTC`
        - `--bronze_bucket`: `s3://cryptodata-insights-bronze`
        - `--silver_bucket`: `s3://cryptodata-insights-silver`
        - `--gold_bucket`: `s3://cryptodata-insights-gold`
4.  *Gestione Script e Deployment:*
    Per garantire il controllo delle versioni sul codice ETL, il bucket `s3://cryptodata-insights-scripts` è stato organizzato secondo una struttura gerarchica rigorosa.
    - *Struttura S3:*
      ```text
      s3://cryptodata-insights-scripts/
      └── glue/
          └── etl/
              ├── v1/
              │   └── crypto_etl_job.py
              ├── v2/
              │   └── crypto_etl_job.py
              └── latest/
                  └── crypto_etl_job.py
      ```
      L'utilizzo di cartelle versionate (`v1`, `v2`...) permette di conservare lo storico delle modifiche, mentre la cartella `latest` funge da puntatore stabile per l'esecuzione produttiva.

    - *Procedura di Aggiornamento (Checklist):*
      1.  Modificare e testare localmente lo script Python.
      2.  Caricare il nuovo file in una nuova cartella incrementale (es. `s3://.../glue/etl/v3/crypto_etl_job.py`).
      3.  Copiare la nuova versione nella cartella `latest`, sovrascrivendo il file esistente.
      4.  Se il path nel Glue Job punta a `latest`, l'aggiornamento è automatico alla successiva esecuzione.

    - *Configurazione Job:*
      Nel campo *Script path* del Glue Job, è stato impostato il percorso assoluto alla versione `latest`:
      `s3://cryptodata-insights-scripts/glue/etl/latest/crypto_etl_job.py`
      Questa configurazione disaccoppia il ciclo di vita del codice dalla definizione del job, facilitando eventuali rollback (basta ripristinare il file in `latest` da una versione precedente).

    - *Best Practices Adottate:*
      - Non sovrascrivere mai le cartelle numerate (`v1`, `v2`, etc.).
      - Ogni versione corrisponde a una modifica logica significativa.
      - Testare sempre una nuova versione copiandola in `latest` ed eseguendo il job con un parametro limitato (es. `--coin=BTC`) prima dell'esecuzione completa.

=== Gestione Concorrenza del Job (Maximum concurrent runs)
Essendo il job parametrico, la pipeline di orchestrazione avvia due esecuzioni parallele per processare simultaneamente i flussi BTC e XMR. È quindi fondamentale configurare Glue per accettare 2 run contemporanee dello stesso job.

- AWS Console -> *AWS Glue* -> *ETL jobs*
- Aprire il job *`CryptoData-ETL-Generic`*
- Click *Edit*
- Nella sezione *Job details / Advanced properties / Concurrency* (usare la voce presente in console) trovare il campo:
  *Maximum concurrent runs*
- Impostare valore *2*
- Click *Save*

Questa impostazione è necessaria per l'orchestrazione in parallelo in Step Functions e previene l'errore `ConcurrentRunsExceededException`.

=== Script ETL PySpark
Di seguito viene riportato il codice completo sviluppato per il job. Lo script gestisce l'intero ciclo di vita del dato: pulizia (Silver) e aggregazione (Gold).

```python
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date, regexp_replace, avg, when, last, lit, date_trunc
from pyspark.sql.window import Window

# 1. Recupero Parametri
args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 'coin', 'bronze_bucket', 'silver_bucket', 'gold_bucket'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

COIN = args['coin']  # Es: 'BTC' o 'XMR'
BRONZE_URI = args['bronze_bucket']
SILVER_URI = args['silver_bucket']
GOLD_URI = args['gold_bucket']

# Mapping nomi file in base alla coin
files_map = {
    'BTC': {'price': 'BTC_EUR_Historical_Data.csv', 'trend': 'google_trend_bitcoin.csv'},
    'XMR': {'price': 'XMR_EUR Kraken Historical Data.csv', 'trend': 'google_trend_monero.csv'}
}
current_files = files_map[COIN]

# --- FUNZIONI DI SUPPORTO ---

def read_csv(path):
    # Riduzione dipendenza da inferSchema per maggiore stabilità
    return spark.read.option("header", "true").option("inferSchema", "false").csv(path)

def clean_price_data(df):
    # Parsing Prezzo: Cast a string -> replace -> double
    df = df.withColumn("Price_Clean", 
                       regexp_replace(col("Price").cast("string"), ",", "").cast("double"))
    
    # Parsing Data: Formato rilevato mm/dd/yyyy
    df = df.withColumn("Date_Clean", to_date(col("Date"), "MM/dd/yyyy"))

    # Filtro data valida PRIMA del forward fill per stabilità
    df = df.filter(col("Date_Clean").isNotNull())
    
    # Gestione missing values (-1): Sostituisci con NULL e applica Forward Fill
    w_ffill = Window.orderBy("Date_Clean").rowsBetween(Window.unboundedPreceding, 0)
    
    df_cleaned = df.withColumn("Price_Null", 
                               when(col("Price_Clean") == -1, None)
                               .otherwise(col("Price_Clean"))) \
                   .withColumn("Price_Filled", 
                               last("Price_Null", ignorenulls=True).over(w_ffill))
    
    # Aggiunta colonna COIN e calcolo chiave temporale settimanale per Join
    return df_cleaned.select(
        col("Date_Clean").alias("date"),
        col("Price_Filled").alias("price"),
        lit(COIN).alias("coin"),
        to_date(date_trunc("week", col("Date_Clean"))).alias("week_start") # Chiave di Join robusta
    )

def clean_trend_data(df):
    # Rename colonne dinamico (es "interesse bitcoin" -> "interest")
    trend_col = [c for c in df.columns if "interesse" in c.lower()][0]
    
    # Normalizzazione week_start coerente con i prezzi
    return df.select(
        to_date(date_trunc("week", to_date(col("Settimana"), "yyyy-MM-dd"))).alias("week_start"),
        col(trend_col).cast("integer").alias("trend_score"),
        lit(COIN).alias("coin")
    )

# --- PIPELINE ESECUZIONE ---

# 1. Ingestion & Silver Layer (Price)
path_price = f"{BRONZE_URI}/{current_files['price']}"
df_price_raw = read_csv(path_price)
df_price_silver = clean_price_data(df_price_raw)

# Scrittura Silver Price
silver_path_price = f"{SILVER_URI}/{COIN}/price"
df_price_silver.write.mode("overwrite").parquet(silver_path_price)

# 2. Ingestion & Silver Layer (Trend)
path_trend = f"{BRONZE_URI}/{current_files['trend']}"
df_trend_raw = read_csv(path_trend)
df_trend_silver = clean_trend_data(df_trend_raw)

# Scrittura Silver Trend
silver_path_trend = f"{SILVER_URI}/{COIN}/trend"
df_trend_silver.write.mode("overwrite").parquet(silver_path_trend)

# 3. Gold Layer: Arricchimento
# Calcolo Media Mobile 10 giorni
w_avg = Window.partitionBy("coin").orderBy("date").rowsBetween(-9, 0)
df_semigold = df_price_silver.withColumn("moving_avg_10d", avg("price").over(w_avg))

# Join Temporale Robusto su week_start
# Ogni giorno eredita il trend della settimana di appartenenza
df_gold = df_semigold.join(df_trend_silver, on=["week_start", "coin"], how="left") \
                     .drop("week_start") \
                     .orderBy("date")

# Gestione NULL nel trend
# Manteniamo NULL se il dato di trend manca per quella settimana specifica (nessun fill con 0)
# df_gold = df_gold.fillna(0, subset=["trend_score"])

# Scrittura Gold
gold_path = f"{GOLD_URI}/{COIN}/final_dataset"
df_gold.write.mode("overwrite").parquet(gold_path)

job.commit()
```

=== Output Prodotti
L'esecuzione del job popola i bucket S3 con i seguenti artefatti, parametrizzati per valuta (`{coin}`, es. `BTC`, `XMR`):

1.  *Silver Layer:*
    - `s3://...-silver/{coin}/price/`: Dataset prezzi pulito (colonne: `date`, `price`, `coin`, `week_start`).
    - `s3://...-silver/{coin}/trend/`: Dataset trend normalizzato (colonne: `week_start`, `trend_score`, `coin`).
2.  *Gold Layer:*
    - `s3://...-gold/{coin}/final_dataset/`: Tabella master analitica pronta per Redshift.
    - Colonne finali: `date`, `price`, `coin`, `moving_avg_10d`, `trend_score`.

#figure(
  image("../assets/glue_job_run_success.png", width: 100%),
  caption: "Esecuzione del Glue Job `CryptoData-ETL-Generic` con stato `Succeeded`, a conferma della corretta configurazione del ruolo IAM, dei parametri di input e della logica ETL implementata."
)