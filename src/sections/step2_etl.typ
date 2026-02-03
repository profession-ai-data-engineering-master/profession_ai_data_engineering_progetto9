#import "../utils.typ": screenshot-placeholder

== Step 2: ETL Processing (AWS Glue)

Il cuore della pipeline è rappresentato dalla fase di ETL (Extract, Transform, Load), responsabile della trasformazione dei dati grezzi in informazioni analitiche. Per questo progetto, è stato scelto *AWS Glue* come ambiente di esecuzione serverless per processare i flussi dati di Bitcoin e Monero.

=== Analisi Preliminare dei File RAW (CSV)
Prima di sviluppare la logica di trasformazione, è stata eseguita un'analisi strutturale sui file CSV sorgente depositati nel *Bronze Bucket*.

*Raw Data: Storico Prezzi (BTC_EUR / XMR_EUR)*
- *Schema:* `"Date"`, `"Price"`, `"Open"`, `"High"`, `"Low"`, `"Vol."`, `"Change %"`.
- *Formato Dati e Parsing:*
    - `Date`: Formato stringa "mm/dd/yyyy", coerente con la logica di parsing implementata.
    - `Price`: Formato numerico locale EN-US (virgola come separatore delle migliaia), normalizzato nello script prima del casting.
- *Qualità del Dato:* Sono stati rilevati valori "sentinel" (`-1`) nella colonna `Price`, che indicano dati mancanti da gestire.

*Raw Data: Google Trends*
- *Schema:* `"Settimana"`, `"interesse [coin]"` (es. `"interesse bitcoin"`).
- *Formato Dati:*
    - `Settimana`: Formato standard ISO "yyyy-MM-dd" (es. "2019-03-17").
    - `Interesse`: Intero tra 0 e 100.
- *Granularità:* Settimanale (diverso dalla granularità giornaliera dei prezzi).

=== Scelta Implementativa: Job Parametrico
Per massimizzare la manutenibilità del codice e ridurre la duplicazione, si è optato per lo sviluppo di un *unico AWS Glue Job parametrico*, anziché creare script distinti per ogni criptovaluta.

Il job accetta in input un parametro `--coin` (es. `BTC` o `XMR`) e adatta dinamicamente i percorsi di lettura e scrittura.
- *Vantaggi:* Unica codebase da mantenere; facilità di onboarding per nuove valute.
- *Logica:* Lo script determina quali file leggere dal Bronze Bucket basandosi sul parametro fornito e indirizza l'output nelle cartelle corrispondenti del Silver e Gold Bucket.
- *Consistenza Temporale:* La chiave di join `week_start` viene calcolata normalizzando le date tramite `date_trunc("week", ...)` sia per i prezzi che per i trend. Questo approccio assicura un allineamento temporale robusto e privo di ambiguità legate al calendario ISO.
- *Integrità del Dato:* Il forward-fill sui prezzi viene applicato solo ai record con date valide, garantendo la correttezza della serie temporale. I valori di trend mancanti sono mantenuti `NULL` per preservare la distinzione tra assenza di dato e valore zero.

=== Procedura Operativa: Creazione Glue Job
Di seguito è riportata la procedura eseguita sulla AWS Console per configurare il job:

1.  *Configurazione IAM:*
    - Creazione di un ruolo IAM `GlueServiceRole-Crypto` con permessi di lettura/scrittura sui bucket S3 (`bronze`, `silver`, `gold`, `scripts`) e permessi di esecuzione per Glue.
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
4.  *Deployment Script:* Lo script Python è stato salvato nel bucket `scripts` e referenziato nella configurazione del job.

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

// Screenshot 3
#screenshot-placeholder(
  "Script PySpark del Glue Job con parsing dei CSV reali e logica ETL.",
  "glue_job_logic.png"
)
