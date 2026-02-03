#import "../utils.typ": screenshot-placeholder

== Step 2: ETL Processing (AWS Glue)

Il cuore della pipeline è rappresentato dalla fase di ETL (Extract, Transform, Load), responsabile della trasformazione dei dati grezzi in informazioni analitiche. Per questo progetto, è stato scelto *AWS Glue* come ambiente di esecuzione serverless per processare i flussi dati di Bitcoin e Monero.

=== Analisi Preliminare dei File RAW (CSV)
Prima di sviluppare la logica di trasformazione, è stata eseguita un'analisi strutturale sui file CSV sorgente depositati nel *Bronze Bucket*.

*Raw Data: Storico Prezzi (BTC_EUR / XMR_EUR)*
- *Schema:* `"Date"`, `"Price"`, `"Open"`, `"High"`, `"Low"`, `"Vol."`, `"Change %"`.
- *Formato Dati:*
    - `Date`: Formato stringa "mm/dd/yyyy" (es. "03/12/2024").
    - `Price`: Numerico con separatore di migliaia (virgola), es. "65,619.5".
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
    - Worker Type: `G 1X` (sufficiente per la mole di dati).
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
from pyspark.sql.functions import col, to_date, regexp_replace, avg, when, last, weekofyear, year
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
    return spark.read.option("header", "true").option("inferSchema", "true").csv(path)

def clean_price_data(df):
    # Parsing Prezzo: Rimuovi virgola e converti in double
    df = df.withColumn("Price_Clean", 
                       regexp_replace(col("Price"), ",", "").cast("double"))
    
    # Parsing Data: Formato rilevato mm/dd/yyyy
    df = df.withColumn("Date_Clean", to_date(col("Date"), "MM/dd/yyyy"))
    
    # Gestione missing values (-1): Sostituisci con NULL e applica Forward Fill
    w_ffill = Window.orderBy("Date_Clean").rowsBetween(Window.unboundedPreceding, 0)
    
    df_cleaned = df.withColumn("Price_Null", 
                               when(col("Price_Clean") == -1, None)
                               .otherwise(col("Price_Clean"))) \
                   .withColumn("Price_Filled", 
                               last("Price_Null", ignorenulls=True).over(w_ffill))
    
    return df_cleaned.select(
        col("Date_Clean").alias("date"),
        col("Price_Filled").alias("price")
    ).filter(col("date").isNotNull())

def clean_trend_data(df):
    # Rename colonne dinamico (es "interesse bitcoin" -> "interest")
    trend_col = [c for c in df.columns if "interesse" in c.lower()][0]
    
    return df.select(
        to_date(col("Settimana"), "yyyy-MM-dd").alias("week_start"),
        col(trend_col).cast("integer").alias("trend_score")
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
w_avg = Window.orderBy("date").rowsBetween(-9, 0)
df_semigold = df_price_silver.withColumn("moving_avg_10d", avg("price").over(w_avg))

# Preparazione Join: Estrai anno e settimana per joinare
df_semigold = df_semigold.withColumn("year", year("date")) \
                         .withColumn("week", weekofyear("date"))

df_trend_silver = df_trend_silver.withColumn("year", year("week_start")) \
                                 .withColumn("week", weekofyear("week_start"))

# Join (Left Join per mantenere report giornaliero, arricchito col dato settimanale)
df_gold = df_semigold.join(df_trend_silver, on=["year", "week"], how="left") \
                     .drop("year", "week", "week_start") \
                     .orderBy("date")

# Fill trend settimanale (lo stesso valore per tutta la settimana)
w_trend_fill = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
df_gold = df_gold.withColumn("trend_score", last("trend_score", ignorenulls=True).over(w_trend_fill))

# Scrittura Gold
gold_path = f"{GOLD_URI}/{COIN}/final_dataset"
df_gold.write.mode("overwrite").parquet(gold_path)

job.commit()
```

=== Output Prodotti
L'esecuzione del job popola i bucket S3 con i seguenti artefatti:

1.  *Silver Layer:*
    - `s3://...-silver/BTC/price/`: Dataset prezzi pulito (Date, Price), formato Parquet.
    - `s3://...-silver/BTC/trend/`: Dataset trend normalizzato (Week, Score), formato Parquet.
2.  *Gold Layer:*
    - `s3://...-gold/BTC/final_dataset/`: Tabella master analitica.
    - Colonne finali: `date`, `price`, `moving_avg_10d`, `trend_score`.

// Screenshot 3
#screenshot-placeholder(
  "Script PySpark del Glue Job con parsing dei CSV reali e logica ETL.",
  "glue_job_logic.png"
)
