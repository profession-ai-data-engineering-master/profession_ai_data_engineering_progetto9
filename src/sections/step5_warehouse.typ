== Step 5: Data Warehousing (Redshift)

In questa fase ho configurato *Amazon Redshift Serverless* con l'obiettivo di centralizzare i dati elaborati (Gold Layer) e renderli disponibili per query analitiche e dashboarding. Ho utilizzato il *Redshift Query Editor v2* per definire lo schema del database e per importare i file Parquet residenti su S3.

=== Configurazione IAM per Redshift (Policy e Ruolo)
Poiché Redshift necessita di permessi espliciti per leggere i file dal bucket S3, ho provveduto a creare una Policy e un Ruolo dedicati.

1.  Dalla console *IAM* -> *Policies* -> *Create policy*, ho aperto l'editor *JSON* e ho incollato la seguente policy (avendo cura di gestire correttamente l'account ID):
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::cryptodata-insights-gold",
                "arn:aws:s3:::cryptodata-insights-gold/*"
            ]
        }
    ]
}
```
2.  Ho salvato la policy con il nome `CryptoData-Redshift-S3Access`.
3.  Successivamente, in *Roles* -> *Create role*, ho selezionato *AWS Service* con use case *Redshift - Customizable* (adatto all'ambiente Serverless).
4.  Nello step "Add permissions", ho cercato e selezionato la policy `CryptoData-Redshift-S3Access` appena creata.
5.  Ho finalizzato la creazione assegnando il nome `CryptoData-Redshift-Role`. Ho quindi preso nota dell'*ARN* del ruolo (es. `arn:aws:iam::123456789012:role/CryptoData-Redshift-Role`) per utilizzarlo nelle fasi successive.

=== Creazione Redshift Serverless (Namespace e Workgroup)
Ho optato per la modalità Serverless per evitare la gestione e i costi fissi di un cluster provisioned.

1.  Dalla console AWS ho selezionato *Redshift Serverless*.
2.  Ho avviato la creazione cliccando su *Create workgroup*, che ha generato contestualmente anche il namespace necessario.
3.  Ho configurato il Workgroup come segue:
    - Workgroup name: `cryptodata-workgroup`.
    - Capacity (RPUs): ho impostato il minimo (es. 8 RPU) per contenere i costi.
    - Network: ho mantenuto la *VPC di default* e le relative subnet, assicurandomi che il Security Group permettesse il traffico necessario per l'accesso tramite console.
4.  Ho configurato il Namespace:
    - Namespace name: `cryptodata-namespace`.
    - Admin user credentials: Username `admin`.
5.  Nella sezione *Permissions*, tramite "Manage IAM roles" -> "Associate IAM roles", ho associato il ruolo `CryptoData-Redshift-Role` creato in precedenza.
6.  Ho cliccato su *Create* e atteso che lo stato diventasse _Available_.

#figure(
  image("../assets/redshift_serverless_namespace_workgroup.png", width: 100%),
  caption: "Dashboard di Amazon Redshift Serverless con Workgroup e Namespace attivi."
)

=== Creazione Schema e Tabelle (Query Editor v2)
Per interagire con il database, ho utilizzato l'editor SQL integrato nel browser.

1.  Ho aperto il *Query Editor v2* dal menu Redshift.
2.  Mi sono connesso al database `dev` utilizzando le credenziali admin impostate nel Namespace.
3.  Ho creato uno schema dedicato per il progetto:
```sql
CREATE SCHEMA cryptodata;
```
4.  Ho definito la tabella unificata per i dati di mercato, impostando una `SORTKEY` sulla colonna `date` per ottimizzare le performance delle query temporali:

```sql
CREATE TABLE cryptodata.crypto_market (
    coin VARCHAR(10),
    date DATE,
    price DOUBLE PRECISION,
    moving_avg_10d DOUBLE PRECISION,
    trend_score INTEGER
)
SORTKEY(date);
```

=== Caricamento Dati (COPY da S3)
Per importare i file Parquet generati dal Glue Job ("Gold layer"), ho utilizzato il comando `COPY`, sfruttandone l'efficienza e la capacità di parallelismo.

Ho eseguito i seguenti comandi SQL (inserendo l'ARN del ruolo IAM precedentemente creato):

```sql
-- Caricamento dati Bitcoin
COPY cryptodata.crypto_market (coin, date, price, moving_avg_10d, trend_score)
FROM 's3://cryptodata-insights-gold/BTC/final_dataset/'
IAM_ROLE 'arn:aws:iam::123456789012:role/CryptoData-Redshift-Role'
FORMAT AS PARQUET;

-- Caricamento dati Monero
COPY cryptodata.crypto_market(coin, date, price, moving_avg_10d, trend_score)
FROM 's3://cryptodata-insights-gold/XMR/final_dataset/'
IAM_ROLE 'arn:aws:iam::123456789012:role/CryptoData-Redshift-Role'
FORMAT AS PARQUET;
```

_Nota_: Grazie alla corretta partizione dei dataset, Redshift ha letto automaticamente tutti i file `.parquet` presenti nelle cartelle specificate.

=== Validazione
Infine, ho verificato che i dati fossero stati caricati correttamente eseguendo alcune query di controllo per confermare la completezza e la coerenza del dataset.

```sql
-- Conteggio totale righe
SELECT count(*) FROM cryptodata.crypto_market;

-- Anteprima ultimi dati
SELECT * FROM cryptodata.crypto_market ORDER BY date DESC LIMIT 10;

-- Statistiche per criptovaluta
SELECT coin, 
       min(date) as start_date, 
       max(date) as end_date, 
       count(*) as daily_records 
FROM cryptodata.crypto_market 
GROUP BY coin;
```

#figure(
  image("../assets/redshift_query_editor_copy_success_1.png", width: 100%),
  caption: "Esecuzione avvenuta con successo del comando COPY e query di verifica nel Query Editor v2."
)

#figure(
  image("../assets/redshift_query_editor_copy_success_2.png", width: 100%),
  caption: "Esecuzione avvenuta con successo del comando COPY e query di verifica nel Query Editor v2."
)

#figure(
  image("../assets/redshift_query_editor_copy_success_3.png", width: 100%),
  caption: "Esecuzione avvenuta con successo del comando COPY e query di verifica nel Query Editor v2."
)