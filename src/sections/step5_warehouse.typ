== Step 5: Data Warehousing (Redshift)

In questa fase configuriamo *Amazon Redshift Serverless* per centralizzare i dati elaborati (Gold Layer) e renderli disponibili per query analitiche e dashboarding. Utilizzeremo il *Redshift Query Editor v2* per creare lo schema del database e caricare i file Parquet da S3.

=== Prerequisiti
Prima di iniziare, assicurarsi di avere:
- *Bucket S3 Gold*: `cryptodata-insights-gold` (o nome analogo usato nello Step 2).
- *Dataset Gold*: path simili a `s3://cryptodata-insights-gold/BTC/final_dataset/` e `.../XMR/final_dataset/`.
- *Schema dati atteso*: colonne `date`, `coin`, `price`, `moving_avg_10d`, `trend_score`.

=== Configurazione IAM per Redshift (Policy e Ruolo)
Redshift necessita di permessi per leggere i file dal bucket S3. Creiamo una Policy e un Ruolo dedicati.

1. Andare nella console *IAM* -> *Policies* -> *Create policy*.
2. Selezionare l'editor *JSON* e incollare la seguente policy (sostituire `<ACCOUNT_ID>` se necessario, o usare `*` con cautela):
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
3. Nominare la policy `CryptoData-Redshift-S3Access` e salvare.
4. Andare su *Roles* -> *Create role*.
5. Trusted entity type: *AWS Service*. Service or use case: *Redshift*. Selezionare *Redshift - Customizable* (o use-case standard per Serverless).
6. Nello step "Add permissions", cercare e selezionare la policy appena creata `CryptoData-Redshift-S3Access`.
7. Nominare il ruolo `CryptoData-Redshift-Role` e creare. Copiare l'*ARN* del ruolo (es. `arn:aws:iam::123456789012:role/CryptoData-Redshift-Role`) da usare successivamente.

//#figure(
//  image("../assets/redshift_iam_role_attached.png", width: 100%),
//  caption: "Creazione del ruolo IAM per Redshift con permessi di lettura su S3."
//)

=== Creazione Redshift Serverless (Namespace e Workgroup)
Utilizziamo la modalità Serverless per evitare la gestione di cluster fissi.

1. Dalla console AWS, cercare *Amazon Redshift* e selezionare nel menu a sinistra *Redshift Serverless*.
2. Cliccare su *Create workgroup* (che creerà anche un namespace se è il primo avvio).
3. *Workgroup configuration*:
   - Workgroup name: `cryptodata-workgroup`.
   - Capacity (RPUs): impostare il minimo (es. 8 RPU) per limitare i costi nell'ambiente di laboratorio.
   - Network: scegliere la *VPC di default* e le subnets predefinite. Assicurarsi che il Security Group permetta il traffico necessario (o usare accesso "Publicly accessible" solo se strettamente necessario per client esterni, per il Query Editor v2 basta l'accesso console).
4. *Namespace configuration*:
   - Namespace name: `cryptodata-namespace`.
   - Admin user credentials: Username `admin`, Password (generare e salvare in modo sicuro).
5. *Permissions*:
   - Cliccare su "Manage IAM roles" -> "Associate IAM roles".
   - Selezionare `CryptoData-Redshift-Role` creato in precedenza.
6. Cliccare *Create* e attendere che lo stato diventi _Available_.

//#figure(
//  image("../assets/redshift_serverless_namespace_workgroup.png", width: 100%),
//  caption: "Dashboard di Amazon Redshift Serverless con Workgroup e Namespace attivi."
//)

=== Creazione Schema e Tabelle (Query Editor v2)
Utilizziamo l'editor SQL integrato nel browser.

1. Cliccare su *Query Editor v2* dal menu Redshift.
2. Collegarsi al database (`dev`) usando le credenziali admin impostate nel Namespace.
3. Creare uno schema dedicato per il progetto:
```sql
CREATE SCHEMA cryptodata;
```
4. Creare la tabella unificata per i dati di mercato. Usiamo `SORTKEY` sulla data per ottimizzare le query temporali.

```sql
CREATE TABLE cryptodata.crypto_market (
    date DATE,
    coin VARCHAR(10),
    price DOUBLE PRECISION,
    moving_avg_10d DOUBLE PRECISION,
    trend_score INTEGER
)
SORTKEY(date);
```

=== Caricamento Dati (COPY da S3)
Per importare i file Parquet generati dal Glue Job ("Gold layer"), utilizziamo il comando `COPY`. Questo comando è efficiente e parallelo.

Eseguire il comando seguente (sostituendo l'ARN del ruolo IAM):

```sql
-- Caricamento dati Bitcoin
COPY cryptodata.crypto_market
FROM 's3://cryptodata-insights-gold/BTC/final_dataset/'
IAM_ROLE 'arn:aws:iam::<ACCOUNT_ID>:role/CryptoData-Redshift-Role'
FORMAT AS PARQUET;

-- Caricamento dati Monero
COPY cryptodata.crypto_market
FROM 's3://cryptodata-insights-gold/XMR/final_dataset/'
IAM_ROLE 'arn:aws:iam::<ACCOUNT_ID>:role/CryptoData-Redshift-Role'
FORMAT AS PARQUET;
```

_Nota_: Se i dataset sono partizionati correttamente, Redshift leggerà automaticamente tutti i file `.parquet` nella cartella specificata.

=== Validazione
Verifichiamo che i dati siano stati caricati correttamente con alcune query di controllo.

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

//#figure(
//  image("../assets/redshift_query_editor_copy_success.png", width: 100%),
//  caption: "Esecuzione avvenuta con successo del comando COPY e query di verifica nel Query Editor v2."
//)
