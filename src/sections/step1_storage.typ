#import "../utils.typ": screenshot-placeholder

== Step 1: Configurazione Storage (Amazon S3)

Lo Storage Layer del progetto è stato organizzato su Amazon S3 seguendo il paradigma *Medallion Architecture*, al fine di garantire una chiara separazione tra dati grezzi, trasformati e pronti per l'analisi.

=== Definizione dei Bucket e Naming Convention
Prima di procedere con la creazione delle risorse, è stata definita una convenzione di naming chiara e coerente per i bucket S3, al fine di garantirne l'identificabilità univoca all'interno dell'account AWS.

Sono stati stabiliti i seguenti nomi per i bucket:
- `cryptodata-insights-bronze`: Bucket per il Bronze Layer (Dati Grezzi).
- `cryptodata-insights-silver`: Bucket per il Silver Layer (Dati Puliti).
- `cryptodata-insights-gold`: Bucket per il Gold Layer (Dati Aggregati).
- `cryptodata-insights-scripts`: Bucket di supporto per gli script ETL.

=== Procedura Operativa
La configurazione dell'infrastruttura di storage è stata eseguita tramite la AWS Management Console seguendo questi passaggi:
1.  Accesso alla console AWS e navigazione al servizio *Amazon S3*.
2.  Avvio della procedura di creazione tramite il pulsante "Create bucket".
3.  Configurazione dei singoli bucket utilizzando i nomi definiti in precedenza:
    - Inserimento del nome del bucket (es. `cryptodata-insights-bronze`).
    - Mantenimento delle impostazioni di default per il blocco dell'accesso pubblico e l'abilitazione della crittografia lato server (SSE-S3).
4.  Ripetizione dell'operazione per tutti e quattro i bucket pianificati.
5.  Verifica finale nella dashboard S3 per confermare la corretta creazione e disponibilità delle risorse.

I bucket così configurati costituiscono le fondamenta su cui poggeranno le fasi successive di ETL, orchestrazione e analisi dei dati, come illustrato nella figura seguente.

// Inserimento Screenshot
#figure(
  image("../assets/step1_s3_setup.png", width: 90%),
  caption: "Struttura dei bucket Amazon S3 configurata per la pipeline CryptoData Insights (Bronze, Silver, Gold e Scripts)."
)
