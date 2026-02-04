#import "../utils.typ": screenshot-placeholder

== Step 1: Configurazione Storage (Amazon S3)

Ho organizzato lo Storage Layer del progetto su Amazon S3 seguendo il paradigma *Medallion Architecture*, con l'obiettivo di garantire una chiara separazione tra i dati grezzi, quelli trasformati e quelli pronti per l'analisi.

=== Definizione dei Bucket e Naming Convention
Prima di procedere con la creazione delle risorse, ho definito una convenzione di naming chiara e coerente per i bucket S3, per assicurarne l'identificabilità univoca all'interno del mio account AWS.

Ho stabilito i seguenti nomi per i bucket:
- `cryptodata-insights-bronze`: Bucket per il Bronze Layer (Dati Grezzi).
- `cryptodata-insights-silver`: Bucket per il Silver Layer (Dati Puliti).
- `cryptodata-insights-gold`: Bucket per il Gold Layer (Dati Aggregati).
- `cryptodata-insights-scripts`: Bucket di supporto per gli script ETL.

=== Procedura Operativa
Ho eseguito la configurazione dell'infrastruttura di storage tramite la AWS Management Console seguendo questi passaggi:
1.  Ho effettuato l'accesso alla console AWS e ho navigato al servizio *Amazon S3*.
2.  Ho avviato la procedura di creazione tramite il pulsante "Create bucket".
3.  Ho configurato i singoli bucket utilizzando i nomi definiti in precedenza:
    - Ho inserito il nome del bucket (es. `cryptodata-insights-bronze`).
    - Ho mantenuto le impostazioni di default per il blocco dell'accesso pubblico e l'abilitazione della crittografia lato server (SSE-S3).
4.  Ho ripetuto l'operazione per tutti e quattro i bucket pianificati.
5.  Infine, ho verificato tramite la dashboard S3 la corretta creazione e la disponibilità delle risorse.

I bucket che ho così configurato costituiscono le fondamenta su cui poggeranno le fasi successive di ETL, orchestrazione e analisi dei dati, come illustrato nella figura seguente.

// Inserimento Screenshot
#figure(
  image("../assets/step1_s3_setup.png", width: 90%),
  caption: "Struttura dei bucket Amazon S3 configurata per la pipeline CryptoData Insights (Bronze, Silver, Gold e Scripts)."
)
