# CryptoData Insights: Pipeline E2E AWS per l'Analisi di BTC e XMR

## 📋 Descrizione del Progetto

CryptoData Insights è una soluzione di data analytics progettata per raccogliere, pulire e trasformare dati di mercato sulle principali criptovalute, specificamente Bitcoin (BTC) e Monero (XMR). 

In un mercato volatile come quello crypto, disporre di dati accurati e tempestivi è fondamentale. Questo progetto implementa una pipeline End-to-End (E2E) completamente automatizzata su **AWS**, che correla i dati di prezzo giornalieri con i trend di ricerca settimanali di Google Trends. L'obiettivo è trasformare dati grezzi in insight azionabili su **Amazon Redshift**, supportando decisioni strategiche basate sui dati.

## 🏗️ Architettura della Soluzione

La soluzione è composta da due pipeline parallele (una per BTC e una per XMR) orchestrate per massimizzare l'efficienza.

1.  **Ingestion:** Caricamento dati grezzi su Amazon S3 (Bucket "Raw").
2.  **Processing (ETL):** Pulizia, trasformazione e arricchimento dei dati utilizzando **AWS Glue** o **Amazon EMR**.
3.  **Storage Intermedio:** Salvataggio degli artefatti puliti in formato Parquet su Amazon S3 (Bucket "Argento").
4.  **Loading:** Caricamento dei dati finali su **Amazon Redshift** per l'analisi SQL.
5.  **Orchestration:** Coordinamento dei flussi di lavoro tramite **AWS Step Functions**.
6.  **Visualization (Opzionale):** Dashboard interattive su **Amazon QuickSight**.

## 💾 Dati e Dataset

Il dataset include una coppia di file CSV per ogni criptovaluta (BTC/EUR e XMR/EUR) e file relativi ai Google Trends.
**Link al Dataset:** [Google Drive Folder](https://drive.google.com/drive/folders/1q-6IWRebAe51xhhY_dWI5QWfiBWEH8E2)

### 1. File di Prezzo (Es. BTC/EUR)
File CSV giornaliero contenente lo storico dei prezzi.
*   **Date:** Stringa (formato esteso, es. "03/12/2024").
*   **Price:** Numerico (es. 145.4).
    *   *Nota:* I valori mancanti sono indicati con `-1`.

### 2. File Google Trends
File CSV settimanale che misura l'interesse di ricerca globale.
*   **Settimana:** Indice temporale settimanale.
*   **Interesse:** Valore intero (0-100), indice di popolarità delle ricerche.

## ⚙️ Dettagli della Pipeline

### Fase 1: Preparazione e Pulizia (Raw -> Silver)
*   **Input:** Bucket S3 "Raw".
*   **Azioni:**
    *   Lettura dei file CSV.
    *   Gestione dei valori di prezzo mancanti (`-1`): eliminazione riga, riempimento con valore precedente (forward fill) o media/mediana dei 5 precedenti.
    *   Conversione in formato ottimizzato **Parquet**.
*   **Output:** Bucket S3 "Argento".

### Fase 2: Analisi e Aggregazione
*   **Smoothing:** Calcolo della **Media Mobile a 10 giorni** sui prezzi per ridurre il rumore.
*   **Join:** Unione dei dati di prezzo (giornalieri) con i Google Trends (settimanali).
*   **Schema Finale:** `data`, `prezzo_smussato`, `indice_google_trend`.

### Fase 3: Caricamento e Orchestrazione
*   **Redshift:** Le tabelle finali vengono caricate nel Data Warehouse.
*   **Step Functions:** Gestisce il parallelismo tra la pipeline BTC e la pipeline XMR, monitorando lo stato di esecuzione e gestendo eventuali errori.

## 🛠️ Stack Tecnologico

*   **Cloud Provider:** Amazon Web Services (AWS)
*   **Storage:** Amazon S3 (Raw, Silver Layers)
*   **Compute & ETL:** AWS Glue ETL oppure Amazon EMR (scelta implementativa)
*   **Orchestration:** AWS Step Functions
*   **Data Warahouse:** Amazon Redshift
*   **Visualization:** Amazon QuickSight (Opzionale)

## 🚀 Vantaggi
*   **Automazione:** Riduzione del lavoro manuale e degli errori operativi.
*   **Scalabilità:** Architettura pronta per gestire grandi volumi di dati e nuove criptovalute.
*   **Flessibilità:** Possibilità di personalizzare le logiche di trasformazione in Glue/EMR.

---
**Modalità di consegna:** File ZIP del progetto.
