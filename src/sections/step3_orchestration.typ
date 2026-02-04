#import "../utils.typ": screenshot-placeholder

== Step 3: Orchestrazione (Step Functions)

Per l'orchestrazione dei job ETL, ho scelto di utilizzare *AWS Step Functions*. Questa decisione mi ha permesso di creare un flusso di lavoro che gestisce centralmente il parallelismo delle esecuzioni e il monitoraggio di eventuali errori.

=== Configurazione IAM per Step Functions (Policy + Ruolo)

Prima di definire la logica della State Machine, mi sono concentrato sulla sicurezza, predisponendo i permessi necessari tramite una Policy custom e un Ruolo di esecuzione dedicato.

==== Fase 1: Creazione Policy IAM (Customer Managed)
Per garantire il rispetto del principio del privilegio minimo, ho definito una policy specifica che concede all'orchestratore solo i permessi strettamente necessari.

1.  Dalla Console AWS (*IAM* -> *Policies*), ho creato una nuova policy incollando la definizione JSON nel tab dedicato.
2.  Ho assegnato alla policy il nome `CryptoData-StepFunctions-GluePolicy`.

Questa policy autorizza esplicitamente l'avvio e il monitoraggio del job Glue specifico e la scrittura dei log su CloudWatch, senza esporre risorse non necessarie.

*Policy JSON (Permessi minimi)*:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "GlueStartAndMonitor",
      "Effect": "Allow",
      "Action": ["glue:StartJobRun", "glue:GetJobRun"],
      "Resource": "arn:aws:glue:eu-central-1:607374883457:job/CryptoData-ETL-Generic"
    },
    {
      "Sid": "CloudWatchLogsWrite",
      "Effect": "Allow",
      "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
      "Resource": [
        "arn:aws:logs:eu-central-1:607374883457:log-group:/aws/states/*",
        "arn:aws:logs:eu-central-1:607374883457:log-group:/aws/states/*:*"
      ]
    }
  ]
}
```

==== Fase 2: Creazione Ruolo IAM (Execution Role)
Successivamente, ho proceduto alla creazione del ruolo IAM che la Step Function assumerà durante l'esecuzione.

1.  In *IAM* -> *Roles*, ho creato un nuovo ruolo selezionando *Step Functions* come caso d'uso.
2.  Ho associato al ruolo la policy `CryptoData-StepFunctions-GluePolicy` creata nella fase precedente.
3.  Ho finalizzato la creazione assegnando il nome `StepFunctionsRole-CryptoData-Orchestrator`.

Ho verificato inoltre la relazione di fiducia (Trust Relationship), fondamentale per permettere al servizio `states.amazonaws.com` di assumere il ruolo.

*Trust Relationship (Relazione di fiducia)*:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "Service": "states.amazonaws.com" },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

=== Creazione della State Machine
Ho configurato la macchina a stati `CryptoData-Orchestrator` utilizzando il Workflow Studio della Console AWS, seguendo un approccio visuale:

1.  Ho creato una nuova state machine partendo dal template *Blank* (Standard) e ho associato il ruolo `StepFunctionsRole-CryptoData-Orchestrator`.
2.  Per ottimizzare i tempi di elaborazione, ho progettato il flusso inserendo uno stato *Parallel*. Questo mi ha consentito di definire due rami di esecuzione distinti:
    - Nel primo ramo, ho configurato il task *AWS Glue: StartJobRun* rinominandolo `RunGlueBTC`.
    - Nel secondo ramo, ho inserito un task identico rinominandolo `RunGlueXMR`.
3.  Ho concluso il flusso collegando l'uscita dello stato parallelo a uno stato finale di `Success`.
4.  Per garantire la robustezza della pipeline, ho aggiunto un blocco *Catch* sullo stato Parallel, configurandolo per intercettare qualsiasi errore (`States.ALL`) e reindirizzare il flusso verso uno stato `Fail`.

=== Configurazione dei Job
Per sfruttare la natura parametrica del job Glue implementato nello Step 2, ho iniettato i parametri specifici (`Arguments`) direttamente nella definizione dei task della Step Function. In questo modo, lo stesso job viene invocato due volte con argomenti diversi:

- Per il task `RunGlueBTC` ho impostato:
  - `--coin`: `BTC`
  - `--bronze_bucket`: `s3://cryptodata-insights-bronze`
  - `--silver_bucket`: `s3://cryptodata-insights-silver`
  - `--gold_bucket`: `s3://cryptodata-insights-gold`

- Per il task `RunGlueXMR` ho impostato:
  - `--coin`: `XMR`
  - (Mantenendo identici i riferimenti ai bucket)

=== Snippet ASL (Amazon States Language)
Di seguito riporto la definizione JSON completa della state machine che ho implementato in Amazon States Language:

```json
{
  "Comment": "Orchestrator per CryptoData ETL",
  "StartAt": "ParallelProcessing",
  "States": {
    "ParallelProcessing": {
      "Type": "Parallel",
      "Next": "Success",
      "Catch": [ { "ErrorEquals": ["States.ALL"], "Next": "Fail" } ],
      "Branches": [
        {
          "StartAt": "RunGlueBTC",
          "States": {
            "RunGlueBTC": {
              "Type": "Task",
              "Resource": "arn:aws:states:::glue:startJobRun.sync",
              "Parameters": {
                "JobName": "CryptoData-ETL-Generic",
                "Arguments": {
                  "--coin": "BTC",
                  "--bronze_bucket": "s3://cryptodata-insights-bronze",
                  "--silver_bucket": "s3://cryptodata-insights-silver",
                  "--gold_bucket": "s3://cryptodata-insights-gold"
                }
              },
              "End": true
            }
          }
        },
        {
          "StartAt": "RunGlueXMR",
          "States": {
            "RunGlueXMR": {
              "Type": "Task",
              "Resource": "arn:aws:states:::glue:startJobRun.sync",
              "Parameters": {
                "JobName": "CryptoData-ETL-Generic",
                "Arguments": {
                  "--coin": "XMR",
                  "--bronze_bucket": "s3://cryptodata-insights-bronze",
                  "--silver_bucket": "s3://cryptodata-insights-silver",
                  "--gold_bucket": "s3://cryptodata-insights-gold"
                }
              },
              "End": true
            }
          }
        }
      ]
    },
    "Success": { "Type": "Succeed" },
    "Fail": { "Type": "Fail" }
  }
}
```

#figure(
  image("../assets/step_functions_graph.png", width: 50%),
  caption: "Diagramma del flusso di lavoro visualizzato in Workflow Studio. Si evidenzia lo stato `ParallelProcessing` che ramifica l'esecuzione verso i due job Glue (`RunGlueBTC` e `RunGlueXMR`), convergendo successivamente nello stato di successo o fallimento."
)