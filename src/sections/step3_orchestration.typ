#import "../utils.typ": screenshot-placeholder

== Step 3: Orchestrazione (Step Functions)

Per orchestrare l'esecuzione dei job ETL ho utilizzato *AWS Step Functions*, creando un flusso che gestisce il parallelismo e il monitoraggio degli errori in modo centralizzato.

=== Configurazione IAM per Step Functions (Policy + Ruolo)

Prima di configurare la State Machine, è stato necessario predisporre i permessi di sicurezza creando una Policy custom e un Ruolo di esecuzione dedicato.

==== Fase 1: Creazione Policy IAM (Customer Managed)
Per garantire il principio del minimo privilegio, ho creato una policy specifica per l'orchestratore.

1.  AWS Console -> *IAM* -> *Policies* -> *Create policy*.
2.  Selezionare la tab *JSON* e incollare la definizione seguente.
3.  Clic su *Next*, assegnare il nome `CryptoData-StepFunctions-GluePolicy` e cliccare su *Create policy*.

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
Successivamente, ho creato il ruolo IAM che la Step Function assumerà.

1.  AWS Console -> *IAM* -> *Roles* -> *Create role*.
2.  *Trusted entity type*: Selezionare *AWS Service*.
3.  *Use case*: Selezionare *Step Functions*.
4.  *Permissions*: Cercare e selezionare la policy appena creata: `CryptoData-StepFunctions-GluePolicy`.
5.  *Role name*: Inserire `StepFunctionsRole-CryptoData-Orchestrator` e cliccare su *Create role*.

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
Ho configurato la macchina a stati `CryptoData-Orchestrator` direttamente dalla Console AWS:

1.  *Navigazione*: Step Functions -> State machines -> *Create state machine*.
2.  *Tipo*: Ho selezionato il template *Blank* (Standard) per costruire il flusso da zero.
3.  *Permissions*: Nella fase di configurazione, alla voce *Execution role* ho selezionato *Choose an existing role* e impostato `StepFunctionsRole-CryptoData-Orchestrator`.
4.  *Workflow Studio*:
    - Ho trascinato uno stato *Parallel* per eseguire contemporaneamente i due rami di elaborazione.
    - Nel primo ramo, ho aggiunto un task *AWS Glue: StartJobRun* rinominandolo `RunGlueBTC`.
    - Nel secondo ramo, ho aggiunto un task identico rinominandolo `RunGlueXMR`.
    - Ho collegato l'uscita dello stato Parallel a uno stato finale di *Success* (`Succeed`).
    - Ho configurato un *Catch* sullo stato Parallel che reindirizza a uno stato *Fail* in caso di errore di uno qualsiasi dei job.

=== Configurazione dei Job
Per rendere dinamico il job Glue, ho configurato i parametri di input (`Arguments`) direttamente nella definizione del task in Step Functions:

- Per il task `RunGlueBTC`:
  - `--coin`: `BTC`
  - `--bronze_bucket`: `s3://cryptodata-insights-bronze`
  - `--silver_bucket`: `s3://cryptodata-insights-silver`
  - `--gold_bucket`: `s3://cryptodata-insights-gold`

- Per il task `RunGlueXMR`:
  - `--coin`: `XMR`
  - (Gli altri parametri dei bucket rimangono identici)

=== Snippet ASL (Amazon States Language)
Ecco la definizione JSON essenziale della state machine implementata:

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