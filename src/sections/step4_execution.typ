#import "../utils.typ": screenshot-placeholder

== Step 4: Esecuzione e Monitoraggio
La pipeline è stata avviata ed è stato verificato il corretto completamento di tutti gli step attraverso il pannello di controllo della console AWS, che fornisce feedback immediato sullo stato dell'orchestrazione.

=== Monitoraggio dell’Orchestrazione
L’esecuzione della pipeline è stata monitorata tramite le viste di esecuzione fornite da AWS Step Functions, che consentono di osservare lo stato dei singoli task, il parallelismo effettivo e l’esito finale della State Machine.

#figure(
  image("../assets/step_functions_graph_execution_success.png", width: 100%),
  caption: "Visualizzazione grafica dell'esecuzione (\"Graph view\"). I rami paralleli verdi confermano che i job `RunGlueBTC` e `RunGlueXMR` sono stati eseguiti con successo, validando la logica di parallelismo della State Machine."
)

#figure(
  image("../assets/step_functions_events_execution_success.png", width: 100%),
  caption: "Dettaglio degli eventi di esecuzione (\"Table view\"). I timestamp evidenziano la transizione simultanea verso i task paralleli, confermando la concorrenza temporale gestita dall'orchestratore senza dipendenze sequenziali bloccanti."
)

#figure(
  image("../assets/step_functions_execution_list.png", width: 100%),
  caption: "Lista delle esecuzioni storiche. Lo stato `Succeeded` ricorrente dimostra la stabilità del workflow e la capacità dell'orchestratore di gestire correttamente cicli di esecuzione multipli."
)

=== Nota sul Logging
In questa fase il monitoraggio si concentra sull’orchestrazione della pipeline. I log applicativi dettagliati dei job ETL sono disponibili separatamente all’interno di AWS Glue e CloudWatch Logs, ma non sono oggetto delle schermate riportate in questo step, che mira a validare esclusivamente il flusso di controllo.