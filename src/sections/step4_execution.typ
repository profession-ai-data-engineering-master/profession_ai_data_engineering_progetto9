#import "../utils.typ": screenshot-placeholder

== Step 4: Esecuzione e Monitoraggio
Dopo aver completato la configurazione, ho avviato la pipeline e ne ho verificato il corretto completamento attraverso il pannello di controllo della console AWS, ottenendo un feedback immediato sullo stato dell'orchestrazione.

=== Monitoraggio dell’Orchestrazione
Ho monitorato l'esecuzione della pipeline sfruttando le viste dettagliate fornite da AWS Step Functions. Questo mi ha permesso di osservare in tempo reale lo stato dei singoli task, confermare l'effettivo parallelismo dei job e validare l'esito finale della State Machine.

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
In questa fase ho concentrato l'attività di monitoraggio esclusivamente sull'orchestrazione della pipeline. I log applicativi dettagliati dei job ETL restano disponibili separatamente all'interno di AWS Glue e CloudWatch Logs, ma ho scelto di non includerli in questo step per focalizzare l'attenzione sulla validazione del flusso di controllo.