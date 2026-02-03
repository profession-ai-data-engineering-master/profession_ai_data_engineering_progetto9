#import "../utils.typ": screenshot-placeholder

== Step 3: Orchestrazione (Step Functions)
Per gestire l'esecuzione parallela e le dipendenze, è stata creata una State Machine in AWS Step Functions.
Il flusso prevede:
1.  Stato `Parallel`: Esecuzione simultanea del Job BTC e del Job XMR.
2.  Stato `Crawler/Load`: Aggiornamento del catalogo dati o comando COPY verso Redshift.
3.  Stato `Success/Fail`: Notifica dell'esito.

// Screenshot 4
#screenshot-placeholder(
  "Il grafico (Graph View) della State Machine in Step Functions.",
  "step_functions_graph.png"
)
