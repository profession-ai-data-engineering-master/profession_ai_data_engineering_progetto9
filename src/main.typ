#import "utils.typ": screenshot-placeholder

#set page(
  paper: "a4",
  margin: (x: 1.8cm, y: 1.5cm),
)
#set text(
  font: "New Computer Modern",
  size: 11pt,
)
#set heading(numbering: "1.")

#let title = "Pipeline E2E AWS per l'Analisi di Criptovalute: Il caso CryptoData Insights"
#let authors = ("Studente / Data Engineer")
#let date = datetime.today().display()

// Title block
#align(center, text(17pt, weight: "bold", title))
#v(0.5em)
#align(center, text(12pt, style: "italic", authors))
#v(0.5em)
#align(center, text(date))
#v(2em)

// Indice (Table of Contents)
#show outline.entry: it => {
  it
  v(1pt, weak: true)
}
#outline(title: "Indice dei Contenuti", indent: auto)
#v(2em)

// Two-column layout removed for better readability
// #show: rest => columns(2, rest)

// Importazione delle Sezioni
#include "sections/abstract.typ"
#include "sections/introduction.typ"
#include "sections/architecture.typ"

= Implementazione della Pipeline AWS
La seguente sezione descrive gli step operativi eseguiti sulla piattaforma AWS per realizzare l'infrastruttura.

#include "sections/step1_storage.typ"
#include "sections/step2_warehouse.typ"
#include "sections/step3_etl.typ"
#include "sections/step4_orchestration.typ"
#include "sections/step5_execution.typ"
#include "sections/step6_results.typ"

#include "sections/conclusions.typ"
