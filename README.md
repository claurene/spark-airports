# spark-airports
Par Laurene CLADT

## Données
- http://stat-computing.org/dataexpo/2009/the-data.html (2006, 2007 et 2008)
- http://stat-computing.org/dataexpo/2009/supplemental-data.html (données complémentaires)

## Sujet
### Travail demandé
Vous devez fournir les différents programmes Hadoop Map Reduce/Spark permettant de répondre aux
questions suivantes:

- quel est le meilleur moment (jour (par exemple le 15), jour dans la semaine (par exemple le mardi),
mois (mois de mai) pour partir si on veut minimiser les retards ?
- quelles sont les causes principales des retards (âge des avions, météo, compagnie...) ?
- quelles sont les compagnies les plus/moins sujettes aux retards (on classifiera les compagnies en 5
groupes) ?
- quels sont les 3 aéroports les plus/moins sujets aux retards (départ/arrivée) ?

On fera les hypothèses suivantes: si les attributs ArrDelay et DepDelay sont NA , on traitera la valeur
comme étant 0 . Si les atttributs Cancelled et Diverted sont NA , on considèrera que le vol n’a pas été
annulé/dérouté.

On vous demande également de restituer les résultats de façon graphique. Les graphiques devront être clairs
et devront permettre de rapidement répondre aux questions ci-dessus.

Vous présenterez également les données techniques (temps de traitement), et les hypothèses que vous aurez
été amnenés à faire.
