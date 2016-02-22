# Anleitung zur Einrichtigung des Systems für HUGO

## Verwendetes System
Für das Projekt wurde die Cloudera VM in der Version 5.5.0 verwendet. Mit der vorherigen Version (5.4.2) gab es einige Komplikationen die mit einer Nutzung der neuen Version umgangen werden konnten.

## HBase Einrichten
HUGO benötigt zum speichern seiner Daten zwei Tabellen in welchen Ergebnisse gespeichert werden. Zum einen die Tabelle "vmd" (Video Meta Data") in welcher die folgenden Informationen gespeichert werden:
- filesize
- hyperlink
- name
- offset
- path
- blocksize

Die unterteilen sich in zwei Column Families "metadata" und "block"

Um die HBase Shell aufzurufen den folgenden Befehl in der CLI eingeben:

``` hbase shell ```

Die Tablle "vmd" wird über den folgenden Befehl erstellt:

``` create "vmd", "metadata", {NAME => "block", VERSIONS => 20} ```


Die zweite Tabelle (imageData) speichert die Ergebnisse (Durchschnittsfarbe und Dominantefarbe) des MapReduce Jobs zu jedem Frame.
Die Tablle "imageData" wird über den folgenden Befehl erstellt:

``` create "imageData", "averageColor", "dominantColor" ```

