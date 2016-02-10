# hugo

Hier die Anmerkungen von Herrn George

  * Der Nutzer fordert eine Analyse eines Videos an (per Name, oder sonstige ID)
  * Bei der Anforderung kann der Anwender sagen, in wieviele "Bucket" jede Farbe gruppiert werden soll (255 bis 1)
  * Wenn diese in HBase vorliegt, dann wird das Ergebnis angezeigt
  * Ansonsten wird geprüft, ob das Video schon mal geraden wurde, und wenn ja, wird es noch mal direkt in HDFS verarbeitet indem ein Oozie Workflow angestossen wird, der die RGB Werte gruppiert und aggregiert
  *  Dazu wird von Mapper nach Reducer ein Datensatz übergeben ähnlich dem WordCount Beispiel, wobei der Schlüssel z.B. "R10" ist, der die roten (R) Werte von 0-10 gruppiert
  *  Der Compiler ist hier bestimmt super effektiv, denn mit nur 255 Werten pro Farbe und kleineren Gruppen, kann man dort schon hoch verdichten
  *  Die UI kann über REST den Workflow starten und überwachen und den Fortschritt anzeigen
  *  Ist das Video noch nicht in HDFS, dann wird dieses über einen weiteren Oozie Workflow erst heruntergeladen, in HDFS an eine Datei angehängt (HDFS Append) und dann in HBase der Name der Datei, der Offset des Videos darin und die Länge, welche dann oben in der wiederholten Verarbeitung genutzt wird
  *  Wird die Datei grösser als ein eingestellter Schwellenwert, dann wird eine neue Datei angelegt
