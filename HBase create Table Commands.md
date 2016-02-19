Über die Kommandozeile in die HBase Shell gehen:
``` hbase shell ```

Table für Videoinformatione erstellen:
"vmd" steht für Video Meta Data
``` create Table "vmd", "metadata", {NAME => "block", VERSIONS => 20} ```

Prüfen über:
```describe "vmd" ```

Table für Imageinformationen erstellen:
```create Table "imageData", "avarageColor", "dominantColor" ```
