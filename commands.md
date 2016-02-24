# Anleitung zur Einrichtigung des Systems für HUGO

## Verwendetes System
Für das Projekt wurde die Cloudera VM in der Version 5.5.0 verwendet. Mit der vorherigen Version (5.4.2) gab es einige Komplikationen die mit einer Nutzung der neuen Version umgangen werden konnten.

## 1. Git Repository runterladen
```
$ mkdir BDE
$ mkdir BDE/hugo
$ cd BDE/hugo
$ clone https://github.com/whabada/hugo.git
$ cd $HOME
```
## 2. Web Server einrichten

Als Webserver wurde ein Apache2
Über den Befehl
```
$ sudo yum install php
$ sudo yum install httpd
```
wird dieser installiert

Die Konfigurations des Web Servers wurde durch uns geändert. Daher muss die neue httpd.conf aus dem Repository in das httpd Verzeichnis kopiert werden.

```$ sudo cp -f BDE/hugo/Website_files/httpd_conf/httpd.conf /etc/httpd/httpd.conf```

Die bestehenden Website Files aus dem Repository können jetzt in das html Verzeichnis des Webserver kopiert werden.

```$ sudo cp -a -f BDE/hugo/Website_files/html/. /var/www/html/.```

Für das Verzeichnis html müssen noch die Lese- und Schreibrechte geändert werden:

```
$ sudo chmod 777 -R /var/www/
$ sudo chmod 777 -R /var/www/html/cgi-bin/data.txt
$ sudo chmod 777 -R /var/www/html/
```

Requests Framework installieren
```
$ git clone git://github.com/kennethreitz/requests.git
$ cd requests
$ python setup.py install
```

Hadoopy Framework installieren
```
$ git clone https://github.com/bwhite/hadoopy.git
$ cd hadoopy
$ python setup.py install
```


Anschließend kann der Webserver gestartet werden. Das muss jedes mal erfolgen wenn die VM neugestartet wird

```
sudo /etc/init.d/httpd start
```
Die Webseite ist dann über http://localhost:81 zu erreichen.


## 3. HDFS Einrichten
Ordnerstruktur im HDFS einrichten
```
$ hdfs dfs mkdir hugo
$ hdfs dfs mkdir hugo/Frames
$ hdfs dfs mkdir hugo/videos
$ hdfs dfs mkdir hugo/tmp_video
$ hdfs dfs mkdir hugo/resultImages
```

Oozie Ornder kopieren

``` $ hdfs dfs -put BDE/hugo/Oozie_files/oozie/ ```

## 4. HBase Einrichten
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


## 5. HUGO Starten

Die Webseite über ```localhost:81``` im Webbrowser aufrufen.
Als Hyperlink kann dann ein Video von WikiMedia ```https://commons.wikimedia.org/wiki/Main_Page```angeben werden.
Der Link des Videos muss der direkte Downloadlink zu der Datei sein und darf keine Klammern enthalten, was bei manchen Videos der Fall ist.
Anschließend muss noch eine Blocksize eingeben Werden. Dies muss eine ganze Zahl in dem bereicht von 1 - 255 sein.
Die Analyse wird dann über einen Button gestartet. 

Der Vorgang kann je nach Videolänge auch über 15 min dauern.
Beispielshalber dauert die Analyse eines 2 Minuten Videos auf einem unserer Systeme(MacBook Pro, 8 GB RAM) ca. 30 min.

