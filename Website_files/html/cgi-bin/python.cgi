#!/usr/bin/python

import cgi
import cgitb
import urllib2
import urllib
import json
import requests
import time
import cgi
import sys
import hadoopy
import os
import shutil

# HEADERS
print "Content-Type:text/html; charset=UTF-8"
print  # blank line required at end of headers

sys.stdout.flush()
# CONTENT
form = cgi.FieldStorage()
url =  form.getvalue('URL')
blocksize = form.getvalue('Blocksize')
analysisType = form.getvalue('type')

text_file = open("data.txt", "w")
text_file.write("getVideoRecord\n")
text_file.write(url + "\n")
text_file.write(blocksize + "\n")
text_file.write(analysisType)
text_file.close()

hdfs_path = '/user/cloudera/oozie/data.txt'
local_path = 'data.txt'
hadoopy.rmr(hdfs_path)
hadoopy.put(local_path, hdfs_path)


XML_STRING = open('properties.xml').read()
url = "http://localhost:11000/oozie/v1/jobs?action=start"
headers = {'Content-type': 'application/xml'}
r = requests.post(url, data=XML_STRING, headers=headers)
result = r.json()
jobId = result["id"]

req = urllib2.Request('http://localhost:11000/oozie/v1/job/' + result["id"] + '?show=info&timezone=GMT')
response = urllib2.urlopen(req)
output = response.read()
data = json.loads(output)
status = data['status']
c = 0
steps = 6

req = urllib2.Request('http://localhost:11000/oozie/v1/job/' + result["id"] + '?show=info&timezone=GMT')
response = urllib2.urlopen(req)
output = response.read()
data = json.loads(output)

print "<html>"
print	"<head>"
print	"<title>HUGO</title>"
print	"</head>"
print	'<body bgcolor = "lightgrey" style="font-family: Arial">'
print      	'<h1 align=center><font color="red">H</font><font color="blue">U</font><font color="green">G</font><font color="yellow">O</font></h1>'
print   	'<h2 align=center><font color="red">H</font>ighly <font color="blue">U</font>seful <font color="green">G</font>raphical <font color="yellow">O</font>utput</h2>'
print	'<div id="progress" style="width:500px;border:1px solid #ccc;"><div style="width:0%;background-color:#ddd;\">&nbsp;</div></div>'
print	'<div id="information" style="width"></div>'
namez = ''
while (status != 'SUCCEEDED') :
 req = urllib2.Request('http://localhost:11000/oozie/v1/job/' + result["id"] + '?show=info&timezone=GMT')
 response = urllib2.urlopen(req)
 output = response.read()
 data = json.loads(output)
 actions = data['actions']
 action = actions[c];
 name = action['name']
 status = data['status']
 es = action['status']
 if es == 'OK':
   c = c+1
   name = action['transition']
 percent = float(c) / steps
 percent = percent * 100.0
 print '<script language="javascript">document.getElementById("progress").innerHTML="<div style=\"width:' + str(int(percent)) + '%;background-color:#ddd;\">&nbsp;</div>";</script>'
 print '<script language="javascript">document.getElementById("information").innerHTML="' + name + '"</script>'
 sys.stdout.flush()
 time.sleep(5.0)

print "job fertig"
os.remove("data.txt")
hadoopy.get(hdfs_path, local_path)


f = open('data.txt', 'r')
imagepath = f.readline()
imagename = imagepath.rsplit('/', 1)[1]
i = imagename.replace("\n", "")
o = '/var/www/html/' + i
outputpath = "/var/www/http/result.png"
try:
  os.remove(i)
except: 
  pass

try:
  os.remove("/var/www/html/" + i)
except: 
  pass

try:
  hadoopy.get(imagepath, outputpath)
except: 
  pass



shutil.copyfile(i, o)

print '<img align=center src="/' + i + '" alt="result" width="80%">'
print '<script language="javascript">document.getElementById("progress").innerHTML="<div style=\"width:100%;background-color:black;\">&nbsp;</div>";</script>';



print "</body></html>"

