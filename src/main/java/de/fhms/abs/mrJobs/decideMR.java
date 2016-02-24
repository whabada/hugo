package de.fhms.abs.mrJobs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class decideMR {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		String[] data = getInput(args);
		
		if (data[3] == "average") {
			BlocksizeMR.main(args);
		} else {
			DominantColorMR.main(args);
		}
	}
	
	public static String[] getInput(String[] argv) throws IOException {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/etc/alternatives/hadoop-conf/core-site.xml"));
		conf.addResource(new Path("/etc/alternatives/hadoop-conf/hdfs-site.xml"));
		FileSystem fs = FileSystem.get(conf);

		FSDataInputStream in = fs.open(new Path (argv[0]));
		FSDataInputStream in2 = fs.open(new Path (argv[0]));

		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		BufferedReader reader2 = new BufferedReader(new InputStreamReader(in2));
		int counter = 0;
		String line = reader.readLine(); 

		while (line != null){
			counter++;
			line = reader.readLine();
		}
		String[] array = new String[counter];
		counter = 0;
		line = reader2.readLine();
		while (line != null){
			array[counter] = line;
			counter++;
			line = reader2.readLine();
		}
		return array;
	}
	
	public static void writeDataFile (String[] data) throws IOException {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/etc/alternatives/hadoop-conf/core-site.xml"));
		conf.addResource(new Path("/etc/alternatives/hadoop-conf/hdfs-site.xml"));
		FileSystem fs = FileSystem.get(conf);

		FSDataOutputStream os = fs.create(new Path("oozie/data.txt"));
		BufferedWriter oFile = new BufferedWriter(new OutputStreamWriter(os));

		for (String l: data){
			oFile.write(l + "\n");
		}

		oFile.flush();
		oFile.close();
	}

}
