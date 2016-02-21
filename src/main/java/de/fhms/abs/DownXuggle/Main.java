package de.fhms.abs.DownXuggle;

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

public class Main {

	public static void main(String[] args) throws IOException {

		String vidPath;
		int counter;
		String outPath;
		
		if (args.length < -1){ //TODO Anpassen wenn Link uebergeben wird
			System.out.println("input missing");
		}
		else {
			String[] data = getInput(args);
			String url = data[0];
			//String url = "https://upload.wikimedia.org/wikipedia/commons/4/4a/Anguilla-shoal-bay.ogg";
			
			//vidPath = VideoDownloader.download(url);
			FileSystem fs = FileSystem.get(new Configuration());
			String homePath= fs.getWorkingDirectory().toString();
			vidPath = "hugo/tmp_video/" + data[1];
			String vidFileName = data[1].split("\\.")[0];
			if (vidPath != null && !vidPath.equals("")){
				outPath = "Frames/";
				String[] inOut = new String[]{vidPath, outPath, vidFileName};
				VideoFrameSplitter.split(inOut);
				System.out.println("Success.");

				if (VideoFrameSplitter.getCounter() > 0){
					counter = VideoFrameSplitter.getCounter();
					String op = VideoFrameSplitter.getOutputfilename()+"/links.txt";
					FSDataOutputStream os = fs.create(new Path(op));
					BufferedWriter oFile = new BufferedWriter(new OutputStreamWriter(os));

					for (int i=0; i<counter; i++){
						
						String line = String.format(fs.getWorkingDirectory()+"/" + VideoFrameSplitter.getOutputfilename()+vidFileName+i+".png" + "\n",
						System.getProperty("line.separator"));
						oFile.write(line);
						System.out.println(line);
					}
					//os.close();
					oFile.close();

					String[] dataNew = new String[] {data[2], data[0]};
					writeDataFile(dataNew);
				}

			}

		}
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
}
