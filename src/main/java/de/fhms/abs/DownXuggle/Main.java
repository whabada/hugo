package de.fhms.abs.DownXuggle;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
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
			//String url = args[0];
			String url = "https://upload.wikimedia.org/wikipedia/commons/4/4a/Anguilla-shoal-bay.ogg";
			vidPath = VideoDownloader.download(url);
			FileSystem fs = FileSystem.get(new Configuration());
			String homePath= fs.getWorkingDirectory().toString();

			if (vidPath != null && !vidPath.equals("")){
				outPath = "Frames/";
				String[] inOut = new String[]{homePath+"/"+vidPath, outPath};
				VideoFrameSplitter.split(inOut);
				System.out.println("Success.");

				if (VideoFrameSplitter.getCounter() > 0){
					counter = VideoFrameSplitter.getCounter();
					String op = VideoFrameSplitter.getOutputfilename()+"/links.txt";
					FSDataOutputStream os = fs.create(new Path(op));
					BufferedWriter oFile = new BufferedWriter(new OutputStreamWriter(os));

					for (int i=0; i<counter; i++){
						String line = String.format(fs.getWorkingDirectory()+"/" + VideoFrameSplitter.getOutputfilename()+VideoDownloader.getVideoFilename()+i+".png" + "\n",
						System.getProperty("line.separator"));
						oFile.write(line);
						System.out.println(line);
					}
					//os.close();
					oFile.close();

				}

			}

		}
	}

}
