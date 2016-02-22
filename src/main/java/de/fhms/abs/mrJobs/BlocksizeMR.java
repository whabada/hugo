package de.fhms.abs.mrJobs;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;

import javax.imageio.ImageIO;
import javax.imageio.stream.ImageInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class BlocksizeMR extends Configured implements Tool {

	private final static DoubleWritable one = new DoubleWritable(1);
	private static long time = 0;
	private static Path filePath = null;
	
	public static class ColorAnalyzerMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
		
		int blocksize = 0;

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			Configuration conf = HBaseConfiguration.create(context.getConfiguration());
			 blocksize= Integer.valueOf(conf.get("blocksize")); //holt den Key fur die DB
		}
		/**
		 * Die Mapper Funktion erhaelt eine txt Datei, welche eine url zum Bild enthaelt. 
		 * Es wird die die Farbe saemtlicher Pixel eingeladenen Bildes analysiert, 
		 * in dem fuer jeden Farbwert gezaehlt wird, wie haeufig die Farbe erscheint.  
		 * Diese Werte sind in Farbbloecke sortiert
		 * Die Funktion funktioniert nur, wenn das Bild nicht null ist und groeßer als 1x1 ist.
		 *  
		 * @param key Nummer des Frames 
		 * @param Text url, link zum Bild. 
		 * @param context
		 * @return (Key, one); Für jede Farbe R, G, B wird ausgebenen "RGB Farbcode, one", bspw. R10, one. 
		 * @return ebenso wird die summe aller Pixel angehanden. 
		 * @throws IOException
		 * @throws InterruptedException
		 */
		public void map(LongWritable key, Text url, Context context) throws IOException, InterruptedException {

			BufferedImage image = null;

			FileSystem fs = FileSystem.get(context.getConfiguration());
			try { //Bild einladen
				Path inputPath = new Path(new URI(url.toString()));
			//	System.out.println(inputPath.toString());
				FSDataInputStream in = fs.open(inputPath);
				ImageInputStream imageInput = ImageIO.createImageInputStream(in);
				image = ImageIO.read(imageInput); 
			} catch (IOException e) {
				System.out.println(e.getMessage());
			} catch (URISyntaxException e) {
				e.printStackTrace();
				System.out.println(e.getMessage());
			} 

			if (image != null && image.getWidth() > 1 && image.getHeight() > 1) {
				// Get dimensions of image
				int w = image.getWidth();
				int h = image.getHeight();
				long sumPixel = w*h;

				int r = 0, g = 0, b = 0;
				for (int x = 0; x < w; x++) {
					for (int y = 0; y < h; y++) {
						Color pixel = new Color(image.getRGB(x, y));
						r = pixel.getRed(); //rot
						g = pixel.getGreen(); // gruen
						b = pixel.getBlue(); // blau
						//TODO: Blocksize weitergeben
						Text RKey =new Text("R "+blocks(r,blocksize)+ "," + String.valueOf(sumPixel));
						Text GKey =new Text("G "+blocks(g,blocksize)+ "," + String.valueOf(sumPixel));
						Text BKey =new Text("B "+blocks(b,blocksize)+ "," + String.valueOf(sumPixel));

						context.write(RKey, one);
						context.write(GKey, one);
						context.write(BKey, one); 
					}
				}
			}
			else { 
				System.out.println("kein Bild."); //Kein Bild geladen
			}
		}
	}

	public static class AverageColorCombiner extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{

		/**
		 * Combiner aggregiert die Haeufigkeit eines "gesehenen" Farbcodes.
		 * Berechnet anschliessend den relativen Anteil der Farbe am gesamten Farbspektrum
		 * Berechnet abschliessend den Anteil am Durchschnitt.  
		 * @param Text key besteht aus dem Farbtyp, den Farbcode und Haeufigkeit, z.B. R 10, 4
		 * @param DoubleWritable values
		 * @param Context context
		 * @return Text: Farbtyp
		 * @return DoubleWritable: Farbanteil am Durchschnitt
		 * Gibt als neuen key den Farbcode aus (R G oder B) sowie den Anteil am Durchschnitt
		 */
		@Override
		protected void reduce(Text key, Iterable<DoubleWritable> values,
				Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {

			String[] splitKey = key.toString().split(",");
			String code = splitKey[0]; //Farbcode extrahieren, z.B. R10
			long sumPixel =Long.parseLong(splitKey[1]); //Anzahl der Pixel gesamt

			String[] codeString = code.split(" ");
			String farbtype = codeString[0]; //extrahieren des Farbtypes, R, G oder B 
			int farbcode = Integer.parseInt(codeString[1]); //extrahieren des Farbcodes, um Beispiel 10.

			double amount = 0; 
			// Aufsummieren der Haeufigkeit, summiert nach R G und B 
			if (code.startsWith("R")){ 
				for (DoubleWritable val : values) {
					amount += val.get();
				}
			}

			else if (code.startsWith("G")){
				for (DoubleWritable val : values) {
					amount += val.get();
				}

			}
			else if (code.startsWith("B")){
				for (DoubleWritable val : values) {
					amount += val.get();
				}
			}

			double relAnt = amount / sumPixel; //Berechnung relativer Anteil 
			double antDurch = relAnt*farbcode; //Berechnung Farbanteil am Durchschnitt

			context.write(new Text(farbtype), new DoubleWritable(antDurch));

		}
	}

	public static class AverageColorReducer extends TableReducer<Text, DoubleWritable, ImmutableBytesWritable> {
		private String tmpKey ="";
		/**
		 * @param key: Die Eingangskey ist der Farbcode (R,G,B).
		 * @param Iterable values: enthalten den durchschnittlichen Anteil des Farbcodes 
		 * @param context
		 * @return Gibt den finalen Durchschnittswert aller R Werte, aller G Werte und aller B Werte aus, geschrieben in HBASE
		 * @return key: R, G oder B 
		 * @return text: resultValue
		 */
		@Override
		protected void setup(Reducer<Text, DoubleWritable, ImmutableBytesWritable, Mutation>.Context context)
				throws IOException, InterruptedException {
			Configuration conf = HBaseConfiguration.create(context.getConfiguration());
			tmpKey = conf.get("keyDB"); //holt den Key fur die DB
		}
		@Override
		protected void reduce(Text key, Iterable<DoubleWritable> values,Context context)
				throws IOException, InterruptedException {
			int result = 0;

			for (DoubleWritable val : values){
				result += val.get();
			}
			String resultStr = String.valueOf(result);
			Put put = new Put (Bytes.toBytes(tmpKey));
			put.addColumn(Bytes.toBytes("averageColor"), Bytes.toBytes(key.toString()), Bytes.toBytes(resultStr));
			put.addColumn(Bytes.toBytes("dominantColor"), Bytes.toBytes(key.toString()), Bytes.toBytes(0));

			context.write(null, put); 
		}
	}
	/**
	 * Gibt einen Intervall aus, in der die Zahl liegt. bspw. liegt die zahl 15 zwischen 10-20 wird 10 zurueck gegebe
	 * @param k Zahl, die geprueft werden soll
	 * @return untere grenze, in der eine zahl liegt
	 */
	public static String blocks(long k, int b){
		String result = "";

		int grenzeOben =b;
		int grenzeUnten=0;

		if(k==0){
			return result = String.valueOf(0);
		}
		for (int i=0; i<=255; i+=b){

			if(k<grenzeOben && k>=grenzeUnten){
				return result = String.valueOf(i);
			}
			grenzeOben+=b;
			grenzeUnten+=b;
		}
		return result;
	}

	//	@Override
	public int run(String[] args) throws Exception {

		Path inputPath = new Path (args[0]);

		Configuration conf = HBaseConfiguration.create(getConf());
		conf.set("keyDB", args[1]); //DBKey aus Conf abfragen
		conf.set("blocksize", args[2]); //Blocksize aus Conf abfragen

		Job job = new Job(conf, "color Analyzer");
		job.setJarByClass(BlocksizeMR.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(ColorAnalyzerMapper.class);
		job.setCombinerClass(AverageColorCombiner.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class); 

		TableMapReduceUtil.initTableReducerJob("imageData", AverageColorReducer.class, job);

		FileInputFormat.addInputPath(job, inputPath);	

		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * Mein Mathode fuer den MR Job. Es wird fuer jede Zeile der Links.txt ein MapReduceJob gestartet 
	 * und ein OutputOrdner angeben.
	 * @param args: sind leer
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		String[] data = getInput(args);
		String bs = data [0]; //Blocksize
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path("/etc/alternatives/hadoop-conf/core-site.xml"));
		conf.addResource(new Path("/etc/alternatives/hadoop-conf/hdfs-site.xml"));
		FileSystem fs = FileSystem.get(conf);

		String staticDir = fs.getHomeDirectory()+"/hugo"+"/Frames/";
		Path inputPath = getLastModifiedHdfsFile(fs, new Path(staticDir));
	
		//System.out.println("1: " + inputPath);
		FSDataInputStream in = fs.open(inputPath);
		int counter = 0;
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));

		String line = reader.readLine(); 
		String lineZero = ""; //Erste Line enthaelt DBkey
		String keyDB = ""; //Initialisierung 
		String indexcount = "";
		int res=-1;

		while (line != null){
			indexcount = imageCount(counter); //counter wird auf siebenstelliges Format gebracht und ...

			if (counter == 0){
				lineZero = line;
				counter++;
				keyDB = lineZero+"_"+indexcount; //... an Key angehangen
			}
			else {
				String [] tempDir = createTmpDir(line);
				if(!tempDir[0].equals("") && !tempDir[1].equals("")){
					String newPathStr = tempDir[0];

					String op = newPathStr+".txt";
					FSDataOutputStream os = fs.create(new Path(op));
					BufferedWriter oFile = new BufferedWriter(new OutputStreamWriter(os));
					oFile.write(line);
					oFile.flush();
					oFile.close();

					keyDB = lineZero+ "_" +indexcount; //... an Key angehangen
					String [] array = new String[]{op, keyDB, bs}; //Uebergabe der Parameter fuer Run Methode
					res = ToolRunner.run(new Configuration(), new BlocksizeMR(), array); //start ders MR Jobs

					counter ++; //counter inkrementieren
					String delDirStr = tempDir[1];
					fs.delete(new Path(delDirStr), true); //temporaererstellter Pfad loeschen
				}
				line = reader.readLine();
			}
		}
		reader.close();  
		
		String[] data2 = new String[] {"getImagesOfVideo", data[1]};
		writeDataFile(data2);
		
		System.exit(res);

	}
	/**
	 * Diese Methode schiebt den Order "tmp" vor dem Unterordner Frames und gibt ebenso das erstellte Directory zurueck
	 * @param url: String Datei mit dem Verzeichnisnamen
	 * @return String [] bestehend aus dem String neuen Pfadnamen, allerdings nur das Wort Frames im Pfad enthalten ist 
	 * sowie String delDir, welcher den zu löschenden Pfad angibt.
	 */
	public static String[] createTmpDir (String url){

		String [] pathArray = url.split("/");
		int pos = -1;
		for (int i=0; i<pathArray.length; i++){
			if (pathArray[i].contains("Frames")){
				pos = i;
			}
		}
		if (pos != -1){
			String[] newPath = new String [pathArray.length+1];
			String delDirStr = "";
			boolean posReached = false;
			for (int i=0; i<newPath.length; i++){
				if (i == pos){
					posReached = true;
					newPath[i] = "tmp/";
					delDirStr += newPath[i];
				}
				else if (posReached == true){
					newPath[i] = pathArray[i-1];
					if (i != newPath.length-1){
						newPath[i]+="/";
					}
				}
				else {
					newPath[i] = pathArray[i]+"/";
					delDirStr += newPath[i];
				}
			}
			String newPathStr = "";
			//		System.out.println(delDirStr);
			for (int i=0; i <newPath.length; i++){
				newPathStr += newPath[i];
			}
			return new String []{newPathStr, delDirStr};
		}
		return new String []{"",""};
	}

	/**
	 * die Methode bringt den uebergebenen Integer in eine 7 stellige Form
	 * @param i Zahl, die anhangen werden soll
	 * @return i in siebenstelliger Form
	 */
	public static String imageCount(int i) {
		if (i<10) {
			return "000000" + i;
		} else if (i<100) {
			return "00000" + i;
		} else if (i<1000) {
			return "0000" + i;
		} else if (i<1000) {
			return "000" + i;
		} else if (i<10000) {
			return "00" + i;
		} else if (i<100000) {
			return "0" + i;
		} else if (i<1000000) {
			return "" + i;
		} else {
			return "" + i;
		}
	}
	/**
	 * 
	 * @param fs FileSystem
	 * @param rootDir Dir, von dem aus die Methode gestartet werden soll
	 * @return filePath zu der Datei, die als letztes modifziert wurde
	 */
	private static Path getLastModifiedHdfsFile(FileSystem fs, Path rootDir) {
		
		try {

			FileStatus[] status = fs.listStatus(rootDir);
			for (FileStatus file : status) {
				if (file.isDir()) {
					//System.out.println("DIRECTORY : " + file.getPath() + " - Last modification time : " + file.getModificationTime());
					getLastModifiedHdfsFile(fs, file.getPath());
				} else {
					if (file.getModificationTime() > time) {
						time = file.getModificationTime();
						filePath = file.getPath();
					}
				}
			}
		} catch (IOException e) {
			System.out.println("File not found");
			e.printStackTrace();
		}

	//	System.out.println("FILE : " + filePath + " - Last modification time : " + time);
		return filePath;  
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
