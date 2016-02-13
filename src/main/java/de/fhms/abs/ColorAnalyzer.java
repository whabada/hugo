package de.fhms.abs;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

import javax.imageio.ImageIO;
import javax.imageio.stream.ImageInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class ColorAnalyzer extends Configured implements Tool {

	private final static DoubleWritable one = new DoubleWritable(1);
	private final static String URL = "UrlString";

	public static class ColorAnalyzerMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

		private String urlStr;
		private Configuration conf;

		@Override
		protected void setup(Context context) {
			conf = context.getConfiguration();
			urlStr = conf.get("UrlString");
		} //TODO Mapper wird haeufiger aufgerufen als er soll

		/**
		 * Die Mapper Funktion erhaelt ein Bild und analysiert die Farbe saemtlicher Pixel des erhaltenen Images. 
		 * Die Funktion funktioniert nur, wenn das Bild nicht null ist und groeßer als 1x1 ist.
		 * Für jede Farbe R, G, B wird ausgebenen "RGB Farbcode, 1". 
		 * @param key Nummer des Frames 
		 * @param Text url
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		public void map(LongWritable key, Text url, Context context) throws IOException, InterruptedException {

			BufferedImage image = null;
			FileSystem fs = FileSystem.get(context.getConfiguration());
			Path inputPath = new Path(urlStr);
			try {
				image = ImageIO.read(new File ("/home/cloudera/Pictures/sample_10.png"));
			/*	FSDataInputStream in = fs.open(inputPath);
				ImageInputStream imageInput = ImageIO.createImageInputStream(in);
				image = ImageIO.read(imageInput); 
				imageInput.close();*/
			} catch (IOException e) {
				System.out.println("no input!");
			} 

			if (image != null && image.getWidth() > 1 && image.getHeight() > 1) {
				// Get dimensions of image
				int w = image.getWidth();
				int h = image.getHeight();
				long sumPixel = w*h;
				int counter =0;
				counter ++;
				System.out.print(counter+ " ");

				int r = 0, g = 0, b = 0;
				for (int x = 0; x < w; x++) {
					for (int y = 0; y < h; y++) {
						Color pixel = new Color(image.getRGB(x, y));
						r = pixel.getRed(); //rot
						g = pixel.getGreen(); // gruen
						b = pixel.getBlue(); // blau

						//TODO RGB Codes falsch!
						Text RKey =new Text("R "+String.valueOf(r)+ "," + String.valueOf(sumPixel));
						Text GKey =new Text("G "+String.valueOf(g)+ "," + String.valueOf(sumPixel));
						Text BKey =new Text("B "+String.valueOf(b)+ "," + String.valueOf(sumPixel));
						
				/*		System.out.println("r: " + r);
						System.out.println("g: " + g);
						System.out.println("b: " + b); */


						/** TODO Blocks kommen spaeter.
						 * TODO Das mit den Blocks stimmt nicht. 
						 * Muss der nicht zaehlen, wie haeuig ein Block auftritt? Aber was dann?
						Text RKey =new Text("R"+blocks(sumr)+ "," + String.valueOf(sumPixel));
						Text GKey =new Text("G"+blocks(sumg)+ "," + String.valueOf(sumPixel));
						Text BKey =new Text("B"+blocks(sumb)+ "," + String.valueOf(sumPixel));
						 */
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
//TODO Combiner nicht richtig
	public static class AverageColorCombiner extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{

		@Override
		protected void reduce(Text key, Iterable<DoubleWritable> values,
				Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {

			String[] splitKey = key.toString().split(",");
			String code = splitKey[0];
			long sumPixel =Long.parseLong(splitKey[1]);
			System.out.println("code: " + code);
			System.out.println("sumpixel: "+ sumPixel);

			String[] codeString = code.split(" ");
			String farbtype = codeString[0];
			int farbcode = Integer.parseInt(codeString[1]);
			System.out.println("farbcode: "+ farbcode);

			double amount = 0;

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
			System.out.println("amount: "+ amount);


			double relAnt = amount / sumPixel; //Relativer Anteil 
			double antDurch = relAnt*farbcode; //Anteil am Durchschnitt
			System.out.println("relAnt: "+ relAnt);
			System.out.println("antDurch: "+ antDurch);


			context.write(new Text(farbtype), new DoubleWritable(antDurch));

		}
	}

	public static class AverageColorReducer extends Reducer<Text, DoubleWritable, Text, Text> {

		/** 
		 * Hier wird gezaehlt, wie häufig ein R G B Code vom Mapper "gesehen" wurde
		 * Dieses Ergebnis wird aufsummiert und dann als Durchschnittswert ausgegeben 
		 */

		@Override
		protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

			//	String[] splitKey = key.toString().split(",");
			//	String code = splitKey[0];
			//	long farbcode = Long.parseLong(splitKey[1]);
			//	String code = key.toString();

			//long sumPixel =0;
			//	long sumPixel =Long.parseLong(splitKey[2]);
			//	long farbsum=0; 
			/*
			String[] splitResult = key.toString().split(",");
			String r = splitResult[0];
			String g = splitResult[1];
			String b = splitResult[2]; 
						Text result = new Text (r + " " + g + " "+ b);
			 */

			int result = 0;
			
			for (DoubleWritable val : values){
				result += val.get();
				System.out.println("result: "+ result);

			}
			
			context.write(key, new Text(String.valueOf(result)));

			/*
			for (IntWritable val : values) {//je nachdem wie haeufig ein Wert angegeben wird, wird dieser addiert
				sumr += Long.valueOf(splitKey[0]);  
				sumg += Long.valueOf(splitKey[1]);
				sumb += Long.valueOf(splitKey[2]); ;
			} 

			//Durchschnitt bilden
			sumr = sumr/sumPixel;
			sumb = sumb/sumPixel;
			sumg = sumg/sumPixel; */

			/*
			if (code.startsWith("R")){ 
//TODO das stimmt nicht! der zaehlt ja die Anzahl, wie haeufig der das gesehen hat, nicht die Anzahl der Farbe
				for (IntWritable val : values) {
					farbsum += farbcode; 
				}
			}

			if (code.startsWith("G")){
				for (IntWritable val : values) {
					farbsum += farbcode;
				}

			}
			if (code.startsWith("B")){
				for (IntWritable val : values) {
					farbsum += farbcode;
				}
			}

			farbsum = farbsum/sumPixel; 


		//	Text resultText = new Text(sumr + " " + sumg + " " + sumb);
			Text resultText = new Text(String.valueOf(farbsum));
			Text resultKey = new Text (code+splitKey[1]);
			context.write(resultKey, resultText);	 */

		}
	}
	/**
	 * Gibt einen Intervall aus, in der die Zahl liegt. bspw. liegt die zahl 15 zwischen 10-20 wird 10 zurueck gegebe
	 * @param k Zahl, die geprueft werden soll
	 * @return untere grenze, in der eine zahl liegt
	 */
	public static String blocks(long k){
		String result = "";

		int grenzeOben =10;
		int grenzeUnten=0;
		for (int i=0; i<=255; i+=10){

			if(k==0){
				return result = String.valueOf(0);
			}

			else if(k<grenzeOben && k>=grenzeUnten){
				return result = String.valueOf(i);
			}

			grenzeOben+=10;
			grenzeUnten+=10;
		}

		return result;
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
	//	Path inputPath = new Path (args[0]);
	//	Path outputPath = new Path(args[1]);
		Path inputPath = new Path("sample_10.png");
		Path outputPath = new Path("pic1Out");
		conf.set(ColorAnalyzer.URL, inputPath.toString());

		conf.addResource(new Path("/etc/alternatives/hadoop-conf/core-site.xml"));
		conf.addResource(new Path("/etc/alternatives/hadoop-conf/hdfs-site.xml")); 

		FileSystem fs = FileSystem.get(conf);

		if (fs.exists(outputPath)) { 
			fs.delete(outputPath, true);
		}

		Job job = new Job(conf, "color Analyzer");
		job.setJarByClass(ColorAnalyzer.class);

		job.setMapperClass(ColorAnalyzerMapper.class);
		job.setReducerClass(AverageColorReducer.class);
		job.setCombinerClass(AverageColorCombiner.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class); 
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//job.setInputFormatClass(FileInputFormat.class);

		FileInputFormat.addInputPath(job, inputPath);	
		FileOutputFormat.setOutputPath(job, outputPath);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {

	/*	if (args.length <2){
			System.out.println("input and output missing!");
		} */
	
		int res = ToolRunner.run(new Configuration(), new ColorAnalyzer(), args);
		System.exit(res);
	} 
}
