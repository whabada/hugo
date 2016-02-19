package de.fhms.abs.mrJobs;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.IOException;

import javax.imageio.ImageIO;
import javax.imageio.stream.ImageInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class AverageColorMr extends Configured implements Tool {

	private final static DoubleWritable one = new DoubleWritable(1);

	public static class ColorAnalyzerMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

		/**
		 * Die Mapper Funktion erhaelt eine txt Datei, welche eine url zum Bild enthaelt. 
		 * Es wird die die Farbe saemtlicher Pixel eingeladenen Bildes analysiert, 
		 * in dem fuer jeden Farbwert gezaehlt wird, wie haeufig die Farbe erscheint. 
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
			Path inputPath = new Path(url.toString());
			try { //Bild einladen
				FSDataInputStream in = fs.open(inputPath);
				ImageInputStream imageInput = ImageIO.createImageInputStream(in);
				image = ImageIO.read(imageInput); 
			} catch (IOException e) {
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

						Text RKey =new Text("R "+String.valueOf(r)+ "," + String.valueOf(sumPixel));
						Text GKey =new Text("G "+String.valueOf(g)+ "," + String.valueOf(sumPixel));
						Text BKey =new Text("B "+String.valueOf(b)+ "," + String.valueOf(sumPixel));

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

	public static class AverageColorReducer extends Reducer<Text, DoubleWritable, Text, Text> {

		/**
		 * @param key: Die Eingangskey ist der Farbcode (R,G,B).
		 * @param Iterable values: enthalten den durchschnittlichen Anteil des Farbcodes 
		 * @param context
		 * @return Gibt den finalen Durchschnittswert aller R Werte, aller G Werte und aller B Werte aus
		 * @return key: R, G oder B 
		 * @return text: resultValue
		 */
		@Override
		protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

			int result = 0;

			for (DoubleWritable val : values){
				result += val.get();
			}
			context.write(key, new Text(String.valueOf(result)));
		}
	}

//	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Path inputPath = new Path (args[0]);
		Path outputPath = new Path(args[1]);

		conf.addResource(new Path("/etc/alternatives/hadoop-conf/core-site.xml"));
		conf.addResource(new Path("/etc/alternatives/hadoop-conf/hdfs-site.xml")); 

		FileSystem fs = FileSystem.get(conf);

		if (fs.exists(outputPath)) { 
			fs.delete(outputPath, true);
		}

		Job job = Job.getInstance(conf);
		job.setJarByClass(AverageColorMr.class);

		job.setMapperClass(ColorAnalyzerMapper.class);
		job.setReducerClass(AverageColorReducer.class);
		job.setCombinerClass(AverageColorCombiner.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class); 
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, inputPath);	
		FileOutputFormat.setOutputPath(job, outputPath);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {

		if (args.length <2){
			System.out.println("input and output missing!");
		} 

		//TODO hier muss dann eine Forschleife hin, die sich alle Bilder holt und für jedes Bild einen Outbut generieren
		int res = ToolRunner.run(new Configuration(), new AverageColorMr(), args);
		System.exit(res);
	} 
}
