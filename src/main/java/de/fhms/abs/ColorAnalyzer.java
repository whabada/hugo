package de.fhms.abs;

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

	private final static IntWritable one = new IntWritable(1);
	private final static String URL = "UrlString";

	public static class ColorAnalyzerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		private Text rgb = new Text();
		private String urlStr;

		@Override
		protected void setup(Context context) {
			Configuration c = context.getConfiguration();
			urlStr = c.get("UrlString");
		} 

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

			//TODO das Image aus einer url lesen, die abgegeben wird

			BufferedImage image = null;
			FileSystem fs = FileSystem.get(context.getConfiguration());
			Path inputPath = new Path(urlStr);
			try {
				FSDataInputStream in = fs.open(inputPath);
				ImageInputStream imageInput = ImageIO.createImageInputStream(in);
				image = ImageIO.read(imageInput); 

			} catch (IOException e) {
			}

			if (image != null && image.getWidth() > 1 && image.getHeight() > 1) {
				// Get dimensions of image
				int w = image.getWidth();
				int h = image.getHeight();
				long sumPixel = w*h;
				long sumr = 0, sumg = 0, sumb = 0;
				for (int x = 0; x < w; x++) {
					for (int y = 0; y < h; y++) {
						Color pixel = new Color(image.getRGB(x, y));
						sumr = pixel.getRed(); //rot
						sumg = pixel.getGreen(); // gruen
						sumb = pixel.getBlue(); // blau
						//TODO ab hier weiter
						Text RKey =new Text("R"+blocks(sumr));
						Text GKey =new Text("G"+blocks(sumg));
						Text BKey =new Text("B"+blocks(sumb));
						Text pixelAmount = new Text("SumPixel,"+sumPixel);

						/*		String result = RKey.toString() + " " +  GKey.toString() + " " + BKey.toString() + " " + sumPixel;
						this.rgb.set(result);
						context.write(rgb, one); */
						context.write(RKey, one);
						context.write(GKey, one);
						context.write(BKey, one); 
						context.write(pixelAmount, one);
					}
				}
			}
			else { 
				System.out.println("kein Bild."); //Kein Bild geladen
			}
		}
	}

	public static class AverageColorReducer extends Reducer<Text, IntWritable, Text, Text> {
		private IntWritable sum = new IntWritable();

		/*	  @Override
          public void setup(Context context) throws IOException, InterruptedException {
                    mos = new MultipleOutputs<Text, Text>(context);
          } */
		/**
		 * 
		 * Hier wird gezaehlt, wie häufig ein R G B Code vom Mapper "gesehen" wurde
		 * Dieses Ergebnis wird aufsummiert und dann als Durchschnittswert ausgegeben 
		 */

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

			String[] splitKey = key.toString().split(",");
			String code = splitKey[0];
			

			long sumPixel =0;
	//		long sumPixel =Long.parseLong(splitKey[1]);;
			long sumr = 0;
			long sumg = 0;
			long sumb = 0;

			//TODO Hier ist ja was nicht richtig: Ich zaehle doch jeden Block, nicht jeden R, G oder B Block. 

			if (code.startsWith("pixelAmount")){
				sumPixel = Long.parseLong(splitKey[1]);
			}

			
			if (code.startsWith("R")){

				for (IntWritable val : values) {
					sumr += val.get();
				}
				sumr = sumr / sumPixel;
			}

			if (code.startsWith("G")){
				for (IntWritable val : values) {
					sumg += val.get();
				}
				sumg = sumg/sumPixel;

			}
			if (code.startsWith("B")){
				for (IntWritable val : values) {
					sumb += val.get();
				}
				sumb = sumb/sumPixel;
			}

			Text resultText = new Text(sumr + " " + sumg + " " + sumb);
			context.write(key, resultText);		
		}
	}

	public static String blocks(long k){
		String result = "";

		int grenzeOben =10;
		int grenzeUnten=0;
		for (int i=0; i<=255; i+=10){

			if(k==0)
				return result = String.valueOf(0);

			else if(k<grenzeOben && k>=grenzeUnten)
				return result = String.valueOf(i);

			grenzeOben+=10;
			grenzeUnten+=10;
		}

		return result;
	}
	

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Path inputPath = new Path (args[0]);
		Path outputPath = new Path(args[1]);
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

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, inputPath);	
		FileOutputFormat.setOutputPath(job, outputPath);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {

		if (args.length <2){
			System.out.println("input and output missing!");
		}
		int res = ToolRunner.run(new Configuration(), new ColorAnalyzer(), args);
		System.exit(res);
	} 
}
