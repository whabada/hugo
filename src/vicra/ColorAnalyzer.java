package vicra;

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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class ColorAnalyzer extends Configured implements Tool {

	private final static IntWritable one = new IntWritable(1);

	public static class ColorAnalyzerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		private Text rgb = new Text();

		/**
		 * Die Mapper Funktion erhaelt ein Bild und analysiert die Farbe saemtlicher Pixel des erhaltenen Images. 
		 * Die Funktion funktioniert nur, wenn das Bild nicht null ist und groeßer als 1x1 ist.
		 * Für jede Farbe R, G, B wird ausgebenen "RGB Farbcode, 1". 
		 * @param key Nummer des Frames 
		 * @param BufferedImage image
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		public void map(LongWritable key, Text url, Context context) throws IOException, InterruptedException {
			System.out.println("map");

			BufferedImage image = null;
		//	FileSystem fs = FileSystem.get(context.getConfiguration());
		//	Path inputPath = new Path(url.toString());
			try {
				 image = ImageIO.read(new File("/home/cloudera/Pictures/sample_01.jpg"));
				// image = ImageIO.read(new File(url.toString()));
				//FSDataInputStream in = fs.open(inputPath);
				//ImageInputStream imageInput = ImageIO.createImageInputStream(in);
				//image = ImageIO.read(imageInput);

			} catch (IOException e) {
			}


			if (image != null && image.getWidth() > 1 && image.getHeight() > 1) {
				// Get dimensions of image
				int w = image.getWidth();
				int h = image.getHeight();
				long sumPixel = w*h;
				String result ="";
				long sumr = 0, sumg = 0, sumb = 0;
				for (int x = 0; x < w; x++) {
					for (int y = 0; y < h; y++) {
						Color pixel = new Color(image.getRGB(x, y));
						sumr = pixel.getRed(); //rot
						sumg = pixel.getGreen(); // gruen
						sumb = pixel.getBlue(); // blau

						result = sumr + " " +  sumg + " " + sumb;// +" " + sumPixel;
						this.rgb.set(result);
						context.write(rgb, one);
					}
				}
				//		sumr = sumr / sumPixel;
				//		sumg = sumg / sumPixel;
				//		sumb = sumb / sumPixel;

				//TODO Gibt jetzt fuer alle Pixel einen Wert raus
				//TODO Eingabe in 10er Rangen der Werte

			}
			else { 
				System.out.println("kein Bild."); //Kein Bild geladen
			}
		}
	}

	public static class AverageColorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable sum = new IntWritable();
		/**
		 * 
		 * Hier wird gezaehlt, wie häufig ein R G B Code vom Mapper "gesehen" wurde
		 * Dieses Ergebnis wird aufsummiert und dann als Durchschnittswert ausgegeben 
		 */
		@Override
		protected void reduce(Text rgb, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			/**
			long sumPixel =0;
			long sumr = 0;
			long sumg = 0;
			long sumb = 0;
			 */

			int total = 0;
			for (IntWritable val : values) {
				total += val.get();
			}
			sum.set(total);
			context.write(rgb, sum);

			/*	for (IntWritable val : values){
//TODO Das funktioniert so nicht. Der RGB Wert ist der Key, die Anzahl, wie haeufig ich diesen sehe,
			 * ist der value. Ich muss die RGB Werte gruppieren GROUPBY, und dann zaehlen - 
			 * Ich muss den Schluessel auseinander nehmen, nicht den Wert
				// 
				// im Text sind die Werte wie folgt angeordnet: sumr + " " +  sumg + " " + sumb +" " + sumPixel;

				String [] result = val.toString().split("\t"); //RGB Werte durch Split wieder auseinander gezogen
				// RGBwerte werden durch die Anzahl der Pixel geteilt und als Text ausgegeben
				sumPixel = Long.parseLong(result[3]);
				sumr += Long.parseLong(result[0]) / sumPixel;
				sumg += Long.parseLong(result[1]) / sumPixel;
				sumb += Long.parseLong(result[2]) / sumPixel;			
			}

			long [] resultArray = new long [3];
			resultArray[0]= sumr;
			resultArray[1]= sumg;
			resultArray[2]= sumb;
			String stringResult = resultArray [0]+ " " + resultArray [1] + resultArray [2];
			Text result = new Text(stringResult);
			context.write(key, result); */
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);

		conf.addResource(new Path("/etc/alternatives/hadoop-conf/core-site.xml"));
		conf.addResource(new Path("/etc/alternatives/hadoop-conf/hdfs-site.xml"));

		/*
		Path inputPath = new Path ("/home/cloudera/Pictures/sample_01.jpg");
		Path outputPath = new Path("picOut1/");
		Path outputPath2 = new Path("picOut2/");
		 */
		Path inputPath = new Path (args[0]);
		Path outputPath = new Path(args[1]);

		if (fs.exists(outputPath)) { 
			fs.delete(outputPath, true);
		}

		/**
		 * 
		FSDataInputStream in = fs.open(inputPath);
		FSDataOutputStream out = fs.create(outputPath);
		ImageInputStream imageInput = ImageIO.createImageInputStream(in);
		BufferedImage image = ImageIO.read(imageInput);
		 */


		Job job = Job.getInstance();
		job.setJarByClass(ColorAnalyzer.class);

		job.setMapperClass(ColorAnalyzerMapper.class);
		job.setReducerClass(AverageColorReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, inputPath);	
		FileOutputFormat.setOutputPath(job, outputPath);

		System.out.println("run finish");
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {

		if (args.length <2){
			System.out.println("input and output missing!");
		}
		int res = ToolRunner.run(new Configuration(),
				new ColorAnalyzer(), args);
		System.exit(res);
	}


}
