package de.fhms.abs.hbase;

import de.fhms.abs.*;
import de.fhms.abs.DownXuggle.VideoDownloader;
import de.fhms.abs.outputgenerator.generateImage;

import java.net.URL;
import java.nio.ByteBuffer;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mortbay.log.Slf4jLog;

import ch.qos.logback.core.filter.Filter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import javax.imageio.ImageIO;

public class hbase extends Configured implements Tool {

	/** The identifier for the application table. */
	public static final TableName VIDEO_DATA = TableName.valueOf("vmd");
	public static final TableName IMAGE_DATA = TableName.valueOf("imageData");
	/** The name of the column family used by the application. */
	public static final byte[] VIDEO_DATA_CF1 = Bytes.toBytes("metadata");
	public static final byte[] VIDEO_DATA_CF2 = Bytes.toBytes("block");
	public static final byte[] IMAGE_DATA_CF1 = Bytes.toBytes("averageColor");
	public static final byte[] IMAGE_DATA_CF2 = Bytes.toBytes("dominantColor");
	public static Connection con = null;

	public static String videoBlockSize = "";
	public static Boolean blockSizeAnalyzed = false;

	public static int outputHeight = 0;
	public static int outputWidth = 0;

	public int run(String[] argv) throws Exception {
		setConf(HBaseConfiguration.create(getConf()));
		Table table = null;
		try {
			// establish the connection to the cluster.
			con = ConnectionFactory.createConnection(getConf());
			if (argv[0].equals("getVideoRecord")) {
				//#################################################################################################

				//Input
				//- videoKey
				//- BlockSize
				String videokey = "";
				//videoBlockSize = "";
				if (argv.length < 1) {
					videokey = "https://upload.wikimedia.org/wikipedia/commons/4/4a/Anguilla-shoal-bay.ogg";
					videoBlockSize = "40";
				} else {
					videokey = argv[1];
					videoBlockSize = argv[2];
				}

				if (getVideoRecord(VIDEO_DATA, videokey)) {
					System.out.println("Video in HBase vorhanden");
					//Wurde die Blocksize bereits analysiert
					if (blockSizeAnalyzed) {
						//Analyse Daten sind für dieses Video und diese Blockgröße bereits vorhanden
						System.out.println("Analyse Daten ausgeben");
						/*
						 * ##############################
						 * Analyse ausgeben
						 */
					} else {
						//Start analyse 
						System.out.println("Analyse für Blocksize " + videoBlockSize + " starten");
						/*
						 * ################################
						 * MapReduce Job von Ben anstoßen
						 */
					}
				} else {
					System.out.println("Video noch nicht vorhanden -> Download");
					String[] data = new String[] {"addVideoRecord", argv[1], argv[2]};
					writeDataFile(data);
				}


			} else if (argv[0].equals("getImagesOfVideo")) {
				//#################################################################################################
				String videokey = "";
				if (argv.length < 1) {
					videokey = "https://upload.wikimedia.org/wikipedia/commons/4/4a/Anguilla-shoal-bay.ogg";
				} else {
					videokey = argv[1];
				}
				int[] data = getImagesOfVideo(IMAGE_DATA, videokey);
				generateImage.getImageFromArray(data, outputWidth, outputHeight, argv[2]);

			} else if (argv[0].equals("addImageRecord")) {
				//#################################################################################################
				String videokey = "";
				int imageCount = 0;
				String avarageResult = "0;0;0";
				String dominantResult = "0;0;0";
				if (argv.length < 1) {
					videokey = "https://upload.wikimedia.org/wikipedia/commons/4/4a/Anguilla-shoal-bay.ogg";
					imageCount = 100;
				} else {
					videokey = argv[1];
					imageCount = Integer.parseInt(argv[2]);
					avarageResult = argv[3];
					dominantResult = argv[4];
				}
				addImageRecord(videokey + "_" + imageCount(imageCount), avarageResult, dominantResult);

			} else if (argv[0].equals("addVideoRecord")) {
				//#################################################################################################
				System.out.println("addVideoRecord");
				String videokey = "";
				if (argv.length < 1) {
					videokey = "https://upload.wikimedia.org/wikipedia/commons/4/4a/Anguilla-shoal-bay.ogg";
					videoBlockSize = "40";
				} else {
					videokey = argv[1];
					videoBlockSize = argv[2];
				}
				System.out.println("Vars wurden gesetzt");
				System.out.println(videokey);
				String[] videoInformation = VideoDownloader.videoToHdfs(videokey);
				String name = videoInformation[0];
				String filepath = videoInformation[1];
				String hyperlink = videoInformation[2];
				String videoSize = videoInformation[3];
				String offset = videoInformation[4];

				System.out.println(videoInformation[0]);
				System.out.println(videoInformation[1]);
				System.out.println(videoInformation[2]);
				System.out.println(videoInformation[3]);
				System.out.println(videoInformation[4]);

				addVideoRecord(VIDEO_DATA, hyperlink, name, hyperlink, filepath, offset, videoBlockSize, videoSize);

				//extractVideo(videoInformation);
				String[] data = new String[] {videoInformation[2], videoInformation[0], argv[2]};
				writeDataFile(data);
			}

			//int[] data = getImagesOfVideo(IMAGE_DATA, "image_");
			//generateImage.getImageFromArray(data, 100, 20);
			/*Generate Testimages
			Integer r = 0;
			for (int i = 0; i < 100; i++) {

				String avarageResult = "";
				r = r + 20;
				if (r > 255) {
					r = 0;
				}

				avarageResult = r + ";0;0";
				String dominantResult = "0;0;0";
				addImageRecord("image_" + i, avarageResult, dominantResult);
			}
			 */
		} finally {
			// close everything down
			if (table != null) table.close();
			if (con != null) con.close();
		}
		return 0;
	}

	public static void addVideoRecord(TableName tableName, String rowKey,
			String name, String hyperlink, String path, String offset, String blocksize, String filesize) throws Exception {
		Table table = con.getTable(tableName);
		Put p = new Put(Bytes.toBytes(rowKey));
		p.addColumn(VIDEO_DATA_CF1, Bytes.toBytes("name"), Bytes.toBytes(name));
		p.addColumn(VIDEO_DATA_CF1, Bytes.toBytes("hyperlink"), Bytes.toBytes(hyperlink));
		p.addColumn(VIDEO_DATA_CF1, Bytes.toBytes("path"), Bytes.toBytes(path));
		p.addColumn(VIDEO_DATA_CF1, Bytes.toBytes("offset"), Bytes.toBytes(offset));
		p.addColumn(VIDEO_DATA_CF1, Bytes.toBytes("filesize"), Bytes.toBytes(filesize));
		p.addColumn(VIDEO_DATA_CF2, Bytes.toBytes("size"), Bytes.toBytes(blocksize));

		table.put(p);
	}

	public static void addImageRecord(String rowKey, String avarageResult, String dominantResult) throws Exception {
		Table table = con.getTable(IMAGE_DATA);
		Put p = new Put(Bytes.toBytes(rowKey));
		String[] avarageColor = avarageResult.split(";");
		String[] dominantColor = avarageResult.split(";");

		p.addColumn(IMAGE_DATA_CF1, Bytes.toBytes("R"), Bytes.toBytes(avarageColor[0]));
		p.addColumn(IMAGE_DATA_CF1, Bytes.toBytes("G"), Bytes.toBytes(avarageColor[1]));
		p.addColumn(IMAGE_DATA_CF1, Bytes.toBytes("B"), Bytes.toBytes(avarageColor[2]));
		p.addColumn(IMAGE_DATA_CF2, Bytes.toBytes("R"), Bytes.toBytes(dominantColor[0]));
		p.addColumn(IMAGE_DATA_CF2, Bytes.toBytes("G"), Bytes.toBytes(dominantColor[1]));
		p.addColumn(IMAGE_DATA_CF2, Bytes.toBytes("B"), Bytes.toBytes(dominantColor[2]));

		table.put(p);
	}

	public static boolean getVideoRecord (TableName tableName, String rowKey) throws IOException{
		Table table = con.getTable(tableName);
		Get get = new Get(rowKey.getBytes()).setMaxVersions();
		Result rs = table.get(get);
		for(KeyValue kv : rs.raw()){
			if (new String(kv.getQualifier()).equals("size")) {
				if (new String(kv.getValue()).equals(videoBlockSize)) {
					blockSizeAnalyzed = true;
				}
			}
			/*
			System.out.print(new String(kv.getRow()) + " " );
			System.out.print(new String(kv.getFamily()) + ":" );
			System.out.print(new String(kv.getQualifier()) + " " );
			System.out.print(kv.getTimestamp() + " " );
			System.out.println(new String(kv.getValue()));*/
		}

		if (rs.isEmpty()) {
			return false;
		} else {
			return true;
		}

	}

	public static int[] getImagesOfVideo (TableName tableName, String key) {
		try{
			System.out.println("get Images of Video");
			Table table = con.getTable(tableName);
			Scan s = new Scan();
			PrefixFilter filter = new PrefixFilter(Bytes.toBytes(key));
			s.setFilter(filter);
			ResultScanner ss = table.getScanner(s);
			ResultScanner rs = table.getScanner(s);

			Integer count = 0;
			for(Result r:ss){
				count++;
			}

			System.out.println(count);
			int pixels = count * (count/5);
			System.out.println("Anzahl der Pixel im Array" + pixels);
			outputWidth = count;
			outputHeight = count/4;
			int[] data = new int[pixels];
			int i = 0;

			int red = 0;
			int green = 0;
			int blue = 0;

			int durchlaufe = 0;
			for(Result r:rs){
				int count2 = 0;
				for(KeyValue kv : r.raw()){

					if((new String(kv.getFamily())).equals("averageColor")) {
						count2++;
						if (new String(kv.getQualifier()).equals("R")) {
							red = new Integer(new String(kv.getValue()));
						} else if (new String(kv.getQualifier()).equals("G")) {
							green = new Integer(new String(kv.getValue()));
						} else if (new String(kv.getQualifier()).equals("B")) {
							blue = new Integer(new String(kv.getValue()));
						}
						if (count2 == 3) {
							for (int j = 0; j < pixels; j = j+outputWidth) {
								durchlaufe++;

								data[i+j] = (red << 16) | (green << 8) | blue;
								System.out.println(data[i+j]);
							}
							i++;
							count2 = 0;
						}
					}
				}
			}
			
			System.out.println("hbaseGenerateImage: Array Size: " + data.length);
			System.out.println("hbaseGenerateImage: Width: " + outputWidth);
			System.out.println("hbaseGenerateImage: Height: " + outputHeight);
			System.out.println("hbaseGenerateImage: Array Value 0: " + data[0]);
			
			
			int x = 200;
			if (data.length < 10) {
				outputHeight = outputHeight * x;
				outputWidth = outputWidth * x;
				int[] data2 = new int[outputHeight * outputWidth];
						
				for (int n = 0; n < data.length; n++) {
					for (int c = 0; c < x; c++) {
						for (int r = 0; r < data2.length; r  = r + outputWidth) {
							data2[n*x+c+r] = data[n];
						}
					}
					
				}
				/*for (int n = 0; n < data.length; n++) {
					for (int m = 0; m < x; m++) {
						data2[n * x + m] = data[n];
					}
				}
				*/
				return data2;
			} else {
				return data;
			}
			//return data;
		} catch (IOException e){
			e.printStackTrace();
		}
		return null;
	}

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
	public static void main(String[] argv) throws Exception {
		System.setProperty("hadoop.home.dir", "/usr/lib/hadoop");
		System.setProperty("$HBASE_HOME", "/usr/lib/hbase");
		System.setProperty("java.class.path", "/usr/lib/hbase/client/*:/home/cloudera/.m2/*:.");
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
		//String[] array = new String[] {"getImagesOfVideo", "https"};
		//String[] array = new String[] {"addVideoRecord", "https://upload.wikimedia.org/wikipedia/commons/4/4a/Anguilla-shoal-bay.ogg", "40"};
		//String[] data = new String[] {"getVideoRecord", "https://upload.wikimedia.org/wikipedia/commons/4/4a/Anguilla-shoal-bay.ogg", "40"};
		//String[] data = new String[] {"addImageRecord", "https://upload.wikimedia.org/wikipedia/commons/4/4a/Anguilla-shoal-bay.ogg", "19", "255;255;255", "0;0;0"};

		int ret = ToolRunner.run(new hbase(), array);
		System.exit(ret);
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

	public static void extractVideo (String[] data) throws IOException {
		String name = data[0];
		String filepath = data[1];
		String hyperlink = data[2];
		String videoSize = data[3];
		String offset = data[4];

		System.out.println("name " + data[0]);
		System.out.println("filepath" + data[1]);
		System.out.println("hyperlink" + data[2]);
		System.out.println("videosize" + data[3]);
		System.out.println("offset" + data[4]);
		Configuration conf = new Configuration();
		conf.addResource(new Path("/etc/alternatives/hadoop-conf/core-site.xml"));
		conf.addResource(new Path("/etc/alternatives/hadoop-conf/hdfs-site.xml"));
		FileSystem fs = FileSystem.get(conf);

		Path outFile = new Path(fs.getHomeDirectory() + "/hugo/tmp_video/" + name);
		FSDataOutputStream out = null;
		out = fs.create(outFile);

		System.out.println(filepath);
		Path path = new Path(filepath);
		FSDataInputStream is = fs.open(path);
		
		int len = 0;
		byte[] buffer = new byte[1024];
		is.read(new byte[Integer.valueOf(offset) - 1]);
		int count = 0;
		System.out.println("Tmp outfile: " + outFile.toString());
		if(is != null) {
			Boolean read = true;
			while (read) {
				if (count + 1024 > Integer.valueOf(videoSize)) {
					byte[] buffer2 = new byte[Integer.valueOf(videoSize) - count];
					len = is.read(buffer2);
					out.write(buffer2, 0, len);
					read = false;
					System.out.println(len);
				} else {
					len = is.read(buffer);
					count = count + len;
					System.out.println(len);
					out.write(buffer,0, len);
					
				}
			}
			System.out.println("extract Video: Daten wurden geschrieben");
		}

		is.close();
		out.flush();
		out.close();
	}
}