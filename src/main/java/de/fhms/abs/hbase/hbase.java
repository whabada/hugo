package de.fhms.abs.hbase;

import de.fhms.abs.*;
import de.fhms.abs.DownXuggle.VideoDownloader;
import de.fhms.abs.outputgenerator.generateImage;

import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class hbase extends Configured implements Tool {

	/** The identifier for the application table. */
	public static final TableName VIDEO_DATA = TableName.valueOf("vmd");
	public static final TableName IMAGE_DATA = TableName.valueOf("imageData");
	/** The name of the column family used by the application. */
	public static final byte[] VIDEO_DATA_CF1 = Bytes.toBytes("metadata");
	public static final byte[] VIDEO_DATA_CF2 = Bytes.toBytes("group");
	public static final byte[] IMAGE_DATA_CF1 = Bytes.toBytes("avarageColor");
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
				generateImage.getImageFromArray(data, outputWidth, outputHeight);

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
				String videokey = "";
				if (argv.length < 1) {
					videokey = "https://upload.wikimedia.org/wikipedia/commons/4/4a/Anguilla-shoal-bay.ogg";
					videoBlockSize = "40";
				} else {
					videokey = argv[1];
					videoBlockSize = argv[2];
				}
				
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
			outputWidth = count;
			outputHeight = count/5;
			int[] data = new int[pixels];
			int i = 0;

			int red = 0;
			int green = 0;
			int blue = 0;

			int durchlaufe = 0;
			for(Result r:rs){
				int count2 = 0;
				for(KeyValue kv : r.raw()){

					if((new String(kv.getFamily())).equals("avarageColor")) {
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
			return data;
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
		
		//String[] data = new String[] {"getImagesOfVideo", "https"};
		String[] data = new String[] {"addVideoRecord", "https://upload.wikimedia.org/wikipedia/commons/4/4a/Anguilla-shoal-bay.ogg", "40"};
		//String[] data = new String[] {"getVideoRecord", "https://upload.wikimedia.org/wikipedia/commons/4/4a/Anguilla-shoal-bay.ogg", "40"};
		//String[] data = new String[] {"addImageRecord", "https://upload.wikimedia.org/wikipedia/commons/4/4a/Anguilla-shoal-bay.ogg", "19", "255;255;255", "0;0;0"};
		
		int ret = ToolRunner.run(new hbase(), data);
		System.exit(ret);
	}
}