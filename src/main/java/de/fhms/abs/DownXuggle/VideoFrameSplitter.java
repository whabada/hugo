package de.fhms.abs.DownXuggle;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Timestamp;

import javax.imageio.ImageIO;
import javax.imageio.stream.ImageOutputStream;

import org.apache.avro.util.ByteBufferOutputStream;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;

import com.xuggle.mediatool.IMediaReader;
import com.xuggle.mediatool.MediaListenerAdapter;
import com.xuggle.mediatool.ToolFactory;
import com.xuggle.mediatool.event.IVideoPictureEvent;
import com.xuggle.xuggler.Global;


public class VideoFrameSplitter {

	public static final double SECONDS_BETWEEN_FRAMES = 5;
	/*TODO Anpassung an HDFS: Das Einlesen von einer File aus dem HDFS funktioniert nicht,
	 * Fehler in Line 60, Ursprung in Line 48, wenn man einen HDFS Link einsetzt.
	 */
	private static String inputFilename = "/home/cloudera/Videos/FILE.ogg"; 
	private static String outputFilePrefix;
	private static int counter; 
	private static String outputFilename;
	private static String fileNameForReturn;
	private static String outputFilePath;

	//Namen des Videos holen
	private static String videoFileName = VideoDownloader.getVideoFilename();

	// Ein Video Stream Index, den wir nutzen, dass wir auch tatsaechlich nur Frames eines bestimmten VideoStreams nehmen
	private static int mVideoStreamIndex = -1;

	// Time of last frame write
	private static long mLastPtsWrite = Global.NO_PTS;

	public static final long MICRO_SECONDS_BETWEEN_FRAMES = 
			(long)(Global.DEFAULT_PTS_PER_SECOND * SECONDS_BETWEEN_FRAMES);

	public static void split (String[] args) {
		
		//TimeStamp zur eindeutigkeit der Frames
		String getTimeStamp = new Timestamp(System.currentTimeMillis()).toString();
		//TimeStamp bereinigen
		getTimeStamp = getTimeStamp.replaceAll(":" , "-");
		getTimeStamp = getTimeStamp.replaceAll(" ", "-");
		getTimeStamp = getTimeStamp.replaceAll("\\.", "-"); 

		IMediaReader mediaReader = ToolFactory.makeReader(inputFilename);
		inputFilename = args[0];
		outputFilePrefix = args[1] + videoFileName + "-" + getTimeStamp + "/";

		//initialisiere counter
		counter =0;

		// erstellt BufferedImages mit BGR 24 Bit. 
		mediaReader.setBufferedImageTypeToGenerate(BufferedImage.TYPE_3BYTE_BGR);

		mediaReader.addListener(new ImageSnapListener());

		//Media File Inhalte auslesen und an den listener uebergeben
		while (mediaReader.readPacket() == null) ;

	}

	private static class ImageSnapListener extends MediaListenerAdapter {

		public void onVideoPicture(IVideoPictureEvent event) {

			if (event.getStreamIndex() != mVideoStreamIndex) {
				//Falls es noch keine ID gibt, wird diese gesetzt

				if (mVideoStreamIndex == -1)
					mVideoStreamIndex = event.getStreamIndex();
				else
					return;
			} 

			// falls uninitialisert, wird das mLastPtsWrite gesetzt
			if (mLastPtsWrite == Global.NO_PTS)
				mLastPtsWrite = event.getTimeStamp() - MICRO_SECONDS_BETWEEN_FRAMES; 

			// naechsten Frame schreiben, wenn MICRO_SECONDS_BETWEEN_FRAMES erreicht ist
			if (event.getTimeStamp() - mLastPtsWrite >= 
					MICRO_SECONDS_BETWEEN_FRAMES) {

				String outputFilename = imageToFile(event.getImage());

				/*	Fuers tracken
				double seconds = ((double) event.getTimeStamp()) / 
						Global.DEFAULT_PTS_PER_SECOND;
				System.out.printf(
						"at elapsed time of %6.3f seconds wrote: %s\n",
						seconds, outputFilename);  */

				// update mLastPtsWrite
				mLastPtsWrite += MICRO_SECONDS_BETWEEN_FRAMES;
			}
		}

		private String imageToFile(BufferedImage image) {
			if (image == null){
				System.out.println("Image is null");
				return "";
			}
			try { 
				outputFilename = outputFilePrefix + videoFileName + counter + ".png"; 
				outputFilePath = "hugo/";

				Configuration conf = new Configuration();
				conf.addResource(new Path("/etc/alternatives/hadoop-conf/core-site.xml"));
				conf.addResource(new Path("/etc/alternatives/hadoop-conf/hdfs-site.xml"));
				FileSystem fs = FileSystem.get(conf);

				ByteArrayOutputStream os = new ByteArrayOutputStream();
				ImageIO.write(image, "png", os);
				InputStream is = new ByteArrayInputStream(os.toByteArray());
				// Frames hinzugefügt

				Path outFile = new Path(outputFilePath + outputFilename);
				if (fs.exists(outFile)){
					fs.delete(outFile, true);
				}
				FSDataOutputStream out = fs.create(outFile);
				outputFilename = outputFilePrefix + counter + ".png"; 

				byte[] buffer = new byte[1024];
				int len1 = 0;
				while ((len1 = is.read(buffer)) > 0) {
					out.write(buffer,0,len1);
				}

				out.close(); 
				counter ++;
				return outputFilename;
			} 
			catch (IOException e) {
				e.printStackTrace();
				return null;
			}
		}
	}
	public static int getCounter(){
		return counter;
	}

	public static String getOutputfilename(){
		return outputFilePath+outputFilePrefix;

	}
}