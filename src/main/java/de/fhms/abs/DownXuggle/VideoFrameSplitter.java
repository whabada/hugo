package de.fhms.abs.DownXuggle;

import java.awt.image.BufferedImage;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

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

	// Ein Video Stream Index, den wir nutzen, dass wir auch tatsaechlich nur Frames eines bestimmten VideoStreams nehmen
	private static int mVideoStreamIndex = -1;

	// Time of last frame write
	private static long mLastPtsWrite = Global.NO_PTS;

	public static final long MICRO_SECONDS_BETWEEN_FRAMES = 
			(long)(Global.DEFAULT_PTS_PER_SECOND * SECONDS_BETWEEN_FRAMES);

	public static void split (String[] args) {

		IMediaReader mediaReader = ToolFactory.makeReader(inputFilename);
		inputFilename = args[0];
		outputFilePrefix = args[1];

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
			try { 

				outputFilename = outputFilePrefix + counter + ".jpg"; 
				
				Configuration conf = new Configuration();
				conf.addResource(new Path("/etc/alternatives/hadoop-conf/core-site.xml"));
				conf.addResource(new Path("/etc/alternatives/hadoop-conf/hdfs-site.xml"));
				FileSystem fs = FileSystem.get(conf);
				FSDataInputStream is = fs.open(new Path(inputFilename));

				Path outFile = new Path("hugo/" + outputFilename);
				if (fs.exists(outFile)){
					fs.delete(outFile, true);
				}
				FSDataOutputStream out = fs.create(outFile);

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
		return "hugo/"+outputFilePrefix;

	}
}