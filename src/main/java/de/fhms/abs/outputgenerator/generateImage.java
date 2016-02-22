package de.fhms.abs.outputgenerator;
import java.awt.Image;
import java.awt.image.BufferedImage;
import java.awt.image.WritableRaster;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import javax.imageio.ImageIO;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class generateImage {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		int width = 400;
	    int height = 100;
	    int[] data = new int[width * height];
	    int i = 0;
	    for (int y = 0; y < height; y++) {
	      int red = (y * 255) / (height - 1);
	      for (int x = 0; x < width; x++) {
	        int green = (x * 255) / (width - 1);
	        int blue = 128;
	        data[i++] = (red << 16) | (green << 8) | blue;
	      }
	    }
	    
	    BufferedImage image = getImageFromArray(data, width, height, "test");
	}

	public static BufferedImage getImageFromArray(int[] pixels, int width, int height, String name) throws IOException {
        BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
        image.setRGB(0, 0, width, height, pixels, 0, 0);
        
        String outputFilename =  name +".png"; 
        String outputFilePath = "hugo/resultImages/";

		Configuration conf = new Configuration();
		conf.addResource(new Path("/etc/alternatives/hadoop-conf/core-site.xml"));
		conf.addResource(new Path("/etc/alternatives/hadoop-conf/hdfs-site.xml"));
		FileSystem fs = FileSystem.get(conf);

		ByteArrayOutputStream os = new ByteArrayOutputStream();
		ImageIO.write(image, "png", os);
		InputStream is = new ByteArrayInputStream(os.toByteArray());
		
		Path outFile = new Path(outputFilePath + outputFilename);
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
        //File outputfile = new File("/home/cloudera/Pictures/image.jpg");
	    //ImageIO.write(image, "jpg", outputfile);
        return image;
    }
}
