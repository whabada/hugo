package de.fhms.abs.outputgenerator;
import java.awt.Image;
import java.awt.image.BufferedImage;
import java.awt.image.WritableRaster;
import java.io.File;
import java.io.IOException;

import javax.imageio.ImageIO;


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
	    
	    BufferedImage image = getImageFromArray(data, width, height);
	    
	}

	public static BufferedImage getImageFromArray(int[] pixels, int width, int height) throws IOException {
		System.out.println(width * height);
        BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
        //WritableRaster raster = (WritableRaster) image.getData();
        //raster.setPixels(0,0,width,height,pixels);
        image.setRGB(0, 0, width, height, pixels, 0, 8);
        File outputfile = new File("/home/cloudera/Pictures/image.jpg");
	    ImageIO.write(image, "jpg", outputfile);
        return image;
    }
}
