package de.fhms.abs;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Progressable;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.CookieStore;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.ClientContext;
import org.apache.http.client.utils.URIUtils;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;

public class VideoDownloader {

    public static void main(String[] args) throws IOException {
    	URL u = null;
    	if (args.length < 1) {
    		 u = new URL("https://upload.wikimedia.org/wikipedia/commons/4/4a/Anguilla-shoal-bay.ogg");
    	} else {
    		u = new URL(args[0]);
    	}
    	
    	System.out.println(u.getHost());
    	System.out.println(u.getPath());
    	String[] splitUrl =  u.getPath().split("/");
    	String fileName = splitUrl[splitUrl.length -1];
    	InputStream is = u.openStream();
        //String storagePath = Environment.getExternalStorageDirectory().toString();
        String storagePath = "/home/cloudera/Videos/";
        File f = new File(storagePath,fileName);
        FileOutputStream fos = new FileOutputStream(f);
        
        /*if(is != null) {
            while ((len1 = is.read(buffer)) > 0) {
                fos.write(buffer,0, len1);  
            }
        } */
        Configuration conf = new Configuration();
        conf.addResource(new Path("/etc/alternatives/hadoop-conf/core-site.xml"));
        conf.addResource(new Path("/etc/alternatives/hadoop-conf/hdfs-site.xml"));
        FileSystem fs = FileSystem.get(conf);

        Path outFile = new Path("vicra/" + fileName);
        FSDataOutputStream out = fs.create(outFile);
        
        byte[] buffer = new byte[1024];
        int len1 = 0;
        if(is != null) {
            while ((len1 = is.read(buffer)) > 0) {
                out.write(buffer,0, len1);  
            }
        }

        is.close();
        out.close();
        System.out.println("fertig");
    }
    
    public static String[] videoToHdfs(String url) throws IOException {
    	URL u = new URL(url);

    	String[] urlSplit = url.split("/");
    	String videoName = urlSplit[urlSplit.length-1];
    	//System.out.println(videoName);
    	//System.out.println(u.getHost());
    	//System.out.println(u.getPath());
    	InputStream is = u.openStream();
    	InputStream is2 = u.openStream();

        Configuration conf = new Configuration();
        conf.addResource(new Path("/etc/alternatives/hadoop-conf/core-site.xml"));
        conf.addResource(new Path("/etc/alternatives/hadoop-conf/hdfs-site.xml"));
        FileSystem fs = FileSystem.get(conf);
        System.out.println(fs.getHomeDirectory());
        
        Path p = new Path(fs.getHomeDirectory() + "/vicra");
        Path path = getLastModifiedHdfsFile(fs, p);
        FileSystem hdfs = path.getFileSystem(conf);
        ContentSummary cSummary = hdfs.getContentSummary(path);
        long hdfsFileLength = cSummary.getLength();
        
        byte[] buffer = new byte[1024];
        int len1 = 0;
        int videoSizeCount = 0;
        if(is != null) {
            while ((len1 = is.read(buffer)) > 0) {
                videoSizeCount = videoSizeCount + len1;
            }
        }
        is.close();
        System.out.println("video size: " + videoSizeCount);
        String offset = Long.toString(hdfsFileLength+1); 

        Path outFile = new Path("vicra/" + path.getName());
        fs.setReplication(outFile, (short) 1);
        FSDataOutputStream out = null;
        if(hdfsFileLength+videoSizeCount > 19328060) {
        	System.out.println("new File");
        	//Neue Datei
        	String fileName = path.getName();
        	String[] fileNameSplit = fileName.split("_");
        	String fileNamePrefix = fileNameSplit[0];
        	String fileNameCount = fileNameSplit[1].split("\\.")[0];
        	
        	fileNameCount = String.valueOf(Integer.parseInt(fileNameCount) + 1);
        	outFile = new Path("hugo/" + fileNamePrefix + "_" + fileNameCount + ".mp4");
        	offset = "0";
        	out = fs.create(outFile);
        } else {
        	System.out.println("append");
        	//Anfügen
        	out = fs.append(outFile);
        } 
        System.out.println("outfile: " + outFile.toString());
        
        len1 = 0;
        if(is2 != null) {
            while ((len1 = is2.read(buffer)) > 0) {
                out.write(buffer,0, len1);  
            }
        }
        
        is2.close();
        out.close();
        System.out.println("download finish");
        
        String name = videoName;
		String filepath = outFile.toString();
		String hyperlink = url;
		String videoSize = Integer.toString(videoSizeCount);
		
        String[] videoInformation = new String[5];
        videoInformation[0] = name;
        videoInformation[1] = filepath;
        videoInformation[2] = hyperlink;
        videoInformation[3] = videoSize;
        videoInformation[4] = offset;
        
		return videoInformation;
    }
    
    private static Path getLastModifiedHdfsFile(FileSystem fs, Path rootDir) {
        // TODO Auto-generated method stub
    	Path filePath = null;
    	long time = 0;
        try {

            FileStatus[] status = fs.listStatus(rootDir);
            for (FileStatus file : status) {
                if (file.isDir()) {
                    //System.out.println("DIRECTORY : " + file.getPath() + " - Last modification time : " + file.getModificationTime());
                	getLastModifiedHdfsFile(fs, file.getPath());
                } else {
                	if (file.getModificationTime() > time) {
                		time = file.getModificationTime();
                		filePath = file.getPath();
                	}
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        System.out.println("FILE : " + filePath + " - Last modification time : " + time);
        return filePath;  
    }
}
