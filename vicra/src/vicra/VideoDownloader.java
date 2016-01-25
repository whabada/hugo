package vicra;

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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
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
    	//InputStream is = new URL("http://commons.wikimedia.org/wiki/File:Anguilla-shoal-bay.ogg").openStream();
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
        }*/
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
}
