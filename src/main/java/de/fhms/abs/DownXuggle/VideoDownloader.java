package de.fhms.abs.DownXuggle;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class VideoDownloader {

	public static String download (String url) throws IOException {
		URL u = null;
		if (url.equals("")) {
			u = new URL("https://upload.wikimedia.org/wikipedia/commons/4/4a/Anguilla-shoal-bay.ogg");
		} else {
			u = new URL(url);
		}

		//	System.out.println(u.getHost());
		//  	System.out.println(u.getPath());
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

		Path outFile = new Path("hugo/" + fileName);
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
		System.out.println("done");
		return outFile.toString();
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

		Path p = new Path(fs.getHomeDirectory() + "/hugo/videos/");
		Path path = getLastModifiedHdfsFile(fs, p);
		Boolean initialVideoFile = false;
		int videoSizeCount = 0;
		long hdfsFileLength = 0;
		if (path == null) {
			System.out.println("initial File");
			initialVideoFile = true;
			path = new Path(fs.getHomeDirectory() + "/hugo/videos/video_0.mp4");
		} else {
			FileSystem hdfs = path.getFileSystem(conf);
			ContentSummary cSummary = hdfs.getContentSummary(path);
			hdfsFileLength = cSummary.getLength();

			byte[] buffer = new byte[1024];
			int len1 = 0;

			if(is != null) {
				while ((len1 = is.read(buffer)) > 0) {
					videoSizeCount = videoSizeCount + len1;
				}
			}
			is.close();
		}

		System.out.println("video size: " + videoSizeCount);
		String offset = Long.toString(hdfsFileLength+1); 

		Path outFile = new Path(fs.getHomeDirectory() + "/hugo/videos/" + path.getName());
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
			outFile = new Path(fs.getHomeDirectory() + "/hugo/videos/" + fileNamePrefix + "_" + fileNameCount + ".mp4");
			offset = "0";
			out = fs.create(outFile);
		} else if (initialVideoFile) {
			System.out.println("test");
			outFile = path;
			offset = "0";
			out = fs.create(outFile);
		} else {
			System.out.println("append");
			//Anfügen
			out = fs.append(outFile);
		} 
		System.out.println("outfile: " + outFile.toString());

		int len2 = 0;
		byte[] buffer2 = new byte[1024];
		if(is2 != null) {
			while ((len2 = is2.read(buffer2)) > 0) {
				out.write(buffer2,0, len2);  
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
			System.out.println("File not found");
			e.printStackTrace();
		}

		System.out.println("FILE : " + filePath + " - Last modification time : " + time);
		return filePath;  
	}
}
