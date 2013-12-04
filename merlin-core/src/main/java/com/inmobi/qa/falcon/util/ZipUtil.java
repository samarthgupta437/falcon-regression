/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.inmobi.qa.falcon.util;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import lombok.Cleanup;

/**
 *
 * @author rishu.mehrotra
 * @author samarth.gupta
 */
public class ZipUtil {



	public static String unzipFileToString(String filePath) throws Exception
	{
		StringBuffer outputBuffer=new StringBuffer();

		@Cleanup InputStream in=new GZIPInputStream(new FileInputStream(filePath));

		byte[] buffer=new byte[2048];

		int noRead;

		while ((noRead = in.read(buffer)) != -1) {
			outputBuffer.append(new String(buffer, 0, noRead));

		}

		return outputBuffer.toString();
	}

	public static String unzipFileToAnotherFile(String inputFilePath,String destFileName) throws Exception
	{

		String parsedOutput=unzipFileToString(inputFilePath);


		File file = new File("/tmp/"+destFileName);
		@Cleanup FileWriter fileWriter =new FileWriter(file);
		@Cleanup BufferedWriter bf = new BufferedWriter(fileWriter);
		bf.write(parsedOutput);
		bf.flush();
		fileWriter.flush();
		return "/tmp/"+destFileName;

	}

	public static File unzipFileToAnotherFile(File inputFilePath) throws Exception
	{

		String parsedOutput=unzipFileToString(inputFilePath.getPath());

		String newFileName = inputFilePath.getName().substring(0, inputFilePath.getName().lastIndexOf(".")-1);

		File file = new File(newFileName);

		if(file.exists())
			file.delete();

		file.createNewFile();
		@Cleanup FileWriter fileWriter =new FileWriter(file);
		@Cleanup BufferedWriter bf = new BufferedWriter(fileWriter);
		bf.write(parsedOutput);
		bf.flush();
		fileWriter.flush();
		return file;

	}
	public static void zipStringToFile(String source,String filePath) throws Exception
	{

		File file=new File(filePath);
		if(file.exists())
		{
			throw new Exception("trying to write to an already existing file!");
		}
		file.createNewFile();
		@Cleanup OutputStream out=new GZIPOutputStream(new FileOutputStream(file));

		out.write(source.getBytes());

		out.flush();


	}


	public static void zipFile(String fileName) throws Exception
	{
		File file = new File(fileName);
		System.out.println(" you are going to gzip the  : " 
				+ file + "file");


		File gzFile = new File(file + ".gz");
		if(gzFile.exists())
			gzFile.delete();


		FileOutputStream fos = new FileOutputStream(gzFile);
		System.out.println(" Now the name of this gzip file is  : " 
				+ file + ".gz" );


		GZIPOutputStream gzos = new GZIPOutputStream(fos);
		System.out.println(" opening the input stream");
		FileInputStream fin = new FileInputStream(file);
		BufferedInputStream in = new BufferedInputStream(fin);
		System.out.println  ("Transferring file from" + fileName + " to "+ file + ".gz");
		byte[] buffer = new byte[1024];
		int i;
		while ((i = in.read(buffer)) >= 0){
			gzos.write(buffer,0,i);
		}
		System.out.println(" file is in now gzip format");
		in.close();

		File delete = new File(fileName);
		delete.delete();
		gzos.close();

	}

	public static int zipHDFSFile(String hadoopURL, Path file) throws Exception{
		// not implemented 
		
		Configuration conf = new Configuration();
		if(null!=hadoopURL){
			conf.set("fs.default.name","hdfs://"+hadoopURL+"");
		}
		FileSystem fileSystem = FileSystem.get(conf);

		FSDataOutputStream out = fileSystem.create(new Path(file.getName()+".gz"));
		
		//FSDataOutputStream output =  
		return 1;
	}
}  

