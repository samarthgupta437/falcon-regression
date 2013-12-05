package com.inmobi.qa.falcon.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.security.PrivilegedExceptionAction;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import com.inmobi.qa.falcon.bundle.Bundle;
import com.inmobi.qa.falcon.generated.feed.*;
import com.inmobi.qa.falcon.generated.feed.Location;
import com.inmobi.qa.falcon.generated.process.Process;
import com.inmobi.qa.falcon.generated.process.Property;
import com.inmobi.qa.falcon.interfaces.IEntityManagerHelper;
import com.inmobi.qa.falcon.supportClasses.Consumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.XOozieClient;

import org.custommonkey.xmlunit.Diff;
import org.custommonkey.xmlunit.XMLUnit;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.Assert;
import org.testng.TestNGException;
import org.testng.log4testng.Logger;
import org.xml.sax.InputSource;

import com.inmobi.qa.falcon.generated.cluster.Interface;
import com.inmobi.qa.falcon.generated.cluster.Interfacetype;
import com.inmobi.qa.falcon.generated.coordinator.COORDINATORAPP;
import com.inmobi.qa.falcon.generated.dependencies.Frequency;
import com.inmobi.qa.falcon.generated.feed.Feed;
import com.inmobi.qa.falcon.generated.feed.Partition;
import com.inmobi.qa.falcon.generated.process.Input;
import com.inmobi.qa.falcon.generated.process.Output;
import com.inmobi.qa.falcon.helpers.ColoHelper;
import com.inmobi.qa.falcon.helpers.PrismHelper;
import com.inmobi.qa.falcon.response.APIResult;
import com.inmobi.qa.falcon.response.ProcessInstancesResult;
import com.inmobi.qa.falcon.response.ServiceResponse;
import com.inmobi.qa.falcon.supportClasses.ENTITY_TYPE;
import com.inmobi.qa.falcon.supportClasses.GetBundle;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.UserInfo;
/**
 *
 * @author rishu.mehrotra
 * @author samarth.gupta
 */
public class Util {


	static Logger logger=Logger.getLogger(Util.class);

	//This section is for properties that are read from the config.properties file and then used throughout
  final static Properties properties = getPropertiesObj
    ("src/main/resources/"+System.getProperty("environment"));
  final static String HOST_NAME = properties.getProperty("qa_host");
	final static String USER_NAME=properties.getProperty("username");
	final static String PASSWORD=properties.getProperty("password");
	final static String HADOOP_LOCATION=properties.getProperty("hadoop_location");
	final static String HADOOP_URL=properties.getProperty("hadoop_url");
	final static String OOZIE_URL=properties.getProperty("oozie_url");
	final static String OOZIE_LOCATION=properties.getProperty("oozie_location");
	final static String HOSTNAME=properties.getProperty("ivory_hostname");
	final static String ACTIVEMQ=properties.getProperty("activemq_url");
  final static String IDENTITY_FILE = properties.getProperty("identityFile",
    System.getProperty("user.home") +"/.ssh/id_rsa");
	//config file property section ends here



	//final static String HOST_NAME="10.14.110.46";
	//final static String HOST_NAME="10.14.111.146";
	//final static String USER_NAME="rishu";
	//final static String PASSWORD="Rishu@123";
	final static String PROCESS_STORE_COMMAND="ls /var/lib/tomcat6/target/store/PROCESS";
	final static String ARCHIVE_STORE_COMMAND="ls /var/lib/tomcat6/target/store/archive/PROCESS";

	//final static String PROCESS_STORE_COMMAND="ls /var/lib/jenkins/jobs/Ivory/workspace/target/store/PROCESS";
	//final static String ARCHIVE_STORE_COMMAND="ls /var/lib/jenkins/jobs/Ivory/workspace/target/store/archive/PROCESS";
	//final static String DATA_STORE_COMMAND="ls /var/lib/jenkins/jobs/Ivory/workspace/target/store/DATASET";
	//final static String DATA_ARCHIVE_STORE_COMMAND="ls /var/lib/jenkins/jobs/Ivory/workspace/target/store/archive/DATASET";

	final static String DATA_STORE_COMMAND="ls /var/lib/tomcat6/target/store/FEED";
	final static String DATA_ARCHIVE_STORE_COMMAND="ls /var/lib/tomcat6/target/store/archive/FEED";
	final static String CLUSTER_STORE_COMMAND="ls /var/lib/tomcat6/target/store/CLUSTER";
	final static String CLUSTER_ARCHIVE_STORE_COMMAND="ls /var/lib/tomcat6/target/store/archive/CLUSTER";

	//final static String CLUSTER_STORE_COMMAND="/var/lib/jenkins/jobs/Ivory/workspace/target/store/CLUSTER";
	//final static String CLUSTER_ARCHIVE_STORE_COMMAND="/var/lib/jenkins/jobs/Ivory/workspace/target/store/archive/CLUSTER";
	//final static String HADOOP_GET_COMMAND=HADOOP_LOCATION+"  fs -cat hdfs://"+HADOOP_URL+"/projects/ivory/workflow/";
	final static String HADOOP_GET_COMMAND=HADOOP_LOCATION+"  fs -cat hdfs://"+HADOOP_URL+"/projects/ivory/staging/ivory/workflows/process";

	//final static String HOSTNAME="http://10.14.111.93:15000/ivory-webapp-0.1-SNAPSHOT";
	public static ServiceResponse sendRequest(String url) throws Exception
	{
		HttpClient client=new DefaultHttpClient();

		HttpRequestBase request=null;

		if((Thread.currentThread().getStackTrace()[2].getMethodName().contains("delete")))
		{
			request=new HttpDelete();
		}
		else if((Thread.currentThread().getStackTrace()[2].getMethodName().contains("suspend"))||(Thread.currentThread().getStackTrace()[2].getMethodName().contains("resume")) || (Thread.currentThread().getStackTrace()[2].getMethodName().contains("schedule")) )
		{
			request=new HttpPost();
		}
		else
		{
			request=new HttpGet();
		}


		request.setHeader("Remote-User",System.getenv("USER"));

		logger.info("hitting the url: "+url);

		request.setURI(new URI(url));

		HttpResponse response=client.execute(request);
		//logger.info("http response: "+ response);

		BufferedReader reader=new BufferedReader(new InputStreamReader(response.getEntity().getContent()));

		String line=new String();
		StringBuffer string_response=new StringBuffer();

		while((line=reader.readLine())!=null)
		{
			string_response.append(line);
		}

		logger.info("The web service response status is "+response.getStatusLine().getStatusCode());
		System.out.println("The web service response status is "+response.getStatusLine().getStatusCode());
		logger.info("The web service response is: "+string_response.toString()+"\n");
		System.out.println("The web service response is: "+string_response.toString()+"\n");
		//return string_response.toString();
		return new ServiceResponse(string_response.toString(),response.getStatusLine().getStatusCode());
	}



	public static ServiceResponse sendRequest(String url,String data) throws Exception
	{


		HttpClient client=new DefaultHttpClient();

		HttpPost post=new HttpPost(url);

		post.setHeader("Content-Type","text/xml");
		post.setHeader("Remote-User",System.getenv("USER"));
		post.setEntity(new StringEntity(data));

		//		logger.info("hitting the URL: "+url);
		//
		//		HttpResponse response=client.execute(post);
		//		logger.info("The web service response status is "+response.getStatusLine().getStatusCode());

		System.out.println("hitting the URL: "+url);

		long start_time=System.currentTimeMillis();
		HttpResponse response=client.execute(post);
		System.out.println("The web service response status is "+response.getStatusLine().getStatusCode());
		System.out.println("time taken:"+(System.currentTimeMillis()-start_time));
		System.out.println("time taken:"+(System.currentTimeMillis()-start_time));


		BufferedReader reader=new BufferedReader(new InputStreamReader(response.getEntity().getContent()));

		String line=new String();

		String string_response=new String();

		while((line=reader.readLine())!=null)
		{
			string_response=string_response+line;
		}

		System.out.println("The web service response is "+string_response+"\n");


		return new ServiceResponse(string_response,response.getStatusLine().getStatusCode());
	}

	public static String getExpectedErrorMessage(String filename) throws Exception
	{
		/*for(ErrorMapping error: ErrorMapping.values())
		{
			if(error.getFilename().equalsIgnoreCase(filename))
			{
				return error.getErrorMessage();
			}*/

		Properties properties = new Properties();

		properties.load(new FileInputStream("src/main/resources/errorMapping.properties"));


		return properties.getProperty(filename);
	}


	public static File[] getFiles(String directoryPath) throws Exception
	{
		System.out.println("directoryPath: "+directoryPath);

		File dir=new File(directoryPath);

		return dir.listFiles();
	}





	public static Object[][] getDataFromFolder(String folderPath,String methodName) throws Exception
	{

		File[] files=Util.getFiles(folderPath);

		List<Object[]> testList=new ArrayList<Object[]>();
		Object[][] testData=new Object[files.length][3];


		for(int i=0;i<files.length;i++)
		{

			if(files[i].getName().contains("svn") || files[i].getName().contains(".DS") || files[i].getName()==null) {continue;}

			testData[i][0]= fileToString(files[i]);

			if(methodName.contains("Invalid") || (methodName.contains("InValid")))
			{
				testData[i][1]=APIResult.Status.FAILED;
			}
			else
			{
				testData[i][1]=APIResult.Status.SUCCEEDED;
			}  

			testData[i][2]=files[i].getName();
			testList.add(testData[i]);
		}
		return testList.toArray(new Object[testList.size()][3]);

	}

	public static Object[][] getDataFromFolder(String folderPath,String methodName, int numberOfFiles) throws Exception
	{



		File[] files=Util.getFiles(folderPath);

		List<Object[]> testList=new ArrayList<Object[]>();
		Object[][] testData=new Object[files.length][3];


		for(int i=0;i<numberOfFiles;i++)
		{

			if(files[i].getName().contains("svn") || files[i].getName().contains(".DS") || files[i].getName()==null) {
				numberOfFiles++;
				continue;}


			testData[i][0]= fileToString(files[i]);

			if(methodName.contains("Invalid") || (methodName.contains("inValid")))
			{
				testData[i][1]=APIResult.Status.FAILED;
			}
			else
			{
				testData[i][1]=APIResult.Status.SUCCEEDED;
			}  

			testData[i][2]=files[i].getName();

			testList.add(testData[i]);
		}

		return testList.toArray(new Object[testList.size()][3]);


	}


	public static String fileToString(File file) throws Exception
	{

		StringBuffer fileData = new StringBuffer(1000);
		char[] buf = new char[1024];
		BufferedReader reader = new BufferedReader(
				new FileReader(file));

		int numRead=0;
		while((numRead=reader.read(buf)) != -1){
			String readData = String.valueOf(buf, 0, numRead);
			fileData.append(readData);
			buf = new char[1024];
		}
		reader.close();
		return fileData.toString();
	}

	public static String getTestDataFolderPath(String methodName)
	{
		StringBuilder filePath=new StringBuilder("src/test/resources");


		//pathBuilder
		if(methodName.contains("Data"))
		{
			filePath.append("/data");
		}
		else if(methodName.contains("Process"))
		{
			filePath.append("/process");
		}

		if(methodName.contains("submit"))
		{
			filePath.append("/submit");
		}
		else if(methodName.contains("get"))
		{
			filePath.append("/get");
		}
		else if(methodName.contains("Special"))
		{
			filePath.append("/special");
		}
		else if(methodName.contains("delete"))
		{
			filePath.append("/delete");
		}
		else
		{
			filePath.append("/validate");
		}

		if(methodName.contains("Invalid") || methodName.contains("InValid"))
		{
			filePath.append("/invalid");
		}
		else
		{
			filePath.append("/valid");
		}

		return filePath.toString();
	}



	public static String getProcessName(String data) throws Exception
	{
		JAXBContext jc=JAXBContext.newInstance(com.inmobi.qa.falcon.generated.process.Process.class);

		Unmarshaller u=jc.createUnmarshaller();

		Process processElement=(Process)u.unmarshal((new StringReader(data)));

		return processElement.getName();
	}

	public static String getProcessName(File file) throws Exception
	{
		return getProcessName(fileToString(file));
	}


	public static String getCurrentMethodName() {

		return new Exception().getStackTrace()[1].getMethodName();
	}

	private static boolean isXML(String data) throws Exception
	{
		final String XML_PATTERN_STR = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><(\\S+?)(.*?)>(.*?)</\\1>";

		if (data != null && data.trim().length() > 0) {

			if (data.trim().startsWith("<")) {

				return true; //find a better way of validation
			}    

		}

		return false;

	}


	public static APIResult parseResponse(ServiceResponse response) throws Exception
	{


		if(!isXML(response.getMessage()))
		{

			// -- DEPRECATED CODE BEGINS
			//should be json. Lets just convert and send back the object
			//                JsonParser parser=new JsonParser();
			//                JsonElement element=parser.parse(response);
			//                return new Gson().fromJson(element,APIResult.class);
			// -- DEPRECATED CODE ENDS

			//this implies its largely an error :| all our successful responses return us a 200 and an xml :) 
			return new APIResult(APIResult.Status.FAILED,response.getMessage(),"somerandomstring",response.getCode());

		}


		JAXBContext jc=JAXBContext.newInstance(APIResult.class); 

		Unmarshaller u=jc.createUnmarshaller();
		// logger.info("response in parseResponse: "+(APIResult)u.unmarshal(new InputSource(new StringReader(response))));

		APIResult temp=null;
		if(response.getMessage().contains("requestId"))
		{
			temp=(APIResult)u.unmarshal(new InputSource(new StringReader(response.getMessage())));
			temp.setStatusCode(response.getCode());
		}
		else
		{
			temp=new APIResult();
			temp.setStatusCode(response.getCode());
			temp.setMessage(response.getMessage());
			temp.setRequestId("");
			if(response.getCode()==200)
			{
				temp.setStatus(APIResult.Status.SUCCEEDED);
			}
			else
			{
				temp.setStatus(APIResult.Status.FAILED);
			}
		}


		return temp;


	}

	public static ArrayList<String> getProcessStoreInfo(IEntityManagerHelper helper) throws Exception
	{
		return runRemoteScript(helper.getQaHost(),helper.getUsername(),
      helper.getPassword(),"ls "+helper.getStoreLocation()+"/store/PROCESS", helper.getIdentityFile());
	}

	public static ArrayList<String> getDataSetStoreInfo(IEntityManagerHelper helper) throws Exception
	{
		return runRemoteScript(helper.getQaHost(),helper.getUsername(),helper.getPassword(),"ls "+helper.getStoreLocation()+"/store/FEED", helper.getIdentityFile());
	}

	public static ArrayList<String> getDataSetArchiveInfo(IEntityManagerHelper helper) throws Exception
	{
		return runRemoteScript(helper.getQaHost(),helper.getUsername(),helper.getPassword(),"ls "+helper.getStoreLocation()+"/store/archive/FEED", helper.getIdentityFile());
	}

	public static ArrayList<String> getArchiveStoreInfo(IEntityManagerHelper helper) throws Exception
	{
		return runRemoteScript(helper.getQaHost(),helper.getUsername(),helper.getPassword(),"ls "+helper.getStoreLocation()+"/store/archive/PROCESS", helper.getIdentityFile());
	}

	public static ArrayList<String> getClusterStoreInfo(IEntityManagerHelper helper) throws Exception
	{
		Util.print("getting Storeinfo from box: "+helper.getQaHost());
		return runRemoteScript(helper.getQaHost(),helper.getUsername(),helper.getPassword(),"ls "+helper.getStoreLocation()+"/store/CLUSTER", helper.getIdentityFile());
	}

	public static ArrayList<String> getClusterArchiveInfo(IEntityManagerHelper helper) throws Exception
	{
		return runRemoteScript(helper.getQaHost(),helper.getUsername(),helper.getPassword(),"ls "+helper.getStoreLocation()+"/store/archive/CLUSTER", helper.getIdentityFile());
	}


	public static ArrayList<String> runRemoteScript(String hostName,
                                                  String userName,
                                                  String password,
                                                  String command,
                                                  String identityFile) throws
    Exception
    {
        JSch jsch = new JSch();
        Session session = jsch.getSession(userName, hostName, 22);

        System.out.println("host_name: "+hostName+ " user_name: "+userName +" password: "+password+ " command: "+command);
        // only set the password if its not empty
        if (null != password && !password.isEmpty()) {
          session.setUserInfo(new HardcodedUserInfo(password));
        }
        Properties config = new Properties();
        config.setProperty("StrictHostKeyChecking", "no");
        config.setProperty("UserKnownHostsFile", "/dev/null");
        // only set the password if its not empty
        if (null == password || password.isEmpty()) {
          jsch.addIdentity(identityFile);
        }
        session.setConfig(config);

        session.connect();

        Assert.assertTrue(session.isConnected(),"The session was not connected correctly!");

        ChannelExec channel = (ChannelExec) session.openChannel("exec");


        logger.info("executing the command..."+command);
        channel.setCommand(command);
        channel.setPty(true);
        channel.connect();
        Assert.assertTrue(channel.isConnected(),"The channel was not connected correctly!");
        logger.info("now reading the line....");

        //now to read output
        ArrayList<String> data = new ArrayList<String>();

        InputStream in = channel.getInputStream();

        Assert.assertTrue(channel.isConnected(),"The channel was not connected correctly!");

        BufferedReader r = new BufferedReader(new InputStreamReader(in));

        String line=new String();
        while (true) {

            while ((line=r.readLine())!=null) {
                //logger.info(line);
                data.add(line);

            }
            if (channel.isClosed()) {

                break;
            }

        }

        in.close();
        r.close();

        channel.disconnect();
        session.disconnect();

        return data;  
    }

	private static ArrayList<String> runRemoteScriptForErrStream(String host_name,String user_name,String password,String command) throws Exception
	{
		JSch jsch = new JSch();
		Session session = jsch.getSession(user_name, host_name, 22);
		//logger.info("Done");

		session.setUserInfo(new HardcodedUserInfo(password));
		Properties config = new Properties();
		config.setProperty("StrictHostKeyChecking", "no");
		session.setConfig(config);

		session.connect();

		Assert.assertTrue(session.isConnected(),"The session was not connected correctly!");

		ChannelExec channel = (ChannelExec) session.openChannel("exec");


		logger.info("executing the command...");

		channel.setCommand(command);
		channel.connect();
		Assert.assertTrue(channel.isConnected(),"The channel was not connected correctly!");



		//Assert.assertTrue("The channel did not connect correctly for setting the path!",channel.isConnected());

		logger.info("now reading the line....");

		//now to read output
		ArrayList<String> data = new ArrayList<String>();

		//InputStream in = channel.getInputStream();
		InputStream in =channel.getErrStream();

		Assert.assertTrue(channel.isConnected(),"The channel was not connected correctly!");

		BufferedReader r = new BufferedReader(new InputStreamReader(in));


		String line=new String();
		while (true) {

			while ((line=r.readLine())!=null) {
				logger.info(line);
				data.add(line);

			}
			if (channel.isClosed()) {

				break;
			}

		}

		in.close();
		r.close();

		channel.disconnect();
		session.disconnect();



		return data;                


	}

	private static class HardcodedUserInfo implements UserInfo {

		private final String password;

		private HardcodedUserInfo(String password) {
			this.password = password;
		}

		public String getPassphrase() {
			return null;
		}

		public String getPassword() {
			return password;
		}

		public boolean promptPassword(String s) {
			return true;
		}

		public boolean promptPassphrase(String s) {
			return true;
		}

		public boolean promptYesNo(String s) {
			return true;
		}

		public void showMessage(String s) {
			logger.info("message = " + s);
		}
	}


	public static String readEntityName(String data) throws Exception
	{


		JAXBContext jc=JAXBContext.newInstance(Process.class); 

		Unmarshaller u=jc.createUnmarshaller();

		Process processElement=(Process)u.unmarshal((new StringReader(data)));

		return processElement.getName();
	}

	public static String readClusterName(String data) throws Exception
	{
		JAXBContext jc=JAXBContext.newInstance(com.inmobi.qa.falcon.generated.cluster.Cluster.class);
		Unmarshaller u=jc.createUnmarshaller();

		com.inmobi.qa.falcon.generated.cluster.Cluster clusterElement=(com.inmobi.qa.falcon.generated.cluster.Cluster)u.unmarshal(new StringReader(data));

		return clusterElement.getName();
	}

	//    public static String readDatasetName(String data) throws Exception
	//    {
	//    	JAXBContext jc=JAXBContext.newInstance(Dataset.class);
	//    	Unmarshaller u=jc.createUnmarshaller();
	//        
	//    	Dataset datasetElement=(Dataset)u.unmarshal((new StringReader(data)));
	//        
	//        return datasetElement.getName();
	//    	
	//     }

	public static String readDatasetName(String data) throws Exception
	{
		JAXBContext jc=JAXBContext.newInstance(Feed.class);
		Unmarshaller u=jc.createUnmarshaller();

		Feed datasetElement=(Feed)u.unmarshal((new StringReader(data)));

		return datasetElement.getName();

	}


	public static String generateUniqueProcessEntity(String data) throws Exception
	{
		JAXBContext jc=JAXBContext.newInstance(Process.class); 

		Unmarshaller u=jc.createUnmarshaller();

		Process processElement=(Process)u.unmarshal((new StringReader(data)));

		processElement.setName(processElement.getName()+"-"+UUID.randomUUID());

		//lets marshall it back and return 
		java.io.StringWriter sw = new StringWriter();

		Marshaller marshaller = jc.createMarshaller();
		//marshaller.setProperty(Marshaller.JAXB_ENCODING, "UTF-8");
		marshaller.marshal(processElement,sw);

		return sw.toString();

	}

	public static String setProcessName(String data,String name) throws Exception
	{
		JAXBContext jc=JAXBContext.newInstance(Process.class); 

		Unmarshaller u=jc.createUnmarshaller();

		Process processElement=(Process)u.unmarshal((new StringReader(data)));

		processElement.setName(name);

		//lets marshall it back and return 
		java.io.StringWriter sw = new StringWriter();

		Marshaller marshaller = jc.createMarshaller();
		//marshaller.setProperty(Marshaller.JAXB_ENCODING, "UTF-8");
		marshaller.marshal(processElement,sw);

		return sw.toString();

	}


	public static String generateUniqueClusterEntity(String data) throws Exception
	{
		JAXBContext jc=JAXBContext.newInstance(com.inmobi.qa.falcon.generated.cluster.Cluster.class);

		Unmarshaller u=jc.createUnmarshaller();

		com.inmobi.qa.falcon.generated.cluster.Cluster clusterElement=(com.inmobi.qa.falcon.generated.cluster.Cluster)u.unmarshal((new StringReader(data)));

		clusterElement.setName(clusterElement.getName()+"-"+UUID.randomUUID());

		//lets marshall it back and return 
		java.io.StringWriter sw = new StringWriter();

		Marshaller marshaller = jc.createMarshaller();
		//marshaller.setProperty(Marshaller.JAXB_ENCODING, "UTF-8");
		marshaller.marshal(clusterElement,sw);

		return sw.toString();
	}


	//    public static String generateUniqueDataEntity(String data) throws Exception
	//    {
	//        JAXBContext jc=JAXBContext.newInstance(Dataset.class); 
	//       
	//        Unmarshaller u=jc.createUnmarshaller();
	//        
	//        Dataset dataElement=(Dataset)u.unmarshal((new StringReader(data)));
	//        
	//        dataElement.setName(dataElement.getName()+"-"+UUID.randomUUID());
	//        
	//        //lets marshall it back and return 
	//        java.io.StringWriter sw = new StringWriter();
	//
	//        Marshaller marshaller = jc.createMarshaller();
	//        //marshaller.setProperty(Marshaller.JAXB_ENCODING, "UTF-8");
	//        marshaller.marshal(dataElement,sw);
	//                
	//        return sw.toString();
	//    }

	public static String generateUniqueDataEntity(String data) throws Exception
	{
		JAXBContext jc=JAXBContext.newInstance(Feed.class); 

		Unmarshaller u=jc.createUnmarshaller();

		Feed dataElement=(Feed)u.unmarshal((new StringReader(data)));

		dataElement.setName(dataElement.getName()+"-"+UUID.randomUUID());

		return instanceUtil.feedElementToString(dataElement);
	}

	public static COORDINATORAPP getOozieCoordinator(String processName) throws Exception
	{

		//ArrayList<String> tempData=runRemoteScript(HOST_NAME,USER_NAME,PASSWORD,HADOOP_GET_COMMAND+"/"+processName+" FALCON_PROCESS_"+processName+".xml");

		ArrayList<String> tempData=runRemoteScript(HOST_NAME,USER_NAME,PASSWORD,HADOOP_GET_COMMAND+"/"+processName+"/FALCON_PROCESS_DEFAULT/coordinator.xml", IDENTITY_FILE);

		JAXBContext jc=JAXBContext.newInstance(COORDINATORAPP.class);
		Unmarshaller u=jc.createUnmarshaller();

		if(!tempData.isEmpty())
		{
			//StringReader sr=new StringReader(tempData.get(0));
			StringReader sr=new StringReader(arrayToString(tempData));

			return ((JAXBElement<COORDINATORAPP>)u.unmarshal(sr)).getValue();
		}
		return null;
	}

	public static COORDINATORAPP getOozieCoordinator(String processName,String path) throws Exception
	{

		//ArrayList<String> tempData=runRemoteScript(HOST_NAME,USER_NAME,PASSWORD,HADOOP_GET_COMMAND+"/"+processName+" IVORY_PROCESS_"+processName+".xml");

		ArrayList<String> tempData=runRemoteScript(HOST_NAME,USER_NAME,PASSWORD,
      HADOOP_LOCATION+"  fs -cat "+path, IDENTITY_FILE);

		JAXBContext jc=JAXBContext.newInstance(COORDINATORAPP.class);
		Unmarshaller u=jc.createUnmarshaller();

		if(!tempData.isEmpty())
		{
			//StringReader sr=new StringReader(tempData.get(0));
			StringReader sr=new StringReader(arrayToString(tempData));

			return ((JAXBElement<COORDINATORAPP>)u.unmarshal(sr)).getValue();
		}
		return null;
	}

	private static String arrayToString(ArrayList<String> data) throws Exception
	{
		StringBuffer builderData=new StringBuffer();

		for(String line:data)
		{
			builderData.append(line);
			builderData.append("\n");
		}

		return builderData.toString();
	}

	public static Object[][] readBundles() throws Exception
	{
		final String FILEPATH="src/test/resources/bundles";

		List<Bundle> bundleSet=(new Util()).getDataFromFolder(FILEPATH);

		Object[][] testData=new Object[bundleSet.size()][1];

		for(int i=0;i<bundleSet.size();i++)
		{
			//bundleSet.get(i).setClusterData(getEnvClusterXML(bundleSet.get(i).getClusterData()));
			testData[i][0]=bundleSet.get(i);
		}

		return testData;
	}

	/*	public static String getEnvClusterXML(String cluster) throws Exception
	{
		Cluster clusterObject=getClusterObject(cluster);

		//now read and set relevant values
		for(Interface iface:clusterObject.getInterfaces().getInterface())
		{
			if(iface.getType().equals(Interfacetype.READONLY))
			{
				iface.setEndpoint(readPropertiesFile("cluster_readonly"));
			}
			else if(iface.getType().equals(Interfacetype.WRITE))
			{
				iface.setEndpoint(readPropertiesFile("cluster_write"));
			}
			else if(iface.getType().equals(Interfacetype.EXECUTE))
			{
				iface.setEndpoint(readPropertiesFile("cluster_execute"));
			}
			else if(iface.getType().equals(Interfacetype.WORKFLOW))
			{
				iface.setEndpoint(readPropertiesFile("oozie_url"));
			}
			else if(iface.getType().equals(Interfacetype.MESSAGING))
			{
				iface.setEndpoint(readPropertiesFile("activemq_url"));
			}
		}

		JAXBContext context=JAXBContext.newInstance(Cluster.class);
		Marshaller m=context.createMarshaller();
		StringWriter writer=new StringWriter();

		m.marshal(clusterObject,writer);
		return writer.toString();
	}*/

	public static String getEnvClusterXML(String filename,String cluster) throws Exception
	{
		com.inmobi.qa.falcon.generated.cluster.Cluster clusterObject=getClusterObject(cluster);

		//now read and set relevant values
		for(Interface iface:clusterObject.getInterfaces().getInterface())
		{
			if(iface.getType().equals(Interfacetype.READONLY))
			{
				iface.setEndpoint(readPropertiesFile(filename,"cluster_readonly"));
			}
			else if(iface.getType().equals(Interfacetype.WRITE))
			{
				iface.setEndpoint(readPropertiesFile(filename,"cluster_write"));
			}
			else if(iface.getType().equals(Interfacetype.EXECUTE))
			{
				iface.setEndpoint(readPropertiesFile(filename,"cluster_execute"));
			}
			else if(iface.getType().equals(Interfacetype.WORKFLOW))
			{
				iface.setEndpoint(readPropertiesFile(filename,"oozie_url"));
			}
			else if(iface.getType().equals(Interfacetype.MESSAGING))
			{
				iface.setEndpoint(readPropertiesFile(filename,"activemq_url"));
			}

		}


		//set colo name:
		clusterObject.setColo(readPropertiesFile(filename,"colo"));

		JAXBContext context=JAXBContext.newInstance(com.inmobi.qa.falcon.generated.cluster.Cluster.class);
		Marshaller m=context.createMarshaller();
		StringWriter writer=new StringWriter();

		m.marshal(clusterObject,writer);
		return writer.toString();
	}

	public static String readPropertiesFile(String property)
	{
		String desired_property=new String();

		try {
			logger.info("will read from config file for env: "+System.getProperty("environment"));
			FileInputStream conf_stream=new FileInputStream(new File("src/main/resources/"+System.getProperty("environment")));


			Properties properties=new Properties();
			properties.load(conf_stream);

			desired_property=properties.getProperty(property);

			conf_stream.close();


		}
		catch(Exception e)
		{
			logger.info(e.getStackTrace());
		}

		finally {
			return desired_property;
		}
	}

	public static String readPropertiesFile(String filename,String property)
	{
		String desired_property=new String();

		try {
			//logger.info("will read from config file for env: "+System.getProperty("environment"));
			FileInputStream conf_stream=new FileInputStream(new File("src/main/resources/"+filename));


			Properties properties=new Properties();
			properties.load(conf_stream);

			desired_property=properties.getProperty(property);

			conf_stream.close();


		}
		catch(Exception e)
		{
			logger.info(e.getStackTrace());
		}

		finally {
			return desired_property;
		}
	}

	private static String getEnvFeedXML(String feed) throws Exception
	{
		//not used now. Maybe later.
		return feed;
	}

	private static String getEnvProcessXML(String process) throws Exception
	{
		//Not used now. Maybe later.
		return process;
	}

	public static Object[][] readBundles(String path) throws Exception
	{
		@SuppressWarnings("RedundantStringConstructorCall")
		final String FILEPATH=new String(path);

		List<Bundle> bundleSet=(new Util()).getDataFromFolder(FILEPATH);

		Object[][] testData=new Object[bundleSet.size()][1];

		for(int i=0;i<bundleSet.size();i++)
		{
			//bundleSet.get(i).setClusterData(getEnvClusterXML(bundleSet.get(i).getClusterData()));
			testData[i][0]=bundleSet.get(i);
		}

		return testData;
	}

	public static Object[][] readELBundles() throws Exception
	{
		final String FILEPATH="src/test/resources/ELbundle";

		List<Bundle> bundleSet=(new Util()).getDataFromFolder(FILEPATH);

		Object[][] testData=new Object[bundleSet.size()][1];

		for(int i=0;i<bundleSet.size();i++)
		{
			//bundleSet.get(i).setClusterData(getEnvClusterXML(bundleSet.get(i).getClusterData()));
			testData[i][0]=bundleSet.get(i);
		}

		return testData;
	}

	public static Bundle[] getBundleData(String path) throws Exception
	{
		final String FILEPATH=new String(path);

		List<Bundle> bundleSet=(new Util()).getDataFromFolder(FILEPATH);

		//		for(Bundle bundle:bundleSet)
		//		{
		//			bundle.setClusterData(getEnvClusterXML(bundle.getClusterData()));
		//		}

		return bundleSet.toArray(new Bundle[bundleSet.size()]);
	}

	public static ArrayList<String> getOozieJobStatus(PrismHelper coloHelper,String processName) throws Exception
	{
		//return runRemoteScript(HOST_NAME,USER_NAME,PASSWORD,"oozie job -info "+jobId+" | head -5 | tail -1").get(0).split(":")[1].trim();
		String tempFileName="sample-"+UUID.randomUUID()+".tmp";
		//ArrayList<String> oozieLocation=runRemoteScript(HOST_NAME,USER_NAME,PASSWORD,"which oozie");
		final String oozieLocation=coloHelper.getProcessHelper().getOozieLocation()+"/oozie";
		logger.info(coloHelper.getProcessHelper().getOozieLocation()+"/oozie jobs -oozie "+coloHelper.getProcessHelper().getOozieURL()+"  -jobtype bundle -localtime -filter \"status=RUNNING;name=FALCON_PROCESS_"+processName+"\" | tail -2 | head -1");
		//ArrayList<String>jobList= runRemoteScript(HOST_NAME,USER_NAME,PASSWORD,OOZIE_LOCATION+"/oozie jobs -oozie "+OOZIE_URL+"  -jobtype coordinator -localtime -filter \"status=RUNNING;name=IVORY_PROCESS_"+processName+"\" | tail -2 | head -1");
		String expectedState="RUNNING";
		String statusCommand=coloHelper.getProcessHelper().getOozieLocation()+"/oozie jobs -oozie "+coloHelper.getProcessHelper().getOozieURL()+"  -jobtype bundle -localtime -filter \"status=RUNNING;name=FALCON_PROCESS_"+processName+"\" | tail -2 | head -1";

		ArrayList<String> jobList=new ArrayList();

		for(int seconds=0;seconds<20;seconds++)
		{
			jobList= runRemoteScript(coloHelper.getProcessHelper().getQaHost(),
        coloHelper.getProcessHelper().getUsername(),
        coloHelper.getProcessHelper().getPassword(),statusCommand,
        coloHelper.getProcessHelper().getIdentityFile());

			if( (expectedState!=null || !(expectedState.equals(""))) && jobList.get(0).contains(expectedState))
			{
				logger.info(jobList.get(0));
				break;
			}
			else
			{
				Thread.sleep(1000);
			}

		}

		if(!(jobList.isEmpty()) || null!=jobList)
		{
			logger.info(jobList.get(0));
		}
		return jobList;


	}

	public static ArrayList<String> getOozieJobStatus(String processName) throws Exception
	{
		//return runRemoteScript(HOST_NAME,USER_NAME,PASSWORD,"oozie job -info "+jobId+" | head -5 | tail -1").get(0).split(":")[1].trim();
		String tempFileName="sample-"+UUID.randomUUID()+".tmp";
		//ArrayList<String> oozieLocation=runRemoteScript(HOST_NAME,USER_NAME,PASSWORD,"which oozie");
		final String oozieLocation=OOZIE_LOCATION+"/oozie";
		logger.info(OOZIE_LOCATION+"/oozie jobs -oozie "+OOZIE_URL+"  -jobtype bundle -localtime -filter \"status=RUNNING;name=FALCON_PROCESS_"+processName+"\" | tail -2 | head -1");
		//ArrayList<String>jobList= runRemoteScript(HOST_NAME,USER_NAME,PASSWORD,OOZIE_LOCATION+"/oozie jobs -oozie "+OOZIE_URL+"  -jobtype coordinator -localtime -filter \"status=RUNNING;name=IVORY_PROCESS_"+processName+"\" | tail -2 | head -1");
		String expectedState="RUNNING";
		String statusCommand=OOZIE_LOCATION+"/oozie jobs -oozie "+OOZIE_URL+"  -jobtype bundle -localtime -filter \"status=RUNNING;name=FALCON_PROCESS_"+processName+"\" | tail -2 | head -1";

		ArrayList<String> jobList=new ArrayList();

		for(int seconds=0;seconds<20;seconds++)
		{
			jobList= runRemoteScript(HOST_NAME,USER_NAME,PASSWORD,statusCommand, IDENTITY_FILE);

			if( (expectedState!=null || !(expectedState.equals(""))) && jobList.get(0).contains(expectedState))
			{
				logger.info(jobList.get(0));
				break;
			}
			else
			{
				Thread.sleep(1000);
			}

		}

		if(!(jobList.isEmpty()) || null!=jobList)
		{
			logger.info(jobList.get(0));
		}
		return jobList;


	}

	public static ArrayList<String> getOozieJobStatus(PrismHelper prismHelper,String processName,String expectedState) throws Exception
	{
		//return runRemoteScript(HOST_NAME,USER_NAME,PASSWORD,"oozie job -info "+jobId+" | head -5 | tail -1").get(0).split(":")[1].trim();
		String tempFileName="sample-"+UUID.randomUUID()+".tmp";
		//ArrayList<String> oozieLocation=runRemoteScript(HOST_NAME,USER_NAME,PASSWORD,"which oozie");
		final String oozieLocation=prismHelper.getProcessHelper().getOozieLocation()+"/oozie";

		String statusCommand=prismHelper.getProcessHelper().getOozieLocation()+"/oozie jobs -oozie "+prismHelper.getProcessHelper().getOozieURL()+"  -jobtype bundle -localtime -filter \"";

		if(expectedState.equals("NONE"))
		{
			statusCommand = statusCommand ;
		}
		else if(null!=expectedState || !(expectedState.equalsIgnoreCase("")))
		{
			statusCommand+="status="+expectedState+";";
		}


		statusCommand+="name=FALCON_PROCESS_"+processName+"\" | tail -2 | head -1";
		logger.info(statusCommand);
		//ArrayList<String>jobList= runRemoteScript(HOST_NAME,USER_NAME,PASSWORD,OOZIE_LOCATION+"/oozie jobs -oozie "+OOZIE_URL+"  -jobtype coordinator -localtime -filter \"status="+expectedState+";name="+processName+"\" | tail -2 | head -1");
		ArrayList<String> jobList=new ArrayList();

		for(int seconds=0;seconds<20;seconds++)
		{
			jobList= runRemoteScript(prismHelper.getProcessHelper().getQaHost(),
        prismHelper.getProcessHelper().getUsername(),
        prismHelper.getProcessHelper().getPassword(),statusCommand,
        prismHelper.getProcessHelper().getIdentityFile());

			if( (expectedState!=null || expectedState.equalsIgnoreCase("NONE")) || !(expectedState.equals("") && jobList.get(0).contains(expectedState)))
			{
				break;
			}
			else
			{
				Thread.sleep(1000);
			}

		}

		if(!(jobList.isEmpty()) || null!=jobList)
		{
			logger.info(jobList.get(0));
		}
		return jobList;
	}        

	public static ArrayList<String> getOozieJobStatus(String processName,String expectedState,ColoHelper colohelper) throws Exception
	{
		//return runRemoteScript(HOST_NAME,USER_NAME,PASSWORD,"oozie job -info "+jobId+" | head -5 | tail -1").get(0).split(":")[1].trim();
		String tempFileName="sample-"+UUID.randomUUID()+".tmp";
		//ArrayList<String> oozieLocation=runRemoteScript(HOST_NAME,USER_NAME,PASSWORD,"which oozie");
		final String oozieLocation=colohelper.getProcessHelper().getOozieLocation()+"/oozie";

		String statusCommand=colohelper.getProcessHelper().getOozieLocation()+"/oozie jobs -oozie "+colohelper.getProcessHelper().getOozieURL()+"  -jobtype bundle -localtime -filter \"";

		if(expectedState.equals("NONE"))
		{
			statusCommand = statusCommand ;
		}
		else if(null!=expectedState || !(expectedState.equalsIgnoreCase("")))
		{
			statusCommand+="status="+expectedState+";";
		}


		statusCommand+="name=FALCON_PROCESS_"+processName+"\" | tail -2 | head -1";
		logger.info(statusCommand);
		//ArrayList<String>jobList= runRemoteScript(HOST_NAME,USER_NAME,PASSWORD,OOZIE_LOCATION+"/oozie jobs -oozie "+OOZIE_URL+"  -jobtype coordinator -localtime -filter \"status="+expectedState+";name="+processName+"\" | tail -2 | head -1");
		ArrayList<String> jobList=new ArrayList();

		for(int seconds=0;seconds<20;seconds++)
		{
			jobList= runRemoteScript(colohelper.getProcessHelper().getQaHost(),
        colohelper.getProcessHelper().getUsername(),
        colohelper.getProcessHelper().getPassword(),statusCommand,
        colohelper.getProcessHelper().getIdentityFile());

			if( (expectedState!=null || expectedState.equalsIgnoreCase("NONE")) || !(expectedState.equals("") && jobList.get(0).contains(expectedState)))
			{
				break;
			}
			else
			{
				Thread.sleep(1000);
			}

		}

		if(!(jobList.isEmpty()) || null!=jobList)
		{
			logger.info(jobList.get(0));
		}
		return jobList;
	}

	public static ArrayList<String> getOozieFeedJobStatus(String processName,String expectedState) throws Exception
	{
		//return runRemoteScript(HOST_NAME,USER_NAME,PASSWORD,"oozie job -info "+jobId+" | head -5 | tail -1").get(0).split(":")[1].trim();
		String tempFileName="sample-"+UUID.randomUUID()+".tmp";
		//ArrayList<String> oozieLocation=runRemoteScript(HOST_NAME,USER_NAME,PASSWORD,"which oozie");
		final String oozieLocation=OOZIE_LOCATION+"/oozie";

		String statusCommand=OOZIE_LOCATION+"/oozie jobs -oozie "+OOZIE_URL+"  -jobtype bundle -localtime -filter \"";
		//String statusCommand=coloHelper.getFeedHelper().getOozieLocation()+"/oozie jobs -oozie "+coloHelper.getFeedHelper().getOozieURL()+"  -jobtype bundle -localtime -filter \"";

		if(expectedState.equals("NONE"))
		{
			statusCommand = statusCommand ;
		}
		else if(null!=expectedState || !(expectedState.equalsIgnoreCase("")))
		{
			statusCommand+="status="+expectedState+";";
		}


		statusCommand+="name=FALCON_FEED_"+processName+"\" | tail -2 | head -1";
		logger.info(statusCommand);
		//ArrayList<String>jobList= runRemoteScript(HOST_NAME,USER_NAME,PASSWORD,OOZIE_LOCATION+"/oozie jobs -oozie "+OOZIE_URL+"  -jobtype coordinator -localtime -filter \"status="+expectedState+";name="+processName+"\" | tail -2 | head -1");
		ArrayList<String> jobList=new ArrayList();

		for(int seconds=0;seconds<20;seconds++)
		{
			jobList= runRemoteScript(HOST_NAME,USER_NAME,PASSWORD,statusCommand, IDENTITY_FILE);

			if( (expectedState!=null || expectedState.equalsIgnoreCase("NONE")) || !(expectedState.equals("") && jobList.get(0).contains(expectedState)))
			{
				break;
			}
			else
			{
				Thread.sleep(1000);
			}

		}

		if(!(jobList.isEmpty()) || null!=jobList)
		{
			logger.info(jobList.get(0));
		}
		return jobList;
	}

	public static ArrayList<String> getOozieFeedJobStatus(String processName,String expectedState,PrismHelper coloHelper) throws Exception
	{
		//return runRemoteScript(HOST_NAME,USER_NAME,PASSWORD,"oozie job -info "+jobId+" | head -5 | tail -1").get(0).split(":")[1].trim();
		String tempFileName="sample-"+UUID.randomUUID()+".tmp";
		//ArrayList<String> oozieLocation=runRemoteScript(HOST_NAME,USER_NAME,PASSWORD,"which oozie");
		final String oozieLocation=coloHelper.getFeedHelper().getOozieLocation()+"/oozie";

		String statusCommand=coloHelper.getFeedHelper().getOozieLocation()+"/oozie jobs -oozie "+coloHelper.getFeedHelper().getOozieURL()+"  -jobtype bundle -localtime -filter \"";

		if(expectedState.equals("NONE"))
		{
			statusCommand = statusCommand ;
		}
		else if(null!=expectedState || !(expectedState.equalsIgnoreCase("")))
		{
			statusCommand+="status="+expectedState+";";
		}


		statusCommand+="name=FALCON_FEED_"+processName+"\" | tail -2 | head -1";
		logger.info(statusCommand);
		//ArrayList<String>jobList= runRemoteScript(HOST_NAME,USER_NAME,PASSWORD,OOZIE_LOCATION+"/oozie jobs -oozie "+OOZIE_URL+"  -jobtype coordinator -localtime -filter \"status="+expectedState+";name="+processName+"\" | tail -2 | head -1");
		ArrayList<String> jobList=new ArrayList();

		for(int seconds=0;seconds<20;seconds++)
		{
			jobList= runRemoteScript(coloHelper.getFeedHelper().getQaHost(),
        coloHelper.getFeedHelper().getUsername(),coloHelper.getFeedHelper()
        .getPassword(),statusCommand, coloHelper.getProcessHelper().getIdentityFile());

			if( (expectedState!=null || expectedState.equalsIgnoreCase("NONE")) || !(expectedState.equals("") && jobList.get(0).contains(expectedState)))
			{
				break;
			}
			else
			{
				Thread.sleep(1000);
			}

		}

		if(!(jobList.isEmpty()) || null!=jobList)
		{
			logger.info(jobList.get(0));
		}
		return jobList;
	}        

	public static void validateCodeForValidRequest(int code) throws Exception
	{
		Assert.assertEquals(code,200,"Code is different than 200!");
	}




	public enum URLS
	{

		//HOSTNAME("http://10.14.111.146:15000"),
		SUBMIT_URL("/api/entities/submit"),
		GET_ENTITY_DEFINITION("/api/entities/definition"),
		DELETE_URL("/api/entities/delete"),
		SCHEDULE_URL( "/api/entities/schedule"),
		VALIDATE_URL("/api/entities/validate"),
		SUSPEND_URL("/api/entities/suspend"),
		RESUME_URL("/api/entities/resume"),
		STATUS_URL("/api/entities/status"),
		SUBMIT_AND_SCHEDULE_URL("/api/entities/submitAndSchedule"),
		INSTANCE_RUNNING("/api/instance/running"),
		INSTANCE_STATUS("/api/instance/status"),
		INSTANCE_KILL("/api/instance/kill"),
		INSTANCE_RESUME("/api/instance/resume"),
		INSTANCE_SUSPEND("/api/instance/suspend"),
		PROCESS_UPDATE("/api/entities/update/process"),
		INSTANCE_RERUN("/api/instance/rerun"),
		FEED_UPDATE("/api/entities/update/feed");

		private final String url;

		URLS(String url){
			this.url = url ; }

		public String getValue()
		{
			return this.url ;
		}
	}


	public static void assertSucceeded(ServiceResponse response) throws Exception
	{
		Assert.assertEquals(Util.parseResponse(response).getStatus(),APIResult.Status.SUCCEEDED);
		Assert.assertEquals(Util.parseResponse(response).getStatusCode(),200);
		Assert.assertNotNull(Util.parseResponse(response).getMessage());
		//Assert.assertNotNull(Util.parseResponse(response).getRequestId());
	}

	public static void assertSucceeded(ServiceResponse response, String message) throws Exception
	{
		Assert.assertEquals(Util.parseResponse(response).getStatus(),APIResult.Status.SUCCEEDED,message);
		Assert.assertEquals(Util.parseResponse(response).getStatusCode(),200,message);
		//Assert.assertNotNull(Util.parseResponse(response).getRequestId());
	}

	public static void assertPartialSucceeded(ServiceResponse response) throws Exception
	{
		Assert.assertEquals(Util.parseResponse(response).getStatus(),APIResult.Status.PARTIAL);
		Assert.assertEquals(Util.parseResponse(response).getStatusCode(),400);
		Assert.assertNotNull(Util.parseResponse(response).getMessage());
		//Assert.assertNotNull(Util.parseResponse(response).getRequestId());
	}

	public static void assertFailed(ServiceResponse response) throws Exception
	{
		if(response.message.equals("null"))
			Assert.assertTrue(false,"response message should not be null");

		Assert.assertEquals(Util.parseResponse(response).getStatus(),APIResult.Status.FAILED);
		Assert.assertEquals(Util.parseResponse(response).getStatusCode(),400);
		//Assert.assertNotNull(Util.parseResponse(response).getRequestId());
	}
	public static void assertFailed(ServiceResponse response, String message) throws Exception
	{
		if(response.message.equals("null"))
			Assert.assertTrue(false,"response message should not be null");

		Assert.assertEquals(Util.parseResponse(response).getStatus(),APIResult.Status.FAILED,message);
		Assert.assertEquals(Util.parseResponse(response).getStatusCode(),400,message);
		Assert.assertNotNull(Util.parseResponse(response).getRequestId());
	}

	public static void print(String message)
	{
		logger.info(message);
	}

	public static String getCoordID(String response)
	{
		return response.substring(0,response.indexOf(" "));
	}

	public static String getDatasetPath(Bundle bundle) throws Exception
	{
		JAXBContext jc=JAXBContext.newInstance(Feed.class); 

		Unmarshaller u=jc.createUnmarshaller();

		Feed dataElement=(Feed)u.unmarshal((new StringReader(bundle.dataSets.get(0))));

		if(!dataElement.getName().contains("raaw-logs16"))
		{
			dataElement = (Feed)u.unmarshal(new StringReader(bundle.dataSets.get(1)));

		}

		return dataElement.getLocations().getLocation().get(0).getPath();

	}

	public static ArrayList<String> getMissingDependencies(PrismHelper helper,String bundleID) throws Exception
	{
		XOozieClient oozieClient=new XOozieClient(readPropertiesFile(helper.getEnvFileName(),"oozie_url"));
		BundleJob bundleJob = oozieClient.getBundleJobInfo(bundleID);
		CoordinatorJob jobInfo = oozieClient.getCoordJobInfo(bundleJob.getCoordinators().get(0).getId());
		List<CoordinatorAction> actions = jobInfo.getActions();

		Util.print("conf from event: "+actions.get(0).getMissingDependencies());

		String[] missingDependencies = actions.get(0).getMissingDependencies().split("#");
		return new ArrayList<String>(Arrays.asList(missingDependencies));
	}

	public static String getJobErrors(String bundleID) throws Exception
	{
		XOozieClient oozieClient=new XOozieClient(OOZIE_URL);
		logger.info("looking for bundleId info with id: "+bundleID);
		BundleJob bundleJob = oozieClient.getBundleJobInfo(bundleID);
		CoordinatorJob jobInfo = oozieClient.getCoordJobInfo(bundleJob.getCoordinators().get(0).getId());
		logger.info("now looking for coordinator info with id: "+jobInfo.getId());

		int idleCounter=0;
		while(null==jobInfo.getActions().get(0).getExternalId() || idleCounter<600)
		{
			//sleep
			Thread.sleep(1000);
			jobInfo = oozieClient.getCoordJobInfo(bundleJob.getCoordinators().get(0).getId());
			idleCounter++;
		}

		WorkflowJob workflowInfo=oozieClient.getJobInfo(jobInfo.getActions().get(0).getExternalId());

		logger.info("now looking for workflow info with id: "+workflowInfo.getExternalId());
		logger.info("status is: "+workflowInfo.getStatus().name());


		idleCounter=0;
		while(null==workflowInfo.getActions() || idleCounter<600)
		{
			Thread.sleep(1000);
			idleCounter++;
			workflowInfo=oozieClient.getJobInfo(jobInfo.getActions().get(0).getExternalId());
		}

		logger.info("we will check now if workflow has some actions....");

		//logger.info(workflowInfo.getActions());
		idleCounter=0;
		while(!(workflowInfo.getActions().size()>=2) || idleCounter<600)
		{
			//keep looping
			Thread.sleep(1000);
			idleCounter++;
			workflowInfo=oozieClient.getJobInfo(jobInfo.getActions().get(0).getExternalId());
		}


		for(WorkflowAction action:workflowInfo.getActions())
		{
			if(action.getName().equalsIgnoreCase("user-workflow"))
			{
				//while(action.getStatus()!=WorkflowAction.Status.ERROR || action.getStatus()!=WorkflowAction.Status.DONE || action.getStatus()!=WorkflowAction.Status.OK || action.getStatus()!=WorkflowAction.Status.KILLED)
				while(true)
				{

					action=oozieClient.getWorkflowActionInfo(action.getId());
					if(action.getStatus().equals(WorkflowAction.Status.ERROR) || action.getStatus().equals(WorkflowAction.Status.DONE) || action.getStatus().equals(WorkflowAction.Status.OK) || action.getStatus().equals(WorkflowAction.Status.KILLED))
					{
						WorkflowJob info=oozieClient.getJobInfo(action.getExternalId());
						logger.info("encountered error with workflow with id: "+info.getId());
						return info.getActions().get(0).getErrorMessage();
					}

				}
			}


			//WorkflowAction job=oozieClient.getWorkflowActionInfo(action.getExternalId());



		}

		return null;

	}

	public static ArrayList<String> getCoordinatorJobs(PrismHelper prismHelper,String bundleID) throws Exception
	{
		ArrayList<String> jobIds=new ArrayList<String>();
		XOozieClient oozieClient=new XOozieClient(prismHelper.getClusterHelper().getOozieURL());
		BundleJob bundleJob = oozieClient.getBundleJobInfo(bundleID);
		CoordinatorJob jobInfo = oozieClient.getCoordJobInfo(bundleJob.getCoordinators().get(0).getId());

		for(CoordinatorAction action:jobInfo.getActions())
		{
			CoordinatorAction actionInfo=oozieClient.getCoordActionInfo(action.getExternalId());
			if(actionInfo.getStatus().equals(CoordinatorAction.Status.SUCCEEDED))
			{

				jobIds.add(action.getExternalId());
			}
		}


		return jobIds;

	}

	public static ArrayList<String> getCoordinatorJobs(PrismHelper prismHelper,String bundleID, String s) throws Exception
	{
		ArrayList<String> jobIds=new ArrayList<String>();
		XOozieClient oozieClient=new XOozieClient(prismHelper.getClusterHelper().getOozieURL());
		BundleJob bundleJob = oozieClient.getBundleJobInfo(bundleID);
		CoordinatorJob jobInfo = oozieClient.getCoordJobInfo(bundleJob.getCoordinators().get(0).getId());

		for(CoordinatorAction action:jobInfo.getActions())
		{                        
			jobIds.add(action.getId());

		}


		return jobIds;

	}

	public static String getWorkflowInfo(PrismHelper prismHelper,String workflowId) throws Exception
	{
		XOozieClient oozieClient=new XOozieClient(prismHelper.getClusterHelper().getOozieURL());
		logger.info("fetching info for workflow with id: "+workflowId);
		WorkflowJob job=oozieClient.getJobInfo(workflowId);
		return job.getStatus().toString();
	}

	public static Date getNominalTime(PrismHelper prismHelper,String bundleID) throws Exception
	{
		XOozieClient oozieClient=new XOozieClient(prismHelper.getClusterHelper().getOozieURL());
		BundleJob bundleJob = oozieClient.getBundleJobInfo(bundleID);
		CoordinatorJob jobInfo = oozieClient.getCoordJobInfo(bundleJob.getCoordinators().get(0).getId());		
		List<CoordinatorAction> actions = jobInfo.getActions();

		return actions.get(0).getNominalTime() ;

	}

	public static String CreateFeedWithNewPartitions(String feed,String ... partitions) throws Exception
	{
		JAXBContext feedContext= JAXBContext.newInstance(Feed.class);
		Unmarshaller unmarshaller=feedContext.createUnmarshaller();
		Feed feedObject=(Feed)unmarshaller.unmarshal(new StringReader(feed));

		Partitions partitionSet=new Partitions();

		if(null!=feedObject.getPartitions() && null!=feedObject.getPartitions().getPartition())
		{
			for(Partition partition:feedObject.getPartitions().getPartition())
			{
				partitionSet.addPartition(partition);
			}
		}

		for(String partition:partitions)
		{
			Partition feedPartition=new Partition(); 
			feedPartition.setName(partition);
			partitionSet.addPartition(feedPartition);
		}

		//add partition to the feed
		feedObject.setPartitions(partitionSet);

		//now assign the new feedobject to the existing feed :P
		Marshaller marshaller=feedContext.createMarshaller();
		StringWriter feeder=new StringWriter();
		marshaller.marshal(feedObject,feeder);
		return feeder.toString();
	}

	public static String InsertPartitionInProcess(String process,String feedName,String ... partitions) throws Exception
	{
		JAXBContext processContext= JAXBContext.newInstance(Process.class);
		Unmarshaller unmarshaller=processContext.createUnmarshaller();
		Process processObject=(Process)unmarshaller.unmarshal(new StringReader(process));

		for(String partition:partitions)
		{

			for(Input input:processObject.getInputs().getInput())
			{
				if(input.getFeed().equalsIgnoreCase(feedName))
				{
					//insert the new partition here
					if(null!=input.getPartition()) {input.setPartition(input.getPartition()+","+partition);}
					else
					{
						input.setPartition(partition);
					}
					break;
				}
			}


		}

		//now assign the new feedobject to the existing feed :P
		Marshaller marshaller=processContext.createMarshaller();
		StringWriter feeder=new StringWriter();
		marshaller.marshal(processObject,feeder);
		return feeder.toString();
	}

	//needs to be rewritten to randomly pick an input feed
	public static String getInputFeedFromBundle(Bundle bundle) throws Exception
	{
		String processData=bundle.getProcessData();

		JAXBContext processContext= JAXBContext.newInstance(Process.class);
		Unmarshaller unmarshaller=processContext.createUnmarshaller();
		Process processObject=(Process)unmarshaller.unmarshal(new StringReader(processData));

		for(Input input:processObject.getInputs().getInput())
		{
			for(String feed:bundle.getDataSets())
			{
				if(Util.readDatasetName(feed).equalsIgnoreCase(input.getFeed()))
				{
					return feed;
				}
			}
		}
		return null;

	}

	//        public static ArrayList<String> getHadoopData() throws Exception
	//        {
	//            
	//            //this command copies hadoop files in a directory....then gets the contents
	//            String command=HADOOP_LOCATION+"  dfs -lsr hdfs://"+HADOOP_URL+"/retention/testFolders | awk '///retention//testFolders/ {print $8}'";
	//            
	//            ArrayList<String> result=runRemoteScript(HOST_NAME,USER_NAME,PASSWORD,command);
	//            
	//            ArrayList<String> finalResult=new ArrayList<String>();
	//            
	//            for(String single:result)
	//            {
	//                if(!single.equalsIgnoreCase(""))
	//                {
	//                    String [] splittered=single.split("testFolders/");
	//                    finalResult.add(splittered[splittered.length-1]);
	//                }
	//            }
	//            
	//            return finalResult;
	//        }

	public static ArrayList<String> getHadoopData(PrismHelper prismHelper,String feed) throws Exception
	{

		//this command copies hadoop files in a directory....then gets the contents
		//String command=HADOOP_LOCATION+"  dfs -lsr hdfs://"+HADOOP_URL+"/retention/testFolders | awk '///retention//testFolders/ {print $8}'";
		String command=prismHelper.getClusterHelper().getHadoopLocation()+"  dfs -lsr hdfs://"+prismHelper.getClusterHelper().getHadoopURL()+"/retention/testFolders | awk '{print $8}'";
		ArrayList<String> result=runRemoteScript(prismHelper.getClusterHelper()
      .getQaHost(),prismHelper.getClusterHelper().getUsername(),
      prismHelper.getClusterHelper().getPassword(),command,
      prismHelper.getClusterHelper().getIdentityFile());

		ArrayList<String> finalResult=new ArrayList<String>();

		String feedPath=getFeedPath(feed);

		for(String single:result)
		{
			if(!single.equalsIgnoreCase(""))
			{
				if(feedPath.split("/").length==single.split("/").length)
				{

					String [] splittered=single.split("testFolders/");
					finalResult.add(splittered[splittered.length-1]);
				}
			}
		}

		return finalResult;
	}

	//	public static ArrayList<String> getHadoopLateData(PrismHelper prismHelper,String feed) throws Exception
	//	{
	//
	//		//this command copies hadoop files in a directory....then gets the contents
	//		String command=prismHelper.getClusterHelper().getHadoopLocation()+"  dfs -lsr hdfs://"+prismHelper.getClusterHelper().getHadoopURL()+"/lateDataTest/testFolders | awk '{print $8}'";
	//
	//		ArrayList<String> result=runRemoteScript(prismHelper.getClusterHelper().getQaHost(),prismHelper.getClusterHelper().getHostname(),prismHelper.getClusterHelper().getPassword(),command);
	//
	//		ArrayList<String> finalResult=new ArrayList<String>();
	//
	//		String feedPath=getFeedPath(feed);
	//
	//		for(String single:result)
	//		{
	//			if(!single.equalsIgnoreCase(""))
	//			{
	//				if(feedPath.split("/").length==single.split("/").length)
	//				{
	//
	//					String [] splittered=single.split("testFolders/");
	//					finalResult.add(splittered[splittered.length-1]);
	//				}
	//			}
	//		}
	//
	//		return finalResult;
	//	}        

	public static ArrayList<String> getHadoopLateData(PrismHelper prismHelper,String feed) throws Exception
	{

		//this command copies hadoop files in a directory....then gets the contents
		String command=prismHelper.getClusterHelper().getHadoopLocation()+"  dfs -lsr hdfs://"+prismHelper.getClusterHelper().getHadoopURL()+"/lateDataTest/testFolders | awk '{print $8}'";

		ArrayList<String> result=runRemoteScript(prismHelper.getClusterHelper()
      .getQaHost(),prismHelper.getClusterHelper().getUsername(),
      prismHelper.getClusterHelper().getPassword(),command,
      prismHelper.getClusterHelper().getIdentityFile());

		ArrayList<String> finalResult=new ArrayList<String>();

		String feedPath=getFeedPath(feed);

		for(String single:result)
		{
			if(!single.equalsIgnoreCase(""))
			{
				if(feedPath.split("/").length==single.split("/").length)
				{

					String [] splittered=single.split("testFolders/");
					finalResult.add(splittered[splittered.length-1]);
				}
			}
		}

		return finalResult;
	}

	public static String insertRetentionValueInFeed(String feed,String retentionValue) throws Exception
	{
		JAXBContext context=JAXBContext.newInstance(Feed.class);
		Unmarshaller um=context.createUnmarshaller();

		Feed feedObject=(Feed)um.unmarshal(new StringReader(feed));

		//insert retentionclause
		feedObject.getClusters().getCluster().get(0).getRetention().setLimit(new Frequency(retentionValue));

		for(com.inmobi.qa.falcon.generated.feed.Cluster cluster:feedObject.getClusters().getCluster())
		{
			cluster.getRetention().setLimit(new Frequency(retentionValue));
		}

		StringWriter writer=new StringWriter();

		Marshaller m=context.createMarshaller();

		m.marshal(feedObject,writer);

		return writer.toString();

	}

	public static String insertRetentionValueInFeed(String feed,String retentionValue,String clusterName) throws Exception
	{
		JAXBContext context=JAXBContext.newInstance(Feed.class);
		Unmarshaller um=context.createUnmarshaller();

		Feed feedObject=(Feed)um.unmarshal(new StringReader(feed));

		//insert retentionclause

		for(int i=0;i<feedObject.getClusters().getCluster().size();i++)
		{
			if(feedObject.getClusters().getCluster().get(i).getName().equalsIgnoreCase(clusterName))
			{
				feedObject.getClusters().getCluster().get(i).getRetention().setLimit(new Frequency(retentionValue));
			}
		}

		StringWriter writer=new StringWriter();

		Marshaller m=context.createMarshaller();

		m.marshal(feedObject,writer);

		return writer.toString();

	}        

	public static void replenishData() throws Exception
	{
		ArrayList<String> deleteMessage=runRemoteScript(HOST_NAME,USER_NAME,
      PASSWORD,HADOOP_LOCATION+"  dfs -rmr " +
      "hdfs://"+HADOOP_URL+"/projects/bi/ioout", IDENTITY_FILE);
		//Assert.assertEquals(deleteMessage.get(0),"Deleted hdfs://10.14.110.46/projects/bi/ioout","seems to be a problem with delete");
		runRemoteScript(HOST_NAME,USER_NAME, PASSWORD,HADOOP_LOCATION+"  dfs " +
      "-copyFromLocal /tmp/tmpData/* hdfs://"+HADOOP_URL+"/projects/bi/", IDENTITY_FILE);
	}

	public static void replenishData(List<String> folderList) throws Exception
	{

		//purge data first
		runRemoteScript(HOST_NAME,USER_NAME, PASSWORD,HADOOP_LOCATION+"  dfs -rmr" +
      "  hdfs://"+HADOOP_URL+"/retention/testFolders/", IDENTITY_FILE);
		//createHDFSFolders(HOST_NAME, USER_NAME, PASSWORD, folderList);
		createHDFSFolders(folderList);

		//            for(String folder:folderList)
		//            {
		//                
		//                //Assert.assertEquals(deleteMessage.get(0),"Deleted hdfs://10.14.110.46/projects/bi/ioout","seems to be a problem with delete");
		//                runRemoteScript(HOST_NAME,USER_NAME, PASSWORD,HADOOP_LOCATION+"  dfs -mkdir  hdfs://"+HADOOP_URL+"/retention/testFolders/"+folder);
		//                runRemoteScript(HOST_NAME,USER_NAME, PASSWORD,HADOOP_LOCATION+"  dfs -mkdir  hdfs://"+HADOOP_URL+"/retention/testFolders/somethingRandom");
		//            }
	}

	public static void replenishData(PrismHelper prismHelper,List<String> folderList) throws Exception
	{

		//purge data first
		runRemoteScript(prismHelper.getClusterHelper().getQaHost(),
      prismHelper.getClusterHelper().getUsername(),
      prismHelper.getClusterHelper().getPassword(),
      prismHelper.getClusterHelper().getHadoopLocation()+"  dfs -rmr  " +
        "hdfs://"+prismHelper.getClusterHelper().getHadoopURL()
        +"/retention/testFolders/", prismHelper.getClusterHelper().getIdentityFile());
		//createHDFSFolders(HOST_NAME, USER_NAME, PASSWORD, folderList);
		createHDFSFolders(prismHelper,folderList);

		//            for(String folder:folderList)
		//            {
		//                
		//                //Assert.assertEquals(deleteMessage.get(0),"Deleted hdfs://10.14.110.46/projects/bi/ioout","seems to be a problem with delete");
		//                runRemoteScript(HOST_NAME,USER_NAME, PASSWORD,HADOOP_LOCATION+"  dfs -mkdir  hdfs://"+HADOOP_URL+"/retention/testFolders/"+folder);
		//                runRemoteScript(HOST_NAME,USER_NAME, PASSWORD,HADOOP_LOCATION+"  dfs -mkdir  hdfs://"+HADOOP_URL+"/retention/testFolders/somethingRandom");
		//            }
	}        

	public static ArrayList<String> convertDatesToFolders(List<String> dateList,int skipInterval) throws Exception
	{
		logger.info("converting dates to folders....");
		ArrayList<String> folderList=new ArrayList<String>();

		for(String date:dateList)
		{
			for(int i=0;i<24;i+=skipInterval+1)
			{
				if(i<10)
				{
					folderList.add(date+"/0"+i);
				}
				else
				{
					folderList.add(date+"/"+i);
				}  
			}
		}

		return folderList;
	}

	public static List<String> addMinutesToCreatedFolders(List<String> folderList,int skipMinutes) throws Exception
	{
		logger.info("adding minutes to current folders.....");
		ArrayList<String> finalFolderList=new ArrayList<String>();

		if(skipMinutes==0){skipMinutes=1;}

		for(String date:folderList)
		{
			for(int i=0;i<60;i+=skipMinutes)
			{
				if(i<10)
				{
					finalFolderList.add(date+"/0"+i);
				}
				else
				{
					finalFolderList.add(date+"/"+i);
				}  
			}
		}

		return finalFolderList;
	}

	public static ArrayList<String> filterDataOnRetention(String feed,int time, String interval,DateTime endDate,List<String> inputData) throws Exception
	{
		String locationType="";
		String appender="";

		DateTimeFormatter formatter=DateTimeFormat.forPattern("yyyy/MM/dd/HH/mm");
		ArrayList<String> finalData=new ArrayList<String>();

		//determine what kind of data is there in the feed!
		JAXBContext feedContext=JAXBContext.newInstance(Feed.class);
		Feed feedObject=(Feed)feedContext.createUnmarshaller().unmarshal(new StringReader(feed));

		for(com.inmobi.qa.falcon.generated.feed.Location location:feedObject.getLocations().getLocation())
		{
			if(location.getType().equals(LocationType.DATA))
			{
				locationType=location.getPath();
			}
		}



		if(locationType.equalsIgnoreCase("") || locationType.equalsIgnoreCase(null))
		{
			throw new TestNGException("location type was not mentioned in your feed!");
		}

		if(locationType.equalsIgnoreCase("/retention/testFolders/${YEAR}/${MONTH}"))
		{
			//formatter=DateTimeFormat.forPattern("yyyy-MM"); 
			appender="/01/00/01";
		}
		else if(locationType.equalsIgnoreCase("/retention/testFolders/${YEAR}/${MONTH}/${DAY}"))
		{
			//formatter=DateTimeFormat.forPattern("yyyy-MM-dd");  
			//appender="-00-01";
			appender="/01"; //because we already take care of that!
		}
		else if(locationType.equalsIgnoreCase("/retention/testFolders/${YEAR}/${MONTH}/${DAY}/${HOUR}"))
		{
			//formatter=DateTimeFormat.forPattern("yyyy-MM-dd-HH"); 
			appender="/01";
		}
		else if(locationType.equalsIgnoreCase("/retention/testFolders/${YEAR}"))
		{
			//formatter=DateTimeFormat.forPattern("yyyy");
			appender="/01/01/00/01";
		}

		//convert the start and end date boundaries to the same format



		//end date is today's date
		String endLimit=formatter.print(endDate);
		String startLimit="";

		if(interval.equalsIgnoreCase("minutes"))
		{
			startLimit=formatter.print(new DateTime(endDate,DateTimeZone.UTC).minusMinutes(time));
		}
		else if(interval.equalsIgnoreCase("hours"))
		{
			startLimit=formatter.print(new DateTime(endDate,DateTimeZone.UTC).minusHours(time));
		}
		else if(interval.equalsIgnoreCase("days"))
		{
			startLimit=formatter.print(new DateTime(endDate,DateTimeZone.UTC).minusDays(time));
		}
		else if(interval.equalsIgnoreCase("months"))
		{
			//for(int i=0;i<time;i++)
			{
				//startLimit=formatter.print(new DateTime(endDate, DateTimeZone.UTC).minusMonths(time));
				startLimit=formatter.print(new DateTime(endDate, DateTimeZone.UTC).minusDays(31*time));
			}
		}


		//now to actually check!
		for(String testDate:inputData)
		{
			if(!testDate.equalsIgnoreCase("somethingRandom"))
			{
				//				if((testDate+appender).compareTo(startLimit)>=0 && (testDate+appender).compareTo(endLimit)<=0)
				if((testDate+appender).compareTo(startLimit)>=0)
				{
					finalData.add(testDate);
				}
			}
			else
			{
				finalData.add(testDate);
			}
		}

		return finalData;

	}



	public static List<String> getDailyDatesOnEitherSide(int interval,int skip) throws Exception
	{

		//get today's date
		DateTimeFormatter formatter=DateTimeFormat.forPattern("yyyy/MM/dd");


		DateTime today=new DateTime(DateTimeZone.UTC);
		logger.info("today is: "+today.toString());

		List<String> dates=new ArrayList<String>();
		dates.add(formatter.print(today));

		//first lets get all dates before today
		for(int backward=1;backward<=interval;backward+=skip+1)
		{
			dates.add(formatter.print(today.minusDays(backward)));  
		}

		//now the forward dates
		for(int i=1;i<=interval;i+=skip+1)
		{
			dates.add(formatter.print(today.plusDays(i)));
		}

		//logger.info("size of dates="+dates.size());
		//logger.info(dates.get(0));
		return dates;
	}

	public static List<String> getMonthlyDatesOnEitherSide(int interval,int skip) throws Exception
	{
		DateTimeFormatter formatter=DateTimeFormat.forPattern("yyyy/MM");
		//get today's date

		DateTime today=new DateTime(DateTimeZone.UTC);
		logger.info("today is: "+today.toString());

		List<String> dates=new ArrayList<String>();
		dates.add(formatter.print((today)));

		//first lets get all dates before today
		for(int backward=1;backward<=interval;backward+=skip+1)
		{
			dates.add(formatter.print(new LocalDate(today.minusMonths(backward))));  
		}

		//now the forward dates
		for(int i=1;i<=interval;i+=skip+1)
		{
			dates.add(formatter.print(new LocalDate(today.plusMonths(i))));
		}

		//logger.info("size of dates="+dates.size());
		//logger.info(dates.get(0));
		return dates;
	}


	public static List<String> getMinuteDatesOnEitherSide(DateTime startDate,DateTime endDate,int minuteSkip) throws Exception
	{
		DateTimeFormatter formatter=DateTimeFormat.forPattern("yyyy/MM/dd/HH/mm");
		//get today's date

		logger.info("generating data between "+formatter.print(startDate)+" and "+formatter.print(endDate));

		ArrayList<String> dates=new ArrayList<String>();



		while(!startDate.isAfter(endDate))
		{
			dates.add(formatter.print(startDate.plusMinutes(minuteSkip)));
			if(minuteSkip==0){minuteSkip=1;}
			startDate=startDate.plusMinutes(minuteSkip);
		}

		return dates;
	}

	public static List<String> getMinuteDatesOnEitherSide(int interval,int minuteSkip) throws Exception
	{
		DateTimeFormatter formatter=DateTimeFormat.forPattern("yyyy/MM/dd/HH/mm");
		//get today's date

		if(minuteSkip==0) {minuteSkip=1;}

		DateTime today=new DateTime(DateTimeZone.UTC);
		logger.info("today is: "+today.toString());

		ArrayList<String> dates=new ArrayList<String>();
		dates.add(formatter.print(today));

		//first lets get all dates before today
		for(int backward=1;backward<=interval;backward+=minuteSkip)
		{
			dates.add(formatter.print(today.minusMinutes(backward)));  
		}

		//now the forward dates
		for(int i=0;i<=interval;i+=minuteSkip)
		{
			dates.add(formatter.print(today.plusMinutes(i)));
		}

		//logger.info("size of dates="+dates.size());
		//logger.info(dates.get(0));
		return dates;
	}

	public static List<String> getMinuteDatesOnEitherSide(int interval,int daySkip,int minuteSkip) throws Exception
	{
		DateTimeFormatter formatter=DateTimeFormat.forPattern("yyyy/MM/dd/HH");
		//get today's date


		DateTime today=new DateTime(DateTimeZone.UTC);
		logger.info("today is: "+today.toString());

		ArrayList<String> dates=new ArrayList<String>();
		dates.add(formatter.print(today));

		//first lets get all dates before today
		for(int backward=1;backward<=interval;backward+=daySkip+1)
		{
			dates.add(formatter.print(today.minusDays(backward)));  
		}

		//now the forward dates
		for(int i=1;i<=interval;i+=daySkip+1)
		{
			dates.add(formatter.print(today.plusDays(i)));
		}

		//logger.info("size of dates="+dates.size());
		//logger.info(dates.get(0));
		return addMinutesToCreatedFolders(dates, minuteSkip);
	}

	public static List<String> getYearlyDatesOnEitherSide(int interval,int skip) throws Exception
	{
		DateTimeFormatter formatter=DateTimeFormat.forPattern("yyyy");
		//get today's date

		DateTime today=new DateTime(DateTimeZone.UTC);
		logger.info("today is: "+today.toString());

		List<String> dates=new ArrayList<String>();
		dates.add(formatter.print(new LocalDate(today)));

		//first lets get all dates before today
		for(int backward=1;backward<=interval;backward+=skip+1)
		{
			dates.add(formatter.print(new LocalDate(today.minusYears(backward))));  
		}

		//now the forward dates
		for(int i=1;i<=interval;i+=skip+1)
		{
			dates.add(formatter.print(new LocalDate(today.plusYears(i))));
		}

		//logger.info("size of dates="+dates.size());
		//logger.info(dates.get(0));
		return dates;
	}


	//writing command for creating folders in bulk
	private static void createHDFSFolders(String host_name,String user_name,String password,List<String> folderList) throws Exception
	{
		JSch jsch = new JSch();
		Session session = jsch.getSession(user_name, host_name, 22);
		//logger.info("Done");

		session.setUserInfo(new HardcodedUserInfo(password));
		Properties config = new Properties();
		config.setProperty("StrictHostKeyChecking", "no");
		session.setConfig(config);

		session.connect();

		Assert.assertTrue(session.isConnected(),"The session was not connected correctly!");

		folderList.add("somethingRandom");

		for(String folder:folderList)
		{

			ChannelExec channel = (ChannelExec) session.openChannel("exec");


			logger.info("executing the command..."+HADOOP_LOCATION+"  dfs -mkdir  hdfs://"+HADOOP_URL+"/retention/testFolders/"+folder);
			channel.setCommand(HADOOP_LOCATION+"  dfs -mkdir  hdfs://"+HADOOP_URL+"/retention/testFolders/"+folder);
			channel.connect();
			Assert.assertTrue(channel.isConnected(),"The channel was not connected correctly!");



			//Assert.assertTrue("The channel did not connect correctly for setting the path!",channel.isConnected());

			//logger.info("now reading the line....");

			//now to read output
			ArrayList<String> data = new ArrayList<String>();

			InputStream in = channel.getInputStream();
			//InputStream in =channel.getErrStream();

			Assert.assertTrue(channel.isConnected(),"The channel was not connected correctly!");

			BufferedReader r = new BufferedReader(new InputStreamReader(in));


			String line=new String();
			while (true) {

				while ((line=r.readLine())!=null) {
					//logger.info(line);
					data.add(line);

				}
				if (channel.isClosed()) {

					break;
				}

			}

			in.close();
			r.close();


			channel.disconnect();
		}

		session.disconnect();

	}

	public static void createHDFSFolders(PrismHelper prismHelper,List<String> folderList) throws Exception
	{
		Configuration conf=new Configuration();
		conf.set("fs.default.name","hdfs://"+prismHelper.getProcessHelper().getHadoopURL()+"");

		final FileSystem fs=FileSystem.get(conf);

		UserGroupInformation user = UserGroupInformation.createRemoteUser("hdfs");

		//            user.doAs(new PrivilegedExceptionAction<Boolean>)() {
		//            
		//            @Override
		//            public boolean run() throws Exception {
		//                return fs.delete(new Path("/retention/testFolders/"));
		//            }});
		//        
		//        }

		//            user.doAs(new PrivilegedExceptionAction<Boolean>() {
		//
		//                @Override
		//                public Boolean run() throws Exception {
		//                    return fs.delete(new Path("/retention/testFolders/"),true);
		//                }
		//            }); 


		folderList.add("somethingRandom");

		for(final String folder:folderList)
		{
			user.doAs(new PrivilegedExceptionAction<Boolean>() {

				@Override
				public Boolean run() throws Exception {
					logger.info("/retention/testFolders/"+folder);
					return fs.mkdirs(new Path("/retention/testFolders/"+folder));
				}
			}); 
		}
	}        

	public static void createHDFSFolders(List<String> folderList) throws Exception
	{
		Configuration conf=new Configuration();
		conf.set("fs.default.name","hdfs://"+HADOOP_URL+"");

		final FileSystem fs=FileSystem.get(conf);

		UserGroupInformation user = UserGroupInformation.createRemoteUser("hdfs");

		//            user.doAs(new PrivilegedExceptionAction<Boolean>)() {
		//            
		//            @Override
		//            public boolean run() throws Exception {
		//                return fs.delete(new Path("/retention/testFolders/"));
		//            }});
		//        
		//        }

		//            user.doAs(new PrivilegedExceptionAction<Boolean>() {
		//
		//                @Override
		//                public Boolean run() throws Exception {
		//                    return fs.delete(new Path("/retention/testFolders/"),true);
		//                }
		//            }); 


		folderList.add("somethingRandom");

		for(final String folder:folderList)
		{
			user.doAs(new PrivilegedExceptionAction<Boolean>() {

				@Override
				public Boolean run() throws Exception {
					logger.info("/retention/testFolders/"+folder);
					return fs.mkdirs(new Path("/retention/testFolders/"+folder));
				}
			}); 
		}
	}

	public static String readQueueLocationFromCluster(String cluster) throws Exception
	{
		JAXBContext clusterContext=JAXBContext.newInstance(com.inmobi.qa.falcon.generated.cluster.Cluster.class);
		Unmarshaller um=clusterContext.createUnmarshaller();

		com.inmobi.qa.falcon.generated.cluster.Cluster clusterObject=(com.inmobi.qa.falcon.generated.cluster.Cluster)um.unmarshal(new StringReader(cluster));

		for(Interface iface:clusterObject.getInterfaces().getInterface())
		{
			if(iface.getType().equals(Interfacetype.MESSAGING))
			{
				return iface.getEndpoint();
			}
		}

		return "tcp://mk-qa-63:61616?daemon=true";
	}

	public static String setProcessQueueName(String process,String queueName) throws Exception
	{
		JAXBContext context=JAXBContext.newInstance(Process.class);

		Unmarshaller um=context.createUnmarshaller();

		Process processObject=(Process)um.unmarshal(new StringReader(process));


		boolean found=false;
		for(Property prop:processObject.getProperties().getProperty())
		{
			//check if it is present
			if(prop.getName().equalsIgnoreCase("queueName"))
			{
				prop.setValue(queueName);
				found=true;
				break;
			}
		}

		if(!found)
		{
			Property property=new Property();
			property.setName("queueName");
			property.setValue(queueName);
			processObject.getProperties().getProperty().add(property);
		}

		return instanceUtil.processToString(processObject);
	}

	public static String setFeedProperty(String feed,String propertyName,String propertyValue) throws Exception
	{

		Feed feedObject=instanceUtil.getFeedElement(feed);

		boolean found=false;
		for(com.inmobi.qa.falcon.generated.feed.Property prop:feedObject.getProperties().getProperty())
		{
			//check if it is present
			if(prop.getName().equalsIgnoreCase(propertyName))
			{
				prop.setValue(propertyValue);
				found=true;
				break;
			}
		}

		if(!found)
		{
			com.inmobi.qa.falcon.generated.feed.Property property=new com.inmobi.qa.falcon.generated.feed.Property();
			property.setName(propertyName);
			property.setValue(propertyValue);
			feedObject.getProperties().getProperty().add(property);
		}


		return instanceUtil.feedElementToString(feedObject);

	}

	public static String setProcessProperty(String process,String propertyName,String propertyValue) throws Exception
	{

		Process processObject=instanceUtil.getProcessElement(process);

		boolean found=false;
		for(Property prop:processObject.getProperties().getProperty())
		{
			//check if it is present
			if(prop.getName().equalsIgnoreCase(propertyName))
			{
				prop.setValue(propertyValue);
				found=true;
				break;
			}
		}

		if(!found)
		{
			Property property=new Property();
			property.setName(propertyName);
			property.setValue(propertyValue);
			processObject.getProperties().getProperty().add(property);
		}


		return instanceUtil.processToString(processObject);

	}

	public static void validateDataFromFeedQueue(PrismHelper prismHelper,String feedName,List<HashMap<String,String>>queueData,List<String>expectedOutput,List<String>input) throws Exception
	{

		//logger.info("deleted data which has been received from messaging queue:");

		//            for(String data:queueData)
		//            {
		//                logger.info(data);
		//            }




		//just verify that each element in queue is same as deleted data!
		input.removeAll(expectedOutput);

		ArrayList<String> jobIds=getCoordinatorJobs(prismHelper,getCoordID(getOozieFeedJobStatus(feedName,"NONE",prismHelper).get(0)));

		//create queuedata folderList:
		ArrayList<String> deletedFolders=new ArrayList<String>();

		//for(String data:queueData)
		for(HashMap<String,String> data:queueData)
		{
			if(data!=null)
			{
				Assert.assertEquals(data.get("entityName"),feedName);
				//Assert.assertEquals(data.split("\\$")[1],feedName);
				String[] splitData=data.get("feedInstancePaths").split("testFolders/");
				deletedFolders.add(splitData[splitData.length-1]);
				Assert.assertEquals(data.get("operation"),"DELETE");
				Assert.assertEquals(data.get("workflowId"),jobIds.get(0));

				//verify other data also
				Assert.assertEquals(data.get("topicName"),"FALCON."+feedName);
				Assert.assertEquals(data.get("brokerImplClass"),"org.apache.activemq.ActiveMQConnectionFactory");
				Assert.assertEquals(data.get("status"),"SUCCEEDED");
				Assert.assertEquals(data.get("brokerUrl"),prismHelper.getFeedHelper().getActiveMQ());

			}
		}

		//now make sure queueData and input lists are same
		Assert.assertEquals(deletedFolders.size(),input.size(),"Output size is different than expected!");
		Assert.assertTrue(Arrays.deepEquals(input.toArray(new String[input.size()]),deletedFolders.toArray(new String[deletedFolders.size()])),"It appears that the data that is received from queue and the data deleted are not same!");
	}


	public static void CommonDataRetentionWorkflow(PrismHelper prismHelper,Bundle bundle,int time,String interval) throws Exception
	{
		//get Data created in the cluster
		List<String> initialData=Util.getHadoopData(prismHelper,Util.getInputFeedFromBundle(bundle));


		//IEntityManagerHelper feedHelper=EntityHelperFactory.getEntityHelper(ENTITY_TYPE.DATA);
		//schedule feed
		//feedHelper.schedule(bundle.getProcessHelper().getHostname(),Util.getInputFeedFromBundle(bundle));
		prismHelper.getFeedHelper().schedule(URLS.SCHEDULE_URL,Util.getInputFeedFromBundle(bundle));
		logger.info(prismHelper.getClusterHelper().getActiveMQ());
		logger.info(Util.readDatasetName(Util.getInputFeedFromBundle(bundle)));
		Consumer consumer=new Consumer("FALCON."+Util.readDatasetName(Util.getInputFeedFromBundle(bundle)),prismHelper.getClusterHelper().getActiveMQ());
		consumer.start();

		DateTime currentTime=new DateTime(DateTimeZone.UTC);

		String bundleId=Util.getCoordID(Util.getOozieFeedJobStatus(Util.readDatasetName(Util.getInputFeedFromBundle(bundle)),"NONE",prismHelper).get(0));


		//ArrayList<String> workflows=Util.getCoordinatorJobs(bundleId);
		ArrayList<String> workflows=getFeedRetentionJobs(prismHelper,bundleId);
		logger.info("got a workflow list of length:" +workflows.size());
		Collections.sort(workflows);

		for(String workflow:workflows)
		{
			logger.info(workflow);
		}

		if(null!=workflows && !workflows.isEmpty())
		{

			String workflowId=workflows.get(0);
			String status=getWorkflowInfo(prismHelper,workflowId);
			while(!(status.equalsIgnoreCase("KILLED") || status.equalsIgnoreCase("FAILED") || status.equalsIgnoreCase("SUCCEEDED")))
			{
				Thread.sleep(1000); 
				status=getWorkflowInfo(prismHelper,workflowId);
			}
		}

		//getstatus of the workflow

		//Thread.sleep(60000);

		//check the queue:
		consumer.stop();

		logger.info("deleted data which has been received from messaging queue:");
		for(HashMap<String,String> data:consumer.getMessageData())
		{
			logger.info("*************************************");
			for(String key:data.keySet())
			{
				logger.info(key+"="+data.get(key));
			}
			logger.info("*************************************");
		}

		//now look for cluster data

		ArrayList<String> finalData=Util.getHadoopData(prismHelper,Util.getInputFeedFromBundle(bundle));

		//now see if retention value was matched to as expected
		ArrayList<String> expectedOutput=Util.filterDataOnRetention(Util.getInputFeedFromBundle(bundle),time,interval, currentTime,initialData);

		logger.info("initial data in system was:");
		for(String line:initialData)
		{
			logger.info(line);
		}

		logger.info("system output is:");
		for(String line:finalData)
		{
			logger.info(line);
		}

		logger.info("actual output is:");
		for(String line:expectedOutput)
		{
			logger.info(line);
		}

		Util.validateDataFromFeedQueue(prismHelper,Util.readDatasetName(getInputFeedFromBundle(bundle)),consumer.getMessageData(),expectedOutput,initialData);

		Assert.assertEquals(finalData.size(), expectedOutput.size(),"sizes of outputs are different! please check");

		Assert.assertTrue(Arrays.deepEquals(finalData.toArray(new String[finalData.size()]),expectedOutput.toArray(new String[expectedOutput.size()]))); 
	}

	public static ArrayList<String> getOutputFeedsFromBundle(Bundle bundle) throws Exception
	{
		ArrayList<String> feedList=new ArrayList<String>();

		String processData=bundle.getProcessData();

		JAXBContext processContext= JAXBContext.newInstance(Process.class);
		Unmarshaller unmarshaller=processContext.createUnmarshaller();
		Process processObject=(Process)unmarshaller.unmarshal(new StringReader(processData));

		for(Output output:processObject.getOutputs().getOutput())
		{
			for(String feed:bundle.getDataSets())
			{
				if(Util.readDatasetName(feed).equalsIgnoreCase(output.getFeed()))
				{
					feedList.add(feed);
				}
			}
		}
		return feedList;

	}

	public static HashMap<String,String> getOutputFeedMapFromBundle(Bundle bundle) throws Exception
	{
		HashMap<String,String> feedMap=new HashMap<String, String>();

		String processData=bundle.getProcessData();

		JAXBContext processContext= JAXBContext.newInstance(Process.class);
		Unmarshaller unmarshaller=processContext.createUnmarshaller();
		Process processObject=(Process)unmarshaller.unmarshal(new StringReader(processData));

		for(Output output:processObject.getOutputs().getOutput())
		{
			for(String feed:bundle.getDataSets())
			{
				if(Util.readDatasetName(feed).equalsIgnoreCase(output.getFeed()))
				{
					feedMap.put(output.getName(),getFeedPath(feed));
				}
			}
		}
		return feedMap;

	}

	public static String getFeedPath(String feed) throws Exception
	{
		JAXBContext context=JAXBContext.newInstance(Feed.class);
		Unmarshaller um=context.createUnmarshaller();
		Feed feedObject=(Feed)um.unmarshal(new StringReader(feed));

		for(Location location:feedObject.getLocations().getLocation())
		{
			if(location.getType().equals(LocationType.DATA))
			{
				return location.getPath();
			}
		}

		return null;
	}

	public static String printCoordinator(COORDINATORAPP coordinator) throws Exception
	{
		JAXBContext context=JAXBContext.newInstance(COORDINATORAPP.class);
		Marshaller m=context.createMarshaller();

		StringWriter sw=new StringWriter();
		m.marshal(coordinator,sw);

		return sw.toString();

	}


	private static BufferedReader getErrorReader(java.lang.Process process) throws Exception
	{
		return new BufferedReader(new InputStreamReader(process.getErrorStream()));
	}

	private static BufferedReader getOutputReader(java.lang.Process process) throws Exception
	{
		return new BufferedReader(new InputStreamReader(process.getInputStream()));
	}


	public static String executeCommand(String command) throws Exception
	{
		Util.print("Command to be executed: "+command);
		StringBuilder errors=new StringBuilder();
		StringBuilder output=new StringBuilder();

		Runtime rt=Runtime.getRuntime();
		java.lang.Process proc=rt.exec(command);

		BufferedReader errorReader=getErrorReader(proc);
		BufferedReader consoleReader=getOutputReader(proc);

		String line="";
		while((line=errorReader.readLine())!=null)
		{
			logger.info(line);
			errors.append(line);
			errors.append("\n");
		}

		while((line=consoleReader.readLine())!=null)
		{
			logger.info(line);
			output.append(line);
			output.append("\n");
		}

		int exitVal=proc.waitFor();

		if(exitVal==0)
		{
			Util.print("Exceuted command output: "+output.toString());
			return output.toString().trim();
		}
		else
		{
			Util.print("Executed command error: "+ errors.toString());
			return errors.toString();
		}


	}



	public static void createHDFSPartitionFolders(List<String> folderList) throws Exception
	{
		//purge data first
		runRemoteScript(HOST_NAME,USER_NAME, PASSWORD,HADOOP_LOCATION+"  dfs -rmr" +
      "  hdfs://"+HADOOP_URL+"/partition/testFolders/", IDENTITY_FILE);

		Configuration conf=new Configuration();
		conf.set("fs.default.name","hdfs://"+HADOOP_URL+"");

		final FileSystem fs=FileSystem.get(conf);

		UserGroupInformation user = UserGroupInformation.createRemoteUser("hdfs");

		//            user.doAs(new PrivilegedExceptionAction<Boolean>)() {
		//            
		//            @Override
		//            public boolean run() throws Exception {
		//                return fs.delete(new Path("/retention/testFolders/"));
		//            }});
		//        
		//        }

		//            user.doAs(new PrivilegedExceptionAction<Boolean>() {
		//
		//                @Override
		//                public Boolean run() throws Exception {
		//                    return fs.delete(new Path("/retention/testFolders/"),true);
		//                }
		//            }); 

		String [] partitions=new String[]{"test1","test@2","test#!3","test5"};

		for(final String folder:folderList)
		{
			for(final String partition:partitions)
			{
				user.doAs(new PrivilegedExceptionAction<Boolean>() {

					@Override
					public Boolean run() throws Exception {
						//return fs.mkdirs(new Path("/partition/testFolders/"+folder+"/"+partition));
						return fs.mkdirs(new Path("/partition/testFolders/"+folder));
					}
				}); 
			}
		}

		Util.copyDataToFolders(folderList,"src/test/resources/OozieExampleInputData/normalInput","/partition/testFolders/");

	}

	public static void getCoordinator(final String processName) throws Exception
	{
		Configuration conf=new Configuration();
		conf.set("fs.default.name","hdfs://"+HADOOP_URL);

		final FileSystem fs=FileSystem.get(conf);

		UserGroupInformation user = UserGroupInformation.createRemoteUser("hdfs");

		FileStatus[] list=fs.listStatus(new Path("/projects/ivory/staging/ivory/workflows/process/"+processName));



	}

	public static String insertLateFeedValue(String feed,String delay,String delayUnit) throws Exception
	{
		JAXBContext context=JAXBContext.newInstance(Feed.class);
		Unmarshaller um=context.createUnmarshaller();
		Feed feedObject=(Feed)um.unmarshal(new StringReader(feed));



		String delayTime="";

		if(delayUnit.equalsIgnoreCase("hours"))
		{
			delayTime="hours("+delay+")";
		}
		else if(delayUnit.equalsIgnoreCase("minutes"))
		{
			delayTime="minutes("+delay+")";
		}
		else if(delayUnit.equalsIgnoreCase("days"))
		{
			delayTime="days("+delay+")";
		}
		else if(delayUnit.equalsIgnoreCase("months"))
		{
			delayTime="months("+delay+")";
		}

		feedObject.getLateArrival().setCutOff(new Frequency(delayTime));

		Marshaller m=context.createMarshaller();
		StringWriter sw=new StringWriter();

		m.marshal(feedObject,sw);

		return sw.toString();
	}

	public static void populateTestDataForLateFeedProcessing(PrismHelper prismHelper,List<String> folderList,String directory) throws Exception
	{
		createLateDataFolders(prismHelper,folderList);
		copyDataToFolders(prismHelper,folderList,directory);
	}

	public static void createLateDataFolders(PrismHelper prismHelper,List<String> folderList) throws Exception
	{
		logger.info("creating late data folders.....");
		Configuration conf=new Configuration();
		conf.set("fs.default.name","hdfs://"+prismHelper.getProcessHelper().getHadoopURL()+"");

		final FileSystem fs=FileSystem.get(conf);

		UserGroupInformation user = UserGroupInformation.createRemoteUser("hdfs");

		//            user.doAs(new PrivilegedExceptionAction<Boolean>)() {
		//            
		//            @Override
		//            public boolean run() throws Exception {
		//                return fs.delete(new Path("/retention/testFolders/"));
		//            }});
		//        
		//        }

		//            user.doAs(new PrivilegedExceptionAction<Boolean>() {
		//
		//                @Override
		//                public Boolean run() throws Exception {
		//                    return fs.delete(new Path("/retention/testFolders/"),true);
		//                }
		//            }); 


		folderList.add("somethingRandom");

		for(final String folder:folderList)
		{
			user.doAs(new PrivilegedExceptionAction<Boolean>() {

				@Override
				public Boolean run() throws Exception {
					return fs.mkdirs(new Path("/lateDataTest/testFolders/"+folder));

				}
			}); 
		}

		logger.info("created all late data folders.....");
	}        

	public static void createLateDataFolders(List<String> folderList) throws Exception
	{
		logger.info("creating late data folders.....");
		Configuration conf=new Configuration();
		conf.set("fs.default.name","hdfs://"+HADOOP_URL+"");

		final FileSystem fs=FileSystem.get(conf);

		UserGroupInformation user = UserGroupInformation.createRemoteUser("hdfs");

		//            user.doAs(new PrivilegedExceptionAction<Boolean>)() {
		//            
		//            @Override
		//            public boolean run() throws Exception {
		//                return fs.delete(new Path("/retention/testFolders/"));
		//            }});
		//        
		//        }

		//            user.doAs(new PrivilegedExceptionAction<Boolean>() {
		//
		//                @Override
		//                public Boolean run() throws Exception {
		//                    return fs.delete(new Path("/retention/testFolders/"),true);
		//                }
		//            }); 


		folderList.add("somethingRandom");

		for(final String folder:folderList)
		{
			user.doAs(new PrivilegedExceptionAction<Boolean>() {

				@Override
				public Boolean run() throws Exception {
					return fs.mkdirs(new Path("/lateDataTest/testFolders/"+folder));

				}
			}); 
		}

		logger.info("created all late data folders.....");
	}

	public static void copyDataToFolders(List<String> folderList,String directory,final String prefix) throws Exception
	{
		logger.info("copying data into folders....");

		Configuration conf=new Configuration();
		conf.set("fs.default.name","hdfs://"+HADOOP_URL);

		final FileSystem fs=FileSystem.get(conf);

		UserGroupInformation user = UserGroupInformation.createRemoteUser("hdfs");


		for(final String folder:folderList)
		{
			for(final File file:new File(directory).listFiles())
			{
				if(!file.isDirectory())
				{
					user.doAs(new PrivilegedExceptionAction<Boolean>() {

						@Override
						public Boolean run() throws Exception {

							fs.copyFromLocalFile(new Path(file.getAbsolutePath()),new Path(prefix+folder));
							return true;

						}
					});
				}
			}
		}

		logger.info("copied data into latedata folders....");
	}


	public static void copyDataToFolders(PrismHelper prismHelper,List<String> folderList,String directory) throws Exception
	{
		logger.info("copying data into folders....");

		Configuration conf=new Configuration();
		conf.set("fs.default.name","hdfs://"+prismHelper.getClusterHelper().getHadoopURL()+"");

		final FileSystem fs=FileSystem.get(conf);

		UserGroupInformation user = UserGroupInformation.createRemoteUser("hdfs");


		for(final String folder:folderList)
		{
			for(final File file:new File(directory).listFiles())
			{
				if(!file.isDirectory())
				{
					user.doAs(new PrivilegedExceptionAction<Boolean>() {

						@Override
						public Boolean run() throws Exception {

							fs.copyFromLocalFile(new Path(file.getAbsolutePath()),new Path("/lateDataTest/testFolders/"+folder));
							return true;

						}
					});
				}
			}
		}

		logger.info("copied data into latedata folders....");
	}



	public static DateTime getSystemDate(PrismHelper prismHelper) throws Exception {

		DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy'-'MM'-'dd'T'HH':'mm'Z'");


		return fmt.parseDateTime(runRemoteScript(prismHelper.getClusterHelper()
      .getQaHost(), prismHelper.getClusterHelper().getUsername(),
      prismHelper.getClusterHelper().getPassword(),"date '+%Y-%m-%dT%H:%MZ'",
      prismHelper.getClusterHelper().getIdentityFile()).get(0));

	}

	public static DateFormat getIvoryDateFormat() {
		return new SimpleDateFormat("yyyy'-'MM'-'dd'T'HH':'mm'Z'");
	}



	public static ArrayList<String> getBundles(String processName) throws Exception {
		return runRemoteScript(HOST_NAME, USER_NAME, PASSWORD,
      OOZIE_LOCATION+"/oozie jobs -oozie "+OOZIE_URL+"  -jobtype bundle " +
        "-localtime -filter name=FALCON_PROCESS_"+processName+"|grep 000|awk " +
        "'{print $1}'", IDENTITY_FILE);
	}

	public static ArrayList<String> getBundles(String processName,String entityType) throws Exception {

		return runRemoteScript(HOST_NAME, USER_NAME, PASSWORD,
      OOZIE_LOCATION+"/oozie jobs -oozie "+OOZIE_URL+"  -jobtype bundle " +
        "-localtime -filter name=FALCON_PROCESS_"+processName+"|grep 000|awk " +
        "'{print $1}'", IDENTITY_FILE);
	}

	public static ArrayList<String> getBundles(PrismHelper coloHelper,String entityName,String entityType) throws Exception {

		if(entityType.equalsIgnoreCase("feed"))
		{
			return runRemoteScript(coloHelper.getFeedHelper().getQaHost(),
        coloHelper.getFeedHelper().getUsername(), coloHelper.getFeedHelper()
        .getPassword(),coloHelper.getFeedHelper().getOozieLocation()+"/oozie " +
        "jobs -oozie "+coloHelper.getFeedHelper().getOozieURL()+"  -jobtype " +
        "bundle -localtime -filter name=FALCON_FEED_"+entityName+"|grep " +
        "000|awk '{print $1}'", coloHelper.getFeedHelper().getIdentityFile());

		}
		else
		{
			return runRemoteScript(coloHelper.getFeedHelper().getQaHost(), coloHelper.getFeedHelper().getUsername(), coloHelper.getFeedHelper().getPassword(),coloHelper.getFeedHelper().getOozieLocation()+"/oozie jobs -oozie "+coloHelper.getFeedHelper().getOozieURL()+"  -jobtype bundle -localtime -filter name=FALCON_PROCESS_"+entityName+"|grep 000|awk '{print $1}'", coloHelper.getFeedHelper().getIdentityFile());
		}
	}

	public static String setFeedPathValue(String feed,String pathValue) throws Exception
	{
		JAXBContext feedContext=JAXBContext.newInstance(Feed.class);
		Feed feedObject=(Feed)feedContext.createUnmarshaller().unmarshal(new StringReader(feed));

		//set the value
		for(Location location: feedObject.getLocations().getLocation())
		{
			if(location.getType().equals(LocationType.DATA))
			{
				location.setPath(pathValue);
			}
		}

		StringWriter feedWriter =new StringWriter();
		feedContext.createMarshaller().marshal(feedObject,feedWriter);
		return feedWriter.toString();
	}


	public static DateTime getStartTimeForDefaultCoordinator(String bundleID) throws Exception
	{
		ArrayList<DateTime> startTimes=new ArrayList<DateTime>();

		XOozieClient oozieClient=new XOozieClient(OOZIE_URL);
		BundleJob bundleJob = oozieClient.getBundleJobInfo(bundleID);
		CoordinatorJob jobInfo = oozieClient.getCoordJobInfo(bundleJob.getCoordinators().get(0).getId());
		for(CoordinatorJob job:bundleJob.getCoordinators())
		{
			if(job.getAppName().contains("DEFAULT"))
			{
				return new DateTime(job.getNextMaterializedTime(),DateTimeZone.UTC);
			}

		}

		return null;
	}

	public static ArrayList<DateTime> getStartTimeForRunningCoordinators(PrismHelper prismHelper,String bundleID) throws Exception
	{
		ArrayList<DateTime> startTimes=new ArrayList<DateTime>();

		XOozieClient oozieClient=new XOozieClient(prismHelper.getClusterHelper().getOozieURL());
		BundleJob bundleJob = oozieClient.getBundleJobInfo(bundleID);
		CoordinatorJob jobInfo = oozieClient.getCoordJobInfo(bundleJob.getCoordinators().get(0).getId());


		for(CoordinatorJob job:bundleJob.getCoordinators())
		{

			if(job.getAppName().contains("DEFAULT"))
			{
				//logger.info(job.getAppName());

				jobInfo=oozieClient.getCoordJobInfo(job.getId());

				for(CoordinatorAction action:jobInfo.getActions())
				{
					DateTime temp=new DateTime(action.getCreatedTime(),DateTimeZone.UTC);
					logger.info(temp);
					startTimes.add(temp);
				}
			}

			Collections.sort(startTimes);

			if(startTimes!=null && !(startTimes.isEmpty()))
			{
				return startTimes;
			}
		}

		return null;
	}

	public static String getBundleStatus(PrismHelper prismHelper,String bundleId) throws Exception
	{
		XOozieClient oozieClient=new XOozieClient(prismHelper.getClusterHelper().getOozieURL());
		BundleJob bundleJob = oozieClient.getBundleJobInfo(bundleId);
		return bundleJob.getStatus().toString();
	}

	public static String findFolderBetweenGivenTimeStamps(DateTime startTime,DateTime endTime,List<String> folderList) throws Exception
	{
		DateTimeFormatter formatter=DateTimeFormat.forPattern("yyyy/MM/dd/HH/mm");

		for(String folder:folderList)
		{
			if(folder.compareTo(formatter.print(startTime))>=0 && folder.compareTo(formatter.print(endTime))<=0)
			{
				return folder;
			}
		}
		return null;
	}





	public static boolean isOver(String bundleId,DateTime timeStamp) throws Exception
	{
		DateTimeFormatter formatter=DateTimeFormat.forPattern("yyyy/MM/dd/HH/mm");
		XOozieClient oozieClient=new XOozieClient(OOZIE_URL);
		BundleJob bundleJob = oozieClient.getBundleJobInfo(bundleId);
		CoordinatorJob jobInfo = oozieClient.getCoordJobInfo(bundleJob.getCoordinators().get(0).getId());


		for(CoordinatorJob job:bundleJob.getCoordinators())
		{

			if(job.getAppName().contains("DEFAULT"))
			{
				//logger.info(job.getAppName());

				jobInfo=oozieClient.getCoordJobInfo(job.getId());

				for(CoordinatorAction action:jobInfo.getActions())
				{
					//logger.info("comparing" + formatter.print(new DateTime(action.getNominalTime(),DateTimeZone.UTC)) +" and "+formatter.print(timeStamp)+":" );
					if(formatter.print(new DateTime(action.getNominalTime(),DateTimeZone.UTC)).equals(formatter.print(timeStamp)))
					{
						if(action.getStatus().equals(CoordinatorAction.Status.SUCCEEDED) || action.getStatus().equals(CoordinatorAction.Status.FAILED) || action.getStatus().equals(CoordinatorAction.Status.KILLED))
						{
							return true;
						}
						else
						{
							//logger.info("This instance" +action.getExternalId()+ "has not ended yet.....");
						}
					}
				}
			}


		}

		return false;

	}

	//    	public static boolean isOver(String processName,DateTime time) throws Exception
	//    	{
	//    		
	//    		DateTimeFormatter formatter= DateTimeFormat.forPattern("yyyy-MM-dd/HH:mm");
	//    		ProcessEntityHelperImpl processHelper=new ProcessEntityHelperImpl();
	//    		ProcessInstancesResult r  = processHelper.getProcessInstanceStatus(processName,"?start="+formatter.print(time).replace('/','T')+"Z");
	//    		
	//    		if(null==r.getInstances()) {return false;}
	//    		
	//    		WorkflowStatus status=r.getInstances()[0].getStatus();
	//    		
	//    		
	//    		if(status.equals(WorkflowStatus.FAILED) || status.equals(WorkflowStatus.KILLED) || status.equals(WorkflowStatus.SUCCEEDED))
	//    		{
	//    			return true;
	//    		}
	//    		return false;
	//    	}

	public static void HDFSCleanup(String hdfsPath) throws Exception
	{
		runRemoteScript(HOST_NAME,USER_NAME, PASSWORD,HADOOP_LOCATION+"  dfs -rmr" +
      "  hdfs://"+HADOOP_URL+"/"+hdfsPath, IDENTITY_FILE);
	}

	public static void HDFSCleanup(PrismHelper prismHelper,String hdfsPath) throws Exception
	{
		runRemoteScript(prismHelper.getProcessHelper().getQaHost(),
      prismHelper.getProcessHelper().getUsername(),
      prismHelper.getProcessHelper().getPassword(),
      prismHelper.getProcessHelper().getHadoopLocation()+"  dfs -rmr  " +
        "hdfs://"+prismHelper.getProcessHelper().getHadoopURL()+"/"+hdfsPath,
      prismHelper.getProcessHelper().getIdentityFile());
	}        

	//	public static void lateDataReplenish(int interval,int daySkip,int minuteSkip) throws Exception
	//	{
	//		List<String> folderData=Util.getMinuteDatesOnEitherSide(interval,minuteSkip);
	//		/*List<String> folderData=Util.getDailyDatesOnEitherSide(interval,daySkip);
	//    		List<String> hourlyData=Util.convertDatesToFolders(folderData,0);
	//    		List<String> finalData=Util.addMinutesToCreatedFolders(hourlyData,minuteSkip);*/ 
	//
	//		Util.createLateDataFolders(folderData);
	//		Util.copyDataToFolders(folderData,"src/test/resources/OozieExampleInputData/normalInput");
	//	}

	public static void lateDataReplenish(PrismHelper prismHelper,int interval,int daySkip,int minuteSkip) throws Exception
	{
		List<String> folderData=Util.getMinuteDatesOnEitherSide(interval,minuteSkip);
		/*List<String> folderData=Util.getDailyDatesOnEitherSide(interval,daySkip);
    		List<String> hourlyData=Util.convertDatesToFolders(folderData,0);
    		List<String> finalData=Util.addMinutesToCreatedFolders(hourlyData,minuteSkip);*/ 

		Util.createLateDataFolders(prismHelper,folderData);
		Util.copyDataToFolders(prismHelper,folderData,"src/test/resources/OozieExampleInputData/normalInput");
	}        


	public static void lateDataReplenish(String baseFolder,int interval,int daySkip,int minuteSkip) throws Exception
	{
		List<String> folderData=Util.getMinuteDatesOnEitherSide(interval,minuteSkip);
		/*List<String> folderData=Util.getDailyDatesOnEitherSide(interval,daySkip);
    		List<String> hourlyData=Util.convertDatesToFolders(folderData,0);
    		List<String> finalData=Util.addMinutesToCreatedFolders(hourlyData,minuteSkip);*/ 

		Util.createLateDataFolders(folderData);
		Util.copyDataToFolders(baseFolder,folderData,"src/test/resources/OozieExampleInputData/normalInput/_SUCCESS","src/test/resources/OozieExampleInputData/normalInput/log_01.txt");
	}


	public static void lateDataReplenish(String baseFolder,int interval,int daySkip,int minuteSkip,String ... files) throws Exception
	{
		List<String> folderData=Util.getMinuteDatesOnEitherSide(interval,minuteSkip);
		/*List<String> folderData=Util.getDailyDatesOnEitherSide(interval,daySkip);
    		List<String> hourlyData=Util.convertDatesToFolders(folderData,0);
    		List<String> finalData=Util.addMinutesToCreatedFolders(hourlyData,minuteSkip);*/ 

		Util.createLateDataFolders(folderData);
		Util.copyDataToFolders(baseFolder,folderData,files);
	}

	public static void lateDataReplenish(PrismHelper prismHelper,String baseFolder,int interval,int daySkip,int minuteSkip,String ... files) throws Exception
	{
		List<String> folderData=Util.getMinuteDatesOnEitherSide(interval,minuteSkip);
		/*List<String> folderData=Util.getDailyDatesOnEitherSide(interval,daySkip);
    		List<String> hourlyData=Util.convertDatesToFolders(folderData,0);
    		List<String> finalData=Util.addMinutesToCreatedFolders(hourlyData,minuteSkip);*/ 

		Util.createLateDataFolders(prismHelper,folderData);
		Util.copyDataToFolders(prismHelper,baseFolder,folderData,files);
	}        

	public static void injectMoreData(PrismHelper prismHelper,final String remoteLocation,String localLocation) throws Exception
	{
		Configuration conf=new Configuration();
		conf.set("fs.default.name","hdfs://"+prismHelper.getClusterHelper().getHadoopURL()+"");

		final FileSystem fs=FileSystem.get(conf);

		UserGroupInformation user = UserGroupInformation.createRemoteUser("hdfs");


		File [] files=new File(localLocation).listFiles();
		for(final File file:files)
		{
			if(!file.isDirectory())
			{
				user.doAs(new PrivilegedExceptionAction<Boolean>() {

					@Override
					public Boolean run() throws Exception {

						String path="/lateDataTest/testFolders/"+remoteLocation+"/"+System.currentTimeMillis()/1000+"/";
						System.out.println("inserting data@ "+path);
						fs.copyFromLocalFile(new Path(file.getAbsolutePath()),new Path(path));
						return true;

					}
				}); 
			}
		}

	}

	public static void injectMoreData(final String remoteLocation,String localLocation,final String baseFolder) throws Exception
	{
		Configuration conf=new Configuration();
		conf.set("fs.default.name","hdfs://"+HADOOP_URL);

		final FileSystem fs=FileSystem.get(conf);

		UserGroupInformation user = UserGroupInformation.createRemoteUser("hdfs");


		File [] files=new File(localLocation).listFiles();
		for(final File file:files)
		{
			if(!file.isDirectory())
			{
				user.doAs(new PrivilegedExceptionAction<Boolean>() {

					@Override
					public Boolean run() throws Exception {

						fs.copyFromLocalFile(new Path(file.getAbsolutePath()),new Path(baseFolder+"/"+remoteLocation+"/"));
						return true;

					}
				}); 
			}
		}

	}


	public static String getInputFeedNameFromBundle(Bundle b) throws Exception {
		String feedData=getInputFeedFromBundle(b);

		JAXBContext processContext= JAXBContext.newInstance(Feed.class);
		Unmarshaller unmarshaller=processContext.createUnmarshaller();
		Feed feedObject=(Feed)unmarshaller.unmarshal(new StringReader(feedData));

		return feedObject.getName();
	}
	public static void lateDataReplenish(int interval,int daySkip,int minuteSkip,String folderPrefix) throws Exception
	{
		List<String> folderPaths=Util.getMinuteDatesOnEitherSide(interval,minuteSkip);
		Util.print("folderData: "+folderPaths.toString());
		/*List<String> folderData=Util.getDailyDatesOnEitherSide(interval,daySkip);
    		List<String> hourlyData=Util.convertDatesToFolders(folderData,0);
    		List<String> finalData=Util.addMinutesToCreatedFolders(hourlyData,minuteSkip);*/ 

		Util.createLateDataFolders(folderPaths,folderPrefix);
		Util.copyDataToFolders(folderPrefix,folderPaths,"src/test/resources/OozieExampleInputData/normalInput/_SUCCESS","src/test/resources/OozieExampleInputData/normalInput/log_01.txt");
	}


	public static void lateDataReplenish(PrismHelper prismHelper,int interval,int daySkip,int minuteSkip,String folderPrefix) throws Exception
	{
		List<String> folderPaths=Util.getMinuteDatesOnEitherSide(interval,minuteSkip);
		Util.print("folderData: "+folderPaths.toString());
		/*List<String> folderData=Util.getDailyDatesOnEitherSide(interval,daySkip);
    		List<String> hourlyData=Util.convertDatesToFolders(folderData,0);
    		List<String> finalData=Util.addMinutesToCreatedFolders(hourlyData,minuteSkip);*/ 

		Util.createLateDataFolders(prismHelper,folderPaths,folderPrefix);
		Util.copyDataToFolders(prismHelper,folderPrefix,folderPaths,"src/test/resources/OozieExampleInputData/normalInput/_SUCCESS","src/test/resources/OozieExampleInputData/normalInput/log_01.txt");
	}

	public static void createLateDataFolders(PrismHelper prismHelper,List<String> folderList,final String FolderPrefix) throws Exception
	{
		Configuration conf=new Configuration();
		conf.set("fs.default.name","hdfs://"+prismHelper.getProcessHelper().getHadoopURL()+"");

		final FileSystem fs=FileSystem.get(conf);

		UserGroupInformation user = UserGroupInformation.createRemoteUser("hdfs");

		for(final String folder:folderList)
		{
			user.doAs(new PrivilegedExceptionAction<Boolean>() {

				@Override
				public Boolean run() throws Exception {
					//Util.print("creatingHDFSPath: "+FolderPrefix+folder);
					return fs.mkdirs(new Path(FolderPrefix+folder));

				}
			}); 
		}
	}

	public static void createLateDataFolders(List<String> folderList,final String FolderPrefix) throws Exception
	{
		Configuration conf=new Configuration();
		conf.set("fs.default.name","hdfs://"+HADOOP_URL+"");

		final FileSystem fs=FileSystem.get(conf);

		UserGroupInformation user = UserGroupInformation.createRemoteUser("hdfs");

		for(final String folder:folderList)
		{
			user.doAs(new PrivilegedExceptionAction<Boolean>() {

				@Override
				public Boolean run() throws Exception {
					//Util.print("creatingHDFSPath: "+FolderPrefix+folder);
					return fs.mkdirs(new Path(FolderPrefix+folder));

				}
			}); 
		}
	}

	public static void copyDataToFolders(PrismHelper prismHelper,final String folderPrefix,List<String> folderList,String ... fileLocations) throws Exception
	{
		Configuration conf=new Configuration();
		conf.set("fs.default.name","hdfs://"+prismHelper.getProcessHelper().getHadoopURL());

		final FileSystem fs=FileSystem.get(conf);

		UserGroupInformation user = UserGroupInformation.createRemoteUser("hdfs");



		for(final String folder:folderList)
		{

			String folder_space = folder.replaceAll("/","_");
			File f = new File("src/test/resources/OozieExampleInputData/normalInput/"+folder_space+".txt");
			if(!f.exists()){
				f.createNewFile();
				//System.out.println("New file \"myfile.txt\" has been created to the current directory");
			}


			FileWriter fr = new FileWriter(f);
			fr.append("folder");
			fr.close();
			fs.copyFromLocalFile(new Path(f.getAbsolutePath()),new Path(folderPrefix+folder));
			f.delete();


			for(final String file:fileLocations)
			{
				user.doAs(new PrivilegedExceptionAction<Boolean>() {

					@Override
					public Boolean run() throws Exception {
						logger.info("copying  "+file+" to "+folderPrefix+folder);
						fs.copyFromLocalFile(new Path(file),new Path(folderPrefix+folder));
						return true;

					}
				}); 
			}
		}
	}        

	public static void copyDataToFolders(final String folderPrefix,List<String> folderList,String ... fileLocations) throws Exception
	{
		Configuration conf=new Configuration();
		conf.set("fs.default.name","hdfs://"+HADOOP_URL);

		final FileSystem fs=FileSystem.get(conf);

		UserGroupInformation user = UserGroupInformation.createRemoteUser("hdfs");



		for(final String folder:folderList)
		{
			for(final String file:fileLocations)
			{
				user.doAs(new PrivilegedExceptionAction<Boolean>() {

					@Override
					public Boolean run() throws Exception {
						//logger.info("copying  "+file+" to "+folderPrefix+folder);
						fs.copyFromLocalFile(new Path(file),new Path(folderPrefix+folder));
						return true;

					}
				}); 
			}
		}
	}
	public static String getFeedName(String feedData) throws Exception
	{
		JAXBContext processContext= JAXBContext.newInstance(Feed.class);
		Unmarshaller unmarshaller=processContext.createUnmarshaller();
		Feed feedObject=(Feed)unmarshaller.unmarshal(new StringReader(feedData));
		return feedObject.getName();
	}



	public static String getOutputFeedFromBundle(Bundle bundle) throws Exception {
		String processData=bundle.getProcessData();

		JAXBContext processContext= JAXBContext.newInstance(Process.class);
		Unmarshaller unmarshaller=processContext.createUnmarshaller();
		Process processObject=(Process)unmarshaller.unmarshal(new StringReader(processData));

		for(Output output:processObject.getOutputs().getOutput())
		{
			for(String feed:bundle.getDataSets())
			{
				if(Util.readDatasetName(feed).equalsIgnoreCase(output.getFeed()))
				{
					return feed;
				}
			}
		}
		return null;
	}

	public static String getOutputFeedNameFromBundle(Bundle b) throws Exception {
		return Util.getFeedName(Util.getOutputFeedFromBundle(b));
	}

	public static String setFeedName(String outputFeed, String newName) throws Exception {
		JAXBContext feedContext=JAXBContext.newInstance(Feed.class);
		Feed feedObject=(Feed)feedContext.createUnmarshaller().unmarshal(new StringReader(outputFeed));

		//set the value
		feedObject.setName(newName);
		StringWriter feedWriter =new StringWriter();
		feedContext.createMarshaller().marshal(feedObject,feedWriter);
		return feedWriter.toString().trim();
	}



	public static String setProcessOutputFeed(String process,
			String outputFeed) throws Exception {
		JAXBContext processContext=JAXBContext.newInstance(Process.class);
		Process processObject=(Process)processContext.createUnmarshaller().unmarshal(new StringReader(process));

		//set the value
		processObject.getOutputs().getOutput().get(0).setFeed(outputFeed);
		StringWriter processWriter =new StringWriter();
		processContext.createMarshaller().marshal(processObject,processWriter);
		return processWriter.toString();
	}



	public static boolean allRelevantWorkflowsAreOver(String insertionFolder,String ... bundleIds) throws Exception
	{
		boolean finished=true;

		XOozieClient oozieClient=new XOozieClient(OOZIE_URL);

		for(String bundleId:bundleIds)
		{

			BundleJob bundleJob = oozieClient.getBundleJobInfo(bundleId);

			DateTimeFormatter formatter=DateTimeFormat.forPattern("yyyy/MM/dd/HH/mm");
			List<String> actualNominalTimes=new ArrayList<String>();

			for(CoordinatorJob job:bundleJob.getCoordinators())
			{
				if(job.getAppName().contains("DEFAULT"))
				{

					CoordinatorJob coordJob=oozieClient.getCoordJobInfo(job.getId());


					for(CoordinatorAction action:coordJob.getActions())
					{
						CoordinatorAction actionMan=oozieClient.getCoordActionInfo(action.getId());

						if(actionMan.getRunConf().contains(insertionFolder))
						{
							if((actionMan.getStatus().equals(CoordinatorAction.Status.SUCCEEDED)) || actionMan.getStatus().equals(CoordinatorAction.Status.KILLED) || actionMan.getStatus().equals(CoordinatorAction.Status.FAILED))
							{
								finished&=true; 
							}
							else
							{
								finished&=false;
							}
						}
					}
				}
			}
		}

		return finished;
	}

	public static ArrayList<String> getFeedRetentionJobs(PrismHelper prismHelper,String bundleID) throws Exception
	{
		ArrayList<String> jobIds=new ArrayList<String>();
		XOozieClient oozieClient=new XOozieClient(prismHelper.getFeedHelper().getOozieURL());
		BundleJob bundleJob = oozieClient.getBundleJobInfo(bundleID);
		CoordinatorJob jobInfo = oozieClient.getCoordJobInfo(bundleJob.getCoordinators().get(0).getId());

		while(jobInfo.getActions().isEmpty())
		{
			//keep dancing
			jobInfo=oozieClient.getCoordJobInfo(bundleJob.getCoordinators().get(0).getId());
		}

		logger.info("got coordinator jobInfo array of length:"+jobInfo.getActions());
		for(CoordinatorAction action:jobInfo.getActions())
		{
			logger.info(action.getId());
		}
		for(CoordinatorAction action:jobInfo.getActions())
		{
			CoordinatorAction actionInfo=oozieClient.getCoordActionInfo(action.getId());

			while(!actionInfo.getStatus().equals(CoordinatorAction.Status.SUCCEEDED))
			{
				//keep waiting till eternity. this can be dangerous :|
				actionInfo=oozieClient.getCoordActionInfo(action.getId());
			}
			//if(actionInfo.getStatus().equals(CoordinatorAction.Status.SUCCEEDED))
			{

				jobIds.add(action.getId());
			}
		}



		return jobIds;

	}

	public static void verifyBundleDeletion(Bundle bundle,PrismHelper ... prismHelper) throws Exception
	{

		for(PrismHelper helper:prismHelper)
		{
			ArrayList<String> processList=runRemoteScript(helper.getFeedHelper()
        .getQaHost(),helper.getFeedHelper().getUsername(),
        helper.getFeedHelper().getPassword(),helper.getFeedHelper()
        .getHadoopLocation()+"  fs -ls hdfs://"+helper.getFeedHelper()
        .getHadoopURL()+"/projects/ivory/staging/ivory/workflows/process | " +
        "awk '{print $8}'", helper.getFeedHelper().getIdentityFile());

			//make sure process bundle is not there
			Assert.assertFalse(processList.contains("/projects/ivory/staging/ivory/workflows/process/"+Util.readEntityName(bundle.getProcessData())),"Process "+Util.readEntityName(bundle.getProcessData())+" did not have its bundle removed!!!!");

			//make sure feed bundle is not there
			ArrayList<String> feedList=runRemoteScript(helper.getFeedHelper().getQaHost(),helper.getFeedHelper().getUsername(), helper.getFeedHelper().getPassword(),helper.getFeedHelper().getHadoopLocation()+"  fs -ls hdfs://"+helper.getFeedHelper().getHadoopURL()+"/projects/ivory/staging/ivory/workflows/feed | awk '{print $8}'", helper.getFeedHelper().getIdentityFile());

			for(String data:bundle.getDataSets())
			{
				Assert.assertFalse(feedList.contains("/projects/ivory/staging/ivory/workflows/feed/"+Util.readDatasetName(data)),"Feed "+Util.readDatasetName(data)+" did not have its bundle removed!!!!");
			}
		}
	}

	public static void verifyProcessDeletion(String process,PrismHelper ... prismHelper) throws Exception
	{
		for(PrismHelper helper:prismHelper)
		{
			ArrayList<String> processList=runRemoteScript(helper.getFeedHelper().getQaHost(),helper.getFeedHelper().getUsername(),helper.getFeedHelper().getPassword(),helper.getFeedHelper().getHadoopLocation()+"  fs -ls hdfs://"+helper.getFeedHelper().getHadoopURL()+"/projects/ivory/staging/ivory/workflows/process | awk '{print $8}'", helper.getFeedHelper().getIdentityFile());

			//make sure process bundle is not there
			Assert.assertFalse(processList.contains("/projects/ivory/staging/ivory/workflows/process/"+Util.readEntityName(process)),"Process "+Util.readEntityName(process)+" did not have its bundle removed!!!!");
		}
	}

	public static void verifyFeedDeletion(String feed,PrismHelper ... prismHelper) throws Exception
	{
		for(PrismHelper helper:prismHelper)
		{
			//make sure feed bundle is not there
			ArrayList<String> feedList=runRemoteScript(helper.getFeedHelper().getQaHost(),helper.getFeedHelper().getUsername(),helper.getFeedHelper().getPassword(),helper.getFeedHelper().getHadoopLocation()+"  fs -ls hdfs://"+helper.getFeedHelper().getHadoopURL()+"/projects/ivory/staging/ivory/workflows/feed | awk '{print $8}'", helper.getFeedHelper().getIdentityFile());

			Assert.assertFalse(feedList.contains("/projects/ivory/staging/ivory/workflows/feed/"+Util.readDatasetName(feed)),"Feed "+Util.readDatasetName(feed)+" did not have its bundle removed!!!!");
		}

	}

	public static void waitTillCertainPercentageOfProcessIsOver(String bundleId,int percentage, CoordinatorAction.Status s) throws Exception
	{

		CoordinatorJob defaultCoordinator=getDefaultOozieCoord(bundleId);

		while(defaultCoordinator.getStatus().equals(CoordinatorJob.Status.PREP))
		{
			defaultCoordinator=getDefaultOozieCoord(bundleId);
		}

		int totalCount=defaultCoordinator.getActions().size();

		int percentageConversion=(percentage*totalCount)/100;

		while(true && percentageConversion>0)
		{
			Thread.sleep(30000);
			int doneBynow=0;
			for(CoordinatorAction action:defaultCoordinator.getActions())
			{
				CoordinatorAction actionInfo=getOozieActionInfo(action.getId());
				if(actionInfo.getStatus().equals(s))
				{
					doneBynow++;
					if(doneBynow==percentageConversion)
					{
						return; 
					}
				}
			}
		}
	}


	public static CoordinatorJob getDefaultOozieCoord(String bundleId) throws Exception
	{
		XOozieClient client=new XOozieClient(OOZIE_URL);
		BundleJob bundlejob=client.getBundleJobInfo(bundleId); 

		for(CoordinatorJob coord:bundlejob.getCoordinators())
		{
			if(coord.getAppName().contains("DEFAULT"))
			{
				return client.getCoordJobInfo(coord.getId());
			}
		}
		return null;
	}     

	public static CoordinatorJob getDefaultOozieCoord(String bundleId,boolean isFeed) throws Exception
	{
		XOozieClient client=new XOozieClient(OOZIE_URL);
		BundleJob bundlejob=client.getBundleJobInfo(bundleId); 

		for(CoordinatorJob coord:bundlejob.getCoordinators())
		{
			if(coord.getAppName().contains("DEFAULT"))
			{
				return client.getCoordJobInfo(coord.getId());
			}
		}
		return null;
	}             

	public static CoordinatorJob getDefaultOozieCoord(PrismHelper prismHelper,String bundleId) throws Exception
	{
		XOozieClient client=new XOozieClient(prismHelper.getClusterHelper().getOozieURL());
		BundleJob bundlejob=client.getBundleJobInfo(bundleId); 

		for(CoordinatorJob coord:bundlejob.getCoordinators())
		{
			if(coord.getAppName().contains("DEFAULT"))
			{
				return client.getCoordJobInfo(coord.getId());
			}
		}
		return null;
	}             

	public static CoordinatorAction getOozieActionInfo(String actionId) throws Exception
	{
		XOozieClient client=new XOozieClient(OOZIE_URL);
		return client.getCoordActionInfo(actionId); 
	}


	public static Object[][] readAvailabilityBUndle() throws Exception {
		final String FILEPATH="src/test/resources/AvailabilityBundle";

		List<Bundle> bundleSet=(new Util()).getDataFromFolder(FILEPATH);

		Object[][] testData=new Object[bundleSet.size()][1];

		for(int i=0;i<bundleSet.size();i++)
		{
			testData[i][0]=bundleSet.get(i);
		}

		return testData;
	}

	public static com.inmobi.qa.falcon.generated.cluster.Cluster getClusterObject(String clusterXML) throws Exception
	{
		JAXBContext context=JAXBContext.newInstance(com.inmobi.qa.falcon.generated.cluster.Cluster.class);
		Unmarshaller um=context.createUnmarshaller();
		return (com.inmobi.qa.falcon.generated.cluster.Cluster)um.unmarshal(new StringReader(clusterXML));
	}

	public static ArrayList<String> getInstanceFinishTimes(ColoHelper coloHelper,String workflowId) throws Exception
	{
		ArrayList<String> raw=runRemoteScript(coloHelper.getProcessHelper()
      .getQaHost(), coloHelper.getProcessHelper().getUsername(),
      coloHelper.getProcessHelper().getPassword(),
      "cat /var/log/ivory/application.* | grep \""+workflowId+"\" | grep " +
        "\"Received\" | awk '{print $2}'", coloHelper.getProcessHelper().getIdentityFile());
		ArrayList<String> finalList=new ArrayList<String>();
		for(String line:raw)
		{
			finalList.add(line.split(",")[0]);

		}

		return finalList;
	}

	public static ArrayList<String> getInstanceRetryTimes(ColoHelper coloHelper,String workflowId) throws Exception
	{
		ArrayList<String> raw=runRemoteScript(coloHelper.getProcessHelper()
      .getQaHost(), coloHelper.getProcessHelper().getUsername(),
      coloHelper.getProcessHelper().getPassword(),
      "cat /var/log/ivory/application.* | grep \""+workflowId+"\" | grep " +
        "\"Retrying attempt\" | awk '{print $2}'",
      coloHelper.getProcessHelper().getIdentityFile());
		ArrayList<String> finalList=new ArrayList<String>();
		for(String line:raw)
		{
			finalList.add(line.split(",")[0]);
		}

		return finalList;
	}

	public static void shutDownService(IEntityManagerHelper helper) throws Exception
	{
		runRemoteScriptAsSudo(helper.getQaHost(),helper.getUsername(),
      helper.getPassword(),helper.getServiceStopCmd(),
      helper.getServiceUser(), helper.getIdentityFile());
	}

	public static void startService(IEntityManagerHelper helper) throws Exception
	{
		runRemoteScriptAsSudo(helper.getQaHost(),helper.getUsername(),
      helper.getPassword(),helper.getServiceStartCmd(), helper.getServiceUser(), helper.getIdentityFile());
	}

	public static void restartService(IEntityManagerHelper helper) throws Exception
	{
		Util.print("restarting service for: "+helper.getQaHost() );

		//check if needs to be restarted or not
	//	ArrayList<String> tomcatStatus = runRemoteScriptAsSudo(helper.getQaHost(),helper.getUsername(),helper.getPassword()," /etc/init.d/tomcat6 status");
	//	if(tomcatStatus.get(0).contains("Tomcat servlet engine is running with pid"))
	//		return;
		runRemoteScriptAsSudo(helper.getQaHost(),helper.getUsername(),
      helper.getPassword(),helper.getServiceStopCmd(), helper.getServiceUser(), helper.getIdentityFile());
		Thread.sleep(10000);
		runRemoteScriptAsSudo(helper.getQaHost(),helper.getUsername(),
      helper.getPassword(),helper.getServiceStartCmd(), helper.getServiceUser(), helper.getIdentityFile());
	}        


	private static ArrayList<String> runRemoteScriptAsSudo(String hostName,
                                                         String userName,
                                                         String password,
                                                         String command,
                                                         String
                                                           runAs,
                                                         String identityFile
                                                         ) throws
    Exception
	{
		JSch jsch = new JSch();
		Session session = jsch.getSession(userName, hostName, 22);
    // only set the password if its not empty
    if (null != password && !password.isEmpty()) {
      session.setUserInfo(new HardcodedUserInfo(password));
    }
		Properties config = new Properties();
		config.setProperty("StrictHostKeyChecking", "no");
    config.setProperty("UserKnownHostsFile", "/dev/null");
    // only set the password if its not empty
    if (null == password || password.isEmpty()) {
      jsch.addIdentity(identityFile);
    }
		session.setConfig(config);
		session.connect();
		Assert.assertTrue(session.isConnected(),"The session was not connected correctly!");

		ArrayList<String> data = new ArrayList<String>();

		ChannelExec channel = (ChannelExec) session.openChannel("exec");
    channel.setPty(true);
    String runCmd = null;
    if (null == runAs || runAs.isEmpty()) {
      runCmd = "sudo -S -p '' "+command;
    } else {
      runCmd = String.format("sudo su %s -c p '%s'",runAs, command);
    }
		channel.setCommand(runCmd);
		InputStream in=channel.getInputStream();
		OutputStream out=channel.getOutputStream();
		channel.setErrStream(System.err);

		channel.connect();
		Thread.sleep(10000);
    // only print the password if its not empty
    if (null != password && !password.isEmpty()) {
		  out.write((password+"\n").getBytes());
		  out.flush();
    }

		in.close();
		channel.disconnect();
		session.disconnect();
    out.close();
		return data;
	}

	public static void verifyNoJobsFoundInOozie(ArrayList<String> data) throws Exception
	{
		Assert.assertTrue(data.get(0).contains("No Jobs match your criteria!"),"Job was found on this oozie when not expected! Please check!");
	}

	public static Bundle getUniqueBundleForColo(Bundle sourceBundle,PrismHelper prismHelper) throws Exception
	{
		Bundle newBundle= new Bundle(sourceBundle,prismHelper.getEnvFileName());
		newBundle.generateUniqueBundle();
		return newBundle;
	}

	public static Process getProcessObject(String processData) throws Exception
	{
		JAXBContext context=JAXBContext.newInstance(Process.class);
		Unmarshaller um=context.createUnmarshaller();
		return (Process)um.unmarshal(new StringReader(processData));
	}

	public static Feed getFeedObject(String feedData) throws Exception
	{
		JAXBContext context=JAXBContext.newInstance(Feed.class);
		Unmarshaller um=context.createUnmarshaller();

		return (Feed)um.unmarshal(new StringReader(feedData)); 


	}

	public static void compareXMLs(String source,String target) throws Exception
	{
		Diff diff=XMLUnit.compareXML(source, target);
		Assert.assertTrue(diff.similar());

	}

	public static int getNumberOfWorkflowInstances(PrismHelper prismHelper,String bundleId) throws Exception
	{
		return getDefaultOozieCoord(prismHelper,bundleId).getActions().size();
	}

	public static void validateNumberOfWorkflowInstances(PrismHelper prismHelper,int originalCount,String oldBundleId,String updatedBundleId) throws Exception
	{
		//first make sure sum of all parts is same
		Assert.assertEquals(getNumberOfWorkflowInstances(prismHelper,oldBundleId)+getNumberOfWorkflowInstances(prismHelper,updatedBundleId),originalCount,"The total number of workflow instances dont match post update! Please check.");

	} 

	public static List<String> generateUniqueClusterEntity(List<String> clusterData) throws Exception
	{
		ArrayList<String> newList=new ArrayList<String>();
		for(String cluster:clusterData)
		{
			newList.add(generateUniqueClusterEntity(cluster));
		}

		return newList;
	}        

	public static ArrayList<String> getBundles(String entityName,String entityType,IEntityManagerHelper helper) throws Exception {
		if(entityType.equals("FEED"))
			return runRemoteScript(helper.getQaHost(), helper.getUsername(),
        helper.getPassword(),helper.getOozieLocation()+"/oozie jobs -oozie " +
        ""+helper.getOozieURL()+"  -jobtype bundle -localtime -filter " +
        "name=FALCON_FEED_"+entityName+"|grep 000|awk '{print $1}'", helper.getIdentityFile());
		else
			return runRemoteScript(helper.getQaHost(), helper.getUsername(),
        helper.getPassword(),helper.getOozieLocation()+"/oozie jobs -oozie " +
        ""+helper.getOozieURL()+"  -jobtype bundle -localtime -filter " +
        "name=FALCON_PROCESS_"+entityName+"|grep 000|awk '{print $1}'",
        helper.getIdentityFile());

	}

	public static void dumpConsumerData(Consumer consumer) throws Exception
	{
		logger.info("dumping all queue data:");

		for(HashMap<String,String> data:consumer.getMessageData())
		{
			logger.info("*************************************");
			for(String key:data.keySet())
			{
				logger.info(key+"="+data.get(key));
			}
			logger.info("*************************************");
		}
	}        

	public static void lateDataReplenish(PrismHelper prismHelper,int interval,int daySkip,int minuteSkip,String folderPrefix,String postFix) throws Exception
	{
		List<String> folderPaths=Util.getMinuteDatesOnEitherSide(interval,minuteSkip);
		Util.print("folderData: "+folderPaths.toString());
		/*List<String> folderData=Util.getDailyDatesOnEitherSide(interval,daySkip);
        		List<String> hourlyData=Util.convertDatesToFolders(folderData,0);
        		List<String> finalData=Util.addMinutesToCreatedFolders(hourlyData,minuteSkip);*/ 

		if(postFix!=null)
		{
			for(int i = 0 ; i < folderPaths.size() ; i++)
				folderPaths.set(i,folderPaths.get(i)+postFix);
		}

		Util.createLateDataFolders(prismHelper,folderPaths,folderPrefix);
		Util.copyDataToFolders(prismHelper,folderPrefix,folderPaths,"src/test/resources/OozieExampleInputData/normalInput/_SUCCESS","src/test/resources/OozieExampleInputData/normalInput/log_01.txt");
	}

	public static int getRunIdOfSpecifiedInstance(int index,String bundleId,ColoHelper coloHelper) throws Exception
	{
		XOozieClient oozieClient=new XOozieClient(coloHelper.getProcessHelper().getOozieURL());
		BundleJob bundleJob = oozieClient.getBundleJobInfo(bundleId); 

		for(CoordinatorJob job:bundleJob.getCoordinators())
		{
			if(job.getAppName().contains("DEFAULT"))
			{

				CoordinatorJob coordJob=oozieClient.getCoordJobInfo(job.getId());

				for(int i=1;i<=coordJob.getActions().size();i++)
				{
					if(i==index)
					{
						WorkflowJob actionInfo=oozieClient.getJobInfo(coordJob.getActions().get(i-1).getId()); 

						return actionInfo.getRun();
					}
				}
			}
		}

		return -1;
	}



	public static void assertSucceeded(ProcessInstancesResult response) {		
		Assert.assertNotNull(response.getMessage());
		Assert.assertTrue(response.getMessage().contains("SUCCEEDED") || response.getStatus().toString().equals("SUCCEEDED"));
	}

	public static void assertFailed(ProcessInstancesResult response) {		
		Assert.assertNotNull(response.getMessage());
		Assert.assertTrue(response.getMessage().contains("FAILED") || response.getStatus().toString().equals("FAILED"));
	}

	public static void assertPartial(ProcessInstancesResult response) {		
		Assert.assertNotNull(response.getMessage());
		Assert.assertTrue(response.getMessage().contains("PARTIAL") || response.getStatus().toString().equals("PARTIAL"));
	}

	public static boolean isBundleOver(ColoHelper coloHelper,String bundleId) throws Exception
	{
		XOozieClient client=new XOozieClient(coloHelper.getClusterHelper().getOozieURL());

		BundleJob bundleJob = client.getBundleJobInfo(bundleId); 

		if(bundleJob.getStatus().equals(BundleJob.Status.DONEWITHERROR) || bundleJob.getStatus().equals(BundleJob.Status.FAILED) || bundleJob.getStatus().equals(BundleJob.Status.SUCCEEDED) || bundleJob.getStatus().equals(BundleJob.Status.KILLED))
		{
			return true;
		}


		Thread.sleep(20000);
		return false;
	}    

	public static void lateDataReplenishWithout_Success(PrismHelper prismHelper,int interval,int daySkip,int minuteSkip,String folderPrefix,String postFix) throws Exception
	{
		List<String> folderPaths=Util.getMinuteDatesOnEitherSide(interval,minuteSkip);
		Util.print("folderData: "+folderPaths.toString());
		/*List<String> folderData=Util.getDailyDatesOnEitherSide(interval,daySkip);
            		List<String> hourlyData=Util.convertDatesToFolders(folderData,0);
            		List<String> finalData=Util.addMinutesToCreatedFolders(hourlyData,minuteSkip);*/ 

		if(postFix!=null)
		{
			for(int i = 0 ; i < folderPaths.size() ; i++)
				folderPaths.set(i,folderPaths.get(i)+postFix);
		}

		Util.createLateDataFolders(prismHelper,folderPaths,folderPrefix);
		Util.copyDataToFolders(prismHelper,folderPrefix,folderPaths,"src/test/resources/OozieExampleInputData/normalInput/log_01.txt");
	}

	static Path stringToPath(String location) {

		return new Path(location);
	}



	public static void putFileInFolderHDFS(PrismHelper prismHelper,int interval,int daySkip,int minuteSkip,String folderPrefix, String fileToBePut) throws Exception {
		List<String> folderPaths=Util.getMinuteDatesOnEitherSide(interval,minuteSkip);
		Util.print("folderData: "+folderPaths.toString());

		Util.createLateDataFolders(prismHelper,folderPaths,folderPrefix);

		if(fileToBePut.equals("_SUCCESS"))
			Util.copyDataToFolders(prismHelper,folderPrefix,folderPaths,"src/test/resources/OozieExampleInputData/normalInput/_SUCCESS");

		else
			Util.copyDataToFolders(prismHelper,folderPrefix,folderPaths,"src/test/resources/OozieExampleInputData/normalInput/log_01.txt");

	}



	public static Object[][] readNoOutputBundles()throws Exception {

		final String FILEPATH="src/test/resources/ProcessWithNoOutput";

		List<Bundle> bundleSet=(new Util()).getDataFromFolder(FILEPATH);

		Object[][] testData=new Object[bundleSet.size()][1];

		for(int i=0;i<bundleSet.size();i++)
		{
			//bundleSet.get(i).setClusterData(getEnvClusterXML(bundleSet.get(i).getClusterData()));
			testData[i][0]=bundleSet.get(i);
		}

		return testData;
	}

	static PrismHelper prismHelper=new PrismHelper("prism.properties");


	public static void submitAllClusters(Bundle... b) throws Exception{

		for(int i = 0 ; i < b.length ; i++)
		{
			Util.print("Submitting Cluster: "+b[i].getClusters().get(0));
			ServiceResponse r = prismHelper.getClusterHelper().submitEntity(URLS.SUBMIT_URL,b[i].getClusters().get(0));
			Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

		}
	}



	public static Object[][] readBundle(
			GetBundle bundlePath) throws Exception{
		final String FILEPATH = bundlePath.getValue();

		List<Bundle> bundleSet = (new Util()).getDataFromFolder(FILEPATH);

		Object[][] testData = new Object[bundleSet.size()][1];

		for (int i = 0; i < bundleSet.size(); i++) {
			testData[i][0] = bundleSet.get(i);
		}

		return testData;
	}

/*	private static final TSerializer SER = new TSerializer(
			new TBinaryProtocol.Factory());

	public static void WriteToFileAdRR(String fileName, ArrayList<AdRR> adrrs) throws IOException, TException{

		File file = new File(fileName);
		if(file.exists())
			file.delete();

		FileWriter writer = new FileWriter(file);
		for(AdRR adRR:adrrs ){
			Log.info("writing adrr to file: "+ adRR.toString());
			writer.write(getSerRR(adRR));
			writer.write("\n");
		}
		writer.close();
	}

	public static String getSerRR(AdRR adRR) throws TException {
		byte[] data= SER.serialize(adRR);

		Text val = new Text(
				Base64.encodeBase64(data));
		return  val.toString();

	}*/

	public static ArrayList<String> getBundles(PrismHelper coloHelper,
			String entityName, ENTITY_TYPE entityType) throws Exception {

		if (entityType.equals(ENTITY_TYPE.FEED)) {
			return runRemoteScript(
					coloHelper.getFeedHelper().getQaHost(),
					coloHelper.getFeedHelper().getUsername(),
					coloHelper.getFeedHelper().getPassword(),
					coloHelper.getFeedHelper().getOozieLocation()
					+ "/oozie jobs -oozie "
					+ coloHelper.getFeedHelper().getOozieURL()
					+ "  -jobtype bundle -localtime -filter name=FALCON_FEED_"
					+ entityName + "|grep 000|awk '{print $1}'",
        coloHelper.getFeedHelper().getIdentityFile());
		} else {
			return runRemoteScript(
					coloHelper.getFeedHelper().getQaHost(),
					coloHelper.getFeedHelper().getUsername(),
					coloHelper.getFeedHelper().getPassword(),
					coloHelper.getFeedHelper().getOozieLocation()
					+ "/oozie jobs -oozie "
					+ coloHelper.getFeedHelper().getOozieURL()
					+ "  -jobtype bundle -localtime -filter name=FALCON_PROCESS_"
					+ entityName + "|grep 000|awk '{print $1}'",
        coloHelper.getFeedHelper().getIdentityFile());
		}
	}

/*	public static void WriteToFileCRR(String fileName,
			ArrayList<ClickRequestResponse> crr) throws Exception {

		File file = new File(fileName);
		if(file.exists())
			file.delete();

		FileWriter writer = new FileWriter(file);
		for(ClickRequestResponse c:crr ){
			Log.info("writing adrr to file: "+ c.toString());
			writer.write(getSerCRR(c));
			writer.write("\n");
		}
		writer.close();

	}
	private static String getSerCRR(ClickRequestResponse c)throws Exception {
		byte[] data= SER.serialize(c);

		Text val = new Text(
				Base64.encodeBase64(data));
		return  val.toString();
	}
*/
	public  List<Bundle> getDataFromFolder(String folderPath)
	throws Exception {

		File[] files = Util.getFiles(folderPath);
	//	if(files==null || files.length<=0)
	//	{
	//		System.out.println("WARNING: no files found in mentioned resource bundle");
	//		System.exit(0);
	//	}

		///////////// testing getclass loader to get files from jar /////////////////
		/*
				Enumeration<URL> resources01 = getClass().getClassLoader().getResources("LocalDC_feedReplicaltion_BillingRC");
				System.out.println("exists = " + resources01.hasMoreElements());

				Enumeration<URL> resources02 = getClass().getClassLoader().getResources("errorMapping.properties");
				System.out.println("exists = " + resources02.hasMoreElements());


				InputStream is = getClass().getClassLoader().getResourceAsStream("LocalDC_feedReplicaltion_BillingRC/FETL-BillingRC.xml");
				InputStreamReader isr = new InputStreamReader(is);
				BufferedReader br = new BufferedReader(isr);
				String line;
			    while ((line = br.readLine()) != null) 
			    {
			      Util.print(line);
			    }
			    br.close();
			    isr.close();
			    is.close();

				 is = getClass().getClassLoader().getResourceAsStream("errorMapping.properties");
				 isr = new InputStreamReader(is);
				 br = new BufferedReader(isr);

			    while ((line = br.readLine()) != null) 
			    {
			      Util.print(line);
			    }
			    br.close();
			    isr.close();
			    is.close();


				String[] resourceList_dir = getResourceListing(this.getClass(),"LocalDC_feedReplicaltion_BillingRC");

				for(int i = 0 ; i < resourceList_dir.length ; i++)
					Util.print(resourceList_dir[i]);



				String[] resourceList_recur = getResourceListing(this.getClass(),"IgnoreBundle");

				for(int i = 0 ; i < resourceList_recur.length ; i++)
					Util.print(resourceList_recur[i]);




				Util.print(br.toString());

				CodeSource src = this.getClass().getProtectionDomain().getCodeSource();
				if (src != null) {
					URL jar = src.getLocation();
					ZipInputStream zip = new ZipInputStream(jar.openStream());



					Util.print(zip.getNextEntry().toString());
				}

				URL u = this.getClass().getResource("FETL-BillingRC.xml");

				Util.print(u.toURI().toString());

				System.out.println(this.getClass().getResourceAsStream("FETL-BillingRC.xml").read());
				System.out.println(new File(this.getClass().getResource("FETL-BillingRC.xml").getPath()));


				String[] resourceList_file = getResourceListing(this.getClass(),"LocalDC_feedReplicaltion_BillingRC/FETL-BillingRC.xml");

				for(int i = 0 ; i < resourceList_file.length ; i++)
					Util.print(resourceList_file[i]);		*/
		////////////// testing close here ////////////////////////////////

		List<Bundle> bundleList = new ArrayList<Bundle>();

		ArrayList<String> dataSets = new ArrayList<String>();
		String processData = new String();
		String clusterData = new String();

		for (int i = 0; i < files.length; i++) {

			if (files[i].getName().contains("svn")
					|| files[i].getName().contains(".DS")
					|| files[i].getName() == null) {
				continue;
			} else {
				if (files[i].isDirectory()) {
					bundleList.addAll(getDataFromFolder(new String(files[i]
					                                                     .getAbsolutePath())));
				} else {

					String data = fileToString(new File(files[i].getAbsolutePath()));

					if (data.contains("uri:ivory:process:0.1") || data.contains("uri:falcon:process:0.1")) {
						System.out.println("data been added to process: "+data);
						processData = data;
					} else if (data.contains("uri:ivory:cluster:0.1") || data.contains("uri:falcon:cluster:0.1")) {
						System.out.println("data been added to cluster: "+data);
						clusterData = data;
					} else if(data.contains("uri:ivory:feed:0.1") || data.contains("uri:falcon:feed:0.1")){
						System.out.println("data been added to feed: "+data);
						dataSets.add(data);
					}

					/*	if (files[i].getName().contains("process")) {
						processData = fileToString(new File(
								files[i].getAbsolutePath()));
					} else if (files[i].getName().contains("cluster")) {
						clusterData = fileToString(new File(
								files[i].getAbsolutePath()));
					} else if(files[i].getName().contains("feed")){
						dataSets.add(fileToString(new File(files[i]
						                                         .getAbsolutePath())));
					}*/

				}
			}

		}
		if (!(dataSets.isEmpty()) && processData != ""
			&& !"".equals(clusterData)) {
			bundleList.add(new Bundle(dataSets, processData, clusterData));
		}
		else if(processData != ""
			&& !"".equals(clusterData))
			bundleList.add(new Bundle(dataSets, processData, clusterData));

		return bundleList;

	}


	/**
	 * List directory contents for a resource folder. Not recursive.
	 * This is basically a brute-force implementation.
	 * Works for regular files and also JARs.
	 * 
	 * @author Greg Briggs
	 * @param clazz Any java class that lives in the same place as the resources you want.
	 * @param path Should end with "/", but not start with one.
	 * @return Just the name of each member item, not the full paths.
	 * @throws URISyntaxException 
	 * @throws IOException 
	 */
	String[] getResourceListing(Class clazz, String path) throws URISyntaxException, IOException {
		URL dirURL = clazz.getClassLoader().getResource(path);
		if (dirURL != null && dirURL.getProtocol().equals("file")) {
			/* A file path: easy enough */
			return new File(dirURL.toURI()).list();
		} 

		if (dirURL == null) {
			/* 
			 * In case of a jar file, we can't actually find a directory.
			 * Have to assume the same jar as clazz.
			 */
			String me = clazz.getName().replace(".", "/")+".class";
			dirURL = clazz.getClassLoader().getResource(me);
		}

		if (dirURL.getProtocol().equals("jar")) {
			/* A JAR path */
			String jarPath = dirURL.getPath().substring(5, dirURL.getPath().indexOf("!")); //strip out only the JAR file
			JarFile jar = new JarFile(URLDecoder.decode(jarPath, "UTF-8"));
			Enumeration<JarEntry> entries = jar.entries(); //gives ALL entries in jar
			Set<String> result = new HashSet<String>(); //avoid duplicates in case it is a subdirectory
			while(entries.hasMoreElements()) {
				String name = entries.nextElement().getName();
				if (name.startsWith(path)) { //filter according to the path
					String entry = name.substring(path.length());
					int checkSubdir = entry.indexOf("/");
					if (checkSubdir >= 0) {
						// if it is a subdirectory, we just return the directory name
						entry = entry.substring(0, checkSubdir);
					}
					result.add(entry);
				}
			}
			return result.toArray(new String[result.size()]);
		} 

		throw new UnsupportedOperationException("Cannot list files for URL "+dirURL);
	}

	public  Object[][] readBundle(
			String bundlePath) throws Exception{
		final String FILEPATH = bundlePath;

		List<Bundle> bundleSet = getDataFromFolder(FILEPATH);

		Object[][] testData = new Object[bundleSet.size()][1];

		for (int i = 0; i < bundleSet.size(); i++) {
			testData[i][0] = bundleSet.get(i);
		}

		return testData;
	}

	public static ArrayList<Long> getSequentialRandomNumbers(int minValue,long maxValue,int numberOFRecords){

		Util.print("getting "+ numberOFRecords+ " random numbers between: "+ minValue + " and "+maxValue);

		ArrayList<Long> randomRecords = new ArrayList<Long>();
		for(int recordNumber = 0 ; recordNumber< numberOFRecords ; recordNumber++){

			randomRecords.add(minValue + (long)(Math.random() * ((maxValue - minValue) + 1)));
		}

		Collections.sort(randomRecords);

		return randomRecords;
	}
	public static void forceRestartService(IEntityManagerHelper helper) throws Exception{
		Util.print("force restarting service for: "+helper.getQaHost() );

		//check if needs to be restarted or not
		runRemoteScriptAsSudo(helper.getQaHost(),helper.getUsername(),
      helper.getPassword(),helper.getServiceRestartCmd(),
      helper.getServiceUser(), helper.getIdentityFile());

	}

  public static Properties getPropertiesObj(String filename) {
    Properties properties=new Properties();
    try {
      FileInputStream conf_stream=new FileInputStream(new File("src/main/resources/"+filename));
      properties.load(conf_stream);
      conf_stream.close();
    }
    catch(Exception e)
    {
      logger.info(e.getStackTrace());
    }

    finally {
      return properties;
    }
  }
}
