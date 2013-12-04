package com.inmobi.qa.falcon.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import com.inmobi.qa.falcon.bundle.Bundle;
import com.inmobi.qa.falcon.generated.coordinator.COORDINATORAPP;
import com.inmobi.qa.falcon.generated.dependencies.Frequency;
import com.inmobi.qa.falcon.generated.process.Process;
import com.inmobi.qa.falcon.helpers.ColoHelper;
import com.inmobi.qa.falcon.interfaces.IEntityManagerHelper;
import com.inmobi.qa.falcon.response.ProcessInstancesResult;
import com.inmobi.qa.falcon.supportClasses.ENTITY_TYPE;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job.Status;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.XOozieClient;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.Assert;
import org.testng.log4testng.Logger;

import com.google.gson.GsonBuilder;
import com.inmobi.qa.falcon.generated.feed.ClusterType;
import com.inmobi.qa.falcon.generated.feed.Feed;
import com.inmobi.qa.falcon.generated.feed.LocationType;
import com.inmobi.qa.falcon.generated.feed.Retention;
import com.inmobi.qa.falcon.generated.process.Input;
import com.inmobi.qa.falcon.generated.process.Output;
import com.inmobi.qa.falcon.helpers.PrismHelper;
import com.inmobi.qa.falcon.response.APIResult;
import com.inmobi.qa.falcon.response.ServiceResponse;

/**
 * 
 * @author samarth.gupta
 *
 */
public class instanceUtil {


	static XOozieClient oozieClient=null;
	static String hdfs_url=null;


	public instanceUtil(String envFileName) throws Exception
	{
		oozieClient=new XOozieClient(Util.readPropertiesFile(envFileName,"oozie_url"));
		hdfs_url="hdfs://"+Util.readPropertiesFile(envFileName,"hadoop_url");

	}

	//static XOozieClient oozieClient=new XOozieClient(Util.readPropertiesFile("oozie_url"));
	//static String hdfs_url = "hdfs://"+Util.readPropertiesFile("hadoop_url");
	static Logger logger=Logger.getLogger(instanceUtil.class);

	public static ProcessInstancesResult sendRequestProcessInstance(String url) throws Exception
	{

		HttpRequestBase request=null;
		if(Thread.currentThread().getStackTrace()[3].getMethodName().contains("Suspend")|| Thread.currentThread().getStackTrace()[3].getMethodName().contains("Resume")|| Thread.currentThread().getStackTrace()[3].getMethodName().contains("Kill")|| Thread.currentThread().getStackTrace()[3].getMethodName().contains("Rerun"))
		{
			request = new HttpPost();

		}
		else
			request=new HttpGet();
		request.setHeader("Remote-User","rishu");
		return hitUrl(url,request);
	}

	public static ProcessInstancesResult sendRequestProcessInstanceRerun(
			String url) throws Exception {
		HttpPost post=new HttpPost(url);
		post.setHeader("Content-Type","text/xml");
		//post.setEntity(new StringEntity(postData));
		post.setHeader("Remote-User","rishu");

		return hitUrl(url,post);

	}
	public static ProcessInstancesResult hitUrl(String url,HttpRequestBase request) throws Exception {
		logger.info("hitting the url: "+url);

		request.setURI(new URI(url));
		HttpClient client=new DefaultHttpClient();

		HttpResponse response=client.execute(request);


		BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent(), "UTF-8"));
		StringBuilder string_response = new StringBuilder();
		for (String line = null; (line = reader.readLine()) != null;) {
			string_response.append(line).append("\n");
		}
		String jsonString = string_response.toString();
		logger.info("The web service response status is "+response.getStatusLine().getStatusCode());
		logger.info("The web service response is: "+string_response.toString()+"\n");
		ProcessInstancesResult r = new ProcessInstancesResult();
		if(jsonString.contains("(PROCESS) not found"))
		{
			r.setStatusCode(777);
			return r;
		}
		else if(jsonString.contains("Parameter start is empty") || jsonString.contains("Unparseable date:"))
		{
			r.setStatusCode(2);
			return r;
		}
		else if(response.getStatusLine().getStatusCode() == 400 && jsonString.contains("(FEED) not found")){
			r.setStatusCode(400); 
			return r;
		}
		else if((response.getStatusLine().getStatusCode()==400 && jsonString.contains("is beforePROCESS  start")) || response.getStatusLine().getStatusCode()==400 && jsonString.contains("is after end date")
				||(response.getStatusLine().getStatusCode()==400 && jsonString.contains("is after PROCESS's end"))||(response.getStatusLine().getStatusCode()==400 && jsonString.contains("is before PROCESS's  start")))
		{
			r.setStatusCode(400);
			return r;
		}
		r =new GsonBuilder().setPrettyPrinting().create().fromJson(jsonString,ProcessInstancesResult.class);

		Util.print("r.getMessage(): "+ r.getMessage());
		Util.print("r.getStatusCode(): "+ r.getStatusCode());
		Util.print("r.getStatus() "+r.getStatus());
		Util.print("r.getInstances()"+r.getInstances());
		return  r;		
	}


	public static String insertSquareBraces(String malformedJSON)
	{
		String correctJSON = malformedJSON.substring(0,malformedJSON.indexOf("\"instances\"")+12)+ "[" + malformedJSON.substring(malformedJSON.indexOf("{\"instance\":"),malformedJSON.lastIndexOf('}'))+"]"+"}";
		Util.print("correctJSON "+correctJSON );
		return correctJSON ;
	}

	public static void  validateSuccess(ProcessInstancesResult r,Bundle b,ProcessInstancesResult.WorkflowStatus ws) throws Exception
	{
		Assert.assertEquals(r.getStatus(),APIResult.Status.SUCCEEDED);
		Assert.assertEquals(b.getProcessConcurrency(),runningInstancesInResult(r,ws));
	}

	public static int runningInstancesInResult(ProcessInstancesResult r,ProcessInstancesResult.WorkflowStatus ws) {
		ProcessInstancesResult.ProcessInstance[] pArray = r.getInstances();
		int runningCount=0;
		//Util.print("function runningInstancesInResult: Start");
		Util.print("pArray: "+pArray.toString());
		for(int instanceIndex = 0 ; instanceIndex < pArray.length; instanceIndex++)
		{
			Util.print("pArray["+instanceIndex+"]: "+pArray[instanceIndex].getStatus()+" , "+pArray[instanceIndex].getInstance());

			if(pArray[instanceIndex].getStatus().equals(ws))
			{
				runningCount++;
			}
		}
		return runningCount;
	}

	public static String getProcessInstanceStart(Bundle b,String clusterName) throws Exception {

		JAXBContext jc=JAXBContext.newInstance(com.inmobi.qa.falcon.generated.process.Process.class);

		Unmarshaller u=jc.createUnmarshaller();

		Process processElement=(Process)u.unmarshal((new StringReader(b.getProcessData())));

		return instanceUtil.dateToOozieDate(b.getClusterObjectFromProcess(clusterName).getValidity().getStart());
	}

	public static void validateSuccessWOInstances(ProcessInstancesResult r) {
		Assert.assertTrue(r.getMessage().contains("is successful"));
		Assert.assertEquals(r.getStatus(),APIResult.Status.SUCCEEDED);
		if(r.getInstances()!=null)
			Assert.assertTrue(false);
	}

	public static void validateSuccessWithStatusCode(ProcessInstancesResult r,
			int expectedErrorCode) {
		Assert.assertEquals(r.getStatusCode(),expectedErrorCode,"Parameter start is empty shiuld have the response");		
	}

	public static void writeProcessElement(Bundle bundle, Process processElement) throws Exception {
		JAXBContext jc=JAXBContext.newInstance(Process.class); 
		java.io.StringWriter sw = new StringWriter();
		Marshaller marshaller = jc.createMarshaller();
		marshaller.marshal(processElement,sw);
		logger.info("modified process is: "+sw);
		bundle.setProcessData(sw.toString());
	}

	public static Process getProcessElement(Bundle bundle) throws Exception {
		JAXBContext jc=JAXBContext.newInstance(Process.class); 
		Unmarshaller u=jc.createUnmarshaller();
		return (Process)u.unmarshal((new StringReader(bundle.getProcessData())));

	}

	public static Feed getFeedElement(Bundle bundle, String feedName) throws Exception {
		JAXBContext jc=JAXBContext.newInstance(Feed.class); 
		Unmarshaller u=jc.createUnmarshaller();
		Feed feedElement=(Feed)u.unmarshal((new StringReader(bundle.dataSets.get(0))));
		if(!feedElement.getName().contains(feedName))
		{
			feedElement = (Feed)u.unmarshal(new StringReader(bundle.dataSets.get(1)));

		}
		return feedElement;
	}

	public static void writeFeedElement(Bundle bundle, Feed feedElement,
			String feedName) throws Exception {
		JAXBContext jc=JAXBContext.newInstance(Feed.class); 
		Unmarshaller u=jc.createUnmarshaller();
		java.io.StringWriter sw = new StringWriter();
		Marshaller marshaller = jc.createMarshaller();
		marshaller.marshal(feedElement,sw);
		//logger.info("feed to be written is: "+sw);
		int index = 0 ; 
		Feed dataElement=(Feed)u.unmarshal(new StringReader(bundle.dataSets.get(0)));
		if(!dataElement.getName().contains(feedName))
		{
			dataElement = (Feed)u.unmarshal(new StringReader(bundle.dataSets.get(1)));
			index =1;
		}
		bundle.getDataSets().set(index, sw.toString());
	}

	public static void validateSuccessOnlyStart(ProcessInstancesResult r,
			Bundle b, ProcessInstancesResult.WorkflowStatus ws) throws Exception {
		Assert.assertEquals(r.getStatus(),APIResult.Status.SUCCEEDED);
		Assert.assertEquals(1,runningInstancesInResult(r,ws));
		//Assert.assertEquals(r.getInstances()[0].instance, getProcessInstanceStart(b,clusterName));

	}

	public static void validateResponse(ProcessInstancesResult r, int totalInstances, int runningInstances,
			int suspendedInstances, int waitingInstances, int killedInstances) {

		int actualRunningInstances = 0;
		int actualSuspendedInstances = 0;
		int actualWaitingInstances = 0;
		int actualKilledInstances = 0;
		ProcessInstancesResult.ProcessInstance[] pArray = r.getInstances();
		Util.print("pArray: "+pArray.toString());
		Assert.assertEquals(pArray.length,totalInstances);
		for(int instanceIndex = 0 ; instanceIndex < pArray.length; instanceIndex++)
		{
			Util.print("pArray["+instanceIndex+"]: "+pArray[instanceIndex].getStatus()+" , "+pArray[instanceIndex].getInstance());

			if(pArray[instanceIndex].getStatus().equals(ProcessInstancesResult.WorkflowStatus.RUNNING))
				actualRunningInstances++;
			else if(pArray[instanceIndex].getStatus().equals(ProcessInstancesResult.WorkflowStatus.SUSPENDED))
				actualSuspendedInstances++;
			else if(pArray[instanceIndex].getStatus().equals(ProcessInstancesResult.WorkflowStatus.WAITING))
				actualWaitingInstances++;
			else if(pArray[instanceIndex].getStatus().equals(ProcessInstancesResult.WorkflowStatus.KILLED))
				actualKilledInstances++;
		}

		Assert.assertEquals(actualRunningInstances, runningInstances);
		Assert.assertEquals(actualSuspendedInstances, suspendedInstances);
		Assert.assertEquals(actualWaitingInstances, waitingInstances);
		Assert.assertEquals(actualKilledInstances, killedInstances);
	}

	public static ArrayList<String> getWorkflows(PrismHelper prismHelper,String processName,
			WorkflowAction.Status ws) throws Exception {

		String bundleID = Util.getCoordID(Util.getOozieJobStatus(prismHelper,processName,"NONE").get(0));
		oozieClient=new XOozieClient(Util.readPropertiesFile(prismHelper.getEnvFileName(),"oozie_url"));


		//BundleJob bundleJob = oozieClient.getBundleJobInfo(bundleID);
		//CoordinatorJob jobInfo = oozieClient.getCoordJobInfo(bundleJob.getCoordinators().get(0).getId());
		ArrayList<String> workflows=Util.getCoordinatorJobs(prismHelper,bundleID,"any");

		ArrayList<String> toBeReturned = new ArrayList<String>();
		for(String jobID : workflows)
		{
			CoordinatorAction wfJob = oozieClient.getCoordActionInfo(jobID);
			WorkflowAction wa = oozieClient.getWorkflowActionInfo(wfJob.getExternalId());
			Util.print("wa.getExternalId(): "+wa.getExternalId()+" wa.getExternalStatus():  "+wa.getExternalStatus());
			Util.print("wf id: "+ jobID +"  wf status: "+wa.getStatus());
			//org.apache.oozie.client.WorkflowAction.Status.
			if(wa.getStatus().equals(ws))
				toBeReturned.add(jobID);
		}
		return toBeReturned;
	}


	public static boolean isWorkflowRunning(String workflowID) throws Exception {
		CoordinatorAction wfJob = oozieClient.getCoordActionInfo(workflowID);
		WorkflowAction wa = oozieClient.getWorkflowActionInfo(wfJob.getExternalId());
		if(wa.getStatus().toString().equals("RUNNING"))
			return true;
		return false;
	}

	public static void areWorkflowsRunning(PrismHelper prismHelper,String ProcessName, int totalWorkflows, int runningWorkflows, int killedWorkflows, int succeededWorkflows) throws Exception {

		ArrayList<WorkflowAction> was = getWorkflowActions(prismHelper,ProcessName);
		if(totalWorkflows!=-1)
			Assert.assertEquals(was.size(), totalWorkflows);
		int actualRunningWorkflows=0;
		int actualKilledWorkflows=0;
		int actualSucceededWorkflows=0;
		Util.print("was: "+was);
		for(int instanceIndex = 0 ; instanceIndex < was.size(); instanceIndex++)
		{
			Util.print("was.get("+instanceIndex+").getStatus(): "+was.get(instanceIndex).getStatus()+" , "+was.get(instanceIndex).getName());

			if(was.get(instanceIndex).getStatus().toString().equals("RUNNING"))
				actualRunningWorkflows++;
			else if(was.get(instanceIndex).getStatus().toString().equals("KILLED"))
				actualKilledWorkflows++;
			else if(was.get(instanceIndex).getStatus().toString().equals("SUCCEEDED"))
				actualSucceededWorkflows++;
		}
		if(runningWorkflows!=-1)
			Assert.assertEquals(actualRunningWorkflows, runningWorkflows);
		if(killedWorkflows!=-1)
			Assert.assertEquals(actualKilledWorkflows, killedWorkflows);
		if(succeededWorkflows!=-1)
			Assert.assertEquals(actualSucceededWorkflows, succeededWorkflows);

	}


	public static List<CoordinatorAction> getProcessInstanceList(ColoHelper coloHelper,String processName,ENTITY_TYPE entityType) throws Exception{

		XOozieClient oozieClient=new XOozieClient(coloHelper.getProcessHelper().getOozieURL());
		String coordId = getLatestCoordinatorID(coloHelper,processName,entityType);
		//String coordId = getDefaultCoordinatorFromProcessName(processName);
		Util.print("default coordID: "+ coordId);
		return oozieClient.getCoordJobInfo(coordId).getActions();
	}        

	public static List<CoordinatorAction> getProcessInstanceList(ColoHelper coloHelper,String processName,String entityType) throws Exception{

		XOozieClient oozieClient=new XOozieClient(coloHelper.getProcessHelper().getOozieURL());
		String coordId = getLatestCoordinatorID(coloHelper,processName,entityType);
		//String coordId = getDefaultCoordinatorFromProcessName(processName);
		Util.print("default coordID: "+ coordId);
		return oozieClient.getCoordJobInfo(coordId).getActions();
	}   
	public static String getLatestCoordinatorID(ColoHelper coloHelper,String processName,String entityType) throws  Exception {
		return getDefaultCoordIDFromBundle(coloHelper,getLatestBundleID(coloHelper,processName,entityType));
	}
	public static String getLatestCoordinatorID(ColoHelper coloHelper,String processName,ENTITY_TYPE entityType) throws  Exception {
		return getDefaultCoordIDFromBundle(coloHelper,getLatestBundleID(coloHelper,processName,entityType));
	}



	public static String getDefaultCoordIDFromBundle(ColoHelper coloHelper,String bundleId) throws OozieClientException {

		XOozieClient oozieClient=new XOozieClient(coloHelper.getProcessHelper().getOozieURL());
		BundleJob bundleInfo = oozieClient.getBundleJobInfo(bundleId);
		List<CoordinatorJob> coords = bundleInfo.getCoordinators();
		int min = 100000 ;
		String minString="";
		for(int i = 0 ; i <coords.size() ; i++ )
		{
			String strID = coords.get(i).getId();
			if(min > Integer.parseInt(strID.substring(0, strID.indexOf("-"))))
			{
				min = Integer.parseInt(strID.substring(0, strID.indexOf("-")));
				minString= coords.get(i).getId();
			}
		}

		Util.print("function getDefaultCoordIDFromBundle: minString: "+minString);
		return minString;

	}


	public static ArrayList<WorkflowAction> getWorkflowActions(PrismHelper prismHelper,
			String processName) throws Exception {
		XOozieClient oozieClient=new XOozieClient(prismHelper.getProcessHelper().getOozieURL());

		String bundleID = Util.getCoordID(Util.getOozieJobStatus(prismHelper,processName,"NONE").get(0));
		ArrayList<WorkflowAction> was = new ArrayList<WorkflowAction>();
		ArrayList<String> workflows=Util.getCoordinatorJobs(prismHelper,bundleID,"any");		

		for(String jobID : workflows)
		{

			was.add(oozieClient.getWorkflowActionInfo(oozieClient.getCoordActionInfo(jobID).getExternalId()));
		}
		return was;
	}

	public static void validateNumberOfInstanceWithStatus(List<CoordinatorAction> list,CoordinatorAction.Status status, int noOfInstance) {

		int actualInstance = 0 ;
		for(int i = 0 ; i < list.size();i++ ){

			if(list.get(i).getStatus().equals(status))
				actualInstance++;
		}
		Assert.assertEquals(actualInstance,noOfInstance,"number if process instance with desired status did not match");
	}

	public static int getInstanceCountWithStatus(ColoHelper coloHelper,String processName,
			org.apache.oozie.client.CoordinatorAction.Status status,ENTITY_TYPE entityType) throws Exception {
		List<CoordinatorAction>	list = getProcessInstanceList(coloHelper,processName,entityType);
		int instanceCount = 0 ;
		for(int i = 0 ; i < list.size();i++ ){

			if(list.get(i).getStatus().equals(status))
				instanceCount++;
		}
		return instanceCount;

	}
	public static int getInstanceCountWithStatus(ColoHelper coloHelper,String processName,
			org.apache.oozie.client.CoordinatorAction.Status status,String entityType) throws Exception {
		List<CoordinatorAction>	list = getProcessInstanceList(coloHelper,processName,entityType);
		int instanceCount = 0 ;
		for(int i = 0 ; i < list.size();i++ ){

			if(list.get(i).getStatus().equals(status))
				instanceCount++;
		}
		return instanceCount;

	}


	public static String getTimeWrtSystemTime(int minutes) throws Exception {

		DateTime jodaTime = new DateTime(DateTimeZone.UTC);
		if(minutes>0)
			jodaTime = jodaTime.plusMinutes(minutes);
		else
			jodaTime = jodaTime.minusMinutes(-1*minutes);

		DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy'-'MM'-'dd'T'HH':'mm'Z'");
		String str = fmt.print(jodaTime);
		return str;
	}

	public static String addMinsToTime(String time, int minutes) throws ParseException {

		DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy'-'MM'-'dd'T'HH':'mm'Z'");
		DateTime jodaTime = fmt.parseDateTime(time);

		if(minutes>0)
			jodaTime = jodaTime.plusMinutes(minutes);
		else
			jodaTime = jodaTime.minusMinutes(-1*minutes);

		String str = fmt.print(jodaTime);
		return str;
	}



	public static Status getDefaultCoordinatorStatus(ColoHelper colohelper,String processName,int bundleNumber) throws Exception {

		XOozieClient oozieClient=new XOozieClient(colohelper.getProcessHelper().getOozieURL());
		String coordId = getDefaultCoordinatorFromProcessName(colohelper,processName,bundleNumber);
		return oozieClient.getCoordJobInfo(coordId).getStatus();
	}        



	public static String getDefaultCoordinatorFromProcessName(
			ColoHelper coloHelper,String processName,int bundleNumber) throws Exception {
		//String bundleId = Util.getCoordID(Util.getOozieJobStatus(processName,"NONE").get(0));
		String bundleID = getSequenceBundleID(coloHelper,processName,ENTITY_TYPE.PROCESS,bundleNumber);
		return getDefaultCoordIDFromBundle(coloHelper,bundleID);
	}        



	public static String getBundleCoordinator(ColoHelper coloHelper,String bundleID) throws Exception {
		String defaultCoordId = getDefaultCoordIDFromBundle(coloHelper,bundleID);
		Util.print("function getBundleCoordinator: defaultCoordId = getDefaultCoordIDFromBundle(bundleID): "+defaultCoordId);
		return getCoordWorkFlowCoordinator(coloHelper,defaultCoordId);
	}        

	public static List<CoordinatorJob> getBundleCoordinators(String bundleID,IEntityManagerHelper helper) throws Exception {
		XOozieClient localOozieClient=new XOozieClient(helper.getOozieURL());
		BundleJob bundleInfo = localOozieClient.getBundleJobInfo(bundleID);
		List<CoordinatorJob> coords = bundleInfo.getCoordinators();
		return coords;
	}



	public static String getCoordWorkFlowCoordinator(ColoHelper colo,String defaultCoordId) throws Exception {
		System.setProperty("java.security.krb5.realm", "");
		System.setProperty("java.security.krb5.kdc", "");

		Util.print("function getCoordWorkFlowCoordinator: "+defaultCoordId);
		String coordHdfsPath = getCoordHdfsPath(defaultCoordId);
		Path path = new Path(coordHdfsPath+"/coordinator.xml");
		Configuration conf=new Configuration();
		conf.set("fs.default.name","hdfs://"+Util.readPropertiesFile(colo.getEnvFileName(),"hadoop_url"));
		final FileSystem fs=FileSystem.get(conf);
		//FSDataInputStream in = fs.open(path);

		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(path)));
		String line;
		line=br.readLine();
		String strFile = new String();
		while (line != null){
			// logger.info(line);
			if(line!=null)
				strFile = strFile+line.trim();
			line=br.readLine();
		}

		return strFile;
	}

	public static String getCoordWorkFlowCoordinator(PrismHelper prismHelper,String defaultCoordId) throws Exception {
		System.setProperty("java.security.krb5.realm", "");
		System.setProperty("java.security.krb5.kdc", "");

		Util.print("function getCoordWorkFlowCoordinator: "+defaultCoordId);
		String coordHdfsPath = getCoordHdfsPath(prismHelper,defaultCoordId);
		Path path = new Path(coordHdfsPath+"/coordinator.xml");
		Configuration conf=new Configuration();
		conf.set("fs.default.name","hdfs://"+prismHelper.getProcessHelper().getHadoopURL());
		final FileSystem fs=FileSystem.get(conf);
		//FSDataInputStream in = fs.open(path);

		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(path)));
		String line;
		line=br.readLine();
		String strFile = new String();
		while (line != null){
			// logger.info(line);
			if(line!=null)
				strFile = strFile+line.trim();
			line=br.readLine();
		}

		return strFile;
	}        

	public static String getCoordHdfsPath(String CoordId) throws OozieClientException, InterruptedException {
		CoordinatorJob coordJob =oozieClient.getCoordJobInfo(CoordId);

		for(int i= 0 ; i < 30 ; i++)
		{
			if(coordJob.getActions().size()>0)
				break;

			Thread.sleep(45000);
			coordJob =oozieClient.getCoordJobInfo(CoordId);
		}

		if(coordJob.getActions().size()<1)
			Assert.assertTrue(false,"function getCoordHdfsPath: coordJob.getActions().size() was less than 1");

		String eventConf = coordJob.getActions().get(0).getRunConf();
		//Util.print(eventConf);
		eventConf =eventConf.replaceAll("\r\n","");
		eventConf =eventConf.replaceAll(" ","");
		eventConf = eventConf.substring(eventConf.indexOf("<name>oozie.coord.application.path</name><value>")+"<name>oozie.coord.application.path</name><value>".length());
		int endIndex = eventConf.indexOf("</value>");
		String hdfsPath =eventConf.substring(0,endIndex );
		return hdfsPath;
	}

	public static String getCoordHdfsPath(PrismHelper prismHelper,String CoordId) throws OozieClientException, InterruptedException {

		XOozieClient oozieClient=new XOozieClient(prismHelper.getClusterHelper().getOozieURL());
		CoordinatorJob coordJob =oozieClient.getCoordJobInfo(CoordId);

		for(int i= 0 ; i < 30 ; i++)
		{
			if(coordJob.getActions().size()>0)
				break;

			Thread.sleep(45000);
			coordJob =oozieClient.getCoordJobInfo(CoordId);
		}

		if(coordJob.getActions().size()<1)
			Assert.assertTrue(false,"function getCoordHdfsPath: coordJob.getActions().size() was less than 1");

		String eventConf = coordJob.getActions().get(0).getRunConf();
		//Util.print(eventConf);
		eventConf =eventConf.replaceAll("\r\n","");
		eventConf =eventConf.replaceAll(" ","");
		eventConf = eventConf.substring(eventConf.indexOf("<name>oozie.coord.application.path</name><value>")+"<name>oozie.coord.application.path</name><value>".length());
		int endIndex = eventConf.indexOf("</value>");
		String hdfsPath =eventConf.substring(0,endIndex );
		return hdfsPath;
	}        




	public static String getLatestBundleID(ColoHelper coloHelper,String processName,ENTITY_TYPE entityType) throws Exception {
		ArrayList<String> bundleIds = Util.getBundles(coloHelper, processName, entityType);

		String max = "";
		int maxID = -1;
		for(int i =0 ; i < bundleIds.size() ; i++)
		{
			String strID = bundleIds.get(i);
			if(maxID < Integer.parseInt(strID.substring(0, strID.indexOf("-"))))
			{
				maxID = Integer.parseInt(strID.substring(0, strID.indexOf("-")));
				max= bundleIds.get(i);
			}
		}
		return max;
	}        

	public static String getLatestBundleID(ColoHelper coloHelper,String processName,String entityType) throws Exception {
		ArrayList<String> bundleIds = Util.getBundles(coloHelper, processName, entityType);

		String max = "";
		int maxID = -1;
		for(int i =0 ; i < bundleIds.size() ; i++)
		{
			String strID = bundleIds.get(i);
			if(maxID < Integer.parseInt(strID.substring(0, strID.indexOf("-"))))
			{
				maxID = Integer.parseInt(strID.substring(0, strID.indexOf("-")));
				max= bundleIds.get(i);
			}
		}
		return max;
	}        

	public static String getLatestBundleID(String processName,String entityType,IEntityManagerHelper helper) throws Exception {
		ArrayList<String> bundleIds = Util.getBundles(processName,entityType,helper);

		String max = "";
		int maxID = -1;
		for(int i =0 ; i < bundleIds.size() ; i++)
		{
			String strID = bundleIds.get(i);
			if(maxID < Integer.parseInt(strID.substring(0, strID.indexOf("-"))))
			{
				maxID = Integer.parseInt(strID.substring(0, strID.indexOf("-")));
				max= bundleIds.get(i);
			}
		}
		return max;
	}

	public static String getLatestCoordinator(ColoHelper coloHelper,String processName,ENTITY_TYPE entityType) throws Exception {
		return  getBundleCoordinator(coloHelper,getLatestBundleID(coloHelper,processName,entityType));
	}

	public static String getProcessWorkFlowPath(ColoHelper coloHelper,String processName,ENTITY_TYPE entityType) throws Exception {
		String coordinator = getLatestCoordinator(coloHelper,processName,entityType);
		coordinator =coordinator.replaceAll("\r\n","");
		coordinator =coordinator.replaceAll(" ","");
		coordinator= coordinator.substring(coordinator.indexOf("<property><name>oozie.libpath</name><value>${nameNode}")+"<property><name>oozie.libpath</name><value>${nameNode}".length());
		return coordinator.substring(0, coordinator.indexOf("/lib</value>"));

	}

	//	public static COORDINATORAPP getLatestCoordinatorApp(String processName,String entityType) throws Exception {
	//		String bundleID = getLatestBundleID(processName,entityType);
	//		Util.print("fuction getLatestCoordinatorApp: bundleID = getLatestBundleID(processName,entityType) "+bundleID);
	//		String coordStr = getBundleCoordinator(bundleID);
	//		JAXBContext jc=JAXBContext.newInstance(COORDINATORAPP.class);
	//		Unmarshaller u=jc.createUnmarshaller();
	//
	//		@SuppressWarnings("unchecked")
	//		COORDINATORAPP coord=((JAXBElement<COORDINATORAPP>)u.unmarshal(new StringReader(coordStr))).getValue();
	//
	//		logger.info(coord.getName());
	//		return coord;
	//	}

	public static COORDINATORAPP getLatestCoordinatorApp(ColoHelper coloHelper,String processName,ENTITY_TYPE entityType) throws Exception {
		String bundleID = getLatestBundleID(coloHelper,processName,entityType);
		Util.print("fuction getLatestCoordinatorApp: bundleID = getLatestBundleID(processName,entityType) "+bundleID);
		String coordStr = getBundleCoordinator(coloHelper,bundleID);
		JAXBContext jc=JAXBContext.newInstance(COORDINATORAPP.class);
		Unmarshaller u=jc.createUnmarshaller();

		@SuppressWarnings("unchecked")
		COORDINATORAPP coord=((JAXBElement<COORDINATORAPP>)u.unmarshal(new StringReader(coordStr))).getValue();

		logger.info(coord.getName());
		return coord;
	}        

	public static COORDINATORAPP getCoordinatorApp(
			ColoHelper coloHelper,String entityName, String entityType,int coordNumber) throws Exception {
		String bundleID = getSequenceBundleID(entityName,entityType,coordNumber);
		String coordStr = getBundleCoordinator(coloHelper,bundleID);
		JAXBContext jc=JAXBContext.newInstance(COORDINATORAPP.class);
		Unmarshaller u=jc.createUnmarshaller();

		@SuppressWarnings("unchecked")
		COORDINATORAPP coord=((JAXBElement<COORDINATORAPP>)u.unmarshal(new StringReader(coordStr))).getValue();

		logger.info(coord.getName());
		return coord;
	}

	public static String getSequenceBundleID(String entityName,
			String entityType, int coordNumber) throws Exception {

		ArrayList<String> bundleIds = Util.getBundles(entityName,entityType);
		Map <Integer, String> bundleMap = new TreeMap<Integer, String>();
		String bundleID ="";
		for(int i =0 ; i < bundleIds.size() ; i++)
		{
			String strID = bundleIds.get(i);
			Util.print("getSequenceBundleID: "+ strID);
			int key = Integer.parseInt(strID.substring(0, strID.indexOf("-")));
			bundleMap.put(key, strID);
		}

		for (Map.Entry<Integer, String> entry : bundleMap.entrySet()) {
			logger.info("Key = " + entry.getKey() + ", Value = " + entry.getValue());
		}

		int i = 0 ;
		for(Integer key : bundleMap.keySet())
		{
			bundleID = bundleMap.get(key);
			if(i==coordNumber)
				return bundleID;
			i++;
		}
		return null;
	}

	public static String getSequenceBundleID(PrismHelper prismHelper,String entityName,
			ENTITY_TYPE entityType, int bundleNumber) throws Exception {

		ArrayList<String> bundleIds = Util.getBundles(prismHelper,entityName,entityType);
		Map <Integer, String> bundleMap = new TreeMap<Integer, String>();
		String bundleID ="";
		for(int i =0 ; i < bundleIds.size() ; i++)
		{
			String strID = bundleIds.get(i);
			Util.print("getSequenceBundleID: "+ strID);
			int key = Integer.parseInt(strID.substring(0, strID.indexOf("-")));
			bundleMap.put(key, strID);
		}

		for (Map.Entry<Integer, String> entry : bundleMap.entrySet()) {
			logger.info("Key = " + entry.getKey() + ", Value = " + entry.getValue());
		}

		int i = 0 ;
		for(Integer key : bundleMap.keySet())
		{
			bundleID = bundleMap.get(key);
			if(i==bundleNumber)
				return bundleID;
			i++;
		}
		return null;
	}        

	public static String getProcessInstanceNominalTime(ColoHelper coloHelper,String entityName, int bundleNumber,
			int instanceNumber,String entityType) throws Exception {

		String bundleID = getSequenceBundleID(entityName,entityType,bundleNumber);
		String coordID = getDefaultCoordIDFromBundle(coloHelper,bundleID);
		return getNominalTimeOfInstance(coordID,instanceNumber);
	}

	public static String getNominalTimeOfInstance(String coordID,int instanceNumber) throws Exception{
		CoordinatorJob coordInfo = oozieClient.getCoordJobInfo(coordID);
		List<CoordinatorAction> instanceList = coordInfo.getActions();
		return dateToOozieDate(instanceList.get(instanceNumber).getNominalTime());

	}

	public static String dateToOozieDate(Date dt) throws ParseException {

		/*DateFormat formatter = Util.getIvoryDateFormat();
		Calendar cal = Calendar.getInstance();
		cal.setTime(dt);
		TimeZone z = cal.getTimeZone();
		int offset = z.getRawOffset();
		int offsetHrs = offset / 1000 / 60 / 60;
		int offsetMins = offset / 1000 / 60 % 60;
		cal.add(Calendar.HOUR_OF_DAY, (-offsetHrs));
		cal.add(Calendar.MINUTE, (-offsetMins));		
		return formatter.format(cal.getTime());*/
		DateTime jodaTime = new DateTime(dt,DateTimeZone.UTC);
		Util.print("jadaSystemTime: "+jodaTime );
		DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy'-'MM'-'dd'T'HH':'mm'Z'");
		String str = fmt.print(jodaTime);
		return str;
	}




	public static ArrayList<String> getCommonInstancesInBundles(ColoHelper coloHelper,String BundleID1,
			String BundleID2) throws Exception {
		ArrayList<String> startInstanceList1 = getBundleInsatnceStartTimeList(coloHelper,BundleID1);
		ArrayList<String> startInstanceList2 = getBundleInsatnceStartTimeList(coloHelper,BundleID2);
		startInstanceList1.retainAll(startInstanceList2);
		return startInstanceList1;



	}



	public static ArrayList<String> getBundleInsatnceStartTimeList(
			ColoHelper coloHelper,String bundleID) throws Exception {

		return  getCoordInsatnceStartTimeList(instanceUtil.getDefaultCoordIDFromBundle(coloHelper,bundleID));

	}


	public static ArrayList<String> getCoordInsatnceStartTimeList(
			String coordId) throws Exception {
		CoordinatorJob coordInfo = oozieClient.getCoordJobInfo(coordId);
		ArrayList<String> list = new ArrayList<String>();
		Util.print("coordID: "+coordId);
		List<CoordinatorAction> actionList = coordInfo.getActions();

		for(int i = 0 ; i < actionList.size(); i++)
		{
			Util.print("actionTime: "+instanceUtil.dateToOozieDate(actionList.get(i).getNominalTime()));
			list.add(instanceUtil.dateToOozieDate(actionList.get(i).getCreatedTime()));
		}
		return list;
	}

	public static ArrayList<String> getCoordInsatnceStartTimeList(
			PrismHelper prismHelper,String coordId) throws Exception {

		XOozieClient oozieClient=new XOozieClient(prismHelper.getProcessHelper().getOozieURL());
		CoordinatorJob coordInfo = oozieClient.getCoordJobInfo(coordId);
		ArrayList<String> list = new ArrayList<String>();
		Util.print("coordID: "+coordId);
		List<CoordinatorAction> actionList = coordInfo.getActions();

		for(int i = 0 ; i < actionList.size(); i++)
		{
			Util.print("actionTime: "+instanceUtil.dateToOozieDate(actionList.get(i).getNominalTime()));
			list.add(instanceUtil.dateToOozieDate(actionList.get(i).getCreatedTime()));
		}
		return list;
	}        

	public static CoordinatorAction.Status getInstanceStatus(ColoHelper coloHelper,String processName, int bundleNumber, int instanceNumber) throws Exception {
		String bundleID = instanceUtil.getSequenceBundleID(coloHelper,processName,ENTITY_TYPE.PROCESS, bundleNumber);
		String coordID = instanceUtil.getDefaultCoordIDFromBundle(coloHelper,bundleID);
		XOozieClient oozieClient=new XOozieClient(coloHelper.getProcessHelper().getOozieURL());
		CoordinatorJob coordInfo = oozieClient.getCoordJobInfo(coordID);
		return coordInfo.getActions().get(instanceNumber).getStatus();

	}

	public static String getLateCoordIDFromProcess(String processName, int bundleNumber) throws Exception
	{
		String bundleID = instanceUtil.getSequenceBundleID(processName, "PROCESS", bundleNumber);
		return instanceUtil.getLateCoordFromBundle(bundleID);
	}

	public static String getLateCoordFromBundle(String bundleID) throws Exception{
		BundleJob bundleInfo = oozieClient.getBundleJobInfo(bundleID);
		List<CoordinatorJob> coords = bundleInfo.getCoordinators();
		int max = -1 ;
		String maxString="";
		for(int i = 0 ; i <coords.size() ; i++ )
		{
			String strID = coords.get(i).getId();
			if(max < Integer.parseInt(strID.substring(0, strID.indexOf("-"))))
			{
				max = Integer.parseInt(strID.substring(0, strID.indexOf("-")));
				maxString= coords.get(i).getId();
			}
		}
		return maxString;
	}

	public static COORDINATORAPP getLateCoordinatorApp(ColoHelper colo,
			String entityName,int coordNumber) throws Exception {
		String coordStr = getCoordWorkFlowCoordinator(colo,getLateCoordIDFromProcess(entityName,coordNumber));
		JAXBContext jc=JAXBContext.newInstance(COORDINATORAPP.class);
		Unmarshaller u=jc.createUnmarshaller();

		@SuppressWarnings("unchecked")
		COORDINATORAPP coord=((JAXBElement<COORDINATORAPP>)u.unmarshal(new StringReader(coordStr))).getValue();

		logger.info(coord.getName());
		return coord;
	}

	public static ArrayList<String> getInputFoldersForInstance(ColoHelper coloHelper,String processName, int bundleNumber,
			int instanceNumber,boolean isGated) throws Exception {

		String bundleID = instanceUtil.getSequenceBundleID(coloHelper,processName,ENTITY_TYPE.PROCESS, bundleNumber);
		String coordID = instanceUtil.getDefaultCoordIDFromBundle(coloHelper,bundleID);
		XOozieClient oozieClient=new XOozieClient(coloHelper.getProcessHelper().getOozieURL());
		CoordinatorJob coordInfo = oozieClient.getCoordJobInfo(coordID);

		return instanceUtil.getInputFolderPathFromInstanceRunConf(oozieClient.getJobInfo(coordInfo.getActions().get(instanceNumber).getExternalId()).getConf(),isGated);

	}

	public static ArrayList<String> getInputFolderPathFromInstanceRunConf(
			String runConf,boolean isGated) {
		String conf = "";


		if(runConf.indexOf("input</name>")!=-1)
			conf = runConf.substring(runConf.indexOf("input</name>")+12);
		else if(runConf.indexOf("ivoryInPaths</name>")!=-1)
			conf = runConf.substring(runConf.indexOf("ivoryInPaths</name>")+19);
		else{
			Util.print("cound not find any input path in runTimeConf");
			return null;
		}



		//	Util.print("conf1: "+conf);


		conf = conf.substring(conf.indexOf("<value>")+7);
		//	Util.print("conf2: "+conf);

		conf = conf.substring(0,conf.indexOf("</value>"));
		Util.print("conf3: "+conf);

		//	return new ArrayList<String>(Arrays.asList(conf.split(",")));

		ArrayList<String> inputWise = new ArrayList<String>(Arrays.asList(conf.split("#")));

		ArrayList<String> returnObject  = new ArrayList<String>();

		/*	int size = 0;
		if(isGated)
			size = inputWise.size()-1;
		else
			size = inputWise.size();*/

		int size = inputWise.size();

		//inputWise.get(0).replaceAll("*/","");

		for(int i = 0 ; i <size; i++ )
		{
			returnObject.addAll(Arrays.asList(inputWise.get(i).split(",")));
		}


		for(int i = 0 ; i <size; i++ )
		{
			if(returnObject.get(i).contains("*"))
				returnObject.set(i, returnObject.get(i).substring(0, returnObject.get(i).indexOf("*")-1));
		}


		return returnObject;
	}

	public static void putDataInFolders(ColoHelper colo,
			final ArrayList<String> inputFoldersForInstance) throws Exception {

		for(int i = 0 ; i < inputFoldersForInstance.size() ; i++)
			putDataInFolder(colo,inputFoldersForInstance.get(i));

	}

	public static void putDataInFolders(ColoHelper colo,
			final ArrayList<String> inputFoldersForInstance,String type) throws Exception {

		for(int i = 0 ; i < inputFoldersForInstance.size() ; i++)
			putDataInFolder(colo,inputFoldersForInstance.get(i),type);

	}


	public static void putDataInFolder(ColoHelper colo,final String remoteLocation) throws Exception {

		Configuration conf=new Configuration();
		conf.set("fs.default.name","hdfs://"+Util.readPropertiesFile(colo.getEnvFileName(),"hadoop_url"));
		//System.out.println("prop: "+conf.get("fs.default.name"));

		final FileSystem fs=FileSystem.get(conf);
		//System.out.println("fs uri: "+fs.getUri());

		UserGroupInformation user = UserGroupInformation.createRemoteUser("hdfs");


		File [] files=new File("src/test/resources/OozieExampleInputData/normalInput").listFiles();
		//System.out.println("files: "+files);
		for(final File file:files)
		{
			if(!file.isDirectory())
			{
			   // System.out.println("inside if block");
				user.doAs(new PrivilegedExceptionAction<Boolean>() {

					@Override
					
					public Boolean run() throws Exception {
					    
						Util.print("putDataInFolder: "+remoteLocation);
						fs.copyFromLocalFile(new Path(file.getAbsolutePath()),new Path(remoteLocation));
						return true;

					}
				}); 
			}
		}

	}



	public static void putDataInFolder(ColoHelper colo,final String remoteLocation,String type) throws Exception {

		Configuration conf=new Configuration();
		conf.set("fs.default.name","hdfs://"+Util.readPropertiesFile(colo.getEnvFileName(),"hadoop_url"));
		File[] files = null;

		final FileSystem fs=FileSystem.get(conf);

		UserGroupInformation user = UserGroupInformation.createRemoteUser("hdfs");

		if("late".equals(type))
			files=new File("src/test/resources/lateData").listFiles();
		else 
			files=new File("src/test/resources/OozieExampleInputData/normalInput").listFiles();	

		for(final File file:files)
		{
			if(!file.isDirectory())
			{
				user.doAs(new PrivilegedExceptionAction<Boolean>() {

					@Override
					public Boolean run() throws Exception {
						Util.print("putDataInFolder: "+remoteLocation);
						fs.copyFromLocalFile(new Path(file.getAbsolutePath()),new Path(remoteLocation));
						return true;

					}
				}); 
			}
		}

	}

	public static CoordinatorAction.Status getLateInstanceStatus(String processName, int bundleNumber, int instanceNumber) throws Exception {
		String bundleID = instanceUtil.getSequenceBundleID(processName,"PROCESS", bundleNumber);
		String coordID = instanceUtil.getLateCoordFromBundle(bundleID);
		CoordinatorJob coordInfo = oozieClient.getCoordJobInfo(coordID);
		return coordInfo.getActions().get(instanceNumber).getStatus();

	}

	public static void sleepTill(PrismHelper prismHelper,String startTimeOfLateCoord) throws Exception {

		DateTime finalDate = new DateTime(instanceUtil.oozieDateToDate(startTimeOfLateCoord));

		while(true)
		{
			DateTime sysDate =  new DateTime(Util.getSystemDate(prismHelper));
			Util.print("sysDate: "+sysDate+ "  finalDate: "+finalDate);
			if(sysDate.compareTo(finalDate) > 0)
				break;

			Thread.sleep(15000);
		}

	}

	public static DateTime oozieDateToDate(String time) throws ParseException {
		DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy'-'MM'-'dd'T'HH':'mm'Z'");
		fmt = fmt.withZoneUTC();
		return fmt.parseDateTime(time);
	}

	public static DateTime stringToDate(String time,String format) throws ParseException {
		DateTimeFormatter fmt = DateTimeFormat.forPattern(format);
		fmt = fmt.withZoneUTC();
		return fmt.parseDateTime(time);
	}

	public static String dateToOozieDateWOOffSet(Date startTime) {

		DateFormat formatter = Util.getIvoryDateFormat();
		Calendar cal = Calendar.getInstance();
		cal.setTime(startTime);
		return formatter.format(cal.getTime());
	}

	public static long diffbw2time(String time1, String time2) throws ParseException {
		DateFormat formatter = Util.getIvoryDateFormat();
		Date dt1 = formatter.parse(time1);
		Date dt2 = formatter.parse(time2);

		Calendar calendar1 = Calendar.getInstance();
		Calendar calendar2 = Calendar.getInstance();
		calendar1.setTime(dt1);
		calendar2.setTime(dt2);
		long milliseconds1 = calendar1.getTimeInMillis();
		long milliseconds2 = calendar2.getTimeInMillis();
		long diff = milliseconds2 - milliseconds1;
		long diffMinutes = diff / (60 * 1000);

		return diffMinutes;

	}

	public static String parseCLIresponse(String cliKill) {
		return cliKill.replaceAll("\n"," ");
	}


	public static void createHDFSFolders(PrismHelper helper,ArrayList<String> folderList) throws Exception {
		logger.info("creating folders.....");



		Configuration conf=new Configuration();
		conf.set("fs.default.name","hdfs://"+helper.getFeedHelper().getHadoopURL());

		final FileSystem fs=FileSystem.get(conf);


		UserGroupInformation user= UserGroupInformation.createRemoteUser("rishu");

		for(final String folder:folderList)
		{
			user.doAs(new PrivilegedExceptionAction<Boolean>() {

				@Override
				public Boolean run() throws Exception {
					return fs.mkdirs(new Path(folder));

				}
			}); 
		}

		logger.info("created folders.....");

	}


	public static void putFileInFolders(ColoHelper colo,ArrayList<String> folderList,
			final String... fileName) throws Exception {
		Configuration conf=new Configuration();
		conf.set("fs.default.name",Util.readPropertiesFile(colo.getEnvFileName(),"cluster_write"));

		final FileSystem fs=FileSystem.get(conf);

		UserGroupInformation user = UserGroupInformation.createRemoteUser("rishu");


		for(final String folder:folderList)
		{

			user.doAs(new PrivilegedExceptionAction<Boolean>() {

				@Override
				public Boolean run() throws Exception {
					for(int i = 0 ; i <fileName.length ; i++ ){
						logger.info("copying  "+fileName[i]+" to "+folder);
						if(fileName[i].equals("_SUCCESS"))
							fs.mkdirs(new Path(folder+"/_SUCCESS"));
						else
							fs.copyFromLocalFile(new Path(fileName[i]),new Path(folder));
					}
					return true;

				}
			}); 
		}
	}

	public static ArrayList<String> getMissingDependencyForInstance(ColoHelper colo,
			String processName, int bundleNumber, int instanceNumber) throws Exception {
		XOozieClient oozieClient=new XOozieClient(Util.readPropertiesFile(colo.getEnvFileName(),"oozie_url"));
		String bundleID = instanceUtil.getSequenceBundleID(processName, "PROCESS",0);

		BundleJob bundleJob = oozieClient.getBundleJobInfo(bundleID);
		CoordinatorJob jobInfo = oozieClient.getCoordJobInfo(bundleJob.getCoordinators().get(0).getId());
		List<CoordinatorAction> actions = jobInfo.getActions();

		Util.print("conf from event: "+actions.get(instanceNumber).getMissingDependencies());

		String[] missingDependencies = actions.get(instanceNumber).getMissingDependencies().split("#");
		return new ArrayList<String>(Arrays.asList(missingDependencies));
	}

	public static ArrayList<String> getFolderlistFromDependencyList(
			ArrayList<String> missingDependencyList) {

		for(int i = 0 ; i <missingDependencyList.size();i++ )
		{
			missingDependencyList.set(i,missingDependencyList.get(i).substring(0, missingDependencyList.get(i).lastIndexOf("/")+1));
		}

		return missingDependencyList;
	}

	/*	public static void createDataWithinDatesAndPrefix(DateTime startDateJoda,
			DateTime endDateJoda, String prefix,int interval) throws Exception {
		List<String> dataDates = Util.getMinuteDatesOnEitherSide(startDateJoda,endDateJoda,interval);

		for(int i = 0 ; i < dataDates.size(); i++)
			dataDates.set(i, prefix + dataDates.get(i));

		ArrayList<String> dataFolder = new ArrayList<String>();

		for(int i = 0 ; i < dataDates.size(); i++)
			dataFolder.add(dataDates.get(i));

		instanceUtil.putDataInFolders(dataFolder);

	}*/
	public static void createDataWithinDatesAndPrefix(ColoHelper colo, DateTime startDateJoda,
			DateTime endDateJoda, String prefix,int interval) throws Exception {
		List<String> dataDates = Util.getMinuteDatesOnEitherSide(startDateJoda,endDateJoda,interval);

		for(int i = 0 ; i < dataDates.size(); i++)
			dataDates.set(i, prefix + dataDates.get(i));

		ArrayList<String> dataFolder = new ArrayList<String>();

		for(int i = 0 ; i < dataDates.size(); i++)
			dataFolder.add(dataDates.get(i));

		instanceUtil.putDataInFolders(colo,dataFolder);

	}
	public static void verifyStatusLog(ProcessInstancesResult r) throws Exception {

		for(ProcessInstancesResult.ProcessInstance instance:r.getInstances()){


			if(instance.getStatus().equals(ProcessInstancesResult.WorkflowStatus.WAITING)){

				Assert.assertEquals(instance.getLogFile(),"-");
				break;
			}
			else if(instance.getStatus().equals(ProcessInstancesResult.WorkflowStatus.RUNNING) || instance.getStatus().equals(ProcessInstancesResult.WorkflowStatus.SUSPENDED) || instance.getStatus().equals(ProcessInstancesResult.WorkflowStatus.KILLED))
			{
				String workFlowUrl = instance.getLogFile();
				ServiceResponse response = 	Util.sendRequest(workFlowUrl);
				Assert.assertEquals(response.getCode(),200);
				break;

			}

			logger.info(instance.getLogFile());
			Assert.assertTrue(instance.getLogFile().contains("oozie.log"),"oozie.log was not created at correct location");
			String parentLocation = instance.getLogFile().substring(0,instance.getLogFile().indexOf("oozie.log")-1);
			Assert.assertTrue(instanceUtil.isFilePresentInHDFS(parentLocation,"oozie.log"));
			for( ProcessInstancesResult.InstanceAction action:instance.actions){
				logger.info(action.getLogFile());
			}
		}

		/*ProcessInstance[] pArray = r.getInstances();
		Util.print("pArray: "+pArray.toString());

		for(int instanceIndex = 0 ; instanceIndex < pArray.length; instanceIndex++)
		{
			Util.print("pArray["+instanceIndex+"]: "+pArray[instanceIndex].getStatus()+" , "+pArray[instanceIndex].getInstance()+ "  "+pArray[instanceIndex].get);
		}*/

	}

	public static boolean isFilePresentInHDFS(final String parentLocation,
			final String fileName) throws Exception{

		Configuration conf=new Configuration();
		conf.set("fs.default.name",hdfs_url);

		//	final String newParentLocation = "hdfs://mk-qa-63"+parentLocation.substring(parentLocation.indexOf("/data"));
		final String newParentLocation = parentLocation.substring(parentLocation.indexOf("/examples"));


		final FileSystem fs=FileSystem.get(conf);

		UserGroupInformation user = UserGroupInformation.createRemoteUser("rishu");

		user.doAs(new PrivilegedExceptionAction<Boolean>() {

			@Override
			public Boolean run() throws Exception {
				Path p = new Path(newParentLocation+"/"+fileName);
				boolean status = fs.exists(p);
				return status;

			}


		});	
		return true;
	}

	public static com.inmobi.qa.falcon.generated.cluster.Cluster getClusterElement(Bundle bundle) throws JAXBException {
		JAXBContext jc=JAXBContext.newInstance(com.inmobi.qa.falcon.generated.cluster.Cluster.class);
		Unmarshaller u=jc.createUnmarshaller();
		com.inmobi.qa.falcon.generated.cluster.Cluster clusterElement=(com.inmobi.qa.falcon.generated.cluster.Cluster)u.unmarshal((new StringReader(bundle.getClusters().get(0))));

		return clusterElement;
	}

	public static void writeClusterElement(Bundle bundle, com.inmobi.qa.falcon.generated.cluster.Cluster c) throws JAXBException {
		JAXBContext jc=JAXBContext.newInstance(com.inmobi.qa.falcon.generated.cluster.Cluster.class);
		java.io.StringWriter sw = new StringWriter();
		Marshaller marshaller = jc.createMarshaller();
		marshaller.marshal(c,sw);
		logger.info("modified process is: "+sw);
		bundle.setClusterData(sw.toString());
	}


	@Deprecated
	/**
	 * method has been replaced
	 */
	public static String setFeedCluster(String feed, com.inmobi.qa.falcon.generated.feed.Validity v1,
			Retention r1, String n1, ClusterType t1,String partition) throws Exception {


		com.inmobi.qa.falcon.generated.feed.Cluster c1 = new com.inmobi.qa.falcon.generated.feed.Cluster();
		c1.setName(n1);
		c1.setRetention(r1);
		c1.setType(t1);
		c1.setValidity(v1);
		if(partition!=null)
			c1.setPartition(partition);

		Feed f = getFeedElement(feed);

		int numberOfInitialClusters = f.getClusters().getCluster().size();
		if(n1 ==  null)
			for(int i = 0 ; i < numberOfInitialClusters ; i++ )	
				f.getClusters().getCluster().set(i,null);
		else
		{
			f.getClusters().getCluster().add(c1);
		}
		return feedElementToString(f);

	}

	public static String setFeedCluster(String feed, com.inmobi.qa.falcon.generated.feed.Validity v1,
			Retention r1, String n1, ClusterType t1,String partition, String ...locations) throws Exception {

		com.inmobi.qa.falcon.generated.feed.Cluster c1 = new com.inmobi.qa.falcon.generated.feed.Cluster();
		c1.setName(n1);
		c1.setRetention(r1);
		if(t1!=null)
			c1.setType(t1);
		c1.setValidity(v1);
		if(partition!=null)
			c1.setPartition(partition);


		com.inmobi.qa.falcon.generated.feed.Locations ls = new com.inmobi.qa.falcon.generated.feed.Locations();
		if(null!=locations)
		{
			for(int i = 0 ; i < locations.length ; i++ )
			{
				com.inmobi.qa.falcon.generated.feed.Location l = new com.inmobi.qa.falcon.generated.feed.Location();
				l.setPath(locations[i]);
				if(i==0)
					l.setType(LocationType.DATA);
				else if(i==1)
					l.setType(LocationType.STATS);
				else if(i==2)
					l.setType(LocationType.META);
				else if(i==3)
					l.setType(LocationType.TMP);
				else
					Assert.assertTrue(false,"correct value of localtions were not passed");

				ls.getLocation().add(l);
			}

			c1.setLocations(ls);
		}
		Feed f = getFeedElement(feed);

		int numberOfInitialClusters = f.getClusters().getCluster().size();
		if(n1 ==  null)
			for(int i = 0 ; i < numberOfInitialClusters ; i++ )	
				f.getClusters().getCluster().set(i,null);
		else
		{
			f.getClusters().getCluster().add(c1);
		}
		return feedElementToString(f);
	}


/*	public static String setFeedCluster(String feed,Validity v1,
			Retention r1, String n1, ClusterType t1,String partition,Frequency delay ,String ...locations) throws Exception {

		Cluster c1 = new Cluster();
		c1.setName(n1);
		c1.setRetention(r1);
		if(t1!=null)
			c1.setType(t1);
		c1.setValidity(v1);
		if(partition!=null)
			c1.setPartition(partition);
		if(delay!=null)
			c1.setDelay(delay);


		Locations ls = new Locations();
		if(null!=locations)
		{
			for(int i = 0 ; i < locations.length ; i++ )
			{
				Location l = new Location();
				l.setPath(locations[i]);
				if(i==0)
					l.setType(LocationType.DATA);
				else if(i==1)
					l.setType(LocationType.STATS);
				else if(i==2)
					l.setType(LocationType.META);
				else if(i==3)
					l.setType(LocationType.TMP);
				else
					Assert.assertTrue(false,"correct value of localtions were not passed");

				ls.getLocation().add(l);
			}

			c1.setLocations(ls);
		}
		Feed f = getFeedElement(feed);

		int numberOfInitialClusters = f.getClusters().getCluster().size();
		if(n1 ==  null)
			for(int i = 0 ; i < numberOfInitialClusters ; i++ )	
				f.getClusters().getCluster().set(i,null);
		else
		{
			f.getClusters().getCluster().add(c1);
		}
		return feedElementToString(f);
	}
*/
	public static Feed getFeedElement(String feed) throws Exception
	{
		JAXBContext jc=JAXBContext.newInstance(Feed.class); 
		Unmarshaller u=jc.createUnmarshaller();
		return (Feed)u.unmarshal((new StringReader(feed)));
	}

	public static String feedElementToString(Feed feedElement) throws Exception
	{
		JAXBContext jc=JAXBContext.newInstance(Feed.class); 
		java.io.StringWriter sw = new StringWriter();
		Marshaller marshaller = jc.createMarshaller();
		marshaller.marshal(feedElement,sw);
		return sw.toString();
	}

	public static ArrayList<String> getReplicationCoordName(String bundlID,
			IEntityManagerHelper helper) throws Exception {
		List<CoordinatorJob> coords = instanceUtil.getBundleCoordinators(bundlID,helper);

		ArrayList<String> ReplicationCoordName = new ArrayList<String>();
		for(int i  = 0 ; i < coords.size() ; i++)
		{
			if(coords.get(i).getAppName().contains("FEED_REPLICATION"))
				ReplicationCoordName.add(coords.get(i).getAppName());
		}

		return ReplicationCoordName;
	}

	public static String getRetentionCoordName(String bundlID,
			IEntityManagerHelper helper) throws Exception {
		List<CoordinatorJob> coords = instanceUtil.getBundleCoordinators(bundlID,helper);
		String RetentionCoordName = null;
		for(int i  = 0 ; i < coords.size() ; i++)
		{
			if(coords.get(i).getAppName().contains("FEED_RETENTION"))
				return coords.get(i).getAppName();
		}

		return RetentionCoordName;
	}

	public static ArrayList<String> getReplicationCoordID(String bundlID,
			IEntityManagerHelper helper) throws Exception {
		List<CoordinatorJob> coords = instanceUtil.getBundleCoordinators(bundlID,helper);
		ArrayList<String> ReplicationCoordID = new ArrayList<String>();
		for(int i  = 0 ; i < coords.size() ; i++)
		{
			if(coords.get(i).getAppName().contains("FEED_REPLICATION"))
				ReplicationCoordID.add(coords.get(i).getId());
		}

		return ReplicationCoordID;
	}

	public static String getRetentionCoordID(String bundlID,
			IEntityManagerHelper helper) throws Exception {
		List<CoordinatorJob> coords = instanceUtil.getBundleCoordinators(bundlID,helper);
		String RetentionCoordID = null;
		for(int i  = 0 ; i < coords.size() ; i++)
		{
			if(coords.get(i).getAppName().contains("FEED_RETENTION"))
				return coords.get(i).getId();
		}

		return RetentionCoordID;
	}

	public static void verifyDataInTarget(IEntityManagerHelper helper,
			String feed) throws Exception {

		Feed f = instanceUtil.getFeedElement(feed);

		List<com.inmobi.qa.falcon.generated.feed.Cluster> sourceClusters = new ArrayList<com.inmobi.qa.falcon.generated.feed.Cluster>();
		List<com.inmobi.qa.falcon.generated.feed.Cluster> targetClusters = new ArrayList<com.inmobi.qa.falcon.generated.feed.Cluster>();


		List<com.inmobi.qa.falcon.generated.feed.Cluster> clusterList = f.getClusters().getCluster();

		String baseFeedPath = new String();

		for(int i = 0 ; i < f.getLocations().getLocation().size() ; i++)
		{
			if(f.getLocations().getLocation().get(i).getType().equals("data"))
				baseFeedPath = f.getLocations().getLocation().get(i).getPath();
		}

		for(int i =0 ; i < clusterList.size() ; i++)
		{
			if(clusterList.get(i).getType().equals(ClusterType.SOURCE))
				sourceClusters.add(clusterList.get(i));
			else
				targetClusters.add(clusterList.get(i));
		}

		if(sourceClusters.size()<1)
			Assert.assertTrue(false,"feed should have atleast one source cluster");
		if(sourceClusters.size()==1)
		{
			ArrayList<String> result = Util.runRemoteScript(helper.getQaHost(), helper.getUsername(), helper.getPassword(),"hadoop fs -ls "+baseFeedPath);
			Util.print("single cluster remote hadoop ls result: "+result);
		}
		else{

			for(int i = 0 ; i < sourceClusters.size() ; i++)
			{
				ArrayList<String> result = Util.runRemoteScript(helper.getQaHost(), helper.getUsername(), helper.getPassword(),"hadoop fs -ls "+baseFeedPath+sourceClusters.get(i).getPartition());
				Util.print(" multiple cluster remote hadoop ls result: "+result);

			}
		}

	}

	public static void putDataInFolders(PrismHelper helper,
			final ArrayList<String> inputFoldersForInstance) throws Exception {

		for(int i = 0 ; i < inputFoldersForInstance.size() ; i++)
			putDataInFolder(helper,inputFoldersForInstance.get(i));

	}

	public static void putDataInFolder(PrismHelper helper,final String remoteLocation) throws Exception {

		Configuration conf=new Configuration();
		conf.set("fs.default.name","hdfs://"+helper.getFeedHelper().getHadoopURL());


		final FileSystem fs=FileSystem.get(conf);

		UserGroupInformation user = UserGroupInformation.createRemoteUser("hdfs");

		if(!fs.exists(new Path(remoteLocation)))
			fs.mkdirs(new Path(remoteLocation));

		File [] files=new File("src/test/resources/OozieExampleInputData/normalInput").listFiles();
		for(final File file:files)
		{
			if(!file.isDirectory())
			{
				user.doAs(new PrivilegedExceptionAction<Boolean>() {

					@Override
					public Boolean run() throws Exception {
						//Util.print("file.getAbsolutePath(): "+file.getAbsolutePath());
						//Util.print("remoteLocation: "+remoteLocation);


						fs.copyFromLocalFile(new Path(file.getAbsolutePath()),new Path(remoteLocation));
						return true;

					}
				}); 
			}
		}

	}

	public static ProcessInstancesResult createAndsendRequestProcessInstance(
			String url, String params, String colo) throws Exception {

		if (params != null && !colo.equals("")) {
			url = url + params + "&" +colo.substring(1);
		}
		else if(params != null)
		{
			url = url + params ;
		}
		else 
			url = url + colo ;


		return instanceUtil.sendRequestProcessInstance(url);

	}

	public static String getFeedPrefix(String feed) throws Exception {
		Feed feedElement = instanceUtil.getFeedElement(feed);
		String p =feedElement.getLocations().getLocation().get(0).getPath();
		p = p.substring(0,p.indexOf("$"));
		return p;
	}

	public static String setProcessCluster(String process,
			String clusterName, com.inmobi.qa.falcon.generated.process.Validity validity) throws Exception {


		com.inmobi.qa.falcon.generated.process.Cluster c = new com.inmobi.qa.falcon.generated.process.Cluster();

		c.setName(clusterName);
		c.setValidity(validity);

		Process p = instanceUtil.getProcessElement(process);


		if(clusterName ==  null)
			p.getClusters().getCluster().set(0,null);
		else
		{
			p.getClusters().getCluster().add(c);
		}
		return processToString(p);

	}

	public static String processToString(Process p) throws Exception {
		JAXBContext jc=JAXBContext.newInstance(Process.class); 
		java.io.StringWriter sw = new StringWriter();
		Marshaller marshaller = jc.createMarshaller();
		marshaller.marshal(p,sw);
		return sw.toString();
	}




	public static Process getProcessElement(String process) throws JAXBException {
		JAXBContext jc=JAXBContext.newInstance(Process.class); 
		Unmarshaller u=jc.createUnmarshaller();
		Process processElement=(Process)u.unmarshal((new StringReader(process)));

		return processElement;
	}

	public static String addProcessInputFeed(String process,String feed,
			String feedName) throws Exception {
		Process processElement=instanceUtil.getProcessElement(process);	
		Input in1 = processElement.getInputs().getInput().get(0);
		Input in2 = new Input( );
		in2.setEnd(in1.getEnd());
		in2.setFeed(feed);
		in2.setName(feedName);
		in2.setPartition(in1.getPartition());
		in2.setStart(in1.getStart());
		processElement.getInputs().getInput().add(in2);
		return processToString(processElement);
	}

	public static org.apache.oozie.client.WorkflowJob.Status getInstanceStatusFromCoord(ColoHelper ua1,
			String coordID, int instanceNumber) throws Exception {
		XOozieClient oozieClient=new XOozieClient(ua1.getProcessHelper().getOozieURL());
		CoordinatorJob coordInfo = oozieClient.getCoordJobInfo(coordID);
		WorkflowJob actionInfo = oozieClient.getJobInfo(coordInfo.getActions().get(instanceNumber).getExternalId());
		return actionInfo.getStatus();
		//return coordInfo.getActions().get(instanceNumber).getStatus();
	}

	public static ArrayList<String> getInputFoldersForInstanceForReplication(
			ColoHelper coloHelper, String coordID, int instanceNumber) throws OozieClientException {
		XOozieClient oozieClient=new XOozieClient(coloHelper.getProcessHelper().getOozieURL());
		CoordinatorAction x = oozieClient.getCoordActionInfo(coordID+"@"+instanceNumber);
		return instanceUtil.getReplicationFolderFromInstanceRunConf(x.getRunConf());
	}

	public static ArrayList<String> getReplicationFolderFromInstanceRunConf(
			String runConf) {
		String conf = "";
		conf = runConf.substring(runConf.indexOf("ivoryInPaths</name>")+19);
		//	Util.print("conf1: "+conf);

		conf = conf.substring(conf.indexOf("<value>")+7);
		//Util.print("conf2: "+conf);

		conf = conf.substring(0,conf.indexOf("</value>"));
		//Util.print("conf3: "+conf);

		return new ArrayList<String>(Arrays.asList(conf.split(",")));
	}

	public static int getInstanceRunIdFromCoord(ColoHelper colo,
			String coordID, int instanceNumber) throws Exception {
		XOozieClient oozieClient=new XOozieClient(colo.getProcessHelper().getOozieURL());
		CoordinatorJob coordInfo = oozieClient.getCoordJobInfo(coordID);

		WorkflowJob actionInfo = oozieClient.getJobInfo(coordInfo.getActions().get(instanceNumber).getExternalId());
		return actionInfo.getRun();
	}

	public static void putLateDataInFolders(ColoHelper helper,
			ArrayList<String> inputFolderList, int lateDataFolderNumber) throws Exception {

		for(int i = 0 ; i < inputFolderList.size() ; i++)
			putLateDataInFolder(helper,inputFolderList.get(i),lateDataFolderNumber);
	}

	public static void putLateDataInFolder(ColoHelper helper, final String  remoteLocation,
			int lateDataFolderNumber) throws IOException, InterruptedException {

		Configuration conf=new Configuration();
		conf.set("fs.default.name","hdfs://"+helper.getFeedHelper().getHadoopURL());


		final FileSystem fs=FileSystem.get(conf);

		UserGroupInformation user = UserGroupInformation.createRemoteUser("hdfs");


		File [] files=new File("src/test/resources/OozieExampleInputData/normalInput").listFiles();
		if(lateDataFolderNumber == 2)
		{
			files = new File("src/test/resources/OozieExampleInputData/2ndLateData").listFiles();
		}

		for(final File file:files)
		{
			if(!file.isDirectory())
			{
				user.doAs(new PrivilegedExceptionAction<Boolean>() {

					@Override
					public Boolean run() throws Exception {
						//	Util.print("putDataInFolder: "+remoteLocation);
						fs.copyFromLocalFile(new Path(file.getAbsolutePath()),new Path(remoteLocation));
						return true;

					}
				}); 
			}
		}
	}

	public static String setFeedFilePath(String feed, String path) throws Exception {
		Feed feedElement = instanceUtil.getFeedElement(feed);
		feedElement.getLocations().getLocation().get(0).setPath(path);
		return instanceUtil.feedElementToString(feedElement);

	}

	public static String setFeedRetention(String feed, Retention r) throws Exception {
		Feed feedElement = instanceUtil.getFeedElement(feed);
		feedElement.getClusters().getCluster().get(0).setRetention(r);
		return instanceUtil.feedElementToString(feedElement);

	}

	public static int checkIfFeedCoordExist(IEntityManagerHelper helper,
			String feedName, String coordType) throws Exception {

		int numberOfCoord = 0 ;

		if(Util.getBundles(feedName,"FEED",helper).size() == 0)
			return 0;


		ArrayList<String> bundleID = Util.getBundles(feedName,"FEED",helper);

		for(int i = 0 ; i < bundleID.size() ; i++){

			List<CoordinatorJob> coords = instanceUtil.getBundleCoordinators(bundleID.get(i),helper);

			for(int j  = 0 ; j < coords.size() ; j++)
			{
				if(coords.get(j).getAppName().contains(coordType))
					numberOfCoord++;
			}
		}
		return numberOfCoord;
	}


	public static String setProcessFrequency(String process,
			Frequency frequency) throws Exception {
		Process p = instanceUtil.getProcessElement(process);

		p.setFrequency(frequency);

		return instanceUtil.processToString(p);
	}

	public static String setProcessName(String process, String newName) throws Exception {
		Process p = instanceUtil.getProcessElement(process);

		p.setName(newName);

		return instanceUtil.processToString(p);
	}


	public static String setProcessValidity(String process,
			String startTime, String endTime) throws Exception {

		Process processElement= instanceUtil.getProcessElement(process);

		for(int i =0 ; i < processElement.getClusters().getCluster().size(); i++)
		{
			processElement.getClusters().getCluster().get(i).getValidity().setStart(instanceUtil.oozieDateToDate(startTime).toDate());
			processElement.getClusters().getCluster().get(i).getValidity().setEnd(instanceUtil.oozieDateToDate(endTime).toDate());

		}

		return instanceUtil.processToString(processElement);
	}

	public static List<CoordinatorAction> getProcessInstanceListFromAllBundles(ColoHelper coloHelper,String processName,ENTITY_TYPE entityType) throws Exception{

		XOozieClient oozieClient=new XOozieClient(coloHelper.getProcessHelper().getOozieURL());

		List<CoordinatorAction> list=new ArrayList<CoordinatorAction>();

		System.out.println("bundle size for process is "+Util.getBundles(coloHelper, processName, entityType).size());

		for(String bundleId:Util.getBundles(coloHelper, processName, entityType))
		{
			BundleJob bundleInfo = oozieClient.getBundleJobInfo(bundleId);
			List<CoordinatorJob> coords = bundleInfo.getCoordinators();  

			System.out.println("number of coords in bundle "+bundleId+"="+coords.size());

			for(CoordinatorJob coord:coords)
			{
				List<CoordinatorAction> actions=oozieClient.getCoordJobInfo(coord.getId()).getActions();
				System.out.println("number of actions in coordinator "+coord.getId()+" is "+actions.size());
				list.addAll(actions);
			}
		}

		String coordId = getLatestCoordinatorID(coloHelper,processName,entityType);
		//String coordId = getDefaultCoordinatorFromProcessName(processName);
		Util.print("default coordID: "+ coordId);

		return list;

	}         
	public static List<CoordinatorAction> getProcessInstanceListFromAllBundles(ColoHelper coloHelper,String processName,String entityType) throws Exception{

		XOozieClient oozieClient=new XOozieClient(coloHelper.getProcessHelper().getOozieURL());

		List<CoordinatorAction> list=new ArrayList<CoordinatorAction>();

		System.out.println("bundle size for process is "+Util.getBundles(coloHelper, processName, entityType).size());

		for(String bundleId:Util.getBundles(coloHelper, processName, entityType))
		{
			BundleJob bundleInfo = oozieClient.getBundleJobInfo(bundleId);
			List<CoordinatorJob> coords = bundleInfo.getCoordinators();  

			System.out.println("number of coords in bundle "+bundleId+"="+coords.size());

			for(CoordinatorJob coord:coords)
			{
				List<CoordinatorAction> actions=oozieClient.getCoordJobInfo(coord.getId()).getActions();
				System.out.println("number of actions in coordinator "+coord.getId()+" is "+actions.size());
				list.addAll(actions);
			}
		}

		String coordId = getLatestCoordinatorID(coloHelper,processName,entityType);
		//String coordId = getDefaultCoordinatorFromProcessName(processName);
		Util.print("default coordID: "+ coordId);

		return list;

	}

	public static String getOutputFolderForInstanceForReplication(
			ColoHelper coloHelper, String coordID, int instanceNumber) throws OozieClientException {
		XOozieClient oozieClient=new XOozieClient(coloHelper.getProcessHelper().getOozieURL());
		CoordinatorJob coordInfo = oozieClient.getCoordJobInfo(coordID);


		return instanceUtil.getReplicatedFolderFromInstanceRunConf(oozieClient.getJobInfo(coordInfo.getActions().get(instanceNumber).getExternalId()).getConf());
	}
	private static String getReplicatedFolderFromInstanceRunConf(
			String runConf) {

		String inputPathExample = instanceUtil.getReplicationFolderFromInstanceRunConf(runConf).get(0);
		String postFix = inputPathExample.substring(inputPathExample.length() -7, inputPathExample.length());

		return getReplicatedFolderBaseFromInstanceRunConf(runConf)+postFix;
	}

	public static String getOutputFolderBaseForInstanceForReplication(
			ColoHelper coloHelper, String coordID, int instanceNumber) throws Exception{
		XOozieClient oozieClient=new XOozieClient(coloHelper.getProcessHelper().getOozieURL());
		CoordinatorJob coordInfo = oozieClient.getCoordJobInfo(coordID);

		return instanceUtil.getReplicatedFolderBaseFromInstanceRunConf(oozieClient.getJobInfo(coordInfo.getActions().get(instanceNumber).getExternalId()).getConf());


	}

	private static String getReplicatedFolderBaseFromInstanceRunConf(
			String runConf) {

		String conf = "";
		conf = runConf.substring(runConf.indexOf("distcpTargetPaths</name>")+24);
		//	Util.print("conf1: "+conf);

		conf = conf.substring(conf.indexOf("<value>")+7);
		//Util.print("conf2: "+conf);

		conf = conf.substring(0,conf.indexOf("</value>"));

		return conf;
	}         

	public static String dateToOozieDate(DateTime jodaTime) throws ParseException {

		Util.print("jadaSystemTime: "+jodaTime );
		DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy'-'MM'-'dd'T'HH':'mm'Z'");
		String str = fmt.print(jodaTime);
		return str;
	}

	public static String getOutputFolderForProcessInstance(
			ColoHelper coloHelper, String processName, int instanceNumber)
	throws Exception{
		String bundleID = instanceUtil.getSequenceBundleID(coloHelper,processName,ENTITY_TYPE.PROCESS, 0);
		String coordID = instanceUtil.getDefaultCoordIDFromBundle(coloHelper,bundleID);
		XOozieClient oozieClient=new XOozieClient(coloHelper.getProcessHelper().getOozieURL());
		CoordinatorJob coordInfo = oozieClient.getCoordJobInfo(coordID);
		//Util.print("createdConf: " +coordInfo.getActions().get(instanceNumber).getCreatedConf());
		//Util.print("runConf: " +coordInfo.getActions().get(instanceNumber).getRunConf());

		return instanceUtil.getOutputFolderFromImstanceRunConf(oozieClient.getJobInfo(coordInfo.getActions().get(instanceNumber).getExternalId()).getConf());

	}

	private static String getOutputFolderFromImstanceRunConf(String runConf) {

		String conf = "";
		conf = runConf.substring(runConf.indexOf("output</name>")+13);
		//	Util.print("conf1: "+conf);

		conf = conf.substring(conf.indexOf("<value>")+7);
		//Util.print("conf2: "+conf);

		conf = conf.substring(0,conf.indexOf("</value>"));

		return conf;

	}


	public static String ClusterElementToString(com.inmobi.qa.falcon.generated.cluster.Cluster c) throws JAXBException {
		JAXBContext jc=JAXBContext.newInstance(com.inmobi.qa.falcon.generated.cluster.Cluster.class);
		java.io.StringWriter sw = new StringWriter();
		Marshaller marshaller = jc.createMarshaller();
		marshaller.marshal(c,sw);
		logger.info("modified process is: "+sw);
		return sw.toString();
	}

	public static Status getProcessCoordinatorStatus(ColoHelper coloHelper,
			String processName, int bundleNumber) throws Exception{

		String bundleID = instanceUtil.getSequenceBundleID(coloHelper,processName,ENTITY_TYPE.PROCESS, bundleNumber);
		String coordID = instanceUtil.getDefaultCoordIDFromBundle(coloHelper,bundleID);
		XOozieClient oozieClient=new XOozieClient(coloHelper.getProcessHelper().getOozieURL());
		CoordinatorJob coordInfo = oozieClient.getCoordJobInfo(coordID);
		return coordInfo.getStatus();
	}

	public static String setProcessClusterName(String processData,
			List<String> clusters) throws Exception {


		Process p = instanceUtil.getProcessElement(processData);

		for(int i = 0 ; i < p.getClusters().getCluster().size() ; i++)
		{
			p.getClusters().getCluster().get(i).setName(instanceUtil.getClusterName(clusters.get(i)));
		}

		return instanceUtil.processToString(p);
	}

	private static String getClusterName(String clusterData) throws Exception {

		com.inmobi.qa.falcon.generated.cluster.Cluster c = instanceUtil.getClusterElement(clusterData);
		return c.getName();		
	}

	public static com.inmobi.qa.falcon.generated.cluster.Cluster getClusterElement(String clusterData) throws Exception{
		JAXBContext jc=JAXBContext.newInstance(com.inmobi.qa.falcon.generated.cluster.Cluster.class);
		Unmarshaller u=jc.createUnmarshaller();
		com.inmobi.qa.falcon.generated.cluster.Cluster clusterElement=(com.inmobi.qa.falcon.generated.cluster.Cluster)u.unmarshal((new StringReader(clusterData)));

		return clusterElement;
	}

	@Deprecated
	public static void waitTillInstanceReachState(ColoHelper coloHelper,
			String processName, int numberOfInstance,
			org.apache.oozie.client.CoordinatorAction.Status expectedStatus,int minutes) throws Exception {


		int sleep = minutes*60/20;

		for(int sleepCount = 0 ; sleepCount < sleep ; sleepCount++ ){

			ArrayList<org.apache.oozie.client.CoordinatorAction.Status> statusList = instanceUtil.getStatusAllInstanceStatusForProcess(coloHelper,processName);
			int instanceWithStatus= 0;
			for(int statusCount = 0 ; statusCount < statusList.size(); statusCount++){

				if(statusList.get(statusCount).equals(expectedStatus))
					instanceWithStatus++;

			}

			if(instanceWithStatus==numberOfInstance)
				break;

			Thread.sleep(20000);

		}				
	}


	public static void waitTillInstanceReachState(ColoHelper coloHelper,
			String entityName, int numberOfInstance,
			org.apache.oozie.client.CoordinatorAction.Status expectedStatus,
			int totalMinutesToWait, ENTITY_TYPE entityType)  throws Exception{

		int sleep = totalMinutesToWait*60/20;

		for(int sleepCount = 0 ; sleepCount < sleep ; sleepCount++ ){

			ArrayList<org.apache.oozie.client.CoordinatorAction.Status> statusList = instanceUtil.getStatusAllInstance(coloHelper,entityName,entityType);

			int instanceWithStatus= 0;
			for(int statusCount = 0 ; statusCount < statusList.size(); statusCount++){

				if(statusList.get(statusCount).equals(expectedStatus))
					instanceWithStatus++;

			}

			if(instanceWithStatus>=numberOfInstance)
				return;

			Thread.sleep(20000);

		}

		Assert.assertTrue(false,"expceted state of instance was never reached");
	}

	@Deprecated
	private static ArrayList<org.apache.oozie.client.CoordinatorAction.Status> getStatusAllInstanceStatusForProcess(
			ColoHelper coloHelper, String processName) throws Exception{

		CoordinatorJob coordInfo = instanceUtil.getCoordJobForProcess(coloHelper,processName);

		ArrayList<org.apache.oozie.client.CoordinatorAction.Status> statusList = new ArrayList<org.apache.oozie.client.CoordinatorAction.Status>();
		for(int count = 0 ; count < coordInfo.getActions().size();count ++)
			statusList.add(coordInfo.getActions().get(count).getStatus());

		return statusList;
	}

	private static ArrayList<org.apache.oozie.client.CoordinatorAction.Status> getStatusAllInstance(
			ColoHelper coloHelper, String entityName,ENTITY_TYPE entityType) throws Exception{

		CoordinatorJob coordInfo = instanceUtil.getCoordJobForProcess(coloHelper,entityName,entityType);

		ArrayList<org.apache.oozie.client.CoordinatorAction.Status> statusList = new ArrayList<org.apache.oozie.client.CoordinatorAction.Status>();
		for(int count = 0 ; count < coordInfo.getActions().size();count ++)
			statusList.add(coordInfo.getActions().get(count).getStatus());

		return statusList;
	}

	public static ArrayList<Path> getAllOutputLocationForProcess(
			ColoHelper coloHelper, String processName) throws Exception{

		ArrayList<Path> returnObject = new ArrayList<Path>();

		CoordinatorJob coordInfo = instanceUtil.getCoordJobForProcess(coloHelper,processName);
		for(int count = 0 ; count < coordInfo.getActions().size();count ++)
		{
			returnObject.add(new Path(instanceUtil.getOutputFolderForProcessInstance(coloHelper, processName, count)));
		}

		return returnObject;

	}

	@Deprecated
	private static CoordinatorJob getCoordJobForProcess(
			ColoHelper coloHelper, String processName) throws Exception{

		String bundleID = instanceUtil.getSequenceBundleID(coloHelper,processName,ENTITY_TYPE.PROCESS, 0);
		String coordID = instanceUtil.getDefaultCoordIDFromBundle(coloHelper,bundleID);
		XOozieClient oozieClient=new XOozieClient(coloHelper.getProcessHelper().getOozieURL());
		return oozieClient.getCoordJobInfo(coordID);
	}

	private static CoordinatorJob getCoordJobForProcess(
			ColoHelper coloHelper, String processName,ENTITY_TYPE entityType) throws Exception{

		String bundleID =	  instanceUtil.getLatestBundleID(coloHelper, processName, entityType);
		//instanceUtil.getSequenceBundleID(coloHelper,processName,entityType, 0);
		String coordID = new String();
		if(entityType.equals(ENTITY_TYPE.PROCESS))
			coordID = instanceUtil.getDefaultCoordIDFromBundle(coloHelper,bundleID);
		else
			coordID = instanceUtil.getReplicationCoordID(bundleID, coloHelper.getFeedHelper()).get(0);
		XOozieClient oozieClient=new XOozieClient(coloHelper.getProcessHelper().getOozieURL());
		return oozieClient.getCoordJobInfo(coordID);
	}
	public static String removeFeedPartitionsTag(String feed) throws Exception{
		Feed f = getFeedElement(feed);

		f.setPartitions(null);

		return feedElementToString(f);

	}

	public static ArrayList<Path> getAllOutputLocationForProcess(
			ColoHelper coloHelper, Bundle bundle) throws Exception{

		List<Output> outputs = bundle.getAllOutputs();
		ArrayList<Path> returnObject = new ArrayList<Path>();

		CoordinatorJob coordInfo = instanceUtil.getCoordJobForProcess(coloHelper,bundle.getProcessName());
		for(int count = 0 ; count < coordInfo.getActions().size();count ++)
		{
			for(Output o: outputs){
				ArrayList<String> paths = instanceUtil.getValueFromConf(coloHelper, bundle.getProcessName(),o.getName(), count);
				for(String strPath: paths)
					returnObject.add(new Path(strPath));

			}
		}

		return returnObject;


	}

	private static ArrayList<String> getValueFromConf(ColoHelper coloHelper,
			String processName, String valueFor, int count) throws Exception{
		String bundleID = instanceUtil.getSequenceBundleID(coloHelper,processName,ENTITY_TYPE.PROCESS, 0);
		String coordID = instanceUtil.getDefaultCoordIDFromBundle(coloHelper,bundleID);
		XOozieClient oozieClient=new XOozieClient(coloHelper.getProcessHelper().getOozieURL());
		CoordinatorJob coordInfo = oozieClient.getCoordJobInfo(coordID);
		//Util.print("createdConf: " +coordInfo.getActions().get(instanceNumber).getCreatedConf());
		//Util.print("runConf: " +coordInfo.getActions().get(instanceNumber).getRunConf());

		return instanceUtil.getValueFromInstanceRunConf(oozieClient.getJobInfo(coordInfo.getActions().get(count).getExternalId()).getConf(),valueFor);
	}

	private static ArrayList<String> getValueFromInstanceRunConf(String runConf,
			String valueFor) {
		String conf = "";
		conf = runConf.substring(runConf.indexOf(valueFor+"</name>")+7+valueFor.length());
		//	Util.print("conf1: "+conf);

		conf = conf.substring(conf.indexOf("<value>")+7);
		//Util.print("conf2: "+conf);

		conf = conf.substring(0,conf.indexOf("</value>"));

		return splitRunConfValue(conf);
	}

	public static ArrayList<String> splitRunConfValue(String value)
	{
		ArrayList<String> inputWise = new ArrayList<String>(Arrays.asList(value.split("#")));

		ArrayList<String> returnObject  = new ArrayList<String>();


		int size = inputWise.size();

		for(int i = 0 ; i <size; i++ )
		{
			returnObject.addAll(Arrays.asList(inputWise.get(i).split(",")));
		}


		for(int i = 0 ; i <size; i++ )
		{
			if(returnObject.get(i).contains("*"))
				returnObject.set(i, returnObject.get(i).substring(0, returnObject.get(i).indexOf("*")-1));
		}

		return returnObject;
	}

	public static void waitForBundleToReachState(
			ColoHelper coloHelper,
			String processName,
			org.apache.oozie.client.Job.Status expectedStatus,
			int totalMinutesToWait)  throws Exception{

		int sleep = totalMinutesToWait*60/20;

		for(int sleepCount = 0 ; sleepCount < sleep ; sleepCount++ ){

			String BundleID = instanceUtil.getLatestBundleID(coloHelper, processName, "PROCESS");

			XOozieClient oozieClient=new XOozieClient(coloHelper.getProcessHelper().getOozieURL());

			BundleJob j = oozieClient.getBundleJobInfo(BundleID);



			if(j.getStatus().equals(expectedStatus))
				break;

			Thread.sleep(20000);
		}
	}

	public static void waitTillParticularInstanceReachState(ColoHelper coloHelper,
			String entityName, int instanceNumber,
			org.apache.oozie.client.CoordinatorAction.Status expectedStatus, int totalMinutesToWait,
			ENTITY_TYPE entityType)  throws Exception{

		int sleep = totalMinutesToWait*60/20;

		for(int sleepCount = 0 ; sleepCount < sleep ; sleepCount++ ){

			ArrayList<org.apache.oozie.client.CoordinatorAction.Status> statusList = instanceUtil.getStatusAllInstance(coloHelper,entityName,entityType);

			if(statusList.get(instanceNumber).equals(expectedStatus))
				break;
			Thread.sleep(20000);

		}


	}

	@Deprecated
	public static ArrayList<String> createEmptyDirWithinDatesAndPrefix(ColoHelper colo,
			DateTime startDateJoda, DateTime endDateJoda, String prefix,
			int interval)  throws Exception{
		List<String> dataDates = Util.getMinuteDatesOnEitherSide(startDateJoda,endDateJoda,interval);

		for(int i = 0 ; i < dataDates.size(); i++)
			dataDates.set(i, prefix + dataDates.get(i));

		ArrayList<String> dataFolder = new ArrayList<String>();

		for(int i = 0 ; i < dataDates.size(); i++)
			dataFolder.add(dataDates.get(i));

		instanceUtil.createHDFSFolders(colo,dataFolder);
		return dataFolder;
	}	

	public static ArrayList<String> createEmptyDirWithinDatesAndPrefix(ColoHelper colo,
			DateTime startDateJoda, DateTime endDateJoda, String prefix,String postfix,
			int interval)  throws Exception{
		List<String> dataDates = Util.getMinuteDatesOnEitherSide(startDateJoda,endDateJoda,interval);

		for(int i = 0 ; i < dataDates.size(); i++)
			dataDates.set(i, prefix + dataDates.get(i));

		if(postfix!=null)
		{
			for(int i = 0 ; i < dataDates.size() ; i++)
				dataDates.set(i,dataDates.get(i)+postfix);
		}
		ArrayList<String> dataFolder = new ArrayList<String>();

		for(int i = 0 ; i < dataDates.size(); i++)
			dataFolder.add(dataDates.get(i));

		instanceUtil.createHDFSFolders(colo,dataFolder);
		return dataFolder;
	}

    public static String oozieDateToDataGenDate(String oozieDate) throws ParseException {

        DateTime jodaTime = new DateTime(instanceUtil.oozieDateToDate(oozieDate),DateTimeZone.UTC);
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZone(DateTimeZone.UTC);
        String str = formatter.print(jodaTime);
        return str;
    }
}

