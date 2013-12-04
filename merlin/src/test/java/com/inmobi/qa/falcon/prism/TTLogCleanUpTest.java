package com.inmobi.qa.falcon.prism;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.joda.time.Minutes;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.inmobi.qa.falcon.bundle.Bundle;
import com.inmobi.qa.falcon.generated.dependencies.Frequency.TimeUnit;
import com.inmobi.qa.falcon.generated.feed.ActionType;
import com.inmobi.qa.falcon.generated.feed.ClusterType;
import com.inmobi.qa.falcon.helpers.ColoHelper;
import com.inmobi.qa.falcon.helpers.PrismHelper;
import com.inmobi.qa.falcon.response.ServiceResponse;
import com.inmobi.qa.falcon.supportClasses.ENTITY_TYPE;
import com.inmobi.qa.falcon.supportClasses.GetBundle;
import com.inmobi.qa.falcon.util.AssertUtil;
import com.inmobi.qa.falcon.util.Util;
import com.inmobi.qa.falcon.util.Util.URLS;
import com.inmobi.qa.falcon.util.hadoopUtil;
import com.inmobi.qa.falcon.util.instanceUtil;
import com.inmobi.qa.falcon.util.xmlUtil;
/**
 * 
 * @author samarth.gupta
 *
 */


public class TTLogCleanUpTest {

	PrismHelper prismHelper=new PrismHelper("prism.properties");

	ColoHelper ua1=new ColoHelper("mk-qa.config.properties");

	ColoHelper ua2 = new ColoHelper("ivoryqa-1.config.properties");

	ColoHelper ua3 = new ColoHelper("gs1001.config.properties");


	//	@BeforeClass(alwaysRun=true)
	public void createTestData() throws Exception
	{

		Util.print("in @BeforeClass");

		System.setProperty("java.security.krb5.realm", "");
		System.setProperty("java.security.krb5.kdc", "");


		Bundle b = new Bundle();
		b = (Bundle)Util.readELBundles()[0][0];
		b  = new Bundle(b,ua2.getEnvFileName());

		String startDate = "2010-01-01T20:00Z";
		String endDate = "2010-01-02T03:04Z";

		b.setInputFeedDataPath("/logCleanUpTest/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
		String prefix = b.getFeedDataPathPrefix();
		Util.HDFSCleanup(ua2,prefix.substring(1));

		DateTime startDateJoda = new DateTime(instanceUtil.oozieDateToDate(startDate));
		DateTime endDateJoda = new DateTime(instanceUtil.oozieDateToDate(endDate));

		List<String> dataDates = Util.getMinuteDatesOnEitherSide(startDateJoda,endDateJoda,20);

		for(int i = 0 ; i < dataDates.size(); i++)
			dataDates.set(i, prefix + dataDates.get(i));

		ArrayList<String> dataFolder = new ArrayList<String>();

		for(int i = 0 ; i < dataDates.size(); i++)
			dataFolder.add(dataDates.get(i));

		instanceUtil.putDataInFolders(ua2,dataFolder);
	}


	@BeforeMethod(alwaysRun=true)
	public void testName(Method method) throws Exception
	{
		Util.print("test name: "+method.getName());
		//restart server as precaution
		Util.forceRestartService(ua1.getClusterHelper());
		Util.forceRestartService(ua2.getClusterHelper());
		Util.forceRestartService(ua3.getClusterHelper());
	}


	@Test(enabled=true, timeOut=12000000)
	public void feedReplicaltionLogCleanUp() throws Exception
	{

		// instance log retention is set as 6 mins. Log clean up service run every 10 minutes
		//when test case starts tomcat has been force restarted , so log cleanup service should have just run. 
		Thread.sleep(30000);
		Bundle b1 = (Bundle)Bundle.readBundle("src/test/resources/LocalDC_feedReplicaltion_BillingRC")[0][0];
		b1.generateUniqueBundle();
		Bundle b2 =  (Bundle)Bundle.readBundle("src/test/resources/LocalDC_feedReplicaltion_BillingRC")[0][0];
		b2.generateUniqueBundle();
		Bundle b3 =  (Bundle)Bundle.readBundle("src/test/resources/LocalDC_feedReplicaltion_BillingRC")[0][0];
		b3.generateUniqueBundle();

		try{

			//tomcat restart time
			DateTime serviceRestartTime = Util.getSystemDate(prismHelper);
			Thread.sleep(30000);

			b1 = new Bundle(b1,ua1.getEnvFileName());
			b2  = new Bundle(b2,ua2.getEnvFileName());
			b3  = new Bundle(b3,ua3.getEnvFileName());

			b1.setCLusterColo("ua1");
			b2.setCLusterColo("ua2");
			b3.setCLusterColo("ua3");

			Bundle.submitCluster(b1,b2,b3);

			String startTimeUA1 = "2012-10-01T12:05Z" ;
			String startTimeUA2 = "2012-10-01T12:10Z";


		//	instanceUtil.putFileInFolders(ua2,instanceUtil.createEmptyDirWithinDatesAndPrefix(ua2, instanceUtil.oozieDateToDate("2012-10-01T12:04Z"), instanceUtil.oozieDateToDate("2012-10-01T12:10Z"), "/logCleanUpTest/", 1),"thriftRRMar0602.gz");

			
			
			String feed = b1.getDataSets().get(0);
			feed = instanceUtil.setFeedFilePath(feed,"/logCleanUpTest/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/");
			feed =  instanceUtil.setFeedCluster(feed,xmlUtil.createValidity("2012-10-01T12:00Z","2010-01-01T00:00Z"),xmlUtil.createRtention("days(10000)",ActionType.DELETE),null,ClusterType.SOURCE,null);

			feed = instanceUtil.setFeedCluster(feed,xmlUtil.createValidity(startTimeUA1,"2012-10-01T12:10Z"),xmlUtil.createRtention("days(10000)",ActionType.DELETE),Util.readClusterName(b1.getClusters().get(0)),ClusterType.TARGET,"${cluster.colo}");
			feed = instanceUtil.setFeedCluster(feed,xmlUtil.createValidity(startTimeUA2,"2012-10-01T12:25Z"),xmlUtil.createRtention("days(10000)",ActionType.DELETE),Util.readClusterName(b3.getClusters().get(0)),ClusterType.TARGET,"${cluster.colo}");
			feed = instanceUtil.setFeedCluster(feed,xmlUtil.createValidity("2012-10-01T12:00Z","2099-01-01T00:00Z"),xmlUtil.createRtention("days(10000)",ActionType.DELETE),Util.readClusterName(b2.getClusters().get(0)),ClusterType.SOURCE,null);

			//clean target if old data exists
			String prefix = "/logCleanUpTest/2012/10/01/12/";
			Util.HDFSCleanup(ua3,prefix.substring(1));
			Util.HDFSCleanup(ua1,prefix.substring(1));


			Util.print("feed: "+feed);

			ServiceResponse r = prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
			Thread.sleep(10000);
			AssertUtil.assertSucceeded(r);

			r= prismHelper.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);
			Thread.sleep(15000);

			
			//wait for first instance to succeed 
			instanceUtil.waitTillParticularInstanceReachState(ua1,Util.getFeedName(feed),0, org.apache.oozie.client.CoordinatorAction.Status.SUCCEEDED, 20,ENTITY_TYPE.FEED);
			instanceUtil.waitTillParticularInstanceReachState(ua3,Util.getFeedName(feed),0, org.apache.oozie.client.CoordinatorAction.Status.SUCCEEDED, 20,ENTITY_TYPE.FEED);

			//check in current time is past 3 minutes from start of tomcat
			DateTime firstInstanceCompleteTime = Util.getSystemDate(prismHelper);
			Minutes m = Minutes.minutesBetween(serviceRestartTime,firstInstanceCompleteTime);

	/*		if(m.getMinutes() <5){
				Util.print("sleeping for: "+(5-m.getMinutes())*60*1000);
				Thread.sleep((5-m.getMinutes())*60*1000);
			}*/
			//rerun first instance of replicaltion
			ua1.getFeedHelper().getProcessInstanceRerun(Util.getFeedName(feed),"?start=2012-10-01T12:05Z");
			ua3.getFeedHelper().getProcessInstanceRerun(Util.getFeedName(feed),"?start=2012-10-01T12:10Z");


			//wait for 10 minutes complete 
			while(m.getMinutes()<10){
				m = Minutes.minutesBetween(serviceRestartTime,Util.getSystemDate(prismHelper));
				Util.print("minutes remaining for 10 min restart: "+m.getMinutes());
				Thread.sleep(20000);

			}

			
			
			
			//check for logs .... only first instance log should have been deleted
			ArrayList<Path> ua1ReplicatedData =hadoopUtil.getAllFilesRecursivelyHDFS(ua1, new Path("/dataBillingRC/fetlrc/billing/2012/10/01/12/"));
			//check for no ua2 or ua3 in ua1
			AssertUtil.failIfStringFoundInPath(ua1ReplicatedData,"ua2","ua3");

			ArrayList<Path> ua2ReplicatedData =hadoopUtil.getAllFilesRecursivelyHDFS(ua2, new Path("/dataBillingRC/fetlrc/billing/2012/10/01/12/"));
			AssertUtil.failIfStringFoundInPath(ua2ReplicatedData,"ua1","ua3");


			ArrayList<Path> ua1ReplicatedData00 =hadoopUtil.getAllFilesRecursivelyHDFS(ua1, new Path("/dataBillingRC/fetlrc/billing/2012/10/01/12/00/"),"_SUCCESS");
			ArrayList<Path> ua1ReplicatedData10 =hadoopUtil.getAllFilesRecursivelyHDFS(ua1, new Path("/dataBillingRC/fetlrc/billing/2012/10/01/12/10/"),"_SUCCESS");

			ArrayList<Path> ua2ReplicatedData10 =hadoopUtil.getAllFilesRecursivelyHDFS(ua2, new Path("/dataBillingRC/fetlrc/billing/2012/10/01/12/10"),"_SUCCESS");
			ArrayList<Path> ua2ReplicatedData15 =hadoopUtil.getAllFilesRecursivelyHDFS(ua2, new Path("/dataBillingRC/fetlrc/billing/2012/10/01/12/15"),"_SUCCESS");

			ArrayList<Path> ua3OriginalData00ua1 = hadoopUtil.getAllFilesRecursivelyHDFS(ua2, new Path("/dataBillingRC/fetlrc/billing/2012/10/01/12/00/ua1"),"_SUCCESS");
			ArrayList<Path> ua3OriginalData10ua1 = hadoopUtil.getAllFilesRecursivelyHDFS(ua2, new Path("/dataBillingRC/fetlrc/billing/2012/10/01/12/10/ua1"),"_SUCCESS");
			ArrayList<Path> ua3OriginalData10ua2 = hadoopUtil.getAllFilesRecursivelyHDFS(ua2, new Path("/dataBillingRC/fetlrc/billing/2012/10/01/12/10/ua2"),"_SUCCESS");
			ArrayList<Path> ua3OriginalData15ua2 = hadoopUtil.getAllFilesRecursivelyHDFS(ua2, new Path("/dataBillingRC/fetlrc/billing/2012/10/01/12/15/ua2"),"_SUCCESS");

			AssertUtil.checkForPathsSizes(ua1ReplicatedData00,new ArrayList<Path>());
			AssertUtil.checkForPathsSizes(ua1ReplicatedData10,ua3OriginalData10ua1);
			AssertUtil.checkForPathsSizes(ua2ReplicatedData10,ua3OriginalData10ua2);
			AssertUtil.checkForPathsSizes(ua2ReplicatedData15,ua3OriginalData15ua2);

		}

		finally{

			prismHelper.getFeedHelper().delete(URLS.DELETE_URL, b1.getDataSets().get(0));
			prismHelper.getClusterHelper().delete(URLS.DELETE_URL, b2.getClusters().get(0));
			prismHelper.getClusterHelper().delete(URLS.DELETE_URL, b1.getClusters().get(0));
			prismHelper.getClusterHelper().delete(URLS.DELETE_URL, b3.getClusters().get(0));
		}
	}


	@Test(enabled=false, timeOut=12000000)
	public void processLogCleanUp() throws Exception
	{
		//restart service at start of test case
		// let the process have 3 instance
		// wait for 5 mins. till then first instance should have succeeded. 
		// rerun the first instance
		//wait for 10 mins from the restart. 
		//log of first run of first instance should only be deleted. others should be present. 
		DateTime serviceRestartTime = Util.getSystemDate(prismHelper);
		System.out.println("serviceRestartTime: "+serviceRestartTime);
		Thread.sleep(20000);

		Bundle b = new Bundle();


		try{

			b = (Bundle)Util.readBundle(GetBundle.RegularBundle)[0][0];
			b  = new Bundle(b,ua3.getEnvFileName());
			b.setInputFeedDataPath("/logRetentionTest/process/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");

			instanceUtil.putFileInFolders(ua3,instanceUtil.createEmptyDirWithinDatesAndPrefix(ua3, instanceUtil.oozieDateToDate("2010-01-02T00:40Z"), instanceUtil.oozieDateToDate("2010-01-02T01:10Z"), "/logRetentionTest/process/", 1),"log_01.txt");

			b.setProcessValidity("2010-01-02T01:00Z","2010-01-02T01:07Z");
			b.setProcessPeriodicity(5,TimeUnit.minutes);
			b.setOutputFeedPeriodicity(5,TimeUnit.minutes);
			b.setOutputFeedLocationData("/samarth/testProcessInstanceRerun_singleSucceeded/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessConcurrency(1);
			b.submitAndScheduleBundle(prismHelper);

			//sleep for 5 mins
			Util.print("sleeping for: 300000 milli seconds");
		//	Thread.sleep(300000);

			//rerun the first instance
			prismHelper.getProcessHelper().getProcessInstanceRerun(Util.readEntityName(b.getProcessData()),"?start=2010-01-02T01:00Z");

			//wait for 10 mins to complete from restart
			Minutes m = Minutes.minutesBetween( serviceRestartTime,Util.getSystemDate(prismHelper));
			while(m.getMinutes()<10){
				m = Minutes.minutesBetween(serviceRestartTime,Util.getSystemDate(prismHelper));
				Util.print("minutes remaining for 10 min restart: "+ (10 - m.getMinutes()));
				Thread.sleep(20000);
			}

		}
		finally{
				b.deleteBundle(prismHelper);

		}

	}

//	@AfterClass(alwaysRun=true)
	public void deleteData() throws Exception
	{
		Util.print("in @AfterClass");

		System.setProperty("java.security.krb5.realm", "");
		System.setProperty("java.security.krb5.kdc", "");


		Bundle b = new Bundle();
		b = (Bundle)Util.readELBundles()[0][0];
		b  = new Bundle(b,ua2.getEnvFileName());
		b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
		String prefix = b.getFeedDataPathPrefix();
		Util.HDFSCleanup(prefix.substring(1));
	}
}
