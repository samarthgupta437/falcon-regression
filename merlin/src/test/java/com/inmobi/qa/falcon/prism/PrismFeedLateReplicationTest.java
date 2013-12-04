package com.inmobi.qa.falcon.prism;

import java.lang.reflect.Method;
import java.util.ArrayList;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.inmobi.qa.falcon.bundle.Bundle;
import com.inmobi.qa.falcon.generated.feed.ActionType;
import com.inmobi.qa.falcon.generated.feed.ClusterType;
import com.inmobi.qa.falcon.helpers.ColoHelper;
import com.inmobi.qa.falcon.helpers.PrismHelper;
import com.inmobi.qa.falcon.response.ServiceResponse;
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

public class PrismFeedLateReplicationTest {


	@BeforeMethod(alwaysRun=true)
	public void testName(Method method) throws Exception
	{
		Util.print("test name: "+method.getName());
		//restart server as precaution
		//	Util.restartService(ua1.getClusterHelper());
		//	Util.restartService(ua2.getClusterHelper());
		//	Util.restartService(ua3.getClusterHelper());


	}

	public PrismFeedLateReplicationTest() throws Exception{

	}

	PrismHelper prismHelper=new PrismHelper("prism.properties");

	ColoHelper ua1=new ColoHelper("gs1001.config.properties");

	ColoHelper ua2 = new ColoHelper("ivoryqa-1.config.properties");

	ColoHelper ua3 = new ColoHelper("mk-qa.config.properties");

	@SuppressWarnings("deprecation")
	@Test(groups = {"multiCluster"})
	public void multipleSourceOneTarget_pastData() throws Exception
	{

		Bundle b1 = (Bundle)Util.readELBundles()[0][0];
		b1.generateUniqueBundle();
		Bundle b2 = (Bundle)Util.readELBundles()[0][0];
		b2.generateUniqueBundle();
		Bundle b3 = (Bundle)Util.readELBundles()[0][0];
		b3.generateUniqueBundle();

		try{
			b1 = new Bundle(b1,ua1.getEnvFileName());
			b2  = new Bundle(b2,ua2.getEnvFileName());
			b3  = new Bundle(b3,ua3.getEnvFileName());


			b1.setInputFeedDataPath("/samarthRetention/input-data/rawLogs/oozieExample/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/");

			b1.setCLusterColo("ua1");
			Util.print("cluster b1: "+b1.getClusters().get(0));

			ServiceResponse r = prismHelper.getClusterHelper().submitEntity(URLS.SUBMIT_URL,b1.getClusters().get(0));
			Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));


			b2.setCLusterColo("ua2");
			Util.print("cluster b2: "+b2.getClusters().get(0));
			r = prismHelper.getClusterHelper().submitEntity(URLS.SUBMIT_URL,b2.getClusters().get(0));
			Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));


			b3.setCLusterColo("ua3");
			Util.print("cluster b3: "+b3.getClusters().get(0));
			r = prismHelper.getClusterHelper().submitEntity(URLS.SUBMIT_URL,b3.getClusters().get(0));
			Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));


			String feed = b1.getDataSets().get(0);
			feed =  instanceUtil.setFeedCluster(feed,xmlUtil.createValidity("2009-02-01T00:00Z","2012-01-01T00:00Z"),xmlUtil.createRtention("hours(10)",ActionType.DELETE),null,ClusterType.SOURCE,null);

			String postFix = "/US/ua2" ;
			String prefix = b1.getFeedDataPathPrefix();
			Util.HDFSCleanup(ua2,prefix.substring(1));
			Util.lateDataReplenish(ua2,90,0,1,prefix,postFix);


			postFix = "/UK/ua3" ;
			prefix = b1.getFeedDataPathPrefix();
			Util.HDFSCleanup(ua3,prefix.substring(1));
			Util.lateDataReplenish(ua3,90,0,1,prefix,postFix);

			String startTime = instanceUtil.getTimeWrtSystemTime(-30);

			feed = instanceUtil.setFeedCluster(feed,xmlUtil.createValidity(startTime,"2099-01-01T00:00Z"),xmlUtil.createRtention("hours(10)",ActionType.DELETE),Util.readClusterName(b2.getClusters().get(0)),ClusterType.SOURCE,"US/${cluster.colo}");
			feed = instanceUtil.setFeedCluster(feed,xmlUtil.createValidity(startTime,"2099-01-01T00:00Z"),xmlUtil.createRtention("hours(10)",ActionType.DELETE),Util.readClusterName(b1.getClusters().get(0)),ClusterType.TARGET,null);
			feed = instanceUtil.setFeedCluster(feed,xmlUtil.createValidity(startTime,"2099-01-01T00:00Z"),xmlUtil.createRtention("hours(10)",ActionType.DELETE),Util.readClusterName(b3.getClusters().get(0)),ClusterType.SOURCE,"UK/${cluster.colo}");


			Util.print("feed: "+feed);

			r= prismHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed);
			Thread.sleep(10000);

			String TargetBundleID = 	instanceUtil.getLatestBundleID(Util.readDatasetName(feed),"FEED",ua1.getFeedHelper());

			//wait till 1st instance of replication coord is SUCCEEDED
			ArrayList<String> replicationCoordIDTarget    = instanceUtil.getReplicationCoordID(TargetBundleID,ua1.getFeedHelper());

			for(int i = 0 ; i < 30 ; i++)
			{
				if(instanceUtil.getInstanceStatusFromCoord(ua1,replicationCoordIDTarget.get(0),0).toString().equals("SUCCEEDED") && instanceUtil.getInstanceStatusFromCoord(ua1,replicationCoordIDTarget.get(1),0).toString().equals("SUCCEEDED"))
					break;
				Thread.sleep(20000);
			}

			Thread.sleep(15000);

			ArrayList<String>  inputFolderListForColo1=	instanceUtil.getInputFoldersForInstanceForReplication(ua1,replicationCoordIDTarget.get(0),0);
			ArrayList<String>  inputFolderListForColo2=	instanceUtil.getInputFoldersForInstanceForReplication(ua1,replicationCoordIDTarget.get(1),0);

			Util.print("folder list 1: "+inputFolderListForColo1.toString());
			Util.print("folder list 2: "+inputFolderListForColo2.toString());

			instanceUtil.putDataInFolders(ua2, inputFolderListForColo1);
			instanceUtil.putDataInFolders(ua3, inputFolderListForColo2);




			Util.print("test");




		}

		finally{
			b1.deleteBundle(prismHelper);
			b2.deleteBundle(prismHelper);
			b3.deleteBundle(prismHelper);
		}
	}

	@Test(groups = {"multiCluster"})
	public void multipleSourceOneTarget_futureData() throws Exception
	{

		Bundle b1 = (Bundle)Util.readELBundles()[0][0];
		b1.generateUniqueBundle();
		Bundle b2 = (Bundle)Util.readELBundles()[0][0];
		b2.generateUniqueBundle();
		Bundle b3 = (Bundle)Util.readELBundles()[0][0];
		b3.generateUniqueBundle();

		try{
			b1 = new Bundle(b1,ua1.getEnvFileName());
			b2  = new Bundle(b2,ua2.getEnvFileName());
			b3  = new Bundle(b3,ua3.getEnvFileName());


			b1.setInputFeedDataPath("/samarthRetention/input-data/rawLogs/oozieExample/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/");
			Bundle.submitCluster(b1,b2,b3);

			String feed = b1.getDataSets().get(0);
			feed =  instanceUtil.setFeedCluster(feed,xmlUtil.createValidity("2009-02-01T00:00Z","2012-01-01T00:00Z"),xmlUtil.createRtention("hours(10)",ActionType.DELETE),null,ClusterType.SOURCE,null);



			String startTime = instanceUtil.getTimeWrtSystemTime(3);

			feed = instanceUtil.setFeedCluster(feed,xmlUtil.createValidity(startTime,"2099-01-01T00:00Z"),xmlUtil.createRtention("hours(10)",ActionType.DELETE),Util.readClusterName(b2.getClusters().get(0)),ClusterType.SOURCE,"US/${cluster.colo}");
			feed = instanceUtil.setFeedCluster(feed,xmlUtil.createValidity(startTime,"2099-01-01T00:00Z"),xmlUtil.createRtention("hours(10)",ActionType.DELETE),Util.readClusterName(b1.getClusters().get(0)),ClusterType.TARGET,null);
			feed = instanceUtil.setFeedCluster(feed,xmlUtil.createValidity(startTime,"2099-01-01T00:00Z"),xmlUtil.createRtention("hours(10)",ActionType.DELETE),Util.readClusterName(b3.getClusters().get(0)),ClusterType.SOURCE,"UK/${cluster.colo}");


			Util.print("feed: "+feed);

			ServiceResponse r = prismHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed);
			Thread.sleep(10000);


			String postFix = "/US/ua2" ;
			String prefix = b1.getFeedDataPathPrefix();
			Util.HDFSCleanup(ua2,prefix.substring(1));
			Util.lateDataReplenish(ua2,90,0,1,prefix,postFix);


			postFix = "/UK/ua3" ;
			prefix = b1.getFeedDataPathPrefix();
			Util.HDFSCleanup(ua3,prefix.substring(1));
			Util.lateDataReplenish(ua3,90,0,1,prefix,postFix);

			Thread.sleep(60000);

			//wait till 1st instance of replication coord is SUCCEEDED
			String TargetBundleID = 	instanceUtil.getLatestBundleID(Util.readDatasetName(feed),"FEED",ua1.getFeedHelper());

			ArrayList<String> replicationCoordIDTarget    = instanceUtil.getReplicationCoordID(TargetBundleID,ua1.getFeedHelper());

			for(int i = 0 ; i < 30 ; i++)
			{
				if(instanceUtil.getInstanceStatusFromCoord(ua1,replicationCoordIDTarget.get(0),0).toString().equals("SUCCEEDED") && instanceUtil.getInstanceStatusFromCoord(ua1,replicationCoordIDTarget.get(1),0).toString().equals("SUCCEEDED"))
					break;

				Util.print("still in for loop");
				Thread.sleep(20000);
			}

			Thread.sleep(15000);

			ArrayList<String>  inputFolderListForColo1=	instanceUtil.getInputFoldersForInstanceForReplication(ua1,replicationCoordIDTarget.get(0),1);
			ArrayList<String>  inputFolderListForColo2=	instanceUtil.getInputFoldersForInstanceForReplication(ua1,replicationCoordIDTarget.get(1),1);

			Util.print("folder list 1: "+inputFolderListForColo1.toString());
			Util.print("folder list 2: "+inputFolderListForColo2.toString());

			instanceUtil.putDataInFolders(ua2, inputFolderListForColo1);
			instanceUtil.putDataInFolders(ua3, inputFolderListForColo2);

			//sleep till late starts
			instanceUtil.sleepTill(ua1, instanceUtil.addMinsToTime(startTime,4));

			//check for run id to  be 1
			if(!(instanceUtil.getInstanceRunIdFromCoord(ua1,replicationCoordIDTarget.get(0),0) == 1 && instanceUtil.getInstanceRunIdFromCoord(ua1,replicationCoordIDTarget.get(1),0) == 1))
				Assert.assertTrue(false);



			//wait for lates run to complete
			for(int i = 0 ; i < 30 ; i++)
			{
				if(instanceUtil.getInstanceStatusFromCoord(ua1,replicationCoordIDTarget.get(0),0).toString().equals("SUCCEEDED") && instanceUtil.getInstanceStatusFromCoord(ua1,replicationCoordIDTarget.get(1),0).toString().equals("SUCCEEDED"))
					break;

				Util.print("still in for loop");
				Thread.sleep(20000);
			}


			Thread.sleep(30000);

			//put data for the second time
			instanceUtil.putLateDataInFolders(ua2, inputFolderListForColo1,2);
			instanceUtil.putLateDataInFolders(ua3, inputFolderListForColo2,2);

			//sleep till late 2 starts 
			instanceUtil.sleepTill(ua1, instanceUtil.addMinsToTime(startTime,9));

			//check for run id to be 2
			if(!(instanceUtil.getInstanceRunIdFromCoord(ua1,replicationCoordIDTarget.get(0),0) == 2 && instanceUtil.getInstanceRunIdFromCoord(ua1,replicationCoordIDTarget.get(1),0) == 2))
				Assert.assertTrue(false);

		}

		finally{
			b1.deleteBundle(prismHelper);
			b2.deleteBundle(prismHelper);
			b3.deleteBundle(prismHelper);
		}
	}




	@Test(groups = {"multiCluster"}) 
	public void mixedTest01() throws Exception
	{
		//		this test case does the following 
		//		two source ua2 and ua3
		//		ua3 has following part data
		//		ua1/ua2
		//		ua1/ua2
		//		ua1/ua2
		//		
		//		ua2 has following part data
		//		ua1/ua3
		//		ua1/ua3
		//		ua1/ua3
		//		
		//		ua1 is the target, which in the end should have all ua1 data
		//		
		//		after first instance succeed data in put into relevant source and late should rerun
		//		
		//		after first late succeed data is put into other source and late should not rerun
		//		

		Bundle b1 = (Bundle)Util.readELBundles()[0][0];
		b1.generateUniqueBundle();
		Bundle b2 = (Bundle)Util.readELBundles()[0][0];
		b2.generateUniqueBundle();
		Bundle b3 = (Bundle)Util.readELBundles()[0][0];
		b3.generateUniqueBundle();

		try{
			b1 = new Bundle(b1,ua1.getEnvFileName());
			b2  = new Bundle(b2,ua2.getEnvFileName());
			b3  = new Bundle(b3,ua3.getEnvFileName());


			b1.setInputFeedDataPath("/samarthRetention/input-data/rawLogs/oozieExample/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/");
			Bundle.submitCluster(b1,b2,b3);


			String feed = b1.getDataSets().get(0);
			feed =  instanceUtil.setFeedCluster(feed,xmlUtil.createValidity("2009-02-01T00:00Z","2012-01-01T00:00Z"),xmlUtil.createRtention("hours(10)",ActionType.DELETE),null,ClusterType.SOURCE,null);



			String startTime = instanceUtil.getTimeWrtSystemTime(3);

			feed = instanceUtil.setFeedCluster(feed,xmlUtil.createValidity(startTime,"2099-01-01T00:00Z"),xmlUtil.createRtention("hours(10)",ActionType.DELETE),Util.readClusterName(b2.getClusters().get(0)),ClusterType.SOURCE,"ua1/${cluster.colo}");
			feed = instanceUtil.setFeedCluster(feed,xmlUtil.createValidity(startTime,"2099-01-01T00:00Z"),xmlUtil.createRtention("hours(10)",ActionType.DELETE),Util.readClusterName(b1.getClusters().get(0)),ClusterType.TARGET,null);
			feed = instanceUtil.setFeedCluster(feed,xmlUtil.createValidity(startTime,"2099-01-01T00:00Z"),xmlUtil.createRtention("hours(10)",ActionType.DELETE),Util.readClusterName(b3.getClusters().get(0)),ClusterType.SOURCE,"ua1/${cluster.colo}");

			Util.print("feed: "+feed);

			//create data in colos
/*
			String postFix = "/ua1/ua2" ;
			String prefix = b1.getFeedDataPathPrefix();
			Util.HDFSCleanup(ua2,prefix.substring(1));
			Util.lateDataReplenishWithout_Success(ua2,90,0,1,prefix,postFix);

			postFix = "/ua2/ua2" ;
			Util.lateDataReplenishWithout_Success(ua2,90,0,1,prefix,postFix);

			postFix = "/ua3/ua2" ;
			Util.lateDataReplenishWithout_Success(ua2,90,0,1,prefix,postFix);

			//put _SUCCESS in parent folder UA2
			Util.putFileInFolderHDFS(ua2,90,0,1,prefix,"_SUCCESS");

			postFix = "/ua1/ua3" ;
			Util.HDFSCleanup(ua3,prefix.substring(1));
			Util.lateDataReplenish(ua3,90,0,1,prefix,postFix);

			postFix = "/ua2/ua3" ;
			Util.lateDataReplenish(ua3,90,0,1,prefix,postFix);

			postFix = "/ua3/ua3" ;
			Util.lateDataReplenish(ua3,90,0,1,prefix,postFix);

			//put _SUCCESS in parent folder of UA3
			Util.putFileInFolderHDFS(ua3,90,0,1,prefix,"_SUCCESS");			
*/
			Thread.sleep(15000);

			//submit and schedule feed
			Util.print("feed: "+feed);

			ServiceResponse r = prismHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed);
			Thread.sleep(10000);

			//wait till 1st instance of replication coord is SUCCEEDED
			String TargetBundleID = 	instanceUtil.getLatestBundleID(Util.readDatasetName(feed),"FEED",ua1.getFeedHelper());

			ArrayList<String> replicationCoordIDTarget    = instanceUtil.getReplicationCoordID(TargetBundleID,ua1.getFeedHelper());

			for(int i = 0 ; i < 30 ; i++)
			{
				if(instanceUtil.getInstanceStatusFromCoord(ua1,replicationCoordIDTarget.get(0),0).toString().equals("SUCCEEDED") && instanceUtil.getInstanceStatusFromCoord(ua1,replicationCoordIDTarget.get(1),0).toString().equals("SUCCEEDED"))
					break;

				Util.print("still in for loop");
				Thread.sleep(20000);
			}

			Thread.sleep(15000);

			//check for exact folders to be created in ua1 :  ua1/ua2 and ua1/ua3 no other should be present. both of them should have _success


			ArrayList<String>  inputFolderListForColo1=	instanceUtil.getInputFoldersForInstanceForReplication(ua1,replicationCoordIDTarget.get(0),1);
			ArrayList<String>  inputFolderListForColo2=	instanceUtil.getInputFoldersForInstanceForReplication(ua1,replicationCoordIDTarget.get(1),1);

			String outPutLocaltion = instanceUtil.getOutputFolderForInstanceForReplication(ua1,replicationCoordIDTarget.get(0),1);
			String outPutBaseLocaltion = instanceUtil.getOutputFolderBaseForInstanceForReplication(ua1,replicationCoordIDTarget.get(0),1);

			ArrayList<String> subfolders = hadoopUtil.getHDFSSubFoldersName(ua1,outPutBaseLocaltion);

			if(!(subfolders.size() == 1 && subfolders.get(0).equals("ua1")))
				Assert.assertTrue(false);

			if(hadoopUtil.isFilePresentHDFS(ua1,outPutBaseLocaltion,"_SUCCESS"))
				Assert.assertTrue(false);


			if(!hadoopUtil.isFilePresentHDFS(ua1,outPutLocaltion,"_SUCCESS"))
				Assert.assertTrue(false);



			Util.print("folder list 1: "+inputFolderListForColo1.toString());
			Util.print("folder list 2: "+inputFolderListForColo2.toString());

			instanceUtil.putDataInFolders(ua2, inputFolderListForColo1);
			instanceUtil.putDataInFolders(ua3, inputFolderListForColo2);

			//sleep till late starts
			instanceUtil.sleepTill(ua1, instanceUtil.addMinsToTime(startTime,4));

			//check for run id to  be 1
			if(!(instanceUtil.getInstanceRunIdFromCoord(ua1,replicationCoordIDTarget.get(0),0) == 1 && instanceUtil.getInstanceRunIdFromCoord(ua1,replicationCoordIDTarget.get(1),0) == 1))
				Assert.assertTrue(false);



			//wait for lates run to complete
			for(int i = 0 ; i < 30 ; i++)
			{
				if(instanceUtil.getInstanceStatusFromCoord(ua1,replicationCoordIDTarget.get(0),0).toString().equals("SUCCEEDED") && instanceUtil.getInstanceStatusFromCoord(ua1,replicationCoordIDTarget.get(1),0).toString().equals("SUCCEEDED"))
					break;

				Util.print("still in for loop");
				Thread.sleep(20000);
			}


			Thread.sleep(30000);

			//put data for the second time
			instanceUtil.putLateDataInFolders(ua2, inputFolderListForColo1,2);
			instanceUtil.putLateDataInFolders(ua3, inputFolderListForColo2,2);

			//sleep till late 2 starts 
			instanceUtil.sleepTill(ua1, instanceUtil.addMinsToTime(startTime,9));

			//check for run id to be 2
			if(!(instanceUtil.getInstanceRunIdFromCoord(ua1,replicationCoordIDTarget.get(0),0) == 2 && instanceUtil.getInstanceRunIdFromCoord(ua1,replicationCoordIDTarget.get(1),0) == 2))
				Assert.assertTrue(false);

		}

		finally{
			b1.deleteBundle(prismHelper);
			b2.deleteBundle(prismHelper);
			b3.deleteBundle(prismHelper);
		}
	}	



	@Test(groups = {"multiCluster"})
	public void mixedTest02() throws Exception
	{
		// only difference between mixed 01 and 02 is of availability flag. feed has _success as availability flag ...so replication should not start till _success is put in ua2

		//		this test case does the following 
		//		two source ua2 and ua3
		//		ua3 has follwing part data
		//		ua1/ua2
		//		ua1/ua2
		//		ua1/ua2
		//		
		//		ua2 has following part data
		//		ua1/ua3
		//		ua1/ua3
		//		ua1/ua3
		//		
		//		ua1 is the target, which in the end should have all ua1 data
		//		
		//		after first instance succeed data in put into relevant source and late should rerun
		//		
		//		after first late succeed data is put into other source and late should not rerun
		//		

		Bundle b1 = (Bundle)Util.readELBundles()[0][0];
		b1.generateUniqueBundle();
		Bundle b2 = (Bundle)Util.readELBundles()[0][0];
		b2.generateUniqueBundle();
		Bundle b3 = (Bundle)Util.readELBundles()[0][0];
		b3.generateUniqueBundle();

		try{
			b1 = new Bundle(b1,ua1.getEnvFileName());
			b2  = new Bundle(b2,ua2.getEnvFileName());
			b3  = new Bundle(b3,ua3.getEnvFileName());


			b1.setInputFeedDataPath("/samarthRetention/input-data/rawLogs/oozieExample/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/");


			b1.setCLusterColo("ua1");
			Util.print("cluster b1: "+b1.getClusters().get(0));

			ServiceResponse r = prismHelper.getClusterHelper().submitEntity(URLS.SUBMIT_URL,b1.getClusters().get(0));
			Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));


			b2.setCLusterColo("ua2");
			Util.print("cluster b2: "+b2.getClusters().get(0));
			r = prismHelper.getClusterHelper().submitEntity(URLS.SUBMIT_URL,b2.getClusters().get(0));
			Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));


			b3.setCLusterColo("ua3");
			Util.print("cluster b3: "+b3.getClusters().get(0));
			r = prismHelper.getClusterHelper().submitEntity(URLS.SUBMIT_URL,b3.getClusters().get(0));
			Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));


			//set availability flag as _success
			b1.setInputFeedAvailabilityFlag("_SUCCESS");


			//get feed
			String feed = b1.getDataSets().get(0);
			feed =  instanceUtil.setFeedCluster(feed,xmlUtil.createValidity("2009-02-01T00:00Z","2012-01-01T00:00Z"),xmlUtil.createRtention("hours(10)",ActionType.DELETE),null,ClusterType.SOURCE,null);



			String startTime = instanceUtil.getTimeWrtSystemTime(3);

			feed = instanceUtil.setFeedCluster(feed,xmlUtil.createValidity(startTime,"2099-01-01T00:00Z"),xmlUtil.createRtention("hours(10)",ActionType.DELETE),Util.readClusterName(b2.getClusters().get(0)),ClusterType.SOURCE,"ua1/${cluster.colo}");
			feed = instanceUtil.setFeedCluster(feed,xmlUtil.createValidity(startTime,"2099-01-01T00:00Z"),xmlUtil.createRtention("hours(10)",ActionType.DELETE),Util.readClusterName(b1.getClusters().get(0)),ClusterType.TARGET,null);
			feed = instanceUtil.setFeedCluster(feed,xmlUtil.createValidity(startTime,"2099-01-01T00:00Z"),xmlUtil.createRtention("hours(10)",ActionType.DELETE),Util.readClusterName(b3.getClusters().get(0)),ClusterType.SOURCE,"ua1/${cluster.colo}");


			//create data in colos

			String postFix = "/ua1/ua2" ;
			String prefix = b1.getFeedDataPathPrefix();
			Util.HDFSCleanup(ua2,prefix.substring(1));
			Util.lateDataReplenishWithout_Success(ua2,90,0,1,prefix,postFix);

			postFix = "/ua2/ua2" ;
			Util.lateDataReplenishWithout_Success(ua2,90,0,1,prefix,postFix);

			postFix = "/ua3/ua2" ;
			Util.lateDataReplenishWithout_Success(ua2,90,0,1,prefix,postFix);

			//put _SUCCESS in parent folder UA2
			Util.putFileInFolderHDFS(ua2,90,0,1,prefix,"_SUCCESS");

			postFix = "/ua1/ua3" ;
			Util.HDFSCleanup(ua3,prefix.substring(1));
			Util.lateDataReplenish(ua3,90,0,1,prefix,postFix);

			postFix = "/ua2/ua3" ;
			Util.lateDataReplenish(ua3,90,0,1,prefix,postFix);

			postFix = "/ua3/ua3" ;
			Util.lateDataReplenish(ua3,90,0,1,prefix,postFix);

			//put _SUCCESS in parent folder of UA3
			Util.putFileInFolderHDFS(ua3,90,0,1,prefix,"_SUCCESS");			

			Thread.sleep(15000);

			//submit and schedule feed
			Util.print("feed: "+feed);


			r= prismHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed);
			Thread.sleep(10000);

			//wait till 1st instance of replication coord is SUCCEEDED
			String TargetBundleID = 	instanceUtil.getLatestBundleID(Util.readDatasetName(feed),"FEED",ua1.getFeedHelper());

			ArrayList<String> replicationCoordIDTarget    = instanceUtil.getReplicationCoordID(TargetBundleID,ua1.getFeedHelper());

			for(int i = 0 ; i < 30 ; i++)
			{
				if(instanceUtil.getInstanceStatusFromCoord(ua1,replicationCoordIDTarget.get(0),0).toString().equals("SUCCEEDED") && instanceUtil.getInstanceStatusFromCoord(ua1,replicationCoordIDTarget.get(1),0).toString().equals("SUCCEEDED"))
					break;

				Util.print("still in for loop");
				Thread.sleep(20000);
			}

			Thread.sleep(15000);

			//check for exact folders to be created in ua1 :  ua1/ua2 and ua1/ua3 no other should be present. both of them should have _success


			ArrayList<String>  inputFolderListForColo1=	instanceUtil.getInputFoldersForInstanceForReplication(ua1,replicationCoordIDTarget.get(0),0);
			ArrayList<String>  inputFolderListForColo2=	instanceUtil.getInputFoldersForInstanceForReplication(ua1,replicationCoordIDTarget.get(1),0);

			String outPutLocaltion = instanceUtil.getOutputFolderForInstanceForReplication(ua1,replicationCoordIDTarget.get(0),0);
			String outPutBaseLocaltion = instanceUtil.getOutputFolderBaseForInstanceForReplication(ua1,replicationCoordIDTarget.get(0),0);

			ArrayList<String> subfolders = hadoopUtil.getHDFSSubFoldersName(ua1,outPutBaseLocaltion);

			if(!(subfolders.size() == 1 && subfolders.get(0).equals("ua1")))
				Assert.assertTrue(false);

			if(hadoopUtil.isFilePresentHDFS(ua1,outPutBaseLocaltion,"_SUCCESS"))
				Assert.assertTrue(false);


			if(!hadoopUtil.isFilePresentHDFS(ua1,outPutLocaltion,"_SUCCESS"))
				Assert.assertTrue(false);



			Util.print("folder list 1: "+inputFolderListForColo1.toString());
			Util.print("folder list 2: "+inputFolderListForColo2.toString());

			instanceUtil.putDataInFolders(ua2, inputFolderListForColo1);
			instanceUtil.putDataInFolders(ua3, inputFolderListForColo2);

			//sleep till late starts
			instanceUtil.sleepTill(ua1, instanceUtil.addMinsToTime(startTime,4));

			//check for run id to  be 1
			if(!(instanceUtil.getInstanceRunIdFromCoord(ua1,replicationCoordIDTarget.get(0),0) == 1 && instanceUtil.getInstanceRunIdFromCoord(ua1,replicationCoordIDTarget.get(1),0) == 1))
				Assert.assertTrue(false);

		}

		finally{
			b1.deleteBundle(prismHelper);
			b2.deleteBundle(prismHelper);
			b3.deleteBundle(prismHelper);
		}
	}	
}
