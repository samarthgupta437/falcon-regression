package com.inmobi.qa.falcon.prism;

import java.lang.reflect.Method;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.inmobi.qa.falcon.bundle.Bundle;
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
public class PrismFeedReplicationPartitionExpTest {
	PrismHelper prismHelper=new PrismHelper("prism.properties");

	ColoHelper ua1=new ColoHelper("mk-qa.config.properties");

	ColoHelper ua2 = new ColoHelper("ivoryqa-1.config.properties");

	ColoHelper ua3 = new ColoHelper("gs1001.config.properties");



	
// pt : partition in target
// ps: partition in source
	
	
	@BeforeClass(alwaysRun=true)
	public void createTestData() throws Exception{
		
		System.out.println("creating test data");
		
		hadoopUtil.createDir(ua3, "/localDC/rc/billing/2012/10/01/12/00/ua2/");
		hadoopUtil.createDir(ua3, "/localDC/rc/billing/2012/10/01/12/05/ua2/");
		hadoopUtil.createDir(ua3, "/localDC/rc/billing/2012/10/01/12/10/ua2/");
		hadoopUtil.createDir(ua3, "/localDC/rc/billing/2012/10/01/12/15/ua2/");

		hadoopUtil.copyDataToFolder(ua3,new Path("/localDC/rc/billing/2012/10/01/12/00/ua2/"), "feed-s4Replication.xml");
		hadoopUtil.copyDataToFolder(ua3,new Path("/localDC/rc/billing/2012/10/01/12/05/ua2/"), "log_01.txt");
		hadoopUtil.copyDataToFolder(ua3,new Path("/localDC/rc/billing/2012/10/01/12/10/ua2/"), "src/main/resources/gs1001.config.properties");
		hadoopUtil.copyDataToFolder(ua3,new Path("/localDC/rc/billing/2012/10/01/12/15/ua2/"), "src/main/resources/log4testng.properties");
		
		hadoopUtil.createDir(ua3, "/localDC/rc/billing/2012/10/01/12/00/ua1/");
		hadoopUtil.createDir(ua3, "/localDC/rc/billing/2012/10/01/12/05/ua1/");
		hadoopUtil.createDir(ua3, "/localDC/rc/billing/2012/10/01/12/10/ua1/");
		hadoopUtil.createDir(ua3, "/localDC/rc/billing/2012/10/01/12/15/ua1/");
		
		hadoopUtil.copyDataToFolder(ua3,new Path("/localDC/rc/billing/2012/10/01/12/00/ua1/"), "feed-s4Replication.xml");
		hadoopUtil.copyDataToFolder(ua3,new Path("/localDC/rc/billing/2012/10/01/12/05/ua1/"), "log_01.txt");
		hadoopUtil.copyDataToFolder(ua3,new Path("/localDC/rc/billing/2012/10/01/12/10/ua1/"), "src/main/resources/gs1001.config.properties");
		hadoopUtil.copyDataToFolder(ua3,new Path("/localDC/rc/billing/2012/10/01/12/15/ua1/"), "src/main/resources/log4testng.properties");
		
		hadoopUtil.createDir(ua3, "/localDC/rc/billing/2012/10/01/12/00/ua3/");
		hadoopUtil.createDir(ua3, "/localDC/rc/billing/2012/10/01/12/05/ua3/");
		hadoopUtil.createDir(ua3, "/localDC/rc/billing/2012/10/01/12/10/ua3/");
		hadoopUtil.createDir(ua3, "/localDC/rc/billing/2012/10/01/12/15/ua3/");
		
		hadoopUtil.copyDataToFolder(ua3,new Path("/localDC/rc/billing/2012/10/01/12/00/ua3/"), "feed-s4Replication.xml");
		hadoopUtil.copyDataToFolder(ua3,new Path("/localDC/rc/billing/2012/10/01/12/05/ua3/"), "log_01.txt");
		hadoopUtil.copyDataToFolder(ua3,new Path("/localDC/rc/billing/2012/10/01/12/10/ua3/"), "src/main/resources/gs1001.config.properties");
		hadoopUtil.copyDataToFolder(ua3,new Path("/localDC/rc/billing/2012/10/01/12/15/ua3/"), "src/main/resources/log4testng.properties");
		
		
//---------------------------------------//
		String source02 = "/dataBillingRC/fetlrc/billing";
		hadoopUtil.createDir(ua3, source02+"/2012/10/01/12/00/ua2/");
		hadoopUtil.createDir(ua3, source02+"/2012/10/01/12/05/ua2/");
		hadoopUtil.createDir(ua3, source02+"/2012/10/01/12/10/ua2/");
		hadoopUtil.createDir(ua3, source02+"/2012/10/01/12/15/ua2/");
		hadoopUtil.createDir(ua3, source02+"/2012/10/01/12/20/ua2/");

		hadoopUtil.copyDataToFolder(ua3,new Path(source02+"/2012/10/01/12/00/ua2/"), "feed-s4Replication.xml");
		hadoopUtil.copyDataToFolder(ua3,new Path(source02+"/2012/10/01/12/05/ua2/"), "log_01.txt");
		hadoopUtil.copyDataToFolder(ua3,new Path(source02+"/2012/10/01/12/10/ua2/"), "src/main/resources/gs1001.config.properties");
		hadoopUtil.copyDataToFolder(ua3,new Path(source02+"/2012/10/01/12/15/ua2/"), "src/main/resources/log4testng.properties");
		hadoopUtil.copyDataToFolder(ua3,new Path(source02+"/2012/10/01/12/20/ua2/"), "src/main/resources/log4testng.properties");

		
		hadoopUtil.createDir(ua3, source02+"/2012/10/01/12/00/ua1/");
		hadoopUtil.createDir(ua3, source02+"/2012/10/01/12/05/ua1/");
		hadoopUtil.createDir(ua3, source02+"/2012/10/01/12/10/ua1/");
		hadoopUtil.createDir(ua3, source02+"/2012/10/01/12/15/ua1/");
		hadoopUtil.createDir(ua3, source02+"/2012/10/01/12/20/ua1/");

		
		hadoopUtil.copyDataToFolder(ua3,new Path(source02+"/2012/10/01/12/00/ua1/"), "feed-s4Replication.xml");
		hadoopUtil.copyDataToFolder(ua3,new Path(source02+"/2012/10/01/12/05/ua1/"), "log_01.txt");
		hadoopUtil.copyDataToFolder(ua3,new Path(source02+"/2012/10/01/12/10/ua1/"), "src/main/resources/gs1001.config.properties");
		hadoopUtil.copyDataToFolder(ua3,new Path(source02+"/2012/10/01/12/15/ua1/"), "src/main/resources/log4testng.properties");
		hadoopUtil.copyDataToFolder(ua3,new Path(source02+"/2012/10/01/12/20/ua1/"), "src/main/resources/log4testng.properties");

		
		hadoopUtil.createDir(ua3, source02+"/2012/10/01/12/00/ua3/");
		hadoopUtil.createDir(ua3, source02+"/2012/10/01/12/05/ua3/");
		hadoopUtil.createDir(ua3, source02+"/2012/10/01/12/10/ua3/");
		hadoopUtil.createDir(ua3, source02+"/2012/10/01/12/15/ua3/");
		hadoopUtil.createDir(ua3, source02+"/2012/10/01/12/20/ua3/");

		
		hadoopUtil.copyDataToFolder(ua3,new Path(source02+"/2012/10/01/12/00/ua3/"), "feed-s4Replication.xml");
		hadoopUtil.copyDataToFolder(ua3,new Path(source02+"/2012/10/01/12/05/ua3/"), "log_01.txt");
		hadoopUtil.copyDataToFolder(ua3,new Path(source02+"/2012/10/01/12/10/ua3/"), "src/main/resources/gs1001.config.properties");
		hadoopUtil.copyDataToFolder(ua3,new Path(source02+"/2012/10/01/12/15/ua3/"), "src/main/resources/log4testng.properties");
		hadoopUtil.copyDataToFolder(ua3,new Path(source02+"/2012/10/01/12/20/ua3/"), "src/main/resources/log4testng.properties");

		System.out.println("completed creating test data");

	}
	
	@BeforeMethod(alwaysRun=true)
	public void testName(Method method) throws Exception
	{
		Util.print("test name: "+method.getName());
		//restart server as precaution
		//Util.restartService(ua1.getClusterHelper());
		//Util.restartService(ua2.getClusterHelper());
		//Util.restartService(ua3.getClusterHelper());
	}
	
	
	
	@Test(enabled=true)
	public void blankPartition() throws Exception{
		//this test is for ideal condition when data is present in all the required places and replication takes place normally
		//partition is left blank
		
		Bundle b1 = (Bundle)Bundle.readBundle("src/test/resources/LocalDC_feedReplicaltion_BillingRC")[0][0];
		b1.generateUniqueBundle();
		Bundle b2 = (Bundle)Bundle.readBundle("src/test/resources/LocalDC_feedReplicaltion_BillingRC")[0][0];
		b2.generateUniqueBundle();
		Bundle b3 = (Bundle)Bundle.readBundle("src/test/resources/LocalDC_feedReplicaltion_BillingRC")[0][0];
		b3.generateUniqueBundle();
		try{
			b1 = new Bundle(b1,ua1.getEnvFileName());
			b2  = new Bundle(b2,ua2.getEnvFileName());
			b3  = new Bundle(b3,ua3.getEnvFileName());
			
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

			String startTimeUA1 = "2012-10-01T12:05Z" ;
			String startTimeUA2 = "2012-10-01T12:10Z";
			

			String feed = b1.getDataSets().get(0);
			feed =  instanceUtil.setFeedCluster(feed,xmlUtil.createValidity("2012-10-01T12:00Z","2010-01-01T00:00Z"),xmlUtil.createRtention("days(10000)",ActionType.DELETE),null,ClusterType.SOURCE,null,null);
		
			feed = instanceUtil.setFeedCluster(feed,xmlUtil.createValidity(startTimeUA1,"2012-10-01T12:10Z"),xmlUtil.createRtention("days(10000)",ActionType.DELETE),Util.readClusterName(b1.getClusters().get(0)),ClusterType.SOURCE,"","/localDC/rc/billing/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			feed = instanceUtil.setFeedCluster(feed,xmlUtil.createValidity(startTimeUA2,"2012-10-01T12:25Z"),xmlUtil.createRtention("days(10000)",ActionType.DELETE),Util.readClusterName(b2.getClusters().get(0)),ClusterType.TARGET,"","/clusterPath/localDC/rc/billing/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			feed = instanceUtil.setFeedCluster(feed,xmlUtil.createValidity("2012-10-01T12:00Z","2099-01-01T00:00Z"),xmlUtil.createRtention("days(10000)",ActionType.DELETE),Util.readClusterName(b3.getClusters().get(0)),ClusterType.SOURCE,"",null);

			//clean target if old data exists
			String prefix = "/data/fetlrc/billing/2012/10/01/12/";
			Util.HDFSCleanup(ua1,prefix.substring(1));
			Util.HDFSCleanup(ua2,prefix.substring(1));

			
			Util.print("feed: "+feed);

			r= prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
			Thread.sleep(10000);
			AssertUtil.assertFailed(r, "submit of feed should have fialed as the partiton in source is blank");
		
		}

		finally{
			
			prismHelper.getFeedHelper().delete(URLS.DELETE_URL, b1.getDataSets().get(0));
			prismHelper.getClusterHelper().delete(URLS.DELETE_URL, b2.getClusters().get(0));
			prismHelper.getClusterHelper().delete(URLS.DELETE_URL, b1.getClusters().get(0));
			prismHelper.getClusterHelper().delete(URLS.DELETE_URL, b3.getClusters().get(0));
		}
	}
	
	
	@Test(enabled=true)
	public void normalTest_1s1t1n_ps() throws Exception{
		//this test is for ideal condition when data is present in all the required places and replication takes place normally

		// there are 1 source clusters 10.14.110.46
		//10.14.118.26 is the target
		//data should be replicated to 10.14.118.26 from 46
		
		// path for data in target cluster should also be customized

		Bundle b1 = (Bundle)Bundle.readBundle("src/test/resources/LocalDC_feedReplicaltion_BillingRC")[0][0];
		b1.generateUniqueBundle();
		Bundle b2 = (Bundle)Bundle.readBundle("src/test/resources/LocalDC_feedReplicaltion_BillingRC")[0][0];
		b2.generateUniqueBundle();
		Bundle b3 = (Bundle)Bundle.readBundle("src/test/resources/LocalDC_feedReplicaltion_BillingRC")[0][0];
		b3.generateUniqueBundle();

		try{
			b1 = new Bundle(b1,ua1.getEnvFileName());
			b2  = new Bundle(b2,ua2.getEnvFileName());
			b3  = new Bundle(b3,ua3.getEnvFileName());
			
			Bundle.submitCluster(b1,b2,b3);
			String startTimeUA1 = "2012-10-01T12:00Z" ;
			String startTimeUA2 = "2012-10-01T12:00Z";
			

			String feed = b1.getDataSets().get(0);
			feed =  instanceUtil.setFeedCluster(feed,xmlUtil.createValidity("2012-10-01T12:00Z","2010-01-01T00:00Z"),xmlUtil.createRtention("days(100000)",ActionType.DELETE),null,ClusterType.SOURCE,null,null);
		
			feed = instanceUtil.setFeedCluster(feed,xmlUtil.createValidity(startTimeUA1,"2099-10-01T12:10Z"),xmlUtil.createRtention("days(100000)",ActionType.DELETE),Util.readClusterName(b1.getClusters().get(0)),null,null,null);
			feed = instanceUtil.setFeedCluster(feed,xmlUtil.createValidity(startTimeUA2,"2099-10-01T12:25Z"),xmlUtil.createRtention("days(100000)",ActionType.DELETE),Util.readClusterName(b2.getClusters().get(0)),ClusterType.TARGET,null,"/clusterPath/localDC/rc/billing/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			feed = instanceUtil.setFeedCluster(feed,xmlUtil.createValidity("2012-10-01T12:00Z","2099-01-01T00:00Z"),xmlUtil.createRtention("days(100000)",ActionType.DELETE),Util.readClusterName(b3.getClusters().get(0)),ClusterType.SOURCE,"${cluster.colo}","/localDC/rc/billing/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
	
			
			//clean target if old data exists
			String prefix = "/clusterPath/localDC/rc/billing/";
			Util.HDFSCleanup(ua2,prefix.substring(1));

			
			Util.print("feed: "+feed);

			ServiceResponse r = prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
			Thread.sleep(10000);
			AssertUtil.assertSucceeded(r);

			r= prismHelper.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);
			AssertUtil.assertSucceeded(r);
			Thread.sleep(15000);
			
			hadoopUtil.createDir(ua3, "/localDC/rc/billing/2012/10/01/12/00/ua3/");
			hadoopUtil.createDir(ua3, "/localDC/rc/billing/2012/10/01/12/05/ua3/");
			
			hadoopUtil.copyDataToFolder(ua3,new Path("/localDC/rc/billing/2012/10/01/12/00/ua3/"), "feed-s4Replication.xml");
			hadoopUtil.copyDataToFolder(ua3,new Path("/localDC/rc/billing/2012/10/01/12/05/ua3/"), "log_01.txt");

			
			instanceUtil.waitTillInstanceReachState(ua2,Util.getFeedName(feed),2, org.apache.oozie.client.CoordinatorAction.Status.SUCCEEDED, 7,ENTITY_TYPE.FEED);
			
			Assert.assertEquals(instanceUtil.checkIfFeedCoordExist(ua2.getFeedHelper(),Util.readDatasetName(feed),"REPLICATION"),1);
			Assert.assertEquals(instanceUtil.checkIfFeedCoordExist(ua2.getFeedHelper(),Util.readDatasetName(feed),"RETENTION"),1);
			Assert.assertEquals(instanceUtil.checkIfFeedCoordExist(ua1.getFeedHelper(),Util.readDatasetName(feed),"RETENTION"),1);
			Assert.assertEquals(instanceUtil.checkIfFeedCoordExist(ua3.getFeedHelper(),Util.readDatasetName(feed),"RETENTION"),1);



			//check if data has been replicated correctly
			
			//on ua1 only ua1 should be replicated, ua2 only ua2
			//number of files should be same as source
			
			
			ArrayList<Path> ua2ReplicatedData =hadoopUtil.getAllFilesRecursivelyHDFS(ua2, new Path("/clusterPath/localDC/rc/billing/"));
			AssertUtil.failIfStringFoundInPath(ua2ReplicatedData,"ua1","ua2");


			ArrayList<Path> ua3ReplicatedData00 =hadoopUtil.getAllFilesRecursivelyHDFS(ua3, new Path("/localDC/rc/billing/2012/10/01/12/00/ua3/"));
			ArrayList<Path> ua3ReplicatedData05 =hadoopUtil.getAllFilesRecursivelyHDFS(ua3, new Path("/localDC/rc/billing/2012/10/01/12/05/ua3/"));
		
			ArrayList<Path> ua2ReplicatedData00 =hadoopUtil.getAllFilesRecursivelyHDFS(ua2, new Path("/clusterPath/localDC/rc/billing/2012/10/01/12/00"),"_SUCCESS");
			ArrayList<Path> ua2ReplicatedData05 =hadoopUtil.getAllFilesRecursivelyHDFS(ua2, new Path("/clusterPath/localDC/rc/billing/2012/10/01/12/05"),"_SUCCESS");
		
			AssertUtil.checkForPathsSizes(ua3ReplicatedData00,ua2ReplicatedData00);
			AssertUtil.checkForPathsSizes(ua3ReplicatedData05,ua2ReplicatedData05);

			
		}

		finally{
			
			prismHelper.getFeedHelper().delete(URLS.DELETE_URL, b1.getDataSets().get(0));
			prismHelper.getClusterHelper().delete(URLS.DELETE_URL, b2.getClusters().get(0));
			prismHelper.getClusterHelper().delete(URLS.DELETE_URL, b1.getClusters().get(0));
			prismHelper.getClusterHelper().delete(URLS.DELETE_URL, b3.getClusters().get(0));
			String prefixua2 = "/clusterPath/localDC/rc/billing/";
			Util.HDFSCleanup(ua2,prefixua2.substring(1));
		}
		
		
	}
	
	
	
	@Test(enabled=true)
	public void normalTest_1s1t1n_pt() throws Exception{
		
		//this test is for ideal condition when data is present in all the required places and replication takes place normally

		// there are 1 source clusters 10.14.110.46
		//10.14.118.26 is the target
		//data should be replicated to 10.14.118.26 from 46
		
		// path for data in target cluster should also be customized

		Bundle b1 = (Bundle)Bundle.readBundle("src/test/resources/LocalDC_feedReplicaltion_BillingRC")[0][0];
		b1.generateUniqueBundle();
		Bundle b2 = (Bundle)Bundle.readBundle("src/test/resources/LocalDC_feedReplicaltion_BillingRC")[0][0];
		b2.generateUniqueBundle();
		Bundle b3 = (Bundle)Bundle.readBundle("src/test/resources/LocalDC_feedReplicaltion_BillingRC")[0][0];
		b3.generateUniqueBundle();

		try{
			b1 = new Bundle(b1,ua1.getEnvFileName());
			b2  = new Bundle(b2,ua2.getEnvFileName());
			b3  = new Bundle(b3,ua3.getEnvFileName());
			
			Bundle.submitCluster(b1,b2,b3);

			String startTimeUA1 = "2012-10-01T12:00Z" ;
			String startTimeUA2 = "2012-10-01T12:00Z";
			

			String feed = b1.getDataSets().get(0);
			feed =  instanceUtil.setFeedCluster(feed,xmlUtil.createValidity("2012-10-01T12:00Z","2010-01-01T00:00Z"),xmlUtil.createRtention("days(10000)",ActionType.DELETE),null,ClusterType.SOURCE,null,null);
		
			feed = instanceUtil.setFeedCluster(feed,xmlUtil.createValidity(startTimeUA1,"2099-10-01T12:10Z"),xmlUtil.createRtention("days(10000)",ActionType.DELETE),Util.readClusterName(b1.getClusters().get(0)),null,null,null);
			feed = instanceUtil.setFeedCluster(feed,xmlUtil.createValidity(startTimeUA2,"2099-10-01T12:25Z"),xmlUtil.createRtention("days(10000)",ActionType.DELETE),Util.readClusterName(b2.getClusters().get(0)),ClusterType.TARGET,"${cluster.colo}","/clusterPath/localDC/rc/billing/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			feed = instanceUtil.setFeedCluster(feed,xmlUtil.createValidity("2012-10-01T12:00Z","2099-01-01T00:00Z"),xmlUtil.createRtention("days(10000)",ActionType.DELETE),Util.readClusterName(b3.getClusters().get(0)),ClusterType.SOURCE,null,"/localDC/rc/billing/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
	
			
			//clean target if old data exists
			String prefix = "/clusterPath/localDC/rc/billing/";
			Util.HDFSCleanup(ua2,prefix.substring(1));

			
			Util.print("feed: "+feed);

			ServiceResponse r = prismHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed);
			Thread.sleep(10000);
			AssertUtil.assertSucceeded(r);

/*			r= prismHelper.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);
			AssertUtil.assertSucceeded(r);
			Thread.sleep(15000);*/
			
			
			
			
			instanceUtil.waitTillInstanceReachState(ua2,Util.getFeedName(feed),2, org.apache.oozie.client.CoordinatorAction.Status.SUCCEEDED, 7,ENTITY_TYPE.FEED);
			
			Assert.assertEquals(instanceUtil.checkIfFeedCoordExist(ua2.getFeedHelper(),Util.readDatasetName(feed),"REPLICATION"),1);
			Assert.assertEquals(instanceUtil.checkIfFeedCoordExist(ua2.getFeedHelper(),Util.readDatasetName(feed),"RETENTION"),1);
			Assert.assertEquals(instanceUtil.checkIfFeedCoordExist(ua1.getFeedHelper(),Util.readDatasetName(feed),"RETENTION"),1);
			Assert.assertEquals(instanceUtil.checkIfFeedCoordExist(ua3.getFeedHelper(),Util.readDatasetName(feed),"RETENTION"),1);



			//check if data has been replicated correctly
			
			//on ua1 only ua1 should be replicated, ua2 only ua2
			//number of files should be same as source
			
			
			ArrayList<Path> ua2ReplicatedData =hadoopUtil.getAllFilesRecursivelyHDFS(ua2, new Path("/clusterPath/localDC/rc/billing/"));
			AssertUtil.failIfStringFoundInPath(ua2ReplicatedData,"ua1","ua3");


			ArrayList<Path> ua3ReplicatedData00 =hadoopUtil.getAllFilesRecursivelyHDFS(ua3, new Path("/localDC/rc/billing/2012/10/01/12/00/ua2/"));
			ArrayList<Path> ua3ReplicatedData05 =hadoopUtil.getAllFilesRecursivelyHDFS(ua3, new Path("/localDC/rc/billing/2012/10/01/12/05/ua2/"));
		
			ArrayList<Path> ua2ReplicatedData00 =hadoopUtil.getAllFilesRecursivelyHDFS(ua2, new Path("/clusterPath/localDC/rc/billing/2012/10/01/12/00"),"_SUCCESS");
			ArrayList<Path> ua2ReplicatedData05 =hadoopUtil.getAllFilesRecursivelyHDFS(ua2, new Path("/clusterPath/localDC/rc/billing/2012/10/01/12/05"),"_SUCCESS");
		
			AssertUtil.checkForPathsSizes(ua3ReplicatedData00,ua2ReplicatedData00);
			AssertUtil.checkForPathsSizes(ua3ReplicatedData05,ua2ReplicatedData05);

			
		}

		finally{
			
			prismHelper.getFeedHelper().delete(URLS.DELETE_URL, b1.getDataSets().get(0));
			prismHelper.getClusterHelper().delete(URLS.DELETE_URL, b2.getClusters().get(0));
			prismHelper.getClusterHelper().delete(URLS.DELETE_URL, b1.getClusters().get(0));
			prismHelper.getClusterHelper().delete(URLS.DELETE_URL, b3.getClusters().get(0));
			String prefix = "/clusterPath/localDC/rc/billing/";
			Util.HDFSCleanup(ua2,prefix.substring(1));

		}
		
		
	}
	
	
	
	@Test(enabled=true)
	public void normalTest_1s2t_pt() throws Exception{
		//this test is for ideal condition when data is present in all the required places and replication takes place normally

		//10.14.110.46 is global cluster where test data is present in location /data/fetlrc/billing/2012/10/01/12/(00 to 30)
		//data should be replicated to folder on 10.14.117.33 and 10.14.118.26 as targets 
		//ua3 is the source and ua1 and ua2 are target

		Bundle b1 = (Bundle)Bundle.readBundle("src/test/resources/LocalDC_feedReplicaltion_BillingRC")[0][0];
		b1.generateUniqueBundle();
		Bundle b2 = (Bundle)Bundle.readBundle("src/test/resources/LocalDC_feedReplicaltion_BillingRC")[0][0];
		b2.generateUniqueBundle();
		Bundle b3 = (Bundle)Bundle.readBundle("src/test/resources/LocalDC_feedReplicaltion_BillingRC")[0][0];
		b3.generateUniqueBundle();

		try{
			b1 = new Bundle(b1,ua1.getEnvFileName());
			b2  = new Bundle(b2,ua2.getEnvFileName());
			b3  = new Bundle(b3,ua3.getEnvFileName());
			
	        Bundle.submitCluster(b1,b2,b3);
			String startTimeUA1 = "2012-10-01T12:05Z" ;
			String startTimeUA2 = "2012-10-01T12:10Z";
			

			String feed = b1.getDataSets().get(0);
			feed = instanceUtil.setFeedFilePath(feed,"/dataBillingRC/fetlrc/billing/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/");
			feed =  instanceUtil.setFeedCluster(feed,xmlUtil.createValidity("2012-10-01T12:00Z","2010-01-01T00:00Z"),xmlUtil.createRtention("days(10000)",ActionType.DELETE),null,ClusterType.SOURCE,null);
		
			feed = instanceUtil.setFeedCluster(feed,xmlUtil.createValidity(startTimeUA1,"2012-10-01T12:10Z"),xmlUtil.createRtention("days(10000)",ActionType.DELETE),Util.readClusterName(b1.getClusters().get(0)),ClusterType.TARGET,"${cluster.colo}");
			feed = instanceUtil.setFeedCluster(feed,xmlUtil.createValidity(startTimeUA2,"2012-10-01T12:25Z"),xmlUtil.createRtention("days(10000)",ActionType.DELETE),Util.readClusterName(b2.getClusters().get(0)),ClusterType.TARGET,"${cluster.colo}");
			feed = instanceUtil.setFeedCluster(feed,xmlUtil.createValidity("2012-10-01T12:00Z","2099-01-01T00:00Z"),xmlUtil.createRtention("days(10000)",ActionType.DELETE),Util.readClusterName(b3.getClusters().get(0)),ClusterType.SOURCE,null);

			//clean target if old data exists
			String prefix = "/dataBillingRC/fetlrc/billing/2012/10/01/12/";
			Util.HDFSCleanup(ua1,prefix.substring(1));
			Util.HDFSCleanup(ua2,prefix.substring(1));

			
			Util.print("feed: "+feed);

			ServiceResponse r = prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
			Thread.sleep(10000);
			AssertUtil.assertSucceeded(r);

			r= prismHelper.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);
			Thread.sleep(15000);
			
			instanceUtil.waitTillInstanceReachState(ua1,Util.getFeedName(feed),1, org.apache.oozie.client.CoordinatorAction.Status.SUCCEEDED, 7,ENTITY_TYPE.FEED);
			instanceUtil.waitTillInstanceReachState(ua2,Util.getFeedName(feed),3, org.apache.oozie.client.CoordinatorAction.Status.SUCCEEDED, 7,ENTITY_TYPE.FEED);

			//check if data has been replicated correctly
			
			//on ua1 only ua1 should be replicated, ua2 only ua2
			//number of files should be same as source
			
			
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
			String prefixua1 = "/localDC/rc/billing/";
			String prefixua2 = "/localDC/rc/billing/";

			Util.HDFSCleanup(ua1,prefixua1.substring(1));
			Util.HDFSCleanup(ua2,prefixua2.substring(1));
		}
	}
	
	@Test(enabled=true)
	public void normalTest_2s1t_pt() throws Exception{
		//this test is for ideal condition when data is present in all the required places and replication takes place normally

		// there are 2 source clusters 10.14.110.46 and 10.14.117.33 
		//10.14.118.26 is the target
		//data should be replicated to 10.14.118.26 from ua2 sub dir of 46 and 33
		// source cluster path in 33 should be mentioned in cluster definition 
		// path for data in target cluster should also be customized
		Bundle b1 = (Bundle)Bundle.readBundle("src/test/resources/LocalDC_feedReplicaltion_BillingRC")[0][0];
		b1.generateUniqueBundle();
		Bundle b2 = (Bundle)Bundle.readBundle("src/test/resources/LocalDC_feedReplicaltion_BillingRC")[0][0];
		b2.generateUniqueBundle();
		Bundle b3 = (Bundle)Bundle.readBundle("src/test/resources/LocalDC_feedReplicaltion_BillingRC")[0][0];
		b3.generateUniqueBundle();

		try{
			b1 = new Bundle(b1,ua1.getEnvFileName());
			b2  = new Bundle(b2,ua2.getEnvFileName());
			b3  = new Bundle(b3,ua3.getEnvFileName());
			
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

			String startTimeUA1 = "2012-10-01T12:05Z" ;
			String startTimeUA2 = "2012-10-01T12:10Z";
			

			String feed = b1.getDataSets().get(0);
			feed =  instanceUtil.setFeedCluster(feed,xmlUtil.createValidity("2012-10-01T12:00Z","2010-01-01T00:00Z"),xmlUtil.createRtention("days(10000)",ActionType.DELETE),null,ClusterType.SOURCE,null,null);
		
			feed = instanceUtil.setFeedCluster(feed,xmlUtil.createValidity(startTimeUA1,"2012-10-01T12:10Z"),xmlUtil.createRtention("days(10000)",ActionType.DELETE),Util.readClusterName(b1.getClusters().get(0)),ClusterType.SOURCE,null,"/localDC/rc/billing/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			feed = instanceUtil.setFeedCluster(feed,xmlUtil.createValidity(startTimeUA2,"2012-10-01T12:25Z"),xmlUtil.createRtention("days(10000)",ActionType.DELETE),Util.readClusterName(b2.getClusters().get(0)),ClusterType.TARGET,"${cluster.colo}","/clusterPath/localDC/rc/billing/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			feed = instanceUtil.setFeedCluster(feed,xmlUtil.createValidity("2012-10-01T12:00Z","2099-01-01T00:00Z"),xmlUtil.createRtention("days(10000)",ActionType.DELETE),Util.readClusterName(b3.getClusters().get(0)),ClusterType.SOURCE,null,null);

			//clean target if old data exists
			String prefix = "/data/regression/fetlrc/billing/";
			Util.HDFSCleanup(ua1,prefix.substring(1));
			Util.HDFSCleanup(ua2,prefix.substring(1));

			Util.print("feed: "+feed);

			r= prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
			Thread.sleep(10000);
			AssertUtil.assertSucceeded(r);

			r= prismHelper.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);
			AssertUtil.assertSucceeded(r);
			Thread.sleep(15000);
			
			instanceUtil.waitTillInstanceReachState(ua1,Util.getFeedName(feed),1, org.apache.oozie.client.CoordinatorAction.Status.SUCCEEDED, 7,ENTITY_TYPE.FEED);
			instanceUtil.waitTillInstanceReachState(ua2,Util.getFeedName(feed),3, org.apache.oozie.client.CoordinatorAction.Status.SUCCEEDED, 7,ENTITY_TYPE.FEED);

			//check if data has been replicated correctly
			
			//on ua1 only ua1 should be replicated, ua2 only ua2
			//number of files should be same as source
			
			
			ArrayList<Path> ua1ReplicatedData =hadoopUtil.getAllFilesRecursivelyHDFS(ua1, new Path("/data/fetlrc/billing/2012/10/01/12/"));
			//check for no ua2 or ua3 in ua1
			AssertUtil.failIfStringFoundInPath(ua1ReplicatedData,"ua2","ua3");
			
			ArrayList<Path> ua2ReplicatedData =hadoopUtil.getAllFilesRecursivelyHDFS(ua2, new Path("/data/fetlrc/billing/2012/10/01/12/"));
			AssertUtil.failIfStringFoundInPath(ua2ReplicatedData,"ua1","ua3");


			ArrayList<Path> ua1ReplicatedData00 =hadoopUtil.getAllFilesRecursivelyHDFS(ua1, new Path("/data/fetlrc/billing/2012/10/01/12/00/"),"_SUCCESS");
			ArrayList<Path> ua1ReplicatedData05 =hadoopUtil.getAllFilesRecursivelyHDFS(ua1, new Path("/data/fetlrc/billing/2012/10/01/12/05/"),"_SUCCESS");
		
			ArrayList<Path> ua2ReplicatedData10 =hadoopUtil.getAllFilesRecursivelyHDFS(ua2, new Path("/data/fetlrc/billing/2012/10/01/12/10"),"_SUCCESS");
			ArrayList<Path> ua2ReplicatedData15 =hadoopUtil.getAllFilesRecursivelyHDFS(ua2, new Path("/data/fetlrc/billing/2012/10/01/12/15"),"_SUCCESS");
			
			ArrayList<Path> ua3OriginalData00ua1 = hadoopUtil.getAllFilesRecursivelyHDFS(ua2, new Path("/data/fetlrc/billing/2012/10/01/12/00/ua1"),"_SUCCESS");
			ArrayList<Path> ua3OriginalData05ua1 = hadoopUtil.getAllFilesRecursivelyHDFS(ua2, new Path("/data/fetlrc/billing/2012/10/01/12/05/ua1"),"_SUCCESS");
			ArrayList<Path> ua3OriginalData10ua2 = hadoopUtil.getAllFilesRecursivelyHDFS(ua2, new Path("/data/fetlrc/billing/2012/10/01/12/10/ua2"),"_SUCCESS");
			ArrayList<Path> ua3OriginalData15ua2 = hadoopUtil.getAllFilesRecursivelyHDFS(ua2, new Path("/data/fetlrc/billing/2012/10/01/12/15/ua2"),"_SUCCESS");

			AssertUtil.checkForPathsSizes(ua1ReplicatedData00,new ArrayList<Path>());
			AssertUtil.checkForPathsSizes(ua1ReplicatedData05,ua3OriginalData05ua1);
			AssertUtil.checkForPathsSizes(ua2ReplicatedData10,ua3OriginalData10ua2);
			AssertUtil.checkForPathsSizes(ua2ReplicatedData15,ua3OriginalData15ua2);
			
		}

		finally{
			
			prismHelper.getFeedHelper().delete(URLS.DELETE_URL, b1.getDataSets().get(0));
			prismHelper.getClusterHelper().delete(URLS.DELETE_URL, b2.getClusters().get(0));
			prismHelper.getClusterHelper().delete(URLS.DELETE_URL, b1.getClusters().get(0));
			prismHelper.getClusterHelper().delete(URLS.DELETE_URL, b3.getClusters().get(0));
			String prefixua1 = "/localDC/rc/billing/";
			String prefixua2 = "/localDC/rc/billing/";

			Util.HDFSCleanup(ua1,prefixua1.substring(1));
			Util.HDFSCleanup(ua2,prefixua2.substring(1));
		}
	}
	
	
	
	@Test(enabled=true)
	public void normalTest_1s2t_ps() throws Exception{
		
		//this test is for ideal condition when data is present in all the required places and replication takes place normally

		//10.14.110.46 is global cluster where test data is present in location /data/fetlrc/billing/2012/10/01/12/(00 to 30)
		//data should be replicated to folder on 10.14.117.33 and 10.14.118.26 as targets 
		//ua3 is the source and ua1 and ua2 are target

		Bundle b1 = (Bundle)Bundle.readBundle("src/test/resources/LocalDC_feedReplicaltion_BillingRC")[0][0];
		b1.generateUniqueBundle();
		Bundle b2 = (Bundle)Bundle.readBundle("src/test/resources/LocalDC_feedReplicaltion_BillingRC")[0][0];
		b2.generateUniqueBundle();
		Bundle b3 = (Bundle)Bundle.readBundle("src/test/resources/LocalDC_feedReplicaltion_BillingRC")[0][0];
		b3.generateUniqueBundle();

		try{
			b1 = new Bundle(b1,ua1.getEnvFileName());
			b2  = new Bundle(b2,ua2.getEnvFileName());
			b3  = new Bundle(b3,ua3.getEnvFileName());
			
			Bundle.submitCluster(b1,b2,b3);

			String startTimeUA1 = "2012-10-01T12:05Z" ;
			String startTimeUA2 = "2012-10-01T12:10Z";
			

			String feed = b1.getDataSets().get(0);
			feed = instanceUtil.setFeedFilePath(feed,"/localDC/rc/billing/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/");
			feed =  instanceUtil.setFeedCluster(feed,xmlUtil.createValidity("2012-10-01T12:00Z","2010-01-01T00:00Z"),xmlUtil.createRtention("days(10000000)",ActionType.DELETE),null,ClusterType.SOURCE,null);
		
			feed = instanceUtil.setFeedCluster(feed,xmlUtil.createValidity(startTimeUA1,"2012-10-01T12:11Z"),xmlUtil.createRtention("days(10000000)",ActionType.DELETE),Util.readClusterName(b1.getClusters().get(0)),ClusterType.TARGET,null,"/localDC/rc/billing/ua1/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/");
			feed = instanceUtil.setFeedCluster(feed,xmlUtil.createValidity(startTimeUA2,"2012-10-01T12:26Z"),xmlUtil.createRtention("days(10000000)",ActionType.DELETE),Util.readClusterName(b2.getClusters().get(0)),ClusterType.TARGET,null,"/localDC/rc/billing/ua2/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/");
			feed = instanceUtil.setFeedCluster(feed,xmlUtil.createValidity("2012-10-01T12:00Z","2099-01-01T00:00Z"),xmlUtil.createRtention("days(10000000)",ActionType.DELETE),Util.readClusterName(b3.getClusters().get(0)),ClusterType.SOURCE,"${cluster.colo}");

			//clean target if old data exists
			String prefixua1 = "/localDC/rc/billing/";
			String prefixua2 = "/localDC/rc/billing/";

			Util.HDFSCleanup(ua1,prefixua1.substring(1));
			Util.HDFSCleanup(ua2,prefixua2.substring(1));

			
			Util.print("feed: "+feed);

			ServiceResponse r = prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
			Thread.sleep(10000);
			AssertUtil.assertSucceeded(r);

			r= prismHelper.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);
			Thread.sleep(15000);
			
			instanceUtil.waitTillInstanceReachState(ua1,Util.getFeedName(feed),1, org.apache.oozie.client.CoordinatorAction.Status.SUCCEEDED, 7,ENTITY_TYPE.FEED);
			instanceUtil.waitTillInstanceReachState(ua2,Util.getFeedName(feed),2, org.apache.oozie.client.CoordinatorAction.Status.SUCCEEDED, 7,ENTITY_TYPE.FEED);

			//check if data has been replicated correctly
			
			//on ua1 only ua1 should be replicated, ua2 only ua2
			//number of files should be same as source
			
			
			ArrayList<Path> ua1ReplicatedData =hadoopUtil.getAllFilesRecursivelyHDFS(ua1, new Path("/localDC/rc/billing/ua1/2012/10/01/12/"));
			//check for no ua2 or ua3 in ua1
			AssertUtil.failIfStringFoundInPath(ua1ReplicatedData,"ua2");
			
			ArrayList<Path> ua2ReplicatedData =hadoopUtil.getAllFilesRecursivelyHDFS(ua2, new Path("/localDC/rc/billing/ua2/2012/10/01/12/"));
			AssertUtil.failIfStringFoundInPath(ua2ReplicatedData,"ua1");


			ArrayList<Path> ua1ReplicatedData00 =hadoopUtil.getAllFilesRecursivelyHDFS(ua1, new Path("/localDC/rc/billing/ua1/2012/10/01/12/00/"),"_SUCCESS");
			ArrayList<Path> ua1ReplicatedData05 =hadoopUtil.getAllFilesRecursivelyHDFS(ua1, new Path("/localDC/rc/billing/ua1/2012/10/01/12/05/"),"_SUCCESS");
		
			ArrayList<Path> ua2ReplicatedData10 =hadoopUtil.getAllFilesRecursivelyHDFS(ua2, new Path("/localDC/rc/billing/ua2/2012/10/01/12/10"),"_SUCCESS");
			ArrayList<Path> ua2ReplicatedData15 =hadoopUtil.getAllFilesRecursivelyHDFS(ua2, new Path("/localDC/rc/billing/ua2/2012/10/01/12/15"),"_SUCCESS");
			
		//	ArrayList<Path> ua3OriginalData00ua1 = hadoopUtil.getAllFilesRecursivelyHDFS(ua2, new Path("/localDC/rc/billing/2012/10/01/12/00/ua1"),"_SUCCESS");
			ArrayList<Path> ua3OriginalData05ua1 = hadoopUtil.getAllFilesRecursivelyHDFS(ua3, new Path("/localDC/rc/billing/2012/10/01/12/05/ua3"),"_SUCCESS");
			ArrayList<Path> ua3OriginalData10ua2 = hadoopUtil.getAllFilesRecursivelyHDFS(ua3, new Path("/localDC/rc/billing/2012/10/01/12/10/ua3"),"_SUCCESS");
			ArrayList<Path> ua3OriginalData15ua2 = hadoopUtil.getAllFilesRecursivelyHDFS(ua3, new Path("/localDC/rc/billing/2012/10/01/12/15/ua3"),"_SUCCESS");

			AssertUtil.checkForPathsSizes(ua1ReplicatedData00,new ArrayList<Path>());
			AssertUtil.checkForPathsSizes(ua1ReplicatedData05,ua3OriginalData05ua1);
			AssertUtil.checkForPathsSizes(ua2ReplicatedData10,ua3OriginalData10ua2);
			AssertUtil.checkForPathsSizes(ua2ReplicatedData15,ua3OriginalData15ua2);
			
		}

		finally{
			
			prismHelper.getFeedHelper().delete(URLS.DELETE_URL, b1.getDataSets().get(0));
			prismHelper.getClusterHelper().delete(URLS.DELETE_URL, b2.getClusters().get(0));
			prismHelper.getClusterHelper().delete(URLS.DELETE_URL, b1.getClusters().get(0));
			prismHelper.getClusterHelper().delete(URLS.DELETE_URL, b3.getClusters().get(0));
		//	String prefixua1 = "/localDC/rc/billing/";
		//	String prefixua2 = "/localDC/rc/billing/";

		//	Util.HDFSCleanup(ua1,prefixua1.substring(1));
		//	Util.HDFSCleanup(ua2,prefixua2.substring(1));
		}
		
		
	}
	
	
	@Test(enabled=true)
	public void normalTest_2s1t_ps() throws Exception{
		//this test is for ideal condition when data is present in all the required places and replication takes place normally

		// there are 2 source clusters 10.14.110.46 and 10.14.117.33 
		//10.14.118.26 is the target
		//data should be replicated to 10.14.118.26 from ua2 sub dir of 46 and 33
		// source cluster path in 33 should be mentioned in cluster definition 
		// path for data in target cluster should also be customized
		Bundle b1 = (Bundle)Bundle.readBundle("src/test/resources/LocalDC_feedReplicaltion_BillingRC")[0][0];
		b1.generateUniqueBundle();
		Bundle b2 = (Bundle)Bundle.readBundle("src/test/resources/LocalDC_feedReplicaltion_BillingRC")[0][0];
		b2.generateUniqueBundle();
		Bundle b3 = (Bundle)Bundle.readBundle("src/test/resources/LocalDC_feedReplicaltion_BillingRC")[0][0];
		b3.generateUniqueBundle();

		try{
			b1 = new Bundle(b1,ua1.getEnvFileName());
			b2  = new Bundle(b2,ua2.getEnvFileName());
			b3  = new Bundle(b3,ua3.getEnvFileName());
			
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

			String startTimeUA1 = "2012-10-01T12:00Z" ;
			String startTimeUA2 = "2012-10-01T12:00Z";
			

			String feed = b1.getDataSets().get(0);
			feed =  instanceUtil.setFeedCluster(feed,xmlUtil.createValidity("2012-10-01T12:00Z","2010-01-01T00:00Z"),xmlUtil.createRtention("days(10000)",ActionType.DELETE),null,ClusterType.SOURCE,null,null);
		
			feed = instanceUtil.setFeedCluster(feed,xmlUtil.createValidity(startTimeUA1,"2099-10-01T12:10Z"),xmlUtil.createRtention("days(10000)",ActionType.DELETE),Util.readClusterName(b1.getClusters().get(0)),ClusterType.SOURCE,"${cluster.colo}","/source/localDC/rc/billing/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			feed = instanceUtil.setFeedCluster(feed,xmlUtil.createValidity(startTimeUA2,"2099-10-01T12:25Z"),xmlUtil.createRtention("days(10000)",ActionType.DELETE),Util.readClusterName(b2.getClusters().get(0)),ClusterType.TARGET,null,"/clusterPath/localDC/rc/billing/replicated/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			feed = instanceUtil.setFeedCluster(feed,xmlUtil.createValidity("2012-10-01T12:00Z","2099-01-01T00:00Z"),xmlUtil.createRtention("days(10000)",ActionType.DELETE),Util.readClusterName(b3.getClusters().get(0)),ClusterType.SOURCE,"${cluster.colo}","/localDC/rc/billing/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");

			//clean target if old data exists
			String prefix = "/clusterPath/localDC/rc/billing/";
			Util.HDFSCleanup(ua2,prefix.substring(1));

			
			Util.print("feed: "+feed);

			r= prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
			Thread.sleep(10000);
			AssertUtil.assertSucceeded(r);

			r= prismHelper.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);
			AssertUtil.assertSucceeded(r);
			Thread.sleep(15000);
			
			instanceUtil.waitTillInstanceReachState(ua2,Util.getFeedName(feed),2, org.apache.oozie.client.CoordinatorAction.Status.SUCCEEDED, 7,ENTITY_TYPE.FEED);

			//check if data has been replicated correctly
			
			//on ua1 only ua1 should be replicated, ua2 only ua2
			//number of files should be same as source
			
			
			ArrayList<Path> ua2ReplicatedData =hadoopUtil.getAllFilesRecursivelyHDFS(ua2, new Path("/clusterPath/localDC/rc/billing/replicated/2012/10/01/12/"));
			AssertUtil.failIfStringFoundInPath(ua2ReplicatedData,"ua2");


			ArrayList<Path> ua2ReplicatedData00ua1 =hadoopUtil.getAllFilesRecursivelyHDFS(ua2, new Path("/clusterPath/localDC/rc/billing/replicated/2012/10/01/12/00/ua1"),"_SUCCESS");
			ArrayList<Path> ua2ReplicatedData05ua3 =hadoopUtil.getAllFilesRecursivelyHDFS(ua2, new Path("/clusterPath/localDC/rc/billing/replicated/2012/10/01/12/05/ua3/"),"_SUCCESS");
			
			
			ArrayList<Path> ua1OriginalData00 = hadoopUtil.getAllFilesRecursivelyHDFS(ua1, new Path("/source/localDC/rc/billing/2012/10/01/12/00/ua1"),"_SUCCESS");
			ArrayList<Path> ua3OriginalData05 = hadoopUtil.getAllFilesRecursivelyHDFS(ua3, new Path("/localDC/rc/billing/2012/10/01/12/05/ua1"),"_SUCCESS");
			
			AssertUtil.checkForPathsSizes(ua2ReplicatedData00ua1,ua1OriginalData00);
			AssertUtil.checkForPathsSizes(ua2ReplicatedData05ua3,ua3OriginalData05);

			
		}

		finally{
			
			prismHelper.getFeedHelper().delete(URLS.DELETE_URL, b1.getDataSets().get(0));
			prismHelper.getClusterHelper().delete(URLS.DELETE_URL, b2.getClusters().get(0));
			prismHelper.getClusterHelper().delete(URLS.DELETE_URL, b1.getClusters().get(0));
			prismHelper.getClusterHelper().delete(URLS.DELETE_URL, b3.getClusters().get(0));
			String prefix = "/clusterPath/localDC/rc/billing/";
			Util.HDFSCleanup(ua2,prefix.substring(1));
		}
	}
	
	
	
	@Test(enabled=true)
	public void normalTest_1s2t_pst() throws Exception{
		
		
		//this test is for ideal condition when data is present in all the required places and replication takes place normally

		//10.14.110.46 is global cluster where test data is present in location /data/fetlrc/billing/2012/10/01/12/(00 to 30)
		//data should be replicated to folder on 10.14.117.33 and 10.14.118.26 as targets 
		//ua3 is the source and ua1 and ua2 are target

		Bundle b1 = (Bundle)Bundle.readBundle("src/test/resources/LocalDC_feedReplicaltion_BillingRC")[0][0];
		b1.generateUniqueBundle();
		Bundle b2 = (Bundle)Bundle.readBundle("src/test/resources/LocalDC_feedReplicaltion_BillingRC")[0][0];
		b2.generateUniqueBundle();
		Bundle b3 = (Bundle)Bundle.readBundle("src/test/resources/LocalDC_feedReplicaltion_BillingRC")[0][0];
		b3.generateUniqueBundle();

		try{
			b1 = new Bundle(b1,ua1.getEnvFileName());
			b2  = new Bundle(b2,ua2.getEnvFileName());
			b3  = new Bundle(b3,ua3.getEnvFileName());
			
			

			
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

			String startTimeUA1 = "2012-10-01T12:05Z" ;
			String startTimeUA2 = "2012-10-01T12:10Z";
			

			String feed = b1.getDataSets().get(0);
			feed = instanceUtil.setFeedFilePath(feed,"/localDC/rc/billing/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/");
			feed =  instanceUtil.setFeedCluster(feed,xmlUtil.createValidity("2012-10-01T12:00Z","2010-01-01T00:00Z"),xmlUtil.createRtention("days(10000)",ActionType.DELETE),null,ClusterType.SOURCE,null);
		
			feed = instanceUtil.setFeedCluster(feed,xmlUtil.createValidity(startTimeUA1,"2099-10-01T12:10Z"),xmlUtil.createRtention("days(10000)",ActionType.DELETE),Util.readClusterName(b1.getClusters().get(0)),ClusterType.TARGET,"${cluster.colo}","/localDC/rc/billing/ua1/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/");
			feed = instanceUtil.setFeedCluster(feed,xmlUtil.createValidity(startTimeUA2,"2099-10-01T12:25Z"),xmlUtil.createRtention("days(10000)",ActionType.DELETE),Util.readClusterName(b2.getClusters().get(0)),ClusterType.TARGET,"${cluster.colo}","/localDC/rc/billing/ua2/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/");
			feed = instanceUtil.setFeedCluster(feed,xmlUtil.createValidity("2012-10-01T12:00Z","2099-01-01T00:00Z"),xmlUtil.createRtention("days(10000)",ActionType.DELETE),Util.readClusterName(b3.getClusters().get(0)),ClusterType.SOURCE,"${cluster.colo}");

			//clean target if old data exists
			String prefixua1 = "/localDC/rc/billing/";
			String prefixua2 = "/localDC/rc/billing/";

			Util.HDFSCleanup(ua1,prefixua1.substring(1));
			Util.HDFSCleanup(ua2,prefixua2.substring(1));

			
			Util.print("feed: "+feed);

			r= prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
			Thread.sleep(10000);
			AssertUtil.assertSucceeded(r);

			r= prismHelper.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);
			Thread.sleep(15000);
			
			instanceUtil.waitTillInstanceReachState(ua1,Util.getFeedName(feed),1, org.apache.oozie.client.CoordinatorAction.Status.SUCCEEDED, 7,ENTITY_TYPE.FEED);
			instanceUtil.waitTillInstanceReachState(ua2,Util.getFeedName(feed),3, org.apache.oozie.client.CoordinatorAction.Status.SUCCEEDED, 7,ENTITY_TYPE.FEED);

			//check if data has been replicated correctly
			
			//on ua1 only ua1 should be replicated, ua2 only ua2
			//number of files should be same as source
			
			
			ArrayList<Path> ua1ReplicatedData =hadoopUtil.getAllFilesRecursivelyHDFS(ua1, new Path("/localDC/rc/billing/ua1/2012/10/01/12/"));
			//check for no ua2 or ua3 in ua1
			AssertUtil.failIfStringFoundInPath(ua1ReplicatedData,"ua2","ua3");
			
			ArrayList<Path> ua2ReplicatedData =hadoopUtil.getAllFilesRecursivelyHDFS(ua2, new Path("/localDC/rc/billing/ua2/2012/10/01/12/"));
			AssertUtil.failIfStringFoundInPath(ua2ReplicatedData,"ua1","ua3");


			ArrayList<Path> ua1ReplicatedData00 =hadoopUtil.getAllFilesRecursivelyHDFS(ua1, new Path("/localDC/rc/billing/ua1/2012/10/01/12/00/"),"_SUCCESS");
			ArrayList<Path> ua1ReplicatedData10 =hadoopUtil.getAllFilesRecursivelyHDFS(ua1, new Path("/localDC/rc/billing/ua1/2012/10/01/12/10/"),"_SUCCESS");
		
			ArrayList<Path> ua2ReplicatedData10 =hadoopUtil.getAllFilesRecursivelyHDFS(ua2, new Path("/localDC/rc/billing/ua2/2012/10/01/12/10"),"_SUCCESS");
			ArrayList<Path> ua2ReplicatedData15 =hadoopUtil.getAllFilesRecursivelyHDFS(ua2, new Path("/localDC/rc/billing/ua2/2012/10/01/12/15"),"_SUCCESS");
			
			ArrayList<Path> ua3OriginalData00ua1 = hadoopUtil.getAllFilesRecursivelyHDFS(ua2, new Path("/localDC/rc/billing/2012/10/01/12/00/ua1"),"_SUCCESS");
			ArrayList<Path> ua3OriginalData10ua1 = hadoopUtil.getAllFilesRecursivelyHDFS(ua2, new Path("/localDC/rc/billing/2012/10/01/12/10/ua1"),"_SUCCESS");
			ArrayList<Path> ua3OriginalData10ua2 = hadoopUtil.getAllFilesRecursivelyHDFS(ua2, new Path("/localDC/rc/billing/2012/10/01/12/10/ua2"),"_SUCCESS");
			ArrayList<Path> ua3OriginalData15ua2 = hadoopUtil.getAllFilesRecursivelyHDFS(ua2, new Path("/localDC/rc/billing/2012/10/01/12/15/ua2"),"_SUCCESS");

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
			String prefixua1 = "/localDC/rc/billing/";
			String prefixua2 = "/localDC/rc/billing/";
			Util.HDFSCleanup(ua1,prefixua1.substring(1));
			Util.HDFSCleanup(ua2,prefixua2.substring(1));
		}
		
		
	}
	
	
	@Test(enabled=true)
	public void moreThanOneClusterWithSameNameDiffValidity() throws Exception{
		Bundle b1 = (Bundle)Bundle.readBundle("src/test/resources/LocalDC_feedReplicaltion_BillingRC")[0][0];
		b1.generateUniqueBundle();
		Bundle b2 = (Bundle)Bundle.readBundle("src/test/resources/LocalDC_feedReplicaltion_BillingRC")[0][0];
		b2.generateUniqueBundle();
		Bundle b3 = (Bundle)Bundle.readBundle("src/test/resources/LocalDC_feedReplicaltion_BillingRC")[0][0];
		b3.generateUniqueBundle();
		try{
			b1 = new Bundle(b1,ua1.getEnvFileName());
			b2  = new Bundle(b2,ua2.getEnvFileName());
			b3  = new Bundle(b3,ua3.getEnvFileName());
			
			Bundle.submitCluster(b1,b2,b3);

			String startTimeUA1 = "2012-10-01T12:05Z" ;
			String startTimeUA2 = "2012-10-01T12:10Z";
			

			String feed = b1.getDataSets().get(0);
			feed =  instanceUtil.setFeedCluster(feed,xmlUtil.createValidity("2012-10-01T12:00Z","2010-01-01T00:00Z"),xmlUtil.createRtention("days(10000)",ActionType.DELETE),null,ClusterType.SOURCE,null,null);
		
			feed = instanceUtil.setFeedCluster(feed,xmlUtil.createValidity(startTimeUA1,"2012-10-01T12:10Z"),xmlUtil.createRtention("days(10000)",ActionType.DELETE),Util.readClusterName(b1.getClusters().get(0)),ClusterType.SOURCE,"","/localDC/rc/billing/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			feed = instanceUtil.setFeedCluster(feed,xmlUtil.createValidity(startTimeUA2,"2012-10-01T12:25Z"),xmlUtil.createRtention("days(10000)",ActionType.DELETE),Util.readClusterName(b3.getClusters().get(0)),ClusterType.TARGET,"","/clusterPath/localDC/rc/billing/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			feed = instanceUtil.setFeedCluster(feed,xmlUtil.createValidity("2012-10-01T12:00Z","2099-01-01T00:00Z"),xmlUtil.createRtention("days(10000)",ActionType.DELETE),Util.readClusterName(b3.getClusters().get(0)),ClusterType.SOURCE,"",null);

			Util.print("feed: "+feed);

			ServiceResponse r= prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
			Thread.sleep(10000);
			AssertUtil.assertFailed(r, "is defined more than once for feed");
			Assert.assertTrue(r.getMessage().contains("is defined more than once for feed"));
		
		}

		finally{
			
			prismHelper.getFeedHelper().delete(URLS.DELETE_URL, b1.getDataSets().get(0));
			prismHelper.getClusterHelper().delete(URLS.DELETE_URL, b2.getClusters().get(0));
			prismHelper.getClusterHelper().delete(URLS.DELETE_URL, b1.getClusters().get(0));
			prismHelper.getClusterHelper().delete(URLS.DELETE_URL, b3.getClusters().get(0));
		}
	}
	
	
}
