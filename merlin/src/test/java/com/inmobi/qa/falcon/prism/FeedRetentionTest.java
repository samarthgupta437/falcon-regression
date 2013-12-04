package com.inmobi.qa.falcon.prism;

import java.lang.reflect.Method;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


import com.inmobi.qa.falcon.bundle.Bundle;
import com.inmobi.qa.falcon.generated.feed.ActionType;
import com.inmobi.qa.falcon.generated.feed.ClusterType;
import com.inmobi.qa.falcon.helpers.ColoHelper;
import com.inmobi.qa.falcon.helpers.PrismHelper;
import com.inmobi.qa.falcon.response.ServiceResponse;
import com.inmobi.qa.falcon.util.AssertUtil;
import com.inmobi.qa.falcon.util.Util;
import com.inmobi.qa.falcon.util.Util.URLS;
import com.inmobi.qa.falcon.util.instanceUtil;
import com.inmobi.qa.falcon.util.xmlUtil;
/**
 * 
 * @author samarth.gupta
 *
 */
public class FeedRetentionTest {
	PrismHelper prismHelper=new PrismHelper("prism.properties");
	ColoHelper gs1001 = new ColoHelper("gs1001.config.properties");
    ColoHelper ivoryqa1 = new ColoHelper("ivoryqa-1.config.properties");


	
	@BeforeMethod(alwaysRun=true)
	public void testName(Method method)
	{
		Util.print("test name: "+method.getName());
	}
	@Test(enabled=true)
	public void testRetentionClickRC_2Colo()throws Throwable{
		
		//submit 2 clusters 
		// submit and schedule feed on above 2 clusters, both having different locations
		// submit and schedule process having the above feed as output feed and running on 2 clusters
		
		
		//getImpressionRC bundle
		Bundle b1 = new Bundle();
		Bundle b2 = new Bundle();
		
		try{
			b1 = (Bundle)Bundle.readBundle("src/test/resources/impressionRC")[0][0];
			b1.generateUniqueBundle();
			b1 = new Bundle(b1,gs1001.getEnvFileName());
			
			b2 = (Bundle)Bundle.readBundle("src/test/resources/impressionRC")[0][0];
			b2.generateUniqueBundle();
			b2 = new Bundle(b2,ivoryqa1.getEnvFileName());
			
		

			
			instanceUtil.putFileInFolders(gs1001,instanceUtil.createEmptyDirWithinDatesAndPrefix(gs1001, instanceUtil.oozieDateToDate(instanceUtil.getTimeWrtSystemTime(-5)), instanceUtil.oozieDateToDate(instanceUtil.getTimeWrtSystemTime(10)), "/testInput/ivoryRetention01/data/", 1),"thriftRRMar0602.gz");
			instanceUtil.putFileInFolders(ivoryqa1,instanceUtil.createEmptyDirWithinDatesAndPrefix(ivoryqa1, instanceUtil.oozieDateToDate(instanceUtil.getTimeWrtSystemTime(-5)), instanceUtil.oozieDateToDate(instanceUtil.getTimeWrtSystemTime(10)), "/testInput/ivoryRetention01/data/", 1),"thriftRRMar0602.gz");

//			Bundle.submitCluster(b1,b2);
			prismHelper.getClusterHelper().submitEntity(URLS.SUBMIT_URL, b1.getClusters().get(0));
			prismHelper.getClusterHelper().submitEntity(URLS.SUBMIT_URL, b2.getClusters().get(0));
		
			String feedOutput01 = b1.getFeed("FETL-RequestRC");
			feedOutput01 =  instanceUtil.setFeedCluster(feedOutput01,xmlUtil.createValidity("2010-10-01T12:00Z","2099-01-01T00:00Z"),xmlUtil.createRtention("days(10000)",ActionType.DELETE),null,ClusterType.SOURCE,null,null);
			feedOutput01 = instanceUtil.setFeedCluster(feedOutput01,xmlUtil.createValidity("2010-10-01T12:00Z","2099-10-01T12:10Z"),xmlUtil.createRtention("minutes(5)",ActionType.DELETE),Util.readClusterName(b1.getClusters().get(0)),ClusterType.SOURCE,"${cluster.colo}","/testOutput/op1/ivoryRetention01/data/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}","/testOutput/op1/ivoryRetention01/stats/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}","/testOutput/op1/ivoryRetention01/meta/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}","/testOutput/op1/ivoryRetention01/tmp/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			feedOutput01 = instanceUtil.setFeedCluster(feedOutput01,xmlUtil.createValidity("2010-10-01T12:00Z","2099-10-01T12:25Z"),xmlUtil.createRtention("minutes(5)",ActionType.DELETE),Util.readClusterName(b2.getClusters().get(0)),ClusterType.SOURCE,"${cluster.colo}","/testOutput/op1/ivoryRetention02/data/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}","/testOutput/op1/ivoryRetention02/stats/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}","/testOutput/op1/ivoryRetention02/meta/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}","/testOutput/op1/ivoryRetention02/tmp/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");

			//submit the new output feed
			ServiceResponse r= prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feedOutput01);
			Thread.sleep(10000);
			AssertUtil.assertSucceeded(r);


			String feedOutput02 = b1.getFeed("FETL-ImpressionRC");
			feedOutput02 =  instanceUtil.setFeedCluster(feedOutput02,xmlUtil.createValidity("2010-10-01T12:00Z","2099-01-01T00:00Z"),xmlUtil.createRtention("days(10000)",ActionType.DELETE),null,ClusterType.SOURCE,null,null);
			feedOutput02 = instanceUtil.setFeedCluster(feedOutput02,xmlUtil.createValidity("2010-10-01T12:00Z","2099-10-01T12:10Z"),xmlUtil.createRtention("minutes(5)",ActionType.DELETE),Util.readClusterName(b1.getClusters().get(0)),ClusterType.SOURCE,"${cluster.colo}","/testOutput/op2/ivoryRetention01/data/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}","/testOutput/op2/ivoryRetention01/stats/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}","/testOutput/op2/ivoryRetention01/meta/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}","/testOutput/op2/ivoryRetention01/tmp/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			feedOutput02 = instanceUtil.setFeedCluster(feedOutput02,xmlUtil.createValidity("2010-10-01T12:00Z","2099-10-01T12:25Z"),xmlUtil.createRtention("minutes(5)",ActionType.DELETE),Util.readClusterName(b2.getClusters().get(0)),ClusterType.SOURCE,"${cluster.colo}","/testOutput/op2/ivoryRetention02/data/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}","/testOutput/op2/ivoryRetention02/stats/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}","/testOutput/op2/ivoryRetention02/meta/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}","/testOutput/op2/ivoryRetention02/tmp/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");

			//submit the new output feed
			r= prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feedOutput02);
			Thread.sleep(10000);
			AssertUtil.assertSucceeded(r);
			
			
			
			
			String feedInput = b1.getFeed("FETL2-RRLog");
			feedInput =  instanceUtil.setFeedCluster(feedInput,xmlUtil.createValidity("2010-10-01T12:00Z","2099-01-01T00:00Z"),xmlUtil.createRtention("days(10000)",ActionType.DELETE),null,ClusterType.SOURCE,null,null);
		
			feedInput = instanceUtil.setFeedCluster(feedInput,xmlUtil.createValidity("2010-10-01T12:00Z","2099-10-01T12:10Z"),xmlUtil.createRtention("minutes(5)",ActionType.DELETE),Util.readClusterName(b1.getClusters().get(0)),ClusterType.SOURCE,"${cluster.colo}","/testInput/ivoryRetention01/data/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			feedInput = instanceUtil.setFeedCluster(feedInput,xmlUtil.createValidity("2010-10-01T12:00Z","2099-10-01T12:25Z"),xmlUtil.createRtention("minutes(5)",ActionType.DELETE),Util.readClusterName(b2.getClusters().get(0)),ClusterType.SOURCE,"${cluster.colo}","/testInput/ivoryRetention01/data/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");

			r= prismHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feedInput);
			
			Thread.sleep(10000);
			AssertUtil.assertSucceeded(r);
			

			String process = b1.getProcessData();
			process = instanceUtil.setProcessCluster(process, null, xmlUtil.createProcessValidity("2012-10-01T12:00Z","2012-10-01T12:10Z"));
			
			process = instanceUtil.setProcessCluster(process, Util.readClusterName(b1.getClusters().get(0)), xmlUtil.createProcessValidity(instanceUtil.getTimeWrtSystemTime(-2),instanceUtil.getTimeWrtSystemTime(5)));
			process = instanceUtil.setProcessCluster(process, Util.readClusterName(b2.getClusters().get(0)), xmlUtil.createProcessValidity(instanceUtil.getTimeWrtSystemTime(-2),instanceUtil.getTimeWrtSystemTime(5)));
			
			Util.print("process: "+process);
			
			
			r= prismHelper.getProcessHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, process);
		//	Thread.sleep(10000);
			AssertUtil.assertSucceeded(r);	
			
			
			r= prismHelper.getFeedHelper().schedule(URLS.SCHEDULE_URL, feedOutput01);
			r= prismHelper.getFeedHelper().schedule(URLS.SCHEDULE_URL, feedOutput02);

		}
		finally{
			prismHelper.getProcessHelper().delete(URLS.DELETE_URL,b1.getProcessData());
			prismHelper.getFeedHelper().delete(URLS.DELETE_URL, b1.getDataSets().get(0));
			prismHelper.getFeedHelper().delete(URLS.DELETE_URL, b1.getDataSets().get(1));
			Bundle.deleteCluster(b1,b2);
		} 
		
		
	}

}
