package com.inmobi.qa.falcon.prism;

import org.testng.annotations.Test;

import com.inmobi.qa.falcon.bundle.Bundle;
import com.inmobi.qa.falcon.generated.feed.ActionType;
import com.inmobi.qa.falcon.generated.feed.ClusterType;
import com.inmobi.qa.falcon.helpers.ColoHelper;
import com.inmobi.qa.falcon.helpers.PrismHelper;
import com.inmobi.qa.falcon.util.Util;
import com.inmobi.qa.falcon.util.instanceUtil;
import com.inmobi.qa.falcon.util.xmlUtil;
import com.inmobi.qa.falcon.util.Util.URLS;

/**
 * 
 * @author samarth.gupta
 *
 */
public class FeedReplicationS4 {

	PrismHelper prismHelper=new PrismHelper("prism.properties");

	ColoHelper ua1=new ColoHelper("gs1001.config.properties");
    ColoHelper ua2 = new ColoHelper("ivoryqa-1.config.properties");

	@Test(enabled=true,timeOut=1200000)
	public void ReplicationFromS4() throws Exception
	{

		Bundle b1 = (Bundle)Bundle.readBundle("src/test/resources/S4Replication")[0][0];
		b1.generateUniqueBundle();
		
		Bundle b2 = (Bundle)Bundle.readBundle("src/test/resources/S4Replication")[0][0];
		b2.generateUniqueBundle();
		

		try{
			b1 = new Bundle(b1,ua1.getEnvFileName());
			b2 = new Bundle(b2,ua2.getEnvFileName());
			
			//Bundle.submitCluster(b1,b2);
			
			String feedOutput01 = b1.getDataSets().get(0);
			feedOutput01 =  instanceUtil.setFeedCluster(feedOutput01,xmlUtil.createValidity("2010-10-01T12:00Z","2099-01-01T00:00Z"),xmlUtil.createRtention("days(10000)",ActionType.DELETE),null,ClusterType.SOURCE,null,null);
			feedOutput01 = instanceUtil.setFeedCluster(feedOutput01,xmlUtil.createValidity("2012-12-06T05:00Z","2099-10-01T12:10Z"),xmlUtil.createRtention("minutes(5)",ActionType.DELETE),Util.readClusterName(b2.getClusters().get(0)),ClusterType.SOURCE,"","s4://inmobi-iat-data/userplatform/${YEAR}/${MONTH}/${DAY}/${HOUR}");
			feedOutput01 = instanceUtil.setFeedCluster(feedOutput01,xmlUtil.createValidity("2012-12-06T05:00Z","2099-10-01T12:25Z"),xmlUtil.createRtention("minutes(5)",ActionType.DELETE),Util.readClusterName(b1.getClusters().get(0)),ClusterType.TARGET,"","/replicationS4/${YEAR}/${MONTH}/${DAY}/${HOUR}");

			System.out.println(feedOutput01);
			Bundle.submitCluster(b1,b2);
			prismHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feedOutput01);
			
		}
		finally{
			b1.deleteBundle(prismHelper);
		}
	}
	
}
