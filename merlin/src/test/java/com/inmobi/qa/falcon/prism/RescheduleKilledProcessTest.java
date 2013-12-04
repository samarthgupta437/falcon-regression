package com.inmobi.qa.falcon.prism;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.testng.annotations.Test;

import com.inmobi.qa.falcon.bundle.Bundle;
import com.inmobi.qa.falcon.helpers.ColoHelper;
import com.inmobi.qa.falcon.helpers.PrismHelper;
import com.inmobi.qa.falcon.util.Util;
import com.inmobi.qa.falcon.util.Util.URLS;
import com.inmobi.qa.falcon.util.instanceUtil;
import com.inmobi.qa.falcon.util.xmlUtil;
/**
 * 
 * @author samarth.gupta
 *
 */
public class RescheduleKilledProcessTest {

	PrismHelper prismHelper=new PrismHelper("prism.properties");

	ColoHelper ua1=new ColoHelper("gs1001.config.properties");

	@Test(enabled=false,timeOut=1200000)
	public void recheduleKilledProcess() throws Exception
	{
		// submit and schedule a process with error in workflow .
		//it will get killed 
		Bundle b1 = (Bundle)Util.readELBundles()[0][0];
		b1.generateUniqueBundle();
		try
		{
			//generate bundles according to config files
			b1  = new Bundle(b1,ua1.getEnvFileName());
			String 	processStartTime = instanceUtil.getTimeWrtSystemTime(-11);
			String 	processEndTime = instanceUtil.getTimeWrtSystemTime(06);


			String process = b1.getProcessData();
			process = instanceUtil.setProcessName(process, "zeroInputProcess"+new Random().nextInt());
			List<String> feed = new ArrayList<String>();
			feed.add(Util.getOutputFeedFromBundle(b1));
			process = b1.setProcessFeeds(process,feed,0,0,1);

			process = instanceUtil.setProcessCluster(process,null,xmlUtil.createProcessValidity(processStartTime,"2099-01-01T00:00Z"));
			process = instanceUtil.setProcessCluster(process,Util.readClusterName(b1.getClusters().get(0)),xmlUtil.createProcessValidity(processStartTime,processEndTime));
			b1.setProcessData(process);

			b1.submitAndScheduleBundle(prismHelper);

			prismHelper.getProcessHelper().delete(URLS.DELETE_URL, b1.getProcessData());

	//		prismHelper.getProcessHelper().submitEntity(URLS.SUBMIT_URL, b1.getProcessData());
			
	//		prismHelper.getProcessHelper().delete(URLS.DELETE_URL, b1.getProcessData());
			
			prismHelper.getProcessHelper().submitEntity(URLS.SUBMIT_URL, b1.getProcessData());

			prismHelper.getProcessHelper().schedule(URLS.SCHEDULE_URL, b1.getProcessData());



		}
		finally{
			b1.deleteBundle(prismHelper);
		}


	}


	@Test(enabled=true,timeOut=1200000)
	public void recheduleKilledProcess02() throws Exception
	{
		// submit and schedule a process with error in workflow .
		//it will get killed 
		Bundle b1 = (Bundle)Util.readELBundles()[0][0];
		b1.generateUniqueBundle();
		try
		{
			//generate bundles according to config files
			b1  = new Bundle(b1,ua1.getEnvFileName());
			String 	processStartTime = instanceUtil.getTimeWrtSystemTime(-11);
			String 	processEndTime = instanceUtil.getTimeWrtSystemTime(06);
			b1.setProcessValidity(processStartTime, processEndTime);
			b1.setInputFeedDataPath("/samarth/input-data/rawLogs/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");

			String prefix = instanceUtil.getFeedPrefix(Util.getInputFeedFromBundle(b1));
			Util.HDFSCleanup(ua1,prefix.substring(1));
			Util.lateDataReplenish(ua1,40,0,1,prefix);

			System.out.println("process: "+b1.getProcessData());
			
			b1.submitAndScheduleBundle(prismHelper);

			prismHelper.getProcessHelper().delete(URLS.DELETE_URL, b1.getProcessData());

			prismHelper.getProcessHelper().submitEntity(URLS.SUBMIT_URL, b1.getProcessData());
			prismHelper.getProcessHelper().schedule(URLS.SCHEDULE_URL, b1.getProcessData());
			
			prismHelper.getProcessHelper().delete(URLS.DELETE_URL, b1.getProcessData());

			prismHelper.getProcessHelper().submitEntity(URLS.SUBMIT_URL, b1.getProcessData());
			prismHelper.getProcessHelper().schedule(URLS.SCHEDULE_URL, b1.getProcessData());
			
		}
		finally{
			b1.deleteBundle(prismHelper);
		}
	}
}
