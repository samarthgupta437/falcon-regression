package com.inmobi.qa.falcon;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.oozie.client.CoordinatorAction;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.inmobi.qa.falcon.bundle.Bundle;
import com.inmobi.qa.falcon.generated.dependencies.Frequency.TimeUnit;
import com.inmobi.qa.falcon.helpers.ColoHelper;
import com.inmobi.qa.falcon.helpers.PrismHelper;
import com.inmobi.qa.falcon.supportClasses.Consumer;
import com.inmobi.qa.falcon.util.Util;
import com.inmobi.qa.falcon.util.instanceUtil;

/**
 * 
 * @author samarth.gupta
 *
 */

public class NoOutputPorcessTest {

	PrismHelper prismHelper=new PrismHelper("prism.properties");
	ColoHelper ivoryqa1 = new ColoHelper("ivoryqa-1.config.properties");

	@BeforeClass(alwaysRun=true)
	public void createTestData() throws Exception
	{

		Util.print("in @BeforeClass");

		System.setProperty("java.security.krb5.realm", "");
		System.setProperty("java.security.krb5.kdc", "");


		Bundle b = new Bundle();
		b = (Bundle)Util.readELBundles()[0][0];
		b.generateUniqueBundle();
		b = new Bundle(b,ivoryqa1.getEnvFileName());

		String startDate = "2010-01-03T00:00Z";
		String endDate = "2010-01-03T03:00Z";

		b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
		String prefix = b.getFeedDataPathPrefix();
		Util.HDFSCleanup(ivoryqa1,prefix.substring(1));

		DateTime startDateJoda = new DateTime(instanceUtil.oozieDateToDate(startDate));
		DateTime endDateJoda = new DateTime(instanceUtil.oozieDateToDate(endDate));

		List<String> dataDates = Util.getMinuteDatesOnEitherSide(startDateJoda,endDateJoda,20);

		for(int i = 0 ; i < dataDates.size(); i++)
			dataDates.set(i, prefix + dataDates.get(i));

		ArrayList<String> dataFolder = new ArrayList<String>();

		for(int i = 0 ; i < dataDates.size(); i++)
			dataFolder.add(dataDates.get(i));

		instanceUtil.putDataInFolders(ivoryqa1,dataFolder);
	}


	@BeforeMethod(alwaysRun=true)
	public void testName(Method method)
	{
		Util.print("test name: "+method.getName());
	}


	@Test(enabled=true,groups = {"singleCluster"})
	public void checkForJMSMsgWhenNoOutput() throws Exception{

		Bundle b = new Bundle();
		try{
			b = (Bundle)Util.readNoOutputBundles()[0][0];
			b = new Bundle(b,ivoryqa1.getEnvFileName());
			b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessValidity("2010-01-03T02:30Z","2010-01-03T02:45Z");
			b.setProcessPeriodicity(5,TimeUnit.minutes);
			b.submitAndScheduleBundle(prismHelper);


			Util.print("attaching consumer to:   "+"FALCON.ENTITY.TOPIC");
			Consumer consumer=new Consumer("FALCON.ENTITY.TOPIC",ivoryqa1.getClusterHelper().getActiveMQ());
			consumer.start();
			Thread.sleep(15000);

			//wait for all the instances to complete
			instanceUtil.waitTillInstanceReachState(ivoryqa1,b.getProcessName(),3,CoordinatorAction.Status.SUCCEEDED,20); 

			Assert.assertEquals(consumer.getMessageData().size(), 3, " Message for all the 3 instance not found");

			consumer.stop();

			Util.dumpConsumerData(consumer);      
		}
		finally{
			b.deleteBundle(prismHelper);

		}
	}


	@Test(enabled=true,groups = {"singleCluster"})
	public void rm () throws Exception{

		Bundle b = new Bundle();
		try{
			b = (Bundle)Util.readELBundles()[0][0];
			b = new Bundle(b,ivoryqa1.getEnvFileName());
			b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessValidity("2010-01-03T02:30Z","2010-01-03T02:45Z");
			b.setProcessPeriodicity(5,TimeUnit.minutes);
			b.submitAndScheduleBundle(prismHelper);

			Consumer consumerInternalMsg=new Consumer("FALCON.ENTITY.TOPIC",ivoryqa1.getClusterHelper().getActiveMQ());
			Consumer consumerProcess=new Consumer("FALCON."+b.getProcessName(),ivoryqa1.getClusterHelper().getActiveMQ());


			consumerInternalMsg.start();
			consumerProcess.start();

			Thread.sleep(15000);

			//wait for all the instances to complete
			instanceUtil.waitTillInstanceReachState(ivoryqa1,b.getProcessName(),3,CoordinatorAction.Status.SUCCEEDED,20); 

			Assert.assertEquals(consumerInternalMsg.getMessageData().size(), 3, " Message for all the 3 instance not found");
			Assert.assertEquals(consumerProcess.getMessageData().size(), 3, " Message for all the 3 instance not found");

			consumerInternalMsg.stop();
			consumerProcess.stop();

			Util.dumpConsumerData(consumerInternalMsg);  
			Util.dumpConsumerData(consumerProcess);  


		}
		finally{
			b.deleteBundle(prismHelper);

		}
	}

}
