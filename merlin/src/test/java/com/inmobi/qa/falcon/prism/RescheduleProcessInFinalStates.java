package com.inmobi.qa.falcon.prism;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.oozie.client.Job.Status;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.inmobi.qa.falcon.bundle.Bundle;
import com.inmobi.qa.falcon.generated.dependencies.Frequency.TimeUnit;
import com.inmobi.qa.falcon.helpers.ColoHelper;
import com.inmobi.qa.falcon.helpers.PrismHelper;
import com.inmobi.qa.falcon.response.ServiceResponse;
import com.inmobi.qa.falcon.util.Util;
import com.inmobi.qa.falcon.util.Util.URLS;
import com.inmobi.qa.falcon.util.instanceUtil;
/**
 * 
 * @author samarth.gupta
 *
 */


public class RescheduleProcessInFinalStates {

	
	
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

		String startDate = "2010-01-01T20:00Z";
		String endDate = "2010-01-03T01:04Z";

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


	// DWE mean Done With Error In Oozie
	@Test(enabled=false)
	public void rescheduleSucceeded() throws Exception
	{

		Bundle b = new Bundle();
		try{

			b = (Bundle)Util.readELBundles()[0][0];
			b = new Bundle(b,ivoryqa1.getEnvFileName());
			b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessValidity("2010-01-02T01:00Z","2010-01-02T01:15Z");
			b.setProcessPeriodicity(5,TimeUnit.minutes);
			b.setOutputFeedPeriodicity(5,TimeUnit.minutes);
			b.setOutputFeedLocationData("/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessConcurrency(6);
			b.submitAndScheduleBundle(prismHelper);
			
			instanceUtil.waitForBundleToReachState(ivoryqa1,b.getProcessName(),Status.SUCCEEDED ,20);
			
			Thread.sleep(20000);
			
			//delete the process
			prismHelper.getProcessHelper().delete(URLS.DELETE_URL, b.getProcessData());
			
			//check ... get definition should return process not found
			ServiceResponse r = prismHelper.getProcessHelper().getEntityDefinition(URLS.GET_ENTITY_DEFINITION, b.getProcessData());
			Assert.assertTrue(r.getMessage().contains("(process) not found"));
			Util.assertFailed(r);
			
			//submit and schedule process again 
			r = prismHelper.getProcessHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, b.getProcessData());
		    Util.assertSucceeded(r);
		    Thread.sleep(20000);
			instanceUtil.waitForBundleToReachState(ivoryqa1,b.getProcessName(),Status.SUCCEEDED ,20);

		}
		finally{
			b.deleteBundle(prismHelper);
		}

	}

	@Test(enabled=false)
	public void rescheduleFailed() throws Exception
	{
		Bundle b = new Bundle();
		try{

			b = (Bundle)Util.readELBundles()[0][0];
			b = new Bundle(b,ivoryqa1.getEnvFileName());
			b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessValidity("2010-01-02T01:00Z","2010-01-02T01:15Z");
			b.setProcessPeriodicity(5,TimeUnit.minutes);
			b.setOutputFeedPeriodicity(5,TimeUnit.minutes);
			b.setOutputFeedLocationData("/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessConcurrency(6);
			b.submitAndScheduleBundle(prismHelper);
			
			instanceUtil.waitForBundleToReachState(ivoryqa1,b.getProcessName(),Status.SUCCEEDED ,20);
			
			Thread.sleep(20000);
			
			//delete the process
			prismHelper.getProcessHelper().delete(URLS.DELETE_URL, b.getProcessData());
			
			//check ... get definition should return process not found
			ServiceResponse r = prismHelper.getProcessHelper().getEntityDefinition(URLS.GET_ENTITY_DEFINITION, b.getProcessData());
			Assert.assertTrue(r.getMessage().contains("(process) not found"));
			Util.assertFailed(r);
			
			//submit and schedule process again 
			r = prismHelper.getProcessHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, b.getProcessData());
		    Util.assertSucceeded(r);
		    Thread.sleep(20000);
			instanceUtil.waitForBundleToReachState(ivoryqa1,b.getProcessName(),Status.SUCCEEDED ,20);

		}
		finally{
			b.deleteBundle(prismHelper);
		}

	}

	@Test(enabled=false)
	public void rescheduleDWE() throws Exception
	{
		Bundle b = new Bundle();
		try{

			b = (Bundle)Util.readELBundles()[0][0];
			b = new Bundle(b,ivoryqa1.getEnvFileName());
			b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessValidity("2010-01-02T01:00Z","2010-01-02T01:15Z");
			b.setProcessPeriodicity(5,TimeUnit.minutes);
			b.setOutputFeedPeriodicity(5,TimeUnit.minutes);
			b.setOutputFeedLocationData("/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessConcurrency(6);
			b.submitAndScheduleBundle(prismHelper);
			Thread.sleep(20000);
			
			 prismHelper.getProcessHelper().getProcessInstanceKill(Util.readEntityName(b.getProcessData()),"?start=2010-01-02T01:05Z");

			
			instanceUtil.waitForBundleToReachState(ivoryqa1,b.getProcessName(), Status.DONEWITHERROR ,20);
			
			Thread.sleep(20000);
			
			//delete the process
			prismHelper.getProcessHelper().delete(URLS.DELETE_URL, b.getProcessData());
			
			//check ... get definition should return process not found
			ServiceResponse r = prismHelper.getProcessHelper().getEntityDefinition(URLS.GET_ENTITY_DEFINITION, b.getProcessData());
			Assert.assertTrue(r.getMessage().contains("(process) not found"));
			Util.assertFailed(r);
			
			//submit and schedule process again 
			r = prismHelper.getProcessHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, b.getProcessData());
		    Util.assertSucceeded(r);
		    Thread.sleep(20000);
			instanceUtil.waitForBundleToReachState(ivoryqa1,b.getProcessName(),Status.SUCCEEDED ,20);

		}
		finally{
			b.deleteBundle(prismHelper);
		}

	}

	@Test(enabled=false)
	public void rescheduleSucceededAndDWEOnSomeColo() throws Exception
	{

	}


	@Test(enabled=false)
	public void rescheduleOnlyOnOneAndSucceeded() throws Exception
	{

	}
	
	@Test(enabled=false)
	public void rescheduleOnlyOnOneAndKilled() throws Exception
	{

	}


	@Test(enabled=false)
	public void rescheduleKilled() throws Exception
	{

		Bundle b = new Bundle();
		try{

			b = (Bundle)Util.readELBundles()[0][0];
			b = new Bundle(b,ivoryqa1.getEnvFileName());
			b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessValidity("2010-01-02T01:00Z","2010-01-02T01:15Z");
			b.setProcessPeriodicity(5,TimeUnit.minutes);
			b.setOutputFeedPeriodicity(5,TimeUnit.minutes);
			b.setOutputFeedLocationData("/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessConcurrency(6);
			b.submitAndScheduleBundle(prismHelper);
			Thread.sleep(15000);
			
			 prismHelper.getProcessHelper().delete(URLS.DELETE_URL, b.getProcessData());

			
			instanceUtil.waitForBundleToReachState(ivoryqa1,b.getProcessName(), Status.KILLED ,20);
			
			Thread.sleep(20000);
		
			//check ... get definition should return process not found
			ServiceResponse r = prismHelper.getProcessHelper().getEntityDefinition(URLS.GET_ENTITY_DEFINITION, b.getProcessData());
			Assert.assertTrue(r.getMessage().contains("(process) not found"));
			Util.assertFailed(r);
			
			//submit and schedule process again 
			r = prismHelper.getProcessHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, b.getProcessData());
		    Util.assertSucceeded(r);
		    Thread.sleep(20000);
			instanceUtil.waitForBundleToReachState(ivoryqa1,b.getProcessName(),Status.SUCCEEDED ,20);

		}
		finally{
			b.deleteBundle(prismHelper);
		}
	}
	
	
}
