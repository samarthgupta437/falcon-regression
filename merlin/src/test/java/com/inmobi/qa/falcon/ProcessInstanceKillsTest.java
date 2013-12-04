package com.inmobi.qa.falcon;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.oozie.client.CoordinatorAction;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.inmobi.qa.falcon.bundle.Bundle;
import com.inmobi.qa.falcon.generated.dependencies.Frequency.TimeUnit;
import com.inmobi.qa.falcon.helpers.ColoHelper;
import com.inmobi.qa.falcon.helpers.PrismHelper;
import com.inmobi.qa.falcon.response.ProcessInstancesResult;
import com.inmobi.qa.falcon.response.ProcessInstancesResult.WorkflowStatus;
import com.inmobi.qa.falcon.util.Util;
import com.inmobi.qa.falcon.util.instanceUtil;

/**
 * 
 * @author samarth.gupta
 *
 */

public class ProcessInstanceKillsTest {
	
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
	

	@Test(groups = {"singleCluster"})
	public void testProcessInstanceKill_single() throws Exception
	{       
                
		Bundle b = new Bundle();
		try{

			b = (Bundle)Util.readELBundles()[0][0];
			b = new Bundle(b,ivoryqa1.getEnvFileName());
			b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessValidity("2010-01-02T01:00Z","2010-01-02T01:04Z");
			b.setProcessPeriodicity(5,TimeUnit.minutes);
			b.setOutputFeedPeriodicity(5,TimeUnit.minutes);
			b.setOutputFeedLocationData("/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessConcurrency(1);
			b.submitAndScheduleBundle(prismHelper);
			Thread.sleep(15000);
			ProcessInstancesResult r  =  prismHelper.getProcessHelper().getProcessInstanceKill(Util.readEntityName(b.getProcessData()),"?start=2010-01-02T01:00Z");
			instanceUtil.validateSuccess(r, b, WorkflowStatus.KILLED);
		}
		finally{
			b.deleteBundle(prismHelper);
                        
		}
	}

	
	@Test(groups = {"singleCluster"})
	public void testProcessInstanceKill_startAndEndSame() throws Exception
	{       
		Bundle b = new Bundle();
		try{

			b = (Bundle)Util.readELBundles()[0][0];
			b = new Bundle(b,ivoryqa1.getEnvFileName());
			b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
						
			b.setProcessValidity("2010-01-02T00:00Z","2010-01-02T04:00Z");
			b.setProcessConcurrency(2);
			b.setProcessTimeOut(3,TimeUnit.minutes);
			b.setProcessPeriodicity(1,TimeUnit.minutes);
			b.setOutputFeedPeriodicity(5,TimeUnit.minutes);
			b.setOutputFeedLocationData("/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessConcurrency(10);
			b.submitAndScheduleBundle(prismHelper);
			Thread.sleep(15000);
			
			ProcessInstancesResult r  =  prismHelper.getProcessHelper().getProcessInstanceKill(Util.readEntityName(b.getProcessData()),"?start=2010-01-02T00:03Z&end=2010-01-02T00:03Z");
			instanceUtil.validateResponse(r, 1, 0, 0, 0, 1);
			
		}
		finally{
			b.deleteBundle(prismHelper);
                       
		}
	}
	
	
	
	
	
	@Test(groups = {"singleCluster"})
	public void testProcessInstanceKill_killNonMatrelized() throws Exception
	{       
		Bundle b = new Bundle();
		try{

			b = (Bundle)Util.readELBundles()[0][0];
			b = new Bundle(b,ivoryqa1.getEnvFileName());
			b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
						
			b.setProcessValidity("2010-01-02T00:00Z","2010-01-02T04:00Z");
			b.setProcessConcurrency(2);
			b.setProcessTimeOut(3,TimeUnit.minutes);
			b.setProcessPeriodicity(1,TimeUnit.minutes);
			b.setOutputFeedPeriodicity(5,TimeUnit.minutes);
			b.setOutputFeedLocationData("/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessConcurrency(10);
			b.submitAndScheduleBundle(prismHelper);
			Thread.sleep(15000);
			
			ProcessInstancesResult r  =  prismHelper.getProcessHelper().getProcessInstanceKill(Util.readEntityName(b.getProcessData()),"?start=2010-01-02T00:03Z&end=2010-01-02T00:30Z");

			instanceUtil.validateResponse(r, 3, 0, 0, 0, 3);
			Thread.sleep(15000);
			Util.print(r.toString());
			
		}
		finally{
			b.deleteBundle(prismHelper);
                       
		}
	}
	
	
	
	
	
	@Test(groups = {"singleCluster"})
	public void testProcessInstanceKill_bothStartAndEndInFuture01() throws Exception
	{       
		Bundle b = new Bundle();
		try{

			b = (Bundle)Util.readELBundles()[0][0];
			b = new Bundle(b,ivoryqa1.getEnvFileName());
			b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			
			String startTime = instanceUtil.getTimeWrtSystemTime(-20);
			String endTime = instanceUtil.getTimeWrtSystemTime(400);
			
			String startTimeData = instanceUtil.getTimeWrtSystemTime(-150);
			String endTimeData = instanceUtil.getTimeWrtSystemTime(50);
			
			instanceUtil.createDataWithinDatesAndPrefix(ivoryqa1,instanceUtil.oozieDateToDate(startTimeData), instanceUtil.oozieDateToDate(endTimeData), "/samarthData/", 1);
			
			b.setProcessValidity(startTime,endTime);
			b.setProcessPeriodicity(5,TimeUnit.minutes);
			b.setOutputFeedPeriodicity(5,TimeUnit.minutes);
			b.setOutputFeedLocationData("/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessConcurrency(6);
			b.submitAndScheduleBundle(prismHelper);
			Thread.sleep(15000);
			String startTimeRequest = instanceUtil.getTimeWrtSystemTime(-17);
			String endTimeRequest = instanceUtil.getTimeWrtSystemTime(23);
			ProcessInstancesResult r  =  prismHelper.getProcessHelper().getProcessInstanceKill(Util.readEntityName(b.getProcessData()),"?start="+startTimeRequest+"&end="+endTimeRequest);

			Thread.sleep(15000);
			Util.print(r.toString());
			
		}
		finally{
			b.deleteBundle(prismHelper);
                       
		}
	}
	
	
	

	@Test(groups = {"singleCluster"})
	public void testProcessInstanceKill_bothStartAndEndInFuture() throws Exception
	{       
		Bundle b = new Bundle();
		try{

			b = (Bundle)Util.readELBundles()[0][0];
			b = new Bundle(b,ivoryqa1.getEnvFileName());
			b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessValidity("2010-01-02T01:00Z","2099-01-02T01:21Z");
			b.setProcessPeriodicity(5,TimeUnit.minutes);
			b.setOutputFeedPeriodicity(5,TimeUnit.minutes);
			b.setOutputFeedLocationData("/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessConcurrency(6);
			b.submitAndScheduleBundle(prismHelper);
			Thread.sleep(15000);
			String startTime = instanceUtil.getTimeWrtSystemTime(10);
			String endTime = instanceUtil.getTimeWrtSystemTime(400);
			ProcessInstancesResult r  =  prismHelper.getProcessHelper().getProcessInstanceKill(Util.readEntityName(b.getProcessData()),"?start="+startTime+"&end="+endTime);
			//instanceUtil.validateSuccess(r, b, WorkflowStatus.KILLED);
			Thread.sleep(15000);
			Util.print(r.getMessage());
			Assert.assertEquals(r.getInstances(), null);
			
		}
		finally{
			b.deleteBundle(prismHelper);
                       
		}
	}
	
	
	@Test(groups = {"singleCluster"})
	public void testProcessInstanceKill_multipleInstance() throws Exception
	{       
		Bundle b = new Bundle();
		try{

			b = (Bundle)Util.readELBundles()[0][0];
			b = new Bundle(b,ivoryqa1.getEnvFileName());
			b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessValidity("2010-01-02T01:00Z","2010-01-02T01:21Z");
			b.setProcessPeriodicity(5,TimeUnit.minutes);
			b.setOutputFeedPeriodicity(5,TimeUnit.minutes);
			b.setOutputFeedLocationData("/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessConcurrency(6);
			b.submitAndScheduleBundle(prismHelper);
			Thread.sleep(15000);
			ProcessInstancesResult r  =  prismHelper.getProcessHelper().getProcessInstanceKill(Util.readEntityName(b.getProcessData()),"?start=2010-01-02T01:05Z&end=2010-01-02T01:15Z");
			//instanceUtil.validateSuccess(r, b, WorkflowStatus.KILLED);
			Thread.sleep(15000);
			r =  prismHelper.getProcessHelper().getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),"?start=2010-01-02T01:00Z&end=2010-01-02T01:20Z");
			instanceUtil.validateResponse(r, 5, 2, 0, 0,3);
		}
		finally{
			b.deleteBundle(prismHelper);
                       
		}
	}
	
	
	@Test(groups = {"singleCluster"})
	public void testProcessInstanceKill_lastInstance() throws Exception
	{       
                
		Bundle b = new Bundle();
		try{

			b = (Bundle)Util.readELBundles()[0][0];
			b = new Bundle(b,ivoryqa1.getEnvFileName());
			b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessValidity("2010-01-02T01:00Z","2010-01-02T01:21Z");
			b.setProcessPeriodicity(5,TimeUnit.minutes);
			b.setOutputFeedPeriodicity(5,TimeUnit.minutes);
			b.setOutputFeedLocationData("/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessConcurrency(6);
			b.submitAndScheduleBundle(prismHelper);
			Thread.sleep(15000);
			ProcessInstancesResult r  =   prismHelper.getProcessHelper().getProcessInstanceKill(Util.readEntityName(b.getProcessData()),"?start=2010-01-02T01:20Z");
			//instanceUtil.validateSuccess(r, b, WorkflowStatus.KILLED);
			Thread.sleep(15000);
			r =  prismHelper.getProcessHelper().getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),"?start=2010-01-02T01:00Z&end=2010-01-02T01:20Z");
			instanceUtil.validateResponse(r, 5, 4, 0, 0,1);
		} 
		finally{
			b.deleteBundle(prismHelper);
                        
		}
	}

	
	
	
	@Test(groups = {"singleCluster"})
	public void testProcessInstanceKill_suspended() throws Exception
	{
                
		Bundle b = new Bundle();
		try{

			b = (Bundle)Util.readELBundles()[0][0];
			b = new Bundle(b,ivoryqa1.getEnvFileName());
			b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessValidity("2010-01-02T01:00Z","2010-01-02T01:04Z");
			b.setProcessPeriodicity(5,TimeUnit.minutes);
			b.setOutputFeedPeriodicity(5,TimeUnit.minutes);
			b.setOutputFeedLocationData("/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessConcurrency(1);
			b.submitAndScheduleBundle(prismHelper);
			Thread.sleep(15000);
			 prismHelper.getProcessHelper().getProcessInstanceSuspend(Util.readEntityName(b.getProcessData()),"?start=2010-01-02T01:00Z");
			Thread.sleep(15000);
			ProcessInstancesResult r  =  prismHelper.getProcessHelper().getProcessInstanceKill(Util.readEntityName(b.getProcessData()),"?start=2010-01-02T01:00Z");
			instanceUtil.validateSuccess(r, b, WorkflowStatus.KILLED);
		}
		finally{
			b.deleteBundle(prismHelper);
                        
		}
	}
	
	
	@Test(groups = {"singleCluster"})
	public void testProcessInstanceKill_succeeded() throws Exception
	{
                
		Bundle b = new Bundle();
		try{

			b = (Bundle)Util.readELBundles()[0][0];
			b = new Bundle(b,ivoryqa1.getEnvFileName());
			b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessValidity("2010-01-02T01:00Z","2010-01-02T01:04Z");
			b.setProcessPeriodicity(5,TimeUnit.minutes);
			b.setOutputFeedPeriodicity(5,TimeUnit.minutes);
			b.setOutputFeedLocationData("/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessConcurrency(1);
			b.submitAndScheduleBundle(prismHelper);
			for(int i = 0 ; i < 30 ; i ++)
			{
				if(instanceUtil.getInstanceStatus(ivoryqa1,Util.readEntityName(b.getProcessData()), 0, 0).equals(CoordinatorAction.Status.SUCCEEDED))
					break;
				Thread.sleep(30000);
			}
			ProcessInstancesResult r  =  prismHelper.getProcessHelper().getProcessInstanceKill(Util.readEntityName(b.getProcessData()),"?start=2010-01-02T01:00Z");
			instanceUtil.validateSuccess(r, b, WorkflowStatus.SUCCEEDED);
		}
		finally{
			b.deleteBundle(prismHelper);
                        
		}
	}
	

	
    @AfterClass(alwaysRun=true)
	public void deleteData() throws Exception
	{
		Util.print("in @AfterClass");
		
		System.setProperty("java.security.krb5.realm", "");
		System.setProperty("java.security.krb5.kdc", "");
		
		
		Bundle b = new Bundle();
		b = (Bundle)Util.readELBundles()[0][0];
		b = new Bundle(b,ivoryqa1.getEnvFileName());

		b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
		String prefix = b.getFeedDataPathPrefix();
		Util.HDFSCleanup(ivoryqa1,prefix.substring(1));
	}

}
