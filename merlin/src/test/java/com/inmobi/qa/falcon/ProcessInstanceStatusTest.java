package com.inmobi.qa.falcon;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

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
import com.inmobi.qa.falcon.util.Util.URLS;
import com.inmobi.qa.falcon.util.instanceUtil;

/**
 * 
 * @author samarth.gupta
 *
 */
public class ProcessInstanceStatusTest {

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
	public void testProcessInstanceStatus_StartAndEnd_checkNoInstanceAfterEndDate() throws Exception
	{
		Bundle b = new Bundle();

		try{

			//time out is set as 3 minutes .... getStatus is for a large range in past. 
			//6 instance should be materialized and one in running and other in waiting 
			
			b = (Bundle)Util.readELBundles()[0][0];
			b  = new Bundle(b,ivoryqa1.getEnvFileName());
			b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessValidity("2010-01-02T01:00Z","2010-01-03T10:22Z");
			b.setProcessTimeOut(3,TimeUnit.minutes);
			b.setProcessPeriodicity(1,TimeUnit.minutes);
			b.setProcessConcurrency(1);
			b.submitAndScheduleBundle(prismHelper);
			Thread.sleep(5000);
			ProcessInstancesResult r  =  prismHelper.getProcessHelper().getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),"?start=2010-01-02T01:00Z&end=2010-01-02T10:20Z");
			instanceUtil.validateSuccess(r, b,WorkflowStatus.RUNNING);	
			instanceUtil.validateResponse(r, 6, 1, 0, 5, 0);
		}
		finally{
			b.deleteBundle(prismHelper);		}
	}
	

	@Test(groups = {"singleCluster"})
	public void testProcessInstanceStatus_onlyStartAfterMat() throws Exception
	{
		Bundle b = new Bundle();

		try{

			//time out is set as 3 minutes .... getStatus is for a large range in past. 
			//6 instance should be materialized and one in running and other in waiting 
			
			b = (Bundle)Util.readELBundles()[0][0];
			b  = new Bundle(b,ivoryqa1.getEnvFileName());
			b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessValidity("2010-01-02T01:00Z","2010-01-03T10:22Z");
			b.setProcessTimeOut(3,TimeUnit.minutes);
			b.setProcessPeriodicity(1,TimeUnit.minutes);
			b.setProcessConcurrency(1);
			b.submitAndScheduleBundle(prismHelper);
			Thread.sleep(5000);
			ProcessInstancesResult r  =  prismHelper.getProcessHelper().getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),"?start=2010-01-02T05:00Z");
			Util.assertSucceeded(r);
			Assert.assertEquals(r.getInstances(), null);
		}
		finally{
			b.deleteBundle(prismHelper);		}
	}
	
	
	

	
	@Test(groups = {"singleCluster"})
	public void testProcessInstanceStatus_EndOutOfRange() throws Exception
	{
		Bundle b = new Bundle();

		try{

			b = (Bundle)Util.readELBundles()[0][0];
			b  = new Bundle(b,ivoryqa1.getEnvFileName());
			b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessValidity("2010-01-02T01:00Z","2010-01-02T01:22Z");
			b.setProcessPeriodicity(5,TimeUnit.minutes);
			b.setOutputFeedPeriodicity(5,TimeUnit.minutes);
			b.submitAndScheduleBundle(prismHelper);
			Thread.sleep(15000);			
			ProcessInstancesResult r  =  prismHelper.getProcessHelper().getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),"?start=2010-01-02T01:00Z&end=2010-01-02T01:30Z");
			instanceUtil.validateSuccessWithStatusCode(r, 400);
		}
		finally{
			b.deleteBundle(prismHelper);		}
	}
	
	
	@Test(groups = {"singleCluster"})
	public void testProcessInstanceStatus_dateEmpty() 
	{
		Bundle b = new Bundle();

		try{

			b = (Bundle)Util.readELBundles()[0][0];
			b  = new Bundle(b,ivoryqa1.getEnvFileName());
			b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessValidity("2010-01-02T01:00Z","2010-01-02T02:30Z");
			b.setProcessPeriodicity(5,TimeUnit.minutes);
			b.submitAndScheduleBundle(prismHelper);
			ProcessInstancesResult r  =  prismHelper.getProcessHelper().getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),"");
			instanceUtil.validateSuccessWithStatusCode(r,2);
		} catch (Exception e) {
		    if(!e.getMessage().contains("Expected BEGIN_OBJECT but was STRING at line 1 column"))
		        Assert.assertTrue(false);
        }
		finally{
			try {
                b.deleteBundle(prismHelper);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }		}
	}
	
	@Test(groups = {"singleCluster"})
	public void testProcessInstanceStatus_StartAndEnd() throws Exception
	{
		Bundle b = new Bundle();

		try{

			b = (Bundle)Util.readELBundles()[0][0];
			b  = new Bundle(b,ivoryqa1.getEnvFileName());
			b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessValidity("2010-01-02T01:00Z","2010-01-02T01:22Z");
			b.setProcessPeriodicity(5,TimeUnit.minutes);
			b.submitAndScheduleBundle(prismHelper);
			Thread.sleep(5000);
			ProcessInstancesResult r  =  prismHelper.getProcessHelper().getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),"?start=2010-01-02T01:00Z&end=2010-01-02T01:20Z");
			instanceUtil.validateSuccess(r, b,WorkflowStatus.RUNNING);	
		}
		finally{
			b.deleteBundle(prismHelper);		}
	}
	

	
	
	
	@Test(groups = {"singleCluster"})
	public void testProcessInstanceStatus_StartOutOfRange() throws Exception
	{
		Bundle b = new Bundle();

		try{

			b = (Bundle)Util.readELBundles()[0][0];
			b  = new Bundle(b,ivoryqa1.getEnvFileName());
			b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessValidity("2010-01-02T01:00Z","2010-01-02T01:22Z");
			b.setProcessPeriodicity(5,TimeUnit.minutes);
			b.setOutputFeedPeriodicity(5,TimeUnit.minutes);
			b.submitAndScheduleBundle(prismHelper);
			Thread.sleep(5000);			
			ProcessInstancesResult r  =  prismHelper.getProcessHelper().getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),"?start=2010-01-02T00:00Z&end=2010-01-02T01:20Z");
			instanceUtil.validateSuccessWithStatusCode(r, 400);
		}
		finally{
			b.deleteBundle(prismHelper);
			}
	}

	

	
	
	
	@Test(groups = {"singleCluster"})
	public void testProcessInstanceStatus_killed() throws Exception
	{
		Bundle b = new Bundle();

		try{

			b = (Bundle)Util.readELBundles()[0][0];
			b  = new Bundle(b,ivoryqa1.getEnvFileName());
			b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessValidity("2010-01-02T01:00Z","2010-01-02T01:22Z");
			b.setProcessPeriodicity(5,TimeUnit.minutes);
			b.submitAndScheduleBundle(prismHelper);
			Thread.sleep(15000);
			 prismHelper.getProcessHelper().delete(URLS.DELETE_URL,b.getProcessData());
			Thread.sleep(15000);
			ProcessInstancesResult r  =  prismHelper.getProcessHelper().getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),"?start=2010-01-02T01:00Z&end=2010-01-02T01:20Z");
			if((r.getStatusCode() != 777))
				Assert.assertTrue(false);	
		}
		finally{
			b.deleteBundle(prismHelper);		}
	}
	
	
	


	
	
	
	
	
	@Test(groups = {"singleCluster"})
	public void testProcessInstanceStatus_onlyStartSuspended() throws Exception
	{
		Bundle b = new Bundle();

		try{

			b = (Bundle)Util.readELBundles()[0][0];
			b  = new Bundle(b,ivoryqa1.getEnvFileName());
			b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessValidity("2010-01-02T01:00Z","2010-01-02T01:22Z");
			b.setProcessPeriodicity(5,TimeUnit.minutes);
			b.submitAndScheduleBundle(prismHelper);
			Thread.sleep(15000);
			 prismHelper.getProcessHelper().suspend(URLS.SUSPEND_URL,b.getProcessData());
			Thread.sleep(15000);
			ProcessInstancesResult r  =  prismHelper.getProcessHelper().getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),"?start=2010-01-02T01:00Z");
			instanceUtil.validateSuccessOnlyStart(r, b,WorkflowStatus.SUSPENDED);	
		}
		finally{
			b.deleteBundle(prismHelper);		}
	}
	

	

@Test(groups = {"singleCluster"})
	public void testProcessInstanceStatus_reverseDateRange() throws Exception
	{
		Bundle b = new Bundle();

		try{

			b = (Bundle)Util.readELBundles()[0][0];
			b  = new Bundle(b,ivoryqa1.getEnvFileName());
			b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessValidity("2010-01-02T01:00Z","2010-01-02T01:22Z");
			b.setProcessPeriodicity(5,TimeUnit.minutes);
			b.submitAndScheduleBundle(prismHelper);
			Thread.sleep(15000);
			ProcessInstancesResult r  =  prismHelper.getProcessHelper().getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),"?start=2010-01-02T01:20Z&end=2010-01-02T01:07Z");
			instanceUtil.validateSuccessWithStatusCode(r, 400);
		}
		finally{
			b.deleteBundle(prismHelper);		}
	}

	

	
	@Test(groups = {"singleCluster"})
	public void testProcessInstanceStatus_StartEndOutOfRange() throws Exception
	{
		Bundle b = new Bundle();

		try{

			b = (Bundle)Util.readELBundles()[0][0];
			b  = new Bundle(b,ivoryqa1.getEnvFileName());
			b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessValidity("2010-01-02T01:00Z","2010-01-02T01:22Z");
			b.setProcessPeriodicity(5,TimeUnit.minutes);
			b.setOutputFeedPeriodicity(5,TimeUnit.minutes);
			b.setOutputFeedLocationData("/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessConcurrency(2);
			b.submitAndScheduleBundle(prismHelper);
			Thread.sleep(15000);			
			ProcessInstancesResult r  =  prismHelper.getProcessHelper().getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),"?start=2010-01-02T00:00Z&end=2010-01-02T01:30Z");
			instanceUtil.validateSuccessWithStatusCode(r, 400);	
		}
		finally{
			b.deleteBundle(prismHelper);		}
	}
	
	
	


	@Test(groups = {"singleCluster"})
	public void testProcessInstanceStatus_resumed() throws Exception
	{
		Bundle b = new Bundle();

		try{

			b = (Bundle)Util.readELBundles()[0][0];
			b  = new Bundle(b,ivoryqa1.getEnvFileName());
			b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessValidity("2010-01-02T01:00Z","2010-01-02T01:22Z");
			b.setProcessPeriodicity(5,TimeUnit.minutes);
			b.setOutputFeedPeriodicity(5,TimeUnit.minutes);
			b.setOutputFeedLocationData("/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessConcurrency(2);
			b.submitAndScheduleBundle(prismHelper);
			Thread.sleep(15000);
			 prismHelper.getProcessHelper().suspend(URLS.SUSPEND_URL,b.getProcessData());
			Thread.sleep(15000);
			 prismHelper.getProcessHelper().resume(URLS.RESUME_URL,b.getProcessData());
			Thread.sleep(15000);
			ProcessInstancesResult r  =  prismHelper.getProcessHelper().getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),"?start=2010-01-02T01:00Z&end=2010-01-02T01:22Z");
			instanceUtil.validateSuccess(r, b,WorkflowStatus.RUNNING);	 
		}
		finally{
			b.deleteBundle(prismHelper);		}
	}
	
	@Test(groups = {"singleCluster"})
	public void testProcessInstanceStatus_onlyStart() throws Exception
	{
		Bundle b = new Bundle();

		try{
			b = (Bundle)Util.readELBundles()[0][0];
			b  = new Bundle(b,ivoryqa1.getEnvFileName());
			b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessValidity("2010-01-02T01:00Z","2010-01-02T01:22Z");
			b.setProcessPeriodicity(5,TimeUnit.minutes);
			b.submitAndScheduleBundle(prismHelper);
			Thread.sleep(15000);
			ProcessInstancesResult r  =  prismHelper.getProcessHelper().getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),"?start=2010-01-02T01:00Z");
			instanceUtil.validateSuccessOnlyStart(r, b,WorkflowStatus.RUNNING);	
		}
		finally{
			b.deleteBundle(prismHelper);		}
	}
	

	
	@Test(groups = {"singleCluster"})
	public void testProcessInstanceStatus_invalidName() throws Exception
	{
		Bundle b = new Bundle();

		try{

			b = (Bundle)Util.readELBundles()[0][0];
			b  = new Bundle(b,ivoryqa1.getEnvFileName());
			b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessValidity("2010-01-02T01:00Z","2010-01-02T02:30Z");
			b.setProcessPeriodicity(5,TimeUnit.minutes);
			b.submitAndScheduleBundle(prismHelper);
			ProcessInstancesResult r  =  prismHelper.getProcessHelper().getProcessInstanceStatus("invalidProcess","?start=2010-01-01T01:00Z");
			if(!(r.getStatusCode() == 777))
				Assert.assertTrue(false);		
		}
		finally{
			b.deleteBundle(prismHelper);
		}
	}
	
	
	@Test(groups = {"singleCluster"})
	public void testProcessInstanceStatus_suspended() throws Exception
	{
		Bundle b = new Bundle();

		try{

			b = (Bundle)Util.readELBundles()[0][0];
			b  = new Bundle(b,ivoryqa1.getEnvFileName());
			
			b  = new Bundle(b,ivoryqa1.getEnvFileName());
			b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessValidity("2010-01-02T01:00Z","2010-01-02T01:22Z");
			b.setProcessPeriodicity(5,TimeUnit.minutes);
			//b.setCLusterColo("ua2");
			
			for(int i = 0 ; i < b.getClusters().size() ; i++)
				
			Util.print("cluster to be submitted: " + i + "  "+b.getClusters().get(i));
			
			b.submitAndScheduleBundle(prismHelper);
			Thread.sleep(5000);
			prismHelper.getProcessHelper().suspend(URLS.SUSPEND_URL,b.getProcessData());
			Thread.sleep(5000);
	//		ProcessInstancesResult r  = ivoryqa1.getProcessHelper().getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),"?start=2010-01-02T01:00Z&end=2010-01-02T01:20Z");
			ProcessInstancesResult r  = prismHelper.getProcessHelper().getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),"?start=2010-01-02T01:00Z&end=2010-01-02T01:20Z");

			instanceUtil.validateSuccess(r, b,WorkflowStatus.SUSPENDED);	
		}
		finally{
			prismHelper.getProcessHelper().delete(URLS.DELETE_URL,b.getProcessData());
			prismHelper.getFeedHelper().delete(URLS.DELETE_URL, b.getDataSets().get(0));
			prismHelper.getFeedHelper().delete(URLS.DELETE_URL, b.getDataSets().get(1));
			prismHelper.getClusterHelper().delete(URLS.DELETE_URL,b.getClusters().get(0));
		}
	}

	@Test(groups = {"singleCluster"})
	public void testProcessInstanceStatus_woParams() throws Exception
	{
		Bundle b = new Bundle();

		try{

			b = (Bundle)Util.readELBundles()[0][0];
			b  = new Bundle(b,ivoryqa1.getEnvFileName());
			b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessValidity("2010-01-02T01:00Z","2010-01-02T02:30Z");
			b.setProcessPeriodicity(5,TimeUnit.minutes);
			b.submitAndScheduleBundle(prismHelper);
			ProcessInstancesResult r  =  prismHelper.getProcessHelper().getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),null);
			instanceUtil.validateSuccessWithStatusCode(r,2);
		}
		finally{
			b.deleteBundle(prismHelper);		}
	}

	@Test(groups = {"singleCluster"})
    public void testProcessInstanceStatus_timedOut() throws Exception
    {
	    Bundle b = new Bundle();

        //submit 
        try{

            b = (Bundle)Util.readELBundles()[0][0];
            b  = new Bundle(b,ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/timedoutStatus/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");

            b.setProcessValidity("2010-01-02T01:00Z","2010-01-02T01:11Z");
            b.setProcessPeriodicity(5,TimeUnit.minutes);
            b.setProcessTimeOut(2, TimeUnit.minutes);
            b.setOutputFeedPeriodicity(5,TimeUnit.minutes);
            b.setOutputFeedLocationData("/examples/samarth/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessConcurrency(3);
            b.submitAndScheduleBundle(prismHelper);
            //Thread.sleep(240000);
            org.apache.oozie.client.CoordinatorAction.Status s = null;
            while(!org.apache.oozie.client.CoordinatorAction.Status.TIMEDOUT.equals(s)){
                s = instanceUtil.getInstanceStatus(ivoryqa1, Util.readEntityName(b.getProcessData()), 0, 0);
                Thread.sleep(15000);
            }
            ProcessInstancesResult r  = prismHelper.getProcessHelper().getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),"?start=2010-01-02T01:00Z&end=2010-01-02T01:11Z");
            instanceUtil.validateSuccessWithStatusCode(r,2);
        }
        finally{
            b.deleteBundle(prismHelper);
        }
    }
	
}
