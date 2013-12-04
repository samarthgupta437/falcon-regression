package com.inmobi.qa.falcon;

import com.inmobi.qa.falcon.generated.process.*;
import com.inmobi.qa.falcon.generated.process.Process;
import com.inmobi.qa.falcon.response.APIResult;
import com.inmobi.qa.falcon.response.ServiceResponse;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;

import com.inmobi.qa.falcon.bundle.Bundle;
import com.inmobi.qa.falcon.generated.dependencies.Frequency.TimeUnit;
import com.inmobi.qa.falcon.helpers.ColoHelper;
import com.inmobi.qa.falcon.helpers.PrismHelper;
import com.inmobi.qa.falcon.response.ProcessInstancesResult;
import com.inmobi.qa.falcon.response.ProcessInstancesResult.WorkflowStatus;
import com.inmobi.qa.falcon.util.Util;
import com.inmobi.qa.falcon.util.Util.URLS;
import com.inmobi.qa.falcon.util.instanceUtil;

public class EmbeddedPigScriptTest {
	PrismHelper prismHelper=new PrismHelper("prism.properties");
	ColoHelper ivoryqa1 = new ColoHelper("gs1001.config.properties");

	@BeforeClass(alwaysRun=true)
	public void createTestData() throws Exception {

		Util.print("in @BeforeClass");

		System.setProperty("java.security.krb5.realm", "");
		System.setProperty("java.security.krb5.kdc", "");

		Bundle b = (Bundle)Util.readELBundles()[0][0];
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

		for (String dataDate : dataDates) dataFolder.add(dataDate);

		instanceUtil.putDataInFolders(ivoryqa1,dataFolder);
	}

	@BeforeMethod(alwaysRun=true)
	public void testName(Method method) {
		Util.print("test name: "+method.getName());
	}

	@Test(groups = {"singleCluster"})
	public void getResumedProcessInstance() throws Exception {
		Bundle b = new Bundle();

		try {
			b = (Bundle)Util.readELBundles()[0][0];
			b = new Bundle(b,ivoryqa1.getEnvFileName());
			b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T02:30Z");
			b.setProcessPeriodicity(5, TimeUnit.minutes);
			b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
			b.setProcessConcurrency(3);

			b.setProcessData(b.setProcessInputNames(b.getProcessData(), "INPUT"));
			b.setProcessData(b.setProcessOutputNames(b.getProcessData(), "OUTPUT"));
			b.setProcessWorkflow("/examples/apps/pig/id.pig");
			b.setOutputFeedLocationData("/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");

			final Process processElement = instanceUtil.getProcessElement(b);
			final Properties properties = new Properties();
			final Property property = new Property();
			property.setName("queueName");
			property.setValue("default");
			properties.addProperty(property);
			processElement.setProperties(properties);
			processElement.getWorkflow().setEngine(EngineType.PIG);
			instanceUtil.writeProcessElement(b, processElement);

			b.submitAndScheduleBundle(prismHelper);
			Thread.sleep(15000);
			prismHelper.getProcessHelper().suspend(URLS.SUSPEND_URL,b.getProcessData());
			Thread.sleep(15000);
			ServiceResponse status  = prismHelper.getProcessHelper().getStatus(URLS.STATUS_URL, b.getProcessData());
			Assert.assertTrue(status.getMessage().contains("SUSPENDED"), "Process not suspended.");
			prismHelper.getProcessHelper().resume(URLS.RESUME_URL, b.getProcessData());
			Thread.sleep(15000);
			ProcessInstancesResult r  = prismHelper.getProcessHelper().getRunningInstance(URLS.INSTANCE_RUNNING,Util.readEntityName(b.getProcessData()));
			instanceUtil.validateSuccess(r, b, WorkflowStatus.RUNNING);
		}
		finally{
			b.deleteBundle(prismHelper);
		}
	}

	@Test(groups = {"singleCluster"})
	public void getSuspendedProcessInstance() throws Exception {
		Bundle b = new Bundle();

		try{

			b = (Bundle)Util.readELBundles()[0][0];
			b = new Bundle(b,ivoryqa1.getEnvFileName());
			b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessValidity("2010-01-02T01:00Z","2010-01-02T02:30Z");
			b.setProcessPeriodicity(5,TimeUnit.minutes);
			b.setOutputFeedPeriodicity(5,TimeUnit.minutes);
			b.setOutputFeedLocationData("/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessConcurrency(3);

			b.setProcessData(b.setProcessInputNames(b.getProcessData(), "INPUT"));
			b.setProcessData(b.setProcessOutputNames(b.getProcessData(), "OUTPUT"));
			b.setProcessWorkflow("/examples/apps/pig/id.pig");
			b.setOutputFeedLocationData("/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");

			final Process processElement = instanceUtil.getProcessElement(b);
			final Properties properties = new Properties();
			final Property property = new Property();
			property.setName("queueName");
			property.setValue("default");
			properties.addProperty(property);
			processElement.setProperties(properties);
			processElement.getWorkflow().setEngine(EngineType.PIG);
			instanceUtil.writeProcessElement(b, processElement);

			b.submitAndScheduleBundle(prismHelper);
			prismHelper.getProcessHelper().suspend(URLS.SUSPEND_URL,b.getProcessData());
			Thread.sleep(5000);
			ProcessInstancesResult r  = prismHelper.getProcessHelper().getRunningInstance(URLS.INSTANCE_RUNNING,Util.readEntityName(b.getProcessData()));
			instanceUtil.validateSuccessWOInstances(r);
		}
		finally{
			b.deleteBundle(prismHelper);
		}
	}

	@Test(groups = {"singleCluster"})
	public void getRunningProcessInstance() throws Exception
	{
		Bundle b = new Bundle();
		try {
			b = (Bundle)Util.readELBundles()[0][0];
			b = new Bundle(b,ivoryqa1.getEnvFileName());
			b.setCLusterColo("ua2");
			b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessValidity("2010-01-02T01:00Z","2010-01-02T02:30Z");
			b.setProcessPeriodicity(5,TimeUnit.minutes);

			b.setProcessData(b.setProcessInputNames(b.getProcessData(), "INPUT"));
			b.setProcessData(b.setProcessOutputNames(b.getProcessData(), "OUTPUT"));
			b.setProcessWorkflow("/examples/apps/pig/id.pig");
			b.setOutputFeedLocationData("/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");

			final Process processElement = instanceUtil.getProcessElement(b);
			final Properties properties = new Properties();
			final Property property = new Property();
			property.setName("queueName");
			property.setValue("default");
			properties.addProperty(property);
			processElement.setProperties(properties);
			processElement.getWorkflow().setEngine(EngineType.PIG);
			instanceUtil.writeProcessElement(b, processElement);

			b.submitAndScheduleBundle(prismHelper);
			Thread.sleep(5000);
			ProcessInstancesResult r  = prismHelper.getProcessHelper().getRunningInstance(URLS.INSTANCE_RUNNING,Util.readEntityName(b.getProcessData()));
			instanceUtil.validateSuccess(r,b,WorkflowStatus.RUNNING);
		} finally {
			b.deleteBundle(prismHelper);
		}
	}

	@Test(groups = {"singleCluster"})
	public void getKilledProcessInstance() throws Exception
	{
		Bundle b = new Bundle();

		try{
			b = (Bundle)Util.readELBundles()[0][0];
			b = new Bundle(b,ivoryqa1.getEnvFileName());
			b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");

			b.setProcessData(b.setProcessInputNames(b.getProcessData(), "INPUT"));
			b.setProcessData(b.setProcessOutputNames(b.getProcessData(), "OUTPUT"));
			b.setProcessWorkflow("/examples/apps/pig/id.pig");
			b.setOutputFeedLocationData("/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");

			final Process processElement = instanceUtil.getProcessElement(b);
			final Properties properties = new Properties();
			final Property property = new Property();
			property.setName("queueName");
			property.setValue("default");
			properties.addProperty(property);
			processElement.setProperties(properties);
			processElement.getWorkflow().setEngine(EngineType.PIG);
			instanceUtil.writeProcessElement(b, processElement);

			b.submitAndScheduleBundle(prismHelper);
			prismHelper.getProcessHelper().delete(URLS.DELETE_URL,b.getProcessData());
			Thread.sleep(5000);
			ProcessInstancesResult r  = prismHelper.getProcessHelper().getRunningInstance(URLS.INSTANCE_RUNNING,Util.readEntityName(b.getProcessData()));
			AssertJUnit.assertTrue(r.getStatusCode() == 777);
		}
		finally{
			b.deleteBundle(prismHelper);
		}
	}

	@Test(groups = {"singleCluster"})
	public void getSucceededProcessInstance() throws Exception {
		Bundle b = new Bundle();
		try {
			b = (Bundle)Util.readELBundles()[0][0];
			b = new Bundle(b,ivoryqa1.getEnvFileName());
			b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T02:30Z");
			b.setProcessPeriodicity(5, TimeUnit.minutes);
			b.setProcessData(b.setProcessInputNames(b.getProcessData(), "INPUT"));
			b.setProcessData(b.setProcessOutputNames(b.getProcessData(), "OUTPUT"));
			b.setProcessWorkflow("/examples/apps/pig/id.pig");

			final Process processElement = instanceUtil.getProcessElement(b);
			final Properties properties = new Properties();
			final Property property = new Property();
			property.setName("queueName");
			property.setValue("default");
			properties.addProperty(property);
			processElement.setProperties(properties);
			processElement.getWorkflow().setEngine(EngineType.PIG);
			instanceUtil.writeProcessElement(b, processElement);

			b.submitAndScheduleBundle(prismHelper);
			Thread.sleep(5000);
			ProcessInstancesResult r  = prismHelper.getProcessHelper().getRunningInstance(URLS.INSTANCE_RUNNING,Util.readEntityName(b.getProcessData()));
			instanceUtil.validateSuccess(r, b, WorkflowStatus.RUNNING);

			org.apache.oozie.client.Job.Status s  = null ;
			for(int i = 0 ; i < 60 ; i ++) {
				s = instanceUtil.getDefaultCoordinatorStatus(ivoryqa1,Util.getProcessName(b.getProcessData()), 0);
				if(s.equals(org.apache.oozie.client.Job.Status.SUCCEEDED))
					break;
				Thread.sleep(30000);
			}

			Assert.assertTrue(s != null && s.equals(org.apache.oozie.client.Job.Status.SUCCEEDED),
					"The job did not succeeded even in long time");

			Thread.sleep(5000);
			r  = prismHelper.getProcessHelper().getRunningInstance(URLS.INSTANCE_STATUS, Util.readEntityName(b.getProcessData()));
			instanceUtil.validateSuccessWOInstances(r);
		} finally {
			b.deleteBundle(prismHelper);
		}
	}

	@AfterClass(alwaysRun=true)
	public void deleteData() throws Exception {
		Util.print("in @AfterClass");

		System.setProperty("java.security.krb5.realm", "");
		System.setProperty("java.security.krb5.kdc", "");

		Bundle b = (Bundle)Util.readELBundles()[0][0];
		b = new Bundle(b,ivoryqa1.getEnvFileName());
		b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
		String prefix = b.getFeedDataPathPrefix();
		Util.HDFSCleanup(ivoryqa1,prefix.substring(1));
	}
}