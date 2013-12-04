package com.inmobi.qa.falcon;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.oozie.client.CoordinatorAction;
import org.joda.time.DateTime;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.inmobi.qa.falcon.bundle.Bundle;
import com.inmobi.qa.falcon.generated.dependencies.Frequency.TimeUnit;
import com.inmobi.qa.falcon.helpers.ColoHelper;
import com.inmobi.qa.falcon.helpers.PrismHelper;
import com.inmobi.qa.falcon.util.Util;
import com.inmobi.qa.falcon.util.instanceUtil;
/**
 * 
 * @author samarth.gupta
 *
 */
public class ELExp_FutureAndLatest {


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

		String startDate = instanceUtil.getTimeWrtSystemTime(-150);
		String endDate = instanceUtil.getTimeWrtSystemTime(100);

		b.setInputFeedDataPath("/ELExp_latest/testData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
		String prefix = b.getFeedDataPathPrefix();
		Util.HDFSCleanup(ivoryqa1,prefix.substring(1));

		DateTime startDateJoda = new DateTime(instanceUtil.oozieDateToDate(startDate));
		DateTime endDateJoda = new DateTime(instanceUtil.oozieDateToDate(endDate));

		List<String> dataDates = Util.getMinuteDatesOnEitherSide(startDateJoda,endDateJoda,1);

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


	@Test(groups = {"singleCluster"},dataProvider="EL-DP", dataProviderClass = Bundle.class)
	public void latestTest(Bundle b) throws Exception
	{
		try{
			b = new Bundle(b,ivoryqa1.getEnvFileName());


			b.setInputFeedDataPath("/ELExp_latest/testData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setInputFeedPeriodicity(5,TimeUnit.minutes);

			final String startInstance="2010-04-01T00:00Z" ;
			String endInstance="2013-04-01T00:00Z";
			b.setInputFeedValidity(startInstance,endInstance);
			b.setDatasetInstances("latest(-3)", "latest(0)");
			//b.setDatasetInstances("now(0,-40)", "now(0,0)");

			String processStart = instanceUtil.getTimeWrtSystemTime(-3);
			String processEnd = instanceUtil.getTimeWrtSystemTime(8);
			Util.print("processStart: "+processStart+" processEnd: "+processEnd);
			b.setProcessValidity(processStart,processEnd);

			b.setProcessPeriodicity(5,TimeUnit.minutes);

			b.submitAndScheduleBundle(prismHelper);

			instanceUtil.waitTillInstanceReachState(ivoryqa1,b.getProcessName(),3,CoordinatorAction.Status.SUCCEEDED,20); 

		}
		catch(Exception e)
		{
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}
		finally
		{
			b.deleteBundle(prismHelper);
		}

	}



	@Test(groups = {"singleCluster"},dataProvider="EL-DP", dataProviderClass = Bundle.class)
	public void futureTest(Bundle b) throws Exception
	{
		try{
			b = new Bundle(b,ivoryqa1.getEnvFileName());


			b.setInputFeedDataPath("/ELExp_latest/testData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setInputFeedPeriodicity(5,TimeUnit.minutes);

			final String startInstance="2010-04-01T00:00Z" ;
			String endInstance="2013-04-01T00:00Z";
			b.setInputFeedValidity(startInstance,endInstance);
			b.setDatasetInstances("future(0,10)", "future(3,10)");
			//b.setDatasetInstances("now(0,-40)", "now(0,0)");

			String processStart = instanceUtil.getTimeWrtSystemTime(-3);
			String processEnd = instanceUtil.getTimeWrtSystemTime(8);
			Util.print("processStart: "+processStart+" processEnd: "+processEnd);
			b.setProcessValidity(processStart,processEnd);

			b.setProcessPeriodicity(5,TimeUnit.minutes);

			b.submitAndScheduleBundle(prismHelper);

			instanceUtil.waitTillInstanceReachState(ivoryqa1,b.getProcessName(),3,CoordinatorAction.Status.SUCCEEDED,20); 

		}
		catch(Exception e)
		{
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}
		finally
		{
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

		b.setInputFeedDataPath("/ELExp_latest/testData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
		String prefix = b.getFeedDataPathPrefix();
		Util.HDFSCleanup(ivoryqa1,prefix.substring(1));
	}

}
