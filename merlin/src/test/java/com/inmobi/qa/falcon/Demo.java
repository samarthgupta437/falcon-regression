package com.inmobi.qa.falcon;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.XOozieClient;
import org.apache.oozie.client.rest.RestConstants;
import org.testng.annotations.Test;

import com.inmobi.qa.falcon.bundle.Bundle;
import com.inmobi.qa.falcon.generated.dependencies.Frequency.TimeUnit;
import com.inmobi.qa.falcon.helpers.ColoHelper;
import com.inmobi.qa.falcon.helpers.PrismHelper;
import com.inmobi.qa.falcon.interfaces.EntityHelperFactory;
import com.inmobi.qa.falcon.interfaces.IEntityManagerHelper;
import com.inmobi.qa.falcon.supportClasses.ENTITY_TYPE;
import com.inmobi.qa.falcon.util.Util;
import com.inmobi.qa.falcon.util.instanceUtil;



public class Demo {

	IEntityManagerHelper dataHelper=EntityHelperFactory.getEntityHelper(ENTITY_TYPE.DATA);
	IEntityManagerHelper processHelper=EntityHelperFactory.getEntityHelper(ENTITY_TYPE.PROCESS);

	PrismHelper prismHelper=new PrismHelper("prism.properties");
	ColoHelper gs1001 = new ColoHelper("gs1001.config.properties");
	/*

	@Test(dataProvider="demo-DP") 
	public void demoBundle(Bundle bundle) throws Exception {

		try{
			bundle.submitAndScheduleBundle();

		}
		catch(Exception e)
		{
			e.printStackTrace();
			throw new TestNGException(e.getMessage());
		}

		finally
		{
			bundle.deleteBundle();
		}
	}

	@DataProvider(name="demo-DP")
	public static Object[][] getTestData(Method m) throws Exception
	{

		return Util.readDemoBundle();
	}*/

	@Test
	public void test() throws Exception{


		XOozieClient oozieClient=new XOozieClient(Util.readPropertiesFile(gs1001.getEnvFileName(),"oozie_url"));
		Bundle b = new Bundle();
		try{

			/*		b = (Bundle)Util.readELBundles()[0][0];
			b  = new Bundle(b,gs1001.getEnvFileName());
			b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			String startTime = instanceUtil.getTimeWrtSystemTime(-20);
			String endTime = instanceUtil.getTimeWrtSystemTime(200);


			b.setProcessValidity(startTime,endTime);
			b.setProcessPeriodicity(5,TimeUnit.minutes);
			b.setOutputFeedPeriodicity(5,TimeUnit.minutes);
			b.setOutputFeedLocationData("/examples/samarth/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
			b.setProcessConcurrency(3);
			b.setProcessTimeOut(1, TimeUnit.minutes);
			b.submitAndScheduleBundle(prismHelper);*/
			//	String coordID = instanceUtil.getLatestCoordinator(gs1001, b.getProcessName(), ENTITY_TYPE.PROCESS);
			CoordinatorJob coord = oozieClient.getCoordJobInfo("0000806-130523172649012-oozie-oozi-C");

			String actionId_1 = coord.getId() + "@" + 1;
			String actionId_2 = coord.getId() + "@" + 2;
			String actionId_3 = coord.getId() + "@" + 3;
			CoordinatorAction coordActionInfo_1 = oozieClient.getCoordActionInfo(actionId_1);
			CoordinatorAction coordActionInfo_2 = oozieClient.getCoordActionInfo(actionId_2);
			CoordinatorAction coordActionInfo_3 = oozieClient.getCoordActionInfo(actionId_3);
			String status_1 = coordActionInfo_1.getStatus().name();
			String status_2 = coordActionInfo_2.getStatus().name();

			String status_3 = coordActionInfo_3.getStatus().name();

			Util.print(coordActionInfo_1.getId());
			Util.print(coordActionInfo_1.getJobId());
			Util.print(coordActionInfo_1.getExternalId());
			Util.print(coordActionInfo_1.getCreatedConf());
			Util.print(coordActionInfo_1.getRunConf());
			Util.print(coord.getConf());

			//Properties jobprops = OozieUtils.toProperties(coordActionInfo_1.getCreatedConf());
			// Configuration conf = new Configuration();
			//Properties jobprops = new Properties();
			//  for (Map.Entry<String, String> entry : conf) {
			//     jobprops.put(entry.getKey(), entry.getValue());
			//}
			//Properties jobprops = OozieUtils.toProperties(coordActionInfo_1.getRunConf());
			//Properties jobprops = OozieUtils.toProperties(coord.getConf());

		//	WorkflowJob jobInfo = oozieClient.getJobInfo(coordActionInfo_1.getExternalId());
		//	Properties jobprops = OozieUtils.toProperties(jobInfo.getConf());
			Util.print("-----------------");

		//	Util.print(jobInfo.getConf());
		//	Util.print("parentid: "+jobInfo.getParentId());

		//	jobprops.put(OozieClient.RERUN_FAIL_NODES, "false");
		//	jobprops.remove(OozieClient.COORDINATOR_APP_PATH);
		//	jobprops.remove(OozieClient.BUNDLE_APP_PATH);
			//oozieClient.reRun(actionId_1, jobprops);
			//oozieClient.reRun(coordActionInfo_1.getExternalId(), jobprops);
			oozieClient.reRunCoord("0000806-130523172649012-oozie-oozi-C", RestConstants.JOB_COORD_RERUN_ACTION, Integer.toString(1), true, true);
			System.out.println("rerun done");

		}
		finally{
			//	b.deleteBundle(prismHelper);
		}
	}
}
