package com.inmobi.qa.falcon.prism;

import java.lang.reflect.Method;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.inmobi.qa.falcon.bundle.Bundle;
import com.inmobi.qa.falcon.generated.process.Property;
import com.inmobi.qa.falcon.helpers.ColoHelper;
import com.inmobi.qa.falcon.helpers.PrismHelper;
import com.inmobi.qa.falcon.supportClasses.ENTITY_TYPE;
import com.inmobi.qa.falcon.util.Util;
import com.inmobi.qa.falcon.util.instanceUtil;
/**
 * 
 * @author samarth.gupta
 *
 */


public class ProcessPartitionExpVariableTest {
	
	PrismHelper prismHelper=new PrismHelper("prism.properties");
	ColoHelper ivoryqa1 = new ColoHelper("ivoryqa-1.config.properties");
	
	@BeforeMethod(alwaysRun=true)
	public void testName(Method method)
	{
		Util.print("test name: "+method.getName());
	}

	
	@Test(enabled=true )
	public void ProcessPartitionExpVariableTest_OptionalCompulsaryPartition() throws Exception
	{
		Bundle b = new Bundle();

		try{
			
			String startTime = instanceUtil.getTimeWrtSystemTime(-4);
			String endTime = instanceUtil.getTimeWrtSystemTime(30);

			
			b = (Bundle)Util.readELBundles()[0][0];
			b  = new Bundle(b,ivoryqa1.getEnvFileName());
			
			b = b.getRequiredBundle(b,1,2,1,"/samarthData/input",1,startTime,endTime);
			
			
			
			b.setProcessData(b.setProcessInputNames(b.getProcessData(),"inputData0","inputData"));

			Property p = new Property();
			p.setName("var1");
			p.setValue("hardCoded");
			
			b.setProcessData(b.addProcessProperty(b.getProcessData(),p));
			
			b.setProcessData(b.setProcessInputPartition(b.getProcessData(),"${var1}","${fileTime}"));

			
			for(int i = 0 ; i < b.getClusters().size();i++)
				Util.print(b.getDataSets().get(i));
			
			for(int i = 0 ; i < b.getDataSets().size();i++)
				Util.print(b.getDataSets().get(i));
		
			Util.print(b.getProcessData());
			
			instanceUtil.createDataWithinDatesAndPrefix(ivoryqa1, instanceUtil.oozieDateToDate(instanceUtil.addMinsToTime(startTime, -25)), instanceUtil.oozieDateToDate(instanceUtil.addMinsToTime(endTime, 25)), "/samarthData/input/input1/", 1);
			//instanceUtil.createEmptyDirWithinDatesAndPrefix(ivoryqa1, instanceUtil.oozieDateToDate(instanceUtil.addMinsToTime(startTime, -25)), instanceUtil.oozieDateToDate(instanceUtil.addMinsToTime(endTime, 25)), "/samarthData/input/input0/", 1);

			
			b.submitAndScheduleBundle(b,prismHelper,false);
			
			Thread.sleep(20000);
			

			
			instanceUtil.waitTillInstanceReachState(ivoryqa1, Util.getProcessName(b.getProcessData()), 2,org.apache.oozie.client.CoordinatorAction.Status.SUCCEEDED, 20,ENTITY_TYPE.PROCESS);
			
			
		}
		finally{
			b.deleteBundle(prismHelper);
			Util.HDFSCleanup(ivoryqa1,"/samarthData/input/");

		}
	}
	
	@Test(enabled=false)
	public void ProcessPartitionExpVariableTest_OptionalPartition()
	{
		
	}
	
	@Test(enabled=false)
	public void ProcessPartitionExpVariableTest_CompulsaryPartition()
	{
		
	}
	

	@Test(enabled=false)
	public void ProcessPartitionExpVariableTest_moreThanOnceVariable()
	{
		
	}
}
