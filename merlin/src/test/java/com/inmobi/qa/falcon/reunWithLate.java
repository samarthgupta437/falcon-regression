package com.inmobi.qa.falcon;
//package com.inmobi.qa.airavatqa;
//
//import java.lang.reflect.Method;
//
//import org.apache.oozie.client.CoordinatorAction;
//import org.apache.oozie.client.CoordinatorJob;
//import org.apache.oozie.client.XOozieClient;
//import org.apache.oozie.client.CoordinatorAction.Status;
//import org.testng.Assert;
//import org.testng.annotations.BeforeMethod;
//import org.testng.annotations.Test;
//
//import com.inmobi.qa.airavatqa.core.Bundle;
//import com.inmobi.qa.airavatqa.core.ProcessEntityHelperImpl;
//import com.inmobi.qa.airavatqa.core.Util;
//import com.inmobi.qa.airavatqa.core.Frequency.TimeUnit;
//import com.inmobi.qa.airavatqa.core.Util.URLS;
//import com.inmobi.qa.airavatqa.core.instanceUtil;
//import com.inmobi.qa.airavatqa.generated.coordinator.COORDINATORAPP;
//
//public class reunWithLate {
//
//	@BeforeMethod
//	public void testName(Method method)
//	{
//		Util.print("test name: "+method.getName());
//	}
//	ProcessEntityHelperImpl processHelper=new ProcessEntityHelperImpl();
//
//	
//	static XOozieClient oozieClient=new XOozieClient("http://10.14.110.46:11000/oozie");
//
//	@Test(groups = { "0.1","0.2"})
//	public void rerunWithLate_multipleSucceeded() throws Exception
//	{ 
//		Bundle b = new Bundle();
//
//		try{
//
//			b = (Bundle)Util.readELBundles()[0][0];
//			b.generateUniqueBundle();
//			
//			String processStart = instanceUtil.getTimeWrtSystemTime(-3);
//			String processEnd = instanceUtil.getTimeWrtSystemTime(8);
//			Util.print("process start time: "+processStart);
//			Util.print("process end time: "+processEnd);
//			b.setProcessValidity(processStart,processEnd);			
//			b.setProcessPeriodicity(5,TimeUnit.minutes);
//			b.setOutputFeedPeriodicity(5,TimeUnit.minutes);
//			
//			String feed=Util.setFeedPathValue(Util.getInputFeedFromBundle(b),"/lateDataTest/samarth/testFolders/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
//	    	String lateLimit = "4" ; 
//			feed=Util.insertLateFeedValue(feed,lateLimit,"minutes");
//	    	Util.print("feed is: "+ feed);
//	    	b.getDataSets().remove(Util.getInputFeedFromBundle(b));
//	    	b.getDataSets().add(feed);
//			b.setProcessConcurrency(10);
//	      	String prefix = b.getFeedDataPathPrefix();
//			Util.HDFSCleanup(prefix.substring(1));
//			Util.lateDataReplenish(50,0,1,prefix);
//	    	
//			b.submitAndScheduleBundle(false);
//			Thread.sleep(20000);
//			
//			//instanceUtil.getInputFoldersForInstance(Util.getProcessName(b.getProcessData()), 0, 0);
//
//			
//			for(int i = 0 ; i < 20; i++)
//			{
//				if(instanceUtil.getInstanceStatus(Util.getProcessName(b.getProcessData()), 0, 1).equals(CoordinatorAction.Status.SUCCEEDED))
//					break;
//				
//				Thread.sleep(30000);
//			}
//			
//			if(!instanceUtil.getInstanceStatus(Util.getProcessName(b.getProcessData()), 0, 1).equals(CoordinatorAction.Status.SUCCEEDED))
//				Assert.assertTrue(false);
//			
//			Util.print("GMT string: :"+ Util.getSystemDate().toGMTString() + "normal string: "+ Util.getSystemDate().toString());
//			instanceUtil.putDataInFolders(instanceUtil.getInputFoldersForInstance(Util.getProcessName(b.getProcessData()), 0, 1),"late");
//
//						
//			CoordinatorJob coordJob = oozieClient.getCoordJobInfo(instanceUtil.getLateCoordIDFromProcess(Util.getProcessName(b.getProcessData()), 0));
//			String startTimeOfLateCoord = instanceUtil.dateToOozieDateWOOffSet(coordJob.getStartTime());
//			
//			
//			Util.print(" before sleeping GMT string: :"+ Util.getSystemDate().toGMTString() + "normal string: "+ Util.getSystemDate().toString());
//
//			instanceUtil.sleepTill(startTimeOfLateCoord);
//			
//			/*for(int i = 0 ; i < 20; i++)
//			{
//				if(instanceUtil.getLateInstanceStatus(Util.getProcessName(b.getProcessData()), 0, 1).equals(CoordinatorAction.Status.RUNNING))
//					break;
//				
//				Thread.sleep(30000);
//			}
//			
//			if(!instanceUtil.getLateInstanceStatus(Util.getProcessName(b.getProcessData()), 0, 1).equals(CoordinatorAction.Status.RUNNING))
//				Assert.assertTrue(false);*/
//			
//			
//			processHelper.getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),"?start="+processStart+"&end="+processEnd);
//
//			
//			processHelper.processInstanceRerunCLI(Util.getProcessName(b.getProcessData()),processStart,instanceUtil.addMinsToTime(processStart, 10));
//		
//			processHelper.getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),"?start="+processStart+"&end="+processEnd);
//
//
//		}
//		finally{
//			b.deleteBundle();
//		}
//	}
//}
