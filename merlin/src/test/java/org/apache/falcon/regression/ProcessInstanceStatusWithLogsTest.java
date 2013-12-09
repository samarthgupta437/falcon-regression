/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.falcon.regression;
//package com.inmobi.qa.airavatqa;
//
//import java.lang.reflect.Method;
//import java.util.ArrayList;
//import java.util.List;
//
//import org.apache.oozie.client.CoordinatorAction;
//import org.joda.time.DateTime;
//import org.testng.Assert;
//import org.testng.annotations.AfterClass;
//import org.testng.annotations.BeforeClass;
//import org.testng.annotations.BeforeMethod;
//import org.testng.annotations.Test;
//
//import com.inmobi.qa.airavatqa.core.Bundle;
//import com.inmobi.qa.airavatqa.core.ClusterEntityHelperImpl;
//import com.inmobi.qa.airavatqa.core.ColoHelper;
//import com.inmobi.qa.airavatqa.core.DataEntityHelperImpl;
//import com.inmobi.qa.airavatqa.core.ENTITY_TYPE;
//import com.inmobi.qa.airavatqa.core.EntityHelperFactory;
//import com.inmobi.qa.airavatqa.core.PrismHelper;
//import com.inmobi.qa.airavatqa.core.ProcessEntityHelperImpl;
//import com.inmobi.qa.airavatqa.core.ProcessInstancesResult;
//import org.apache.ivory.entity.v0.Frequency.TimeUnit;
//import com.inmobi.qa.airavatqa.core.ProcessInstancesResult.WorkflowStatus;
//import com.inmobi.qa.airavatqa.core.Util;
//import com.inmobi.qa.airavatqa.core.Util.URLS;
//import com.inmobi.qa.airavatqa.core.instanceUtil;
//import com.inmobi.qa.airavatqa.interfaces.entity.IEntityManagerHelper;
//import org.testng.annotations.AfterGroups;
//import org.testng.annotations.BeforeGroups;
//
//public class ProcessInstanceStatusWithLogsTest {
//
//
//	PrismHelper prismHelper=new PrismHelper("prism.properties");
//	ColoHelper ivoryqa1 = new ColoHelper("ivoryqa-1.config.properties");
//	
//        @BeforeClass(alwaysRun=true)
//	public void createTestData() throws Exception
//	{
//
//		Util.print("in @BeforeClass");
//		
//		System.setProperty("java.security.krb5.realm", "");
//		System.setProperty("java.security.krb5.kdc", "");
//		
//		
//		Bundle b = new Bundle();
//		b = (Bundle)Util.readELBundles()[0][0];
//		b.generateUniqueBundle();
//		b = new Bundle(b,ivoryqa1.getEnvFileName());
//
//		String startDate = "2010-01-01T20:00Z";
//		String endDate = "2010-01-03T01:04Z";
//	
//		b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
//		String prefix = b.getFeedDataPathPrefix();
//		Util.HDFSCleanup(ivoryqa1,prefix.substring(1));
//		
//		DateTime startDateJoda = new DateTime(instanceUtil.oozieDateToDate(startDate));
//		DateTime endDateJoda = new DateTime(instanceUtil.oozieDateToDate(endDate));
//
//		List<String> dataDates = Util.getMinuteDatesOnEitherSide(startDateJoda,endDateJoda,20);
//
//		for(int i = 0 ; i < dataDates.size(); i++)
//			dataDates.set(i, prefix + dataDates.get(i));
//		
//		ArrayList<String> dataFolder = new ArrayList<String>();
//		
//		for(int i = 0 ; i < dataDates.size(); i++)
//			dataFolder.add(dataDates.get(i));
//		
//		instanceUtil.putDataInFolders(ivoryqa1,dataFolder);
//	}
//	
//	
//	
//	@BeforeMethod(alwaysRun=true)
//	public void testName(Method method)
//	{
//		Util.print("test name: "+method.getName());
//	}
//	ProcessEntityHelperImpl processHelper=new ProcessEntityHelperImpl();
//	DataEntityHelperImpl feedHelper = new DataEntityHelperImpl();
//	ClusterEntityHelperImpl clusterHelper = new ClusterEntityHelperImpl();
//
//	
//	
//	@Test
//	public void testProcessInstanceStatus_MRJob_failed() throws Exception
//	{
//		Bundle b = new Bundle();
//
//		try{
//
//			b = (Bundle)Util.readELBundles()[0][0];
//			b.generateUniqueBundle();
//			b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
//			b.setProcessValidity("2010-01-02T11:00Z","2010-01-02T11:06Z");
//			b.setProcessPeriodicity(5,org.apache.ivory.entity.v0.Frequency.TimeUnit.minutes);
//			b.setProcessWorkflow("/examples/apps/errorAggregator");
//			
//			b.submitAndScheduleBundle(prismHelper);
//			
//			Thread.sleep(180000);
//			
//			//ProcessInstancesResult r  = processHelper.getProcessInstanceKill(Util.readEntityName
// (b.getProcessData())
// ,"?start=2010-01-02T11:00Z&type=DEFAULT");
//			//Thread.sleep(30000);
//
//			
//			
//			if(!instanceUtil.getInstanceStatus(Util.readEntityName(b.getProcessData()), 0,
// 0).equals(CoordinatorAction.Status.KILLED))
//				Assert.assertTrue(false);
//			
//			
//			ProcessInstancesResult r  = processHelper.getProcessInstanceStatus(Util.readEntityName
// (b.getProcessData())
// ,"?start=2010-01-02T11:00Z&type=DEFAULT");
//
//			instanceUtil.verifyStatusLog(r);
//			instanceUtil.validateResponse(r,1,0,0,0,1);
//
//			
//		}
//		finally{
//			b.deleteBundle();
//		}
//	}
//	
//	
//	
//	@Test
//	public void testProcessInstanceStatus_MRJob_Killed() throws Exception
//	{
//		Bundle b = new Bundle();
//
//		try{
//
//			b = (Bundle)Util.readELBundles()[0][0];
//			b.generateUniqueBundle();
//			b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
//			b.setProcessValidity("2010-01-02T11:00Z","2010-01-02T11:06Z");
//			b.setProcessPeriodicity(5,TimeUnit.minutes);
//			
//			b.submitAndScheduleBundle(prismHelper);
//			
//			Thread.sleep(30000);
//			
//			ProcessInstancesResult r  = processHelper.getProcessInstanceKill(Util.readEntityName(b
// .getProcessData()),
// "?start=2010-01-02T11:00Z&type=DEFAULT");
//			Thread.sleep(30000);
//
//			
//			
//			if(!instanceUtil.getInstanceStatus(Util.readEntityName(b.getProcessData()), 0,
// 0).equals(CoordinatorAction.Status.KILLED))
//				Assert.assertTrue(false);
//			
//			
//			 r  = processHelper.getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
// "?start=2010-01-02T11:00Z&type=DEFAULT");
//
//			instanceUtil.verifyStatusLog(r);
//			instanceUtil.validateResponse(r,1,0,0,0,1);
//
//			
//		}
//		finally{
//			b.deleteBundle();
//		}
//	}
//	
//	
//	
//
//	@Test
//	public void testProcessInstanceStatus_MRJob_Suspended() throws Exception
//	{
//		Bundle b = new Bundle();
//
//		try{
//
//			b = (Bundle)Util.readELBundles()[0][0];
//			b.generateUniqueBundle();
//			b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
//			b.setProcessValidity("2010-01-02T11:00Z","2010-01-02T11:06Z");
//			b.setProcessPeriodicity(5,TimeUnit.minutes);
//			b.submitAndScheduleBundle(prismHelper);
//			
//			Thread.sleep(30000);
//			
//			ProcessInstancesResult r  = processHelper.getProcessInstanceSuspend(Util
// .readEntityName(b.getProcessData()
// ),"?start=2010-01-02T11:00Z&type=DEFAULT");
//			Thread.sleep(30000);
//
//			
//			for(int i =0 ;  i <30 ; i++)
//			{
//				if(instanceUtil.getInstanceStatus(Util.readEntityName(b.getProcessData()), 0,
// 0).equals(CoordinatorAction.Status.SUSPENDED))
//					break;
//			}
//			
//			 r  = processHelper.getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
// "?start=2010-01-02T11:00Z&type=DEFAULT");
//
//			instanceUtil.verifyStatusLog(r);
//			instanceUtil.validateResponse(r,1,0,1,0,0);
//
//			
//		}
//		finally{
//			b.deleteBundle();
//		}
//	}
//	
//	
//	
//	@Test
//	public void testProcessInstanceStatus_MRJob_Running() throws Exception
//	{
//		Bundle b = new Bundle();
//
//		try{
//
//			b = (Bundle)Util.readELBundles()[0][0];
//			b.generateUniqueBundle();
//			b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
//			b.setProcessValidity("2010-01-02T11:00Z","2010-01-02T11:06Z");
//			b.setProcessPeriodicity(5,TimeUnit.minutes);
//			b.submitAndScheduleBundle(prismHelper);
//			
//			Thread.sleep(30000);
//			
//			
//			for(int i =0 ;  i <30 ; i++)
//			{
//				if(instanceUtil.getInstanceStatus(Util.readEntityName(b.getProcessData()), 0,
// 0).equals(CoordinatorAction.Status.RUNNING))
//					break;
//			}
//			
//			ProcessInstancesResult r  = processHelper.getProcessInstanceStatus(Util.readEntityName
// (b.getProcessData())
// ,"?start=2010-01-02T11:00Z&type=DEFAULT");
//
//			instanceUtil.verifyStatusLog(r);
//			instanceUtil.validateResponse(r,1,1,0,0,0);
//
//			
//		}
//		finally{
//			b.deleteBundle();
//		}
//	}
//	
//	
//	
// @Test
//	public void testProcessInstanceStatus_MRJob_Waiting() throws Exception
//	{
//		Bundle b = new Bundle();
//
//		try{
//
//			b = (Bundle)Util.readELBundles()[0][0];
//			b.generateUniqueBundle();
//			b.setInputFeedDataPath("/samarthData/invalidPath/${YEAR}/${MONTH}/${DAY}/${HOUR}/$
// {MINUTE}");
//			b.setProcessValidity("2010-01-02T11:00Z","2010-01-02T11:06Z");
//			b.setProcessPeriodicity(5,TimeUnit.minutes);
//			b.submitAndScheduleBundle(prismHelper);
//			Thread.sleep(60000);
//			
//			
//			processHelper.suspendViaCLI(Util.readEntityName(b.getProcessData()));
//			
//			ProcessInstancesResult r  = processHelper.getProcessInstanceStatus(Util.readEntityName
// (b.getProcessData())
// ,"?start=2010-01-02T11:00Z&end=2010-01-02T11:05Z&type=DEFAULT");
//			
//			instanceUtil.verifyStatusLog(r);
//			instanceUtil.validateResponse(r,2,0,0,2,0);
//			
//		}
//		finally{
//			b.deleteBundle();
//		}
//	}
//	
//	@Test
//	public void testProcessInstanceStatus_completedMRJob() throws Exception
//	{
//		Bundle b = new Bundle();
//
//		try{
//
//			b = (Bundle)Util.readELBundles()[0][0];
//			b.generateUniqueBundle();
//			b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
//			b.setProcessValidity("2010-01-02T11:00Z","2010-01-02T11:06Z");
//			b.setProcessPeriodicity(5,TimeUnit.minutes);
//			b.submitAndScheduleBundle(prismHelper);
//			Util.print("status immediately after schedule");
//			ProcessInstancesResult r  = processHelper.getProcessInstanceStatus(Util.readEntityName
// (b.getProcessData())
// ,"?start=2010-01-02T11:00Z&end=2010-01-02T11:05Z&type=DEFAULT");
//			Thread.sleep(30000);
//			
//			
//			org.apache.oozie.client.Job.Status s = null;
//			for(int i = 0 ; i < 60 ; i++)
//			{
//				Util.print("status in loop for count: "+i);
//
//				r  = processHelper.getProcessInstanceStatus(Util.readEntityName(b.getProcessData
// ()),
// "?start=2010-01-02T11:00Z&end=2010-01-02T11:05Z&type=DEFAULT");
//
//				s=instanceUtil.getDefaultCoordinatorStatus(Util.getProcessName(b.getProcessData())
// ,0);
//				if(s.equals(org.apache.oozie.client.Job.Status.SUCCEEDED) || s.equals(org.apache
// .oozie.client.Job
// .Status.DONEWITHERROR))
//					break;
//				
//				Thread.sleep(30000);
//				
//			}
//			
//			if(!(s.equals(org.apache.oozie.client.Job.Status.SUCCEEDED)|| s.equals(org.apache
// .oozie.client.Job.Status
// .DONEWITHERROR)))
//				Assert.assertTrue(false,"process should have been SUCCEEDED");
//			
//			r  = processHelper.getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
// "?start=2010-01-02T11:00Z&end=2010-01-02T11:05Z&type=DEFAULT");
//			instanceUtil.verifyStatusLog(r);
//			instanceUtil.validateResponse(r,2,0,0,0,0);
//			
//		}
//		finally{
//			b.deleteBundle();
//		}
//	}
//	
//	
//	@Test
//	public void testProcessInstanceStatus_completedJava_deleted() throws Exception
//	{
//		Bundle b = new Bundle();
//
//		try{
//
//			b = (Bundle)Util.readELBundles()[0][0];
//			b.generateUniqueBundle();
//			b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
//			b.setProcessValidity("2010-01-02T11:00Z","2010-01-02T11:06Z");
//			b.setProcessPeriodicity(5,TimeUnit.minutes);
//			b.setProcessWorkflow("/examples/apps/java-main");
//			b.submitAndScheduleBundle(prismHelper);
//			Util.print("status immediately after schedule");
//			ProcessInstancesResult r  = processHelper.getProcessInstanceStatus(Util.readEntityName
// (b.getProcessData())
// ,"?start=2010-01-02T11:00Z&end=2010-01-02T11:05Z");
//			Thread.sleep(20000);
//			
//			
// 			processHelper.getProcessInstanceKill(Util.readEntityName(b.getProcessData()),
// "?start=2010-01-02T11:00Z&end=2010-01-02T11:05Z");
//			
//			Thread.sleep(30000);
//			
//			
//			
//			r  = processHelper.getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
// "?start=2010-01-02T11:00Z&end=2010-01-02T11:05Z");
//			//instanceUtil.validateResponse(r, 2, 0, 0, 0,0);
//			
//			processHelper.getProcessInstanceRerun(Util.readEntityName(b.getProcessData()),
// "?start=2010-01-02T11:00Z&end=2010-01-02T11:05Z");
//			r  = processHelper.getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
// "?start=2010-01-02T11:00Z&end=2010-01-02T11:05Z&runid=1");
//
//			
//			
//		}
//		finally{
//			b.deleteBundle();
//		}
//	}
//	
//
//	
//	
//    @AfterClass(alwaysRun=true)
//	public void deleteData() throws Exception
//	{
//		Util.print("in @AfterClass");
//		
//		System.setProperty("java.security.krb5.realm", "");
//		System.setProperty("java.security.krb5.kdc", "");
//		
//		
//		Bundle b = new Bundle();
//		b = (Bundle)Util.readELBundles()[0][0];
//		b.generateUniqueBundle();
//		b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
//		String prefix = b.getFeedDataPathPrefix();
//		Util.HDFSCleanup(prefix.substring(1));
//	}
//
//}
