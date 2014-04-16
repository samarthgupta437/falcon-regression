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
//
//import org.apache.oozie.client.CoordinatorAction;
//import org.testng.Assert;
//import org.testng.annotations.BeforeMethod;
//import org.testng.annotations.Test;
//
//import com.inmobi.qa.airavatqa.core.Bundle;
//import com.inmobi.qa.airavatqa.core.ColoHelper;
//import com.inmobi.qa.airavatqa.core.PrismHelper;
//import com.inmobi.qa.airavatqa.core.Util;
//import com.inmobi.qa.airavatqa.core.instanceUtil;
//import org.apache.ivory.entity.v0.Frequency.TimeUnit;
//
//public class AvailabilityFlagTest {
//
//	PrismHelper prismHelper=new PrismHelper("prism.properties");
//	ColoHelper ivoryqa1 = new ColoHelper("ivoryqa-1.config.properties");
//
//
//	@BeforeMethod
//	public void testName(Method method)
//	{
//		Util.print("test name: "+method.getName());
//	}
//
//	@Test(groups = { "0.1","0.2"})
//
//	public void singleFile() throws Exception
//	{
//		Bundle b = new Bundle();
//
//		try{
//
//			String dependency = "depends.txt" ;
//			b = (Bundle)Util.readAvailabilityBUndle()[0][0];
//			b = new Bundle(b,ivoryqa1.getEnvFileName());
//			
//			
//			
//			String START_TIME = instanceUtil.getTimeWrtSystemTime(-37);
//			String END_TIME = instanceUtil.getTimeWrtSystemTime(12);
//			b.setProcessValidity(START_TIME,END_TIME);			
//			b.setProcessPeriodicity(5,TimeUnit.minutes);
//			b.setOutputFeedPeriodicity(5,TimeUnit.minutes);
//			b.setProcessConcurrency(5);
//			b.setInputFeedAvailabilityFlag(dependency);
//
//
//			String prefix = b.getFeedDataPathPrefix();
//			Util.HDFSCleanup(prefix.substring(1));
//			Util.lateDataReplenish(300,0,1,prefix);
//			Thread.sleep(15000);
//
//			b.submitAndScheduleBundle(prismHelper,false);
//
//
//
//			Thread.sleep(30000);
//
//			Assert.assertEquals(instanceUtil.getInstanceStatus(Util.getProcessName(b
// .getProcessData()), 0,0),
// CoordinatorAction.Status.WAITING);
//
//			ArrayList<String> missingDependencyList = instanceUtil.getMissingDependencyForInstance
// (Util.getProcessName
// (b.getProcessData()), 0, 0);
//			ArrayList<String> folderList = instanceUtil.getFolderlistFromDependencyList
// (missingDependencyList);
//			instanceUtil.putFileInFolders(folderList,
// "src/test/resources/AvailabilityBundle/depends.txt");
//
//			Thread.sleep(60000);
//			Assert.assertEquals(instanceUtil.getInstanceStatus(Util.getProcessName(b
// .getProcessData()), 0,0),
// CoordinatorAction.Status.RUNNING);
//
//			Assert.assertEquals(instanceUtil.getInstanceStatus(Util.getProcessName(b
// .getProcessData()), 0,5),
// CoordinatorAction.Status.WAITING);
//
//			missingDependencyList = instanceUtil.getMissingDependencyForInstance(Util
// .getProcessName(b.getProcessData
// ()), 0, 5);
//			folderList = instanceUtil.getFolderlistFromDependencyList(missingDependencyList);
//			instanceUtil.putFileInFolders(folderList,
// "src/test/resources/AvailabilityBundle/depends.txt");
//			Thread.sleep(60000);
//
//			Assert.assertEquals(instanceUtil.getInstanceStatus(Util.getProcessName(b
// .getProcessData()), 0,5),
// CoordinatorAction.Status.RUNNING);
//
//
//		}
//		finally{
//			b.deleteBundle();
//		}
//	}
//
//	@Test(groups = { "0.1","0.2"})
//	public void dependentPath() throws Exception
//	{
//		Bundle b = new Bundle();
//
//		try{
//
//			String dependency = "newFolder/depends.txt" ;
//			b = (Bundle)Util.readAvailabilityBUndle()[0][0];
//			b.generateUniqueBundle();
//			String START_TIME = instanceUtil.getTimeWrtSystemTime(-37);
//			String END_TIME = instanceUtil.getTimeWrtSystemTime(12);
//			b.setProcessValidity(START_TIME,END_TIME);			
//			b.setProcessPeriodicity(5,TimeUnit.minutes);
//			b.setOutputFeedPeriodicity(5,TimeUnit.minutes);
//			b.setProcessConcurrency(5);
//			b.setInputFeedAvailabilityFlag(dependency);
//
//
//			String prefix = b.getFeedDataPathPrefix();
//			Util.HDFSCleanup(prefix.substring(1));
//			Util.lateDataReplenish(300,0,1,prefix);
//			Thread.sleep(15000);
//
//			b.submitAndScheduleBundle(prismHelper,false);
//
//
//
//			Thread.sleep(30000);
//
//			Assert.assertEquals(instanceUtil.getInstanceStatus(Util.getProcessName(b
// .getProcessData()), 0,0),
// CoordinatorAction.Status.WAITING);
//
//			ArrayList<String> missingDependencyList = instanceUtil.getMissingDependencyForInstance
// (Util.getProcessName
// (b.getProcessData()), 0, 0);
//			ArrayList<String> folderList = instanceUtil.getFolderlistFromDependencyList
// (missingDependencyList);
//			instanceUtil.createHDFSFolders(folderList);
//			instanceUtil.putFileInFolders(folderList,
// "src/test/resources/AvailabilityBundle/depends.txt");
//
//			Thread.sleep(60000);
//			Assert.assertEquals(instanceUtil.getInstanceStatus(Util.getProcessName(b
// .getProcessData()), 0,0),
// CoordinatorAction.Status.RUNNING);
//
//			Assert.assertEquals(instanceUtil.getInstanceStatus(Util.getProcessName(b
// .getProcessData()), 0,5),
// CoordinatorAction.Status.WAITING);
//
//			missingDependencyList = instanceUtil.getMissingDependencyForInstance(Util
// .getProcessName(b.getProcessData
// ()), 0, 5);
//			folderList = instanceUtil.getFolderlistFromDependencyList(missingDependencyList);
//			instanceUtil.createHDFSFolders(folderList);
//			instanceUtil.putFileInFolders(folderList,
// "src/test/resources/AvailabilityBundle/depends.txt");
//			Thread.sleep(60000);
//
//			Assert.assertEquals(instanceUtil.getInstanceStatus(Util.getProcessName(b
// .getProcessData()), 0,5),
// CoordinatorAction.Status.RUNNING);
//
//
//		}
//		finally{
//			b.deleteBundle();
//		}
//	}
//
//}
