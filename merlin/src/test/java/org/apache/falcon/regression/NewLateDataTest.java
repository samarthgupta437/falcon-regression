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

///*
// * To change this template, choose Tools | Templates
// * and open the template in the editor.
// */
//package com.inmobi.qa.airavatqa;
//
//import com.inmobi.qa.airavatqa.core.Bundle;
//import com.inmobi.qa.airavatqa.core.ColoHelper;
//import com.inmobi.qa.airavatqa.core.PrismHelper;
//import com.inmobi.qa.airavatqa.core.Util;
//import com.inmobi.qa.airavatqa.core.Util.URLS;
//import com.inmobi.qa.airavatqa.generated.process.PolicyType;
//import com.inmobi.qa.airavatqa.mq.Consumer;
//import java.io.ByteArrayInputStream;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.oozie.client.BundleJob;
//import org.apache.oozie.client.CoordinatorAction;
//import org.apache.oozie.client.CoordinatorJob;
//import org.apache.oozie.client.WorkflowJob;
//import org.apache.oozie.client.XOozieClient;
//import org.joda.time.DateTime;
//import org.joda.time.DateTimeZone;
//import org.joda.time.Minutes;
//import org.joda.time.format.DateTimeFormat;
//import org.joda.time.format.DateTimeFormatter;
//import org.testng.Assert;
//import org.testng.TestNGException;
//import org.testng.annotations.DataProvider;
//import org.testng.annotations.Test;
//
//public class NewLateDataTest {
//    
//    PrismHelper prismHelper=new PrismHelper("prism.properties");
//    ColoHelper UA3ColoHelper=new ColoHelper("gs1001.config.properties");
//    
//    DateTimeFormatter formatter=DateTimeFormat.forPattern("yyyy/MM/dd/HH/mm");
//    
//    @Test(groups = {"0.2"},dataProvider="DP",priority=-20)
//    public void testLateDataWithCutOffInFutureAndBackOffPolicy(Bundle bundle) throws Exception
//    {
//        String delay="15";
//        bundle=new Bundle(bundle,UA3ColoHelper);
//        
//        try {
//            
//            String feed=Util.setFeedPathValue(Util.getInputFeedFromBundle(bundle),
// "/lateDataTest/testFolders/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
//            feed=Util.insertLateFeedValue(feed,delay,"minutes");
//            bundle.getDataSets().remove(Util.getInputFeedFromBundle(bundle));
//            bundle.getDataSets().add(feed);
//            Util.HDFSCleanup(UA3ColoHelper,"lateDataTest/testFolders/");
//            Util.lateDataReplenish(UA3ColoHelper,20,0,0);
//            List<String> initialData=Util.getHadoopLateData(UA3ColoHelper,
// Util.getInputFeedFromBundle(bundle));
//
//            bundle.generateUniqueBundle();
//            
//            for(String cluster:bundle.getClusters())
//            {
//                    System.out.println(cluster);
//                    Util.assertSucceeded(prismHelper.getClusterHelper().submitEntity(URLS
// .SUBMIT_URL,cluster));
//            }  
//            
//            for(String data:bundle.getDataSets())
//            {
//                    Util.assertSucceeded(prismHelper.getFeedHelper().submitEntity(URLS
// .SUBMIT_URL,data));
//            }  
//            
//    	//set process start and end date
//    	DateTime startDate=new DateTime(DateTimeZone.UTC).plusMinutes(2);
//    	DateTime endDate=new DateTime(DateTimeZone.UTC).plusMinutes(5);
//    	  
//    	bundle.setProcessValidity(startDate,endDate);
//        bundle.setProcessLatePolicy(PolicyType.BACKOFF,"minutes(3)");
//        
//        bundle.setProcessWorkflow("/examples/apps/phailFs/workflow.xml");
//    	
//    	//submit and schedule process
//        System.out.println(bundle.getProcessData());
//    	Util.assertSucceeded(prismHelper.getProcessHelper().submitEntity(URLS.SUBMIT_URL,
// bundle.getProcessData()));
//    	
//    	 System.out.println(bundle.getProcessData());
// 
//        Util.assertSucceeded(UA3ColoHelper.getProcessHelper().schedule(URLS.SCHEDULE_URL,
// bundle.getProcessData()));
//        
//        //attach the consumer
//        Consumer consumer=new Consumer("IVORY."+Util.readEntityName(bundle.getProcessData()),
// UA3ColoHelper.getClusterHelper().getActiveMQ());
//        consumer.start();
//        
//        //get the bundle ID
//        String bundleId=Util.getCoordID(Util.getOozieJobStatus(UA3ColoHelper,
// Util.readEntityName(bundle.getProcessData()),"NONE").get(0));
//        String status=Util.getBundleStatus(UA3ColoHelper,bundleId);
//        
//        //Thread.sleep(20000);
//        
//        //DateTime now=Util.getStartTimeForDefaultCoordinator(bundleId);
//        ArrayList<DateTime> dates=null;
//        
//        do {
//        
//        	dates=Util.getStartTimeForRunningCoordinators(UA3ColoHelper,bundleId);
//        	
//        }while(dates==null);
//        
//        
//        
//         System.out.println("Start time: "+formatter.print(startDate));
//         System.out.println("End time: "+formatter.print(endDate));
//         System.out.println("candidate nominal time:"+formatter.print(dates.get(0)));
//         DateTime now=dates.get(0);
//        
//        if(formatter.print(startDate).compareToIgnoreCase(formatter.print(dates.get(0)))>0)
//        {
//        	now=startDate;
//        }
//        
//        System.out.println("Now:"+formatter.print(now));
//        
//        //Assert.assertFalse(!(formatter.print(.plusMinutes(2)).compareTo(formatter.print
// (endDate))<0),
// "Uh oh! The timing is not at all correct!");
//        
//       
//        int diff=getTimeDiff(formatter.print(now),formatter.print(endDate));;
//        
//        String insertionFolder="";
//        DateTime insertionTime=null;
//        
//        boolean toInsert=true;
//        insertionFolder=Util.findFolderBetweenGivenTimeStamps(startDate,
// now.plusMinutes(Integer.parseInt(delay)),
// initialData);
//        //insert data after all relevant workflows are over
//        System.out.println("insertion folder is: "+insertionFolder);
//        while(!allRelevantWorkflowsAreOver(UA3ColoHelper,bundleId, insertionFolder))
//        {
//            //System.out.println("waiting for relevant workflows to be over before inserting
// folder
// "+insertionFolder);
//            //keep waiting
//        }
//        
//        System.out.println("going to insert data at: "+insertionFolder);
//        insertionTime=new DateTime(DateTimeZone.UTC);
//        System.out.println("insertion time is :"+insertionTime);
//        Util.injectMoreData(UA3ColoHelper,insertionFolder,
// "src/test/resources/OozieExampleInputData/lateData");
//        initialData=Util.getHadoopLateData(UA3ColoHelper,Util.getInputFeedFromBundle(bundle));
//        toInsert=false;
//        
//        //now wait till all the workflows are done with late data run as well
//        validateLateRunsForRelevantWorkflows(UA3ColoHelper,bundleId,1,
// bundle.getFeedDataPathPrefix()+insertionFolder);
//        
//        consumer.stop();
//        
//        Util.dumpConsumerData(consumer);        
//        }
//        catch(Exception e)
//        {
//            e.printStackTrace();
//            throw new TestNGException(e.getCause());
//        }
//        finally {
//            //bundle.deleteBundle();
//        }
//    }
//    
//    
//	private boolean allRelevantWorkflowsAreOver(PrismHelper prismHelper,String bundleId,
// String insertionFolder) throws Exception
//	{
//		HashMap<String,Boolean>workflowStatusMap=new HashMap<String, Boolean>(); 
//		
//                XOozieClient oozieClient=new XOozieClient(prismHelper.getProcessHelper()
// .getOozieURL());
//		BundleJob bundleJob = oozieClient.getBundleJobInfo(bundleId);
//    	
//		DateTimeFormatter formatter=DateTimeFormat.forPattern("yyyy/MM/dd/HH/mm");
//		List<String> actualNominalTimes=new ArrayList<String>();
//		
//		for(CoordinatorJob job:bundleJob.getCoordinators())
//		{
//			if(job.getAppName().contains("DEFAULT"))
//			{
//				
//				CoordinatorJob coordJob=oozieClient.getCoordJobInfo(job.getId());
//                                
//                                
//				for(CoordinatorAction action:coordJob.getActions())
//				{
//                                        CoordinatorAction actionMan=oozieClient
// .getCoordActionInfo(action.getId());
//                                        
//                                        if(!(actionMan.getStatus().equals(CoordinatorAction
// .Status.WAITING) ||
// actionMan.getStatus().equals(CoordinatorAction.Status.READY) || actionMan.getStatus().equals
// (CoordinatorAction
// .Status.SUBMITTED) ))
//                                        {
//                                            if(getRunConfInputString(actionMan.getRunConf())
// .contains
// (insertionFolder))
//                                            {
//                                            
//                                                if((actionMan.getStatus().equals
// (CoordinatorAction.Status
// .SUCCEEDED)) || actionMan.getStatus().equals(CoordinatorAction.Status.KILLED) || actionMan
// .getStatus().equals
// (CoordinatorAction.Status.FAILED))
//                                                {
//                                                    System.out.println("found folder
// "+insertionFolder+" in
// workflow: "+actionMan.getId());
//                                                    System.out.println("related workflow
// "+actionMan.getId()+" is
// over....");
//                                                    workflowStatusMap.put(actionMan.getId(),
// Boolean.TRUE);
//                                                }
//                                            else
//                                            {
//                                                workflowStatusMap.put(action.getId(),
// Boolean.FALSE);
//                                            }
//                                        }
//                                            
//                                        
//					}
//				}
//			}
//		}
//		
//                boolean finished=true;
//                
//                if(workflowStatusMap.isEmpty())
//                {
//                    return false;
//                }
//                
//		for(String workflowId:workflowStatusMap.keySet())
//                {
//                  finished&=workflowStatusMap.get(workflowId);  
//                }
//                
//                return finished;
//	}  
//        
//        
//    private int getTimeDiff(String timestamp1,String timestamp2) throws Exception
//    {
//    	DateTimeFormatter formatter=DateTimeFormat.forPattern("yyyy/MM/dd/HH/mm");
//    	//DateTime start=new DateTime(timestamp1,DateTimeZone.UTC);
//    	//DateTime end=new DateTime(timestamp2,DateTimeZone.UTC);
//    	
//    	return Minutes.minutesBetween(formatter.parseDateTime(timestamp1),
// formatter.parseDateTime(timestamp2))
// .getMinutes();
//    }
//    
//    private void validateLateRunsForRelevantWorkflows(PrismHelper prismHelper,String bundleId,
// int maxNumberOfRetries,String insertionFolder) throws Exception
//    {
//       //validate that all failed processes were retried the specified number of times.
//       int attempt=0;
//       boolean result=false;
//       while(true)
//       {
//            result=ensureAllInstancesHaveRerun(prismHelper,bundleId, maxNumberOfRetries,
// insertionFolder);
//            
//            if(result || attempt>3600)
//            {
//                break;
//            }
//            else
//            {
//                Thread.sleep(1000);
//                System.out.println("desired state not reached.This was attempt number: "+attempt);
//                attempt++;
//            }
//       }
//       Assert.assertTrue(result,"all reruns were not attempted correctly!");
//       
//       //now validate correctness of retry timings
//       
//    }  
//    
//        private boolean ensureAllInstancesHaveRerun(PrismHelper prismHelper,String bundleId,
// int maxNumberOfRetries,
// String insertionFolder) throws Exception
//        {
//           boolean retried=false;
//           
//           CoordinatorJob defaultCoordinator=getDefaultOozieCoord(prismHelper,bundleId);
//           //CoordinatorJob lateCoordinator=getLateOozieCoord(bundleId);
//           
//           boolean retriedAllDefault=validateLateReruns(prismHelper,defaultCoordinator,
// maxNumberOfRetries,
// insertionFolder,getAllRelevantWorkflows(prismHelper, bundleId, insertionFolder));
//           //boolean retriedAllLate=validateFailureRetries(lateCoordinator, maxNumberOfRetries);
//           
//           //if(retriedAllDefault && retriedAllLate)
//           if(retriedAllDefault)
//           {
//               return true;
//           }
//           return retried;
//        }
//        
//        private CoordinatorJob getDefaultOozieCoord(PrismHelper prismHelper,
// String bundleId) throws Exception
//        {
//           XOozieClient client=new XOozieClient(prismHelper.getProcessHelper().getOozieURL());
//           BundleJob bundlejob=client.getBundleJobInfo(bundleId); 
//           
//           for(CoordinatorJob coord:bundlejob.getCoordinators())
//           {
//               if(coord.getAppName().contains("DEFAULT"))
//               {
//                    return client.getCoordJobInfo(coord.getId());
//               }
//           }
//           return null;
//        } 
//        
//        
//    private boolean validateLateReruns(PrismHelper prismHelper,CoordinatorJob coordinator,
// int maxNumberOfRetries,
// String insertionFolder,int numberOfRelevantWorkflows) throws Exception
//    {
//       
//        if(maxNumberOfRetries<0)
//        {
//            maxNumberOfRetries=0;
//        }
//        
//        HashMap<String,Boolean> workflowMap=new HashMap<String, Boolean>();
//        
//        XOozieClient client=new XOozieClient(prismHelper.getProcessHelper().getOozieURL());
//        
//        if(coordinator.getActions().size()==0)
//        {
//            return false;
//        }
//        
//        for(CoordinatorAction action:coordinator.getActions())
//        {
//            
//              CoordinatorAction actionMan=client.getCoordActionInfo(action.getId());
//              if(getRunConfInputString(actionMan.getRunConf()).contains(insertionFolder))
//          {
//          
//          
//          if(null==action.getExternalId())
//          {
//              return false;
//          }
//          
//          
//          
//          WorkflowJob actionInfo=client.getJobInfo(action.getExternalId());
//          
//          
//          if(!(actionInfo.getStatus().equals(WorkflowJob.Status.SUCCEEDED) || actionInfo
// .getStatus().equals
// (WorkflowJob.Status.RUNNING)))
//          {
//              
//              System.out.println("workflow "+actionInfo.getId()+" has action number:
// "+actionInfo.getRun());
//              if(actionInfo.getRun()==maxNumberOfRetries)
//              {
//                  workflowMap.put(actionInfo.getId(),true);
//              }
//              else
//              {
//                  Assert.assertTrue(actionInfo.getRun()<maxNumberOfRetries,
// "The workflow exceeded the max number of
// retries specified for it!!!!");
//                  workflowMap.put(actionInfo.getId(),false);
//              }
//              
//          }
//          else if(actionInfo.getStatus().equals(WorkflowJob.Status.SUCCEEDED))
//          {
//              workflowMap.put(actionInfo.getId(),true);
//          }
//          }
//        }
//        
//        //now to check each of these:
//        
//        //first make sure that the map has all the entries for the coordinator:
//        if(workflowMap.size()!=numberOfRelevantWorkflows)
//        {
//            return false;
//        }
//        else
//        {
//           boolean result=true;
//           
//           for(String key:workflowMap.keySet())
//           {
//               result&=workflowMap.get(key);
//           }
//           
//           return result;
//        }
//    }        
//    
//    @DataProvider(name="DP")
//    public Object[][] getData() throws Exception
//    {
//    	return Util.readBundles("src/test/resources/LateDataBundles");
//    }    
//    
//    private String getRunConfInputString(String runConf) throws Exception
//    {
//        Configuration conf=new Configuration(false);
//        conf.addResource(new ByteArrayInputStream(runConf.getBytes()));
//        return conf.get("ivoryInPaths");
//        
//    }
//    
//    
//	private int getAllRelevantWorkflows(PrismHelper prismHelper,String bundleId,
// String insertionFolder) throws Exception
//	{
//                int i=0;
//                
//		
//                XOozieClient oozieClient=new XOozieClient(prismHelper.getProcessHelper()
// .getOozieURL());
//		BundleJob bundleJob = oozieClient.getBundleJobInfo(bundleId);
//    	
//		DateTimeFormatter formatter=DateTimeFormat.forPattern("yyyy/MM/dd/HH/mm");
//		List<String> actualNominalTimes=new ArrayList<String>();
//		
//		for(CoordinatorJob job:bundleJob.getCoordinators())
//		{
//			if(job.getAppName().contains("DEFAULT"))
//			{
//				
//				CoordinatorJob coordJob=oozieClient.getCoordJobInfo(job.getId());
//                                
//                                
//				for(CoordinatorAction action:coordJob.getActions())
//				{
//                                        CoordinatorAction actionMan=oozieClient
// .getCoordActionInfo(action.getId());
//                                        
//                                        if(!(actionMan.getStatus().equals(CoordinatorAction
// .Status.WAITING) ||
// actionMan.getStatus().equals(CoordinatorAction.Status.READY) || actionMan.getStatus().equals
// (CoordinatorAction
// .Status.SUBMITTED) ))
//                                        {
//                                            if(getRunConfInputString(actionMan.getRunConf())
// .contains
// (insertionFolder))
//                                            {
//                                            
//                                            
//                                            i++; 
//                                            }
//                                        }
//				}
//			}
//		}
//                
//                return i;
//        }
//    
//}
//
