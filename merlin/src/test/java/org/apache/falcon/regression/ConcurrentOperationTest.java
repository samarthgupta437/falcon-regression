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
//import com.inmobi.qa.airavatqa.core.Brother;
//import com.inmobi.qa.airavatqa.core.Bundle;
//import com.inmobi.qa.airavatqa.core.ENTITY_TYPE;
//import com.inmobi.qa.airavatqa.core.EntityHelperFactory;
//import com.inmobi.qa.airavatqa.core.ServiceResponse;
//import com.inmobi.qa.airavatqa.core.Util;
//import com.inmobi.qa.airavatqa.core.Util.URLS;
//import com.inmobi.qa.airavatqa.interfaces.entity.IEntityManagerHelper;
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.LinkedHashSet;
//import org.testng.Assert;
//import org.testng.annotations.DataProvider;
//import org.testng.annotations.Test;
//import org.testng.log4testng.Logger;
//
//public class ConcurrentOperationTest {
//    
//    IEntityManagerHelper clusterHelper=EntityHelperFactory.getEntityHelper(ENTITY_TYPE.CLUSTER);
//    IEntityManagerHelper feedHelper=EntityHelperFactory.getEntityHelper(ENTITY_TYPE.DATA);
//    IEntityManagerHelper processHelper=EntityHelperFactory.getEntityHelper(ENTITY_TYPE.PROCESS);
//    Logger logger=Logger.getLogger(this.getClass());
//    
//    ArrayList<ServiceResponse> serviceResponse; 
//    
//    @Test(groups = {"sanity","0.1","0.2"},dataProvider="DP",dataProviderClass=Bundle.class )
//    public void concurrentSubmitSameProcessTest(Bundle bundle) throws Exception
//    {
//        bundle.generateUniqueBundle();
//        
//        //submit cluster and feed
//        Assert.assertEquals(Util.parseResponse(clusterHelper.submitEntity(URLS.SUBMIT_URL,
// bundle.getClusterData())).getStatusCode(),200);
//        
//        for(String feed:bundle.getDataSets())
//        {
//           Assert.assertEquals(Util.parseResponse(feedHelper.submitEntity(URLS.SUBMIT_URL,
// feed)).getStatusCode(),
// 200);
//        }
//        
//        //now submit the same process 
//        ThreadGroup brotherGrimm=new ThreadGroup("mixed");
//        
//        Brother brothers[]=new Brother[10];
//        
//        for(int i=1;i<=brothers.length;i++)
//        {
//            brothers[i-1]=new Brother("brother"+i,"submit", ENTITY_TYPE.PROCESS, brotherGrimm,
// bundle.getProcessData(),URLS.SUBMIT_URL);
//            
//            
//        }
//        
//        for(Brother brother:brothers)
//        {
//           brother.start();
//                  
//        }
//            
//        Thread.sleep(60000);
//        
//        
//        //now to check for operation's success :|
//        
//        LinkedHashSet<Integer> statusCodes=new LinkedHashSet<Integer>();
//        ArrayList<String> statusMessages=new ArrayList<String>();
//        
//        for(Brother brother:brothers)
//        {
//           
//           statusCodes.add(Util.parseResponse(brother.getOutput()).getStatusCode());
//           statusMessages.add(Util.parseResponse(brother.getOutput()).getMessage());
//           Assert.assertNotNull(Util.parseResponse(brother.getOutput()).getMessage());
//        }
//        
//        ArrayList<String> processStore=Util.getProcessStoreInfo();
//        Assert.assertEquals(Collections.frequency(processStore,Util.readEntityName(bundle
// .getProcessData())+".xml")
// ,1);
//        
//        Assert.assertEquals(statusCodes.size(),2);
//        Assert.assertEquals(Collections.frequency(statusCodes,200),1,
// "More than 1 200 actually occurred!");
//        Assert.assertEquals(Collections.frequency(statusCodes,400),1,
// "something wrong with the 400 count!");
//        Assert.assertEquals(Collections.frequency(statusMessages,
// "(process) "+Util.readEntityName(bundle
// .getProcessData()) +" already registered with configuration store. Can't be submitted again.
// Try removing before
// submitting."),9);
//        Assert.assertEquals(Util.parseResponse(processHelper.getStatus(URLS.STATUS_URL,
// bundle.getProcessData())).getMessage(),"NOT_SCHEDULED");
//        
//        
//    }
//    
//    @Test(groups = {"0.1","0.2"},dataProvider="DP",dataProviderClass=Bundle.class )
//    public void concurrentScheduleOnSameProcess(Bundle bundle) throws Exception
//    {
//        bundle.submitBundle();
//        
//        ThreadGroup brotherGrimm=new ThreadGroup("mixed");
//        
//        Brother brothers[]=new Brother[10];
//        
//        
//        for(int i=1;i<=brothers.length;i++)
//        {
//            brothers[i-1]=new Brother("brother"+i,"schedule", ENTITY_TYPE.PROCESS, brotherGrimm,
// bundle.getProcessData(),URLS.SCHEDULE_URL);
//            
//            
//        }
//        
//        for(Brother brother:brothers)
//        {
//           brother.start();
//                  
//        }
//            
//        Thread.sleep(60000);
//        
//        ArrayList<Integer> statusCodes=new ArrayList<Integer>();
//        ArrayList<String> statusMessages=new ArrayList<String>();
//        
//        for(Brother brother:brothers)
//        {
//            statusCodes.add(Util.parseResponse(brother.getOutput()).getStatusCode());
//            statusMessages.add(Util.parseResponse(brother.getOutput()).getMessage());
//        }
//        
//        Assert.assertEquals(Collections.frequency(statusCodes,200),1);
//        Assert.assertEquals(Collections.frequency(statusCodes,400),9);
//        
//        Assert.assertTrue(Util.getOozieJobStatus(Util.readEntityName(bundle.getProcessData()),
// "RUNNING").get(0).contains("RUNNING"));
//        Assert.assertEquals(Util.parseResponse(processHelper.getStatus(URLS.STATUS_URL,
// bundle.getProcessData())).getMessage(),"ACTIVE");
//    }
//  
//
//    
//    @Test(groups = {"0.1","0.2"},dataProvider="DP",dataProviderClass=Bundle.class )
//    public void concurrentScheduleeDifferentProcessTest(Bundle bundle) throws Exception
//    {
//        
//        bundle.generateUniqueBundle();
//        
//        //submit cluster and feed
//        Assert.assertEquals(Util.parseResponse(clusterHelper.submitEntity(URLS.SUBMIT_URL,
// bundle.getClusterData())).getStatusCode(),200);
//        
//        for(String feed:bundle.getDataSets())
//        {
//           Assert.assertEquals(Util.parseResponse(feedHelper.submitEntity(URLS.SUBMIT_URL,
// feed)).getStatusCode(),
// 200);
//        }
//        
//        //now to create and submit 10 different processes
//        String[] processSet=createMultipleProcesses(bundle.getProcessData());
//        for(String process:processSet)
//        {
//           Assert.assertEquals(Util.parseResponse(processHelper.submitEntity(URLS.SUBMIT_URL,
// process)).getStatusCode(),200,"process was not submitted cleanly");
//        }
//  
//        ThreadGroup brotherGrimm=new ThreadGroup("mixed");
//        
//        Brother brothers[]=new Brother[10];
//        
//        
//        for(int i=1;i<=brothers.length;i++)
//        {
//            brothers[i-1]=new Brother("brother"+i,"schedule", ENTITY_TYPE.PROCESS,
// brotherGrimm,processSet[i-1],
// URLS.SCHEDULE_URL);
//      
//        }
//        
//        for(Brother brother:brothers)
//        {
//           brother.start();
//                  
//        }
//            
//        Thread.sleep(30000);
//        
//        
//        //now to check for operation's success :|
//        
//        ArrayList<Integer> statusCodes=new ArrayList<Integer>();
//        ArrayList<String> statusMessages=new ArrayList<String>();
//        
//        for(Brother brother:brothers)
//        {
//           
//           statusCodes.add(Util.parseResponse(brother.getOutput()).getStatusCode());
//           statusMessages.add(Util.parseResponse(brother.getOutput()).getMessage());
//           Assert.assertNotNull(Util.parseResponse(brother.getOutput()).getMessage());
//           Assert.assertTrue(Util.getOozieJobStatus(Util.readEntityName(brother.getData()),
// "RUNNING").get(0).contains("RUNNING"),"process is not running!");
//           Assert.assertEquals(Util.parseResponse(processHelper.getStatus(URLS.STATUS_URL,
// brother.getData())).getMessage(),"ACTIVE");
//
//        }
//        
//        
//        
//        
//        Assert.assertEquals(statusCodes.size(),10);
//        Assert.assertEquals(Collections.frequency(statusCodes,200),10,
// "More than 1 200 actually occurred!");
//        Assert.assertEquals(Collections.frequency(statusCodes,400),0,
// "something wrong with the 400 count!");
//        
//        
//    }  
//    
//    
//    
//    @Test(groups = {"0.1","0.2"},dataProvider="DP",dataProviderClass=Bundle.class )
//    public void concurrentSuspendOnSameProcess(Bundle bundle) throws Exception
//    {
//        bundle.submitAndScheduleBundle();
//        
//        ThreadGroup brotherGrimm=new ThreadGroup("mixed");
//        
//        Brother brothers[]=new Brother[10];
//        
//        
//        for(int i=1;i<=brothers.length;i++)
//        {
//            brothers[i-1]=new Brother("brother"+i,"suspend", ENTITY_TYPE.PROCESS, brotherGrimm,
// bundle.getProcessData(),URLS.SUSPEND_URL);
//            
//            
//        }
//        
//        for(Brother brother:brothers)
//        {
//           brother.start();
//                  
//        }
//            
//        Thread.sleep(25000);
//        
//        ArrayList<Integer> statusCodes=new ArrayList<Integer>();
//        ArrayList<String> statusMessages=new ArrayList<String>();
//        
//        for(Brother brother:brothers)
//        {
//            statusCodes.add(Util.parseResponse(brother.getOutput()).getStatusCode());
//            statusMessages.add(Util.parseResponse(brother.getOutput()).getMessage());
//        }
//        
//        Assert.assertEquals(Collections.frequency(statusCodes,200),10);
//        
//        Assert.assertTrue(Util.getOozieJobStatus(Util.readEntityName(bundle.getProcessData()),
// "SUSPENDED").get(0).contains("SUSPENDED"));
//        Assert.assertEquals(Util.parseResponse(processHelper.getStatus(URLS.STATUS_URL,
// bundle.getProcessData())).getMessage(),"SUSPENDED");
//    }
//    
//    
//    @Test(groups = {"0.1","0.2"},dataProvider="DP",dataProviderClass=Bundle.class )
//    public void concurrentSuspendDifferentProcessTest(Bundle bundle) throws Exception
//    {
//        
//        bundle.generateUniqueBundle();
//        
//        //submit cluster and feed
//        Assert.assertEquals(Util.parseResponse(clusterHelper.submitEntity(URLS.SUBMIT_URL,
// bundle.getClusterData())).getStatusCode(),200);
//        
//        for(String feed:bundle.getDataSets())
//        {
//           Assert.assertEquals(Util.parseResponse(feedHelper.submitEntity(URLS.SUBMIT_URL,
// feed)).getStatusCode(),
// 200);
//        }
//        
//        //now to create and submit 10 different processes
//        String[] processSet=createMultipleProcesses(bundle.getProcessData());
//        for(String process:processSet)
//        {
//           Assert.assertEquals(Util.parseResponse(processHelper.submitEntity(URLS.SUBMIT_URL,
// process)).getStatusCode(),200,"process was not submitted cleanly");
//           Assert.assertEquals(Util.parseResponse(processHelper.schedule(URLS.SCHEDULE_URL,
// process)).getStatusCode(),200,"process was not scheduled cleanly");
//        }
//  
//        ThreadGroup brotherGrimm=new ThreadGroup("mixed");
//        
//        Brother brothers[]=new Brother[10];
//        
//        
//        for(int i=1;i<=brothers.length;i++)
//        {
//            brothers[i-1]=new Brother("brother"+i,"suspend", ENTITY_TYPE.PROCESS, brotherGrimm,
// processSet[i-1],
// URLS.SUSPEND_URL);
//      
//        }
//        
//        for(Brother brother:brothers)
//        {
//           brother.start();
//                  
//        }
//            
//        Thread.sleep(30000);
//        
//        
//        //now to check for operation's success :|
//        
//        ArrayList<Integer> statusCodes=new ArrayList<Integer>();
//        ArrayList<String> statusMessages=new ArrayList<String>();
//        
//        for(Brother brother:brothers)
//        {
//           
//           statusCodes.add(Util.parseResponse(brother.getOutput()).getStatusCode());
//           statusMessages.add(Util.parseResponse(brother.getOutput()).getMessage());
//           //Assert.assertNotNull(Util.parseResponse(brother.getOutput()).getMessage());
//           Assert.assertTrue(Util.getOozieJobStatus(Util.readEntityName(brother.getData()),
// "SUSPENDED").get(0).contains("SUSPENDED"),"process is still running!");
//           Assert.assertEquals(Util.parseResponse(processHelper.getStatus(URLS.STATUS_URL,
// brother.getData())).getMessage(),"SUSPENDED");
//
//        }
//        
//        
//        
//        
//        Assert.assertEquals(statusCodes.size(),10);
//        Assert.assertEquals(Collections.frequency(statusCodes,200),10,
// "More than 1 200 actually occurred!");
//        Assert.assertEquals(Collections.frequency(statusCodes,400),0,
// "something wrong with the 400 count!");
//        
//        
//    } 
//    
//    
//    @Test(groups = {"0.1","0.2"},dataProvider="DP",dataProviderClass=Bundle.class )
//    public void concurrentResumeOnSameProcess(Bundle bundle) throws Exception
//    {
//        bundle.submitAndScheduleBundle();
//        Assert.assertEquals(Util.parseResponse(processHelper.suspend(URLS.SUSPEND_URL,
// bundle.getProcessData())).getStatusCode(),200);
//        
//        ThreadGroup brotherGrimm=new ThreadGroup("mixed");
//        
//        Brother brothers[]=new Brother[10];
//        
//        
//        for(int i=1;i<=brothers.length;i++)
//        {
//            brothers[i-1]=new Brother("brother"+i,"resume", ENTITY_TYPE.PROCESS, brotherGrimm,
// bundle.getProcessData(),URLS.RESUME_URL);
//            
//            
//        }
//        
//        for(Brother brother:brothers)
//        {
//           brother.start();
//                  
//        }
//            
//        Thread.sleep(30000);
//        
//        ArrayList<Integer> statusCodes=new ArrayList<Integer>();
//        ArrayList<String> statusMessages=new ArrayList<String>();
//        
//        for(Brother brother:brothers)
//        {
//            statusCodes.add(Util.parseResponse(brother.getOutput()).getStatusCode());
//            statusMessages.add(Util.parseResponse(brother.getOutput()).getMessage());
//        }
//        
//        Assert.assertEquals(Collections.frequency(statusCodes,200),10);
//        
//        Assert.assertTrue(Util.getOozieJobStatus(Util.readEntityName(bundle.getProcessData()),
// "RUNNING").get(0).contains("RUNNING"));
//        Assert.assertEquals(Util.parseResponse(processHelper.getStatus(URLS.STATUS_URL,
// bundle.getProcessData())).getMessage(),"ACTIVE");
//    }
//    
//    
//    @Test(groups = {"0.1","0.2"},dataProvider="DP",dataProviderClass=Bundle.class )
//    public void concurrentResumeDifferentProcessTest(Bundle bundle) throws Exception
//    {
//        
//        bundle.generateUniqueBundle();
//        
//        //submit cluster and feed
//        Assert.assertEquals(Util.parseResponse(clusterHelper.submitEntity(URLS.SUBMIT_URL,
// bundle.getClusterData())).getStatusCode(),200);
//        
//        for(String feed:bundle.getDataSets())
//        {
//           Assert.assertEquals(Util.parseResponse(feedHelper.submitEntity(URLS.SUBMIT_URL,
// feed)).getStatusCode(),
// 200);
//        }
//        
//        //now to create and submit 10 different processes
//        String[] processSet=createMultipleProcesses(bundle.getProcessData());
//        for(String process:processSet)
//        {
//           Assert.assertEquals(Util.parseResponse(processHelper.submitEntity(URLS.SUBMIT_URL,
// process)).getStatusCode(),200,"process was not submitted cleanly");
//           Assert.assertEquals(Util.parseResponse(processHelper.schedule(URLS.SCHEDULE_URL,
// process)).getStatusCode(),200,"process was not scheduled cleanly");
//           Assert.assertEquals(Util.parseResponse(processHelper.schedule(URLS.SUSPEND_URL,
// process)).getStatusCode(),200,"process was not suspended cleanly");
//        }
//  
//        ThreadGroup brotherGrimm=new ThreadGroup("mixed");
//        
//        Brother brothers[]=new Brother[10];
//        
//        
//        for(int i=1;i<=brothers.length;i++)
//        {
//            brothers[i-1]=new Brother("brother"+i,"resume", ENTITY_TYPE.PROCESS, brotherGrimm,
// processSet[i-1],
// URLS.RESUME_URL);
//      
//        }
//        
//        for(Brother brother:brothers)
//        {
//           brother.start();
//                  
//        }
//            
//        Thread.sleep(30000);
//        
//        
//        //now to check for operation's success :|
//        
//        ArrayList<Integer> statusCodes=new ArrayList<Integer>();
//        ArrayList<String> statusMessages=new ArrayList<String>();
//        
//        for(Brother brother:brothers)
//        {
//           
//           statusCodes.add(Util.parseResponse(brother.getOutput()).getStatusCode());
//           statusMessages.add(Util.parseResponse(brother.getOutput()).getMessage());
//           //Assert.assertNotNull(Util.parseResponse(brother.getOutput()).getMessage());
//           Assert.assertTrue(Util.getOozieJobStatus(Util.readEntityName(brother.getData()),
// "RUNNING").get(0).contains("RUNNING"),"process is still running!");
//           Assert.assertEquals(Util.parseResponse(processHelper.getStatus(URLS.STATUS_URL,
// brother.getData())).getMessage(),"ACTIVE");
//
//        }
//        
//        
//        
//        
//        Assert.assertEquals(statusCodes.size(),10);
//        Assert.assertEquals(Collections.frequency(statusCodes,200),10,
// "More than 1 200 actually occurred!");
//        Assert.assertEquals(Collections.frequency(statusCodes,400),0,
// "something wrong with the 400 count!");
//        
//        
//    } 
//    
//    
//    @Test(groups = {"0.1","0.2"},dataProvider="DP",dataProviderClass=Bundle.class )
//    public void concurrentSubmitAndScheduleSameProcessTest(Bundle bundle) throws Exception
//    {
//        bundle.generateUniqueBundle();
//        
//        //submit cluster and feed
//        Assert.assertEquals(Util.parseResponse(clusterHelper.submitEntity(URLS.SUBMIT_URL,
// bundle.getClusterData())).getStatusCode(),200);
//        
//        for(String feed:bundle.getDataSets())
//        {
//           Assert.assertEquals(Util.parseResponse(feedHelper.submitEntity(URLS.SUBMIT_URL,
// feed)).getStatusCode(),
// 200);
//        }
//        
//        //now submit the same process 
//        ThreadGroup brotherGrimm=new ThreadGroup("mixed");
//        
//        Brother brothers[]=new Brother[10];
//        
//        for(int i=1;i<=brothers.length;i++)
//        {
//            brothers[i-1]=new Brother("brother"+i,"SnS", ENTITY_TYPE.PROCESS, brotherGrimm,
// bundle.getProcessData(),
// URLS.SUBMIT_AND_SCHEDULE_URL);
//            
//            
//        }
//        
//        for(Brother brother:brothers)
//        {
//           brother.start();
//                  
//        }
//            
//        Thread.sleep(60000);
//        
//        
//        //now to check for operation's success :|
//        
//        LinkedHashSet<Integer> statusCodes=new LinkedHashSet<Integer>();
//        ArrayList<String> statusMessages=new ArrayList<String>();
//        
//        for(Brother brother:brothers)
//        {
//           
//           statusCodes.add(Util.parseResponse(brother.getOutput()).getStatusCode());
//           statusMessages.add(Util.parseResponse(brother.getOutput()).getMessage());
//           Assert.assertNotNull(Util.parseResponse(brother.getOutput()).getMessage());
//        }
//        
//        ArrayList<String> processStore=Util.getProcessStoreInfo();
//        Assert.assertEquals(Collections.frequency(processStore,Util.readEntityName(bundle
// .getProcessData())+".xml")
// ,1);
//        
//        Assert.assertEquals(statusCodes.size(),2);
//        Assert.assertEquals(Collections.frequency(statusCodes,200),1,
// "More than 1 200 actually occurred!");
//        Assert.assertEquals(Collections.frequency(statusCodes,400),1,
// "something wrong with the 400 count!");
//        Assert.assertEquals(Collections.frequency(statusMessages,
// "(process) "+Util.readEntityName(bundle
// .getProcessData()) +" already registered with configuration store. Can't be submitted again.
// Try removing before
// submitting."),9);
//        Assert.assertEquals(Util.parseResponse(processHelper.getStatus(URLS.STATUS_URL,
// bundle.getProcessData())).getMessage(),"ACTIVE");
//        Assert.assertTrue(Util.getOozieJobStatus(Util.readEntityName(bundle.getProcessData()),
// "RUNNING").get(0).contains("RUNNING"),"process was not scheduled correctly!");
//        
//        
//    }
// 
//    @Test(groups = {"0.1","0.2"},dataProvider="DP",dataProviderClass=Bundle.class )
//    public void concurrentSubmitAndScheduleDifferentProcessTest(Bundle bundle) throws Exception
//    {
//        bundle.generateUniqueBundle();
//        
//        //submit cluster and feed
//        Assert.assertEquals(Util.parseResponse(clusterHelper.submitEntity(URLS.SUBMIT_URL,
// bundle.getClusterData())).getStatusCode(),200);
//        
//        for(String feed:bundle.getDataSets())
//        {
//           Assert.assertEquals(Util.parseResponse(feedHelper.submitEntity(URLS.SUBMIT_URL,
// feed)).getStatusCode(),
// 200);
//        }
//        
//        //now submit the same process 
//        ThreadGroup brotherGrimm=new ThreadGroup("mixed");
//        
//        Brother brothers[]=new Brother[10];
//        
//        for(int i=1;i<=brothers.length;i++)
//        {
//            brothers[i-1]=new Brother("brother"+i,"SnS", ENTITY_TYPE.PROCESS, brotherGrimm,
// Util.generateUniqueProcessEntity(bundle.getProcessData()),URLS.SUBMIT_AND_SCHEDULE_URL);
//            
//            
//        }
//        
//        for(Brother brother:brothers)
//        {
//           brother.start();
//                  
//        }
//            
//        Thread.sleep(60000);
//        
//        
//        //now to check for operation's success :|
//        
//        LinkedHashSet<Integer> statusCodes=new LinkedHashSet<Integer>();
//        ArrayList<String> statusMessages=new ArrayList<String>();
//        
//        for(Brother brother:brothers)
//        {
//           
//           statusCodes.add(Util.parseResponse(brother.getOutput()).getStatusCode());
//           statusMessages.add(Util.parseResponse(brother.getOutput()).getMessage());
//           Assert.assertNotNull(Util.parseResponse(brother.getOutput()).getMessage());
//           
//           //verification happens here now
//           ArrayList<String> processStore=Util.getProcessStoreInfo();
//           Assert.assertEquals(Collections.frequency(processStore,
// Util.readEntityName(brother.getData())+".xml"),1);
//           Assert.assertTrue(Util.getOozieJobStatus(Util.readEntityName(brother.getData()),
// "RUNNING").get(0).contains("RUNNING"),"process was not scheduled correctly!");
//           Assert.assertEquals(Util.parseResponse(processHelper.getStatus(URLS.STATUS_URL,
// brother.getData())).getMessage(),"ACTIVE");
//        }
//        
//        
//        
//        
//        Assert.assertEquals(statusCodes.size(),1);
//        Assert.assertEquals(Collections.frequency(statusCodes,200),1,
// "More than 1 200 actually occurred!");
//        Assert.assertEquals(Collections.frequency(statusCodes,400),0,
// "something wrong with the 400 count!");
//        Assert.assertEquals(Collections.frequency(statusMessages,
// "PROCESS: "+Util.readEntityName(bundle
// .getProcessData()) +" already registered with configuration store. Can't be submitted again.
// Try removing before
// submitting."),0);
//        
//        
//    }    
//    
//    
//    
//    private String[] createMultipleProcesses(String process) throws Exception
//    {
//        String originalProcess=new String(process);
//        
//        ArrayList<String> processSet=new ArrayList<String>();
//        
//        for(int i=0;i<10;i++)
//        {
//            process=new String(originalProcess);
//            processSet.add(Util.generateUniqueProcessEntity(process));
//            
//        }
//        
//        return processSet.toArray(new String[processSet.size()]);
//    }
//    
//    
//    @Test(groups = {"0.1","0.2"},dataProvider="DP",dataProviderClass=Bundle.class )
//    public void concurrentSubmitDifferentProcessTest(Bundle bundle) throws Exception
//    {
//        bundle.generateUniqueBundle();
//        
//        //submit cluster and feed
//        Assert.assertEquals(Util.parseResponse(clusterHelper.submitEntity(URLS.SUBMIT_URL,
// bundle.getClusterData())).getStatusCode(),200);
//        
//        for(String feed:bundle.getDataSets())
//        {
//           Assert.assertEquals(Util.parseResponse(feedHelper.submitEntity(URLS.SUBMIT_URL,
// feed)).getStatusCode(),
// 200);
//        }
//        
//        //now submit the same process 
//        ThreadGroup brotherGrimm=new ThreadGroup("mixed");
//        
//        Brother brothers[]=new Brother[10];
//        
//        for(int i=1;i<=brothers.length;i++)
//        {
//            brothers[i-1]=new Brother("brother"+i,"submit", ENTITY_TYPE.PROCESS, brotherGrimm,
// Util.generateUniqueProcessEntity(bundle.getProcessData()),URLS.SUBMIT_URL);
//            
//            
//        }
//        
//        for(Brother brother:brothers)
//        {
//           brother.start();
//                  
//        }
//            
//        Thread.sleep(60000);
//        
//        
//        //now to check for operation's success :|
//        
//        LinkedHashSet<Integer> statusCodes=new LinkedHashSet<Integer>();
//        ArrayList<String> statusMessages=new ArrayList<String>();
//        
//        for(Brother brother:brothers)
//        {
//           
//           statusCodes.add(Util.parseResponse(brother.getOutput()).getStatusCode());
//           statusMessages.add(Util.parseResponse(brother.getOutput()).getMessage());
//           Assert.assertNotNull(Util.parseResponse(brother.getOutput()).getMessage());
//           
//           //verification happens here now
//           ArrayList<String> processStore=Util.getProcessStoreInfo();
//           Assert.assertEquals(Collections.frequency(processStore,
// Util.readEntityName(brother.getData())+".xml"),1);
//           Assert.assertEquals(Util.parseResponse(processHelper.getStatus(URLS.STATUS_URL,
// brother.getData())).getMessage(),"NOT_SCHEDULED");
//        }
//        
//        
//        
//        
//        Assert.assertEquals(statusCodes.size(),1);
//        Assert.assertEquals(Collections.frequency(statusCodes,200),1,
// "More than 1 200 actually occurred!");
//        Assert.assertEquals(Collections.frequency(statusCodes,400),0,
// "something wrong with the 400 count!");
//        Assert.assertEquals(Collections.frequency(statusMessages,
// "PROCESS: "+Util.readEntityName(bundle
// .getProcessData()) +" already registered with configuration store. Can't be submitted again.
// Try removing before
// submitting."),0);
//        
//        
//    }
//    
//    
//    @Test(groups = {"0.1","0.2"},dataProvider="DP",dataProviderClass=Bundle.class )
//    public void concurrentDeleteSameSubmittedProcessTest(Bundle bundle) throws Exception
//    {
//        bundle.submitBundle();
//        
//        //now submit the same process 
//        ThreadGroup brotherGrimm=new ThreadGroup("mixed");
//        
//        Brother brothers[]=new Brother[10];
//        
//        for(int i=1;i<=brothers.length;i++)
//        {
//            brothers[i-1]=new Brother("brother"+i,"delete", ENTITY_TYPE.PROCESS, brotherGrimm,
// bundle.getProcessData(),URLS.DELETE_URL);
//            
//            
//        }
//        
//        ArrayList<String> initialProcessArchiveStore=Util.getArchiveStoreInfo();
//        
//        for(Brother brother:brothers)
//        {
//           brother.start();
//                  
//        }
//            
//        Thread.sleep(25000);
//        
//        
//        //now to check for operation's success :|
//        
//        LinkedHashSet<Integer> statusCodes=new LinkedHashSet<Integer>();
//        ArrayList<String> statusMessages=new ArrayList<String>();
//        
//        for(Brother brother:brothers)
//        {
//           
//           statusCodes.add(Util.parseResponse(brother.getOutput()).getStatusCode());
//           statusMessages.add(Util.parseResponse(brother.getOutput()).getMessage());
//           Assert.assertNotNull(Util.parseResponse(brother.getOutput()).getMessage());
//        }
//        
//        
//        Assert.assertEquals(statusCodes.size(),2);
//        Assert.assertEquals(Collections.frequency(statusCodes,200),1,
// "More than 1 200 actually occurred!");
//        Assert.assertEquals(Collections.frequency(statusCodes,400),1,
// "something wrong with the 400 count!");
//        //Assert.assertEquals(Collections.frequency(statusMessages,
// "PROCESS: "+Util.readEntityName(bundle
// .getProcessData()) +" already registered with configuration store. Can't be submitted again.
// Try removing before
// submitting."),9);
//        
//        ArrayList<String> finalArchiveStoreInfo=Util.getArchiveStoreInfo();
//        
//        Assert.assertEquals(finalArchiveStoreInfo.size()-initialProcessArchiveStore.size(),1,
// "more than 1 file was dumped in archive store!");
//        Assert.assertEquals(Util.parseResponse(processHelper.getStatus(URLS.STATUS_URL,
// bundle.getProcessData())).getMessage(),"NOT_FOUND");
//    }
//    
//    @Test(groups = {"0.1","0.2"},dataProvider="DP",dataProviderClass=Bundle.class )
//    public void concurrentDeleteDifferentProcessTest(Bundle bundle) throws Exception
//    {
//        
//        bundle.generateUniqueBundle();
//        
//        //submit cluster and feed
//        Assert.assertEquals(Util.parseResponse(clusterHelper.submitEntity(URLS.SUBMIT_URL,
// bundle.getClusterData())).getStatusCode(),200);
//        
//        for(String feed:bundle.getDataSets())
//        {
//           Assert.assertEquals(Util.parseResponse(feedHelper.submitEntity(URLS.SUBMIT_URL,
// feed)).getStatusCode(),
// 200);
//        }
//        
//        //now to create and submit 10 different processes
//        String[] processSet=createMultipleProcesses(bundle.getProcessData());
//        for(String process:processSet)
//        {
//           Assert.assertEquals(Util.parseResponse(processHelper.submitEntity(URLS.SUBMIT_URL,
// process)).getStatusCode(),200,"process was not submitted cleanly");
//        }
//  
//        ThreadGroup brotherGrimm=new ThreadGroup("mixed");
//        
//        Brother brothers[]=new Brother[10];
//        
//        
//        for(int i=1;i<=brothers.length;i++)
//        {
//            brothers[i-1]=new Brother("brother"+i,"delete", ENTITY_TYPE.PROCESS, brotherGrimm,
// processSet[i-1],
// URLS.DELETE_URL);
//            
//            
//        }
//        
//        for(Brother brother:brothers)
//        {
//           brother.start();
//                  
//        }
//            
//        Thread.sleep(25000);
//        
//        
//        //now to check for operation's success :|
//        
//        ArrayList<Integer> statusCodes=new ArrayList<Integer>();
//        ArrayList<String> statusMessages=new ArrayList<String>();
//        
//        for(Brother brother:brothers)
//        {
//           
//           statusCodes.add(Util.parseResponse(brother.getOutput()).getStatusCode());
//           statusMessages.add(Util.parseResponse(brother.getOutput()).getMessage());
//           Assert.assertNotNull(Util.parseResponse(brother.getOutput()).getMessage());
//           Assert.assertEquals(Util.parseResponse(processHelper.getStatus(URLS.STATUS_URL,
// brother.getData())).getMessage(),"NOT_FOUND");
//
//        }
//        
//        
//        
//        
//        Assert.assertEquals(statusCodes.size(),10);
//        Assert.assertEquals(Collections.frequency(statusCodes,200),10,
// "More than 1 200 actually occurred!");
//        Assert.assertEquals(Collections.frequency(statusCodes,400),0,
// "something wrong with the 400 count!");
//        
//        
//    } 
//    
//
//
//    
//    
//    
//}
