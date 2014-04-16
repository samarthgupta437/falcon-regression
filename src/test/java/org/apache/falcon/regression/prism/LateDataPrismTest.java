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

package org.apache.falcon.regression.prism;
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
/*package com.inmobi.qa.airavatqa.prism;



import java.io.ByteArrayInputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.XOozieClient;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Minutes;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.Assert;
import org.testng.TestNGException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.inmobi.qa.airavatqa.mq.Consumer;
import com.inmobi.qa.ivory.bundle.Bundle;
import com.inmobi.qa.ivory.generated.process.PolicyType;
import com.inmobi.qa.ivory.helpers.ColoHelper;
import com.inmobi.qa.ivory.helpers.PrismHelper;
import com.inmobi.qa.ivory.util.Util;
import com.inmobi.qa.ivory.util.Util.URLS;


public class LateDataPrismTest {
    
	
	
	@BeforeMethod(alwaysRun=true)
	public void testName(Method method)
	{
		Util.print("test name: "+method.getName());
	}
	
	
    PrismHelper prismHelper=new PrismHelper("prism.properties");
    ColoHelper UA3ColoHelper=new ColoHelper("gs1001.config.properties");
    DateTimeFormatter formatter=DateTimeFormat.forPattern("yyyy/MM/dd/HH/mm");
    
    @Test(groups = {"0.2"},dataProvider="DP",priority=-20)
    public void testLateDataWithCutOffInFutureAndExponentialPolicyWithRerunBeyondLimit(Bundle
    bundle) throws Exception
    {
        String delay ="10";
        bundle=new Bundle(bundle,UA3ColoHelper);
        
            String feed=Util.setFeedPathValue(Util.getInputFeedFromBundle(bundle),
            "/lateDataTest/testFolders/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            feed=Util.insertLateFeedValue(feed,delay,"minutes");
            bundle.getDataSets().remove(Util.getInputFeedFromBundle(bundle));
            bundle.getDataSets().add(feed);
            Util.HDFSCleanup(UA3ColoHelper,"lateDataTest/testFolders/");
            Util.lateDataReplenish(UA3ColoHelper,20,0,0);
            List<String> initialData=Util.getHadoopLateData(UA3ColoHelper,
            Util.getInputFeedFromBundle(bundle));

            bundle.generateUniqueBundle();
            
            Util.print("process after generating unique bundle:  "+ bundle.getProcessData());

            
            for(String cluster:bundle.getClusters())
            {
                    System.out.println(cluster);
                    Util.assertSucceeded(prismHelper.getClusterHelper().submitEntity(URLS
                    .SUBMIT_URL,cluster));
            }  
            
            for(String data:bundle.getDataSets())
            {
                    Util.assertSucceeded(prismHelper.getFeedHelper().submitEntity(URLS
                    .SUBMIT_URL,data));
            }
            
            DateTime startDate=new DateTime(DateTimeZone.UTC).plusMinutes(2);
            DateTime endDate=new DateTime(DateTimeZone.UTC).plusMinutes(3);
            
            Util.print("process before setting validation:  "+ bundle.getProcessData());

            
            bundle.setProcessValidity(startDate,endDate);
            
            Util.print("process 1 :  "+ bundle.getProcessData());

            
            bundle.setProcessLatePolicy(PolicyType.EXP_BACKOFF,"minutes(2)");
            
            Util.print("process 2 :  "+ bundle.getProcessData());

            
            bundle.setProcessWorkflow("/examples/apps/phailFs/workflow.xml");
            
            Util.print("process to be sumitted:  "+ bundle.getProcessData());
            
            Util.assertSucceeded(prismHelper.getProcessHelper().submitEntity(URLS.SUBMIT_URL,
            bundle.getProcessData()));
    	
            System.out.println(bundle.getProcessData());
 
            Util.assertSucceeded(UA3ColoHelper.getProcessHelper().schedule(URLS.SCHEDULE_URL,
            bundle.getProcessData()));
        
            //attach the consumer
            Consumer consumer=new Consumer("IVORY."+Util.readEntityName(bundle.getProcessData()),
            UA3ColoHelper.getClusterHelper().getActiveMQ());
            consumer.start();
        
            //get the bundle ID
            String bundleId=Util.getCoordID(Util.getOozieJobStatus(UA3ColoHelper,
            Util.readEntityName(bundle.getProcessData()),"NONE").get(0));
            String status=Util.getBundleStatus(UA3ColoHelper,bundleId);            
            
            
            ArrayList<DateTime> dates=null;
        
            do {
        
        	dates=Util.getStartTimeForRunningCoordinators(UA3ColoHelper,bundleId);
        	
            }while(dates==null);
            
            
         System.out.println("Start time: "+formatter.print(startDate));
         System.out.println("End time: "+formatter.print(endDate));
         System.out.println("candidate nominal time:"+formatter.print(dates.get(0)));
         DateTime now=dates.get(0);
        
        if(formatter.print(startDate).compareToIgnoreCase(formatter.print(dates.get(0)))>0)
        {
        	now=startDate;
        }
        
        System.out.println("Now:"+formatter.print(now));            
        
        String insertionFolder="";
        DateTime insertionTime=null;
        
        boolean toInsert=true;
        insertionFolder=Util.findFolderBetweenGivenTimeStamps(startDate,
        now.plusMinutes(Integer.parseInt(delay)),
        initialData);
        //insert data after all relevant workflows are over
        System.out.println("insertion folder is: "+insertionFolder);
        
        int expectedsize=0;
        
        for(int i=1;i<=4;i++)
        {
        
            while(!allRelevantWorkflowsAreOver(UA3ColoHelper,bundleId, insertionFolder))
            {
                //System.out.println("waiting for relevant workflows to be over before inserting
                folder
                "+insertionFolder);
                //keep waiting
            	Thread.sleep(20000);
            }
        
            System.out.println("going to insert data at: "+insertionFolder);
            insertionTime=new DateTime(DateTimeZone.UTC);
            System.out.println("insertion time is :"+insertionTime);
            Util.injectMoreData(UA3ColoHelper,insertionFolder,
            "src/test/resources/OozieExampleInputData/lateData");
            initialData=Util.getHadoopLateData(UA3ColoHelper,Util.getInputFeedFromBundle(bundle));
            toInsert=false;
            expectedsize=i;
            Thread.sleep(120000*i);
        }
        
        System.out.println("precautionary sleep commencing....");
        Thread.sleep(180000);
        System.out.println("precautionary sleep ended....");
        
        //now wait till the total size of consumer queue is 2
        int attempts=0;
        while(consumer.getMessageData().size()!=expectedsize+1 )
        {
            if(consumer.getMessageData().size()>expectedsize+1)
            {
                throw new TestNGException("extra data received in queue! some problem is there.
                please check!");
            }
            //keep waiting baby
            Thread.sleep(1000);
            attempts++; System.out.println("attempt number: "+attempts); if(attempts==180) {break;}
        }
        
        //now parse the goddamn consumer data for info
        parseConsumerOutput(UA3ColoHelper,bundle.getProcessObject(),consumer.getMessageData(),
        expectedsize+1,
        insertionFolder,expectedsize+1);
        
        
        consumer.stop();
        
        Util.dumpConsumerData(consumer);      
                    
            
        
        try {
            
        }
        catch(Exception e)
        {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        }
        finally{
            
            
        }
    } 
    */
    
  /*  
    @Test(groups = {"0.2"},dataProvider="DP",priority=-20)
    public void testLateDataWithCutOffInFutureAndBackOffPolicy(Bundle bundle) throws Exception
    {
        String delay ="10";
        bundle=new Bundle(bundle,UA3ColoHelper);
        
            String feed=Util.setFeedPathValue(Util.getInputFeedFromBundle(bundle),
            "/lateDataTest/testFolders/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            feed=Util.insertLateFeedValue(feed,delay,"minutes");
            bundle.getDataSets().remove(Util.getInputFeedFromBundle(bundle));
            bundle.getDataSets().add(feed);
            Util.HDFSCleanup(UA3ColoHelper,"lateDataTest/testFolders/");
            Util.lateDataReplenish(UA3ColoHelper,20,0,0);
            List<String> initialData=Util.getHadoopLateData(UA3ColoHelper,
            Util.getInputFeedFromBundle(bundle));

            bundle.generateUniqueBundle();
            
            for(String cluster:bundle.getClusters())
            {
                    System.out.println(cluster);
                    Util.assertSucceeded(prismHelper.getClusterHelper().submitEntity(URLS
                    .SUBMIT_URL,cluster));
            }  
            
            for(String data:bundle.getDataSets())
            {
                    Util.assertSucceeded(prismHelper.getFeedHelper().submitEntity(URLS
                    .SUBMIT_URL,data));
            }
            
            DateTime startDate=new DateTime(DateTimeZone.UTC).plusMinutes(2);
            DateTime endDate=new DateTime(DateTimeZone.UTC).plusMinutes(3);
            
            bundle.setProcessValidity(startDate,endDate);
            bundle.setProcessLatePolicy(PolicyType.PERIODIC,"minutes(2)");
            bundle.setProcessWorkflow("/examples/apps/phailFs/workflow.xml");
            
            Util.assertSucceeded(prismHelper.getProcessHelper().submitEntity(URLS.SUBMIT_URL,
            bundle.getProcessData()));
    	
            System.out.println(bundle.getProcessData());
 
            Util.assertSucceeded(UA3ColoHelper.getProcessHelper().schedule(URLS.SCHEDULE_URL,
            bundle.getProcessData()));
        
            //attach the consumer
            Consumer consumer=new Consumer("IVORY."+Util.readEntityName(bundle.getProcessData()),
            UA3ColoHelper.getClusterHelper().getActiveMQ());
            consumer.start();
        
            //get the bundle ID
            String bundleId=Util.getCoordID(Util.getOozieJobStatus(UA3ColoHelper,
            Util.readEntityName(bundle.getProcessData()),"NONE").get(0));
            String status=Util.getBundleStatus(UA3ColoHelper,bundleId);            
            
            
            ArrayList<DateTime> dates=null;
        
            do {
        
        	dates=Util.getStartTimeForRunningCoordinators(UA3ColoHelper,bundleId);
        	
            }while(dates==null);
            
            
         System.out.println("Start time: "+formatter.print(startDate));
         System.out.println("End time: "+formatter.print(endDate));
         System.out.println("candidate nominal time:"+formatter.print(dates.get(0)));
         DateTime now=dates.get(0);
        
        if(formatter.print(startDate).compareToIgnoreCase(formatter.print(dates.get(0)))>0)
        {
        	now=startDate;
        }
        
        System.out.println("Now:"+formatter.print(now));            
        
        String insertionFolder="";
        DateTime insertionTime=null;
        
        boolean toInsert=true;
        insertionFolder=Util.findFolderBetweenGivenTimeStamps(startDate,
        now.plusMinutes(Integer.parseInt(delay)),
        initialData);
        //insert data after all relevant workflows are over
        System.out.println("insertion folder is: "+insertionFolder);
        
        int expectedsize=0;
        
        for(int i=1;i<=5;i++)
        {
        
            while(!allRelevantWorkflowsAreOver(UA3ColoHelper,bundleId, insertionFolder))
            {
                //System.out.println("waiting for relevant workflows to be over before inserting
                folder
                "+insertionFolder);
                //keep waiting
            }
        
            System.out.println("going to insert data at: "+insertionFolder);
            insertionTime=new DateTime(DateTimeZone.UTC);
            System.out.println("insertion time is :"+insertionTime);
            Util.injectMoreData(UA3ColoHelper,insertionFolder,
            "src/test/resources/OozieExampleInputData/lateData");
            initialData=Util.getHadoopLateData(UA3ColoHelper,Util.getInputFeedFromBundle(bundle));
            toInsert=false;
            expectedsize=i;
            Thread.sleep(120000);
        }
        
        System.out.println("precautionary sleep commencing....");
        Thread.sleep(180000);
        System.out.println("precautionary sleep ended....");
        
        //now wait till the total size of consumer queue is 2
        int attempts=0;
        while(consumer.getMessageData().size()!=expectedsize+1 )
        {
            if(consumer.getMessageData().size()>expectedsize+1)
            {
                throw new TestNGException("extra data received in queue! some problem is there.
                please check!");
            }
            //keep waiting baby
            Thread.sleep(1000);
            attempts++; System.out.println("attempt number: "+attempts); if(attempts==180) {break;}
        }
        
        //now parse the goddamn consumer data for info
        parseConsumerOutput(UA3ColoHelper,bundle.getProcessObject(),consumer.getMessageData(),
        expectedsize+1,
        insertionFolder,expectedsize+1);
        
        
        consumer.stop();
        
        Util.dumpConsumerData(consumer);      
                    
            
        
        try {
            
        }
        catch(Exception e)
        {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        }
        finally{
            
            
        }
    }
    
 
    @Test(groups = {"0.2"},dataProvider="DP",priority=-20)
    public void testLateDataWithCutOffInFutureAndBackOffPolicyAndAlternateDataInsertion(Bundle
    bundle) throws Exception
    {
        String delay ="10";
        bundle=new Bundle(bundle,UA3ColoHelper);
        
            String feed=Util.setFeedPathValue(Util.getInputFeedFromBundle(bundle),
            "/lateDataTest/testFolders/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            feed=Util.insertLateFeedValue(feed,delay,"minutes");
            bundle.getDataSets().remove(Util.getInputFeedFromBundle(bundle));
            bundle.getDataSets().add(feed);
            Util.HDFSCleanup(UA3ColoHelper,"lateDataTest/testFolders/");
            Util.lateDataReplenish(UA3ColoHelper,20,0,0);
            List<String> initialData=Util.getHadoopLateData(UA3ColoHelper,
            Util.getInputFeedFromBundle(bundle));

            bundle.generateUniqueBundle();
            
            for(String cluster:bundle.getClusters())
            {
                    System.out.println(cluster);
                    Util.assertSucceeded(prismHelper.getClusterHelper().submitEntity(URLS
                    .SUBMIT_URL,cluster));
            }  
            
            for(String data:bundle.getDataSets())
            {
                    Util.assertSucceeded(prismHelper.getFeedHelper().submitEntity(URLS
                    .SUBMIT_URL,data));
            }
            
            DateTime startDate=new DateTime(DateTimeZone.UTC).plusMinutes(2);
            DateTime endDate=new DateTime(DateTimeZone.UTC).plusMinutes(3);
            
            bundle.setProcessValidity(startDate,endDate);
            bundle.setProcessLatePolicy(PolicyType.PERIODIC,"minutes(2)");
            bundle.setProcessWorkflow("/examples/apps/phailFs/workflow.xml");
            
            Util.assertSucceeded(prismHelper.getProcessHelper().submitEntity(URLS.SUBMIT_URL,
            bundle.getProcessData()));
    	
            System.out.println(bundle.getProcessData());
 
            Util.assertSucceeded(UA3ColoHelper.getProcessHelper().schedule(URLS.SCHEDULE_URL,
            bundle.getProcessData()));
        
            //attach the consumer
            Consumer consumer=new Consumer("IVORY."+Util.readEntityName(bundle.getProcessData()),
            UA3ColoHelper.getClusterHelper().getActiveMQ());
            consumer.start();
        
            //get the bundle ID
            String bundleId=Util.getCoordID(Util.getOozieJobStatus(UA3ColoHelper,
            Util.readEntityName(bundle.getProcessData()),"NONE").get(0));
            String status=Util.getBundleStatus(UA3ColoHelper,bundleId);            
            
            
            ArrayList<DateTime> dates=null;
        
            do {
        
        	dates=Util.getStartTimeForRunningCoordinators(UA3ColoHelper,bundleId);
        	
            }while(dates==null);
            
            
         System.out.println("Start time: "+formatter.print(startDate));
         System.out.println("End time: "+formatter.print(endDate));
         System.out.println("candidate nominal time:"+formatter.print(dates.get(0)));
         DateTime now=dates.get(0);
        
        if(formatter.print(startDate).compareToIgnoreCase(formatter.print(dates.get(0)))>0)
        {
        	now=startDate;
        }
        
        System.out.println("Now:"+formatter.print(now));            
        
        String insertionFolder="";
        DateTime insertionTime=null;
        
        boolean toInsert=true;
        insertionFolder=Util.findFolderBetweenGivenTimeStamps(startDate,
        now.plusMinutes(Integer.parseInt(delay)),
        initialData);
        //insert data after all relevant workflows are over
        System.out.println("insertion folder is: "+insertionFolder);
        
        int expectedsize=0;
        
        for(int i=1;i<=5;i++)
        {
        
            while(!allRelevantWorkflowsAreOver(UA3ColoHelper,bundleId, insertionFolder))
            {
                //System.out.println("waiting for relevant workflows to be over before inserting
                folder
                "+insertionFolder);
                //keep waiting
            }
        
            
            if(i%2==0)
            {
                System.out.println("going to insert data at: "+insertionFolder);
                insertionTime=new DateTime(DateTimeZone.UTC);
                System.out.println("insertion time is :"+insertionTime);
                Util.injectMoreData(UA3ColoHelper,insertionFolder,
                "src/test/resources/OozieExampleInputData/lateData");
                initialData=Util.getHadoopLateData(UA3ColoHelper,Util.getInputFeedFromBundle
                (bundle));
                toInsert=false;
                expectedsize++;
            }
            else
            {
                System.out.println("skipping data insertion this time....");
            }
            Thread.sleep(120000);
        }
        
        System.out.println("precautionary sleep commencing....");
        Thread.sleep(180000);
        System.out.println("precautionary sleep ended....");
        
        //now wait till the total size of consumer queue is 2
        int attempts=0;
        while(consumer.getMessageData().size()!=expectedsize+1 )
        {
            if(consumer.getMessageData().size()>expectedsize+1)
            {
                throw new TestNGException("extra data received in queue! some problem is there.
                please check!");
            }
            //keep waiting baby
            Thread.sleep(1000);
            attempts++; System.out.println("attempt number: "+attempts); if(attempts==180) {break;}
        }
        
        //now parse the goddamn consumer data for info
        parseConsumerOutput(UA3ColoHelper,bundle.getProcessObject(),consumer.getMessageData(),
        expectedsize+1,
        insertionFolder,expectedsize+1);
        
        
        consumer.stop();
        
        Util.dumpConsumerData(consumer);      
                    
            
        
        try {
            
        }
        catch(Exception e)
        {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        }
        finally{
            
            
        }
    }
    
        
    
    @Test(groups = {"0.2"},dataProvider="DP",priority=-20)
    public void testLateDataWithBackOffPolicyAndDataInsertedAfterLastCheck(Bundle bundle) throws
    Exception
    {
        String delay ="5";
        bundle=new Bundle(bundle,UA3ColoHelper);
        
            String feed=Util.setFeedPathValue(Util.getInputFeedFromBundle(bundle),
            "/lateDataTest/testFolders/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            feed=Util.insertLateFeedValue(feed,delay,"minutes");
            bundle.getDataSets().remove(Util.getInputFeedFromBundle(bundle));
            bundle.getDataSets().add(feed);
            Util.HDFSCleanup(UA3ColoHelper,"lateDataTest/testFolders/");
            Util.lateDataReplenish(UA3ColoHelper,20,0,0);
            List<String> initialData=Util.getHadoopLateData(UA3ColoHelper,
            Util.getInputFeedFromBundle(bundle));

            bundle.generateUniqueBundle();
            
            for(String cluster:bundle.getClusters())
            {
                    System.out.println(cluster);
                    Util.assertSucceeded(prismHelper.getClusterHelper().submitEntity(URLS
                    .SUBMIT_URL,cluster));
            }  
            
            for(String data:bundle.getDataSets())
            {
                    Util.assertSucceeded(prismHelper.getFeedHelper().submitEntity(URLS
                    .SUBMIT_URL,data));
            }
            
            DateTime startDate=new DateTime(DateTimeZone.UTC).plusMinutes(2);
            DateTime endDate=new DateTime(DateTimeZone.UTC).plusMinutes(3);
            
            
            bundle.setProcessValidity(startDate,endDate);
            bundle.setProcessLatePolicy(PolicyType.PERIODIC,"minutes(2)");
            bundle.setProcessWorkflow("/examples/apps/phailFs/workflow.xml");
            
            Util.assertSucceeded(prismHelper.getProcessHelper().submitEntity(URLS.SUBMIT_URL,
            bundle.getProcessData()));
    	
            System.out.println(bundle.getProcessData());
 
            Util.assertSucceeded(UA3ColoHelper.getProcessHelper().schedule(URLS.SCHEDULE_URL,
            bundle.getProcessData()));
        
            //attach the consumer
            Consumer consumer=new Consumer("IVORY."+Util.readEntityName(bundle.getProcessData()),
            UA3ColoHelper.getClusterHelper().getActiveMQ());
            consumer.start();
        
            //get the bundle ID
            String bundleId=Util.getCoordID(Util.getOozieJobStatus(UA3ColoHelper,
            Util.readEntityName(bundle.getProcessData()),"NONE").get(0));
            String status=Util.getBundleStatus(UA3ColoHelper,bundleId);            
            
            
            ArrayList<DateTime> dates=null;
        
            do {
        
        	dates=Util.getStartTimeForRunningCoordinators(UA3ColoHelper,bundleId);
        	
            }while(dates==null);
            
            
         System.out.println("Start time: "+formatter.print(startDate));
         System.out.println("End time: "+formatter.print(endDate));
         System.out.println("candidate nominal time:"+formatter.print(dates.get(0)));
         DateTime now=dates.get(0);
        
        if(formatter.print(startDate).compareToIgnoreCase(formatter.print(dates.get(0)))>0)
        {
        	now=startDate;
        }
        
        System.out.println("Now:"+formatter.print(now));  
        DateTime lastCheck=now.plusMinutes(Integer.parseInt(delay));
        
        String insertionFolder="";
        DateTime insertionTime=null;
        
        boolean toInsert=true;
        insertionFolder=Util.findFolderBetweenGivenTimeStamps(startDate,
        now.plusMinutes(Integer.parseInt(delay)),
        initialData);
        //insert data after all relevant workflows are over
        System.out.println("insertion folder is: "+insertionFolder);
        System.out.println("last check time is: "+lastCheck);
        
        
        int expectedsize=0;
        
        
            while(!(allRelevantWorkflowsAreOver(UA3ColoHelper,bundleId,
            insertionFolder) && lastCheck.isBefore(new
            DateTime(DateTimeZone.UTC))))
            {
                //System.out.println("waiting for relevant workflows to be over before inserting
                folder
                "+insertionFolder);
                //keep waiting
                
                
            }
        
            System.out.println("going to insert data at: "+insertionFolder);
            insertionTime=new DateTime(DateTimeZone.UTC);
            System.out.println("insertion time is :"+insertionTime);
            Util.injectMoreData(UA3ColoHelper,insertionFolder,
            "src/test/resources/OozieExampleInputData/lateData");
            initialData=Util.getHadoopLateData(UA3ColoHelper,Util.getInputFeedFromBundle(bundle));
            toInsert=false;
            expectedsize=0;
            
        
        
        System.out.println("precautionary sleep commencing....");
        Thread.sleep(180000);
        System.out.println("precautionary sleep ended....");
        
        //now wait till the total size of consumer queue is 2
        int attempts=0;
        while(consumer.getMessageData().size()!=expectedsize+1 )
        {
            if(consumer.getMessageData().size()>expectedsize+1)
            {
                throw new TestNGException("extra data received in queue! some problem is there.
                please check!");
            }
            //keep waiting baby
            Thread.sleep(1000);
            attempts++; System.out.println("attempt number: "+attempts); if(attempts==180) {break;}
        }
        
        //now parse the goddamn consumer data for info
        parseConsumerOutput(UA3ColoHelper,bundle.getProcessObject(),consumer.getMessageData(),
        expectedsize+1,
        insertionFolder,1);
        
        
        consumer.stop();
        
        Util.dumpConsumerData(consumer);      
                    
            
        
        try {
            
        }
        catch(Exception e)
        {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        }
        finally{
            
            
        }
    }  
    
    
    @Test(groups = {"0.2"},dataProvider="DP",priority=-20)
    public void testLateDataWithBackOffPolicyAndInstanceRunOverlappingOverAnotherPass(Bundle
    bundle) throws Exception
    {
        String delay ="5";
        bundle=new Bundle(bundle,UA3ColoHelper);
        
            String feed=Util.setFeedPathValue(Util.getInputFeedFromBundle(bundle),
            "/lateDataTest/testFolders/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            feed=Util.insertLateFeedValue(feed,delay,"minutes");
            bundle.getDataSets().remove(Util.getInputFeedFromBundle(bundle));
            bundle.getDataSets().add(feed);
            Util.HDFSCleanup(UA3ColoHelper,"lateDataTest/testFolders/");
            Util.lateDataReplenish(UA3ColoHelper,20,0,0);
            List<String> initialData=Util.getHadoopLateData(UA3ColoHelper,
            Util.getInputFeedFromBundle(bundle));

            bundle.generateUniqueBundle();
            
            for(String cluster:bundle.getClusters())
            {
                    System.out.println(cluster);
                    Util.assertSucceeded(prismHelper.getClusterHelper().submitEntity(URLS
                    .SUBMIT_URL,cluster));
            }  
            
            for(String data:bundle.getDataSets())
            {
                    Util.assertSucceeded(prismHelper.getFeedHelper().submitEntity(URLS
                    .SUBMIT_URL,data));
            }
            
            DateTime startDate=new DateTime(DateTimeZone.UTC).plusMinutes(2);
            DateTime endDate=new DateTime(DateTimeZone.UTC).plusMinutes(3);
            
            
            bundle.setProcessValidity(startDate,endDate);
            bundle.setProcessLatePolicy(PolicyType.PERIODIC,"minutes(1)");
            bundle.setProcessWorkflow("/examples/apps/aggregator/workflow.xml");
            
            Util.assertSucceeded(prismHelper.getProcessHelper().submitEntity(URLS.SUBMIT_URL,
            bundle.getProcessData()));
    	
            System.out.println(bundle.getProcessData());
 
            Util.assertSucceeded(UA3ColoHelper.getProcessHelper().schedule(URLS.SCHEDULE_URL,
            bundle.getProcessData()));
        
            //attach the consumer
            Consumer consumer=new Consumer("IVORY."+Util.readEntityName(bundle.getProcessData()),
            UA3ColoHelper.getClusterHelper().getActiveMQ());
            consumer.start();
        
            //get the bundle ID
            String bundleId=Util.getCoordID(Util.getOozieJobStatus(UA3ColoHelper,
            Util.readEntityName(bundle.getProcessData()),"NONE").get(0));
            String status=Util.getBundleStatus(UA3ColoHelper,bundleId);            
            
            
            ArrayList<DateTime> dates=null;
        
            do {
        
        	dates=Util.getStartTimeForRunningCoordinators(UA3ColoHelper,bundleId);
        	
            }while(dates==null);
            
            
         System.out.println("Start time: "+formatter.print(startDate));
         System.out.println("End time: "+formatter.print(endDate));
         System.out.println("candidate nominal time:"+formatter.print(dates.get(0)));
         DateTime now=dates.get(0);
        
        if(formatter.print(startDate).compareToIgnoreCase(formatter.print(dates.get(0)))>0)
        {
        	now=startDate;
        }
        
        System.out.println("Now:"+formatter.print(now));  
        DateTime insertionTime=now.plusSeconds(30);
        
        String insertionFolder="";
        
        
        boolean toInsert=true;
        insertionFolder=Util.findFolderBetweenGivenTimeStamps(startDate,
        now.plusMinutes(Integer.parseInt(delay)),
        initialData);
        //insert data after all relevant workflows are over
        System.out.println("insertion folder is: "+insertionFolder);
        System.out.println("last insertion time is: "+insertionTime);
        
        
        int expectedsize=1;
        
            System.out.println("waiting to insert data...");
            while(DateTime.now().isBefore(insertionTime)) {}
        
            System.out.println("going to insert data at: "+insertionFolder);
            insertionTime=new DateTime(DateTimeZone.UTC);
            System.out.println("insertion time is :"+insertionTime);
            Util.injectMoreData(UA3ColoHelper,insertionFolder,
            "src/test/resources/OozieExampleInputData/lateData");
            initialData=Util.getHadoopLateData(UA3ColoHelper,Util.getInputFeedFromBundle(bundle));
            toInsert=false;
            
            System.out.println("Going to check on process @....."+DateTime.now(DateTimeZone.UTC)
            .toString());
            while(!(DateTime.now(DateTimeZone.UTC).isAfter(now.plusMinutes(4).plusSeconds(30)))) {}
            System.out.println("checking process @ "+DateTime.now(DateTimeZone.UTC));
            //Assert.assertTrue(Util.getOozieJobStatus(Util.readEntityName(bundle.getProcessData
            ()),"RUNNING",
            UA3ColoHelper).get(0).contains("RUNNING"));
            Assert.assertEquals(Util.getRunIdOfSpecifiedInstance(1, bundleId, UA3ColoHelper),1,
            "run id was not correct!!!");
        
        
        System.out.println("precautionary sleep commencing....");
        Thread.sleep(180000);
        System.out.println("precautionary sleep ended....");
        
        //now wait till the total size of consumer queue is 2
        int attempts=0;

        
        while(consumer.getMessageData().size()!=expectedsize+1 )
        {
            if(consumer.getMessageData().size()>expectedsize+1)
            {
                throw new TestNGException("extra data received in queue! some problem is there.
                please check!");
            }
            //keep waiting baby
            Thread.sleep(1000);
            attempts++; System.out.println("attempt number: "+attempts); if(attempts==180) {break;}
        }
        
        Util.dumpConsumerData(consumer);  
        
        //now parse the goddamn consumer data for info
        parseConsumerOutput(UA3ColoHelper,bundle.getProcessObject(),consumer.getMessageData(),
        expectedsize+1,
        insertionFolder,2);
        
        
        consumer.stop();
        
                    
            
        
        try {
            
        }
        catch(Exception e)
        {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        }
        finally{
            
            
        }
    }     

    
    
    
    
    @Test(groups = {"0.2"},dataProvider="DP",priority=-20)
    public void testLateDataWithCutOffInFutureAndExponentialPolicy(Bundle bundle) throws Exception
    {
        String delay ="10";
        bundle=new Bundle(bundle,UA3ColoHelper);
        
            String feed=Util.setFeedPathValue(Util.getInputFeedFromBundle(bundle),
            "/lateDataTest/testFolders/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            feed=Util.insertLateFeedValue(feed,delay,"minutes");
            bundle.getDataSets().remove(Util.getInputFeedFromBundle(bundle));
            bundle.getDataSets().add(feed);
            Util.HDFSCleanup(UA3ColoHelper,"lateDataTest/testFolders/");
            Util.lateDataReplenish(UA3ColoHelper,20,0,0);
            List<String> initialData=Util.getHadoopLateData(UA3ColoHelper,
            Util.getInputFeedFromBundle(bundle));

            bundle.generateUniqueBundle();
            
            for(String cluster:bundle.getClusters())
            {
                    System.out.println(cluster);
                    Util.assertSucceeded(prismHelper.getClusterHelper().submitEntity(URLS
                    .SUBMIT_URL,cluster));
            }  
            
            for(String data:bundle.getDataSets())
            {
                    Util.assertSucceeded(prismHelper.getFeedHelper().submitEntity(URLS
                    .SUBMIT_URL,data));
            }
            
            DateTime startDate=new DateTime(DateTimeZone.UTC).plusMinutes(2);
            DateTime endDate=new DateTime(DateTimeZone.UTC).plusMinutes(3);
            
            bundle.setProcessValidity(startDate,endDate);
            bundle.setProcessLatePolicy(PolicyType.EXP_BACKOFF,"minutes(2)");
            bundle.setProcessWorkflow("/examples/apps/phailFs/workflow.xml");
            
            Util.assertSucceeded(prismHelper.getProcessHelper().submitEntity(URLS.SUBMIT_URL,
            bundle.getProcessData()));
    	
            System.out.println(bundle.getProcessData());
 
            Util.assertSucceeded(UA3ColoHelper.getProcessHelper().schedule(URLS.SCHEDULE_URL,
            bundle.getProcessData()));
        
            //attach the consumer
            Consumer consumer=new Consumer("IVORY."+Util.readEntityName(bundle.getProcessData()),
            UA3ColoHelper.getClusterHelper().getActiveMQ());
            consumer.start();
        
            //get the bundle ID
            String bundleId=Util.getCoordID(Util.getOozieJobStatus(UA3ColoHelper,
            Util.readEntityName(bundle.getProcessData()),"NONE").get(0));
            String status=Util.getBundleStatus(UA3ColoHelper,bundleId);            
            
            
            ArrayList<DateTime> dates=null;
        
            do {
        
        	dates=Util.getStartTimeForRunningCoordinators(UA3ColoHelper,bundleId);
        	
            }while(dates==null);
            
            
         System.out.println("Start time: "+formatter.print(startDate));
         System.out.println("End time: "+formatter.print(endDate));
         System.out.println("candidate nominal time:"+formatter.print(dates.get(0)));
         DateTime now=dates.get(0);
        
        if(formatter.print(startDate).compareToIgnoreCase(formatter.print(dates.get(0)))>0)
        {
        	now=startDate;
        }
        
        System.out.println("Now:"+formatter.print(now));            
        
        String insertionFolder="";
        DateTime insertionTime=null;
        
        boolean toInsert=true;
        insertionFolder=Util.findFolderBetweenGivenTimeStamps(startDate,
        now.plusMinutes(Integer.parseInt(delay)),
        initialData);
        //insert data after all relevant workflows are over
        System.out.println("insertion folder is: "+insertionFolder);
        
        int expectedsize=0;
        int initialSleep=0;
        
        for(int i=1;i<=3;i++)
        {
        
            while(!allRelevantWorkflowsAreOver(UA3ColoHelper,bundleId, insertionFolder))
            {
                //System.out.println("waiting for relevant workflows to be over before inserting
                folder
                "+insertionFolder);
                //keep waiting
            }
        
            System.out.println("going to insert data at: "+insertionFolder);
            insertionTime=new DateTime(DateTimeZone.UTC);
            System.out.println("insertion time is :"+insertionTime);
            Util.injectMoreData(UA3ColoHelper,insertionFolder,
            "src/test/resources/OozieExampleInputData/lateData");
            initialData=Util.getHadoopLateData(UA3ColoHelper,Util.getInputFeedFromBundle(bundle));
            toInsert=false;
            expectedsize=i;
            Thread.sleep(initialSleep+(2^i)*60*1000);
            
            initialSleep=(2^i)*60*1000;
            
        }
        
        System.out.println("precautionary sleep commencing....");
        Thread.sleep(180000);
        System.out.println("precautionary sleep ended....");
        
        //now wait till the total size of consumer queue is 2
        int attempts=0;
        while(consumer.getMessageData().size()!=expectedsize+1 )
        {
            if(consumer.getMessageData().size()>expectedsize+1)
            {
                throw new TestNGException("extra data received in queue! some problem is there.
                please check!");
            }
            //keep waiting baby
            Thread.sleep(1000);
            attempts++; System.out.println("attempt number: "+attempts); if(attempts==180) {break;}
        }
        
        //now parse the goddamn consumer data for info
        parseConsumerOutput(UA3ColoHelper,bundle.getProcessObject(),consumer.getMessageData(),
        expectedsize+1,
        insertionFolder,expectedsize+1);
        
        
        consumer.stop();
        
        Util.dumpConsumerData(consumer);      
                    
            
        
        try {
            
        }
        catch(Exception e)
        {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        }
        finally{
            
            
        }
    } 
    
    
    @Test(groups = {"0.2"},dataProvider="DP",priority=-20)
    public void testLateDataWithExponentialPolicyAndDataInsertedAfterLastCheck(Bundle bundle)
    throws Exception
    {
        String delay ="5";
        bundle=new Bundle(bundle,UA3ColoHelper);
        
            String feed=Util.setFeedPathValue(Util.getInputFeedFromBundle(bundle),
            "/lateDataTest/testFolders/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            feed=Util.insertLateFeedValue(feed,delay,"minutes");
            bundle.getDataSets().remove(Util.getInputFeedFromBundle(bundle));
            bundle.getDataSets().add(feed);
            Util.HDFSCleanup(UA3ColoHelper,"lateDataTest/testFolders/");
            Util.lateDataReplenish(UA3ColoHelper,20,0,0);
            List<String> initialData=Util.getHadoopLateData(UA3ColoHelper,
            Util.getInputFeedFromBundle(bundle));

            bundle.generateUniqueBundle();
            
            for(String cluster:bundle.getClusters())
            {
                    System.out.println(cluster);
                    Util.assertSucceeded(prismHelper.getClusterHelper().submitEntity(URLS
                    .SUBMIT_URL,cluster));
            }  
            
            for(String data:bundle.getDataSets())
            {
                    Util.assertSucceeded(prismHelper.getFeedHelper().submitEntity(URLS
                    .SUBMIT_URL,data));
            }
            
            DateTime startDate=new DateTime(DateTimeZone.UTC).plusMinutes(2);
            DateTime endDate=new DateTime(DateTimeZone.UTC).plusMinutes(3);
            
            
            bundle.setProcessValidity(startDate,endDate);
            bundle.setProcessLatePolicy(PolicyType.EXP_BACKOFF,"minutes(2)");
            bundle.setProcessWorkflow("/examples/apps/phailFs/workflow.xml");
            
            Util.assertSucceeded(prismHelper.getProcessHelper().submitEntity(URLS.SUBMIT_URL,
            bundle.getProcessData()));
    	
            System.out.println(bundle.getProcessData());
 
            Util.assertSucceeded(UA3ColoHelper.getProcessHelper().schedule(URLS.SCHEDULE_URL,
            bundle.getProcessData()));
        
            //attach the consumer
            Consumer consumer=new Consumer("IVORY."+Util.readEntityName(bundle.getProcessData()),
            UA3ColoHelper.getClusterHelper().getActiveMQ());
            consumer.start();
        
            //get the bundle ID
            String bundleId=Util.getCoordID(Util.getOozieJobStatus(UA3ColoHelper,
            Util.readEntityName(bundle.getProcessData()),"NONE").get(0));
            String status=Util.getBundleStatus(UA3ColoHelper,bundleId);            
            
            
            ArrayList<DateTime> dates=null;
        
            do {
        
        	dates=Util.getStartTimeForRunningCoordinators(UA3ColoHelper,bundleId);
        	
            }while(dates==null);
            
            
         System.out.println("Start time: "+formatter.print(startDate));
         System.out.println("End time: "+formatter.print(endDate));
         System.out.println("candidate nominal time:"+formatter.print(dates.get(0)));
         DateTime now=dates.get(0);
        
        if(formatter.print(startDate).compareToIgnoreCase(formatter.print(dates.get(0)))>0)
        {
        	now=startDate;
        }
        
        System.out.println("Now:"+formatter.print(now));  
        DateTime lastCheck=now.plusMinutes(Integer.parseInt(delay));
        
        String insertionFolder="";
        DateTime insertionTime=null;
        
        boolean toInsert=true;
        insertionFolder=Util.findFolderBetweenGivenTimeStamps(startDate,
        now.plusMinutes(Integer.parseInt(delay)),
        initialData);
        //insert data after all relevant workflows are over
        System.out.println("insertion folder is: "+insertionFolder);
        System.out.println("last check time is: "+lastCheck);
        
        
        int expectedsize=0;
        
        
            while(!(allRelevantWorkflowsAreOver(UA3ColoHelper,bundleId,
            insertionFolder) && lastCheck.isBefore(new
            DateTime(DateTimeZone.UTC))))
            {
                //System.out.println("waiting for relevant workflows to be over before inserting
                folder
                "+insertionFolder);
                //keep waiting
                
                
            }
        
            System.out.println("going to insert data at: "+insertionFolder);
            insertionTime=new DateTime(DateTimeZone.UTC);
            System.out.println("insertion time is :"+insertionTime);
            Util.injectMoreData(UA3ColoHelper,insertionFolder,
            "src/test/resources/OozieExampleInputData/lateData");
            initialData=Util.getHadoopLateData(UA3ColoHelper,Util.getInputFeedFromBundle(bundle));
            toInsert=false;
            expectedsize=0;
            
        
        
        System.out.println("precautionary sleep commencing....");
        Thread.sleep(180000);
        System.out.println("precautionary sleep ended....");
        
        //now wait till the total size of consumer queue is 2
        int attempts=0;
        while(consumer.getMessageData().size()!=expectedsize+1 )
        {
            if(consumer.getMessageData().size()>expectedsize+1)
            {
                throw new TestNGException("extra data received in queue! some problem is there.
                please check!");
            }
            //keep waiting baby
            Thread.sleep(1000);
            attempts++; System.out.println("attempt number: "+attempts); if(attempts==180) {break;}
        }
        
        //now parse the goddamn consumer data for info
        parseConsumerOutput(UA3ColoHelper,bundle.getProcessObject(),consumer.getMessageData(),
        expectedsize+1,
        insertionFolder,1);
        
        
        consumer.stop();
        
        Util.dumpConsumerData(consumer);      
                    
            
        
        try {
            
        }
        catch(Exception e)
        {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        }
        finally{
            
            
        }
    }      
    
  
    
    @Test(groups = {"0.2"},dataProvider="DP",priority=-20)
    public void testLateDataWithExponentialPolicyAndInstanceRunOverlappingOverAnotherPass(Bundle
    bundle) throws
    Exception
    {
        String delay ="5";
        bundle=new Bundle(bundle,UA3ColoHelper);
        
            String feed=Util.setFeedPathValue(Util.getInputFeedFromBundle(bundle),
            "/lateDataTest/testFolders/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            feed=Util.insertLateFeedValue(feed,delay,"minutes");
            bundle.getDataSets().remove(Util.getInputFeedFromBundle(bundle));
            bundle.getDataSets().add(feed);
            Util.HDFSCleanup(UA3ColoHelper,"lateDataTest/testFolders/");
            Util.lateDataReplenish(UA3ColoHelper,20,0,0);
            List<String> initialData=Util.getHadoopLateData(UA3ColoHelper,
            Util.getInputFeedFromBundle(bundle));

            bundle.generateUniqueBundle();
            
            for(String cluster:bundle.getClusters())
            {
                    System.out.println(cluster);
                    Util.assertSucceeded(prismHelper.getClusterHelper().submitEntity(URLS
                    .SUBMIT_URL,cluster));
            }  
            
            for(String data:bundle.getDataSets())
            {
                    Util.assertSucceeded(prismHelper.getFeedHelper().submitEntity(URLS
                    .SUBMIT_URL,data));
            }
            
            DateTime startDate=new DateTime(DateTimeZone.UTC).plusMinutes(2);
            DateTime endDate=new DateTime(DateTimeZone.UTC).plusMinutes(3);
            
            
            bundle.setProcessValidity(startDate,endDate);
            bundle.setProcessLatePolicy(PolicyType.EXP_BACKOFF,"minutes(1)");
            bundle.setProcessWorkflow("/examples/apps/aggregator/workflow.xml");
            
            Util.assertSucceeded(prismHelper.getProcessHelper().submitEntity(URLS.SUBMIT_URL,
            bundle.getProcessData()));
    	
            System.out.println(bundle.getProcessData());
 
            Util.assertSucceeded(UA3ColoHelper.getProcessHelper().schedule(URLS.SCHEDULE_URL,
            bundle.getProcessData()));
        
            //attach the consumer
            Consumer consumer=new Consumer("IVORY."+Util.readEntityName(bundle.getProcessData()),
            UA3ColoHelper.getClusterHelper().getActiveMQ());
            consumer.start();
        
            //get the bundle ID
            String bundleId=Util.getCoordID(Util.getOozieJobStatus(UA3ColoHelper,
            Util.readEntityName(bundle.getProcessData()),"NONE").get(0));
            String status=Util.getBundleStatus(UA3ColoHelper,bundleId);            
            
            
            ArrayList<DateTime> dates=null;
        
            do {
        
        	dates=Util.getStartTimeForRunningCoordinators(UA3ColoHelper,bundleId);
        	
            }while(dates==null);
            
            
         System.out.println("Start time: "+formatter.print(startDate));
         System.out.println("End time: "+formatter.print(endDate));
         System.out.println("candidate nominal time:"+formatter.print(dates.get(0)));
         DateTime now=dates.get(0);
        
        if(formatter.print(startDate).compareToIgnoreCase(formatter.print(dates.get(0)))>0)
        {
        	now=startDate;
        }
        
        System.out.println("Now:"+formatter.print(now));  
        DateTime insertionTime=now.plusSeconds(30);
        
        String insertionFolder="";
        
        
        boolean toInsert=true;
        insertionFolder=Util.findFolderBetweenGivenTimeStamps(startDate,
        now.plusMinutes(Integer.parseInt(delay)),
        initialData);
        //insert data after all relevant workflows are over
        System.out.println("insertion folder is: "+insertionFolder);
        System.out.println("last insertion time is: "+insertionTime);
        
        
        int expectedsize=1;
        
            System.out.println("waiting to insert data...");
            while(DateTime.now().isBefore(insertionTime)) {}
        
            System.out.println("going to insert data at: "+insertionFolder);
            insertionTime=new DateTime(DateTimeZone.UTC);
            System.out.println("insertion time is :"+insertionTime);
            Util.injectMoreData(UA3ColoHelper,insertionFolder,
            "src/test/resources/OozieExampleInputData/lateData");
            initialData=Util.getHadoopLateData(UA3ColoHelper,Util.getInputFeedFromBundle(bundle));
            toInsert=false;
            
            System.out.println("Going to check on process.....");
            while(!(DateTime.now(DateTimeZone.UTC).isAfter(now.plusMinutes(4).plusSeconds(20)))) {}
            System.out.println("checking process @ "+DateTime.now(DateTimeZone.UTC));
            //Assert.assertTrue(Util.getOozieJobStatus(Util.readEntityName(bundle.getProcessData
            ()),"RUNNING",
            UA3ColoHelper).get(0).contains("RUNNING"));
            Assert.assertEquals(Util.getRunIdOfSpecifiedInstance(1, bundleId, UA3ColoHelper),1,
            "run id was not correct!!!");
        
        
        System.out.println("precautionary sleep commencing....");
        Thread.sleep(180000);
        System.out.println("precautionary sleep ended....");
        
        //now wait till the total size of consumer queue is 2
        int attempts=0;

        
        while(consumer.getMessageData().size()!=expectedsize+1 )
        {
            if(consumer.getMessageData().size()>expectedsize+1)
            {
                throw new TestNGException("extra data received in queue! some problem is there.
                please check!");
            }
            //keep waiting baby
            Thread.sleep(1000);
            attempts++; System.out.println("attempt number: "+attempts); if(attempts==180) {break;}
        }
        
        Util.dumpConsumerData(consumer);  
        
        //now parse the goddamn consumer data for info
        parseConsumerOutput(UA3ColoHelper,bundle.getProcessObject(),consumer.getMessageData(),
        expectedsize+1,
        insertionFolder,2);
        
        
        consumer.stop();
        
                    
            
        
        try {
            
        }
        catch(Exception e)
        {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        }
        finally{
            
            
        }
    }
    
    
    @Test(groups = {"0.2"},dataProvider="DP",priority=-20)
    public void testLateDataWithCutOffInFutureAndFinalPolicy(Bundle bundle) throws Exception
    {
        String delay ="10";
        bundle=new Bundle(bundle,UA3ColoHelper);
        
            String feed=Util.setFeedPathValue(Util.getInputFeedFromBundle(bundle),
            "/lateDataTest/testFolders/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            feed=Util.insertLateFeedValue(feed,delay,"minutes");
            bundle.getDataSets().remove(Util.getInputFeedFromBundle(bundle));
            bundle.getDataSets().add(feed);
            Util.HDFSCleanup(UA3ColoHelper,"lateDataTest/testFolders/");
            Util.lateDataReplenish(UA3ColoHelper,20,0,0);
            List<String> initialData=Util.getHadoopLateData(UA3ColoHelper,
            Util.getInputFeedFromBundle(bundle));

            bundle.generateUniqueBundle();
            
            for(String cluster:bundle.getClusters())
            {
                    System.out.println(cluster);
                    Util.assertSucceeded(prismHelper.getClusterHelper().submitEntity(URLS
                    .SUBMIT_URL,cluster));
            }  
            
            for(String data:bundle.getDataSets())
            {
                    Util.assertSucceeded(prismHelper.getFeedHelper().submitEntity(URLS
                    .SUBMIT_URL,data));
            }
            
            DateTime startDate=new DateTime(DateTimeZone.UTC).plusMinutes(2);
            DateTime endDate=new DateTime(DateTimeZone.UTC).plusMinutes(3);
            
            bundle.setProcessValidity(startDate,endDate);
            bundle.setProcessLatePolicy(PolicyType.FINAL,"minutes(2)");
            bundle.setProcessWorkflow("/examples/apps/phailFs/workflow.xml");
            
            Util.assertSucceeded(prismHelper.getProcessHelper().submitEntity(URLS.SUBMIT_URL,
            bundle.getProcessData()));
    	
            System.out.println(bundle.getProcessData());
 
            Util.assertSucceeded(UA3ColoHelper.getProcessHelper().schedule(URLS.SCHEDULE_URL,
            bundle.getProcessData()));
        
            //attach the consumer
            Consumer consumer=new Consumer("IVORY."+Util.readEntityName(bundle.getProcessData()),
            UA3ColoHelper.getClusterHelper().getActiveMQ());
            consumer.start();
        
            //get the bundle ID
            String bundleId=Util.getCoordID(Util.getOozieJobStatus(UA3ColoHelper,
            Util.readEntityName(bundle.getProcessData()),"NONE").get(0));
            String status=Util.getBundleStatus(UA3ColoHelper,bundleId);            
            
            
            ArrayList<DateTime> dates=null;
        
            do {
        
        	dates=Util.getStartTimeForRunningCoordinators(UA3ColoHelper,bundleId);
        	
            }while(dates==null);
            
            
         System.out.println("Start time: "+formatter.print(startDate));
         System.out.println("End time: "+formatter.print(endDate));
         System.out.println("candidate nominal time:"+formatter.print(dates.get(0)));
         DateTime now=dates.get(0);
        
        if(formatter.print(startDate).compareToIgnoreCase(formatter.print(dates.get(0)))>0)
        {
        	now=startDate;
        }
        
        System.out.println("Now:"+formatter.print(now));            
        
        String insertionFolder="";
        DateTime insertionTime=null;
        
        boolean toInsert=true;
        insertionFolder=Util.findFolderBetweenGivenTimeStamps(startDate,
        now.plusMinutes(Integer.parseInt(delay)),
        initialData);
        //insert data after all relevant workflows are over
        System.out.println("insertion folder is: "+insertionFolder);
        
        int expectedsize=1;
        
        for(int i=1;i<=5;i++)
        {
        
            while(!allRelevantWorkflowsAreOver(UA3ColoHelper,bundleId, insertionFolder))
            {
                //System.out.println("waiting for relevant workflows to be over before inserting
                folder
                "+insertionFolder);
                //keep waiting
            }
        
            System.out.println("going to insert data at: "+insertionFolder);
            insertionTime=new DateTime(DateTimeZone.UTC);
            System.out.println("insertion time is :"+insertionTime);
            Util.injectMoreData(UA3ColoHelper,insertionFolder,
            "src/test/resources/OozieExampleInputData/lateData");
            initialData=Util.getHadoopLateData(UA3ColoHelper,Util.getInputFeedFromBundle(bundle));
            toInsert=false;
            Thread.sleep(120000);
        }
        
        System.out.println("precautionary sleep commencing....");
        Thread.sleep(180000);
        System.out.println("precautionary sleep ended....");
        
        //now wait till the total size of consumer queue is 2
        int attempts=0;
        while(consumer.getMessageData().size()!=expectedsize+1 )
        {
            if(consumer.getMessageData().size()>expectedsize+1)
            {
                throw new TestNGException("extra data received in queue! some problem is there.
                please check!");
            }
            //keep waiting baby
            Thread.sleep(1000);
            attempts++; System.out.println("attempt number: "+attempts); if(attempts==180) {break;}
        }
        
        //now parse the goddamn consumer data for info
        parseConsumerOutput(UA3ColoHelper,bundle.getProcessObject(),consumer.getMessageData(),
        expectedsize+1,
        insertionFolder,expectedsize+1);
        
        
        consumer.stop();
        
        Util.dumpConsumerData(consumer);      
                    
            
        
        try {
            
        }
        catch(Exception e)
        {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        }
        finally{
            
            
        }
    } 
    
    
    
    
    @Test(groups = {"0.2"},dataProvider="DP",priority=-20)
    public void testLateDataWithFinalPolicyAndDataInsertedAfterLastCheck(Bundle bundle) throws
    Exception
    {
        String delay ="5";
        bundle=new Bundle(bundle,UA3ColoHelper);
        
            String feed=Util.setFeedPathValue(Util.getInputFeedFromBundle(bundle),
            "/lateDataTest/testFolders/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            feed=Util.insertLateFeedValue(feed,delay,"minutes");
            bundle.getDataSets().remove(Util.getInputFeedFromBundle(bundle));
            bundle.getDataSets().add(feed);
            Util.HDFSCleanup(UA3ColoHelper,"lateDataTest/testFolders/");
            Util.lateDataReplenish(UA3ColoHelper,20,0,0);
            List<String> initialData=Util.getHadoopLateData(UA3ColoHelper,
            Util.getInputFeedFromBundle(bundle));

            bundle.generateUniqueBundle();
            
            for(String cluster:bundle.getClusters())
            {
                    System.out.println(cluster);
                    Util.assertSucceeded(prismHelper.getClusterHelper().submitEntity(URLS
                    .SUBMIT_URL,cluster));
            }  
            
            for(String data:bundle.getDataSets())
            {
                    Util.assertSucceeded(prismHelper.getFeedHelper().submitEntity(URLS
                    .SUBMIT_URL,data));
            }
            
            DateTime startDate=new DateTime(DateTimeZone.UTC).plusMinutes(2);
            DateTime endDate=new DateTime(DateTimeZone.UTC).plusMinutes(3);
            
            
            bundle.setProcessValidity(startDate,endDate);
            bundle.setProcessLatePolicy(PolicyType.FINAL,"minutes(2)");
            bundle.setProcessWorkflow("/examples/apps/phailFs/workflow.xml");
            
            Util.assertSucceeded(prismHelper.getProcessHelper().submitEntity(URLS.SUBMIT_URL,
            bundle.getProcessData()));
    	
            System.out.println(bundle.getProcessData());
 
            Util.assertSucceeded(UA3ColoHelper.getProcessHelper().schedule(URLS.SCHEDULE_URL,
            bundle.getProcessData()));
        
            //attach the consumer
            Consumer consumer=new Consumer("IVORY."+Util.readEntityName(bundle.getProcessData()),
            UA3ColoHelper.getClusterHelper().getActiveMQ());
            consumer.start();
        
            //get the bundle ID
            String bundleId=Util.getCoordID(Util.getOozieJobStatus(UA3ColoHelper,
            Util.readEntityName(bundle.getProcessData()),"NONE").get(0));
            String status=Util.getBundleStatus(UA3ColoHelper,bundleId);            
            
            
            ArrayList<DateTime> dates=null;
        
            do {
        
        	dates=Util.getStartTimeForRunningCoordinators(UA3ColoHelper,bundleId);
        	
            }while(dates==null);
            
            
         System.out.println("Start time: "+formatter.print(startDate));
         System.out.println("End time: "+formatter.print(endDate));
         System.out.println("candidate nominal time:"+formatter.print(dates.get(0)));
         DateTime now=dates.get(0);
        
        if(formatter.print(startDate).compareToIgnoreCase(formatter.print(dates.get(0)))>0)
        {
        	now=startDate;
        }
        
        System.out.println("Now:"+formatter.print(now));  
        DateTime lastCheck=now.plusMinutes(Integer.parseInt(delay));
        
        String insertionFolder="";
        DateTime insertionTime=null;
        
        boolean toInsert=true;
        insertionFolder=Util.findFolderBetweenGivenTimeStamps(startDate,
        now.plusMinutes(Integer.parseInt(delay)),
        initialData);
        //insert data after all relevant workflows are over
        System.out.println("insertion folder is: "+insertionFolder);
        System.out.println("last check time is: "+lastCheck);
        
        
        int expectedsize=0;
        
        
            while(!(allRelevantWorkflowsAreOver(UA3ColoHelper,bundleId,
            insertionFolder) && lastCheck.isBefore(new
            DateTime(DateTimeZone.UTC))))
            {
                //System.out.println("waiting for relevant workflows to be over before inserting
                folder
                "+insertionFolder);
                //keep waiting
                
                
            }
            
            Assert.assertEquals(consumer.getMessageData().size(),1,"where is the first response
            btw!!!");
            System.out.println("going to insert data at: "+insertionFolder);
            insertionTime=new DateTime(DateTimeZone.UTC);
            System.out.println("insertion time is :"+insertionTime);
            Util.injectMoreData(UA3ColoHelper,insertionFolder,
            "src/test/resources/OozieExampleInputData/lateData");
            initialData=Util.getHadoopLateData(UA3ColoHelper,Util.getInputFeedFromBundle(bundle));
            toInsert=false;
            expectedsize=0;
            
        
        
        System.out.println("precautionary sleep commencing....");
        Thread.sleep(180000);
        System.out.println("precautionary sleep ended....");
        
        //now wait till the total size of consumer queue is 2
        int attempts=0;
        while(consumer.getMessageData().size()!=expectedsize+1 )
        {
            if(consumer.getMessageData().size()>expectedsize+1)
            {
                throw new TestNGException("extra data received in queue! some problem is there.
                please check!");
            }
            //keep waiting baby
            Thread.sleep(1000);
            attempts++; System.out.println("attempt number: "+attempts); if(attempts==180) {break;}
        }
        
        //now parse the goddamn consumer data for info
        parseConsumerOutput(UA3ColoHelper,bundle.getProcessObject(),consumer.getMessageData(),
        expectedsize+1,
        insertionFolder,1);
        
        
        consumer.stop();
        
        Util.dumpConsumerData(consumer);      
                    
            
        
        try {
            
        }
        catch(Exception e)
        {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        }
        finally{
            
            
        }
    } 
    
    
    @Test(groups = {"0.2"},dataProvider="DP",priority=-20)
    public void testLateDataWithFinalPolicyAndInstanceRunOverlappingOverAnotherPass(Bundle
    bundle) throws Exception
    {
        String delay ="1";
        bundle=new Bundle(bundle,UA3ColoHelper);
        
            String feed=Util.setFeedPathValue(Util.getInputFeedFromBundle(bundle),
            "/lateDataTest/testFolders/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            feed=Util.insertLateFeedValue(feed,delay,"minutes");
            bundle.getDataSets().remove(Util.getInputFeedFromBundle(bundle));
            bundle.getDataSets().add(feed);
            Util.HDFSCleanup(UA3ColoHelper,"lateDataTest/testFolders/");
            Util.lateDataReplenish(UA3ColoHelper,20,0,0);
            List<String> initialData=Util.getHadoopLateData(UA3ColoHelper,
            Util.getInputFeedFromBundle(bundle));

            bundle.generateUniqueBundle();
            
            for(String cluster:bundle.getClusters())
            {
                    System.out.println(cluster);
                    Util.assertSucceeded(prismHelper.getClusterHelper().submitEntity(URLS
                    .SUBMIT_URL,cluster));
            }  
            
            for(String data:bundle.getDataSets())
            {
                    Util.assertSucceeded(prismHelper.getFeedHelper().submitEntity(URLS
                    .SUBMIT_URL,data));
            }
            
            DateTime startDate=new DateTime(DateTimeZone.UTC).plusMinutes(2);
            DateTime endDate=new DateTime(DateTimeZone.UTC).plusMinutes(3);
            
            
            bundle.setProcessValidity(startDate,endDate);
            bundle.setProcessLatePolicy(PolicyType.EXP_BACKOFF,"minutes(1)");
            bundle.setProcessWorkflow("/examples/apps/aggregator/workflow.xml");
            
            Util.assertSucceeded(prismHelper.getProcessHelper().submitEntity(URLS.SUBMIT_URL,
            bundle.getProcessData()));
    	
            System.out.println(bundle.getProcessData());
 
            Util.assertSucceeded(UA3ColoHelper.getProcessHelper().schedule(URLS.SCHEDULE_URL,
            bundle.getProcessData()));
        
            //attach the consumer
            Consumer consumer=new Consumer("IVORY."+Util.readEntityName(bundle.getProcessData()),
            UA3ColoHelper.getClusterHelper().getActiveMQ());
            consumer.start();
        
            //get the bundle ID
            String bundleId=Util.getCoordID(Util.getOozieJobStatus(UA3ColoHelper,
            Util.readEntityName(bundle.getProcessData()),"NONE").get(0));
            String status=Util.getBundleStatus(UA3ColoHelper,bundleId);            
            
            
            ArrayList<DateTime> dates=null;
        
            do {
        
        	dates=Util.getStartTimeForRunningCoordinators(UA3ColoHelper,bundleId);
        	
            }while(dates==null);
            
            
         System.out.println("Start time: "+formatter.print(startDate));
         System.out.println("End time: "+formatter.print(endDate));
         System.out.println("candidate nominal time:"+formatter.print(dates.get(0)));
         DateTime now=dates.get(0);
        
        if(formatter.print(startDate).compareToIgnoreCase(formatter.print(dates.get(0)))>0)
        {
        	now=startDate;
        }
        
        System.out.println("Now:"+formatter.print(now));  
        DateTime insertionTime=now.plusSeconds(15);
        
        String insertionFolder="";
        
        
        boolean toInsert=true;
        insertionFolder=Util.findFolderBetweenGivenTimeStamps(startDate,
        now.plusMinutes(Integer.parseInt(delay)),
        initialData);
        //insert data after all relevant workflows are over
        System.out.println("insertion folder is: "+insertionFolder);
        System.out.println("last insertion time is: "+insertionTime);
        
        
        int expectedsize=1;
        
            System.out.println("waiting to insert data...");
            while(DateTime.now().isBefore(insertionTime)) {}
        
            System.out.println("going to insert data at: "+insertionFolder);
            insertionTime=new DateTime(DateTimeZone.UTC);
            System.out.println("insertion time is :"+insertionTime);
            Util.injectMoreData(UA3ColoHelper,insertionFolder,
            "src/test/resources/OozieExampleInputData/lateData");
            initialData=Util.getHadoopLateData(UA3ColoHelper,Util.getInputFeedFromBundle(bundle));
            toInsert=false;
            
            
            
            System.out.println("Going to check on process.....");
            while(!(DateTime.now(DateTimeZone.UTC).isAfter(now.plusMinutes(3).plusSeconds(20)))) {}
            System.out.println("checking process @ "+DateTime.now(DateTimeZone.UTC));
            //Assert.assertTrue(Util.getOozieJobStatus(Util.readEntityName(bundle.getProcessData
            ()),"RUNNING",
            UA3ColoHelper).get(0).contains("RUNNING"));
            //Assert.assertEquals(Util.getRunIdOfSpecifiedInstance(1, bundleId, UA3ColoHelper),1,
            "run id was not correct!!!");
        
        
        System.out.println("precautionary sleep commencing....");
        Thread.sleep(180000);
        System.out.println("precautionary sleep ended....");
        
        //now wait till the total size of consumer queue is 2
        int attempts=0;

        
        while(consumer.getMessageData().size()!=expectedsize+1 )
        {
            if(consumer.getMessageData().size()>expectedsize+1)
            {
                throw new TestNGException("extra data received in queue! some problem is there.
                please check!");
            }
            //keep waiting baby
            Thread.sleep(1000);
            attempts++; System.out.println("attempt number: "+attempts); if(attempts==180) {break;}
        }
        
        Util.dumpConsumerData(consumer);  
        
        //now parse the goddamn consumer data for info
        parseConsumerOutput(UA3ColoHelper,bundle.getProcessObject(),consumer.getMessageData(),
        expectedsize+1,
        insertionFolder,2);
        
        
        consumer.stop();
        
                    
            
        
        try {
            
        }
        catch(Exception e)
        {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        }
        finally{
            
            
        }
    }
//    
//    
////    @Test(groups = {"0.2"},dataProvider="DP",priority=-20)
////    public void testLateDataWithCutOffInFutureAndBackOffPolicy(Bundle bundle) throws Exception
////    {
////        String delay ="10";
////        bundle=new Bundle(bundle,UA3ColoHelper);
////        
////            String feed=Util.setFeedPathValue(Util.getInputFeedFromBundle(bundle),
"/lateDataTest/testFolders/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
////            feed=Util.insertLateFeedValue(feed,delay,"minutes");
////            bundle.getDataSets().remove(Util.getInputFeedFromBundle(bundle));
////            bundle.getDataSets().add(feed);
////            Util.HDFSCleanup(UA3ColoHelper,"lateDataTest/testFolders/");
////            Util.lateDataReplenish(UA3ColoHelper,20,0,0);
////            List<String> initialData=Util.getHadoopLateData(UA3ColoHelper,
Util.getInputFeedFromBundle(bundle));
////
////            bundle.generateUniqueBundle();
////            
////            for(String cluster:bundle.getClusters())
////            {
////                    System.out.println(cluster);
////                    Util.assertSucceeded(prismHelper.getClusterHelper().submitEntity(URLS
.SUBMIT_URL,cluster));
////            }  
////            
////            for(String data:bundle.getDataSets())
////            {
////                    Util.assertSucceeded(prismHelper.getFeedHelper().submitEntity(URLS
.SUBMIT_URL,data));
////            }
////            
////            DateTime startDate=new DateTime(DateTimeZone.UTC).plusMinutes(2);
////            DateTime endDate=new DateTime(DateTimeZone.UTC).plusMinutes(3);
////            
////            bundle.setProcessValidity(startDate,endDate);
////            bundle.setProcessLatePolicy(PolicyType.PERIODIC,"minutes(2)");
////            bundle.setProcessWorkflow("/examples/apps/phailFs/workflow.xml");
////            
////            Util.assertSucceeded(prismHelper.getProcessHelper().submitEntity(URLS.SUBMIT_URL,
bundle.getProcessData()));
////    	
////            System.out.println(bundle.getProcessData());
//// 
////            Util.assertSucceeded(UA3ColoHelper.getProcessHelper().schedule(URLS.SCHEDULE_URL,
bundle.getProcessData()));
////        
////            //attach the consumer
////            Consumer consumer=new Consumer("IVORY."+Util.readEntityName(bundle.getProcessData
()),
UA3ColoHelper.getClusterHelper().getActiveMQ());
////            consumer.start();
////        
////            //get the bundle ID
////            String bundleId=Util.getCoordID(Util.getOozieJobStatus(UA3ColoHelper,
Util.readEntityName(bundle.getProcessData()),"NONE").get(0));
////            String status=Util.getBundleStatus(UA3ColoHelper,bundleId);            
////            
////            
////            ArrayList<DateTime> dates=null;
////        
////            do {
////        
////        	dates=Util.getStartTimeForRunningCoordinators(UA3ColoHelper,bundleId);
////        	
////            }while(dates==null);
////            
////            
////         System.out.println("Start time: "+formatter.print(startDate));
////         System.out.println("End time: "+formatter.print(endDate));
////         System.out.println("candidate nominal time:"+formatter.print(dates.get(0)));
////         DateTime now=dates.get(0);
////        
////        if(formatter.print(startDate).compareToIgnoreCase(formatter.print(dates.get(0)))>0)
////        {
////        	now=startDate;
////        }
////        
////        System.out.println("Now:"+formatter.print(now));            
////        
////        String insertionFolder="";
////        DateTime insertionTime=null;
////        
////        boolean toInsert=true;
////        insertionFolder=Util.findFolderBetweenGivenTimeStamps(startDate,
now.plusMinutes(Integer.parseInt(delay)),
initialData);
////        //insert data after all relevant workflows are over
////        System.out.println("insertion folder is: "+insertionFolder);
////        
////        int expectedsize=0;
////        int skippedInstances=0;
////        
////        for(int i=1;i<=5;i++)
////        {
////        
////            while(!allRelevantWorkflowsAreOver(UA3ColoHelper,bundleId, insertionFolder))
////            {
////                if(i%2==0)
////                {
////                  Util.assertSucceeded(UA3ColoHelper.getProcessHelper().suspend(URLS
.SUSPEND_URL,
bundle.getProcessData()));
////                  skippedInstances++;  
////                  break;
////                }
////                else
////                {
////                   
////                Util.assertSucceeded(UA3ColoHelper.getProcessHelper().resume(URLS.RESUME_URL,
bundle.getProcessData()));
////                    
////                }
////                //System.out.println("waiting for relevant workflows to be over before
inserting folder
"+insertionFolder);
////                //keep waiting
////            }
////        
////            System.out.println("going to insert data at: "+insertionFolder);
////            insertionTime=new DateTime(DateTimeZone.UTC);
////            System.out.println("insertion time is :"+insertionTime);
////            Util.injectMoreData(UA3ColoHelper,insertionFolder,
"src/test/resources/OozieExampleInputData/lateData");
////            initialData=Util.getHadoopLateData(UA3ColoHelper,Util.getInputFeedFromBundle
(bundle));
////            toInsert=false;
////            expectedsize=i;
////            
////            Thread.sleep(120000);
////            
////        }
////        
////        System.out.println("precautionary sleep commencing....");
////        Thread.sleep(180000);
////        System.out.println("precautionary sleep ended....");
////        
////        //now wait till the total size of consumer queue is 2
////        int attempts=0;
////        while(consumer.getMessageData().size()!=expectedsize+1-skippedInstances )
////        {
////            if(consumer.getMessageData().size()>expectedsize+1-skippedInstances)
////            {
////                throw new TestNGException("extra data received in queue! some problem is
there. please check!");
////            }
////            //keep waiting baby
////            Thread.sleep(1000);
////            attempts++; System.out.println("attempt number: "+attempts); if(attempts==180)
{break;}
////        }
////        
////        //now parse the goddamn consumer data for info
////        parseConsumerOutput(UA3ColoHelper,bundle.getProcessObject(),consumer.getMessageData(),
expectedsize-skippedInstances+1,insertionFolder,expectedsize-skippedInstances+1);
////        
////        
////        consumer.stop();
////        
////        Util.dumpConsumerData(consumer);      
////                    
////            
////        
////        try {
////            
////        }
////        catch(Exception e)
////        {
////            e.printStackTrace();
////            throw new TestNGException(e.getCause());
////        }
////        finally{
////            
////            
////        }
////    }    
//    
    @Test(groups = {"0.2"},dataProvider="DP",priority=-20)
    @SuppressWarnings("SleepWhileInLoop")
    public void testLateDataWithCutOffInFutureAndPeriodicToExponentialUpdatePolicy(Bundle bundle)
     throws Exception
    {
        String delay ="10";
        bundle=new Bundle(bundle,UA3ColoHelper);
        
            String feed=Util.setFeedPathValue(Util.getInputFeedFromBundle(bundle),
            "/lateDataTest/testFolders/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            feed=Util.insertLateFeedValue(feed,delay,"minutes");
            bundle.getDataSets().remove(Util.getInputFeedFromBundle(bundle));
            bundle.getDataSets().add(feed);
            Util.HDFSCleanup(UA3ColoHelper,"lateDataTest/testFolders/");
            Util.lateDataReplenish(UA3ColoHelper,20,0,0);
            List<String> initialData=Util.getHadoopLateData(UA3ColoHelper,
            Util.getInputFeedFromBundle(bundle));

            bundle.generateUniqueBundle();
            
            for(String cluster:bundle.getClusters())
            {
                    System.out.println(cluster);
                    Util.assertSucceeded(prismHelper.getClusterHelper().submitEntity(URLS
                    .SUBMIT_URL,cluster));
            }  
            
            for(String data:bundle.getDataSets())
            {
                    Util.assertSucceeded(prismHelper.getFeedHelper().submitEntity(URLS
                    .SUBMIT_URL,data));
            }
            
            DateTime startDate=new DateTime(DateTimeZone.UTC).plusMinutes(2);
            DateTime endDate=new DateTime(DateTimeZone.UTC).plusMinutes(3);
            
            bundle.setProcessValidity(startDate,endDate);
            bundle.setProcessLatePolicy(PolicyType.PERIODIC,"minutes(2)");
            bundle.setProcessWorkflow("/examples/apps/phailFs/workflow.xml");
            
            Util.assertSucceeded(prismHelper.getProcessHelper().submitEntity(URLS.SUBMIT_URL,
            bundle.getProcessData()));
    	
            System.out.println(bundle.getProcessData());
 
            Util.assertSucceeded(UA3ColoHelper.getProcessHelper().schedule(URLS.SCHEDULE_URL,
            bundle.getProcessData()));
        
            //attach the consumer
            Consumer consumer=new Consumer("IVORY."+Util.readEntityName(bundle.getProcessData()),
            UA3ColoHelper.getClusterHelper().getActiveMQ());
            consumer.start();
        
            //get the bundle ID
            String bundleId=Util.getCoordID(Util.getOozieJobStatus(UA3ColoHelper,
            Util.readEntityName(bundle.getProcessData()),"NONE").get(0));
            String status=Util.getBundleStatus(UA3ColoHelper,bundleId);            
            
            
            ArrayList<DateTime> dates=null;
        
            do {
        
        	dates=Util.getStartTimeForRunningCoordinators(UA3ColoHelper,bundleId);
        	
            }while(dates==null);
            
            
         System.out.println("Start time: "+formatter.print(startDate));
         System.out.println("End time: "+formatter.print(endDate));
         System.out.println("candidate nominal time:"+formatter.print(dates.get(0)));
         DateTime now=dates.get(0);
        
        if(formatter.print(startDate).compareToIgnoreCase(formatter.print(dates.get(0)))>0)
        {
        	now=startDate;
        }
        
        System.out.println("Now:"+formatter.print(now));            
        
        String insertionFolder="";
        DateTime insertionTime=null;
        
        boolean toInsert=true;
        insertionFolder=Util.findFolderBetweenGivenTimeStamps(startDate,
        now.plusMinutes(Integer.parseInt(delay)),
        initialData);
        //insert data after all relevant workflows are over
        System.out.println("insertion folder is: "+insertionFolder);
        
        int expectedsize=0;
        
        boolean updated=false;
        
        long beforeUpdateWait=120000;
        long postUpdateValue=240000;
        
        for(int i=1;i<=5;i++)
        {
            //System.out.println("i="+i+" and updated="+updated);
            while(!allRelevantWorkflowsAreOver(UA3ColoHelper,bundleId, insertionFolder))
            {
                //System.out.println("waiting for relevant workflows to be over before inserting
                folder
                "+insertionFolder);
                //keep waiting

            }
            
                if(i==3)
                {
                    System.out.println("going to update process now....");
                    LateProcess newlate=bundle.getProcessObject().getLateProcess();
                    newlate.setPolicy(PolicyType.EXP_BACKOFF);
                    
                    bundle.getProcessObject().setLateProcess(newlate);
                    Util.assertSucceeded(prismHelper.getProcessHelper().update(bundle
                    .getProcessData(),
                    bundle.getProcessData()));
                    updated=true;
                }
            
            System.out.println("going to insert data at: "+insertionFolder);
            insertionTime=new DateTime(DateTimeZone.UTC);
            System.out.println("insertion time is :"+insertionTime);
            Util.injectMoreData(UA3ColoHelper,insertionFolder,
            "src/test/resources/OozieExampleInputData/lateData");
            initialData=Util.getHadoopLateData(UA3ColoHelper,Util.getInputFeedFromBundle(bundle));
            
            if(updated)
            {
                Thread.sleep(postUpdateValue);
                
            }
            else
            {
                Thread.sleep(beforeUpdateWait);
                continue;
            }
            expectedsize=i;
        }
        
        System.out.println("precautionary sleep commencing....");
        Thread.sleep(180000);
        System.out.println("precautionary sleep ended....");
        
        //now wait till the total size of consumer queue is 2
        int attempts=0;
        while(consumer.getMessageData().size()!=expectedsize+1 )
        {
            if(consumer.getMessageData().size()>expectedsize+1)
            {
                throw new TestNGException("extra data received in queue! some problem is there.
                please check!");
            }
            //keep waiting baby
            Thread.sleep(1000);
            attempts++; System.out.println("attempt number: "+attempts); if(attempts==180) {break;}
            System.out.println("current attempt is "+attempts);
        }
        
        //now parse the goddamn consumer data for info
        parseConsumerOutput(UA3ColoHelper,bundle.getProcessObject(),consumer.getMessageData(),
        expectedsize+1,
        insertionFolder,expectedsize+1);
        
        
        consumer.stop();
        
        Util.dumpConsumerData(consumer);      
                    
            
        
        try {
            
        }
        catch(Exception e)
        {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        }
        finally{
            
            
        }
    }    
    
    

    private void parseConsumerOutput(ColoHelper coloHelper,com.inmobi.qa.airavatqa.generated
    .process.Process
    processObject,List<HashMap<String,String>> consumerData,int expectedSize,String insertionFolder,
    int expectedCountForEachInstance) throws Exception
    {
        ArrayList<String> relevantFlowList=getAllRelevantWorkflows(UA3ColoHelper,
        instanceUtil.getLatestBundleID(UA3ColoHelper,processObject.getName(),"process"),
        insertionFolder);
        
        
        //check that all such flows are present in the consumer list for the expected number of
        times i.e no messages
         a re missing
        for(String id:relevantFlowList)
        {
            int count=0;
            int runId=-1;
            
            for(HashMap<String,String> entry:consumerData)
            {
                if(id.equalsIgnoreCase(entry.get("workflowId")))
                {
                    count++;
                }
                if(Integer.parseInt(entry.get("runId"))>runId)
                {
                    runId=Integer.parseInt(entry.get("runId"));
                }
            }
            
            Assert.assertEquals(count,expectedCountForEachInstance,"workflow id: "+id+" did not
            have expected number
            of data in the queue!!!");
            Assert.assertEquals(runId,expectedCountForEachInstance-1,"workflow id: "+id+" did not
             have expected
            number of runs in the queue!!!");
        }
        
    }
    
    
    
    
	private boolean allRelevantWorkflowsAreOver(PrismHelper prismHelper,String bundleId,
	String insertionFolder) throws Exception
	{
		HashMap<String,Boolean>workflowStatusMap=new HashMap<String, Boolean>(); 
		
                XOozieClient oozieClient=new XOozieClient(prismHelper.getProcessHelper()
                .getOozieURL());
		BundleJob bundleJob = oozieClient.getBundleJobInfo(bundleId);
    	
		DateTimeFormatter formatter=DateTimeFormat.forPattern("yyyy/MM/dd/HH/mm");
		List<String> actualNominalTimes=new ArrayList<String>();
		
		for(CoordinatorJob job:bundleJob.getCoordinators())
		{
			if(job.getAppName().contains("DEFAULT"))
			{
				
				CoordinatorJob coordJob=oozieClient.getCoordJobInfo(job.getId());
                                
                               
				for(CoordinatorAction action:coordJob.getActions())
				{
                                   //     CoordinatorAction actionMan=oozieClient
                                   .getCoordActionInfo(action.getId());
                                        
                                        CoordinatorAction actionMan= coordJob.getActions().get
                                        (action.getActionNumber
                                        ()-1);
                                        
                                      
                                        
                                        if(!(actionMan.getStatus().equals(CoordinatorAction
                                        .Status.WAITING) ||
                                        actionMan.getStatus().equals(CoordinatorAction.Status
                                        .READY) || actionMan
                                        .getStatus().equals(CoordinatorAction.Status.SUBMITTED) ))
                                        {
                                        	
                                        	  WorkflowJob j = oozieClient.getJobInfo(action
                                        	  .getExternalId());
                                              
                                             // Util.print(" j conf: "+ j.getConf());
                                        	
                                     
                                        	
                                            if(getRunConfInputString(j.getConf()).contains
                                            (insertionFolder))
                                            {
                                            
                                                if((actionMan.getStatus().equals
                                                (CoordinatorAction.Status.SUCCEEDED))
                                                 || actionMan.getStatus().equals
                                                 (CoordinatorAction.Status.KILLED) ||
                                                 actionMan.getStatus().equals(CoordinatorAction
                                                 .Status.FAILED))
                                                {
                                                    System.out.println("found folder
                                                    "+insertionFolder+" in workflow:
                                                     "+actionMan.getId());
                                                    System.out.println("related workflow
                                                    "+actionMan.getId()+" is
                                                    over....");
                                                    workflowStatusMap.put(actionMan.getId(),
                                                    Boolean.TRUE);
                                                }
                                            else
                                            {
                                                workflowStatusMap.put(action.getId(),
                                                Boolean.FALSE);
                                            }
                                        }
                                            
                                        
					}
				}
			}
		}
		
                boolean finished=true;
                
                if(workflowStatusMap.isEmpty())
                {
                    return false;
                }
                
		for(String workflowId:workflowStatusMap.keySet())
                {
                  finished&=workflowStatusMap.get(workflowId);  
                }
                
                return finished;
	}  
        
        
    private int getTimeDiff(String timestamp1,String timestamp2) throws Exception
    {
    	DateTimeFormatter formatter=DateTimeFormat.forPattern("yyyy/MM/dd/HH/mm");
    	//DateTime start=new DateTime(timestamp1,DateTimeZone.UTC);
    	//DateTime end=new DateTime(timestamp2,DateTimeZone.UTC);
    	
    	return Minutes.minutesBetween(formatter.parseDateTime(timestamp1),
    	formatter.parseDateTime(timestamp2))
    	.getMinutes();
    }
    
 
 
    
    @DataProvider(name="DP")
    public Object[][] getData() throws Exception
    {
    	return Util.readBundles("src/test/resources/LateDataBundles");
    }    
    
    private String getRunConfInputString(String runConf) throws Exception
    {
        Configuration conf=new Configuration(false);
        conf.addResource(new ByteArrayInputStream(runConf.getBytes()));
        return conf.get("ivoryInPaths");
        
    }
    
    
	private ArrayList<String> getAllRelevantWorkflows(PrismHelper prismHelper,String bundleId,
	String insertionFolder) throws Exception
	{
                
                ArrayList<String> list=new ArrayList<String>();
		
                XOozieClient oozieClient=new XOozieClient(prismHelper.getProcessHelper()
                .getOozieURL());
		BundleJob bundleJob = oozieClient.getBundleJobInfo(bundleId);
    	
		DateTimeFormatter formatter=DateTimeFormat.forPattern("yyyy/MM/dd/HH/mm");
		List<String> actualNominalTimes=new ArrayList<String>();
		
		for(CoordinatorJob job:bundleJob.getCoordinators())
		{
			if(job.getAppName().contains("DEFAULT"))
			{
				
				CoordinatorJob coordJob=oozieClient.getCoordJobInfo(job.getId());
                                
                
				
				
				for(CoordinatorAction action:coordJob.getActions())
				{
                                        CoordinatorAction actionMan=oozieClient
                                        .getCoordActionInfo(action.getId());
                                        
                                        if(!(actionMan.getStatus().equals(CoordinatorAction
                                        .Status.WAITING) ||
                                        actionMan.getStatus().equals(CoordinatorAction.Status
                                        .READY) || actionMan
                                        .getStatus().equals(CoordinatorAction.Status.SUBMITTED) ))
                                        {
                                        	
                                        	
                                            if(getRunConfInputString(oozieClient.getJobInfo
                                            (action.getExternalId())
                                            .getConf()).contains(insertionFolder))
                                            {
                                            
                                               list.add(actionMan.getExternalId()); 
                                                
                                            }
                                        }
				}
			}
		}
                
                return list;
        }    
    
    
}
*/
