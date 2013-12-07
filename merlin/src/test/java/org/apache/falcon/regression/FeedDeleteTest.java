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
//import com.inmobi.qa.airavatqa.core.ENTITY_TYPE;
//import com.inmobi.qa.airavatqa.core.EntityHelperFactory;
//import com.inmobi.qa.airavatqa.core.ServiceResponse;
//import com.inmobi.qa.airavatqa.core.Util;
//import com.inmobi.qa.airavatqa.core.Util.URLS;
//import com.inmobi.qa.airavatqa.interfaces.entity.IEntityManagerHelper;
//import java.lang.reflect.Method;
//import java.util.ArrayList;
//import java.util.Collections;
//import org.testng.Assert;
//import org.testng.annotations.DataProvider;
//import org.testng.annotations.Test;
//
//public class FeedDeleteTest {
//    
//    IEntityManagerHelper clusterHelper=EntityHelperFactory.getEntityHelper(ENTITY_TYPE.CLUSTER);
//    IEntityManagerHelper feedHelper=EntityHelperFactory.getEntityHelper(ENTITY_TYPE.DATA);
//    
//    public void submitCluster(Bundle bundle) throws Exception
//    {
//        //submit the cluster
//        ServiceResponse response=clusterHelper.submitEntity(URLS.SUBMIT_URL,
// bundle.getClusterData());
//        Assert.assertEquals(Util.parseResponse(response).getStatusCode(),200);
//        Assert.assertNotNull(Util.parseResponse(response).getMessage());
//    }
//    
//    @Test(groups={"0.1","0.2"},dataProvider="DP")
//    public void deleteFeed(Bundle bundle) throws Exception
//    {
//       bundle.generateUniqueBundle();
//       submitCluster(bundle);
//       
//       ArrayList<String> feedStoreInitialState=Util.getDataSetStoreInfo();
//       ArrayList<String> feedArchiveInitialState=Util.getDataSetArchiveInfo();
//       
//       //submit feed
//       String feed=Util.getInputFeedFromBundle(bundle);
//       ServiceResponse response=feedHelper.submitEntity(URLS.SUBMIT_URL,feed);
//       Util.assertSucceeded(response);
//    
//       //delete feed
//       response=feedHelper.delete(URLS.DELETE_URL, feed);
//       Util.assertSucceeded(response);
//       
//       ArrayList<String> feedStoreFinalState=Util.getDataSetStoreInfo();
//       ArrayList<String> feedArchiveFinalState=Util.getDataSetArchiveInfo();
//       
//       //now to check that the process store has not changes from the initial one.
//       Collections.sort(feedStoreFinalState);Collections.sort(feedStoreInitialState);
//       Assert.assertTrue(feedStoreFinalState.equals(feedStoreInitialState),
// "The process store values are not the
// same!");
//       
//       //now to check that there is only one file that has been added to process archive and
// that has the same name
// as our deleted file
//       feedArchiveFinalState.removeAll(feedArchiveInitialState);
//       Assert.assertEquals(feedArchiveFinalState.size(),1,"more than 1 different files were
// found!");
//       Assert.assertTrue(feedArchiveFinalState.get(0).contains(Util.readDatasetName(feed)),
// "seems like some other file was being archived upon deletion!");
//       
//       
//       
//    }
//    
//    
//    @Test(groups={"0.1","0.2"},dataProvider="DP")
//    public void deleteScheduledFeed(Bundle bundle) throws Exception
//    {
//       bundle.generateUniqueBundle();
//       submitCluster(bundle);
//       
//       ArrayList<String> feedStoreInitialState=Util.getDataSetStoreInfo();
//       ArrayList<String> feedArchiveInitialState=Util.getDataSetArchiveInfo();
//       
//       //submit feed
//       String feed=Util.getInputFeedFromBundle(bundle);
//       ServiceResponse response=feedHelper.submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL,feed);
//       Util.assertSucceeded(response);
//    
//       //delete feed
//       response=feedHelper.delete(URLS.DELETE_URL, feed);
//       Util.assertSucceeded(response);
//       
//       ArrayList<String> feedStoreFinalState=Util.getDataSetStoreInfo();
//       ArrayList<String> feedArchiveFinalState=Util.getDataSetArchiveInfo();
//       
//       //now to check that the process store has not changes from the initial one.
//       Collections.sort(feedStoreFinalState);Collections.sort(feedStoreInitialState);
//       Assert.assertTrue(feedStoreFinalState.equals(feedStoreInitialState),
// "The process store values are not the
// same!");
//       
//       //now to check that there is only one file that has been added to process archive and
// that has the same name
// as our deleted file
//       feedArchiveFinalState.removeAll(feedArchiveInitialState);
//       Assert.assertEquals(feedArchiveFinalState.size(),1,"more than 1 different files were
// found!");
//       Assert.assertTrue(feedArchiveFinalState.get(0).contains(Util.readDatasetName(feed)),
// "seems like some other file was being archived upon deletion!");
//       
//       
//       
//    }
//    
//    @Test(groups={"0.1","0.2"},dataProvider="DP")
//    public void deleteAlreadyDeletedFeed(Bundle bundle) throws Exception
//    {
//       bundle.generateUniqueBundle();
//       submitCluster(bundle);
//       
//       ArrayList<String> feedStoreInitialState=Util.getDataSetStoreInfo();
//       ArrayList<String> feedArchiveInitialState=Util.getDataSetArchiveInfo();
//       
//       //submit feed
//       String feed=Util.getInputFeedFromBundle(bundle);
//       ServiceResponse response=feedHelper.submitEntity(URLS.SUBMIT_URL,feed);
//       Util.assertSucceeded(response);
//    
//       //delete feed
//       response=feedHelper.delete(URLS.DELETE_URL, feed);
//       Util.assertSucceeded(response);
//       
//       ArrayList<String> feedStoreFinalState=Util.getDataSetStoreInfo();
//       ArrayList<String> feedArchiveFinalState=Util.getDataSetArchiveInfo();
//       
//       //now to check that the process store has not changes from the initial one.
//       Collections.sort(feedStoreFinalState);Collections.sort(feedStoreInitialState);
//       Assert.assertTrue(feedStoreFinalState.equals(feedStoreInitialState),
// "The process store values are not the
// same!");
//       
//       //now to check that there is only one file that has been added to process archive and
// that has the same name
// as our deleted file
//       feedArchiveFinalState.removeAll(feedArchiveInitialState);
//       Assert.assertEquals(feedArchiveFinalState.size(),1,"more than 1 different files were
// found!");
//       Assert.assertTrue(feedArchiveFinalState.get(0).contains(Util.readDatasetName(feed)),
// "seems like some other file was being archived upon deletion!");
//       
//       response=feedHelper.delete(URLS.DELETE_URL,feed);
//       Util.assertFailed(response);
//       
//       
//    }
//    
//    @Test(groups={"0.1","0.2"},dataProvider="DP")
//    public void deleteNonExistentFeed(Bundle bundle) throws Exception
//    {
//       bundle.generateUniqueBundle();
//       submitCluster(bundle);
//       
//       ArrayList<String> feedStoreInitialState=Util.getDataSetStoreInfo();
//       ArrayList<String> feedArchiveInitialState=Util.getDataSetArchiveInfo();
//       
//       //submit feed
//       String feed=Util.getInputFeedFromBundle(bundle);
//    
//       //delete feed
//       ServiceResponse response=feedHelper.delete(URLS.DELETE_URL, feed);
//       Util.assertFailed(response);
//       
//       ArrayList<String> feedStoreFinalState=Util.getDataSetStoreInfo();
//       ArrayList<String> feedArchiveFinalState=Util.getDataSetArchiveInfo();
//       
//       //now to check that the process store has not changes from the initial one.
//       Collections.sort(feedStoreFinalState);Collections.sort(feedStoreInitialState);
//       Assert.assertTrue(feedStoreFinalState.equals(feedStoreInitialState),
// "The process store values are not the
// same!");
//       
//       //now to check that there is only one file that has been added to process archive and
// that has the same name
// as our deleted file
//       Collections.sort(feedArchiveFinalState);Collections.sort(feedArchiveInitialState);
//       Assert.assertTrue(feedArchiveFinalState.equals(feedArchiveInitialState),
// "The process store values are not
// the same!");
// 
//       
//    }
//    
//    @Test(groups={"0.1","0.2"},dataProvider="DP")
//    public void deleteSuspendedFeed(Bundle bundle) throws Exception
//    {
//       bundle.generateUniqueBundle();
//       submitCluster(bundle);
//       
//       ArrayList<String> feedStoreInitialState=Util.getDataSetStoreInfo();
//       ArrayList<String> feedArchiveInitialState=Util.getDataSetArchiveInfo();
//       
//       //submit feed
//       String feed=Util.getInputFeedFromBundle(bundle);
//       
//       ServiceResponse response=feedHelper.submitEntity(URLS.SUBMIT_URL, feed);
//       Util.assertSucceeded(response);
//       
//       response=feedHelper.schedule(URLS.SCHEDULE_URL, feed);
//       Util.assertSucceeded(response);
//       
//       Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(feed),
// "RUNNING").get(0).contains("RUNNING"));
//       
//       response=feedHelper.suspend(URLS.SUSPEND_URL,feed);
//       Util.assertSucceeded(response);
//       
//       Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(feed),
// "SUSPENDED").get(0).contains("SUSPENDED"));
//       
//       //delete feed
//       response=feedHelper.delete(URLS.DELETE_URL, feed);
//       Util.assertSucceeded(response);
//       
//       ArrayList<String> feedStoreFinalState=Util.getDataSetStoreInfo();
//       ArrayList<String> feedArchiveFinalState=Util.getDataSetArchiveInfo();
//       
//       //now to check that the process store has not changes from the initial one.
//       Collections.sort(feedStoreFinalState);Collections.sort(feedStoreInitialState);
//       Assert.assertTrue(feedStoreFinalState.equals(feedStoreInitialState),
// "The process store values are not the
// same!");
//       
//        //now to check that there is only one file that has been added to process archive and
// that has the same
// name as our deleted file
//       feedArchiveFinalState.removeAll(feedArchiveInitialState);
//       Assert.assertEquals(feedArchiveFinalState.size(),1,"more than 1 different files were
// found!");
//       Assert.assertTrue(feedArchiveFinalState.get(0).contains(Util.readDatasetName(feed)),
// "seems like some other file was being archived upon deletion!");
//       
//       
//    }
//    
//    @DataProvider(name="DP")
//    public Object[][] getBundles(Method m) throws Exception
//    {
//        return Util.readBundles();
//    }
//    
//    
//    
//    
//    
//    
//    
//}
