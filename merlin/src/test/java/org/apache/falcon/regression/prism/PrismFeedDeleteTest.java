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

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.falcon.regression.prism;


import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.generated.feed.ActionType;
import org.apache.falcon.regression.core.generated.feed.ClusterType;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.helpers.PrismHelper;
import org.apache.falcon.regression.core.interfaces.IEntityManagerHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.supportClasses.ENTITY_TYPE;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.core.util.XmlUtil;
import org.testng.Assert;
import org.testng.TestNGException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class PrismFeedDeleteTest {


    @BeforeMethod(alwaysRun = true)
    public void testName(Method method) {
        Util.print("test name: " + method.getName());
    }


    PrismHelper prismHelper = new PrismHelper("prism.properties");
    ColoHelper UA1ColoHelper = new ColoHelper("mk-qa.config.properties");
    ColoHelper UA2ColoHelper = new ColoHelper("ivoryqa-1.config.properties");
    ColoHelper UA3ColoHelper = new ColoHelper("gs1001.config.properties");

    ColoHelper ua1 = new ColoHelper("mk-qa.config.properties");

    ColoHelper ua2 = new ColoHelper("ivoryqa-1.config.properties");
    ColoHelper ua3 = new ColoHelper("gs1001.config.properties");
/* NOTE: All test cases assume that there are two entities scheduled in each colo */

    @Test(dataProvider = "DP", groups = {"multiCluster"})
    public void testUA1FeedDeleteInBothColos(Bundle bundle) throws Exception {
        //create a UA1 bundle
        Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
        UA1Bundle.generateUniqueBundle();


        //now submit the thing to prism


        for (String cluster : UA1Bundle.getClusters()) {
            Util.assertSucceeded(
                    prismHelper.getClusterHelper().submitEntity(Util.URLS.SUBMIT_URL, cluster));
        }

        Util.assertSucceeded(
                prismHelper.getFeedHelper()
                        .submitEntity(Util.URLS.SUBMIT_URL, UA1Bundle.getDataSets().get(0)));
        //fetch the initial store and archive state for prism
        List<String> initialPrismStore = prismHelper.getFeedHelper().getStoreInfo();
        List<String> initialPrismArchiveStore = prismHelper.getFeedHelper().getArchiveInfo();

        //fetch the initial store and archive for both colos
        List<String> initialUA1Store = UA1ColoHelper.getFeedHelper().getStoreInfo();
        List<String> initialUA1ArchiveStore = UA1ColoHelper.getFeedHelper().getArchiveInfo();

        List<String> initialUA2Store = UA2ColoHelper.getFeedHelper().getStoreInfo();
        List<String> initialUA2ArchiveStore = UA2ColoHelper.getFeedHelper().getArchiveInfo();

        //lets now delete the cluster from both colos
        Util.assertSucceeded(prismHelper.getFeedHelper()
                .delete(Util.URLS.DELETE_URL, UA1Bundle.getDataSets().get(0)));

        //now lets get the final states
        List<String> finalPrismStore = prismHelper.getFeedHelper().getStoreInfo();
        List<String> finalPrismArchiveStore = prismHelper.getFeedHelper().getArchiveInfo();

        //fetch the final store and archive for both colos
        List<String> finalUA1Store = UA1ColoHelper.getFeedHelper().getStoreInfo();
        List<String> finalUA1ArchiveStore = UA1ColoHelper.getFeedHelper().getArchiveInfo();

        List<String> finalUA2Store = UA2ColoHelper.getFeedHelper().getStoreInfo();
        List<String> finalUA2ArchiveStore = UA2ColoHelper.getFeedHelper().getArchiveInfo();

        //now ensure that data has been deleted from all cluster store and is present in the
        // cluster archives

        String feedName = Util.readDatasetName(UA1Bundle.getDataSets().get(0));
        //prism:
        compareDataStoreStates(initialPrismStore, finalPrismStore, feedName);
        compareDataStoreStates(finalPrismArchiveStore, initialPrismArchiveStore, feedName);

        //UA1:
        compareDataStoreStates(initialUA1Store, finalUA1Store, feedName);
        compareDataStoreStates(finalUA1ArchiveStore, initialUA1ArchiveStore, feedName);

        //UA2:
        compareDataStoresForEquality(initialUA2Store, finalUA2Store);
        compareDataStoresForEquality(finalUA2ArchiveStore, initialUA2ArchiveStore);


    }

    @Test(dataProvider = "DP", groups = {"multiCluster"})
    public void testUA1FeedDeleteWhen1ColoIsDown(Bundle bundle) throws Exception {
        try {
            //create a UA1 bundle
            Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
            UA1Bundle.generateUniqueBundle();

            //now submit the thing to prism
            //Util.assertSucceeded(prismHelper.getClusterHelper().submitEntity(Util.URLS.SUBMIT_URL,
            // UA1Bundle.getClusterData()));

            for (String cluster : UA1Bundle.getClusters()) {
                Util.assertSucceeded(
                        prismHelper.getClusterHelper().submitEntity(Util.URLS.SUBMIT_URL, cluster));
            }

            Util.assertSucceeded(
                    prismHelper.getFeedHelper()
                            .submitEntity(Util.URLS.SUBMIT_URL, UA1Bundle.getDataSets().get(0)));
            //fetch the initial store and archive state for prism
            List<String> initialPrismStore = prismHelper.getFeedHelper().getStoreInfo();
            List<String> initialPrismArchiveStore = prismHelper.getFeedHelper().getArchiveInfo();

            //fetch the initial store and archive for both colos
            List<String> initialUA1Store = UA1ColoHelper.getFeedHelper().getStoreInfo();
            List<String> initialUA1ArchiveStore = UA1ColoHelper.getFeedHelper().getArchiveInfo();

            List<String> initialUA2Store = UA2ColoHelper.getFeedHelper().getStoreInfo();
            List<String> initialUA2ArchiveStore = UA2ColoHelper.getFeedHelper().getArchiveInfo();


            //bring down UA2 colo :P
            Util.shutDownService(UA1ColoHelper.getFeedHelper());

            //lets now delete the cluster from both colos
            Util.assertFailed(prismHelper.getFeedHelper()
                    .delete(Util.URLS.DELETE_URL, UA1Bundle.getDataSets().get(0)));

            //now lets get the final states
            List<String> finalPrismStore = prismHelper.getFeedHelper().getStoreInfo();
            List<String> finalPrismArchiveStore = prismHelper.getFeedHelper().getArchiveInfo();

            //fetch the final store and archive for both colos
            List<String> finalUA1Store = UA1ColoHelper.getFeedHelper().getStoreInfo();
            List<String> finalUA1ArchiveStore = UA1ColoHelper.getFeedHelper().getArchiveInfo();

            List<String> finalUA2Store = UA2ColoHelper.getFeedHelper().getStoreInfo();
            List<String> finalUA2ArchiveStore = UA2ColoHelper.getFeedHelper().getArchiveInfo();

            //now ensure that data has been deleted from all cluster store and is present in the
            // cluster archives

            String clusterName = Util.readDatasetName(UA1Bundle.getDataSets().get(0));
            //prism:
            compareDataStoresForEquality(initialPrismStore, finalPrismStore);
            compareDataStoresForEquality(finalPrismArchiveStore, initialPrismArchiveStore);

            //UA2:
            compareDataStoresForEquality(initialUA2Store, finalUA2Store);
            compareDataStoresForEquality(finalUA2ArchiveStore, initialUA2ArchiveStore);

            //UA1:
            Assert.assertTrue(
                    Arrays.deepEquals(initialUA1Store.toArray(new String[initialUA1Store.size()]),
                            finalUA1Store.toArray(new String[finalUA1Store.size()])));
            Assert.assertTrue(
                    Arrays.deepEquals(initialUA1ArchiveStore
                            .toArray(new String[initialUA1ArchiveStore.size()]),
                            finalUA1ArchiveStore.toArray(new String[finalUA1ArchiveStore.size()])));

            Util.startService(UA1ColoHelper.getFeedHelper());

            Util.assertSucceeded(
                    prismHelper.getFeedHelper()
                            .delete(Util.URLS.DELETE_URL, UA1Bundle.getDataSets().get(0)));

            List<String> ua2ArchivePostUp = UA2ColoHelper.getFeedHelper().getArchiveInfo();
            List<String> ua2StorePostUp = UA2ColoHelper.getFeedHelper().getStoreInfo();

            List<String> ua1ArchivePostUp = UA1ColoHelper.getFeedHelper().getArchiveInfo();
            List<String> ua1StorePostUp = UA1ColoHelper.getFeedHelper().getStoreInfo();

            List<String> prismHelperArchivePostUp = prismHelper.getFeedHelper().getArchiveInfo();
            List<String> prismHelperStorePostUp = prismHelper.getFeedHelper().getStoreInfo();


            compareDataStoreStates(finalPrismStore, prismHelperStorePostUp, clusterName);
            compareDataStoreStates(prismHelperArchivePostUp, finalPrismArchiveStore, clusterName);

            compareDataStoreStates(initialUA1Store, ua1StorePostUp, clusterName);
            compareDataStoreStates(ua1ArchivePostUp, finalUA1ArchiveStore, clusterName);

            compareDataStoresForEquality(finalUA2Store, ua2StorePostUp);
            compareDataStoresForEquality(finalUA2ArchiveStore, ua2ArchivePostUp);


        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        } finally {

            Util.restartService(UA1ColoHelper.getClusterHelper());
        }

    }
  
 /*   
//    @Test(dataProvider="DP",groups = {"multiCluster"})
//    public void testUA1ClusterDeleteWhen1ColoIsDown(Bundle bundle) throws Exception
//    {
//        try{
//        //create a UA1 bundle
//        Bundle UA1Bundle=new Bundle(bundle,UA1ColoHelper.getEnvFileName());
//        UA1Bundle.generateUniqueBundle();
//        
//        //now submit the thing to prism
//        APIResult clusterResult=Util.parseResponse(prismHelper.getFeedHelper().submitEntity
(Util.URLS.SUBMIT_URL,
UA1Bundle.getDataSets().get(0)));
//        //fetch the initial store and archive state for prism
//        List<String> initialPrismStore=prismHelper.getFeedHelper().getStoreInfo();
//        List<String> initialPrismArchiveStore=prismHelper.getFeedHelper().getArchiveInfo();
//        
//        //fetch the initial store and archive for both colos
//        List<String> initialUA1Store=UA1ColoHelper.getClusterHelper().getStoreInfo();
//        List<String> initialUA1ArchiveStore=UA1ColoHelper.getClusterHelper().getArchiveInfo();
//        
//        List<String> initialUA2Store=UA2ColoHelper.getClusterHelper().getStoreInfo();
//        List<String> initialUA2ArchiveStore=UA2ColoHelper.getClusterHelper().getArchiveInfo();
//        
//        
//        //bring down UA2 colo :P
//        Util.shutDownService(UA1ColoHelper.getClusterHelper());
//        
//        //lets now delete the cluster from both colos
//        APIResult clusterDeleteResult=Util.parseResponse(prismHelper.getFeedHelper().delete
(Util.URLS.DELETE_URL,
UA1Bundle.getDataSets().get(0)));
//        
//        //now lets get the final states
//        List<String> finalPrismStore=prismHelper.getFeedHelper().getStoreInfo();
//        List<String> finalPrismArchiveStore=prismHelper.getFeedHelper().getArchiveInfo();
//        
//        //fetch the final store and archive for both colos
//        List<String> finalUA1Store=UA1ColoHelper.getClusterHelper().getStoreInfo();
//        List<String> finalUA1ArchiveStore=UA1ColoHelper.getClusterHelper().getArchiveInfo();
//        
//        List<String> finalUA2Store=UA2ColoHelper.getClusterHelper().getStoreInfo();
//        List<String> finalUA2ArchiveStore=UA2ColoHelper.getClusterHelper().getArchiveInfo();
//        
//        //now ensure that data has been deleted from all cluster store and is present in the
cluster archives
//        
//        String clusterName=Util.readDatasetName(bundle.getDataSets().get(0));
//        //prism:
//        compareDataStoreStates(initialPrismStore,finalPrismStore,clusterName);
//        compareDataStoreStates(finalPrismArchiveStore,initialPrismArchiveStore, clusterName);
//        
//        //UA2:
//        compareDataStoreStates(initialUA2Store, finalUA2Store, clusterName);
//        compareDataStoreStates(finalUA2ArchiveStore,initialUA2ArchiveStore, clusterName);
//        
//        //UA1:
//          Assert.assertTrue(Arrays.deepEquals(initialUA1Store.toArray(new
String[initialUA1Store.size()]),
finalUA1Store.toArray(new String[finalUA1Store.size()])));
//          Assert.assertTrue(Arrays.deepEquals(initialUA1ArchiveStore.toArray(new
String[initialUA1ArchiveStore.size
()]),finalUA1ArchiveStore.toArray(new String[finalUA1ArchiveStore.size()])));
//        }
//        catch(Exception e)
//        {
//            e.printStackTrace();
//            throw new TestNGException(e.getMessage());
//        }
//        finally {
//            
//            Util.restartService(UA1ColoHelper.getClusterHelper());
//        }
//
//    }  
*/

    @Test(dataProvider = "DP", groups = {"multiCluster"})
    public void testUA1FeedDeleteAlreadyDeletedFeed(Bundle bundle) throws Exception {
        try {
            //create a UA1 bundle
            Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
            UA1Bundle.generateUniqueBundle();

            //now submit the thing to prism
            //Util.assertSucceeded(prismHelper.getClusterHelper().submitEntity(Util.URLS.SUBMIT_URL,
            // UA1Bundle.getClusterData()));

            for (String cluster : UA1Bundle.getClusters()) {
                Util.assertSucceeded(
                        prismHelper.getClusterHelper().submitEntity(Util.URLS.SUBMIT_URL, cluster));
            }

            Util.assertSucceeded(
                    prismHelper.getFeedHelper()
                            .submitEntity(Util.URLS.SUBMIT_URL, UA1Bundle.getDataSets().get(0)));
            Util.assertSucceeded(
                    prismHelper.getFeedHelper()
                            .delete(Util.URLS.DELETE_URL, UA1Bundle.getDataSets().get(0)));

            //fetch the initial store and archive state for prism
            List<String> initialPrismStore = prismHelper.getFeedHelper().getStoreInfo();
            List<String> initialPrismArchiveStore = prismHelper.getFeedHelper().getArchiveInfo();

            //fetch the initial store and archive for both colos
            List<String> initialUA1Store = UA1ColoHelper.getFeedHelper().getStoreInfo();
            List<String> initialUA1ArchiveStore = UA1ColoHelper.getFeedHelper().getArchiveInfo();

            List<String> initialUA2Store = UA2ColoHelper.getFeedHelper().getStoreInfo();
            List<String> initialUA2ArchiveStore = UA2ColoHelper.getFeedHelper().getArchiveInfo();

            Util.assertSucceeded(
                    prismHelper.getFeedHelper()
                            .delete(Util.URLS.DELETE_URL, UA1Bundle.getDataSets().get(0)));

            //now lets get the final states
            List<String> finalPrismStore = prismHelper.getFeedHelper().getStoreInfo();
            List<String> finalPrismArchiveStore = prismHelper.getFeedHelper().getArchiveInfo();

            //fetch the final store and archive for both colos
            List<String> finalUA1Store = UA1ColoHelper.getFeedHelper().getStoreInfo();
            List<String> finalUA1ArchiveStore = UA1ColoHelper.getFeedHelper().getArchiveInfo();

            List<String> finalUA2Store = UA2ColoHelper.getFeedHelper().getStoreInfo();
            List<String> finalUA2ArchiveStore = UA2ColoHelper.getFeedHelper().getArchiveInfo();

            //now ensure that data has been deleted from all cluster store and is present in the
            // cluster archives

            String clusterName = Util.readDatasetName(UA1Bundle.getDataSets().get(0));
            //prism:
            Assert.assertTrue(Arrays.deepEquals(
                    initialPrismStore.toArray(new String[initialPrismStore.size()]),
                    finalPrismStore.toArray(new String[finalPrismStore.size()])));
            Assert.assertTrue(
                    Arrays.deepEquals(initialPrismArchiveStore
                            .toArray(new String[initialPrismArchiveStore.size()]),
                            finalPrismArchiveStore
                                    .toArray(new String[finalPrismArchiveStore.size()])));
            //UA2:
            Assert.assertTrue(
                    Arrays.deepEquals(initialUA2Store.toArray(new String[initialUA2Store.size()]),
                            finalUA2Store.toArray(new String[finalUA2Store.size()])));
            Assert.assertTrue(
                    Arrays.deepEquals(initialUA2ArchiveStore
                            .toArray(new String[initialUA2ArchiveStore.size()]),
                            finalUA2ArchiveStore.toArray(new String[finalUA2ArchiveStore.size()])));
            //UA1:
            Assert.assertTrue(
                    Arrays.deepEquals(initialUA1Store.toArray(new String[initialUA1Store.size()]),
                            finalUA1Store.toArray(new String[finalUA1Store.size()])));
            Assert.assertTrue(
                    Arrays.deepEquals(initialUA1ArchiveStore
                            .toArray(new String[initialUA1ArchiveStore.size()]),
                            finalUA1ArchiveStore.toArray(new String[finalUA1ArchiveStore.size()])));

        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        } finally {

            Util.restartService(UA1ColoHelper.getClusterHelper());
        }

    }


    @Test(dataProvider = "DP", groups = {"multiCluster"})
    public void testUA1FeedDeleteTwiceWhen1ColoIsDownDuring1stDelete(Bundle bundle)
    throws Exception {
        try {
            //create a UA1 bundle
            Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
            UA1Bundle.generateUniqueBundle();

            for (String cluster : UA1Bundle.getClusters()) {
                Util.assertSucceeded(
                        prismHelper.getClusterHelper().submitEntity(Util.URLS.SUBMIT_URL, cluster));
            }

            //Util.assertSucceeded(prismHelper.getClusterHelper().submitEntity(Util.URLS.SUBMIT_URL,
            // UA1Bundle.getClusterData()));
            Util.assertSucceeded(
                    prismHelper.getFeedHelper()
                            .submitEntity(Util.URLS.SUBMIT_URL, UA1Bundle.getDataSets().get(0)));

            Util.shutDownService(UA1ColoHelper.getClusterHelper());


            //lets now delete the cluster from both colos
            Util.assertFailed(prismHelper.getFeedHelper()
                    .delete(Util.URLS.DELETE_URL, UA1Bundle.getDataSets().get(0)));

            //now lets get the final states
            List<String> initialPrismStore = prismHelper.getFeedHelper().getStoreInfo();
            List<String> initialPrismArchiveStore = prismHelper.getFeedHelper().getArchiveInfo();

            //fetch the initial store and archive for both colos
            List<String> initialUA1Store = UA1ColoHelper.getFeedHelper().getStoreInfo();
            List<String> initialUA1ArchiveStore = UA1ColoHelper.getFeedHelper().getArchiveInfo();

            List<String> initialUA2Store = UA2ColoHelper.getFeedHelper().getStoreInfo();
            List<String> initialUA2ArchiveStore = UA2ColoHelper.getFeedHelper().getArchiveInfo();

            //start up service
            Util.startService(UA1ColoHelper.getFeedHelper());

            //delete again
            Util.assertSucceeded(
                    prismHelper.getFeedHelper()
                            .delete(Util.URLS.DELETE_URL, UA1Bundle.getDataSets().get(0)));

            //get final states
            List<String> finalPrismStore = prismHelper.getFeedHelper().getStoreInfo();
            List<String> finalPrismArchiveStore = prismHelper.getFeedHelper().getArchiveInfo();

            //fetch the final store and archive for both colos
            List<String> finalUA1Store = UA1ColoHelper.getFeedHelper().getStoreInfo();
            List<String> finalUA1ArchiveStore = UA1ColoHelper.getFeedHelper().getArchiveInfo();

            List<String> finalUA2Store = UA2ColoHelper.getFeedHelper().getStoreInfo();
            List<String> finalUA2ArchiveStore = UA2ColoHelper.getFeedHelper().getArchiveInfo();

            //now ensure that data has been deleted from all cluster store and is present in the
            // cluster archives

            String clusterName = Util.readDatasetName(UA1Bundle.getDataSets().get(0));
            //prism:

            compareDataStoreStates(initialPrismStore, finalPrismStore, clusterName);
            compareDataStoreStates(finalPrismArchiveStore, initialPrismArchiveStore, clusterName);

            //UA2:
            Assert.assertTrue(
                    Arrays.deepEquals(initialUA2Store.toArray(new String[initialUA2Store.size()]),
                            finalUA2Store.toArray(new String[finalUA2Store.size()])));
            Assert.assertTrue(
                    Arrays.deepEquals(initialUA2ArchiveStore
                            .toArray(new String[initialUA2ArchiveStore.size()]),
                            finalUA2ArchiveStore.toArray(new String[finalUA2ArchiveStore.size()])));

            //UA1:
            compareDataStoreStates(initialUA1Store, finalUA1Store, clusterName);
            compareDataStoreStates(finalUA1ArchiveStore, initialUA1ArchiveStore, clusterName);

        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        } finally {

            Util.restartService(UA1ColoHelper.getClusterHelper());
        }

    }

    @Test(dataProvider = "DP", groups = {"multiCluster"})
    public void testUA1FeedDeleteNonExistent(Bundle bundle) throws Exception {
        try {
            //create a UA1 bundle
            Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
            UA1Bundle.generateUniqueBundle();


            //now lets get the final states
            List<String> initialPrismStore = prismHelper.getFeedHelper().getStoreInfo();
            List<String> initialPrismArchiveStore = prismHelper.getFeedHelper().getArchiveInfo();

            //fetch the initial store and archive for both colos
            List<String> initialUA1Store = UA1ColoHelper.getFeedHelper().getStoreInfo();
            List<String> initialUA1ArchiveStore = UA1ColoHelper.getFeedHelper().getArchiveInfo();

            List<String> initialUA2Store = UA2ColoHelper.getFeedHelper().getStoreInfo();
            List<String> initialUA2ArchiveStore = UA2ColoHelper.getFeedHelper().getArchiveInfo();

            //APIResult clusterResult=Util.parseResponse(prismHelper.getFeedHelper().submitEntity
            // (Util.URLS
            // .SUBMIT_URL,UA1Bundle.getClusterData()));
            //delete
            Util.assertSucceeded(
                    prismHelper.getFeedHelper()
                            .delete(Util.URLS.DELETE_URL, UA1Bundle.getDataSets().get(0)));

            //get final states
            List<String> finalPrismStore = prismHelper.getFeedHelper().getStoreInfo();
            List<String> finalPrismArchiveStore = prismHelper.getFeedHelper().getArchiveInfo();

            //fetch the final store and archive for both colos
            List<String> finalUA1Store = UA1ColoHelper.getFeedHelper().getStoreInfo();
            List<String> finalUA1ArchiveStore = UA1ColoHelper.getFeedHelper().getArchiveInfo();

            List<String> finalUA2Store = UA2ColoHelper.getFeedHelper().getStoreInfo();
            List<String> finalUA2ArchiveStore = UA2ColoHelper.getFeedHelper().getArchiveInfo();

            //now ensure that data has been deleted from all cluster store and is present in the
            // cluster archives

            String clusterName = Util.readDatasetName(UA1Bundle.getDataSets().get(0));
            //prism:
            Assert.assertTrue(Arrays.deepEquals(
                    initialPrismStore.toArray(new String[initialPrismStore.size()]),
                    finalPrismStore.toArray(new String[finalPrismStore.size()])));
            Assert.assertTrue(
                    Arrays.deepEquals(initialPrismArchiveStore
                            .toArray(new String[initialPrismArchiveStore.size()]),
                            finalPrismArchiveStore
                                    .toArray(new String[finalPrismArchiveStore.size()])));

            //UA2:
            Assert.assertTrue(
                    Arrays.deepEquals(initialUA2Store.toArray(new String[initialUA2Store.size()]),
                            finalUA2Store.toArray(new String[finalUA2Store.size()])));
            Assert.assertTrue(
                    Arrays.deepEquals(initialUA2ArchiveStore
                            .toArray(new String[initialUA2ArchiveStore.size()]),
                            finalUA2ArchiveStore.toArray(new String[finalUA2ArchiveStore.size()])));

            //UA1:
            Assert.assertTrue(
                    Arrays.deepEquals(initialUA1Store.toArray(new String[initialUA1Store.size()]),
                            finalUA1Store.toArray(new String[finalUA1Store.size()])));
            Assert.assertTrue(
                    Arrays.deepEquals(initialUA1ArchiveStore
                            .toArray(new String[initialUA1ArchiveStore.size()]),
                            finalUA1ArchiveStore.toArray(new String[finalUA1ArchiveStore.size()])));


        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        }

    }


    @Test(groups = {"multiCluster"})
    public void testUA1FeedDeleteNonExistentWhen1ColoIsDownDuringDelete() throws Exception {
        Bundle b1 = (Bundle) Util.readELBundles()[0][0];
        b1.generateUniqueBundle();
        Bundle b2 = (Bundle) Util.readELBundles()[0][0];
        b2.generateUniqueBundle();

        try {
            b1 = new Bundle(b1, UA1ColoHelper.getEnvFileName());
            b2 = new Bundle(b2, UA2ColoHelper.getEnvFileName());

            b1.setCLusterColo("ua1");
            Util.print("cluster b1: " + b1.getClusters().get(0));

            ServiceResponse r = prismHelper.getClusterHelper()
                    .submitEntity(URLS.SUBMIT_URL, b1.getClusters().get(0));
            Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

            b2.setCLusterColo("ua2");
            Util.print("cluster b2: " + b2.getClusters().get(0));
            r = prismHelper.getClusterHelper()
                    .submitEntity(URLS.SUBMIT_URL, b2.getClusters().get(0));
            Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

            String startTimeUA1 = "2012-10-01T12:00Z";
            String startTimeUA2 = "2012-10-01T12:00Z";

            String feed = b1.getDataSets().get(0);
            feed = InstanceUtil.setFeedCluster(feed,
                    XmlUtil.createValidity("2012-10-01T12:00Z", "2010-01-01T00:00Z"),
                    XmlUtil.createRtention("days(10000)", ActionType.DELETE), null,
                    ClusterType.SOURCE, null, null);
            feed = InstanceUtil
                    .setFeedCluster(feed, XmlUtil.createValidity(startTimeUA1, "2099-10-01T12:10Z"),
                            XmlUtil.createRtention("days(10000)", ActionType.DELETE),
                            Util.readClusterName(b1.getClusters().get(0)), ClusterType.SOURCE,
                            "${cluster.colo}",
                            "/localDC/rc/billing/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            feed = InstanceUtil
                    .setFeedCluster(feed, XmlUtil.createValidity(startTimeUA2, "2099-10-01T12:25Z"),
                            XmlUtil.createRtention("days(10000)", ActionType.DELETE),
                            Util.readClusterName(b2.getClusters().get(0)), ClusterType.TARGET, null,
                            "/clusterPath/localDC/rc/billing/${YEAR}/${MONTH}/${DAY}/${HOUR}/$" +
                                    "{MINUTE}");

            //	Util.print("feed: "+feed);

            Util.shutDownService(UA1ColoHelper.getFeedHelper());

            ServiceResponse response =
                    prismHelper.getFeedHelper().delete(Util.URLS.DELETE_URL, feed);
            Util.assertSucceeded(response);
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        } finally {

            Util.restartService(UA1ColoHelper.getFeedHelper());

        }

    }


    @Test(dataProvider = "DP", groups = {"multiCluster"})
    public void testDeleteFeedScheduledInOneColo(Bundle bundle) throws Exception {
        //create a UA1 bundle
        Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
        UA1Bundle.generateUniqueBundle();

        Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());
        UA2Bundle.generateUniqueBundle();

        submitAndScheduleFeed(UA1Bundle);
        submitAndScheduleFeed(UA2Bundle);

        //fetch the initial store and archive state for prism
        List<String> initialPrismStore = prismHelper.getFeedHelper().getStoreInfo();
        List<String> initialPrismArchiveStore = prismHelper.getFeedHelper().getArchiveInfo();

        //fetch the initial store and archive for both colos
        List<String> initialUA1Store = UA1ColoHelper.getFeedHelper().getStoreInfo();
        List<String> initialUA1ArchiveStore = UA1ColoHelper.getFeedHelper().getArchiveInfo();

        List<String> initialUA2Store = UA2ColoHelper.getFeedHelper().getStoreInfo();
        List<String> initialUA2ArchiveStore = UA2ColoHelper.getFeedHelper().getArchiveInfo();

        //lets now delete the cluster from both colos
        Util.assertSucceeded(prismHelper.getFeedHelper()
                .delete(Util.URLS.DELETE_URL, UA1Bundle.getDataSets().get(0)));

        //now lets get the final states
        List<String> finalPrismStore = prismHelper.getFeedHelper().getStoreInfo();
        List<String> finalPrismArchiveStore = prismHelper.getFeedHelper().getArchiveInfo();

        //fetch the final store and archive for both colos
        List<String> finalUA1Store = UA1ColoHelper.getFeedHelper().getStoreInfo();
        List<String> finalUA1ArchiveStore = UA1ColoHelper.getFeedHelper().getArchiveInfo();

        List<String> finalUA2Store = UA2ColoHelper.getFeedHelper().getStoreInfo();
        List<String> finalUA2ArchiveStore = UA2ColoHelper.getFeedHelper().getArchiveInfo();

        //now ensure that data has been deleted from all cluster store and is present in the
        // cluster archives

        String clusterName = Util.readDatasetName(UA1Bundle.getDataSets().get(0));
        //prism:
        compareDataStoreStates(initialPrismStore, finalPrismStore, clusterName);
        compareDataStoreStates(finalPrismArchiveStore, initialPrismArchiveStore, clusterName);

        //UA1:
        compareDataStoreStates(initialUA1Store, finalUA1Store, clusterName);
        compareDataStoreStates(finalUA1ArchiveStore, initialUA1ArchiveStore, clusterName);

        //UA2:
        compareDataStoresForEquality(initialUA2Store, finalUA2Store);
        compareDataStoresForEquality(finalUA2ArchiveStore, initialUA2ArchiveStore);


    }

    @Test(dataProvider = "DP", groups = {"multiCluster"})
    public void testDeleteFeedSuspendedInOneColo(Bundle bundle) throws Exception {
        //create a UA1 bundle
        Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
        UA1Bundle.generateUniqueBundle();

        Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());
        UA2Bundle.generateUniqueBundle();

        submitAndScheduleFeed(UA1Bundle);
        submitAndScheduleFeed(UA2Bundle);


        //suspend UA1 colo thingy
        Util.assertSucceeded(prismHelper.getFeedHelper()
                .suspend(URLS.SUSPEND_URL, UA1Bundle.getDataSets().get(0)));

        //fetch the initial store and archive state for prism
        List<String> initialPrismStore = prismHelper.getFeedHelper().getStoreInfo();
        List<String> initialPrismArchiveStore = prismHelper.getFeedHelper().getArchiveInfo();

        //fetch the initial store and archive for both colos
        List<String> initialUA1Store = UA1ColoHelper.getFeedHelper().getStoreInfo();
        List<String> initialUA1ArchiveStore = UA1ColoHelper.getFeedHelper().getArchiveInfo();

        List<String> initialUA2Store = UA2ColoHelper.getFeedHelper().getStoreInfo();
        List<String> initialUA2ArchiveStore = UA2ColoHelper.getFeedHelper().getArchiveInfo();

        //lets now delete the cluster from both colos
        Util.assertSucceeded(prismHelper.getFeedHelper()
                .delete(Util.URLS.DELETE_URL, UA1Bundle.getDataSets().get(0)));

        //now lets get the final states
        List<String> finalPrismStore = prismHelper.getFeedHelper().getStoreInfo();
        List<String> finalPrismArchiveStore = prismHelper.getFeedHelper().getArchiveInfo();

        //fetch the final store and archive for both colos
        List<String> finalUA1Store = UA1ColoHelper.getFeedHelper().getStoreInfo();
        List<String> finalUA1ArchiveStore = UA1ColoHelper.getFeedHelper().getArchiveInfo();

        List<String> finalUA2Store = UA2ColoHelper.getFeedHelper().getStoreInfo();
        List<String> finalUA2ArchiveStore = UA2ColoHelper.getFeedHelper().getArchiveInfo();

        //now ensure that data has been deleted from all cluster store and is present in the
        // cluster archives

        String clusterName = Util.readDatasetName(UA1Bundle.getDataSets().get(0));
        //prism:
        compareDataStoreStates(initialPrismStore, finalPrismStore, clusterName);
        compareDataStoreStates(finalPrismArchiveStore, initialPrismArchiveStore, clusterName);

        //UA1:
        compareDataStoreStates(initialUA1Store, finalUA1Store, clusterName);
        compareDataStoreStates(finalUA1ArchiveStore, initialUA1ArchiveStore, clusterName);

        //UA2:
        compareDataStoresForEquality(initialUA2Store, finalUA2Store);
        compareDataStoresForEquality(finalUA2ArchiveStore, initialUA2ArchiveStore);


    }


    @Test(dataProvider = "DP", groups = {"multiCluster"})
    public void testDeleteFeedSuspendedInOneColoWhileBothFeedsAreSuspended(Bundle bundle)
    throws Exception {
        //create a UA1 bundle
        Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
        UA1Bundle.generateUniqueBundle();

        Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());
        UA2Bundle.generateUniqueBundle();

        submitAndScheduleFeed(UA1Bundle);
        submitAndScheduleFeed(UA2Bundle);


        //suspend UA1 colo thingy
        Util.assertSucceeded(prismHelper.getFeedHelper()
                .suspend(URLS.SUSPEND_URL, UA1Bundle.getDataSets().get(0)));
        Util.assertSucceeded(prismHelper.getFeedHelper()
                .suspend(URLS.SUSPEND_URL, UA2Bundle.getDataSets().get(0)));

        //fetch the initial store and archive state for prism
        List<String> initialPrismStore = prismHelper.getFeedHelper().getStoreInfo();
        List<String> initialPrismArchiveStore = prismHelper.getFeedHelper().getArchiveInfo();

        //fetch the initial store and archive for both colos
        List<String> initialUA1Store = UA1ColoHelper.getFeedHelper().getStoreInfo();
        List<String> initialUA1ArchiveStore = UA1ColoHelper.getFeedHelper().getArchiveInfo();

        List<String> initialUA2Store = UA2ColoHelper.getFeedHelper().getStoreInfo();
        List<String> initialUA2ArchiveStore = UA2ColoHelper.getFeedHelper().getArchiveInfo();

        //lets now delete the cluster from both colos
        Util.assertSucceeded(prismHelper.getFeedHelper()
                .delete(Util.URLS.DELETE_URL, UA1Bundle.getDataSets().get(0)));

        //now lets get the final states
        List<String> finalPrismStore = prismHelper.getFeedHelper().getStoreInfo();
        List<String> finalPrismArchiveStore = prismHelper.getFeedHelper().getArchiveInfo();

        //fetch the final store and archive for both colos
        List<String> finalUA1Store = UA1ColoHelper.getFeedHelper().getStoreInfo();
        List<String> finalUA1ArchiveStore = UA1ColoHelper.getFeedHelper().getArchiveInfo();

        List<String> finalUA2Store = UA2ColoHelper.getFeedHelper().getStoreInfo();
        List<String> finalUA2ArchiveStore = UA2ColoHelper.getFeedHelper().getArchiveInfo();

        //now ensure that data has been deleted from all cluster store and is present in the
        // cluster archives

        String clusterName = Util.readDatasetName(UA1Bundle.getDataSets().get(0));
        //prism:
        compareDataStoreStates(initialPrismStore, finalPrismStore, clusterName);
        compareDataStoreStates(finalPrismArchiveStore, initialPrismArchiveStore, clusterName);

        //UA1:
        compareDataStoreStates(initialUA1Store, finalUA1Store, clusterName);
        compareDataStoreStates(finalUA1ArchiveStore, initialUA1ArchiveStore, clusterName);

        //UA2:
        compareDataStoresForEquality(initialUA2Store, finalUA2Store);
        compareDataStoresForEquality(finalUA2ArchiveStore, initialUA2ArchiveStore);


    }

    @Test(dataProvider = "DP", groups = {"multiCluster"})
    public void testDeleteFeedSuspendedInOneColoWhileThatColoIsDown(Bundle bundle)
    throws Exception {
        try {
            //create a UA1 bundle
            Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
            UA1Bundle.generateUniqueBundle();

            Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());
            UA2Bundle.generateUniqueBundle();

            submitAndScheduleFeed(UA1Bundle);
            submitAndScheduleFeed(UA2Bundle);

            Util.assertSucceeded(
                    prismHelper.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getDataSets().get(0)));

            //fetch the initial store and archive state for prism
            List<String> initialPrismStore = prismHelper.getFeedHelper().getStoreInfo();
            List<String> initialPrismArchiveStore = prismHelper.getFeedHelper().getArchiveInfo();

            //fetch the initial store and archive for both colos
            List<String> initialUA1Store = UA1ColoHelper.getFeedHelper().getStoreInfo();
            List<String> initialUA1ArchiveStore = UA1ColoHelper.getFeedHelper().getArchiveInfo();

            List<String> initialUA2Store = UA2ColoHelper.getFeedHelper().getStoreInfo();
            List<String> initialUA2ArchiveStore = UA2ColoHelper.getFeedHelper().getArchiveInfo();

            //shutdown UA1
            Util.shutDownService(UA1ColoHelper.getFeedHelper());

            //lets now delete the cluster from both colos
            Util.assertFailed(prismHelper.getFeedHelper()
                    .delete(Util.URLS.DELETE_URL, UA1Bundle.getDataSets().get(0)));

            //now lets get the final states
            List<String> finalPrismStore = prismHelper.getFeedHelper().getStoreInfo();
            List<String> finalPrismArchiveStore = prismHelper.getFeedHelper().getArchiveInfo();

            //fetch the final store and archive for both colos
            List<String> finalUA1Store = UA1ColoHelper.getFeedHelper().getStoreInfo();
            List<String> finalUA1ArchiveStore = UA1ColoHelper.getFeedHelper().getArchiveInfo();

            List<String> finalUA2Store = UA2ColoHelper.getFeedHelper().getStoreInfo();
            List<String> finalUA2ArchiveStore = UA2ColoHelper.getFeedHelper().getArchiveInfo();

            //now ensure that data has been deleted from all cluster store and is present in the
            // cluster archives

            String clusterName = Util.readDatasetName(UA1Bundle.getDataSets().get(0));
            //prism:
            compareDataStoresForEquality(initialPrismStore, finalPrismStore);
            compareDataStoresForEquality(finalPrismArchiveStore, initialPrismArchiveStore);

            //UA1:
            Assert.assertTrue(
                    Arrays.deepEquals(initialUA1Store.toArray(new String[initialUA1Store.size()]),
                            finalUA1Store.toArray(new String[finalUA1Store.size()])));
            Assert.assertTrue(
                    Arrays.deepEquals(initialUA1ArchiveStore
                            .toArray(new String[initialUA1ArchiveStore.size()]),
                            finalUA1ArchiveStore.toArray(new String[finalUA1ArchiveStore.size()])));

            //UA2:
            compareDataStoresForEquality(initialUA2Store, finalUA2Store);
            compareDataStoresForEquality(finalUA2ArchiveStore, initialUA2ArchiveStore);

            Util.startService(UA1ColoHelper.getClusterHelper());

            Util.assertSucceeded(
                    prismHelper.getFeedHelper()
                            .delete(Util.URLS.DELETE_URL, UA1Bundle.getDataSets().get(0)));

            List<String> UA1StorePostUp = UA1ColoHelper.getFeedHelper().getStoreInfo();
            List<String> UA1ArchivePostUp = UA1ColoHelper.getFeedHelper().getArchiveInfo();

            List<String> ua2StorePostUp = UA2ColoHelper.getFeedHelper().getStoreInfo();
            List<String> ua2ArchivePostUp = UA2ColoHelper.getFeedHelper().getArchiveInfo();

            List<String> prismStorePostUp = prismHelper.getFeedHelper().getStoreInfo();
            List<String> prismArchivePostUp = prismHelper.getFeedHelper().getArchiveInfo();


            compareDataStoresForEquality(ua2StorePostUp, finalUA2Store);
            compareDataStoresForEquality(ua2ArchivePostUp, finalUA2ArchiveStore);

            compareDataStoreStates(finalUA1Store, UA1StorePostUp, clusterName);
            compareDataStoreStates(UA1ArchivePostUp, finalUA1ArchiveStore, clusterName);

            compareDataStoreStates(finalPrismStore, prismStorePostUp, clusterName);
            compareDataStoreStates(prismArchivePostUp, finalPrismArchiveStore, clusterName);


        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        } finally {

            Util.restartService(UA1ColoHelper.getFeedHelper());
        }

    }


    @Test(dataProvider = "DP", groups = {"multiCluster"})
    public void testDeleteFeedSuspendedInOneColoWhileThatColoIsDownAndOtherHasSuspendedFeed(
            Bundle bundle)
    throws Exception {
        try {
            //create a UA1 bundle
            Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
            UA1Bundle.generateUniqueBundle();

            Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());
            UA2Bundle.generateUniqueBundle();

            submitAndScheduleFeed(UA1Bundle);
            submitAndScheduleFeed(UA2Bundle);

            Util.assertSucceeded(
                    prismHelper.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getDataSets().get(0)));
            Util.assertSucceeded(
                    prismHelper.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getDataSets().get(0)));
            //fetch the initial store and archive state for prism
            List<String> initialPrismStore = prismHelper.getFeedHelper().getStoreInfo();
            List<String> initialPrismArchiveStore = prismHelper.getFeedHelper().getArchiveInfo();

            //fetch the initial store and archive for both colos
            List<String> initialUA1Store = UA1ColoHelper.getFeedHelper().getStoreInfo();
            List<String> initialUA1ArchiveStore = UA1ColoHelper.getFeedHelper().getArchiveInfo();

            List<String> initialUA2Store = UA2ColoHelper.getFeedHelper().getStoreInfo();
            List<String> initialUA2ArchiveStore = UA2ColoHelper.getFeedHelper().getArchiveInfo();

            //shutdown UA1
            Util.shutDownService(UA1ColoHelper.getFeedHelper());

            //lets now delete the feed from both colos
            Util.assertFailed(prismHelper.getFeedHelper()
                    .delete(Util.URLS.DELETE_URL, UA1Bundle.getDataSets().get(0)));

            //now lets get the final states
            List<String> finalPrismStore = prismHelper.getFeedHelper().getStoreInfo();
            List<String> finalPrismArchiveStore = prismHelper.getFeedHelper().getArchiveInfo();

            //fetch the final store and archive for both colos
            List<String> finalUA1Store = UA1ColoHelper.getFeedHelper().getStoreInfo();
            List<String> finalUA1ArchiveStore = UA1ColoHelper.getFeedHelper().getArchiveInfo();

            List<String> finalUA2Store = UA2ColoHelper.getFeedHelper().getStoreInfo();
            List<String> finalUA2ArchiveStore = UA2ColoHelper.getFeedHelper().getArchiveInfo();

            //now ensure that data has been deleted from all cluster store and is present in the
            // cluster archives

            String clusterName = Util.readDatasetName(UA1Bundle.getDataSets().get(0));
            //prism:
            compareDataStoresForEquality(initialPrismStore, finalPrismStore);
            compareDataStoresForEquality(finalPrismArchiveStore, initialPrismArchiveStore);

            //UA1:
            Assert.assertTrue(
                    Arrays.deepEquals(initialUA1Store.toArray(new String[initialUA1Store.size()]),
                            finalUA1Store.toArray(new String[finalUA1Store.size()])));
            Assert.assertTrue(
                    Arrays.deepEquals(initialUA1ArchiveStore
                            .toArray(new String[initialUA1ArchiveStore.size()]),
                            finalUA1ArchiveStore.toArray(new String[finalUA1ArchiveStore.size()])));

            //UA2:
            compareDataStoresForEquality(initialUA2Store, finalUA2Store);
            compareDataStoresForEquality(finalUA2ArchiveStore, initialUA2ArchiveStore);

            Util.startService(UA1ColoHelper.getFeedHelper());

            Util.assertSucceeded(
                    prismHelper.getFeedHelper()
                            .delete(Util.URLS.DELETE_URL, UA1Bundle.getDataSets().get(0)));

            HashMap<String, List<String>> finalSystemState = getSystemState(ENTITY_TYPE.DATA);

            compareDataStoreStates(finalSystemState.get("prismArchive"), finalPrismArchiveStore,
                    clusterName);
            compareDataStoreStates(finalPrismStore, finalSystemState.get("prismStore"),
                    clusterName);

            compareDataStoreStates(finalUA1Store, finalSystemState.get("ua1Store"), clusterName);
            compareDataStoreStates(finalSystemState.get("ua1Archive"), finalUA1ArchiveStore,
                    clusterName);

            compareDataStoresForEquality(finalSystemState.get("ua2Archive"), finalUA2ArchiveStore);
            compareDataStoresForEquality(finalSystemState.get("ua2Store"), finalUA2Store);


        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        } finally {

            Util.restartService(UA1ColoHelper.getFeedHelper());
        }

    }

    @Test(dataProvider = "DP", groups = {"multiCluster"})
    public void testDeleteFeedScheduledInOneColoWhileThatColoIsDown(Bundle bundle)
    throws Exception {
        try {
            //create a UA1 bundle
            Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
            UA1Bundle.generateUniqueBundle();

            Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());
            UA2Bundle.generateUniqueBundle();

            submitAndScheduleFeed(UA1Bundle);
            submitAndScheduleFeed(UA2Bundle);

            //fetch the initial store and archive state for prism
            List<String> initialPrismStore = prismHelper.getFeedHelper().getStoreInfo();
            List<String> initialPrismArchiveStore = prismHelper.getFeedHelper().getArchiveInfo();

            //fetch the initial store and archive for both colos
            List<String> initialUA1Store = UA1ColoHelper.getFeedHelper().getStoreInfo();
            List<String> initialUA1ArchiveStore = UA1ColoHelper.getFeedHelper().getArchiveInfo();

            List<String> initialUA2Store = UA2ColoHelper.getFeedHelper().getStoreInfo();
            List<String> initialUA2ArchiveStore = UA2ColoHelper.getFeedHelper().getArchiveInfo();

            //shutdown UA1
            Util.shutDownService(UA1ColoHelper.getFeedHelper());

            //lets now delete the cluster from both colos
            Util.assertFailed(prismHelper.getFeedHelper()
                    .delete(Util.URLS.DELETE_URL, UA1Bundle.getDataSets().get(0)));

            //now lets get the final states
            List<String> finalPrismStore = prismHelper.getFeedHelper().getStoreInfo();
            List<String> finalPrismArchiveStore = prismHelper.getFeedHelper().getArchiveInfo();

            //fetch the final store and archive for both colos
            List<String> finalUA1Store = UA1ColoHelper.getFeedHelper().getStoreInfo();
            List<String> finalUA1ArchiveStore = UA1ColoHelper.getFeedHelper().getArchiveInfo();

            List<String> finalUA2Store = UA2ColoHelper.getFeedHelper().getStoreInfo();
            List<String> finalUA2ArchiveStore = UA2ColoHelper.getFeedHelper().getArchiveInfo();

            //now ensure that data has been deleted from all cluster store and is present in the
            // cluster archives

            String clusterName = Util.readDatasetName(UA1Bundle.getDataSets().get(0));
            //prism:
            compareDataStoresForEquality(initialPrismStore, finalPrismStore);
            compareDataStoresForEquality(finalPrismArchiveStore, initialPrismArchiveStore);

            //UA1:
            Assert.assertTrue(
                    Arrays.deepEquals(initialUA1Store.toArray(new String[initialUA1Store.size()]),
                            finalUA1Store.toArray(new String[finalUA1Store.size()])));
            Assert.assertTrue(
                    Arrays.deepEquals(initialUA1ArchiveStore
                            .toArray(new String[initialUA1ArchiveStore.size()]),
                            finalUA1ArchiveStore.toArray(new String[finalUA1ArchiveStore.size()])));

            //UA2:
            compareDataStoresForEquality(initialUA2Store, finalUA2Store);
            compareDataStoresForEquality(finalUA2ArchiveStore, initialUA2ArchiveStore);


            Util.startService(UA1ColoHelper.getClusterHelper());
            Util.assertSucceeded(
                    prismHelper.getFeedHelper()
                            .delete(Util.URLS.DELETE_URL, UA1Bundle.getDataSets().get(0)));

            HashMap<String, List<String>> systemStatePostUp = getSystemState(ENTITY_TYPE.DATA);

            compareDataStoreStates(finalPrismStore, systemStatePostUp.get("prismStore"),
                    clusterName);
            compareDataStoreStates(systemStatePostUp.get("prismArchive"), finalPrismArchiveStore,
                    clusterName);

            compareDataStoreStates(finalUA1Store, systemStatePostUp.get("ua1Store"), clusterName);
            compareDataStoreStates(systemStatePostUp.get("ua1Archive"), finalUA1ArchiveStore,
                    clusterName);

            compareDataStoresForEquality(finalUA2ArchiveStore, systemStatePostUp.get("ua2Archive"));
            compareDataStoresForEquality(finalUA2Store, systemStatePostUp.get("ua2Store"));

        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        } finally {

            Util.restartService(UA1ColoHelper.getFeedHelper());
        }

    }

    @Test(groups = {"multiCluster"})
    public void testDeleteFeedSuspendedInOneColoWhileAnotherColoIsDown() throws Exception {
        Bundle b1 = (Bundle) Util.readELBundles()[0][0];
        b1.generateUniqueBundle();
        Bundle b2 = (Bundle) Util.readELBundles()[0][0];
        b2.generateUniqueBundle();

        try {
            b1 = new Bundle(b1, UA1ColoHelper.getEnvFileName());
            b2 = new Bundle(b2, UA2ColoHelper.getEnvFileName());

            b1.setCLusterColo("ua1");
            Util.print("cluster b1: " + b1.getClusters().get(0));

            ServiceResponse r = prismHelper.getClusterHelper()
                    .submitEntity(URLS.SUBMIT_URL, b1.getClusters().get(0));
            Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

            b2.setCLusterColo("ua2");
            Util.print("cluster b2: " + b2.getClusters().get(0));
            r = prismHelper.getClusterHelper()
                    .submitEntity(URLS.SUBMIT_URL, b2.getClusters().get(0));
            Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

            String startTimeUA1 = "2012-10-01T12:00Z";
            String startTimeUA2 = "2012-10-01T12:00Z";

            String feed = b1.getDataSets().get(0);
            feed = InstanceUtil.setFeedCluster(feed,
                    XmlUtil.createValidity("2012-10-01T12:00Z", "2010-01-01T00:00Z"),
                    XmlUtil.createRtention("days(10000)", ActionType.DELETE), null,
                    ClusterType.SOURCE, null, null);
            feed = InstanceUtil
                    .setFeedCluster(feed, XmlUtil.createValidity(startTimeUA1, "2099-10-01T12:10Z"),
                            XmlUtil.createRtention("days(10000)", ActionType.DELETE),
                            Util.readClusterName(b1.getClusters().get(0)), ClusterType.SOURCE,
                            "${cluster.colo}",
                            "/localDC/rc/billing/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            feed = InstanceUtil
                    .setFeedCluster(feed, XmlUtil.createValidity(startTimeUA2, "2099-10-01T12:25Z"),
                            XmlUtil.createRtention("days(10000)", ActionType.DELETE),
                            Util.readClusterName(b2.getClusters().get(0)), ClusterType.TARGET, null,
                            "/clusterPath/localDC/rc/billing/${YEAR}/${MONTH}/${DAY}/${HOUR}/$" +
                                    "{MINUTE}");

            Util.print("feed: " + feed);

            r = prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);

            AssertUtil.assertSucceeded(r);

            r = prismHelper.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);
            AssertUtil.assertSucceeded(r);
            Thread.sleep(15000);

            //fetch the initial store and archive state for prism
            List<String> initialPrismStore = prismHelper.getFeedHelper().getStoreInfo();
            List<String> initialPrismArchiveStore = prismHelper.getFeedHelper().getArchiveInfo();

            //fetch the initial store and archive for both colos
            List<String> initialUA1Store = UA1ColoHelper.getFeedHelper().getStoreInfo();
            List<String> initialUA1ArchiveStore = UA1ColoHelper.getFeedHelper().getArchiveInfo();

            List<String> initialUA2Store = UA2ColoHelper.getFeedHelper().getStoreInfo();
            List<String> initialUA2ArchiveStore = UA2ColoHelper.getFeedHelper().getArchiveInfo();

            Util.shutDownService(UA1ColoHelper.getFeedHelper());

            r = prismHelper.getFeedHelper().suspend(URLS.SUSPEND_URL, feed);
            Thread.sleep(10000);
            Util.assertPartialSucceeded(r);
            Assert.assertTrue(
                    r.getMessage().contains("Connection refusedua2/" + Util.getFeedName(feed)));

            ServiceResponse response =
                    prismHelper.getFeedHelper().delete(Util.URLS.DELETE_URL, feed);
            Assert.assertTrue(response.getMessage()
                    .contains("Connection refusedua2/" + Util.getFeedName(feed)));
            Util.assertPartialSucceeded(response);

            //now lets get the final states
            List<String> finalPrismStore = prismHelper.getFeedHelper().getStoreInfo();
            List<String> finalPrismArchiveStore = prismHelper.getFeedHelper().getArchiveInfo();

            //fetch the final store and archive for both colos
            List<String> finalUA1Store = UA1ColoHelper.getFeedHelper().getStoreInfo();
            List<String> finalUA1ArchiveStore = UA1ColoHelper.getFeedHelper().getArchiveInfo();

            List<String> finalUA2Store = UA2ColoHelper.getFeedHelper().getStoreInfo();
            List<String> finalUA2ArchiveStore = UA2ColoHelper.getFeedHelper().getArchiveInfo();

            //now ensure that data has been deleted from all cluster store and is present in the
            // cluster archives

            String clusterName = Util.readDatasetName(b1.getDataSets().get(0));
            //prism:
            compareDataStoresForEquality(initialPrismStore, finalPrismStore);
            compareDataStoresForEquality(finalPrismArchiveStore, initialPrismArchiveStore);

            //UA1:
            // Assert.assertTrue(Arrays.deepEquals(initialUA1Store.toArray(new
            // String[initialUA1Store.size()]),
            // finalUA1Store.toArray(new String[finalUA1Store.size()])));
            //Assert.assertTrue(Arrays.deepEquals(initialUA1ArchiveStore.toArray(new
            // String[initialUA1ArchiveStore
            // .size()]),finalUA1ArchiveStore.toArray(new String[finalUA1ArchiveStore.size()])));
            compareDataStoresForEquality(initialUA1Store, finalUA1Store);
            compareDataStoresForEquality(finalUA1ArchiveStore, initialUA1ArchiveStore);

            //UA2:
            compareDataStoreStates(initialUA2Store, finalUA2Store, clusterName);
            compareDataStoreStates(finalUA2ArchiveStore, initialUA2ArchiveStore, clusterName);


        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        } finally {

            Util.restartService(UA1ColoHelper.getFeedHelper());

        }
    }

    @Test(enabled = true)
    public void testDeleteFeedSuspendedInOneColoWhileAnotherColoIsDownWithFeedSuspended()
    throws Exception {
        Bundle b1 = (Bundle) Util.readELBundles()[0][0];
        b1.generateUniqueBundle();
        Bundle b2 = (Bundle) Util.readELBundles()[0][0];
        b2.generateUniqueBundle();

        try {
            b1 = new Bundle(b1, UA1ColoHelper.getEnvFileName());
            b2 = new Bundle(b2, UA2ColoHelper.getEnvFileName());

            b1.setCLusterColo("ua1");
            Util.print("cluster b1: " + b1.getClusters().get(0));

            ServiceResponse r = prismHelper.getClusterHelper()
                    .submitEntity(URLS.SUBMIT_URL, b1.getClusters().get(0));
            Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

            b2.setCLusterColo("ua2");
            Util.print("cluster b2: " + b2.getClusters().get(0));
            r = prismHelper.getClusterHelper()
                    .submitEntity(URLS.SUBMIT_URL, b2.getClusters().get(0));
            Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

            String startTimeUA1 = "2012-10-01T12:00Z";
            String startTimeUA2 = "2012-10-01T12:00Z";

            String feed = b1.getDataSets().get(0);
            feed = InstanceUtil.setFeedCluster(feed,
                    XmlUtil.createValidity("2012-10-01T12:00Z", "2010-01-01T00:00Z"),
                    XmlUtil.createRtention("days(10000)", ActionType.DELETE), null,
                    ClusterType.SOURCE, null, null);
            feed = InstanceUtil
                    .setFeedCluster(feed, XmlUtil.createValidity(startTimeUA1, "2099-10-01T12:10Z"),
                            XmlUtil.createRtention("days(10000)", ActionType.DELETE),
                            Util.readClusterName(b1.getClusters().get(0)), ClusterType.SOURCE,
                            "${cluster.colo}",
                            "/localDC/rc/billing/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            feed = InstanceUtil
                    .setFeedCluster(feed, XmlUtil.createValidity(startTimeUA2, "2099-10-01T12:25Z"),
                            XmlUtil.createRtention("days(10000)", ActionType.DELETE),
                            Util.readClusterName(b2.getClusters().get(0)), ClusterType.TARGET, null,
                            "/clusterPath/localDC/rc/billing/${YEAR}/${MONTH}/${DAY}/${HOUR}/$" +
                                    "{MINUTE}");

            Util.print("feed: " + feed);

            r = prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);

            AssertUtil.assertSucceeded(r);

            r = prismHelper.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);
            AssertUtil.assertSucceeded(r);
            Thread.sleep(15000);

            //fetch the initial store and archive state for prism
            List<String> initialPrismStore = prismHelper.getFeedHelper().getStoreInfo();
            List<String> initialPrismArchiveStore = prismHelper.getFeedHelper().getArchiveInfo();

            //fetch the initial store and archive for both colos
            List<String> initialUA1Store = UA1ColoHelper.getFeedHelper().getStoreInfo();
            List<String> initialUA1ArchiveStore = UA1ColoHelper.getFeedHelper().getArchiveInfo();

            List<String> initialUA2Store = UA2ColoHelper.getFeedHelper().getStoreInfo();
            List<String> initialUA2ArchiveStore = UA2ColoHelper.getFeedHelper().getArchiveInfo();

            r = prismHelper.getFeedHelper().suspend(URLS.SUSPEND_URL, feed);
            Thread.sleep(10000);
            AssertUtil.assertSucceeded(r);

            Util.shutDownService(UA1ColoHelper.getFeedHelper());

            ServiceResponse response =
                    prismHelper.getFeedHelper().delete(Util.URLS.DELETE_URL, feed);
            Assert.assertTrue(response.getMessage()
                    .contains("Connection refusedua2/" + Util.getFeedName(feed)));
            Util.assertPartialSucceeded(response);

            //now lets get the final states
            List<String> finalPrismStore = prismHelper.getFeedHelper().getStoreInfo();
            List<String> finalPrismArchiveStore = prismHelper.getFeedHelper().getArchiveInfo();

            //fetch the final store and archive for both colos
            List<String> finalUA1Store = UA1ColoHelper.getFeedHelper().getStoreInfo();
            List<String> finalUA1ArchiveStore = UA1ColoHelper.getFeedHelper().getArchiveInfo();

            List<String> finalUA2Store = UA2ColoHelper.getFeedHelper().getStoreInfo();
            List<String> finalUA2ArchiveStore = UA2ColoHelper.getFeedHelper().getArchiveInfo();

            //now ensure that data has been deleted from all cluster store and is present in the
            // cluster archives

            String clusterName = Util.readDatasetName(b1.getDataSets().get(0));
            //prism:
            compareDataStoresForEquality(initialPrismStore, finalPrismStore);
            compareDataStoresForEquality(finalPrismArchiveStore, initialPrismArchiveStore);

            //UA1:
            // Assert.assertTrue(Arrays.deepEquals(initialUA1Store.toArray(new
            // String[initialUA1Store.size()]),
            // finalUA1Store.toArray(new String[finalUA1Store.size()])));
            //Assert.assertTrue(Arrays.deepEquals(initialUA1ArchiveStore.toArray(new
            // String[initialUA1ArchiveStore
            // .size()]),finalUA1ArchiveStore.toArray(new String[finalUA1ArchiveStore.size()])));
            compareDataStoresForEquality(initialUA1Store, finalUA1Store);
            compareDataStoresForEquality(finalUA1ArchiveStore, initialUA1ArchiveStore);

            //UA2:
            compareDataStoreStates(initialUA2Store, finalUA2Store, clusterName);
            compareDataStoreStates(finalUA2ArchiveStore, initialUA2ArchiveStore, clusterName);


        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        } finally {

            Util.restartService(UA1ColoHelper.getFeedHelper());

        }

    }


    @Test(dataProvider = "DP", groups = {"multiCluster"})
    public void testDeleteFeedScheduledInOneColoWhileAnotherColoIsDown(Bundle bundle)
    throws Exception {
        try {
            //create a UA1 bundle
            Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());
            UA2Bundle.generateUniqueBundle();

            Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
            UA1Bundle.generateUniqueBundle();

            submitAndScheduleFeed(UA1Bundle);
            submitAndScheduleFeed(UA2Bundle);

            //fetch the initial store and archive state for prism
            List<String> initialPrismStore = prismHelper.getFeedHelper().getStoreInfo();
            List<String> initialPrismArchiveStore = prismHelper.getFeedHelper().getArchiveInfo();

            //fetch the initial store and archive for both colos
            List<String> initialUA1Store = UA1ColoHelper.getFeedHelper().getStoreInfo();
            List<String> initialUA1ArchiveStore = UA1ColoHelper.getFeedHelper().getArchiveInfo();

            List<String> initialUA2Store = UA2ColoHelper.getFeedHelper().getStoreInfo();
            List<String> initialUA2ArchiveStore = UA2ColoHelper.getFeedHelper().getArchiveInfo();

            //shutdown UA1
            Util.shutDownService(UA1ColoHelper.getFeedHelper());

            //lets now delete the cluster from both colos
            Util.assertSucceeded(
                    prismHelper.getFeedHelper()
                            .delete(Util.URLS.DELETE_URL, UA2Bundle.getDataSets().get(0)));

            //now lets get the final states
            List<String> finalPrismStore = prismHelper.getFeedHelper().getStoreInfo();
            List<String> finalPrismArchiveStore = prismHelper.getFeedHelper().getArchiveInfo();

            //fetch the final store and archive for both colos
            List<String> finalUA1Store = UA1ColoHelper.getFeedHelper().getStoreInfo();
            List<String> finalUA1ArchiveStore = UA1ColoHelper.getFeedHelper().getArchiveInfo();

            List<String> finalUA2Store = UA2ColoHelper.getFeedHelper().getStoreInfo();
            List<String> finalUA2ArchiveStore = UA2ColoHelper.getFeedHelper().getArchiveInfo();

            //now ensure that data has been deleted from all cluster store and is present in the
            // cluster archives

            String clusterName = Util.readDatasetName(UA2Bundle.getDataSets().get(0));
            //prism:
            compareDataStoreStates(initialPrismStore, finalPrismStore, clusterName);
            compareDataStoreStates(finalPrismArchiveStore, initialPrismArchiveStore, clusterName);

            //UA1:
            Assert.assertTrue(
                    Arrays.deepEquals(initialUA1Store.toArray(new String[initialUA1Store.size()]),
                            finalUA1Store.toArray(new String[finalUA1Store.size()])));
            Assert.assertTrue(
                    Arrays.deepEquals(initialUA1ArchiveStore
                            .toArray(new String[initialUA1ArchiveStore.size()]),
                            finalUA1ArchiveStore.toArray(new String[finalUA1ArchiveStore.size()])));

            //UA2:
            compareDataStoreStates(initialUA2Store, finalUA2Store, clusterName);
            compareDataStoreStates(finalUA2ArchiveStore, initialUA2ArchiveStore, clusterName);

            Util.startService(UA1ColoHelper.getFeedHelper());

            Util.assertSucceeded(
                    prismHelper.getFeedHelper()
                            .delete(Util.URLS.DELETE_URL, UA1Bundle.getDataSets().get(0)));

            clusterName = Util.readDatasetName(UA1Bundle.getDataSets().get(0));

            HashMap<String, List<String>> systemPostUp = getSystemState(ENTITY_TYPE.DATA);

            compareDataStoreStates(systemPostUp.get("prismArchive"), finalPrismArchiveStore,
                    clusterName);
            compareDataStoreStates(finalPrismStore, systemPostUp.get("prismStore"), clusterName);

            compareDataStoreStates(systemPostUp.get("ua1Archive"), finalUA1ArchiveStore,
                    clusterName);
            compareDataStoreStates(finalUA1Store, systemPostUp.get("ua1Store"), clusterName);

            compareDataStoresForEquality(finalUA2ArchiveStore, systemPostUp.get("ua2Archive"));
            compareDataStoresForEquality(finalUA2Store, systemPostUp.get("ua2Store"));


        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        } finally {

            Util.restartService(UA1ColoHelper.getFeedHelper());
        }

    }


    @DataProvider(name = "DP")
    public Object[][] getData() throws Exception {
        // return Util.readBundles("src/test/resources/LateDataBundles");
        return Util.readELBundles();
    }

    private void compareDataStoreStates(List<String> initialState, List<String> finalState,
                                        String filename)
    throws Exception {
        List<String> temp = new ArrayList<String>();
        temp.addAll(initialState);
        temp.removeAll(finalState);
        Assert.assertEquals(temp.size(), 1);
        Assert.assertTrue(temp.get(0).contains(filename));
        //initialState.addAll(finalState);

    }

    private void submitAndScheduleFeed(Bundle bundle) throws Exception {
        //Util.assertSucceeded(prismHelper.getClusterHelper().submitEntity(Util.URLS.SUBMIT_URL,
        // bundle.getClusterData()));
        for (String cluster : bundle.getClusters()) {
            Util.assertSucceeded(
                    prismHelper.getClusterHelper().submitEntity(Util.URLS.SUBMIT_URL, cluster));
            /// Util.assertSucceeded(prismHelper.getClusterHelper().submitEntity(URLS.SUBMIT_URL,
            // bundle.getClusters().get(0)));
        }
        Util.assertSucceeded(
                prismHelper.getFeedHelper()
                        .submitEntity(Util.URLS.SUBMIT_URL, bundle.getDataSets().get(0)));
        Util.assertSucceeded(prismHelper.getFeedHelper()
                .schedule(Util.URLS.SCHEDULE_URL, bundle.getDataSets().get(0)));
    }

    private void compareDataStoresForEquality(List<String> store1, List<String> store2)
    throws Exception {
        Assert.assertTrue(Arrays.deepEquals(store2.toArray(new String[store2.size()]),
                store1.toArray(new String[store1.size()])));
    }

    public HashMap<String, List<String>> getSystemState(ENTITY_TYPE entityType) throws Exception {
        IEntityManagerHelper prism = prismHelper.getClusterHelper();
        IEntityManagerHelper ua1 = UA1ColoHelper.getClusterHelper();
        IEntityManagerHelper ua2 = UA2ColoHelper.getClusterHelper();

        if (entityType.equals(ENTITY_TYPE.DATA)) {
            prism = prismHelper.getFeedHelper();
            ua1 = UA1ColoHelper.getFeedHelper();
            ua2 = UA2ColoHelper.getFeedHelper();
        }

        if (entityType.equals(ENTITY_TYPE.PROCESS)) {
            prism = prismHelper.getProcessHelper();
            ua1 = UA1ColoHelper.getProcessHelper();
            ua2 = UA2ColoHelper.getProcessHelper();
        }

        HashMap<String, List<String>> temp = new HashMap<String, List<String>>();
        temp.put("prismArchive", prism.getArchiveInfo());
        temp.put("prismStore", prism.getStoreInfo());
        temp.put("ua1Archive", ua1.getArchiveInfo());
        temp.put("ua1Store", ua1.getStoreInfo());
        temp.put("ua2Archive", ua2.getArchiveInfo());
        temp.put("ua2Store", ua2.getStoreInfo());

        return temp;
    }

}
