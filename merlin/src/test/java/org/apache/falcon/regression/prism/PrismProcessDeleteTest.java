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


import junit.framework.Assert;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.helpers.PrismHelper;
import org.apache.falcon.regression.core.interfaces.IEntityManagerHelper;
import org.apache.falcon.regression.core.supportClasses.ENTITY_TYPE;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.testng.TestNGException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class PrismProcessDeleteTest {


    @BeforeMethod(alwaysRun = true)
    public void testName(Method method) {
        Util.print("test name: " + method.getName());
    }


    PrismHelper prismHelper = new PrismHelper("prism.properties");
    ColoHelper UA1ColoHelper = new ColoHelper("gs1001.config.properties");
    ColoHelper UA2ColoHelper = new ColoHelper("ivoryqa-1.config.properties");

	/* NOTE: All test cases assume that there are two entities scheduled in each colo       
        com.inmobi.qa.airavatqa.prism.PrismProcessDeleteTest
        .testUA1ProcessDeleteAlreadyDeletedProcess */


    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testUA1ProcessDeleteInBothColos(Bundle bundle) throws Exception {
        //create a UA1 bundle
        Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
        UA1Bundle.generateUniqueBundle();

        //now submit the thing to prism
        submitAndScheduleProcess(UA1Bundle);
        //fetch the initial store and archive state for prism
        List<String> initialPrismStore = prismHelper.getProcessHelper().getStoreInfo();
        List<String> initialPrismArchiveStore = prismHelper.getProcessHelper().getArchiveInfo();

        //fetch the initial store and archive for both colos
        List<String> initialUA1Store = UA1ColoHelper.getProcessHelper().getStoreInfo();
        List<String> initialUA1ArchiveStore = UA1ColoHelper.getProcessHelper().getArchiveInfo();

        List<String> initialUA2Store = UA2ColoHelper.getProcessHelper().getStoreInfo();
        List<String> initialUA2ArchiveStore = UA2ColoHelper.getProcessHelper().getArchiveInfo();

        //lets now delete the cluster from both colos
        Util.assertSucceeded(prismHelper.getProcessHelper()
                .delete(Util.URLS.DELETE_URL, UA1Bundle.getProcessData()));

        //now lets get the final states
        List<String> finalPrismStore = prismHelper.getProcessHelper().getStoreInfo();
        List<String> finalPrismArchiveStore = prismHelper.getProcessHelper().getArchiveInfo();

        //fetch the final store and archive for both colos
        List<String> finalUA1Store = UA1ColoHelper.getProcessHelper().getStoreInfo();
        List<String> finalUA1ArchiveStore = UA1ColoHelper.getProcessHelper().getArchiveInfo();

        List<String> finalUA2Store = UA2ColoHelper.getProcessHelper().getStoreInfo();
        List<String> finalUA2ArchiveStore = UA2ColoHelper.getProcessHelper().getArchiveInfo();

        //now ensure that data has been deleted from all cluster store and is present in the
        // cluster archives

        String clusterName = Util.readEntityName(bundle.getProcessData());
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

    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testUA1ProcessDeleteWhen1ColoIsDown(Bundle bundle) throws Exception {
        try {
            //create a UA1 bundle
            Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
            UA1Bundle.generateUniqueBundle();

            //now submit the thing to prism
            submitAndScheduleProcess(UA1Bundle);
            //fetch the initial store and archive state for prism
            List<String> initialPrismStore = prismHelper.getProcessHelper().getStoreInfo();
            List<String> initialPrismArchiveStore = prismHelper.getProcessHelper().getArchiveInfo();

            //fetch the initial store and archive for both colos
            List<String> initialUA1Store = UA1ColoHelper.getProcessHelper().getStoreInfo();
            List<String> initialUA1ArchiveStore = UA1ColoHelper.getProcessHelper().getArchiveInfo();

            List<String> initialUA2Store = UA2ColoHelper.getProcessHelper().getStoreInfo();
            List<String> initialUA2ArchiveStore = UA2ColoHelper.getProcessHelper().getArchiveInfo();


            //bring down UA2 colo :P
            Util.shutDownService(UA1ColoHelper.getClusterHelper());

            //lets now delete the cluster from both colos
            Util.assertFailed(prismHelper.getProcessHelper()
                    .delete(Util.URLS.DELETE_URL, UA1Bundle.getProcessData()));

            //now lets get the final states
            List<String> finalPrismStore = prismHelper.getProcessHelper().getStoreInfo();
            List<String> finalPrismArchiveStore = prismHelper.getProcessHelper().getArchiveInfo();

            //fetch the final store and archive for both colos
            List<String> finalUA1Store = UA1ColoHelper.getProcessHelper().getStoreInfo();
            List<String> finalUA1ArchiveStore = UA1ColoHelper.getProcessHelper().getArchiveInfo();

            List<String> finalUA2Store = UA2ColoHelper.getProcessHelper().getStoreInfo();
            List<String> finalUA2ArchiveStore = UA2ColoHelper.getProcessHelper().getArchiveInfo();

            //now ensure that data has been deleted from all cluster store and is present in the
            // cluster archives

            String clusterName = Util.readEntityName(bundle.getProcessData());
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

            //bring service up
            Util.startService(UA1ColoHelper.getProcessHelper());
            Util.assertSucceeded(
                    prismHelper.getProcessHelper()
                            .delete(Util.URLS.DELETE_URL, UA1Bundle.getProcessData()));

            HashMap<String, List<String>> systemPostUp = getSystemState(ENTITY_TYPE.PROCESS);

            compareDataStoreStates(finalPrismStore, systemPostUp.get("prismStore"), clusterName);
            compareDataStoreStates(systemPostUp.get("prismArchive"), finalPrismArchiveStore,
                    clusterName);

            compareDataStoresForEquality(finalUA2Store, systemPostUp.get("ua2Store"));
            compareDataStoresForEquality(finalUA2ArchiveStore, systemPostUp.get("ua2Archive"));

            compareDataStoreStates(finalUA1Store, systemPostUp.get("ua1Store"), clusterName);
            compareDataStoreStates(systemPostUp.get("ua1Archive"), finalUA1ArchiveStore,
                    clusterName);


        } catch (Exception e) {
            System.out.println(e.getMessage());
            throw new TestNGException(e.getMessage());
        } finally {

            Util.restartService(UA1ColoHelper.getClusterHelper());
        }

    }


//    @Test(dataProvider="DP",groups={"prism","0.2"})
//    public void testUA1ClusterDeleteWhen1ColoIsDown(Bundle bundle) throws Exception
//    {
//        try{
//        //create a UA1 bundle
//        Bundle UA1Bundle=new Bundle(bundle,UA1ColoHelper.getEnvFileName());
//        UA1Bundle.generateUniqueBundle();
//        
//        //now submit the thing to prism
//        APIResult clusterResult=Util.parseResponse(prismHelper.getFeedHelper().submitEntity
// (Util.URLS.SUBMIT_URL,
// UA1Bundle.getDataSets().get(0)));
//        //fetch the initial store and archive state for prism
//        List<String> initialPrismStore=prismHelper.getProcessHelper().getStoreInfo();
//        List<String> initialPrismArchiveStore=prismHelper.getProcessHelper().getArchiveInfo();
//        
//        //fetch the initial store and archive for both colos
//        List<String> initialUA1Store=UA1ColoHelper.getProcessHelper().getStoreInfo();
//        List<String> initialUA1ArchiveStore=UA1ColoHelper.getProcessHelper().getArchiveInfo();
//        
//        List<String> initialUA2Store=UA2ColoHelper.getProcessHelper().getStoreInfo();
//        List<String> initialUA2ArchiveStore=UA2ColoHelper.getProcessHelper().getArchiveInfo();
//        
//        
//        //bring down UA2 colo :P
//        Util.shutDownService(UA1ColoHelper.getClusterHelper());
//        
//        //lets now delete the cluster from both colos
//        APIResult clusterDeleteResult=Util.parseResponse(prismHelper.getFeedHelper().delete
// (Util.URLS.DELETE_URL,
// UA1Bundle.getDataSets().get(0)));
//        
//        //now lets get the final states
//        List<String> finalPrismStore=prismHelper.getProcessHelper().getStoreInfo();
//        List<String> finalPrismArchiveStore=prismHelper.getProcessHelper().getArchiveInfo();
//        
//        //fetch the final store and archive for both colos
//        List<String> finalUA1Store=UA1ColoHelper.getProcessHelper().getStoreInfo();
//        List<String> finalUA1ArchiveStore=UA1ColoHelper.getProcessHelper().getArchiveInfo();
//        
//        List<String> finalUA2Store=UA2ColoHelper.getProcessHelper().getStoreInfo();
//        List<String> finalUA2ArchiveStore=UA2ColoHelper.getProcessHelper().getArchiveInfo();
//        
//        //now ensure that data has been deleted from all cluster store and is present in the
// cluster archives
//        
//        String clusterName=Util.readEntityName(bundle.getProcessData());
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
// String[initialUA1Store.size()]),
// finalUA1Store.toArray(new String[finalUA1Store.size()])));
//          Assert.assertTrue(Arrays.deepEquals(initialUA1ArchiveStore.toArray(new
// String[initialUA1ArchiveStore.size
// ()]),finalUA1ArchiveStore.toArray(new String[finalUA1ArchiveStore.size()])));
//        }
//        catch(Exception e)
//        {
//            System.out.println(e.getMessage());
//            throw new TestNGException(e.getMessage());
//        }
//        finally {
//            
//            Util.restartService(UA1ColoHelper.getClusterHelper());
//        }
//
//    }  

    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testUA1ProcessDeleteAlreadyDeletedProcess(Bundle bundle) throws Exception {
        try {
            //create a UA1 bundle
            Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
            UA1Bundle.generateUniqueBundle();

            //now submit the thing to prism
            submitAndScheduleProcess(UA1Bundle);
            Util.assertSucceeded(
                    prismHelper.getProcessHelper()
                            .delete(Util.URLS.DELETE_URL, UA1Bundle.getProcessData()));
            //fetch the initial store and archive state for prism
            List<String> initialPrismStore = prismHelper.getProcessHelper().getStoreInfo();
            List<String> initialPrismArchiveStore = prismHelper.getProcessHelper().getArchiveInfo();

            //fetch the initial store and archive for both colos
            List<String> initialUA1Store = UA1ColoHelper.getProcessHelper().getStoreInfo();
            List<String> initialUA1ArchiveStore = UA1ColoHelper.getProcessHelper().getArchiveInfo();

            List<String> initialUA2Store = UA2ColoHelper.getProcessHelper().getStoreInfo();
            List<String> initialUA2ArchiveStore = UA2ColoHelper.getProcessHelper().getArchiveInfo();

            Util.assertSucceeded(
                    prismHelper.getProcessHelper()
                            .delete(Util.URLS.DELETE_URL, UA1Bundle.getProcessData()));

            //now lets get the final states
            List<String> finalPrismStore = prismHelper.getProcessHelper().getStoreInfo();
            List<String> finalPrismArchiveStore = prismHelper.getProcessHelper().getArchiveInfo();

            //fetch the final store and archive for both colos
            List<String> finalUA1Store = UA1ColoHelper.getProcessHelper().getStoreInfo();
            List<String> finalUA1ArchiveStore = UA1ColoHelper.getProcessHelper().getArchiveInfo();

            List<String> finalUA2Store = UA2ColoHelper.getProcessHelper().getStoreInfo();
            List<String> finalUA2ArchiveStore = UA2ColoHelper.getProcessHelper().getArchiveInfo();

            //now ensure that data has been deleted from all cluster store and is present in the
            // cluster archives

            String clusterName = Util.readEntityName(bundle.getProcessData());
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
            System.out.println(e.getMessage());
            throw new TestNGException(e.getMessage());
        } finally {

        }

    }


    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testUA1ProcessDeleteTwiceWhen1ColoIsDownDuring1stDelete(Bundle bundle)
    throws Exception {
        try {
            //create a UA1 bundle
            Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
            UA1Bundle.generateUniqueBundle();

            submitAndScheduleProcess(UA1Bundle);

            Util.shutDownService(UA1ColoHelper.getClusterHelper());


            //lets now delete the cluster from both colos
            Util.assertFailed(prismHelper.getProcessHelper()
                    .delete(Util.URLS.DELETE_URL, UA1Bundle.getProcessData()));

            //now lets get the final states
            List<String> initialPrismStore = prismHelper.getProcessHelper().getStoreInfo();
            List<String> initialPrismArchiveStore = prismHelper.getProcessHelper().getArchiveInfo();

            //fetch the initial store and archive for both colos
            List<String> initialUA1Store = UA1ColoHelper.getProcessHelper().getStoreInfo();
            List<String> initialUA1ArchiveStore = UA1ColoHelper.getProcessHelper().getArchiveInfo();

            List<String> initialUA2Store = UA2ColoHelper.getProcessHelper().getStoreInfo();
            List<String> initialUA2ArchiveStore = UA2ColoHelper.getProcessHelper().getArchiveInfo();

            //start up service
            Util.startService(UA1ColoHelper.getClusterHelper());

            //delete again
            Util.assertSucceeded(
                    prismHelper.getProcessHelper()
                            .delete(Util.URLS.DELETE_URL, UA1Bundle.getProcessData()));

            //get final states
            List<String> finalPrismStore = prismHelper.getProcessHelper().getStoreInfo();
            List<String> finalPrismArchiveStore = prismHelper.getProcessHelper().getArchiveInfo();

            //fetch the final store and archive for both colos
            List<String> finalUA1Store = UA1ColoHelper.getProcessHelper().getStoreInfo();
            List<String> finalUA1ArchiveStore = UA1ColoHelper.getProcessHelper().getArchiveInfo();

            List<String> finalUA2Store = UA2ColoHelper.getProcessHelper().getStoreInfo();
            List<String> finalUA2ArchiveStore = UA2ColoHelper.getProcessHelper().getArchiveInfo();

            //now ensure that data has been deleted from all cluster store and is present in the
            // cluster archives

            String clusterName = Util.readEntityName(bundle.getProcessData());
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
            compareDataStoreStates(initialUA1Store, finalUA1Store, clusterName);
            compareDataStoreStates(initialUA1ArchiveStore, finalUA1ArchiveStore, clusterName);

        } catch (Exception e) {
            System.out.println(e.getMessage());
            throw new TestNGException(e.getMessage());
        } finally {

            Util.restartService(UA1ColoHelper.getClusterHelper());
        }

    }

    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testUA1ProcessDeleteNonExistent(Bundle bundle) throws Exception {
        try {
            //create a UA1 bundle
            Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
            UA1Bundle.generateUniqueBundle();


            //now lets get the final states
            List<String> initialPrismStore = prismHelper.getProcessHelper().getStoreInfo();
            List<String> initialPrismArchiveStore = prismHelper.getProcessHelper().getArchiveInfo();

            //fetch the initial store and archive for both colos
            List<String> initialUA1Store = UA1ColoHelper.getProcessHelper().getStoreInfo();
            List<String> initialUA1ArchiveStore = UA1ColoHelper.getProcessHelper().getArchiveInfo();

            List<String> initialUA2Store = UA2ColoHelper.getProcessHelper().getStoreInfo();
            List<String> initialUA2ArchiveStore = UA2ColoHelper.getProcessHelper().getArchiveInfo();

            //delete
            Util.assertSucceeded(
                    prismHelper.getProcessHelper()
                            .delete(Util.URLS.DELETE_URL, UA1Bundle.getProcessData()));

            //get final states
            List<String> finalPrismStore = prismHelper.getProcessHelper().getStoreInfo();
            List<String> finalPrismArchiveStore = prismHelper.getProcessHelper().getArchiveInfo();

            //fetch the final store and archive for both colos
            List<String> finalUA1Store = UA1ColoHelper.getProcessHelper().getStoreInfo();
            List<String> finalUA1ArchiveStore = UA1ColoHelper.getProcessHelper().getArchiveInfo();

            List<String> finalUA2Store = UA2ColoHelper.getProcessHelper().getStoreInfo();
            List<String> finalUA2ArchiveStore = UA2ColoHelper.getProcessHelper().getArchiveInfo();

            //now ensure that data has been deleted from all cluster store and is present in the
            // cluster archives

            String clusterName = Util.readEntityName(bundle.getProcessData());
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
            System.out.println(e.getMessage());
            throw new TestNGException(e.getMessage());
        }

    }


    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testUA1ProcessDeleteNonExistentWhen1ColoIsDownDuringDelete(Bundle bundle)
    throws Exception {
        try {
            //create a UA1 bundle
            Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
            UA1Bundle.generateUniqueBundle();


            //now lets get the final states
            List<String> initialPrismStore = prismHelper.getProcessHelper().getStoreInfo();
            List<String> initialPrismArchiveStore = prismHelper.getProcessHelper().getArchiveInfo();

            //fetch the initial store and archive for both colos
            List<String> initialUA1Store = UA1ColoHelper.getProcessHelper().getStoreInfo();
            List<String> initialUA1ArchiveStore = UA1ColoHelper.getProcessHelper().getArchiveInfo();

            List<String> initialUA2Store = UA2ColoHelper.getProcessHelper().getStoreInfo();
            List<String> initialUA2ArchiveStore = UA2ColoHelper.getProcessHelper().getArchiveInfo();

            //bring down UA1
            Util.shutDownService(UA1ColoHelper.getClusterHelper());

            //delete
            Util.assertSucceeded(
                    prismHelper.getProcessHelper()
                            .delete(Util.URLS.DELETE_URL, UA1Bundle.getProcessData()));

            //get final states
            List<String> finalPrismStore = prismHelper.getProcessHelper().getStoreInfo();
            List<String> finalPrismArchiveStore = prismHelper.getProcessHelper().getArchiveInfo();

            //fetch the final store and archive for both colos
            List<String> finalUA1Store = UA1ColoHelper.getProcessHelper().getStoreInfo();
            List<String> finalUA1ArchiveStore = UA1ColoHelper.getProcessHelper().getArchiveInfo();

            List<String> finalUA2Store = UA2ColoHelper.getProcessHelper().getStoreInfo();
            List<String> finalUA2ArchiveStore = UA2ColoHelper.getProcessHelper().getArchiveInfo();

            //now ensure that data has been deleted from all cluster store and is present in the
            // cluster archives

            String clusterName = Util.readEntityName(bundle.getProcessData());
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

            Util.startService(UA1ColoHelper.getClusterHelper());
            Util.assertSucceeded(
                    prismHelper.getProcessHelper()
                            .delete(Util.URLS.DELETE_URL, UA1Bundle.getProcessData()));

        } catch (Exception e) {
            System.out.println(e.getMessage());
            throw new TestNGException(e.getMessage());
        } finally {
            Util.restartService(UA1ColoHelper.getClusterHelper());
        }

    }


    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testDeleteProcessScheduledInOneColo(Bundle bundle) throws Exception {
        //create a UA1 bundle
        Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
        UA1Bundle.generateUniqueBundle();

        Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());
        UA2Bundle.generateUniqueBundle();

        submitAndScheduleProcess(UA1Bundle);
        submitAndScheduleProcess(UA2Bundle);

        //fetch the initial store and archive state for prism
        List<String> initialPrismStore = prismHelper.getProcessHelper().getStoreInfo();
        List<String> initialPrismArchiveStore = prismHelper.getProcessHelper().getArchiveInfo();

        //fetch the initial store and archive for both colos
        List<String> initialUA1Store = UA1ColoHelper.getProcessHelper().getStoreInfo();
        List<String> initialUA1ArchiveStore = UA1ColoHelper.getProcessHelper().getArchiveInfo();

        List<String> initialUA2Store = UA2ColoHelper.getProcessHelper().getStoreInfo();
        List<String> initialUA2ArchiveStore = UA2ColoHelper.getProcessHelper().getArchiveInfo();

        //lets now delete the cluster from both colos
        Util.assertSucceeded(prismHelper.getProcessHelper()
                .delete(Util.URLS.DELETE_URL, UA1Bundle.getProcessData()));

        //now lets get the final states
        List<String> finalPrismStore = prismHelper.getProcessHelper().getStoreInfo();
        List<String> finalPrismArchiveStore = prismHelper.getProcessHelper().getArchiveInfo();

        //fetch the final store and archive for both colos
        List<String> finalUA1Store = UA1ColoHelper.getProcessHelper().getStoreInfo();
        List<String> finalUA1ArchiveStore = UA1ColoHelper.getProcessHelper().getArchiveInfo();

        List<String> finalUA2Store = UA2ColoHelper.getProcessHelper().getStoreInfo();
        List<String> finalUA2ArchiveStore = UA2ColoHelper.getProcessHelper().getArchiveInfo();

        //now ensure that data has been deleted from all cluster store and is present in the
        // cluster archives

        String clusterName = Util.readEntityName(bundle.getProcessData());
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

    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testDeleteProcessSuspendedInOneColo(Bundle bundle) throws Exception {
        //create a UA1 bundle
        Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
        UA1Bundle.generateUniqueBundle();

        Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());
        UA2Bundle.generateUniqueBundle();

        submitAndScheduleProcess(UA1Bundle);
        submitAndScheduleProcess(UA2Bundle);


        //suspend UA1 colo thingy
        Util.assertSucceeded(prismHelper.getProcessHelper()
                .suspend(URLS.SUSPEND_URL, UA1Bundle.getProcessData()));

        //fetch the initial store and archive state for prism
        List<String> initialPrismStore = prismHelper.getProcessHelper().getStoreInfo();
        List<String> initialPrismArchiveStore = prismHelper.getProcessHelper().getArchiveInfo();

        //fetch the initial store and archive for both colos
        List<String> initialUA1Store = UA1ColoHelper.getProcessHelper().getStoreInfo();
        List<String> initialUA1ArchiveStore = UA1ColoHelper.getProcessHelper().getArchiveInfo();

        List<String> initialUA2Store = UA2ColoHelper.getProcessHelper().getStoreInfo();
        List<String> initialUA2ArchiveStore = UA2ColoHelper.getProcessHelper().getArchiveInfo();

        //lets now delete the cluster from both colos
        Util.assertSucceeded(prismHelper.getProcessHelper()
                .delete(Util.URLS.DELETE_URL, UA1Bundle.getProcessData()));

        //now lets get the final states
        List<String> finalPrismStore = prismHelper.getProcessHelper().getStoreInfo();
        List<String> finalPrismArchiveStore = prismHelper.getProcessHelper().getArchiveInfo();

        //fetch the final store and archive for both colos
        List<String> finalUA1Store = UA1ColoHelper.getProcessHelper().getStoreInfo();
        List<String> finalUA1ArchiveStore = UA1ColoHelper.getProcessHelper().getArchiveInfo();

        List<String> finalUA2Store = UA2ColoHelper.getProcessHelper().getStoreInfo();
        List<String> finalUA2ArchiveStore = UA2ColoHelper.getProcessHelper().getArchiveInfo();

        //now ensure that data has been deleted from all cluster store and is present in the
        // cluster archives

        String clusterName = Util.readEntityName(UA1Bundle.getProcessData());
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


    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testDeleteProcessSuspendedInOneColoWhileBothProcessesAreSuspended(Bundle bundle)
    throws Exception {
        //create a UA1 bundle
        Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
        UA1Bundle.generateUniqueBundle();

        Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());
        UA2Bundle.generateUniqueBundle();

        submitAndScheduleProcess(UA1Bundle);
        submitAndScheduleProcess(UA2Bundle);


        //suspend UA1 colo thingy
        Util.assertSucceeded(prismHelper.getProcessHelper()
                .suspend(URLS.SUSPEND_URL, UA1Bundle.getProcessData()));
        Util.assertSucceeded(prismHelper.getProcessHelper()
                .suspend(URLS.SUSPEND_URL, UA2Bundle.getProcessData()));

        //fetch the initial store and archive state for prism
        List<String> initialPrismStore = prismHelper.getProcessHelper().getStoreInfo();
        List<String> initialPrismArchiveStore = prismHelper.getProcessHelper().getArchiveInfo();

        //fetch the initial store and archive for both colos
        List<String> initialUA1Store = UA1ColoHelper.getProcessHelper().getStoreInfo();
        List<String> initialUA1ArchiveStore = UA1ColoHelper.getProcessHelper().getArchiveInfo();

        List<String> initialUA2Store = UA2ColoHelper.getProcessHelper().getStoreInfo();
        List<String> initialUA2ArchiveStore = UA2ColoHelper.getProcessHelper().getArchiveInfo();

        //lets now delete the cluster from both colos
        Util.assertSucceeded(prismHelper.getProcessHelper()
                .delete(Util.URLS.DELETE_URL, UA1Bundle.getProcessData()));

        //now lets get the final states
        List<String> finalPrismStore = prismHelper.getProcessHelper().getStoreInfo();
        List<String> finalPrismArchiveStore = prismHelper.getProcessHelper().getArchiveInfo();

        //fetch the final store and archive for both colos
        List<String> finalUA1Store = UA1ColoHelper.getProcessHelper().getStoreInfo();
        List<String> finalUA1ArchiveStore = UA1ColoHelper.getProcessHelper().getArchiveInfo();

        List<String> finalUA2Store = UA2ColoHelper.getProcessHelper().getStoreInfo();
        List<String> finalUA2ArchiveStore = UA2ColoHelper.getProcessHelper().getArchiveInfo();

        //now ensure that data has been deleted from all cluster store and is present in the
        // cluster archives

        String clusterName = Util.readEntityName(bundle.getProcessData());
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

    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testDeleteProcessSuspendedInOneColoWhileThatColoIsDown(Bundle bundle)
    throws Exception {
        try {
            //create a UA1 bundle
            Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
            UA1Bundle.generateUniqueBundle();

            Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());
            UA2Bundle.generateUniqueBundle();

            submitAndScheduleProcess(UA1Bundle);
            submitAndScheduleProcess(UA2Bundle);

            Util.assertSucceeded(
                    prismHelper.getProcessHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getProcessData()));

            //fetch the initial store and archive state for prism
            List<String> initialPrismStore = prismHelper.getProcessHelper().getStoreInfo();
            List<String> initialPrismArchiveStore = prismHelper.getProcessHelper().getArchiveInfo();

            //fetch the initial store and archive for both colos
            List<String> initialUA1Store = UA1ColoHelper.getProcessHelper().getStoreInfo();
            List<String> initialUA1ArchiveStore = UA1ColoHelper.getProcessHelper().getArchiveInfo();

            List<String> initialUA2Store = UA2ColoHelper.getProcessHelper().getStoreInfo();
            List<String> initialUA2ArchiveStore = UA2ColoHelper.getProcessHelper().getArchiveInfo();

            //shutdown UA1
            Util.shutDownService(UA1ColoHelper.getFeedHelper());

            //lets now delete the cluster from both colos
            Util.assertFailed(prismHelper.getProcessHelper()
                    .delete(Util.URLS.DELETE_URL, UA1Bundle.getProcessData()));

            //now lets get the final states
            List<String> finalPrismStore = prismHelper.getProcessHelper().getStoreInfo();
            List<String> finalPrismArchiveStore = prismHelper.getProcessHelper().getArchiveInfo();

            //fetch the final store and archive for both colos
            List<String> finalUA1Store = UA1ColoHelper.getProcessHelper().getStoreInfo();
            List<String> finalUA1ArchiveStore = UA1ColoHelper.getProcessHelper().getArchiveInfo();

            List<String> finalUA2Store = UA2ColoHelper.getProcessHelper().getStoreInfo();
            List<String> finalUA2ArchiveStore = UA2ColoHelper.getProcessHelper().getArchiveInfo();

            //now ensure that data has been deleted from all cluster store and is present in the
            // cluster archives

            String clusterName = Util.readEntityName(bundle.getProcessData());
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
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        } finally {

            Util.restartService(UA1ColoHelper.getFeedHelper());
        }

    }


    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testDeleteProcessScheduledInOneColoWhileThatColoIsDown(Bundle bundle)
    throws Exception {
        try {
            //create a UA1 bundle
            Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
            UA1Bundle.generateUniqueBundle();

            Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());
            UA2Bundle.generateUniqueBundle();

            submitAndScheduleProcess(UA1Bundle);
            submitAndScheduleProcess(UA2Bundle);

            //fetch the initial store and archive state for prism
            List<String> initialPrismStore = prismHelper.getProcessHelper().getStoreInfo();
            List<String> initialPrismArchiveStore = prismHelper.getProcessHelper().getArchiveInfo();

            //fetch the initial store and archive for both colos
            List<String> initialUA1Store = UA1ColoHelper.getProcessHelper().getStoreInfo();
            List<String> initialUA1ArchiveStore = UA1ColoHelper.getProcessHelper().getArchiveInfo();

            List<String> initialUA2Store = UA2ColoHelper.getProcessHelper().getStoreInfo();
            List<String> initialUA2ArchiveStore = UA2ColoHelper.getProcessHelper().getArchiveInfo();

            //shutdown UA1
            Util.shutDownService(UA1ColoHelper.getFeedHelper());

            //lets now delete the cluster from both colos
            Util.assertFailed(prismHelper.getProcessHelper()
                    .delete(Util.URLS.DELETE_URL, UA1Bundle.getProcessData()));

            //now lets get the final states
            List<String> finalPrismStore = prismHelper.getProcessHelper().getStoreInfo();
            List<String> finalPrismArchiveStore = prismHelper.getProcessHelper().getArchiveInfo();

            //fetch the final store and archive for both colos
            List<String> finalUA1Store = UA1ColoHelper.getProcessHelper().getStoreInfo();
            List<String> finalUA1ArchiveStore = UA1ColoHelper.getProcessHelper().getArchiveInfo();

            List<String> finalUA2Store = UA2ColoHelper.getProcessHelper().getStoreInfo();
            List<String> finalUA2ArchiveStore = UA2ColoHelper.getProcessHelper().getArchiveInfo();

            //now ensure that data has been deleted from all cluster store and is present in the
            // cluster archives

            String clusterName = Util.readEntityName(UA1Bundle.getProcessData());
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
                    prismHelper.getProcessHelper()
                            .delete(Util.URLS.DELETE_URL, UA1Bundle.getProcessData()));

            HashMap<String, List<String>> systemPostUp = getSystemState(ENTITY_TYPE.PROCESS);

            compareDataStoresForEquality(finalUA2Store, systemPostUp.get("ua2Store"));
            compareDataStoresForEquality(finalUA2ArchiveStore, systemPostUp.get("ua2Archive"));

            compareDataStoreStates(finalPrismStore, systemPostUp.get("prismStore"), clusterName);
            compareDataStoreStates(systemPostUp.get("prismArchive"), finalPrismArchiveStore,
                    clusterName);

            compareDataStoreStates(finalUA1Store, systemPostUp.get("ua1Store"), clusterName);
            compareDataStoreStates(systemPostUp.get("ua1Archive"), finalUA1ArchiveStore,
                    clusterName);

        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        } finally {

            Util.restartService(UA1ColoHelper.getFeedHelper());
        }

    }

    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testDeleteProcessSuspendedInOneColoWhileAnotherColoIsDown(Bundle bundle)
    throws Exception {
        try {
            //create a UA1 bundle
            Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());
            UA2Bundle.generateUniqueBundle();

            Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
            UA1Bundle.generateUniqueBundle();

            submitAndScheduleProcess(UA1Bundle);
            submitAndScheduleProcess(UA2Bundle);

            //now submit the thing to prism
            Util.assertSucceeded(
                    prismHelper.getProcessHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getProcessData()));
            //fetch the initial store and archive state for prism
            List<String> initialPrismStore = prismHelper.getProcessHelper().getStoreInfo();
            List<String> initialPrismArchiveStore = prismHelper.getProcessHelper().getArchiveInfo();

            //fetch the initial store and archive for both colos
            List<String> initialUA1Store = UA1ColoHelper.getProcessHelper().getStoreInfo();
            List<String> initialUA1ArchiveStore = UA1ColoHelper.getProcessHelper().getArchiveInfo();

            List<String> initialUA2Store = UA2ColoHelper.getProcessHelper().getStoreInfo();
            List<String> initialUA2ArchiveStore = UA2ColoHelper.getProcessHelper().getArchiveInfo();

            //shutdown UA1
            Util.shutDownService(UA1ColoHelper.getFeedHelper());

            //lets now delete the cluster from both colos
            Util.assertSucceeded(
                    prismHelper.getProcessHelper()
                            .delete(Util.URLS.DELETE_URL, UA2Bundle.getProcessData()));

            //now lets get the final states
            List<String> finalPrismStore = prismHelper.getProcessHelper().getStoreInfo();
            List<String> finalPrismArchiveStore = prismHelper.getProcessHelper().getArchiveInfo();

            //fetch the final store and archive for both colos
            List<String> finalUA1Store = UA1ColoHelper.getProcessHelper().getStoreInfo();
            List<String> finalUA1ArchiveStore = UA1ColoHelper.getProcessHelper().getArchiveInfo();

            List<String> finalUA2Store = UA2ColoHelper.getProcessHelper().getStoreInfo();
            List<String> finalUA2ArchiveStore = UA2ColoHelper.getProcessHelper().getArchiveInfo();

            //now ensure that data has been deleted from all cluster store and is present in the
            // cluster archives

            String clusterName = Util.readEntityName(bundle.getProcessData());
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
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        } finally {

            Util.restartService(UA1ColoHelper.getFeedHelper());
        }

    }


    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testDeleteProcessSuspendedInOneColoWhileAnotherColoIsDownWithFeedSuspended(
            Bundle bundle)
    throws Exception {
        try {
            //create a UA1 bundle
            Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());
            UA2Bundle.generateUniqueBundle();

            Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
            UA1Bundle.generateUniqueBundle();

            submitAndScheduleProcess(UA1Bundle);
            submitAndScheduleProcess(UA2Bundle);

            //now submit the thing to prism
            Util.assertSucceeded(
                    prismHelper.getProcessHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getProcessData()));
            Util.assertSucceeded(
                    prismHelper.getProcessHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getProcessData()));
            //fetch the initial store and archive state for prism
            List<String> initialPrismStore = prismHelper.getProcessHelper().getStoreInfo();
            List<String> initialPrismArchiveStore = prismHelper.getProcessHelper().getArchiveInfo();

            //fetch the initial store and archive for both colos
            List<String> initialUA1Store = UA1ColoHelper.getProcessHelper().getStoreInfo();
            List<String> initialUA1ArchiveStore = UA1ColoHelper.getProcessHelper().getArchiveInfo();

            List<String> initialUA2Store = UA2ColoHelper.getProcessHelper().getStoreInfo();
            List<String> initialUA2ArchiveStore = UA2ColoHelper.getProcessHelper().getArchiveInfo();

            //shutdown UA1
            Util.shutDownService(UA1ColoHelper.getFeedHelper());

            //lets now delete the cluster from both colos
            Util.assertSucceeded(
                    prismHelper.getProcessHelper()
                            .delete(Util.URLS.DELETE_URL, UA2Bundle.getProcessData()));

            //now lets get the final states
            List<String> finalPrismStore = prismHelper.getProcessHelper().getStoreInfo();
            List<String> finalPrismArchiveStore = prismHelper.getProcessHelper().getArchiveInfo();

            //fetch the final store and archive for both colos
            List<String> finalUA1Store = UA1ColoHelper.getProcessHelper().getStoreInfo();
            List<String> finalUA1ArchiveStore = UA1ColoHelper.getProcessHelper().getArchiveInfo();

            List<String> finalUA2Store = UA2ColoHelper.getProcessHelper().getStoreInfo();
            List<String> finalUA2ArchiveStore = UA2ColoHelper.getProcessHelper().getArchiveInfo();

            //now ensure that data has been deleted from all cluster store and is present in the
            // cluster archives

            String clusterName = Util.readEntityName(bundle.getProcessData());
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
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        } finally {

            Util.restartService(UA1ColoHelper.getFeedHelper());
        }

    }

    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testDeleteProcessScheduledInOneColoWhileAnotherColoIsDown(Bundle bundle)
    throws Exception {
        try {
            //create a UA1 bundle
            Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());
            UA2Bundle.generateUniqueBundle();

            Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
            UA1Bundle.generateUniqueBundle();

            submitAndScheduleProcess(UA1Bundle);
            submitAndScheduleProcess(UA2Bundle);

            //fetch the initial store and archive state for prism
            List<String> initialPrismStore = prismHelper.getProcessHelper().getStoreInfo();
            List<String> initialPrismArchiveStore = prismHelper.getProcessHelper().getArchiveInfo();

            //fetch the initial store and archive for both colos
            List<String> initialUA1Store = UA1ColoHelper.getProcessHelper().getStoreInfo();
            List<String> initialUA1ArchiveStore = UA1ColoHelper.getProcessHelper().getArchiveInfo();

            List<String> initialUA2Store = UA2ColoHelper.getProcessHelper().getStoreInfo();
            List<String> initialUA2ArchiveStore = UA2ColoHelper.getProcessHelper().getArchiveInfo();

            //shutdown UA1
            Util.shutDownService(UA1ColoHelper.getFeedHelper());

            //lets now delete the cluster from both colos
            Util.assertSucceeded(
                    prismHelper.getProcessHelper()
                            .delete(Util.URLS.DELETE_URL, UA2Bundle.getProcessData()));

            //now lets get the final states
            List<String> finalPrismStore = prismHelper.getProcessHelper().getStoreInfo();
            List<String> finalPrismArchiveStore = prismHelper.getProcessHelper().getArchiveInfo();

            //fetch the final store and archive for both colos
            List<String> finalUA1Store = UA1ColoHelper.getProcessHelper().getStoreInfo();
            List<String> finalUA1ArchiveStore = UA1ColoHelper.getProcessHelper().getArchiveInfo();

            List<String> finalUA2Store = UA2ColoHelper.getProcessHelper().getStoreInfo();
            List<String> finalUA2ArchiveStore = UA2ColoHelper.getProcessHelper().getArchiveInfo();

            //now ensure that data has been deleted from all cluster store and is present in the
            // cluster archives

            String clusterName = Util.readEntityName(UA2Bundle.getProcessData());
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


            Util.startService(UA1ColoHelper.getClusterHelper());

            Util.assertSucceeded(
                    prismHelper.getProcessHelper()
                            .delete(Util.URLS.DELETE_URL, UA1Bundle.getProcessData()));

            HashMap<String, List<String>> systemPostUp = getSystemState(ENTITY_TYPE.PROCESS);

            clusterName = Util.readEntityName(UA1Bundle.getProcessData());

            compareDataStoresForEquality(finalUA2Store, systemPostUp.get("ua2Store"));
            compareDataStoresForEquality(finalUA2ArchiveStore, systemPostUp.get("ua2Archive"));

            compareDataStoreStates(finalPrismStore, systemPostUp.get("prismStore"), clusterName);
            compareDataStoreStates(systemPostUp.get("prismArchive"), finalPrismArchiveStore,
                    clusterName);

            compareDataStoreStates(finalUA1Store, systemPostUp.get("ua1Store"), clusterName);
            compareDataStoreStates(systemPostUp.get("ua1Archive"), finalUA1ArchiveStore,
                    clusterName);

        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        } finally {

            Util.restartService(UA1ColoHelper.getFeedHelper());
        }

    }


    @DataProvider(name = "DP")
    public Object[][] getData() throws Exception {
        return Util.readBundles("LateDataBundles");
    }

    private void compareDataStoreStates(List<String> initialState, List<String> finalState,
                                        String filename)
    throws Exception {

        List<String> temp = new ArrayList<String>(initialState);
        temp.removeAll(finalState);
        Assert.assertEquals(temp.size(), 1);
        Assert.assertTrue(temp.get(0).contains(filename));

    }


    private void submitAndScheduleProcess(Bundle bundle) throws Exception {
        for (String cluster : bundle.getClusters()) {
            Util.assertSucceeded(
                    prismHelper.getClusterHelper().submitEntity(Util.URLS.SUBMIT_URL, cluster));
        }

        for (String feed : bundle.getDataSets()) {
            Util.assertSucceeded(prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed));
        }

        Util.assertSucceeded(prismHelper.getProcessHelper()
                .submitEntity(URLS.SUBMIT_URL, bundle.getProcessData()));
        Util.assertSucceeded(prismHelper.getProcessHelper()
                .schedule(URLS.SCHEDULE_URL, bundle.getProcessData()));
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
