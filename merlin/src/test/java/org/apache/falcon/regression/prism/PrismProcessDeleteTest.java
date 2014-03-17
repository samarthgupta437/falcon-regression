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


import junit.framework.Assert;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.interfaces.IEntityManagerHelper;
import org.apache.falcon.regression.core.enumsAndConstants.ENTITY_TYPE;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.testng.TestNGException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

@Test(groups = "distributed")
public class PrismProcessDeleteTest extends BaseTestClass {

    Bundle bundle;
    ColoHelper cluster1 = servers.get(0);
    ColoHelper cluster2 = servers.get(1);

    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        Util.print("test name: " + method.getName());
        bundle = Util.readBundles("LateDataBundles")[0][0];
        bundles[0] = new Bundle(bundle, cluster2.getEnvFileName(), cluster2.getPrefix());
        bundles[0].generateUniqueBundle();
        bundles[1] = new Bundle(bundle, cluster1.getEnvFileName(), cluster1.getPrefix());
        bundles[1].generateUniqueBundle();
    }

    @AfterMethod
    public void tearDown() throws Exception {
        removeBundles();
    }

	/* NOTE: All test cases assume that there are two entities scheduled in each colo
        com.inmobi.qa.airavatqa.prism.PrismProcessDeleteTest
        .testUA1ProcessDeleteAlreadyDeletedProcess */


    @Test(groups = {"prism", "0.2"})
    public void testUA1ProcessDeleteInBothColos() throws Exception {
        //now submit the thing to prism
        bundles[0].submitFeedsScheduleProcess();
        //fetch the initial store and archive state for prism
        List<String> initialPrismStore = prism.getProcessHelper().getStoreInfo();
        List<String> initialPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

        //fetch the initial store and archive for both colos
        List<String> initialUA1Store = cluster2.getProcessHelper().getStoreInfo();
        List<String> initialUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

        List<String> initialUA2Store = cluster1.getProcessHelper().getStoreInfo();
        List<String> initialUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

        //lets now delete the cluster from both colos
        Util.assertSucceeded(prism.getProcessHelper()
                .delete(Util.URLS.DELETE_URL, bundles[0].getProcessData()));

        //now lets get the final states
        List<String> finalPrismStore = prism.getProcessHelper().getStoreInfo();
        List<String> finalPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

        //fetch the final store and archive for both colos
        List<String> finalUA1Store = cluster2.getProcessHelper().getStoreInfo();
        List<String> finalUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

        List<String> finalUA2Store = cluster1.getProcessHelper().getStoreInfo();
        List<String> finalUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

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

    @Test(groups = {"prism", "0.2"})
    public void testUA1ProcessDeleteWhen1ColoIsDown() throws Exception {
        try {
            //now submit the thing to prism
            bundles[0].submitFeedsScheduleProcess();
            //fetch the initial store and archive state for prism
            List<String> initialPrismStore = prism.getProcessHelper().getStoreInfo();
            List<String> initialPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

            //fetch the initial store and archive for both colos
            List<String> initialUA1Store = cluster2.getProcessHelper().getStoreInfo();
            List<String> initialUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

            List<String> initialUA2Store = cluster1.getProcessHelper().getStoreInfo();
            List<String> initialUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();


            //bring down UA2 colo :P
            Util.shutDownService(cluster2.getClusterHelper());

            //lets now delete the cluster from both colos
            Util.assertFailed(prism.getProcessHelper()
                    .delete(Util.URLS.DELETE_URL, bundles[0].getProcessData()));

            //now lets get the final states
            List<String> finalPrismStore = prism.getProcessHelper().getStoreInfo();
            List<String> finalPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

            //fetch the final store and archive for both colos
            List<String> finalUA1Store = cluster2.getProcessHelper().getStoreInfo();
            List<String> finalUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

            List<String> finalUA2Store = cluster1.getProcessHelper().getStoreInfo();
            List<String> finalUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

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
            compareDataStoresForEquality(initialUA1Store, finalUA1Store);

            //bring service up
            Util.startService(cluster2.getProcessHelper());
            Util.assertSucceeded(
                    prism.getProcessHelper()
                            .delete(Util.URLS.DELETE_URL, bundles[0].getProcessData()));

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
            Util.restartService(cluster2.getClusterHelper());
        }
    }


    @Test(groups = {"prism", "0.2"})
    public void testUA1ProcessDeleteAlreadyDeletedProcess() throws Exception {
        try {
            //now submit the thing to prism
            bundles[0].submitFeedsScheduleProcess();
            Util.assertSucceeded(
                    prism.getProcessHelper()
                            .delete(Util.URLS.DELETE_URL, bundles[0].getProcessData())
            );
            //fetch the initial store and archive state for prism
            List<String> initialPrismStore = prism.getProcessHelper().getStoreInfo();
            List<String> initialPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

            //fetch the initial store and archive for both colos
            List<String> initialUA1Store = cluster2.getProcessHelper().getStoreInfo();
            List<String> initialUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

            List<String> initialUA2Store = cluster1.getProcessHelper().getStoreInfo();
            List<String> initialUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

            Util.assertSucceeded(
                    prism.getProcessHelper()
                            .delete(Util.URLS.DELETE_URL, bundles[0].getProcessData())
            );

            //now lets get the final states
            List<String> finalPrismStore = prism.getProcessHelper().getStoreInfo();
            List<String> finalPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

            //fetch the final store and archive for both colos
            List<String> finalUA1Store = cluster2.getProcessHelper().getStoreInfo();
            List<String> finalUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

            List<String> finalUA2Store = cluster1.getProcessHelper().getStoreInfo();
            List<String> finalUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

            //now ensure that data has been deleted from all cluster store and is present in the
            // cluster archives

            String clusterName = Util.readEntityName(bundle.getProcessData());
            //prism:
            compareDataStoresForEquality(initialPrismStore, finalPrismStore);
            compareDataStoresForEquality(initialPrismArchiveStore, finalPrismArchiveStore);
            //UA2:
            compareDataStoresForEquality(initialUA2Store, finalUA2Store);
            compareDataStoresForEquality(initialUA2ArchiveStore, finalUA2ArchiveStore);
            //UA1:
            compareDataStoresForEquality(initialUA1Store, finalUA1Store);
            compareDataStoresForEquality(initialUA1ArchiveStore, finalUA1ArchiveStore);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            throw new TestNGException(e.getMessage());
        }
    }


    @Test(groups = {"prism", "0.2"})
    public void testUA1ProcessDeleteTwiceWhen1ColoIsDownDuring1stDelete()
    throws Exception {
        try {
            bundles[0].submitFeedsScheduleProcess();

            Util.shutDownService(cluster2.getClusterHelper());

            //lets now delete the cluster from both colos
            Util.assertFailed(prism.getProcessHelper()
                    .delete(Util.URLS.DELETE_URL, bundles[0].getProcessData()));

            //now lets get the final states
            List<String> initialPrismStore = prism.getProcessHelper().getStoreInfo();
            List<String> initialPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

            //fetch the initial store and archive for both colos
            List<String> initialUA1Store = cluster2.getProcessHelper().getStoreInfo();
            List<String> initialUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

            List<String> initialUA2Store = cluster1.getProcessHelper().getStoreInfo();
            List<String> initialUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

            //start up service
            Util.startService(cluster2.getClusterHelper());

            //delete again
            Util.assertSucceeded(
                    prism.getProcessHelper()
                            .delete(Util.URLS.DELETE_URL, bundles[0].getProcessData()));

            //get final states
            List<String> finalPrismStore = prism.getProcessHelper().getStoreInfo();
            List<String> finalPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

            //fetch the final store and archive for both colos
            List<String> finalUA1Store = cluster2.getProcessHelper().getStoreInfo();
            List<String> finalUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

            List<String> finalUA2Store = cluster1.getProcessHelper().getStoreInfo();
            List<String> finalUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

            //now ensure that data has been deleted from all cluster store and is present in the
            // cluster archives

            String clusterName = Util.readEntityName(bundle.getProcessData());
            //prism:
            compareDataStoreStates(initialPrismStore, finalPrismStore, clusterName);
            compareDataStoreStates(finalPrismArchiveStore, initialPrismArchiveStore, clusterName);

            //UA2:
            compareDataStoresForEquality(initialUA2Store, finalUA2Store);
            compareDataStoresForEquality(initialUA2ArchiveStore, finalUA2ArchiveStore);

            //UA1:
            compareDataStoreStates(initialUA1Store, finalUA1Store, clusterName);
            compareDataStoreStates(finalUA1ArchiveStore, initialUA1ArchiveStore, clusterName);

        } catch (Exception e) {
            System.out.println(e.getMessage());
            throw new TestNGException(e.getMessage());
        } finally {
            Util.restartService(cluster2.getClusterHelper());
        }
    }

    @Test(groups = {"prism", "0.2"})
    public void testUA1ProcessDeleteNonExistent() throws Exception {
        try {
            //now lets get the final states
            List<String> initialPrismStore = prism.getProcessHelper().getStoreInfo();
            List<String> initialPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

            //fetch the initial store and archive for both colos
            List<String> initialUA1Store = cluster2.getProcessHelper().getStoreInfo();
            List<String> initialUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

            List<String> initialUA2Store = cluster1.getProcessHelper().getStoreInfo();
            List<String> initialUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

            //delete
            Util.assertSucceeded(
                    prism.getProcessHelper()
                            .delete(Util.URLS.DELETE_URL, bundles[0].getProcessData()));

            //get final states
            List<String> finalPrismStore = prism.getProcessHelper().getStoreInfo();
            List<String> finalPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

            //fetch the final store and archive for both colos
            List<String> finalUA1Store = cluster2.getProcessHelper().getStoreInfo();
            List<String> finalUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

            List<String> finalUA2Store = cluster1.getProcessHelper().getStoreInfo();
            List<String> finalUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

            //now ensure that data has been deleted from all cluster store and is present in the
            // cluster archives

            String clusterName = Util.readEntityName(bundle.getProcessData());
            //prism:
            compareDataStoresForEquality(initialPrismStore, finalPrismStore);
            compareDataStoresForEquality(initialPrismArchiveStore, finalPrismArchiveStore);

            //UA2:
            compareDataStoresForEquality(initialUA2Store, finalUA2Store);
            compareDataStoresForEquality(initialUA2ArchiveStore, finalUA2ArchiveStore);

            //UA1:
            compareDataStoresForEquality(initialUA1Store, finalUA1Store);
            compareDataStoresForEquality(initialUA1ArchiveStore, finalUA1ArchiveStore);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            throw new TestNGException(e.getMessage());
        }
    }


    @Test(groups = {"prism", "0.2"})
    public void testUA1ProcessDeleteNonExistentWhen1ColoIsDownDuringDelete()
    throws Exception {
        try {
            //now lets get the final states
            List<String> initialPrismStore = prism.getProcessHelper().getStoreInfo();
            List<String> initialPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

            //fetch the initial store and archive for both colos
            List<String> initialUA1Store = cluster2.getProcessHelper().getStoreInfo();
            List<String> initialUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

            List<String> initialUA2Store = cluster1.getProcessHelper().getStoreInfo();
            List<String> initialUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

            //bring down UA1
            Util.shutDownService(cluster2.getClusterHelper());

            //delete
            Util.assertSucceeded(
                    prism.getProcessHelper()
                            .delete(Util.URLS.DELETE_URL, bundles[0].getProcessData())
            );

            //get final states
            List<String> finalPrismStore = prism.getProcessHelper().getStoreInfo();
            List<String> finalPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

            //fetch the final store and archive for both colos
            List<String> finalUA1Store = cluster2.getProcessHelper().getStoreInfo();
            List<String> finalUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

            List<String> finalUA2Store = cluster1.getProcessHelper().getStoreInfo();
            List<String> finalUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

            //now ensure that data has been deleted from all cluster store and is present in the
            // cluster archives

            String clusterName = Util.readEntityName(bundle.getProcessData());
            //prism:
            compareDataStoresForEquality(initialPrismStore, finalPrismStore);
            compareDataStoresForEquality(initialPrismArchiveStore, finalPrismArchiveStore);

            //UA2:
            compareDataStoresForEquality(initialUA2Store, finalUA2Store);
            compareDataStoresForEquality(initialUA2ArchiveStore, finalUA2ArchiveStore);

            //UA1:
            compareDataStoresForEquality(initialUA1Store, finalUA1Store);
            compareDataStoresForEquality(initialUA1ArchiveStore, finalUA1ArchiveStore);

            Util.startService(cluster2.getClusterHelper());
            Util.assertSucceeded(
                    prism.getProcessHelper()
                            .delete(Util.URLS.DELETE_URL, bundles[0].getProcessData())
            );

        } catch (Exception e) {
            System.out.println(e.getMessage());
            throw new TestNGException(e.getMessage());
        } finally {
            Util.restartService(cluster2.getClusterHelper());
        }
    }


    @Test(groups = {"prism", "0.2"})
    public void testDeleteProcessScheduledInOneColo() throws Exception {
        bundles[0].submitFeedsScheduleProcess();
        bundles[1].submitFeedsScheduleProcess();

        //fetch the initial store and archive state for prism
        List<String> initialPrismStore = prism.getProcessHelper().getStoreInfo();
        List<String> initialPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

        //fetch the initial store and archive for both colos
        List<String> initialUA1Store = cluster2.getProcessHelper().getStoreInfo();
        List<String> initialUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

        List<String> initialUA2Store = cluster1.getProcessHelper().getStoreInfo();
        List<String> initialUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

        //lets now delete the cluster from both colos
        Util.assertSucceeded(prism.getProcessHelper()
                .delete(Util.URLS.DELETE_URL, bundles[0].getProcessData()));

        //now lets get the final states
        List<String> finalPrismStore = prism.getProcessHelper().getStoreInfo();
        List<String> finalPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

        //fetch the final store and archive for both colos
        List<String> finalUA1Store = cluster2.getProcessHelper().getStoreInfo();
        List<String> finalUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

        List<String> finalUA2Store = cluster1.getProcessHelper().getStoreInfo();
        List<String> finalUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

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

    @Test(groups = {"prism", "0.2"})
    public void testDeleteProcessSuspendedInOneColo() throws Exception {
        //create a UA1 bundle
        bundles[0].submitFeedsScheduleProcess();
        bundles[1].submitFeedsScheduleProcess();

        //suspend UA1 colo thingy
        Util.assertSucceeded(prism.getProcessHelper()
                .suspend(URLS.SUSPEND_URL, bundles[0].getProcessData()));

        //fetch the initial store and archive state for prism
        List<String> initialPrismStore = prism.getProcessHelper().getStoreInfo();
        List<String> initialPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

        //fetch the initial store and archive for both colos
        List<String> initialUA1Store = cluster2.getProcessHelper().getStoreInfo();
        List<String> initialUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

        List<String> initialUA2Store = cluster1.getProcessHelper().getStoreInfo();
        List<String> initialUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

        //lets now delete the cluster from both colos
        Util.assertSucceeded(prism.getProcessHelper()
                .delete(Util.URLS.DELETE_URL, bundles[0].getProcessData()));

        //now lets get the final states
        List<String> finalPrismStore = prism.getProcessHelper().getStoreInfo();
        List<String> finalPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

        //fetch the final store and archive for both colos
        List<String> finalUA1Store = cluster2.getProcessHelper().getStoreInfo();
        List<String> finalUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

        List<String> finalUA2Store = cluster1.getProcessHelper().getStoreInfo();
        List<String> finalUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

        //now ensure that data has been deleted from all cluster store and is present in the
        // cluster archives

        String clusterName = Util.readEntityName(bundles[0].getProcessData());
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


    @Test(groups = {"prism", "0.2"})
    public void testDeleteProcessSuspendedInOneColoWhileBothProcessesAreSuspended()
    throws Exception {
        bundles[0].submitFeedsScheduleProcess();
        bundles[1].submitFeedsScheduleProcess();

        //suspend UA1 colo thingy
        Util.assertSucceeded(prism.getProcessHelper()
                .suspend(URLS.SUSPEND_URL, bundles[0].getProcessData()));
        Util.assertSucceeded(prism.getProcessHelper()
                .suspend(URLS.SUSPEND_URL, bundles[1].getProcessData()));

        //fetch the initial store and archive state for prism
        List<String> initialPrismStore = prism.getProcessHelper().getStoreInfo();
        List<String> initialPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

        //fetch the initial store and archive for both colos
        List<String> initialUA1Store = cluster2.getProcessHelper().getStoreInfo();
        List<String> initialUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

        List<String> initialUA2Store = cluster1.getProcessHelper().getStoreInfo();
        List<String> initialUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

        //lets now delete the cluster from both colos
        Util.assertSucceeded(prism.getProcessHelper()
                .delete(Util.URLS.DELETE_URL, bundles[0].getProcessData()));

        //now lets get the final states
        List<String> finalPrismStore = prism.getProcessHelper().getStoreInfo();
        List<String> finalPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

        //fetch the final store and archive for both colos
        List<String> finalUA1Store = cluster2.getProcessHelper().getStoreInfo();
        List<String> finalUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

        List<String> finalUA2Store = cluster1.getProcessHelper().getStoreInfo();
        List<String> finalUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

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

    @Test(groups = {"prism", "0.2"})
    public void testDeleteProcessSuspendedInOneColoWhileThatColoIsDown()
    throws Exception {
        try {
            bundles[0].submitFeedsScheduleProcess();
            bundles[1].submitFeedsScheduleProcess();

            Util.assertSucceeded(
                    prism.getProcessHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundles[0].getProcessData())
            );

            //fetch the initial store and archive state for prism
            List<String> initialPrismStore = prism.getProcessHelper().getStoreInfo();
            List<String> initialPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

            //fetch the initial store and archive for both colos
            List<String> initialUA1Store = cluster2.getProcessHelper().getStoreInfo();
            List<String> initialUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

            List<String> initialUA2Store = cluster1.getProcessHelper().getStoreInfo();
            List<String> initialUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

            //shutdown UA1
            Util.shutDownService(cluster2.getFeedHelper());

            //lets now delete the cluster from both colos
            Util.assertFailed(prism.getProcessHelper()
                    .delete(Util.URLS.DELETE_URL, bundles[0].getProcessData()));

            //now lets get the final states
            List<String> finalPrismStore = prism.getProcessHelper().getStoreInfo();
            List<String> finalPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

            //fetch the final store and archive for both colos
            List<String> finalUA1Store = cluster2.getProcessHelper().getStoreInfo();
            List<String> finalUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

            List<String> finalUA2Store = cluster1.getProcessHelper().getStoreInfo();
            List<String> finalUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

            //now ensure that data has been deleted from all cluster store and is present in the
            // cluster archives

            String clusterName = Util.readEntityName(bundle.getProcessData());
            //prism:
            compareDataStoresForEquality(initialPrismStore, finalPrismStore);
            compareDataStoresForEquality(finalPrismArchiveStore, initialPrismArchiveStore);

            //UA1:
            compareDataStoresForEquality(initialUA1Store, finalUA1Store);
            compareDataStoresForEquality(initialUA1ArchiveStore, finalUA1ArchiveStore);

            //UA2:
            compareDataStoresForEquality(initialUA2Store, finalUA2Store);
            compareDataStoresForEquality(finalUA2ArchiveStore, initialUA2ArchiveStore);
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        } finally {
            Util.restartService(cluster2.getFeedHelper());
        }
    }


    @Test(groups = {"prism", "0.2"})
    public void testDeleteProcessScheduledInOneColoWhileThatColoIsDown()
    throws Exception {
        try {
            bundles[0].submitFeedsScheduleProcess();
            bundles[1].submitFeedsScheduleProcess();

            //fetch the initial store and archive state for prism
            List<String> initialPrismStore = prism.getProcessHelper().getStoreInfo();
            List<String> initialPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

            //fetch the initial store and archive for both colos
            List<String> initialUA1Store = cluster2.getProcessHelper().getStoreInfo();
            List<String> initialUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

            List<String> initialUA2Store = cluster1.getProcessHelper().getStoreInfo();
            List<String> initialUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

            //shutdown UA1
            Util.shutDownService(cluster2.getFeedHelper());

            //lets now delete the cluster from both colos
            Util.assertFailed(prism.getProcessHelper()
                    .delete(Util.URLS.DELETE_URL, bundles[0].getProcessData()));

            //now lets get the final states
            List<String> finalPrismStore = prism.getProcessHelper().getStoreInfo();
            List<String> finalPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

            //fetch the final store and archive for both colos
            List<String> finalUA1Store = cluster2.getProcessHelper().getStoreInfo();
            List<String> finalUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

            List<String> finalUA2Store = cluster1.getProcessHelper().getStoreInfo();
            List<String> finalUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

            //now ensure that data has been deleted from all cluster store and is present in the
            // cluster archives

            String clusterName = Util.readEntityName(bundles[0].getProcessData());
            //prism:
            compareDataStoresForEquality(initialPrismStore, finalPrismStore);
            compareDataStoresForEquality(finalPrismArchiveStore, initialPrismArchiveStore);

            //UA1:
            compareDataStoresForEquality(initialUA1Store, finalUA1Store);
            compareDataStoresForEquality(initialUA1ArchiveStore, finalUA1ArchiveStore);

            //UA2:
            compareDataStoresForEquality(initialUA2Store, finalUA2Store);
            compareDataStoresForEquality(finalUA2ArchiveStore, initialUA2ArchiveStore);

            Util.startService(cluster2.getClusterHelper());
            Util.assertSucceeded(
                    prism.getProcessHelper()
                            .delete(Util.URLS.DELETE_URL, bundles[0].getProcessData())
            );

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
            Util.restartService(cluster2.getFeedHelper());
        }
    }

    @Test(groups = {"prism", "0.2"})
    public void testDeleteProcessSuspendedInOneColoWhileAnotherColoIsDown()
    throws Exception {
        try {
            bundles[0].submitFeedsScheduleProcess();
            bundles[1].submitFeedsScheduleProcess();

            //now submit the thing to prism
            Util.assertSucceeded(
                    prism.getProcessHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundles[1].getProcessData())
            );
            //fetch the initial store and archive state for prism
            List<String> initialPrismStore = prism.getProcessHelper().getStoreInfo();
            List<String> initialPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

            //fetch the initial store and archive for both colos
            List<String> initialUA1Store = cluster2.getProcessHelper().getStoreInfo();
            List<String> initialUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

            List<String> initialUA2Store = cluster1.getProcessHelper().getStoreInfo();
            List<String> initialUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

            //shutdown UA1
            Util.shutDownService(cluster2.getFeedHelper());

            //lets now delete the cluster from both colos
            Util.assertSucceeded(
                    prism.getProcessHelper()
                            .delete(Util.URLS.DELETE_URL, bundles[1].getProcessData())
            );

            //now lets get the final states
            List<String> finalPrismStore = prism.getProcessHelper().getStoreInfo();
            List<String> finalPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

            //fetch the final store and archive for both colos
            List<String> finalUA1Store = cluster2.getProcessHelper().getStoreInfo();
            List<String> finalUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

            List<String> finalUA2Store = cluster1.getProcessHelper().getStoreInfo();
            List<String> finalUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

            //now ensure that data has been deleted from all cluster store and is present in the
            // cluster archives

            String clusterName = Util.readEntityName(bundle.getProcessData());
            //prism:
            compareDataStoreStates(initialPrismStore, finalPrismStore, clusterName);
            compareDataStoreStates(finalPrismArchiveStore, initialPrismArchiveStore, clusterName);

            //UA1:
            compareDataStoresForEquality(initialUA1Store, finalUA1Store);
            compareDataStoresForEquality(initialUA1ArchiveStore, finalUA1ArchiveStore);

            //UA2:
            compareDataStoreStates(initialUA2Store, finalUA2Store, clusterName);
            compareDataStoreStates(finalUA2ArchiveStore, initialUA2ArchiveStore, clusterName);
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        } finally {
            Util.restartService(cluster2.getFeedHelper());
        }
    }


    @Test(groups = {"prism", "0.2"})
    public void testDeleteProcessSuspendedInOneColoWhileAnotherColoIsDownWithFeedSuspended() throws Exception {
        try {
            bundles[0].submitFeedsScheduleProcess();
            bundles[1].submitFeedsScheduleProcess();

            //now submit the thing to prism
            Util.assertSucceeded(
                    prism.getProcessHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundles[1].getProcessData())
            );
            Util.assertSucceeded(
                    prism.getProcessHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundles[0].getProcessData()));
            //fetch the initial store and archive state for prism
            List<String> initialPrismStore = prism.getProcessHelper().getStoreInfo();
            List<String> initialPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

            //fetch the initial store and archive for both colos
            List<String> initialUA1Store = cluster2.getProcessHelper().getStoreInfo();
            List<String> initialUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

            List<String> initialUA2Store = cluster1.getProcessHelper().getStoreInfo();
            List<String> initialUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

            //shutdown UA1
            Util.shutDownService(cluster2.getFeedHelper());

            //lets now delete the cluster from both colos
            Util.assertSucceeded(
                    prism.getProcessHelper()
                            .delete(Util.URLS.DELETE_URL, bundles[1].getProcessData())
            );

            //now lets get the final states
            List<String> finalPrismStore = prism.getProcessHelper().getStoreInfo();
            List<String> finalPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

            //fetch the final store and archive for both colos
            List<String> finalUA1Store = cluster2.getProcessHelper().getStoreInfo();
            List<String> finalUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

            List<String> finalUA2Store = cluster1.getProcessHelper().getStoreInfo();
            List<String> finalUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

            //now ensure that data has been deleted from all cluster store and is present in the
            // cluster archives

            String clusterName = Util.readEntityName(bundle.getProcessData());
            //prism:
            compareDataStoreStates(initialPrismStore, finalPrismStore, clusterName);
            compareDataStoreStates(finalPrismArchiveStore, initialPrismArchiveStore, clusterName);

            //UA1:
            compareDataStoresForEquality(initialUA1Store, finalUA1Store);
            compareDataStoresForEquality(initialUA1ArchiveStore, finalUA1ArchiveStore);

            //UA2:
            compareDataStoreStates(initialUA2Store, finalUA2Store, clusterName);
            compareDataStoreStates(finalUA2ArchiveStore, initialUA2ArchiveStore, clusterName);
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        } finally {
            Util.restartService(cluster2.getFeedHelper());
        }
    }

    @Test(groups = {"prism", "0.2"})
    public void testDeleteProcessScheduledInOneColoWhileAnotherColoIsDown()
    throws Exception {
        try {
            bundles[0].submitFeedsScheduleProcess();
            bundles[1].submitFeedsScheduleProcess();

            //fetch the initial store and archive state for prism
            List<String> initialPrismStore = prism.getProcessHelper().getStoreInfo();
            List<String> initialPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

            //fetch the initial store and archive for both colos
            List<String> initialUA1Store = cluster2.getProcessHelper().getStoreInfo();
            List<String> initialUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

            List<String> initialUA2Store = cluster1.getProcessHelper().getStoreInfo();
            List<String> initialUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

            //shutdown UA1
            Util.shutDownService(cluster2.getFeedHelper());

            //lets now delete the cluster from both colos
            Util.assertSucceeded(
                    prism.getProcessHelper()
                            .delete(Util.URLS.DELETE_URL, bundles[1].getProcessData())
            );

            //now lets get the final states
            List<String> finalPrismStore = prism.getProcessHelper().getStoreInfo();
            List<String> finalPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

            //fetch the final store and archive for both colos
            List<String> finalUA1Store = cluster2.getProcessHelper().getStoreInfo();
            List<String> finalUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

            List<String> finalUA2Store = cluster1.getProcessHelper().getStoreInfo();
            List<String> finalUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

            //now ensure that data has been deleted from all cluster store and is present in the
            // cluster archives

            String clusterName = Util.readEntityName(bundles[1].getProcessData());
            //prism:
            compareDataStoreStates(initialPrismStore, finalPrismStore, clusterName);
            compareDataStoreStates(finalPrismArchiveStore, initialPrismArchiveStore, clusterName);

            //UA1:
            compareDataStoresForEquality(initialUA1Store, finalUA1Store);
            compareDataStoresForEquality(initialUA1ArchiveStore, finalUA1ArchiveStore);

            //UA2:
            compareDataStoreStates(initialUA2Store, finalUA2Store, clusterName);
            compareDataStoreStates(finalUA2ArchiveStore, initialUA2ArchiveStore, clusterName);


            Util.startService(cluster2.getClusterHelper());

            Util.assertSucceeded(
                    prism.getProcessHelper()
                            .delete(Util.URLS.DELETE_URL, bundles[0].getProcessData())
            );

            HashMap<String, List<String>> systemPostUp = getSystemState(ENTITY_TYPE.PROCESS);

            clusterName = Util.readEntityName(bundles[0].getProcessData());

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
            Util.restartService(cluster2.getFeedHelper());
        }
    }


    private void compareDataStoreStates(List<String> initialState, List<String> finalState,
                                        String filename)
    throws Exception {

        List<String> temp = new ArrayList<String>(initialState);
        temp.removeAll(finalState);
        Assert.assertEquals(temp.size(), 1);
        Assert.assertTrue(temp.get(0).contains(filename));

    }


    private void compareDataStoresForEquality(List<String> store1, List<String> store2)
    throws Exception {
        Assert.assertTrue(Arrays.deepEquals(store2.toArray(new String[store2.size()]),
                store1.toArray(new String[store1.size()])));
    }

    public HashMap<String, List<String>> getSystemState(ENTITY_TYPE entityType) throws Exception {
        IEntityManagerHelper prizm = prism.getClusterHelper();
        IEntityManagerHelper ua1 = cluster2.getClusterHelper();
        IEntityManagerHelper ua2 = cluster1.getClusterHelper();

        if (entityType.equals(ENTITY_TYPE.DATA)) {
            prizm = prism.getFeedHelper();
            ua1 = cluster2.getFeedHelper();
            ua2 = cluster1.getFeedHelper();
        }

        if (entityType.equals(ENTITY_TYPE.PROCESS)) {
            prizm = prism.getProcessHelper();
            ua1 = cluster2.getProcessHelper();
            ua2 = cluster1.getProcessHelper();
        }

        HashMap<String, List<String>> temp = new HashMap<String, List<String>>();
        temp.put("prismArchive", prizm.getArchiveInfo());
        temp.put("prismStore", prizm.getStoreInfo());
        temp.put("ua1Archive", ua1.getArchiveInfo());
        temp.put("ua1Store", ua1.getStoreInfo());
        temp.put("ua2Archive", ua2.getArchiveInfo());
        temp.put("ua2Store", ua2.getStoreInfo());

        return temp;
    }


}
