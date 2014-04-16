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

import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.PrismUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.net.ConnectException;
import java.util.List;

@Test(groups = "distributed")
public class PrismSubmitTest extends BaseTestClass {

    ColoHelper cluster1 = servers.get(0);
    ColoHelper cluster2 = servers.get(1);
    String baseTestDir = baseHDFSDir + "/PrismSubmitTest";
    String randomHDFSPath = baseTestDir + "/someRandomPath";
    String aggregateWorkflowDir = baseTestDir + "/aggregator";
    boolean restartRequired = false;
    private static final Logger logger = Logger.getLogger(PrismSubmitTest.class);

    @BeforeClass(alwaysRun = true)
    public void uploadWorkflow() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        logger.info("test name: " + method.getName());
        restartRequired = false;
        bundles[0] = Util.readELBundles()[0][0];
        bundles[0] = new Bundle(bundles[0], cluster1);
        bundles[0].generateUniqueBundle();
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
    }


    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        if(restartRequired){
            Util.startService(prism.getFeedHelper());
            Util.startService(cluster1.getFeedHelper());
        }
        removeBundles();
    }

    @Test(groups = "distributed")
    public void submitCluster_1prism1coloPrismdown() throws Exception {
        restartRequired = true;
        Util.shutDownService(prism.getClusterHelper());

        List<String> beforeSubmit = cluster1.getClusterHelper().getStoreInfo();
        try {
            prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL,  bundles[0].getClusters().get(0));
        } catch (ConnectException e) {
            Assert.assertTrue(e.getMessage().contains("Connection to "
                    + prism.getClusterHelper().getHostname() +" refused"), e.getMessage());
        }
        List<String> afterSubmit = cluster1.getClusterHelper().getStoreInfo();

        PrismUtil.compareDataStoreStates(beforeSubmit, afterSubmit, 0);

    }

    @Test(groups = "distributed")
    public void submitCluster_resubmitDiffContent() throws Exception {
        ServiceResponse r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL,  bundles[0].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));
        List<String> beforeSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        List<String> beforeSubmitPrism = cluster2.getClusterHelper().getStoreInfo();

        bundles[0].setCLusterWorkingPath( bundles[0].getClusters().get(0), randomHDFSPath);
        logger.info("modified cluster Data: " +  bundles[0].getClusters().get(0));
        r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL,  bundles[0].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        List<String> afterSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        List<String> afterSubmitPrism = cluster2.getClusterHelper().getStoreInfo();
        PrismUtil.compareDataStoreStates(beforeSubmitCluster, afterSubmitCluster, 0);
        PrismUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism, 0);
    }

    @Test(groups = "distributed")
    public void submitCluster_resubmitAlreadyPARTIALWithAllUp() throws Exception {
            restartRequired = true;
            Util.shutDownService(cluster1.getClusterHelper());
            Thread.sleep(30000);

            ServiceResponse r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL,  bundles[0].getClusters().get(0));

            Assert.assertTrue(r.getMessage().contains("PARTIAL"));

            Util.startService(cluster1.getClusterHelper());
            Thread.sleep(30000);

            r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL,  bundles[0].getClusters().get(0));
            Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));
    }

    @Test(groups = "distributed")
    public void submitProcess_1ColoDownAfter2FeedSubmitStartAfterProcessSubmitAnsDeleteProcess() throws Exception {
        restartRequired = true;
        ServiceResponse r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL,  bundles[0].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        r = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL,  bundles[0].getDataSets().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        r = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL,  bundles[0].getDataSets().get(1));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        Util.shutDownService(cluster1.getClusterHelper());
        Thread.sleep(12000);


        List<String> beforeSubmitCluster1 = cluster1.getProcessHelper().getStoreInfo();
        List<String> beforeSubmitCluster2 = cluster2.getProcessHelper().getStoreInfo();
        List<String> beforeSubmitPrism = prism.getProcessHelper().getStoreInfo();

        r = prism.getProcessHelper().delete(URLS.DELETE_URL,  bundles[0].getProcessData());

        r = prism.getProcessHelper().submitEntity(URLS.SUBMIT_URL,  bundles[0].getProcessData());
        Util.assertFailed(r);
        List<String> afterSubmitCluster1 = cluster1.getProcessHelper().getStoreInfo();
        List<String> afterSubmitCluster2 = cluster2.getProcessHelper().getStoreInfo();
        List<String> afterSubmitPrism = prism.getProcessHelper().getStoreInfo();

        PrismUtil.compareDataStoreStates(beforeSubmitCluster1, afterSubmitCluster1, 0);
        PrismUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism,
                Util.getProcessName( bundles[0].getProcessData()), 1);
        PrismUtil.compareDataStoreStates(beforeSubmitCluster2, afterSubmitCluster2, 0);

        Util.startService(cluster1.getClusterHelper());
        Thread.sleep(15000);

        beforeSubmitCluster1 = cluster1.getProcessHelper().getStoreInfo();
        beforeSubmitCluster2 = cluster2.getProcessHelper().getStoreInfo();
        beforeSubmitPrism = prism.getProcessHelper().getStoreInfo();

        r = prism.getProcessHelper().delete(URLS.DELETE_URL,  bundles[0].getProcessData());
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        afterSubmitCluster1 = cluster1.getProcessHelper().getStoreInfo();
        afterSubmitCluster2 = cluster2.getProcessHelper().getStoreInfo();
        afterSubmitPrism = prism.getProcessHelper().getStoreInfo();

        PrismUtil.compareDataStoreStates(beforeSubmitCluster1, afterSubmitCluster1, 0);
        PrismUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism,
                Util.getProcessName( bundles[0].getProcessData()), -1);
        PrismUtil.compareDataStoreStates(beforeSubmitCluster2, afterSubmitCluster2, 0);
    }

    @Test(groups = "distributed")
    public void submitProcess_ideal() throws Exception {

        ServiceResponse r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL,  bundles[0].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        List<String> beforeSubmitCluster1 = cluster1.getFeedHelper().getStoreInfo();
        List<String> beforeSubmitCluster2 = cluster2.getFeedHelper().getStoreInfo();
        List<String> beforeSubmitPrism = prism.getFeedHelper().getStoreInfo();

        r = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL,  bundles[0].getDataSets().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        r = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL,  bundles[0].getDataSets().get(1));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        List<String> afterSubmitCluster1 = cluster1.getFeedHelper().getStoreInfo();
        List<String> afterSubmitCluster2 = cluster2.getFeedHelper().getStoreInfo();
        List<String> afterSubmitPrism = prism.getFeedHelper().getStoreInfo();

        PrismUtil.compareDataStoreStates(beforeSubmitCluster1, afterSubmitCluster1, 2);
        PrismUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism, 2);
        PrismUtil.compareDataStoreStates(beforeSubmitCluster2, afterSubmitCluster2, 0);

        beforeSubmitCluster1 = cluster1.getProcessHelper().getStoreInfo();
        beforeSubmitCluster2 = cluster2.getProcessHelper().getStoreInfo();
        beforeSubmitPrism = prism.getProcessHelper().getStoreInfo();

        r = prism.getProcessHelper().submitEntity(URLS.SUBMIT_URL,  bundles[0].getProcessData());
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        afterSubmitCluster1 = cluster1.getProcessHelper().getStoreInfo();
        afterSubmitCluster2 = cluster2.getProcessHelper().getStoreInfo();
        afterSubmitPrism = prism.getProcessHelper().getStoreInfo();

        PrismUtil.compareDataStoreStates(beforeSubmitCluster1, afterSubmitCluster1,
                Util.getProcessName( bundles[0].getProcessData()), 1);
        PrismUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism,
                Util.getProcessName( bundles[0].getProcessData()), 1);
        PrismUtil.compareDataStoreStates(beforeSubmitCluster2, afterSubmitCluster2, 0);

    }

    @Test(groups = "distributed")
    public void submitCluster_1prism1coloColoDown() throws Exception {
        restartRequired = true;
        Util.shutDownService(cluster1.getClusterHelper());

        List<String> beforeSubmitCluster2 = cluster2.getClusterHelper().getStoreInfo();
        List<String> beforeSubmitPrism = prism.getClusterHelper().getStoreInfo();

        ServiceResponse r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL,  bundles[0].getClusters().get(0));


        List<String> afterSubmitCluster2 = cluster2.getClusterHelper().getStoreInfo();
        List<String> afterSubmitPrism = prism.getClusterHelper().getStoreInfo();

        Assert.assertTrue(r.getMessage().contains("PARTIAL"));

        PrismUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism,

        Util.readClusterName( bundles[0].getClusters().get(0)), 1);
        PrismUtil.compareDataStoreStates(beforeSubmitCluster2, afterSubmitCluster2,
                Util.readClusterName( bundles[0].getClusters().get(0)), 1);

        Util.startService(cluster1.getClusterHelper());

        Thread.sleep(10000);

        beforeSubmitCluster2 = cluster2.getClusterHelper().getStoreInfo();
        beforeSubmitPrism = prism.getClusterHelper().getStoreInfo();

        r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL,  bundles[0].getClusters().get(0));

        afterSubmitCluster2 = cluster2.getClusterHelper().getStoreInfo();
        afterSubmitPrism = prism.getClusterHelper().getStoreInfo();

        //should be succeeded
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));
        PrismUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism, 0);
        PrismUtil.compareDataStoreStates(beforeSubmitCluster2, afterSubmitCluster2, 0);
    }

    @Test(groups = "distributed")
    public void submitCluster_1prism1coloSubmitDeleted() throws Exception {
        prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL,  bundles[0].getClusters().get(0));
        prism.getClusterHelper().delete(URLS.DELETE_URL,  bundles[0].getClusters().get(0));

        List<String> beforeSubmitCluster1 = cluster1.getClusterHelper().getStoreInfo();
        List<String> beforeSubmitCluster2 = cluster2.getClusterHelper().getStoreInfo();
        List<String> beforeSubmitPrism = prism.getClusterHelper().getStoreInfo();

        ServiceResponse r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL,  bundles[0].getClusters().get(0));

        List<String> afterSubmitCluster1 = cluster1.getClusterHelper().getStoreInfo();
        List<String> afterSubmitCluster2 = cluster2.getClusterHelper().getStoreInfo();
        List<String> afterSubmitPrism = prism.getClusterHelper().getStoreInfo();

        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        PrismUtil.compareDataStoreStates(beforeSubmitCluster1, afterSubmitCluster1,
                Util.readClusterName( bundles[0].getClusters().get(0)), 1);
        PrismUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism,
                Util.readClusterName( bundles[0].getClusters().get(0)), 1);
        PrismUtil.compareDataStoreStates(beforeSubmitCluster2, afterSubmitCluster2,
                Util.readClusterName( bundles[0].getClusters().get(0)), 1);
    }

    @Test(groups = "embedded")
    public void submitProcess_woClusterSubmit() throws Exception {
        ServiceResponse r = prism.getProcessHelper().submitEntity(URLS.SUBMIT_URL,  bundles[0].getProcessData());

        Assert.assertTrue(r.getMessage().contains("FAILED"));
        Assert.assertTrue(r.getMessage().contains("is not registered"));
    }

    @Test(groups = "embedded")
    public void submitProcess_woFeedSubmit() throws Exception {
        ServiceResponse r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL,  bundles[0].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        r = prism.getProcessHelper().submitEntity(URLS.SUBMIT_URL,  bundles[0].getProcessData());
        Assert.assertTrue(r.getMessage().contains("FAILED"));
        Assert.assertTrue(r.getMessage().contains("is not registered"));
    }

    @Test(groups = {"prism", "0.2", "distributed"})
    public void submitCluster_resubmitAlreadyPARTIAL() throws Exception {
        restartRequired = true;
        bundles[1] = new Bundle(bundles[0], cluster2);
        bundles[1].generateUniqueBundle();
        bundles[1].setProcessWorkflow(aggregateWorkflowDir);

        List<String> beforeCluster1 = cluster1.getClusterHelper().getStoreInfo();
        List<String> beforePrism = prism.getClusterHelper().getStoreInfo();
        List<String> beforeCluster2 = cluster2.getClusterHelper().getStoreInfo();

        Util.shutDownService(cluster1.getFeedHelper());

        bundles[1].setCLusterColo(cluster2.getClusterHelper().getColoName());
        logger.info("cluster b2: " + bundles[1].getClusters().get(0));
        ServiceResponse r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundles[1].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("PARTIAL"));

        List<String> parCluster1 = cluster1.getClusterHelper().getStoreInfo();
        List<String> parPrism = prism.getClusterHelper().getStoreInfo();
        List<String> parCluster2 = cluster2.getClusterHelper().getStoreInfo();

        PrismUtil.compareDataStoreStates(parCluster1, beforeCluster1, 0);
        PrismUtil.compareDataStoreStates(beforePrism, parPrism,
          Util.readClusterName(bundles[1].getClusters().get(0)), 1);
        PrismUtil.compareDataStoreStates(beforeCluster2, parCluster2,
                Util.readClusterName(bundles[1].getClusters().get(0)), 1);

        Util.restartService(cluster1.getFeedHelper());

         bundles[0].setCLusterColo(cluster1.getClusterHelper().getColoName());
        logger.info("cluster b1: " +  bundles[0].getClusters().get(0));
        r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL,  bundles[0].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        List<String> afterCluster1 = cluster1.getClusterHelper().getStoreInfo();
        List<String> afterPrism = prism.getClusterHelper().getStoreInfo();
        List<String> afterCluster2 = cluster2.getClusterHelper().getStoreInfo();

        PrismUtil.compareDataStoreStates(parCluster1, afterCluster1,
                Util.readClusterName( bundles[0].getClusters().get(0)), 1);
        PrismUtil.compareDataStoreStates(afterPrism, parPrism, 0);
        PrismUtil.compareDataStoreStates(afterCluster2, parCluster2, 0);
    }

    @Test(groups = "distributed")
    public void submitCluster_polarization() throws Exception {
        restartRequired = true;
        //shutdown one colo and submit
        Util.shutDownService(cluster1.getClusterHelper());
        List<String> beforeSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        List<String> beforeSubmitPrism = cluster2.getClusterHelper().getStoreInfo();
        ServiceResponse r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL,  bundles[0].getClusters().get(0));
        List<String> afterSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        List<String> afterSubmitPrism = cluster2.getClusterHelper().getStoreInfo();
        Assert.assertTrue(r.getMessage().contains("PARTIAL"));
        PrismUtil.compareDataStoreStates(beforeSubmitCluster, afterSubmitCluster, 0);
        PrismUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism,
                Util.readClusterName( bundles[0].getClusters().get(0)), 1);

        //resubmit PARTIAL success
        Util.startService(cluster1.getClusterHelper());
        Thread.sleep(30000);
        beforeSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        beforeSubmitPrism = cluster2.getClusterHelper().getStoreInfo();
        r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL,  bundles[0].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));
        afterSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        afterSubmitPrism = cluster2.getClusterHelper().getStoreInfo();
        PrismUtil.compareDataStoreStates(beforeSubmitCluster, afterSubmitCluster,
                Util.readClusterName( bundles[0].getClusters().get(0)), 1);
        PrismUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism, 0);
    }

    @Test(groups = "distributed")
    public void submitCluster_resubmitDiffContentPARTIAL() throws Exception {
        restartRequired = true;
        Util.shutDownService(cluster1.getClusterHelper());
        Thread.sleep(30000);
        ServiceResponse r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL,  bundles[0].getClusters().get(0));

        Assert.assertTrue(r.getMessage().contains("PARTIAL"));
        Util.startService(cluster1.getClusterHelper());
        Thread.sleep(30000);

        List<String> beforeSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        List<String> beforeSubmitPrism = cluster2.getClusterHelper().getStoreInfo();
         bundles[0].setCLusterWorkingPath( bundles[0].getClusters().get(0), randomHDFSPath);
        r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL,  bundles[0].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        List<String> afterSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        List<String> afterSubmitPrism = cluster2.getClusterHelper().getStoreInfo();
        PrismUtil.compareDataStoreStates(beforeSubmitCluster, afterSubmitCluster,
                Util.readClusterName( bundles[0].getClusters().get(0)), 1);
        PrismUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism, 0);
    }

    @Test
    public void submitCluster_PARTIALDeletedOfPARTIALSubmit() throws Exception {
        restartRequired = true;
        Util.shutDownService(cluster1.getClusterHelper());
        Thread.sleep(30000);
        ServiceResponse r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL,  bundles[0].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("PARTIAL"));

        List<String> beforeSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        List<String> beforeSubmitPrism = cluster2.getClusterHelper().getStoreInfo();
        r = prism.getClusterHelper().delete(URLS.DELETE_URL,  bundles[0].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("PARTIAL"));

        List<String> afterSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        List<String> afterSubmitPrism = cluster2.getClusterHelper().getStoreInfo();
        PrismUtil.compareDataStoreStates(beforeSubmitCluster, afterSubmitCluster, 0);
        PrismUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism,
                Util.readClusterName( bundles[0].getClusters().get(0)), -1);

        r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL,  bundles[0].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("PARTIAL"));
    }

    @Test(groups = "distributed")
    public void submitCluster_submitPartialDeleted() throws Exception {
        restartRequired = true;
        ServiceResponse r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL,  bundles[0].getClusters().get(0));

        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));
        Thread.sleep(30000);

        Util.shutDownService(cluster1.getClusterHelper());
        Thread.sleep(30000);

        List<String> beforeSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        List<String> beforeSubmitPrism = cluster2.getClusterHelper().getStoreInfo();
        r = prism.getClusterHelper().delete(URLS.DELETE_URL,  bundles[0].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("PARTIAL"));
        List<String> afterSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        List<String> afterSubmitPrism = cluster2.getClusterHelper().getStoreInfo();
        PrismUtil.compareDataStoreStates(beforeSubmitCluster, afterSubmitCluster, 0);
        PrismUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism,
                Util.readClusterName( bundles[0].getClusters().get(0)), -1);

        Util.startService(cluster1.getClusterHelper());
        Thread.sleep(30000);

        beforeSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        beforeSubmitPrism = cluster2.getClusterHelper().getStoreInfo();
        r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL,  bundles[0].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));
        afterSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        afterSubmitPrism = cluster2.getClusterHelper().getStoreInfo();
        PrismUtil.compareDataStoreStates(beforeSubmitCluster, afterSubmitCluster, 0);
        PrismUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism,
                Util.readClusterName( bundles[0].getClusters().get(0)), 1);
    }

    @Test(groups = "embedded")
    public void submitCluster_resubmitAlreadySucceeded() throws Exception {
        ServiceResponse r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL,  bundles[0].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        List<String> beforeSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        List<String> beforeSubmitPrism = cluster2.getClusterHelper().getStoreInfo();
        r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL,  bundles[0].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        List<String> afterSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        List<String> afterSubmitPrism = cluster2.getClusterHelper().getStoreInfo();
        PrismUtil.compareDataStoreStates(beforeSubmitCluster, afterSubmitCluster, 0);
        PrismUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism, 0);
    }

    @Test(groups = "distributed")
    public void submitCluster_1prism1coloAllUp() throws Exception {
        List<String> beforeSubmitCluster1 = cluster1.getClusterHelper().getStoreInfo();
        List<String> beforeSubmitCluster2 = cluster2.getClusterHelper().getStoreInfo();
        List<String> beforeSubmitPrism = prism.getClusterHelper().getStoreInfo();

        ServiceResponse r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL,  bundles[0].getClusters().get(0));

        List<String> afterSubmitCluster1 = cluster1.getClusterHelper().getStoreInfo();
        List<String> afterSubmitCluster2 = cluster2.getClusterHelper().getStoreInfo();
        List<String> afterSubmitPrism = prism.getClusterHelper().getStoreInfo();

        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));
        PrismUtil.compareDataStoreStates(beforeSubmitCluster1, afterSubmitCluster1,
                Util.readClusterName( bundles[0].getClusters().get(0)), 1);
        PrismUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism,
                Util.readClusterName( bundles[0].getClusters().get(0)), 1);
        PrismUtil.compareDataStoreStates(beforeSubmitCluster2, afterSubmitCluster2,
                Util.readClusterName( bundles[0].getClusters().get(0)), 1);
    }

    @Test(groups = "embedded")
    public void submitCluster_1prism1coloAlreadySubmitted() throws Exception {
        prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL,  bundles[0].getClusters().get(0));

        List<String> beforeSubmitCluster1 = cluster1.getClusterHelper().getStoreInfo();
        List<String> beforeSubmitCluster2 = cluster2.getClusterHelper().getStoreInfo();
        List<String> beforeSubmitPrism = prism.getClusterHelper().getStoreInfo();

        ServiceResponse r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL,  bundles[0].getClusters().get(0));

        List<String> afterSubmitCluster1 = cluster1.getClusterHelper().getStoreInfo();
        List<String> afterSubmitCluster2 = cluster2.getClusterHelper().getStoreInfo();
        List<String> afterSubmitPrism = prism.getClusterHelper().getStoreInfo();

        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));
        PrismUtil.compareDataStoreStates(beforeSubmitCluster1, afterSubmitCluster1, 0);
        PrismUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism, 0);
        PrismUtil.compareDataStoreStates(beforeSubmitCluster2, afterSubmitCluster2, 0);
    }

    @Test
    public void submitProcess_1ColoDownAfter1FeedSubmitStartAfter2feed() throws Exception {
        restartRequired = true;
        ServiceResponse r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL,  bundles[0].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"), r.getMessage());

        r = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL,  bundles[0].getDataSets().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"), r.getMessage());

        Util.shutDownService(cluster1.getClusterHelper());
        Thread.sleep(30000);

        List<String> beforeSubmitCluster1 = cluster1.getFeedHelper().getStoreInfo();
        List<String> beforeSubmitCluster2 = cluster2.getFeedHelper().getStoreInfo();
        List<String> beforeSubmitPrism = prism.getFeedHelper().getStoreInfo();

        r = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL,  bundles[0].getDataSets().get(1));
        Assert.assertTrue(r.getMessage().contains("FAILED"));

        List<String> afterSubmitCluster1 = cluster1.getFeedHelper().getStoreInfo();
        List<String> afterSubmitCluster2 = cluster2.getFeedHelper().getStoreInfo();
        List<String> afterSubmitPrism = prism.getFeedHelper().getStoreInfo();

        PrismUtil.compareDataStoreStates(beforeSubmitCluster1, afterSubmitCluster1, 0);
        PrismUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism,
                Util.readDatasetName( bundles[0].getDataSets().get(1)), 1);
        PrismUtil.compareDataStoreStates(beforeSubmitCluster2, afterSubmitCluster2, 0);

        Util.startService(cluster1.getClusterHelper());
        Thread.sleep(15000);

        beforeSubmitCluster1 = cluster1.getProcessHelper().getStoreInfo();
        beforeSubmitCluster2 = cluster2.getProcessHelper().getStoreInfo();
        beforeSubmitPrism = prism.getProcessHelper().getStoreInfo();

        r = prism.getProcessHelper().submitEntity(URLS.SUBMIT_URL,  bundles[0].getProcessData());
        Assert.assertTrue(r.getMessage().contains("FAILED"), r.getMessage());

        afterSubmitCluster1 = cluster1.getProcessHelper().getStoreInfo();
        afterSubmitCluster2 = cluster2.getProcessHelper().getStoreInfo();
        afterSubmitPrism = prism.getProcessHelper().getStoreInfo();

        PrismUtil.compareDataStoreStates(beforeSubmitCluster1, afterSubmitCluster1, 0);
        PrismUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism,
                Util.getProcessName( bundles[0].getProcessData()), 1);
        PrismUtil.compareDataStoreStates(beforeSubmitCluster2, afterSubmitCluster2, 0);
    }

  @DataProvider(name = "errorDP")
  public Object[][] getTestData(Method m) throws Exception {
    Object[][] testData = new Object[2][1];
    testData[0][0] = "EmptyInputTagProcess";
    testData[1][0] = "EmptyOutputTagProcess";

    return testData;
  }

}
