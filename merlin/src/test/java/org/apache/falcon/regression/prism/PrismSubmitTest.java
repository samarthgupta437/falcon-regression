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
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.PrismUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.ConnectException;
import java.util.List;

public class PrismSubmitTest extends BaseTestClass {

    ColoHelper cluster1;
    ColoHelper cluster2;
    private Bundle bundle;

    public PrismSubmitTest(){
        super();
        cluster1 = servers.get(0);
        cluster2 = servers.get(1);
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        Util.print("test name: " + method.getName());
        bundle = Util.readELBundles()[0][0];
        bundle = new Bundle(bundle, cluster1.getEnvFileName(), cluster1.getPrefix());
        bundle.generateUniqueBundle();
    }


    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        Util.startService(prism.getFeedHelper());
        Util.startService(cluster1.getFeedHelper());

        bundle.deleteBundle(prism);
    }

    @Test
    public void submitCluster_1prism1coloPrismdown() throws Exception {

        Util.shutDownService(prism.getClusterHelper());

        List<String> beforeSubmit = cluster1.getClusterHelper().getStoreInfo();
        try {
            prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle.getClusters().get(0));
        } catch (ConnectException e) {
            Assert.assertTrue(e.getMessage().contains("Connection to "
                    + prism.getClusterHelper().getHostname() +" refused"), e.getMessage());
        }
        List<String> afterSubmit = cluster1.getClusterHelper().getStoreInfo();

        PrismUtil.compareDataStoreStates(beforeSubmit, afterSubmit, 0);

    }

    @Test
    public void submitCluster_resubmitDiffContent() throws Exception {
        ServiceResponse r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle.getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));
        List<String> beforeSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        List<String> beforeSubmitPrism = cluster2.getClusterHelper().getStoreInfo();

        bundle.setCLusterWorkingPath(bundle.getClusters().get(0), "/projects/ivory/someRandomPath");
        Util.print("modified cluster Data: " + bundle.getClusters().get(0));
        r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle.getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        List<String> afterSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        List<String> afterSubmitPrism = cluster2.getClusterHelper().getStoreInfo();
        PrismUtil.compareDataStoreStates(beforeSubmitCluster, afterSubmitCluster, 0);
        PrismUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism, 0);
    }

    @Test
    public void submitCluster_resubmitAlreadyPARTIALWithAllUp() throws Exception {
            Util.shutDownService(cluster1.getClusterHelper());
            Thread.sleep(30000);

            ServiceResponse r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle.getClusters().get(0));

            Assert.assertTrue(r.getMessage().contains("PARTIAL"));

            Util.startService(cluster1.getClusterHelper());
            Thread.sleep(30000);

            r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle.getClusters().get(0));
            Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));
    }

    @Test
    public void submitProcess_1ColoDownAfter2FeedSubmitStartAfterProcessSubmitAnsDeleteProcess() throws Exception {

        ServiceResponse r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle.getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        r = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, bundle.getDataSets().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        r = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, bundle.getDataSets().get(1));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        Util.shutDownService(cluster1.getClusterHelper());
        Thread.sleep(12000);


        List<String> beforeSubmitCluster1 = cluster1.getProcessHelper().getStoreInfo();
        List<String> beforeSubmitCluster2 = cluster2.getProcessHelper().getStoreInfo();
        List<String> beforeSubmitPrism = prism.getProcessHelper().getStoreInfo();

        r = prism.getProcessHelper().delete(URLS.DELETE_URL, bundle.getProcessData());

        r = prism.getProcessHelper().submitEntity(URLS.SUBMIT_URL, bundle.getProcessData());
        Util.assertFailed(r);
        List<String> afterSubmitCluster1 = cluster1.getProcessHelper().getStoreInfo();
        List<String> afterSubmitCluster2 = cluster2.getProcessHelper().getStoreInfo();
        List<String> afterSubmitPrism = prism.getProcessHelper().getStoreInfo();

        PrismUtil.compareDataStoreStates(beforeSubmitCluster1, afterSubmitCluster1, 0);
        PrismUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism,
                Util.getProcessName(bundle.getProcessData()), 1);
        PrismUtil.compareDataStoreStates(beforeSubmitCluster2, afterSubmitCluster2, 0);

        Util.startService(cluster1.getClusterHelper());
        Thread.sleep(15000);

        beforeSubmitCluster1 = cluster1.getProcessHelper().getStoreInfo();
        beforeSubmitCluster2 = cluster2.getProcessHelper().getStoreInfo();
        beforeSubmitPrism = prism.getProcessHelper().getStoreInfo();

        r = prism.getProcessHelper().delete(URLS.DELETE_URL, bundle.getProcessData());
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        afterSubmitCluster1 = cluster1.getProcessHelper().getStoreInfo();
        afterSubmitCluster2 = cluster2.getProcessHelper().getStoreInfo();
        afterSubmitPrism = prism.getProcessHelper().getStoreInfo();

        PrismUtil.compareDataStoreStates(beforeSubmitCluster1, afterSubmitCluster1, 0);
        PrismUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism,
                Util.getProcessName(bundle.getProcessData()), -1);
        PrismUtil.compareDataStoreStates(beforeSubmitCluster2, afterSubmitCluster2, 0);
    }

    @Test
    public void submitProcess_ideal() throws Exception {

        ServiceResponse r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle.getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        List<String> beforeSubmitCluster1 = cluster1.getFeedHelper().getStoreInfo();
        List<String> beforeSubmitCluster2 = cluster2.getFeedHelper().getStoreInfo();
        List<String> beforeSubmitPrism = prism.getFeedHelper().getStoreInfo();

        r = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, bundle.getDataSets().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        r = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, bundle.getDataSets().get(1));
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

        r = prism.getProcessHelper().submitEntity(URLS.SUBMIT_URL, bundle.getProcessData());
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        afterSubmitCluster1 = cluster1.getProcessHelper().getStoreInfo();
        afterSubmitCluster2 = cluster2.getProcessHelper().getStoreInfo();
        afterSubmitPrism = prism.getProcessHelper().getStoreInfo();

        PrismUtil.compareDataStoreStates(beforeSubmitCluster1, afterSubmitCluster1,
                Util.getProcessName(bundle.getProcessData()), 1);
        PrismUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism,
                Util.getProcessName(bundle.getProcessData()), 1);
        PrismUtil.compareDataStoreStates(beforeSubmitCluster2, afterSubmitCluster2, 0);

    }

    @Test
    public void submitCluster_1prism1coloColoDown() throws Exception {
        Util.shutDownService(cluster1.getClusterHelper());

        List<String> beforeSubmitCluster2 = cluster2.getClusterHelper().getStoreInfo();
        List<String> beforeSubmitPrism = prism.getClusterHelper().getStoreInfo();

        ServiceResponse r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle.getClusters().get(0));


        List<String> afterSubmitCluster2 = cluster2.getClusterHelper().getStoreInfo();
        List<String> afterSubmitPrism = prism.getClusterHelper().getStoreInfo();

        Assert.assertTrue(r.getMessage().contains("PARTIAL"));

        PrismUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism,
          Util.readClusterName(bundle.getClusters().get(0)), 1);
        PrismUtil.compareDataStoreStates(beforeSubmitCluster2, afterSubmitCluster2,
                Util.readClusterName(bundle.getClusters().get(0)), 1);

        Util.startService(cluster1.getClusterHelper());

        Thread.sleep(10000);

        beforeSubmitCluster2 = cluster2.getClusterHelper().getStoreInfo();
        beforeSubmitPrism = prism.getClusterHelper().getStoreInfo();

        r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle.getClusters().get(0));

        afterSubmitCluster2 = cluster2.getClusterHelper().getStoreInfo();
        afterSubmitPrism = prism.getClusterHelper().getStoreInfo();

        //should be succeeded
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));
        PrismUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism, 0);
        PrismUtil.compareDataStoreStates(beforeSubmitCluster2, afterSubmitCluster2, 0);
    }

    @Test
    public void submitCluster_1prism1coloSubmitDeleted() throws Exception {
        prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle.getClusters().get(0));
        prism.getClusterHelper().delete(URLS.DELETE_URL, bundle.getClusters().get(0));

        List<String> beforeSubmitCluster1 = cluster1.getClusterHelper().getStoreInfo();
        List<String> beforeSubmitCluster2 = cluster2.getClusterHelper().getStoreInfo();
        List<String> beforeSubmitPrism = prism.getClusterHelper().getStoreInfo();

        ServiceResponse r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle.getClusters().get(0));

        List<String> afterSubmitCluster1 = cluster1.getClusterHelper().getStoreInfo();
        List<String> afterSubmitCluster2 = cluster2.getClusterHelper().getStoreInfo();
        List<String> afterSubmitPrism = prism.getClusterHelper().getStoreInfo();

        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        PrismUtil.compareDataStoreStates(beforeSubmitCluster1, afterSubmitCluster1,
                Util.readClusterName(bundle.getClusters().get(0)), 1);
        PrismUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism,
                Util.readClusterName(bundle.getClusters().get(0)), 1);
        PrismUtil.compareDataStoreStates(beforeSubmitCluster2, afterSubmitCluster2,
                Util.readClusterName(bundle.getClusters().get(0)), 1);
    }

    @Test
    public void submitProcess_woClusterSubmit() throws Exception {
        ServiceResponse r = prism.getProcessHelper().submitEntity(URLS.SUBMIT_URL, bundle.getProcessData());

        Assert.assertTrue(r.getMessage().contains("FAILED"));
        Assert.assertTrue(r.getMessage().contains("is not registered"));
    }

    @Test
    public void submitProcess_woFeedSubmit() throws Exception {
        ServiceResponse r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle.getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        r = prism.getProcessHelper().submitEntity(URLS.SUBMIT_URL, bundle.getProcessData());
        Assert.assertTrue(r.getMessage().contains("FAILED"));
        Assert.assertTrue(r.getMessage().contains("is not registered"));
    }

    @Test(groups = {"prism", "0.2"})
    public void submitCluster_resubmitAlreadyPARTIAL() throws Exception {
        Bundle bundle2 = new Bundle(bundle, cluster2.getEnvFileName(), cluster2.getPrefix());
        bundle2.generateUniqueBundle();

        List<String> beforeCluster1 = cluster1.getClusterHelper().getStoreInfo();
        List<String> beforePrism = prism.getClusterHelper().getStoreInfo();
        List<String> beforeCluster2 = cluster2.getClusterHelper().getStoreInfo();

        Util.shutDownService(cluster1.getFeedHelper());

        bundle2.setCLusterColo(cluster2.getClusterHelper().getColo().split("=")[1]);
        Util.print("cluster b2: " + bundle2.getClusters().get(0));
        ServiceResponse r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle2.getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("PARTIAL"));

        List<String> parCluster1 = cluster1.getClusterHelper().getStoreInfo();
        List<String> parPrism = prism.getClusterHelper().getStoreInfo();
        List<String> parCluster2 = cluster2.getClusterHelper().getStoreInfo();

        PrismUtil.compareDataStoreStates(parCluster1, beforeCluster1, 0);
        PrismUtil.compareDataStoreStates(beforePrism, parPrism,
          Util.readClusterName(bundle2.getClusters().get(0)), 1);
        PrismUtil.compareDataStoreStates(beforeCluster2, parCluster2,
                Util.readClusterName(bundle2.getClusters().get(0)), 1);

        Util.restartService(cluster1.getFeedHelper());

        bundle.setCLusterColo(cluster1.getClusterHelper().getColo().split("=")[1]);
        Util.print("cluster b1: " + bundle.getClusters().get(0));
        r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle.getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        List<String> afterCluster1 = cluster1.getClusterHelper().getStoreInfo();
        List<String> afterPrism = prism.getClusterHelper().getStoreInfo();
        List<String> afterCluster2 = cluster2.getClusterHelper().getStoreInfo();

        PrismUtil.compareDataStoreStates(parCluster1, afterCluster1,
                Util.readClusterName(bundle.getClusters().get(0)), 1);
        PrismUtil.compareDataStoreStates(afterPrism, parPrism, 0);
        PrismUtil.compareDataStoreStates(afterCluster2, parCluster2, 0);
        bundle2.deleteBundle(prism);
    }

    @Test
    public void submitCluster_polarization() throws Exception {

        //shutdown one colo and submit
        Util.shutDownService(cluster1.getClusterHelper());
        List<String> beforeSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        List<String> beforeSubmitPrism = cluster2.getClusterHelper().getStoreInfo();
        ServiceResponse r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle.getClusters().get(0));
        List<String> afterSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        List<String> afterSubmitPrism = cluster2.getClusterHelper().getStoreInfo();
        Assert.assertTrue(r.getMessage().contains("PARTIAL"));
        PrismUtil.compareDataStoreStates(beforeSubmitCluster, afterSubmitCluster, 0);
        PrismUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism,
                Util.readClusterName(bundle.getClusters().get(0)), 1);

        //resubmit PARTIAL success
        Util.startService(cluster1.getClusterHelper());
        Thread.sleep(30000);
        beforeSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        beforeSubmitPrism = cluster2.getClusterHelper().getStoreInfo();
        r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle.getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));
        afterSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        afterSubmitPrism = cluster2.getClusterHelper().getStoreInfo();
        PrismUtil.compareDataStoreStates(beforeSubmitCluster, afterSubmitCluster,
                Util.readClusterName(bundle.getClusters().get(0)), 1);
        PrismUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism, 0);
    }

    @Test
    public void submitCluster_resubmitDiffContentPARTIAL() throws Exception {
        Util.shutDownService(cluster1.getClusterHelper());
        Thread.sleep(30000);
        ServiceResponse r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle.getClusters().get(0));

        Assert.assertTrue(r.getMessage().contains("PARTIAL"));
        Util.startService(cluster1.getClusterHelper());
        Thread.sleep(30000);

        List<String> beforeSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        List<String> beforeSubmitPrism = cluster2.getClusterHelper().getStoreInfo();
        bundle.setCLusterWorkingPath(bundle.getClusters().get(0), "/projects/ivory/someRandomPath");
        r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle.getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        List<String> afterSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        List<String> afterSubmitPrism = cluster2.getClusterHelper().getStoreInfo();
        PrismUtil.compareDataStoreStates(beforeSubmitCluster, afterSubmitCluster,
                Util.readClusterName(bundle.getClusters().get(0)), 1);
        PrismUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism, 0);
    }

    @Test
    public void submitCluster_PARTIALDeletedOfPARTIALSubmit() throws Exception {
        Util.shutDownService(cluster1.getClusterHelper());
        Thread.sleep(30000);
        ServiceResponse r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle.getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("PARTIAL"));

        List<String> beforeSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        List<String> beforeSubmitPrism = cluster2.getClusterHelper().getStoreInfo();
        r = prism.getClusterHelper().delete(URLS.DELETE_URL, bundle.getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("PARTIAL"));

        List<String> afterSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        List<String> afterSubmitPrism = cluster2.getClusterHelper().getStoreInfo();
        PrismUtil.compareDataStoreStates(beforeSubmitCluster, afterSubmitCluster, 0);
        PrismUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism,
                Util.readClusterName(bundle.getClusters().get(0)), -1);

        r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle.getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("PARTIAL"));
    }

    @Test
    public void submitCluster_submitPartialDeleted() throws Exception {
        ServiceResponse r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle.getClusters().get(0));

        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));
        Thread.sleep(30000);

        Util.shutDownService(cluster1.getClusterHelper());
        Thread.sleep(30000);

        List<String> beforeSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        List<String> beforeSubmitPrism = cluster2.getClusterHelper().getStoreInfo();
        r = prism.getClusterHelper().delete(URLS.DELETE_URL, bundle.getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("PARTIAL"));
        List<String> afterSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        List<String> afterSubmitPrism = cluster2.getClusterHelper().getStoreInfo();
        PrismUtil.compareDataStoreStates(beforeSubmitCluster, afterSubmitCluster, 0);
        PrismUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism,
                Util.readClusterName(bundle.getClusters().get(0)), -1);

        Util.startService(cluster1.getClusterHelper());
        Thread.sleep(30000);

        beforeSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        beforeSubmitPrism = cluster2.getClusterHelper().getStoreInfo();
        r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle.getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));
        afterSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        afterSubmitPrism = cluster2.getClusterHelper().getStoreInfo();
        PrismUtil.compareDataStoreStates(beforeSubmitCluster, afterSubmitCluster, 0);
        PrismUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism,
                Util.readClusterName(bundle.getClusters().get(0)), 1);
    }

    @Test
    public void submitCluster_resubmitAlreadySucceeded() throws Exception {
        ServiceResponse r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle.getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        List<String> beforeSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        List<String> beforeSubmitPrism = cluster2.getClusterHelper().getStoreInfo();
        r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle.getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        List<String> afterSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        List<String> afterSubmitPrism = cluster2.getClusterHelper().getStoreInfo();
        PrismUtil.compareDataStoreStates(beforeSubmitCluster, afterSubmitCluster, 0);
        PrismUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism, 0);
    }

    @Test
    public void submitCluster_1prism1coloAllUp() throws Exception {
        List<String> beforeSubmitCluster1 = cluster1.getClusterHelper().getStoreInfo();
        List<String> beforeSubmitCluster2 = cluster2.getClusterHelper().getStoreInfo();
        List<String> beforeSubmitPrism = prism.getClusterHelper().getStoreInfo();

        ServiceResponse r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle.getClusters().get(0));

        List<String> afterSubmitCluster1 = cluster1.getClusterHelper().getStoreInfo();
        List<String> afterSubmitCluster2 = cluster2.getClusterHelper().getStoreInfo();
        List<String> afterSubmitPrism = prism.getClusterHelper().getStoreInfo();

        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));
        PrismUtil.compareDataStoreStates(beforeSubmitCluster1, afterSubmitCluster1,
                Util.readClusterName(bundle.getClusters().get(0)), 1);
        PrismUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism,
                Util.readClusterName(bundle.getClusters().get(0)), 1);
        PrismUtil.compareDataStoreStates(beforeSubmitCluster2, afterSubmitCluster2,
                Util.readClusterName(bundle.getClusters().get(0)), 1);
    }

    @Test
    public void submitCluster_1prism1coloAlreadySubmitted() throws Exception {
        prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle.getClusters().get(0));

        List<String> beforeSubmitCluster1 = cluster1.getClusterHelper().getStoreInfo();
        List<String> beforeSubmitCluster2 = cluster2.getClusterHelper().getStoreInfo();
        List<String> beforeSubmitPrism = prism.getClusterHelper().getStoreInfo();

        ServiceResponse r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle.getClusters().get(0));

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
        ServiceResponse r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle.getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"), r.getMessage());

        r = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, bundle.getDataSets().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"), r.getMessage());

        Util.shutDownService(cluster1.getClusterHelper());
        Thread.sleep(30000);

        List<String> beforeSubmitCluster1 = cluster1.getFeedHelper().getStoreInfo();
        List<String> beforeSubmitCluster2 = cluster2.getFeedHelper().getStoreInfo();
        List<String> beforeSubmitPrism = prism.getFeedHelper().getStoreInfo();

        r = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, bundle.getDataSets().get(1));
        Assert.assertTrue(r.getMessage().contains("FAILED"));

        List<String> afterSubmitCluster1 = cluster1.getFeedHelper().getStoreInfo();
        List<String> afterSubmitCluster2 = cluster2.getFeedHelper().getStoreInfo();
        List<String> afterSubmitPrism = prism.getFeedHelper().getStoreInfo();

        PrismUtil.compareDataStoreStates(beforeSubmitCluster1, afterSubmitCluster1, 0);
        PrismUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism,
                Util.readDatasetName(bundle.getDataSets().get(1)), 1);
        PrismUtil.compareDataStoreStates(beforeSubmitCluster2, afterSubmitCluster2, 0);

        Util.startService(cluster1.getClusterHelper());
        Thread.sleep(15000);

        beforeSubmitCluster1 = cluster1.getProcessHelper().getStoreInfo();
        beforeSubmitCluster2 = cluster2.getProcessHelper().getStoreInfo();
        beforeSubmitPrism = prism.getProcessHelper().getStoreInfo();

        r = prism.getProcessHelper().submitEntity(URLS.SUBMIT_URL, bundle.getProcessData());
        Assert.assertTrue(r.getMessage().contains("FAILED"), r.getMessage());

        afterSubmitCluster1 = cluster1.getProcessHelper().getStoreInfo();
        afterSubmitCluster2 = cluster2.getProcessHelper().getStoreInfo();
        afterSubmitPrism = prism.getProcessHelper().getStoreInfo();

        PrismUtil.compareDataStoreStates(beforeSubmitCluster1, afterSubmitCluster1, 0);
        PrismUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism,
                Util.getProcessName(bundle.getProcessData()), 1);
        PrismUtil.compareDataStoreStates(beforeSubmitCluster2, afterSubmitCluster2, 0);
    }

   @Test(dataProvider = "errorDP")
  public void submitProcess_emptyTags(String resourceFolder) throws
     JAXBException, IOException {
    bundle = Util.readBundles(resourceFolder)[0][0];
    bundle = new Bundle(bundle, cluster1.getEnvFileName(),
      cluster1.getPrefix());
    bundle.generateUniqueBundle();
     AssertUtil.assertFailed(bundle.submitProcess(false),
       "Process Should not have been submitted");
  }

  @DataProvider(name = "errorDP")
  public Object[][] getTestData(Method m) throws Exception {
    Object[][] testData = new Object[2][1];
    testData[0][0] = "EmptyInputTagProcess";
    testData[1][0] = "EmptyOutputTagProcess";

    return testData;
  }

}
