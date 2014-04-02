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
import org.apache.falcon.regression.core.generated.feed.ActionType;
import org.apache.falcon.regression.core.generated.feed.ClusterType;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.APIResult;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.enumsAndConstants.ENTITY_TYPE;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.core.util.XmlUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

@Test(groups = "distributed")
public class PrismFeedSnSTest extends BaseTestClass {

    ColoHelper cluster1 = servers.get(0);
    ColoHelper cluster2 = servers.get(1);
    OozieClient cluster1OC = serverOC.get(0);
    OozieClient cluster2OC = serverOC.get(1);
    private boolean restartRequired;
    String aggregateWorkflowDir = baseHDFSDir + "/PrismFeedSnSTest/aggregator";

    @BeforeClass
    public void uploadWorkflow() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        Util.print("test name: " + method.getName());
        restartRequired = false;
        Bundle bundle = Util.readELBundles()[0][0];
        for (int i = 0; i < 2; i++) {
            bundles[i] = new Bundle(bundle, servers.get(i));
            bundles[i].generateUniqueBundle();
            bundles[i].setProcessWorkflow(aggregateWorkflowDir);
        }
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        if (restartRequired) {
            Util.restartService(cluster1.getFeedHelper());
        }
        removeBundles();
    }


    @Test(groups = {"prism", "0.2"})
    public void testFeedSnSOnBothColos() throws Exception {
        //schedule both bundles
        bundles[0].submitAndScheduleFeed();
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.FEED, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkNotStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);
        bundles[1].submitAndScheduleFeed();

        //now check if they have been scheduled correctly or not
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.FEED, bundles[1], Job.Status.RUNNING);

        //check if there is no criss cross
        AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.RUNNING);
    }

    @Test(groups = {"prism", "0.2"})
    public void testSnSAlreadyScheduledFeedOnBothColos() throws Exception {
        //schedule both bundles
        bundles[0].submitAndScheduleFeed();
        bundles[1].submitAndScheduleFeed();

        //now check if they have been scheduled correctly or not
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.FEED, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.FEED, bundles[1], Job.Status.RUNNING);

        //check if there is no criss cross
        AssertUtil.checkNotStatus(cluster1OC, ENTITY_TYPE.FEED, bundles[1], Job.Status.RUNNING);
        AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.FEED, bundles[0], Job.Status.RUNNING);


        Util.assertSucceeded(prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[0].getDataSets().get(0)));
        //ensure only one bundle is there
        Assert.assertEquals(Util.getBundles(cluster1OC,
                Util.readDatasetName(bundles[0].getDataSets().get(0)), ENTITY_TYPE.FEED).size(), 1);
        Util.assertSucceeded(prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[1].getDataSets().get(0)));
        Assert.assertEquals(Util.getBundles(cluster2OC,
                Util.readDatasetName(bundles[1].getDataSets().get(0)), ENTITY_TYPE.FEED).size(), 1);
        //now check if they have been scheduled correctly or not
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.FEED, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.FEED, bundles[1], Job.Status.RUNNING);
    }


    @Test(groups = {"prism", "0.2"})
    public void testSnSSuspendedFeedOnBothColos() throws Exception {
        //schedule both bundles
        bundles[0].submitAndScheduleFeed();
        bundles[1].submitAndScheduleFeed();

        Util.assertSucceeded(prism.getFeedHelper()
                .suspend(URLS.SUSPEND_URL, bundles[0].getDataSets().get(0)));
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.FEED, bundles[0], Job.Status.SUSPENDED);
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.FEED, bundles[1], Job.Status.RUNNING);
        //now check if they have been scheduled correctly or not
        Util.assertSucceeded(prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[0].getDataSets().get(0)));
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.FEED, bundles[0], Job.Status.SUSPENDED);
        Assert.assertEquals(Util.getBundles(cluster1OC,
                Util.readDatasetName(bundles[0].getDataSets().get(0)), ENTITY_TYPE.FEED).size(), 1);

        Util.assertSucceeded(cluster1.getFeedHelper()
                .resume(URLS.RESUME_URL, bundles[0].getDataSets().get(0)));
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.FEED, bundles[0], Job.Status.RUNNING);

        Util.assertSucceeded(prism.getFeedHelper()
                .suspend(URLS.SUSPEND_URL, bundles[1].getDataSets().get(0)));
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.FEED, bundles[1], Job.Status.SUSPENDED);
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.FEED, bundles[0], Job.Status.RUNNING);

        Util.assertSucceeded(prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[1].getDataSets().get(0)));
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.FEED, bundles[1], Job.Status.SUSPENDED);
        Assert.assertEquals(Util.getBundles(cluster2OC,
                Util.readDatasetName(bundles[1].getDataSets().get(0)), ENTITY_TYPE.FEED).size(), 1);
        Util.assertSucceeded(cluster2.getFeedHelper()
                .resume(URLS.RESUME_URL, bundles[1].getDataSets().get(0)));
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.FEED, bundles[1], Job.Status.RUNNING);


    }

    @Test(groups = {"prism", "0.2"})
    public void testSnSDeletedFeedOnBothColos() throws Exception {
        //schedule both bundles
        bundles[0].submitAndScheduleFeed();
        bundles[1].submitAndScheduleFeed();

        Util.assertSucceeded(prism.getFeedHelper().delete(URLS.DELETE_URL, bundles[0].getDataSets().get(0)));
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.FEED, bundles[0], Job.Status.KILLED);
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.FEED, bundles[1], Job.Status.RUNNING);

        Util.assertSucceeded(prism.getFeedHelper().delete(URLS.DELETE_URL, bundles[1].getDataSets().get(0)));
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.FEED, bundles[1], Job.Status.KILLED);
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.FEED, bundles[0], Job.Status.KILLED);

        Util.assertSucceeded(prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[0].getDataSets().get(0)));
        Util.assertSucceeded(prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[1].getDataSets().get(0)));
    }

    @Test(groups = {"prism", "0.2"})
    public void testScheduleNonExistentFeedOnBothColos() throws Exception {
        Util.assertFailed(prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[0].getDataSets().get(0)));
        Util.assertFailed(prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[1].getDataSets().get(0)));
    }

    @Test(groups = {"prism", "0.2"})
    public void testFeedSnSOn1ColoWhileOtherColoIsDown() throws Exception {
        restartRequired = true;
        for (String cluster : bundles[1].getClusters()) {
            Util.assertSucceeded(prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, cluster));
        }

        Util.shutDownService(cluster1.getFeedHelper());

        Util.assertSucceeded(prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[1].getDataSets().get(0)));

        //now check if they have been scheduled correctly or not
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.FEED, bundles[1], Job.Status.RUNNING);
        //check if there is no criss cross
        AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.RUNNING);
    }


    @Test(groups = {"prism", "0.2"})
    public void testFeedSnSOn1ColoWhileThatColoIsDown() throws Exception {
        restartRequired = true;
        bundles[0].submitFeed();

        Util.shutDownService(cluster1.getFeedHelper());

        Util.assertFailed(prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[0].getDataSets().get(0)));
        AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.RUNNING);
    }

    @Test(groups = {"prism", "0.2"})
    public void testFeedSnSOn1ColoWhileAnotherColoHasSuspendedFeed() throws Exception {
        bundles[0].submitAndScheduleFeed();
        Util.assertSucceeded(prism.getFeedHelper()
                .suspend(URLS.SUSPEND_URL, bundles[0].getDataSets().get(0)));
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.FEED, bundles[0], Job.Status.SUSPENDED);
        bundles[1].submitAndScheduleFeed();
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.FEED, bundles[1], Job.Status.RUNNING);
        AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.FEED, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.FEED, bundles[0], Job.Status.SUSPENDED);
        AssertUtil.checkNotStatus(cluster1OC, ENTITY_TYPE.FEED, bundles[1], Job.Status.RUNNING);
    }

    @Test(groups = {"prism", "0.2"})
    public void testFeedSnSOn1ColoWhileAnotherColoHasKilledFeed() throws Exception {
        bundles[0].submitAndScheduleFeed();
        Util.assertSucceeded(prism.getFeedHelper().delete(URLS.DELETE_URL, bundles[0].getDataSets().get(0)));
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.FEED, bundles[0], Job.Status.KILLED);
        bundles[1].submitAndScheduleFeed();
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.FEED, bundles[1], Job.Status.RUNNING);
        AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.FEED, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.FEED, bundles[0], Job.Status.KILLED);
        AssertUtil.checkNotStatus(cluster1OC, ENTITY_TYPE.FEED, bundles[1], Job.Status.RUNNING);
    }

    @Test(groups = {"prism", "0.2"})
    public void testFeedSnSOnBothColosUsingColoHelper() throws Exception {
        //schedule both bundles
        bundles[0].submitFeed();
        APIResult result = Util.parseResponse((cluster1.getFeedHelper()
                .submitEntity(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[0].getDataSets().get(0))));
        Assert.assertEquals(result.getStatusCode(), 404);
        AssertUtil.checkNotStatus(cluster1OC, ENTITY_TYPE.FEED, bundles[0], Job.Status.RUNNING);
        bundles[1].submitFeed();
        result = Util.parseResponse(cluster2.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[1].getDataSets().get(0)));
        Assert.assertEquals(result.getStatusCode(), 404);

        AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.FEED, bundles[1], Job.Status.RUNNING);
    }


    @Test(groups = {"prism", "0.2"})
    public void testSnSSuspendedFeedOnBothColosUsingColoHelper() throws Exception {

        //schedule both bundles
        bundles[0].submitFeed();
        Util.assertSucceeded(prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[0].getDataSets().get(0)));
        bundles[1].submitFeed();
        Util.assertSucceeded(prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[1].getDataSets().get(0)));

        Util.assertSucceeded(cluster1.getFeedHelper()
                .suspend(URLS.SUSPEND_URL, bundles[0].getDataSets().get(0)));
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.FEED, bundles[0], Job.Status.SUSPENDED);
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.FEED, bundles[1], Job.Status.RUNNING);
        //now check if they have been scheduled correctly or not
        Util.assertSucceeded(prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[0].getDataSets().get(0)));
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.FEED, bundles[0], Job.Status.SUSPENDED);
        Util.assertSucceeded(cluster1.getFeedHelper().resume(URLS.RESUME_URL, bundles[0].getDataSets().get(0)));

        Util.assertSucceeded(cluster2.getFeedHelper().suspend(URLS.SUSPEND_URL,
                bundles[1].getDataSets().get(0)));
        Util.assertSucceeded(prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[0].getDataSets().get(0)));
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.FEED, bundles[1], Job.Status.SUSPENDED);
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.FEED, bundles[0], Job.Status.RUNNING);
    }


    @Test(groups = {"prism", "0.2"})
    public void testScheduleDeletedFeedOnBothColosUsingColoHelper() throws Exception {

        //schedule both bundles
        bundles[0].submitAndScheduleFeed();
        bundles[1].submitAndScheduleFeed();

        Util.assertSucceeded(prism.getFeedHelper().delete(URLS.DELETE_URL, bundles[0].getDataSets().get(0)));
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.FEED, bundles[0], Job.Status.KILLED);
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.FEED, bundles[1], Job.Status.RUNNING);

        Util.assertSucceeded(prism.getFeedHelper().delete(URLS.DELETE_URL, bundles[1].getDataSets().get(0)));
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.FEED, bundles[1], Job.Status.KILLED);
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.FEED, bundles[0], Job.Status.KILLED);
        Util.assertSucceeded(prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[0].getDataSets().get(0)));
        Util.assertSucceeded(prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[1].getDataSets().get(0)));

        Assert.assertEquals(Util.parseResponse(prism.getFeedHelper()
                        .getStatus(URLS.STATUS_URL, bundles[0].getDataSets().get(0))).getMessage(),
                cluster1.getClusterHelper().getColoName() + "/RUNNING");

        Assert.assertEquals(Util.parseResponse(prism.getFeedHelper()
                        .getStatus(URLS.STATUS_URL, bundles[1].getDataSets().get(0))).getMessage(),
                cluster2.getClusterHelper().getColoName() + "/RUNNING");
    }


    @Test(groups = {"prism", "0.2"})
    public void testSNSNonExistentFeedOnBothColosUsingColoHelper() throws Exception {

        Assert.assertEquals(Util.parseResponse(cluster1.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[0].getDataSets().get(0)))
                .getStatusCode(), 404);
        Assert.assertEquals(Util.parseResponse(cluster2.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[1].getDataSets().get(0)))
                .getStatusCode(), 404);
    }

    @Test(groups = {"prism", "0.2"})
    public void testFeedSnSOn1ColoWhileOtherColoIsDownUsingColoHelper() throws Exception {
        restartRequired = true;
        for (String cluster : bundles[1].getClusters()) {
            Util.assertSucceeded(prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, cluster));
        }

        Util.shutDownService(cluster1.getFeedHelper());

        Util.assertSucceeded(prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[1].getDataSets().get(0)));

        //now check if they have been scheduled correctly or not
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.FEED, bundles[1], Job.Status.RUNNING);
        //check if there is no criss cross
        AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.RUNNING);
    }


    @Test(groups = {"prism", "0.2"})
    public void testFeedSnSOn1ColoWhileThatColoIsDownUsingColoHelper() throws Exception {
        restartRequired = true;

        bundles[0].setCLusterColo(cluster1.getClusterHelper().getColoName());
        Util.print("cluster bundles[0]: " + bundles[0].getClusters().get(0));

        ServiceResponse r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundles[0].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        bundles[1].setCLusterColo(cluster2.getClusterHelper().getColoName());
        Util.print("cluster bundles[1]: " + bundles[1].getClusters().get(0));
        r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundles[1].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        String startTimeUA1 = "2012-10-01T12:00Z";
        String startTimeUA2 = "2012-10-01T12:00Z";

        String feed = bundles[0].getDataSets().get(0);
        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2012-10-01T12:00Z", "2010-01-01T00:00Z"),
                XmlUtil.createRtention("days(10000)", ActionType.DELETE), null,
                ClusterType.SOURCE, null);

        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTimeUA1, "2099-10-01T12:10Z"),
                XmlUtil.createRtention("days(10000)", ActionType.DELETE),
                Util.readClusterName(bundles[0].getClusters().get(0)), ClusterType.SOURCE,
                "${cluster.colo}",
                baseHDFSDir + "/localDC/rc/billing/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");

        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTimeUA2, "2099-10-01T12:25Z"),
                XmlUtil.createRtention("days(10000)", ActionType.DELETE),
                Util.readClusterName(bundles[1].getClusters().get(0)), ClusterType.TARGET, null,
                baseHDFSDir + "/clusterPath/localDC/rc/billing/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");

        Util.print("feed: " + feed);

        Util.shutDownService(cluster1.getFeedHelper());

        ServiceResponse response = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
        Util.assertPartialSucceeded(response);
        response = prism.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);
        Util.assertPartialSucceeded(response);
        Util.startService(cluster1.getFeedHelper());
        prism.getClusterHelper().delete(URLS.DELETE_URL, bundles[0].getClusters().get(0));
        prism.getClusterHelper().delete(URLS.DELETE_URL, bundles[1].getClusters().get(0));

    }


    @Test(groups = {"prism", "0.2"})
    public void testFeedSnSOn1ColoWhileAnotherColoHasSuspendedFeedUsingColoHelper() throws Exception {
        bundles[0].submitAndScheduleFeed();
        Util.assertSucceeded(bundles[0].getFeedHelper().suspend(URLS.SUSPEND_URL, bundles[0].getDataSets().get(0)));
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.FEED, bundles[0], Job.Status.SUSPENDED);

        bundles[1].submitAndScheduleFeed();
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.FEED, bundles[1], Job.Status.RUNNING);
        AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.FEED, bundles[0], Job.Status.SUSPENDED);
        AssertUtil.checkNotStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);
    }


    @Test(groups = {"prism", "0.2"})
    public void testFeedSnSOn1ColoWhileAnotherColoHasKilledFeedUsingColoHelper() throws Exception {
        bundles[0].submitAndScheduleFeed();
        Util.assertSucceeded(prism.getFeedHelper().delete(URLS.DELETE_URL, bundles[0].getDataSets().get(0)));
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.FEED, bundles[0], Job.Status.KILLED);
        bundles[1].submitAndScheduleFeed();
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.FEED, bundles[1], Job.Status.RUNNING);
        AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.FEED, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.FEED, bundles[0], Job.Status.KILLED);
        AssertUtil.checkNotStatus(cluster1OC, ENTITY_TYPE.FEED, bundles[1], Job.Status.RUNNING);
    }

}
