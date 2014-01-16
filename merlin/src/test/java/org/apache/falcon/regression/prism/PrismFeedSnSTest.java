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
import org.apache.falcon.regression.core.supportClasses.ENTITY_TYPE;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.core.util.XmlUtil;
import org.apache.falcon.regression.testHelper.BaseMultiClusterTests;
import org.apache.oozie.client.Job;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

public class PrismFeedSnSTest extends BaseMultiClusterTests{

    private Bundle bundle1;
    private Bundle bundle2;
    private boolean restartRequired;


    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        Util.print("test name: " + method.getName());
        restartRequired = false;
        Bundle bundle = Util.readELBundles()[0][0];
        bundle1 = new Bundle(bundle, server1.getEnvFileName());
        bundle2 = new Bundle(bundle, server2.getEnvFileName());
        bundle1.generateUniqueBundle();
        bundle2.generateUniqueBundle();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        if (restartRequired) {
            Util.restartService(server1.getFeedHelper());
        }
    }


    @Test(groups = {"prism", "0.2"})
    public void testFeedSnSOnBothColos() throws Exception {
        //schedule both bundles
        submitAndScheduleFeed(bundle1);
        checkStatus(server1, bundle1, Job.Status.RUNNING);
        Assert.assertNotEquals(Util.getOozieJobStatus(server1OC,
                Util.readDatasetName(bundle2.getDataSets().get(0)), ENTITY_TYPE.PROCESS), Job.Status.RUNNING);
        submitAndScheduleFeed(bundle2);

        //now check if they have been scheduled correctly or not
        checkStatus(server2, bundle2, Job.Status.RUNNING);

        //check if there is no criss cross
        Assert.assertNotEquals(Util.getOozieJobStatus(server2OC,
                Util.readDatasetName(bundle1.getDataSets().get(0)), ENTITY_TYPE.PROCESS), Job.Status.RUNNING);
    }

    @Test(groups = {"prism", "0.2"})
    public void testSnSAlreadyScheduledFeedOnBothColos() throws Exception {
        //schedule both bundles
        submitAndScheduleFeed(bundle1);
        submitAndScheduleFeed(bundle2);

        //now check if they have been scheduled correctly or not
        checkStatus(server1, bundle1, Job.Status.RUNNING);
        checkStatus(server2, bundle2, Job.Status.RUNNING);

        //check if there is no criss cross
        checkNotStatus(server1, bundle2, Job.Status.RUNNING);
        checkNotStatus(server2, bundle1, Job.Status.RUNNING);


        Util.assertSucceeded(prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundle1.getDataSets().get(0)));
        //ensure only one bundle is there
        Assert.assertEquals(Util.getBundles(server1OC,
                Util.readDatasetName(bundle1.getDataSets().get(0)), ENTITY_TYPE.FEED).size(), 1);
        Util.assertSucceeded(prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundle2.getDataSets().get(0)));
        Assert.assertEquals(Util.getBundles(server2OC,
                Util.readDatasetName(bundle2.getDataSets().get(0)), ENTITY_TYPE.FEED).size(), 1);
        //now check if they have been scheduled correctly or not
        checkStatus(server1, bundle1, Job.Status.RUNNING);
        checkStatus(server2, bundle2, Job.Status.RUNNING);
    }


    @Test(groups = {"prism", "0.2"})
    public void testSnSSuspendedFeedOnBothColos() throws Exception {
        //schedule both bundles
        submitAndScheduleFeed(bundle1);
        submitAndScheduleFeed(bundle2);

        Util.assertSucceeded(prism.getFeedHelper()
                .suspend(URLS.SUSPEND_URL, bundle1.getDataSets().get(0)));
        checkStatus(server1, bundle1, Job.Status.SUSPENDED);
        checkStatus(server2, bundle2, Job.Status.RUNNING);
        //now check if they have been scheduled correctly or not
        Util.assertSucceeded(prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundle1.getDataSets().get(0)));
        checkStatus(server1, bundle1, Job.Status.SUSPENDED);
        Assert.assertEquals(Util.getBundles(server1OC,
                Util.readDatasetName(bundle1.getDataSets().get(0)), ENTITY_TYPE.FEED).size(), 1);

        Util.assertSucceeded(server1.getFeedHelper()
                .resume(URLS.RESUME_URL, bundle1.getDataSets().get(0)));
        checkStatus(server1, bundle1, Job.Status.RUNNING);

        Util.assertSucceeded(prism.getFeedHelper()
                .suspend(URLS.SUSPEND_URL, bundle2.getDataSets().get(0)));
        checkStatus(server2, bundle2, Job.Status.SUSPENDED);
        checkStatus(server1, bundle1, Job.Status.RUNNING);

        Util.assertSucceeded(prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundle2.getDataSets().get(0)));
        checkStatus(server2, bundle2, Job.Status.SUSPENDED);
        Assert.assertEquals(Util.getBundles(server2OC,
                Util.readDatasetName(bundle2.getDataSets().get(0)), ENTITY_TYPE.FEED).size(), 1);
        Util.assertSucceeded(server2.getFeedHelper()
                .resume(URLS.RESUME_URL, bundle2.getDataSets().get(0)));
        checkStatus(server2, bundle2, Job.Status.RUNNING);


    }

    @Test(groups = {"prism", "0.2"})
    public void testSnSDeletedFeedOnBothColos() throws Exception {
        //schedule both bundles
        submitAndScheduleFeed(bundle1);
        submitAndScheduleFeed(bundle2);

        Util.assertSucceeded(prism.getFeedHelper().delete(URLS.DELETE_URL, bundle1.getDataSets().get(0)));
        checkStatus(server1, bundle1, Job.Status.KILLED);
        checkStatus(server2, bundle2, Job.Status.RUNNING);

        Util.assertSucceeded(prism.getFeedHelper().delete(URLS.DELETE_URL, bundle2.getDataSets().get(0)));
        checkStatus(server2, bundle2, Job.Status.KILLED);
        checkStatus(server1, bundle1, Job.Status.KILLED);

        Util.assertSucceeded(prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundle1.getDataSets().get(0)));
        Util.assertSucceeded(prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundle2.getDataSets().get(0)));
    }

    @Test(groups = {"prism", "0.2"})
    public void testScheduleNonExistentFeedOnBothColos() throws Exception {
        Util.assertFailed(prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundle1.getDataSets().get(0)));
        Util.assertFailed(prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundle2.getDataSets().get(0)));
    }

    @Test(groups = {"prism", "0.2"})
    public void testFeedSnSOn1ColoWhileOtherColoIsDown() throws Exception {
        restartRequired = true;
        for (String cluster : bundle2.getClusters()) {
            Util.assertSucceeded(prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, cluster));
        }

        Util.shutDownService(server1.getFeedHelper());

        Util.assertSucceeded(prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundle2.getDataSets().get(0)));

        //now check if they have been scheduled correctly or not
        checkStatus(server2, bundle2, Job.Status.RUNNING);
        //check if there is no criss cross
        Assert.assertNotEquals(Util.getOozieJobStatus(server2OC,
                Util.readDatasetName(bundle1.getDataSets().get(0)), ENTITY_TYPE.PROCESS), Job.Status.RUNNING);
    }


    @Test(groups = {"prism", "0.2"})
    public void testFeedSnSOn1ColoWhileThatColoIsDown() throws Exception {
        restartRequired = true;
        submitFeed(bundle1);

        Util.shutDownService(server1.getFeedHelper());

        Util.assertFailed(prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundle1.getDataSets().get(0)));
        Assert.assertNotEquals(Util.getOozieJobStatus(server2OC,
                Util.readDatasetName(bundle1.getDataSets().get(0)), ENTITY_TYPE.PROCESS), Job.Status.RUNNING);
    }

    @Test(groups = {"prism", "0.2"})
    public void testFeedSnSOn1ColoWhileAnotherColoHasSuspendedFeed() throws Exception {
        submitAndScheduleFeed(bundle1);
        Util.assertSucceeded(prism.getFeedHelper()
                .suspend(URLS.SUSPEND_URL, bundle1.getDataSets().get(0)));
        checkStatus(server1, bundle1, Job.Status.SUSPENDED);
        submitAndScheduleFeed(bundle2);
        checkStatus(server2, bundle2, Job.Status.RUNNING);
        checkNotStatus(server2, bundle1, Job.Status.RUNNING);
        checkStatus(server1, bundle1, Job.Status.SUSPENDED);
        checkNotStatus(server1, bundle2, Job.Status.RUNNING);
    }

    @Test(groups = {"prism", "0.2"})
    public void testFeedSnSOn1ColoWhileAnotherColoHasKilledFeed() throws Exception {
        submitAndScheduleFeed(bundle1);
        Util.assertSucceeded(prism.getFeedHelper().delete(URLS.DELETE_URL, bundle1.getDataSets().get(0)));
        checkStatus(server1, bundle1, Job.Status.KILLED);
        submitAndScheduleFeed(bundle2);
        checkStatus(server2, bundle2, Job.Status.RUNNING);
        checkNotStatus(server2, bundle1, Job.Status.RUNNING);
        checkStatus(server1, bundle1, Job.Status.KILLED);
        checkNotStatus(server1, bundle2, Job.Status.RUNNING);
    }

    @Test(groups = {"prism", "0.2"})
    public void testFeedSnSOnBothColosUsingColoHelper() throws Exception {
        //schedule both bundles
        submitFeed(bundle1);
        APIResult result = Util.parseResponse((server1.getFeedHelper()
                .submitEntity(URLS.SUBMIT_AND_SCHEDULE_URL, bundle1.getDataSets().get(0))));
        Assert.assertEquals(result.getStatusCode(), 404);
        checkNotStatus(server1, bundle1, Job.Status.RUNNING);
        submitFeed(bundle2);
        result = Util.parseResponse(server2.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundle2.getDataSets().get(0)));
        Assert.assertEquals(result.getStatusCode(), 404);

        checkNotStatus(server2, bundle2, Job.Status.RUNNING);
    }


    @Test(groups = {"prism", "0.2"})
    public void testSnSSuspendedFeedOnBothColosUsingColoHelper() throws Exception {

        //schedule both bundles
        submitFeed(bundle1);
        Util.assertSucceeded(prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundle1.getDataSets().get(0)));
        submitFeed(bundle2);
        Util.assertSucceeded(prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundle2.getDataSets().get(0)));

        Util.assertSucceeded(server1.getFeedHelper()
                .suspend(URLS.SUSPEND_URL, bundle1.getDataSets().get(0)));
        checkStatus(server1, bundle1, Job.Status.SUSPENDED);
        checkStatus(server2, bundle2, Job.Status.RUNNING);
        //now check if they have been scheduled correctly or not
        Util.assertSucceeded(prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundle1.getDataSets().get(0)));
        checkStatus(server1, bundle1, Job.Status.SUSPENDED);
        Util.assertSucceeded(server1.getFeedHelper().resume(URLS.RESUME_URL, bundle1.getDataSets().get(0)));

        Util.assertSucceeded(server2.getFeedHelper().suspend(URLS.SUSPEND_URL, bundle2.getDataSets().get(0)));
        Util.assertSucceeded(prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundle1.getDataSets().get(0)));
        checkStatus(server2, bundle2, Job.Status.SUSPENDED);
        checkStatus(server1, bundle1, Job.Status.RUNNING);
    }


    @Test(groups = {"prism", "0.2"})
    public void testScheduleDeletedFeedOnBothColosUsingColoHelper() throws Exception {

        //schedule both bundles
        submitAndScheduleFeed(bundle1);
        submitAndScheduleFeed(bundle2);

        Util.assertSucceeded(prism.getFeedHelper().delete(URLS.DELETE_URL, bundle1.getDataSets().get(0)));
        checkStatus(server1, bundle1, Job.Status.KILLED);
        checkStatus(server2, bundle2, Job.Status.RUNNING);

        Util.assertSucceeded(prism.getFeedHelper().delete(URLS.DELETE_URL, bundle2.getDataSets().get(0)));
        checkStatus(server2, bundle2, Job.Status.KILLED);
        checkStatus(server1, bundle1, Job.Status.KILLED);
        Util.assertSucceeded(prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundle1.getDataSets().get(0)));
        Util.assertSucceeded(prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundle2.getDataSets().get(0)));

        Assert.assertEquals(Util.parseResponse(prism.getFeedHelper()
                        .getStatus(URLS.STATUS_URL, bundle1.getDataSets().get(0))).getMessage(),
                server1.getClusterHelper().getColo().split("=")[1] + "/RUNNING");

        Assert.assertEquals(Util.parseResponse(prism.getFeedHelper()
                        .getStatus(URLS.STATUS_URL, bundle2.getDataSets().get(0))).getMessage(),
                server2.getClusterHelper().getColo().split("=")[1] + "/RUNNING");
    }


    @Test(groups = {"prism", "0.2"})
    public void testSNSNonExistentFeedOnBothColosUsingColoHelper() throws Exception {

        Assert.assertEquals(Util.parseResponse(server1.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundle1.getDataSets().get(0)))
                .getStatusCode(), 404);
        Assert.assertEquals(Util.parseResponse(server2.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundle2.getDataSets().get(0)))
                .getStatusCode(), 404);
    }

    @Test(groups = {"prism", "0.2"})
    public void testFeedSnSOn1ColoWhileOtherColoIsDownUsingColoHelper() throws Exception {
        restartRequired = true;
        for (String cluster : bundle2.getClusters()) {
            Util.assertSucceeded(prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, cluster));
        }

        Util.shutDownService(server1.getFeedHelper());

        Util.assertSucceeded(prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundle2.getDataSets().get(0)));

        //now check if they have been scheduled correctly or not
        checkStatus(server2, bundle2, Job.Status.RUNNING);
        //check if there is no criss cross
        Assert.assertNotEquals(Util.getOozieJobStatus(server2OC,
                Util.readDatasetName(bundle1.getDataSets().get(0)), ENTITY_TYPE.PROCESS), Job.Status.RUNNING);
    }


    @Test(groups = {"prism", "0.2"})
    public void testFeedSnSOn1ColoWhileThatColoIsDownUsingColoHelper() throws Exception {
        restartRequired = true;

        bundle1.setCLusterColo(server1.getClusterHelper().getColo().split("=")[1]);
        Util.print("cluster bundle1: " + bundle1.getClusters().get(0));

        ServiceResponse r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle1.getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        bundle2.setCLusterColo(server2.getClusterHelper().getColo().split("=")[1]);
        Util.print("cluster bundle2: " + bundle2.getClusters().get(0));
        r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle2.getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        String startTimeUA1 = "2012-10-01T12:00Z";
        String startTimeUA2 = "2012-10-01T12:00Z";

        String feed = bundle1.getDataSets().get(0);
        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2012-10-01T12:00Z", "2010-01-01T00:00Z"),
                XmlUtil.createRtention("days(10000)", ActionType.DELETE), null,
                ClusterType.SOURCE, null, null);

        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTimeUA1, "2099-10-01T12:10Z"),
                XmlUtil.createRtention("days(10000)", ActionType.DELETE),
                Util.readClusterName(bundle1.getClusters().get(0)), ClusterType.SOURCE,
                "${cluster.colo}",
                baseHDFSDir + "/localDC/rc/billing/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");

        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTimeUA2, "2099-10-01T12:25Z"),
                XmlUtil.createRtention("days(10000)", ActionType.DELETE),
                Util.readClusterName(bundle2.getClusters().get(0)), ClusterType.TARGET, null,
                baseHDFSDir + "/clusterPath/localDC/rc/billing/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");

        Util.print("feed: " + feed);

        Util.shutDownService(server1.getFeedHelper());

        ServiceResponse response = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
        Util.assertPartialSucceeded(response);
        response = prism.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);
        Util.assertPartialSucceeded(response);
        Util.startService(server1.getFeedHelper());
        prism.getClusterHelper().delete(URLS.DELETE_URL, bundle1.getClusters().get(0));
        prism.getClusterHelper().delete(URLS.DELETE_URL, bundle2.getClusters().get(0));

    }


    @Test(groups = {"prism", "0.2"})
    public void testFeedSnSOn1ColoWhileAnotherColoHasSuspendedFeedUsingColoHelper() throws Exception {
        submitAndScheduleFeed(bundle1);
        Util.assertSucceeded(bundle1.getFeedHelper().suspend(URLS.SUSPEND_URL, bundle1.getDataSets().get(0)));
        checkStatus(server1, bundle1, Job.Status.SUSPENDED);

        submitAndScheduleFeed(bundle2);
        checkStatus(server2, bundle2, Job.Status.RUNNING);
        Assert.assertNotEquals(Util.getOozieJobStatus(server2OC,
                Util.readDatasetName(bundle1.getDataSets().get(0)), ENTITY_TYPE.PROCESS), Job.Status.RUNNING);
        checkStatus(server1, bundle1, Job.Status.SUSPENDED);
        Assert.assertNotEquals(Util.getOozieJobStatus(server1OC,
                Util.readDatasetName(bundle2.getDataSets().get(0)), ENTITY_TYPE.PROCESS), Job.Status.RUNNING);
    }


    @Test(groups = {"prism", "0.2"})
    public void testFeedSnSOn1ColoWhileAnotherColoHasKilledFeedUsingColoHelper() throws Exception {
        submitAndScheduleFeed(bundle1);
        Util.assertSucceeded(prism.getFeedHelper().delete(URLS.DELETE_URL, bundle1.getDataSets().get(0)));
        checkStatus(server1, bundle1, Job.Status.KILLED);
        submitAndScheduleFeed(bundle2);
        checkStatus(server2, bundle2, Job.Status.RUNNING);
        checkNotStatus(server2, bundle1, Job.Status.RUNNING);
        checkStatus(server1, bundle1, Job.Status.KILLED);
        checkNotStatus(server1, bundle2, Job.Status.RUNNING);
    }

    private void checkStatus(ColoHelper coloHelper, Bundle bundle, Job.Status expectedStatus) throws Exception {
        Assert.assertTrue(Util.verifyOozieJobStatus(coloHelper.getFeedHelper().getOozieClient(),
                Util.readDatasetName(bundle.getDataSets().get(0)), ENTITY_TYPE.FEED, expectedStatus));
    }

    private void checkNotStatus(ColoHelper coloHelper, Bundle bundle, Job.Status expectedStatus) throws Exception {
        Assert.assertNotEquals(Util.getOozieJobStatus(coloHelper.getFeedHelper().getOozieClient(),
                Util.readDatasetName(bundle.getDataSets().get(0)), ENTITY_TYPE.FEED), expectedStatus);
    }

    private void submitFeed(Bundle bundle) throws Exception {
        for (String cluster : bundle.getClusters()) {
            Util.assertSucceeded(prism.getClusterHelper().submitEntity(Util.URLS.SUBMIT_URL, cluster));
        }

        Util.assertSucceeded(prism.getFeedHelper().submitEntity(Util.URLS.SUBMIT_URL, bundle.getDataSets().get(0)));
    }


    private void submitAndScheduleFeed(Bundle bundle) throws Exception {
        for (String cluster : bundle.getClusters()) {
            Util.assertSucceeded(prism.getClusterHelper().submitEntity(Util.URLS.SUBMIT_URL, cluster));
        }
        Util.assertSucceeded(prism.getFeedHelper()
                .submitAndSchedule(Util.URLS.SUBMIT_AND_SCHEDULE_URL, bundle.getDataSets().get(0)));
    }

}
