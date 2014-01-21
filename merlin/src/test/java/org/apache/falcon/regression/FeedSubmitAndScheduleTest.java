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

import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.response.APIResult;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.supportClasses.ENTITY_TYPE;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.testHelper.BaseSingleClusterTests;
import org.apache.oozie.client.Job;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;

/**
 * Feed submit and schedule tests.
 */
public class FeedSubmitAndScheduleTest extends BaseSingleClusterTests {

    private Bundle bundle;

    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        Util.print("test name: " + method.getName());
        bundle = Util.readELBundles()[0][0];
        bundle = new Bundle(bundle, server1.getEnvFileName());
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        bundle.deleteBundle(prism);
    }

    @Test(groups = {"singleCluster"})
    public void snsNewFeed() throws Exception {
        Assert.assertEquals(Util.parseResponse(prism.getClusterHelper()
                .submitEntity(URLS.SUBMIT_URL, bundle.getClusters().get(0))).getStatusCode(), 200);
        ServiceResponse response = prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundle.getDataSets().get(0));
        Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 200);
        Assert.assertEquals(Util.parseResponse(response).getStatus(),
                APIResult.Status.SUCCEEDED);
        Assert.assertNotNull(Util.parseResponse(response).getMessage());
        TimeUnit.SECONDS.sleep(5);
    }

    @Test(groups = {"singleCluster"})
    public void snsExistingFeed() throws Exception {
        Assert.assertEquals(Util.parseResponse(prism.getClusterHelper()
                .submitEntity(URLS.SUBMIT_URL, bundle.getClusters().get(0))).getStatusCode(), 200);

        ServiceResponse response = prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundle.getDataSets().get(0));


        Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 200);
        Assert.assertEquals(Util.parseResponse(response).getStatus(),
                APIResult.Status.SUCCEEDED);
        Assert.assertNotNull(Util.parseResponse(response).getMessage());
        Assert.assertTrue(Util.verifyOozieJobStatus(server1OC,
                Util.readDatasetName(bundle.getDataSets().get(0)), ENTITY_TYPE.FEED, Job.Status.RUNNING));
        //try to submitand schedule the same process again
        response = prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundle.getDataSets().get(0));

        Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 200);
        Assert.assertNotNull(Util.parseResponse(response).getMessage());
        Assert.assertTrue(Util.verifyOozieJobStatus(server1OC,
                Util.readDatasetName(bundle.getDataSets().get(0)), ENTITY_TYPE.FEED, Job.Status.RUNNING));
    }

    @Test(groups = {"singleCluster"})
    public void snsFeedWithoutCluster() throws Exception {
        ServiceResponse response = prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundle.getDataSets().get(0));

        Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 400);
        Assert.assertEquals(Util.parseResponse(response).getStatus(), APIResult.Status.FAILED);
        Assert.assertNotNull(Util.parseResponse(response).getMessage());
    }


    @Test(groups = {"singleCluster"})
    public void snsRunningProcess() throws Exception {
        Assert.assertEquals(Util.parseResponse(prism.getClusterHelper()
                .submitEntity(URLS.SUBMIT_URL, bundle.getClusters().get(0))).getStatusCode(), 200);

        ServiceResponse response = prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundle.getDataSets().get(0));

        Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 200);
        Assert.assertEquals(Util.parseResponse(response).getStatus(),
                APIResult.Status.SUCCEEDED);
        Assert.assertNotNull(Util.parseResponse(response).getMessage());
        Assert.assertTrue(Util.verifyOozieJobStatus(server1OC,
                Util.readDatasetName(bundle.getDataSets().get(0)), ENTITY_TYPE.FEED, Job.Status.RUNNING));
        response = prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundle.getDataSets().get(0));

        Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 200);
        Assert.assertEquals(Util.parseResponse(response).getStatus(),
                APIResult.Status.SUCCEEDED);
        Assert.assertNotNull(Util.parseResponse(response).getMessage());
    }


    @Test(groups = {"singleCluster"})
    public void snsDeletedFeed() throws Exception {
        Assert.assertEquals(Util.parseResponse(prism.getClusterHelper()
                .submitEntity(URLS.SUBMIT_URL, bundle.getClusters().get(0))).getStatusCode(), 200);


        ServiceResponse response = prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundle.getDataSets().get(0));

        Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 200);
        Assert.assertEquals(Util.parseResponse(response).getStatus(),
                APIResult.Status.SUCCEEDED);
        Assert.assertNotNull(Util.parseResponse(response).getMessage());
        Assert.assertTrue(Util.verifyOozieJobStatus(server1OC,
                Util.readDatasetName(bundle.getDataSets().get(0)), ENTITY_TYPE.FEED, Job.Status.RUNNING));
        Assert.assertEquals(
                Util.parseResponse(prism.getFeedHelper()
                        .delete(URLS.DELETE_URL, bundle.getDataSets().get(0)))
                        .getStatusCode(), 200);
        Assert.assertTrue(Util.verifyOozieJobStatus(server1OC,
                Util.readDatasetName(bundle.getDataSets().get(0)), ENTITY_TYPE.FEED, Job.Status.KILLED));
        response = prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundle.getDataSets().get(0));

        Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 200);
        Assert.assertEquals(Util.parseResponse(response).getStatus(),
                APIResult.Status.SUCCEEDED);
        Assert.assertNotNull(Util.parseResponse(response).getMessage());
        Assert.assertTrue(Util.verifyOozieJobStatus(server1OC,
                Util.readDatasetName(bundle.getDataSets().get(0)), ENTITY_TYPE.FEED, Job.Status.RUNNING));
    }


    @Test(groups = {"singleCluster"})
    public void snsSuspendedFeed() throws Exception {
        Assert.assertEquals(Util.parseResponse(prism.getClusterHelper()
                .submitEntity(URLS.SUBMIT_URL, bundle.getClusters().get(0))).getStatusCode(), 200);


        ServiceResponse response = prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundle.getDataSets().get(0));

        Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 200);
        Assert.assertEquals(Util.parseResponse(response).getStatus(),
                APIResult.Status.SUCCEEDED);
        Assert.assertNotNull(Util.parseResponse(response).getMessage());
        TimeUnit.SECONDS.sleep(20);
        Assert.assertTrue(Util.verifyOozieJobStatus(server1OC,
                Util.readDatasetName(bundle.getDataSets().get(0)), ENTITY_TYPE.FEED, Job.Status.RUNNING));
        Assert.assertEquals(Util.parseResponse(
                prism.getFeedHelper()
                        .suspend(URLS.SUSPEND_URL, bundle.getDataSets().get(0)))
                .getStatusCode(),
                200);
        Assert.assertTrue(Util.verifyOozieJobStatus(server1OC,
                Util.readDatasetName(bundle.getDataSets().get(0)), ENTITY_TYPE.FEED, Job.Status.SUSPENDED));
        response = prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundle.getDataSets().get(0));

        Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 200);
        Assert.assertEquals(Util.parseResponse(response).getStatus(),
                APIResult.Status.SUCCEEDED);
        Assert.assertNotNull(Util.parseResponse(response).getMessage());
        Assert.assertTrue(Util.verifyOozieJobStatus(server1OC,
                Util.readDatasetName(bundle.getDataSets().get(0)), ENTITY_TYPE.FEED, Job.Status.SUSPENDED));
    }
}



