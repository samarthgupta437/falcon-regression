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

/**
 * Feed status tests.
 */
public class FeedStatusTest extends BaseSingleClusterTests {
    
    private Bundle bundle;
    private String feed;

    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        Util.print("test name: " + method.getName());
        bundle = Util.readELBundles()[0][0];
        bundle.generateUniqueBundle();
        bundle = new Bundle(bundle, server1.getEnvFileName(), server1.getPrefix());

        //submit the cluster
        ServiceResponse response = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle.getClusters().get(0));
        Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 200);
        Assert.assertNotNull(Util.parseResponse(response).getMessage());

        feed = Util.getInputFeedFromBundle(bundle);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        prism.getFeedHelper().delete(URLS.DELETE_URL, Util.getInputFeedFromBundle(bundle));
    }


    @Test(groups = {"singleCluster"})
    public void getStatusForScheduledFeed() throws Exception {
        ServiceResponse response = prism.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed);
        Util.print(feed);
        Util.assertSucceeded(response);

        response = prism.getFeedHelper().getStatus(URLS.STATUS_URL, feed);

        Util.assertSucceeded(response);

        Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 200);
        Assert.assertNotNull(Util.parseResponse(response).getMessage());

        String colo = prism.getFeedHelper().getColo();
        Assert.assertTrue(response.getMessage().contains(colo + "/RUNNING"));
        Assert.assertTrue(Util.verifyOozieJobStatus(server1OC,
                Util.readDatasetName(feed), ENTITY_TYPE.FEED, Job.Status.RUNNING));
    }


    @Test(groups = {"singleCluster"})
    public void getStatusForSuspendedFeed() throws Exception {
        ServiceResponse response = prism.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed);

        Util.assertSucceeded(response);

        response = prism.getFeedHelper().suspend(URLS.SUSPEND_URL, feed);
        Util.assertSucceeded(response);

        response = prism.getFeedHelper().getStatus(URLS.STATUS_URL, feed);

        Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 200);
        Assert.assertNotNull(Util.parseResponse(response).getMessage());
        String colo = prism.getFeedHelper().getColo();
        Assert.assertTrue(response.getMessage().contains(colo + "/SUSPENDED"));
        Assert.assertTrue(Util.verifyOozieJobStatus(server1OC,
                Util.readDatasetName(feed), ENTITY_TYPE.FEED, Job.Status.SUSPENDED));
        
    }


    @Test(groups = {"singleCluster"})
    public void getStatusForSubmittedFeed() throws Exception {
        ServiceResponse response = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);

        Util.assertSucceeded(response);

        response = prism.getFeedHelper().getStatus(URLS.STATUS_URL, feed);

        Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 200);
        Assert.assertNotNull(Util.parseResponse(response).getMessage());
        String colo = prism.getFeedHelper().getColo();
        Assert.assertTrue(response.getMessage().contains(colo + "/SUBMITTED"));
        Assert.assertTrue(Util.getOozieJobStatus(server1OC,
                Util.readDatasetName(feed), ENTITY_TYPE.FEED) != Job.Status.RUNNING);
    }


    @Test(groups = {"singleCluster"})
    public void getStatusForDeletedFeed() throws Exception {
        ServiceResponse response =
                prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
        Util.assertSucceeded(response);

        response = prism.getFeedHelper().delete(URLS.DELETE_URL, feed);
        Util.assertSucceeded(response);

        response = prism.getFeedHelper().getStatus(URLS.STATUS_URL, feed);
        Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 400);
        Assert.assertNotNull(Util.parseResponse(response).getMessage());

        Assert.assertTrue(
                response.getMessage().contains(Util.getFeedName(feed) + " (FEED) not found"));
        Assert.assertTrue(Util.getOozieJobStatus(server1OC,
                Util.readDatasetName(feed), ENTITY_TYPE.FEED) != Job.Status.KILLED);
    }


    @Test(groups = {"singleCluster"})
    public void getStatusForNonExistentFeed() throws Exception {
        ServiceResponse response = prism.getFeedHelper().getStatus(URLS.STATUS_URL, feed);
        Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 400);
        Assert.assertNotNull(Util.parseResponse(response).getMessage());
        Assert.assertTrue(
                response.getMessage().contains(Util.getFeedName(feed) + " (FEED) not found"));

    }
}
