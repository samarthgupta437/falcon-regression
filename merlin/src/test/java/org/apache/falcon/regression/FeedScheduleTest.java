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
import org.apache.falcon.regression.core.util.AssertUtil;
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
 * Feed schedule tests.
 */
public class FeedScheduleTest extends BaseSingleClusterTests {

    private Bundle bundle;
    private String feed;

    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        Util.print("test name: " + method.getName());
        bundle = Util.readELBundles()[0][0];
        bundle = new Bundle(bundle, server1.getEnvFileName(), server1.getPrefix());
        Bundle.submitCluster(bundle);
        feed = Util.getInputFeedFromBundle(bundle);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        prism.getFeedHelper().delete(URLS.DELETE_URL, Util.getInputFeedFromBundle(bundle));
    }



    @Test(groups = {"singleCluster"})
    public void scheduleAlreadyScheduledFeed() throws Exception {
        ServiceResponse response = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
        Util.assertSucceeded(response);

        response = prism.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);
        Util.assertSucceeded(response);
        Assert.assertTrue(Util.verifyOozieJobStatus(server1OC,
                Util.readDatasetName(feed), ENTITY_TYPE.FEED, Job.Status.RUNNING));
        //now try re-scheduling again
        response = prism.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);
        AssertUtil.assertSucceeded(response);
    }


    @Test(groups = {"singleCluster"})
    public void scheduleValidFeed() throws Exception {
        //submit feed
        ServiceResponse response = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
        Util.assertSucceeded(response);

        //now schedule the thing
        response = prism.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);
        Util.assertSucceeded(response);
        Assert.assertTrue(Util.verifyOozieJobStatus(server1OC,
                Util.readDatasetName(feed), ENTITY_TYPE.FEED, Job.Status.RUNNING));
    }


    @Test(groups = {"singleCluster"})
    public void scheduleSuspendedFeed() throws Exception {
        Util.assertSucceeded(prism.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed));

        //now suspend
        Util.assertSucceeded(prism.getFeedHelper().suspend(URLS.SUSPEND_URL, feed));
        Assert.assertTrue(Util.verifyOozieJobStatus(server1OC,
                Util.readDatasetName(feed), ENTITY_TYPE.FEED, Job.Status.SUSPENDED));
        //now schedule this!
        Util.assertSucceeded(prism.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed));
        Assert.assertTrue(Util.verifyOozieJobStatus(server1OC,
                Util.readDatasetName(feed), ENTITY_TYPE.FEED, Job.Status.SUSPENDED));
    }

    @Test(groups = {"singleCluster"})
    public void scheduleKilledFeed() throws Exception {
        Util.assertSucceeded(prism.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed));

        //now suspend
        Util.assertSucceeded(prism.getFeedHelper().delete(URLS.DELETE_URL, feed));
        Assert.assertTrue(Util.verifyOozieJobStatus(server1OC,
                Util.readDatasetName(feed), ENTITY_TYPE.FEED, Job.Status.KILLED));
        //now schedule this!
        Util.assertFailed(prism.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed));
    }

    @Test(groups = {"singleCluster"})
    public void scheduleNonExistentFeed() throws Exception {
        Util.assertFailed(prism.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed));
    }
}
