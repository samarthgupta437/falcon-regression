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
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.supportClasses.ENTITY_TYPE;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

/**
 * Feed schedule tests.
 */
@Test(groups = "embedded")
public class FeedScheduleTest extends BaseTestClass {

    ColoHelper cluster = servers.get(0);;
    OozieClient clusterOC = serverOC.get(0);
    private String feed;

    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        Util.print("test name: " + method.getName());
        bundles[0] = Util.readELBundles()[0][0];
        bundles[0] = new Bundle(bundles[0], cluster.getEnvFileName(), cluster.getPrefix());
        bundles[0].generateUniqueBundle();
        Bundle.submitCluster(bundles[0]);
        feed = Util.getInputFeedFromBundle(bundles[0]);
    }

    @Test(groups = {"singleCluster"})
    public void scheduleAlreadyScheduledFeed() throws Exception {
        ServiceResponse response = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
        Util.assertSucceeded(response);

        response = prism.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);
        Util.assertSucceeded(response);
        AssertUtil.checkStatus(clusterOC, ENTITY_TYPE.FEED, feed, Job.Status.RUNNING);
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
        AssertUtil.checkStatus(clusterOC, ENTITY_TYPE.FEED, feed, Job.Status.RUNNING);
    }


    @Test(groups = {"singleCluster"})
    public void scheduleSuspendedFeed() throws Exception {
        Util.assertSucceeded(prism.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed));

        //now suspend
        Util.assertSucceeded(prism.getFeedHelper().suspend(URLS.SUSPEND_URL, feed));
        AssertUtil.checkStatus(clusterOC, ENTITY_TYPE.FEED, feed, Job.Status.SUSPENDED);
        //now schedule this!
        Util.assertSucceeded(prism.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed));
        AssertUtil.checkStatus(clusterOC, ENTITY_TYPE.FEED, feed, Job.Status.SUSPENDED);
    }

    @Test(groups = {"singleCluster"})
    public void scheduleKilledFeed() throws Exception {
        Util.assertSucceeded(prism.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed));

        //now suspend
        Util.assertSucceeded(prism.getFeedHelper().delete(URLS.DELETE_URL, feed));
        AssertUtil.checkStatus(clusterOC, ENTITY_TYPE.FEED, feed, Job.Status.KILLED);
        //now schedule this!
        Util.assertFailed(prism.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed));
    }

    @Test(groups = {"singleCluster"})
    public void scheduleNonExistentFeed() throws Exception {
        Util.assertFailed(prism.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed));
    }
}
