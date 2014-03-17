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


import junit.framework.Assert;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.interfaces.IEntityManagerHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.enumsAndConstants.ENTITY_TYPE;
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
 * Feed resume tests.
 */
@Test(groups = "embedded")
public class FeedResumeTest extends BaseTestClass {

    private final IEntityManagerHelper feedHelper = prism.getFeedHelper();
    private String feed;
    ColoHelper cluster = servers.get(0);
    OozieClient clusterOC = serverOC.get(0);

    @BeforeMethod(alwaysRun = true)
    public void setup(Method method) throws Exception {
        Util.print("test name: " + method.getName());
        bundles[0] = Util.readELBundles()[0][0];
        bundles[0].generateUniqueBundle();
        bundles[0] = new Bundle(bundles[0], cluster.getEnvFileName(), cluster.getPrefix());
        bundles[0].submitClusters(prism);
        feed = Util.getInputFeedFromBundle(bundles[0]);
    }

    @AfterMethod
    public void tearDown() throws Exception {
        removeBundles();
    }

    @Test(groups = {"singleCluster"})
    public void resumeSuspendedFeed() throws Exception {
        Util.assertSucceeded(feedHelper.submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed));
        Util.assertSucceeded(feedHelper.suspend(URLS.SUSPEND_URL, feed));
        AssertUtil.checkStatus(clusterOC, ENTITY_TYPE.FEED, feed, Job.Status.SUSPENDED);
        Util.assertSucceeded(feedHelper.resume(URLS.RESUME_URL, feed));

        ServiceResponse response = feedHelper.getStatus(URLS.STATUS_URL, feed);

        String colo = feedHelper.getColo();
        Assert.assertTrue(response.getMessage().contains(colo + "/RUNNING"));
        AssertUtil.checkStatus(clusterOC, ENTITY_TYPE.FEED, feed, Job.Status.RUNNING);
    }


    @Test(groups = {"singleCluster"})
    public void resumeNonExistentFeed() throws Exception {
        Util.assertFailed(feedHelper.resume(URLS.RESUME_URL, feed));
    }


    @Test(groups = {"singleCluster"})
    public void resumeDeletedFeed() throws Exception {
        Util.assertSucceeded(feedHelper.submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed));

        Util.assertSucceeded(feedHelper.delete(URLS.DELETE_URL, feed));

        Util.assertFailed(feedHelper.resume(URLS.RESUME_URL, feed));
    }


    @Test(groups = {"singleCluster"})
    public void resumeScheduledFeed() throws Exception {
        Util.assertSucceeded(feedHelper.submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed));

        AssertUtil.checkStatus(clusterOC, ENTITY_TYPE.FEED, feed, Job.Status.RUNNING);
        Util.assertSucceeded(feedHelper.resume(URLS.RESUME_URL, feed));


        ServiceResponse response = feedHelper.getStatus(URLS.STATUS_URL, feed);
        String colo = feedHelper.getColo();
        Assert.assertTrue(response.getMessage().contains(colo + "/RUNNING"));
        AssertUtil.checkStatus(clusterOC, ENTITY_TYPE.FEED, feed, Job.Status.RUNNING);
    }
}
