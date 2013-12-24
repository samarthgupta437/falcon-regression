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
import org.apache.falcon.regression.core.helpers.PrismHelper;
import org.apache.falcon.regression.core.interfaces.IEntityManagerHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.testng.TestNGException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

/**
 * Feed resume tests.
 */
public class FeedResumeTest {

    private final PrismHelper prismHelper = new PrismHelper("prism.properties");
    private final IEntityManagerHelper feedHelper = prismHelper.getFeedHelper();

    private final ColoHelper ivoryqa1 = new ColoHelper("ivoryqa-1.config.properties");

    @BeforeMethod(alwaysRun = true)
    public void testName(Method method) {
        Util.print("test name: " + method.getName());
    }

    @Test(groups = {"singleCluster"}, dataProvider = "DP")
    public void resumeSuspendedFeed(Bundle bundle) throws Exception {
        try {
            bundle.generateUniqueBundle();

            bundle = new Bundle(bundle, ivoryqa1.getEnvFileName());
            Bundle.submitCluster(bundle);

            String feed = Util.getInputFeedFromBundle(bundle);

            Util.assertSucceeded(feedHelper.submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed));
            Util.assertSucceeded(feedHelper.suspend(URLS.SUSPEND_URL, feed));

            final ArrayList<String> jobStatus =
                    Util.getOozieFeedJobStatus(Util.readDatasetName(feed), "SUSPENDED", ivoryqa1);
            Assert.assertTrue(jobStatus.get(0).contains("SUSPENDED"));


            Util.assertSucceeded(feedHelper.resume(URLS.RESUME_URL, feed));

            ServiceResponse response = feedHelper.getStatus(URLS.STATUS_URL, feed);

            String colo = feedHelper.getColo();
            Assert.assertTrue(response.getMessage().contains(colo + "/RUNNING"));

            Assert.assertTrue(
                    Util.getOozieFeedJobStatus(Util.readDatasetName(feed), "RUNNING", ivoryqa1)
                            .get(0)
                            .contains("RUNNING"));

        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        } finally {
            feedHelper
                    .delete(URLS.DELETE_URL, Util.getInputFeedFromBundle(bundle));
        }
    }


    public void submitCluster(Bundle bundle) throws Exception {
        ServiceResponse response = prismHelper.getClusterHelper().submitEntity(
                URLS.SUBMIT_URL, bundle.getClusters().get(0));

        Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 200);
        Assert.assertNotNull(Util.parseResponse(response).getMessage());
    }


    @Test(groups = {"singleCluster"}, dataProvider = "DP")
    public void resumeNonExistentFeed(Bundle bundle) throws Exception {
        try {
            bundle.generateUniqueBundle();
            bundle = new Bundle(bundle, ivoryqa1.getEnvFileName());
            submitCluster(bundle);
            String feed = Util.getInputFeedFromBundle(bundle);

            Util.assertFailed(feedHelper.resume(URLS.RESUME_URL, feed));

        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        }
    }


    @Test(groups = {"singleCluster"}, dataProvider = "DP")
    public void resumeDeletedFeed(Bundle bundle) throws Exception {
        try {
            bundle.generateUniqueBundle();
            bundle = new Bundle(bundle, ivoryqa1.getEnvFileName());
            submitCluster(bundle);
            String feed = Util.getInputFeedFromBundle(bundle);

            Util.assertSucceeded(feedHelper.submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed));

            Util.assertSucceeded(feedHelper.delete(URLS.DELETE_URL, feed));

            Util.assertFailed(feedHelper.resume(URLS.RESUME_URL, feed));

        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        } finally {

            feedHelper
                    .delete(URLS.DELETE_URL, Util.getInputFeedFromBundle(bundle));
        }
    }


    @Test(groups = {"singleCluster"}, dataProvider = "DP")
    public void resumeScheduledFeed(Bundle bundle) throws Exception {
        try {
            bundle.generateUniqueBundle();
            bundle = new Bundle(bundle, ivoryqa1.getEnvFileName());
            submitCluster(bundle);
            String feed = Util.getInputFeedFromBundle(bundle);

            Util.assertSucceeded(feedHelper.submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed));

            Assert.assertTrue(Util.getOozieFeedJobStatus(Util.readDatasetName(feed), "RUNNING", ivoryqa1)
                            .get(0)
                            .contains("RUNNING"));

            Util.assertSucceeded(feedHelper.resume(URLS.RESUME_URL, feed));


            ServiceResponse response = feedHelper.getStatus(URLS.STATUS_URL, feed);
            String colo = feedHelper.getColo();
            Assert.assertTrue(response.getMessage().contains(colo + "/RUNNING"));
            Assert.assertTrue(
                    Util.getOozieFeedJobStatus(Util.readDatasetName(feed), "RUNNING", ivoryqa1)
                            .get(0)
                            .contains("RUNNING"));

        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        } finally {
            feedHelper.delete(URLS.DELETE_URL, Util.getInputFeedFromBundle(bundle));
        }
    }


    @DataProvider(name = "DP")
    public Object[][] getData(Method m) throws Exception {
        return Util.readELBundles();
    }
}
