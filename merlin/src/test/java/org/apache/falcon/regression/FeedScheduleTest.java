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

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.falcon.regression;

import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.helpers.PrismHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.testng.Assert;
import org.testng.TestNGException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

/**
 * Feed schedule tests.
 */
public class FeedScheduleTest {
    private final PrismHelper prismHelper = new PrismHelper("prism.properties");
    private final ColoHelper ivoryqa1 = new ColoHelper("ivoryqa-1.config.properties");


    @BeforeMethod(alwaysRun = true)
    public void testName(Method method) {
        Util.print("test name: " + method.getName());
    }


    @Test(groups = {"singleCluster"})
    public void scheduleAlreadyScheduledFeed() {
        Bundle bundle = new Bundle();
        try {

            bundle = (Bundle) Util.readELBundles()[0][0];
            bundle = new Bundle(bundle, ivoryqa1.getEnvFileName());
            Bundle.submitCluster(bundle);
            String feed = Util.getInputFeedFromBundle(bundle);

            ServiceResponse response = prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
            Util.assertSucceeded(response);

            response = prismHelper.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);
            Util.assertSucceeded(response);

            Assert.assertTrue(
                    Util.getOozieFeedJobStatus(Util.readDatasetName(feed), "RUNNING", ivoryqa1)
                            .get(0)
                            .contains("RUNNING"));

            //now try re-scheduling again
            response = prismHelper.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);
            AssertUtil.assertSucceeded(response);

        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        } finally {

            try {

                prismHelper.getFeedHelper()
                        .delete(URLS.DELETE_URL, Util.getInputFeedFromBundle(bundle));
            } catch (Exception e) {
            }
        }
    }


    @Test(groups = {"singleCluster"})
    public void scheduleValidFeed() throws Exception {
        Bundle bundle = new Bundle();
        try {

            bundle = (Bundle) Util.readELBundles()[0][0];
            bundle = new Bundle(bundle, ivoryqa1.getEnvFileName());
            Bundle.submitCluster(bundle);
            String feed = Util.getInputFeedFromBundle(bundle);
            //submit feed
            ServiceResponse response = prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
            Util.assertSucceeded(response);

            //now schedule the thing
            response = prismHelper.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);
            Util.assertSucceeded(response);

            Assert.assertTrue(
                    Util.getOozieFeedJobStatus(Util.readDatasetName(feed), "RUNNING", ivoryqa1)
                            .get(0)
                            .contains("RUNNING"));
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        } finally {
            prismHelper.getFeedHelper()
                    .delete(URLS.DELETE_URL, Util.getInputFeedFromBundle(bundle));
        }
    }


    @Test(groups = {"singleCluster"})
    public void scheduleSuspendedFeed() throws Exception {
        Bundle bundle = new Bundle();
        try {

            bundle = (Bundle) Util.readELBundles()[0][0];
            bundle = new Bundle(bundle, ivoryqa1.getEnvFileName());
            Bundle.submitCluster(bundle);
            String feed = Util.getInputFeedFromBundle(bundle);
            Util.assertSucceeded(prismHelper.getFeedHelper()
                    .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed));

            //now suspend
            Util.assertSucceeded(prismHelper.getFeedHelper().suspend(URLS.SUSPEND_URL, feed));
            Assert.assertTrue(
                    Util.getOozieFeedJobStatus(Util.readDatasetName(feed), "SUSPENDED", ivoryqa1)
                            .get(0)
                            .contains("SUSPENDED"));

            //now schedule this!
            Util.assertSucceeded(prismHelper.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed));
            Assert.assertTrue(
                    Util.getOozieFeedJobStatus(Util.readDatasetName(feed), "SUSPENDED", ivoryqa1)
                            .get(0)
                            .contains("SUSPENDED"));

        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        } finally {
            prismHelper.getFeedHelper().delete(URLS.DELETE_URL, Util.getInputFeedFromBundle(bundle));
        }
    }

    @Test(groups = {"singleCluster"})
    public void scheduleKilledFeed() throws Exception {
        Bundle bundle = new Bundle();
        try {

            bundle = (Bundle) Util.readELBundles()[0][0];
            bundle = new Bundle(bundle, ivoryqa1.getEnvFileName());
            Bundle.submitCluster(bundle);
            String feed = Util.getInputFeedFromBundle(bundle);
            Util.assertSucceeded(prismHelper.getFeedHelper().submitAndSchedule(
                    URLS.SUBMIT_AND_SCHEDULE_URL, feed));

            //now suspend
            Util.assertSucceeded(prismHelper.getFeedHelper().delete(URLS.DELETE_URL, feed));
            Assert.assertTrue(
                    Util.getOozieFeedJobStatus(Util.readDatasetName(feed), "KILLED", ivoryqa1)
                            .get(0)
                            .contains("KILLED"));

            //now schedule this!
            Util.assertFailed(prismHelper.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed));

        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        } finally {
            prismHelper.getFeedHelper()
                    .delete(URLS.DELETE_URL, Util.getInputFeedFromBundle(bundle));
        }
    }

    @Test(groups = {"singleCluster"}, dataProvider = "DP", dataProviderClass = FeedSubmitTest.class)
    public void scheduleNonExistentFeed(Bundle bundle) throws Exception {
        bundle.generateUniqueBundle();
        bundle = new Bundle(bundle, ivoryqa1.getEnvFileName());
        Bundle.submitCluster(bundle);
        String feed = Util.getInputFeedFromBundle(bundle);
        Util.assertFailed(prismHelper.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed));
    }
}
