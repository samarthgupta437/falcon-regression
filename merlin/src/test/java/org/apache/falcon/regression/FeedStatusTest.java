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
import org.apache.falcon.regression.core.supportClasses.ENTITY_TYPE;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.oozie.client.Job;
import org.testng.Assert;
import org.testng.TestNGException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

/**
 * Feed status tests.
 */
public class FeedStatusTest {
    private final PrismHelper prismHelper = new PrismHelper("prism.properties");
    private final ColoHelper ivoryqa1 = new ColoHelper("ivoryqa-1.config.properties");

    @BeforeMethod(alwaysRun = true)
    public void testName(Method method) {
        Util.print("test name: " + method.getName());
    }

    public void submitCluster(Bundle bundle) throws Exception {

        ServiceResponse response = prismHelper.getClusterHelper().submitEntity(
                URLS.SUBMIT_URL, bundle.getClusters().get(0));

        Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 200);
        Assert.assertNotNull(Util.parseResponse(response).getMessage());
    }


    @Test(groups = {"singleCluster"}, dataProvider = "DP")
    public void getStatusForScheduledFeed(Bundle bundle) throws Exception {
        try {
            bundle.generateUniqueBundle();
            bundle = new Bundle(bundle, ivoryqa1.getEnvFileName());
            submitCluster(bundle);
            String feed = Util.getInputFeedFromBundle(bundle);
            ServiceResponse response =
                    prismHelper.getFeedHelper()
                            .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed);
            Util.print(feed);
            Util.assertSucceeded(response);

            response = prismHelper.getFeedHelper().getStatus(URLS.STATUS_URL, feed);

            Util.assertSucceeded(response);

            Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 200);
            Assert.assertNotNull(Util.parseResponse(response).getMessage());

            String colo = prismHelper.getFeedHelper().getColo();
            Assert.assertTrue(response.getMessage().contains(colo + "/RUNNING"));
            Assert.assertTrue(Util.verifyOozieJobStatus(ivoryqa1.getFeedHelper().getOozieClient(),
                    Util.readDatasetName(feed), ENTITY_TYPE.FEED, Job.Status.RUNNING));
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        } finally {

            prismHelper.getFeedHelper()
                    .delete(URLS.DELETE_URL, Util.getInputFeedFromBundle(bundle));
        }
    }


    @Test(groups = {"singleCluster"}, dataProvider = "DP")
    public void getStatusForSuspendedFeed(Bundle bundle) throws Exception {
        try {
            bundle.generateUniqueBundle();
            bundle = new Bundle(bundle, ivoryqa1.getEnvFileName());
            submitCluster(bundle);
            String feed = Util.getInputFeedFromBundle(bundle);
            ServiceResponse response =
                    prismHelper.getFeedHelper()
                            .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed);

            Util.assertSucceeded(response);

            response = prismHelper.getFeedHelper().suspend(URLS.SUSPEND_URL, feed);
            Util.assertSucceeded(response);

            response = prismHelper.getFeedHelper().getStatus(URLS.STATUS_URL, feed);

            Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 200);
            Assert.assertNotNull(Util.parseResponse(response).getMessage());
            String colo = prismHelper.getFeedHelper().getColo();
            Assert.assertTrue(response.getMessage().contains(colo + "/SUSPENDED"));
            Assert.assertTrue(Util.verifyOozieJobStatus(ivoryqa1.getFeedHelper().getOozieClient(),
                    Util.readDatasetName(feed), ENTITY_TYPE.FEED, Job.Status.SUSPENDED));
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        } finally {

            prismHelper.getFeedHelper()
                    .delete(URLS.DELETE_URL, Util.getInputFeedFromBundle(bundle));
        }
    }


    @Test(groups = {"singleCluster"}, dataProvider = "DP")
    public void getStatusForSubmittedFeed(Bundle bundle) throws Exception {
        try {
            bundle.generateUniqueBundle();
            bundle = new Bundle(bundle, ivoryqa1.getEnvFileName());
            submitCluster(bundle);
            String feed = Util.getInputFeedFromBundle(bundle);
            ServiceResponse response =
                    prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);

            Util.assertSucceeded(response);

            response = prismHelper.getFeedHelper().getStatus(URLS.STATUS_URL, feed);

            Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 200);
            Assert.assertNotNull(Util.parseResponse(response).getMessage());
            String colo = prismHelper.getFeedHelper().getColo();
            Assert.assertTrue(response.getMessage().contains(colo + "/SUBMITTED"));
            Assert.assertTrue(Util.getOozieJobStatus(ivoryqa1.getFeedHelper().getOozieClient(),
                    Util.readDatasetName(feed), ENTITY_TYPE.FEED) != Job.Status.RUNNING);
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        } finally {

            prismHelper.getFeedHelper()
                    .delete(URLS.DELETE_URL, Util.getInputFeedFromBundle(bundle));
        }
    }


    @Test(groups = {"singleCluster"}, dataProvider = "DP")
    public void getStatusForDeletedFeed(Bundle bundle) throws Exception {
        try {
            bundle.generateUniqueBundle();
            bundle = new Bundle(bundle, ivoryqa1.getEnvFileName());

            submitCluster(bundle);
            String feed = Util.getInputFeedFromBundle(bundle);
            ServiceResponse response =
                    prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
            Util.assertSucceeded(response);

            response = prismHelper.getFeedHelper().delete(URLS.DELETE_URL, feed);
            Util.assertSucceeded(response);

            response = prismHelper.getFeedHelper().getStatus(URLS.STATUS_URL, feed);
            Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 400);
            Assert.assertNotNull(Util.parseResponse(response).getMessage());

            Assert.assertTrue(
                    response.getMessage().contains(Util.getFeedName(feed) + " (FEED) not found"));
            Assert.assertTrue(Util.getOozieJobStatus(ivoryqa1.getFeedHelper().getOozieClient(),
                    Util.readDatasetName(feed), ENTITY_TYPE.FEED) != Job.Status.KILLED);
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        } finally {

            prismHelper.getFeedHelper()
                    .delete(URLS.DELETE_URL, Util.getInputFeedFromBundle(bundle));
        }
    }


    @Test(groups = {"singleCluster"}, dataProvider = "DP")
    public void getStatusForNonExistentFeed(Bundle bundle) throws Exception {

        bundle.generateUniqueBundle();
        bundle = new Bundle(bundle, ivoryqa1.getEnvFileName());
        submitCluster(bundle);
        String feed = Util.getInputFeedFromBundle(bundle);
        ServiceResponse response = prismHelper.getFeedHelper().getStatus(URLS.STATUS_URL, feed);
        Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 400);
        Assert.assertNotNull(Util.parseResponse(response).getMessage());
        Assert.assertTrue(
                response.getMessage().contains(Util.getFeedName(feed) + " (FEED) not found"));

    }

    @DataProvider(name = "DP")
    public Object[][] getData(Method m) throws Exception {
        return Util.readELBundles();
    }
}
