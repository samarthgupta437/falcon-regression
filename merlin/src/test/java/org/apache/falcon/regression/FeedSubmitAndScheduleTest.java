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

import junit.framework.Assert;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.helpers.PrismHelper;
import org.apache.falcon.regression.core.interfaces.EntityHelperFactory;
import org.apache.falcon.regression.core.interfaces.IEntityManagerHelper;
import org.apache.falcon.regression.core.response.APIResult;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.supportClasses.ENTITY_TYPE;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.oozie.client.Job;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;

/**
 * Feed submit and schedule tests.
 */
public class FeedSubmitAndScheduleTest {

    @BeforeMethod(alwaysRun = true)
    public void testName(Method method) {
        Util.print("test name: " + method.getName());
    }

    private final PrismHelper prismHelper = new PrismHelper("prism.properties");
    private final ColoHelper ivoryqa1 = new ColoHelper("ivoryqa-1.config.properties");


    @Test(groups = {"singleCluster"}, dataProvider = "DP")
    public void snsNewFeed(Bundle bundle) throws Exception {
        try {
            bundle = (Bundle) Util.readELBundles()[0][0];
            bundle = new Bundle(bundle, ivoryqa1.getEnvFileName());
            Assert.assertEquals(Util.parseResponse(
                    prismHelper.getClusterHelper()
                            .submitEntity(URLS.SUBMIT_URL, bundle.getClusters().get(0)))
                    .getStatusCode(), 200);
            ServiceResponse response = prismHelper.getFeedHelper()
                    .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundle.getDataSets().get(0));
            Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 200);
            Assert.assertEquals(Util.parseResponse(response).getStatus(),
                    APIResult.Status.SUCCEEDED);
            Assert.assertNotNull(Util.parseResponse(response).getMessage());
            TimeUnit.SECONDS.sleep(5);
        } finally {
            bundle.deleteBundle(prismHelper);
        }
    }

    @Test(groups = {"singleCluster"}, dataProvider = "DP")
    public void snsExistingFeed(Bundle bundle) throws Exception {
        try {
            bundle = (Bundle) Util.readELBundles()[0][0];
            bundle = new Bundle(bundle, ivoryqa1.getEnvFileName());
            Assert.assertEquals(Util.parseResponse(
                    prismHelper.getClusterHelper()
                            .submitEntity(URLS.SUBMIT_URL, bundle.getClusters().get(0)))
                    .getStatusCode(), 200);

            ServiceResponse response = prismHelper.getFeedHelper()
                    .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundle.getDataSets().get(0));


            Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 200);
            Assert.assertEquals(Util.parseResponse(response).getStatus(),
                    APIResult.Status.SUCCEEDED);
            Assert.assertNotNull(Util.parseResponse(response).getMessage());
            Assert.assertTrue(Util.verifyOozieJobStatus(ivoryqa1.getFeedHelper().getOozieClient(),
                    Util.readDatasetName(bundle.getDataSets().get(0)), ENTITY_TYPE.FEED, Job.Status.RUNNING));
            //try to submitand schedule the same process again
            response = prismHelper.getFeedHelper()
                    .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundle.getDataSets().get(0));

            Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 200);
            Assert.assertNotNull(Util.parseResponse(response).getMessage());
            Assert.assertTrue(Util.verifyOozieJobStatus(ivoryqa1.getFeedHelper().getOozieClient(),
                    Util.readDatasetName(bundle.getDataSets().get(0)), ENTITY_TYPE.FEED, Job.Status.RUNNING));
        } finally {
            bundle.deleteBundle(prismHelper);
        }

    }

    @Test(groups = {"singleCluster"}, dataProvider = "DP")
    public void snsFeedWithoutCluster(Bundle bundle) throws Exception {
        try {
            bundle = (Bundle) Util.readELBundles()[0][0];
            bundle = new Bundle(bundle, ivoryqa1.getEnvFileName());
            ServiceResponse response = prismHelper.getFeedHelper()
                    .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundle.getDataSets().get(0));

            Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 400);
            Assert.assertEquals(Util.parseResponse(response).getStatus(), APIResult.Status.FAILED);
            Assert.assertNotNull(Util.parseResponse(response).getMessage());


        } finally {
            bundle.deleteBundle(prismHelper);
        }

    }


    @Test(groups = {"singleCluster"}, dataProvider = "DP")
    public void snsRunningProcess(Bundle bundle) throws Exception {
        try {

            bundle = (Bundle) Util.readELBundles()[0][0];
            bundle = new Bundle(bundle, ivoryqa1.getEnvFileName());
            Assert.assertEquals(Util.parseResponse(
                    prismHelper.getClusterHelper()
                            .submitEntity(URLS.SUBMIT_URL, bundle.getClusters().get(0)))
                    .getStatusCode(), 200);

            ServiceResponse response = prismHelper.getFeedHelper()
                    .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundle.getDataSets().get(0));

            Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 200);
            Assert.assertEquals(Util.parseResponse(response).getStatus(),
                    APIResult.Status.SUCCEEDED);
            Assert.assertNotNull(Util.parseResponse(response).getMessage());
            Assert.assertTrue(Util.verifyOozieJobStatus(ivoryqa1.getFeedHelper().getOozieClient(),
                    Util.readDatasetName(bundle.getDataSets().get(0)), ENTITY_TYPE.FEED, Job.Status.RUNNING));
            response = prismHelper.getFeedHelper()
                    .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundle.getDataSets().get(0));

            Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 200);
            Assert.assertEquals(Util.parseResponse(response).getStatus(),
                    APIResult.Status.SUCCEEDED);
            Assert.assertNotNull(Util.parseResponse(response).getMessage());

        } finally {
            bundle.deleteBundle(prismHelper);
        }

    }


    @Test(groups = {"singleCluster"}, dataProvider = "DP")
    public void snsDeletedFeed(Bundle bundle) throws Exception {
        try {
            bundle = (Bundle) Util.readELBundles()[0][0];
            bundle = new Bundle(bundle, ivoryqa1.getEnvFileName());
            Assert.assertEquals(Util.parseResponse(
                    prismHelper.getClusterHelper()
                            .submitEntity(URLS.SUBMIT_URL, bundle.getClusters().get(0)))
                    .getStatusCode(), 200);


            ServiceResponse response = prismHelper.getFeedHelper()
                    .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundle.getDataSets().get(0));

            Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 200);
            Assert.assertEquals(Util.parseResponse(response).getStatus(),
                    APIResult.Status.SUCCEEDED);
            Assert.assertNotNull(Util.parseResponse(response).getMessage());
            Assert.assertTrue(Util.verifyOozieJobStatus(ivoryqa1.getFeedHelper().getOozieClient(),
                    Util.readDatasetName(bundle.getDataSets().get(0)), ENTITY_TYPE.FEED, Job.Status.RUNNING));
            Assert.assertEquals(
                    Util.parseResponse(prismHelper.getFeedHelper()
                            .delete(URLS.DELETE_URL, bundle.getDataSets().get(0)))
                            .getStatusCode(), 200);
            Assert.assertTrue(Util.verifyOozieJobStatus(ivoryqa1.getFeedHelper().getOozieClient(),
                    Util.readDatasetName(bundle.getDataSets().get(0)), ENTITY_TYPE.FEED, Job.Status.KILLED));
            response = prismHelper.getFeedHelper()
                    .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundle.getDataSets().get(0));

            Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 200);
            Assert.assertEquals(Util.parseResponse(response).getStatus(),
                    APIResult.Status.SUCCEEDED);
            Assert.assertNotNull(Util.parseResponse(response).getMessage());
            Assert.assertTrue(Util.verifyOozieJobStatus(ivoryqa1.getFeedHelper().getOozieClient(),
                    Util.readDatasetName(bundle.getDataSets().get(0)), ENTITY_TYPE.FEED, Job.Status.RUNNING));
        } finally {
            bundle.deleteBundle(prismHelper);
        }
    }


    @Test(groups = {"singleCluster"}, dataProvider = "DP")
    public void snsSuspendedFeed(Bundle bundle) throws Exception {
        try {
            bundle = (Bundle) Util.readELBundles()[0][0];
            bundle = new Bundle(bundle, ivoryqa1.getEnvFileName());
            Assert.assertEquals(Util.parseResponse(
                    prismHelper.getClusterHelper()
                            .submitEntity(URLS.SUBMIT_URL, bundle.getClusters().get(0)))
                    .getStatusCode(), 200);


            ServiceResponse response = prismHelper.getFeedHelper()
                    .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundle.getDataSets().get(0));

            Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 200);
            Assert.assertEquals(Util.parseResponse(response).getStatus(),
                    APIResult.Status.SUCCEEDED);
            Assert.assertNotNull(Util.parseResponse(response).getMessage());
            TimeUnit.SECONDS.sleep(20);
            Assert.assertTrue(Util.verifyOozieJobStatus(ivoryqa1.getFeedHelper().getOozieClient(),
                    Util.readDatasetName(bundle.getDataSets().get(0)), ENTITY_TYPE.FEED, Job.Status.RUNNING));
            Assert.assertEquals(Util.parseResponse(
                    prismHelper.getFeedHelper()
                            .suspend(URLS.SUSPEND_URL, bundle.getDataSets().get(0)))
                    .getStatusCode(),
                    200);
            Assert.assertTrue(Util.verifyOozieJobStatus(ivoryqa1.getFeedHelper().getOozieClient(),
                    Util.readDatasetName(bundle.getDataSets().get(0)), ENTITY_TYPE.FEED, Job.Status.SUSPENDED));
            response = prismHelper.getFeedHelper()
                    .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundle.getDataSets().get(0));

            Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 200);
            Assert.assertEquals(Util.parseResponse(response).getStatus(),
                    APIResult.Status.SUCCEEDED);
            Assert.assertNotNull(Util.parseResponse(response).getMessage());
            Assert.assertTrue(Util.verifyOozieJobStatus(ivoryqa1.getFeedHelper().getOozieClient(),
                    Util.readDatasetName(bundle.getDataSets().get(0)), ENTITY_TYPE.FEED, Job.Status.SUSPENDED));
        } finally {
            bundle.deleteBundle(prismHelper);
        }

    }


    @DataProvider(name = "DP")
    public Object[][] getBundleData() throws Exception {
        return Util.readELBundles();
    }
}



