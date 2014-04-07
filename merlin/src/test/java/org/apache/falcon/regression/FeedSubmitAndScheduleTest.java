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
import org.apache.falcon.regression.core.response.APIResult;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.enumsAndConstants.ENTITY_TYPE;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

/**
 * Feed submit and schedule tests.
 */
@Test(groups = "embedded")
public class FeedSubmitAndScheduleTest extends BaseTestClass {

    ColoHelper cluster = servers.get(0);
    OozieClient clusterOC = serverOC.get(0);
    String aggregateWorkflowDir = baseHDFSDir + "/FeedSubmitAndScheduleTest/aggregator";
    private static final Logger logger = Logger.getLogger(FeedSubmitAndScheduleTest.class);

    public void uploadWorkflow() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        logger.info("test name: " + method.getName());
        bundles[0] = Util.readELBundles()[0][0];
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].generateUniqueBundle();
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
    }

    @AfterMethod
    public void tearDown() throws Exception {
        removeBundles();
    }

    @Test(groups = {"singleCluster"})
    public void snsNewFeed() throws Exception {
        submitFirstClusterScheduleFirstFeed();
        TimeUnit.SECONDS.sleep(5);
    }

    private void submitFirstClusterScheduleFirstFeed()
    throws JAXBException, IOException, URISyntaxException, AuthenticationException {
        Assert.assertEquals(Util.parseResponse(prism.getClusterHelper()
                .submitEntity(URLS.SUBMIT_URL, bundles[0].getClusters().get(0))).getStatusCode(), 200);
        ServiceResponse response = prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[0].getDataSets().get(0));
        Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 200);
        Assert.assertEquals(Util.parseResponse(response).getStatus(),
                APIResult.Status.SUCCEEDED);
        Assert.assertNotNull(Util.parseResponse(response).getMessage());
    }

    @Test(groups = {"singleCluster"})
    public void snsExistingFeed() throws Exception {
        submitFirstClusterScheduleFirstFeed();
        AssertUtil.checkStatus(clusterOC, ENTITY_TYPE.FEED, bundles[0], Job.Status.RUNNING);
        //try to submitand schedule the same process again
        ServiceResponse response = prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[0].getDataSets().get(0));

        Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 200);
        Assert.assertNotNull(Util.parseResponse(response).getMessage());
        AssertUtil.checkStatus(clusterOC, ENTITY_TYPE.FEED, bundles[0], Job.Status.RUNNING);
    }

    @Test(groups = {"singleCluster"})
    public void snsFeedWithoutCluster() throws Exception {
        ServiceResponse response = prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[0].getDataSets().get(0));

        Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 400);
        Assert.assertEquals(Util.parseResponse(response).getStatus(), APIResult.Status.FAILED);
        Assert.assertNotNull(Util.parseResponse(response).getMessage());
    }


    @Test(groups = {"singleCluster"})
    public void snsRunningProcess() throws Exception {
        submitFirstClusterScheduleFirstFeed();
        AssertUtil.checkStatus(clusterOC, ENTITY_TYPE.FEED, bundles[0], Job.Status.RUNNING);
        ServiceResponse response = prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[0].getDataSets().get(0));

        Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 200);
        Assert.assertEquals(Util.parseResponse(response).getStatus(),
                APIResult.Status.SUCCEEDED);
        Assert.assertNotNull(Util.parseResponse(response).getMessage());
    }


    @Test(groups = {"singleCluster"})
    public void snsDeletedFeed() throws Exception {
        submitFirstClusterScheduleFirstFeed();
        AssertUtil.checkStatus(clusterOC, ENTITY_TYPE.FEED, bundles[0], Job.Status.RUNNING);
        Assert.assertEquals(
                Util.parseResponse(prism.getFeedHelper()
                        .delete(URLS.DELETE_URL, bundles[0].getDataSets().get(0)))
                        .getStatusCode(), 200);
        AssertUtil.checkStatus(clusterOC, ENTITY_TYPE.FEED, bundles[0], Job.Status.KILLED);
        ServiceResponse response = prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[0].getDataSets().get(0));

        Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 200);
        Assert.assertEquals(Util.parseResponse(response).getStatus(),
                APIResult.Status.SUCCEEDED);
        Assert.assertNotNull(Util.parseResponse(response).getMessage());
        AssertUtil.checkStatus(clusterOC, ENTITY_TYPE.FEED, bundles[0], Job.Status.RUNNING);
    }


    @Test(groups = {"singleCluster"})
    public void snsSuspendedFeed() throws Exception {
        submitFirstClusterScheduleFirstFeed();
        TimeUnit.SECONDS.sleep(20);
        AssertUtil.checkStatus(clusterOC, ENTITY_TYPE.FEED, bundles[0], Job.Status.RUNNING);
        Assert.assertEquals(Util.parseResponse(
                prism.getFeedHelper()
                        .suspend(URLS.SUSPEND_URL, bundles[0].getDataSets().get(0)))
                .getStatusCode(),
                200);
        AssertUtil.checkStatus(clusterOC, ENTITY_TYPE.FEED, bundles[0], Job.Status.SUSPENDED);
        ServiceResponse response = prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[0].getDataSets().get(0));

        Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 200);
        Assert.assertEquals(Util.parseResponse(response).getStatus(),
                APIResult.Status.SUCCEEDED);
        Assert.assertNotNull(Util.parseResponse(response).getMessage());
        AssertUtil.checkStatus(clusterOC, ENTITY_TYPE.FEED, bundles[0], Job.Status.SUSPENDED);
    }
}



