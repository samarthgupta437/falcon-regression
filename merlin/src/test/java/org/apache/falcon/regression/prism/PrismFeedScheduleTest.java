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

package org.apache.falcon.regression.prism;

import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.supportClasses.ENTITY_TYPE;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.testHelper.BaseMultiClusterTests;
import org.apache.oozie.client.Job;
import org.testng.Assert;
import org.testng.TestNGException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

public class PrismFeedScheduleTest extends BaseMultiClusterTests{

    @BeforeMethod(alwaysRun = true)
    public void testName(Method method) {
        Util.print("test name: " + method.getName());
    }

    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testFeedScheduleOn1ColoWhileAnotherColoHasSuspendedFeed(Bundle bundle)
    throws Exception {
        try {
            Bundle UA1Bundle = new Bundle(bundle, server1.getEnvFileName());
            Bundle UA2Bundle = new Bundle(bundle, server1.getEnvFileName());

            UA1Bundle.generateUniqueBundle();
            UA2Bundle.generateUniqueBundle();

            System.out.println("cluster: " + UA1Bundle.getClusters().get(0));
            System.out.println("feed: " + UA1Bundle.getDataSets().get(0));

            submitAndScheduleFeed(UA1Bundle);
            Util.assertSucceeded(prism.getFeedHelper()
                    .suspend(URLS.SUSPEND_URL, UA1Bundle.getDataSets().get(0)));
            Assert.assertTrue(Util.verifyOozieJobStatus(server1.getFeedHelper().getOozieClient(),
                    Util.readDatasetName(UA1Bundle.getDataSets().get(0)), ENTITY_TYPE.FEED, Job.Status.SUSPENDED));
            submitAndScheduleFeed(UA2Bundle);
            Assert.assertTrue(Util.verifyOozieJobStatus(server1.getFeedHelper().getOozieClient(),
                    Util.readDatasetName(UA2Bundle.getDataSets().get(0)), ENTITY_TYPE.FEED, Job.Status.RUNNING));
            Assert.assertTrue(Util.getOozieJobStatus(server1.getFeedHelper().getOozieClient(),
                    Util.readDatasetName(UA1Bundle.getDataSets().get(0)), ENTITY_TYPE.PROCESS) != Job.Status.RUNNING);
            Assert.assertTrue(Util.verifyOozieJobStatus(server1.getFeedHelper().getOozieClient(),
                    Util.readDatasetName(UA1Bundle.getDataSets().get(0)), ENTITY_TYPE.FEED, Job.Status.SUSPENDED));
            Assert.assertTrue(Util.getOozieJobStatus(server1.getFeedHelper().getOozieClient(),
                    Util.readDatasetName(UA2Bundle.getDataSets().get(0)), ENTITY_TYPE.PROCESS) != Job.Status.RUNNING);
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        }

    }

    private void submitFeed(Bundle bundle) throws Exception {
        for (String cluster : bundle.getClusters()) {
            Util.assertSucceeded(
                    prism.getClusterHelper().submitEntity(Util.URLS.SUBMIT_URL, cluster));
        }
        Util.assertSucceeded(
                prism.getFeedHelper()
                        .submitEntity(Util.URLS.SUBMIT_URL, bundle.getDataSets().get(0)));
    }


    private void submitAndScheduleFeed(Bundle bundle) throws Exception {
        submitFeed(bundle);
        Util.assertSucceeded(prism.getFeedHelper()
                .schedule(Util.URLS.SCHEDULE_URL, bundle.getDataSets().get(0)));
    }

    @DataProvider(name = "DP")
    public Object[][] getData() throws Exception {
        return Util.readBundles("LateDataBundles");
    }
}
