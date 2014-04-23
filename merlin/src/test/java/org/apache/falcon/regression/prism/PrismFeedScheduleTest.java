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
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.enumsAndConstants.ENTITY_TYPE;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.log4j.Logger;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.lang.reflect.Method;

@Test(groups = "embedded")
public class PrismFeedScheduleTest extends BaseTestClass {

    ColoHelper cluster1 = servers.get(0);
    ColoHelper cluster2 = servers.get(1);
    OozieClient cluster1OC = serverOC.get(0);
    OozieClient cluster2OC = serverOC.get(1);
    String aggregateWorkflowDir = baseHDFSDir + "/PrismFeedScheduleTest/aggregator";
    private static final Logger logger = Logger.getLogger(PrismFeedScheduleTest.class);

    @BeforeClass
    public void uploadWorkflow() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws IOException, JAXBException {
        logger.info("test name: " + method.getName());
        Bundle bundle = BundleUtil.readBundles("LateDataBundles")[0][0];

        for (int i = 0; i < 2; i++) {
            bundles[i] = new Bundle(bundle, servers.get(i));
            bundles[i].generateUniqueBundle();
            bundles[i].setProcessWorkflow(aggregateWorkflowDir);
        }
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        removeBundles();
    }

    @Test(groups = {"prism", "0.2"})
    public void testFeedScheduleOn1ColoWhileAnotherColoHasSuspendedFeed()
    throws Exception {
        logger.info("cluster: " + bundles[0].getClusters().get(0));
        logger.info("feed: " + bundles[0].getDataSets().get(0));

        bundles[0].submitAndScheduleFeed();
        AssertUtil.assertSucceeded(prism.getFeedHelper()
                .suspend(URLS.SUSPEND_URL, bundles[0].getDataSets().get(0)));
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.FEED, bundles[0], Job.Status.SUSPENDED);
        bundles[1].submitAndScheduleFeed();
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.FEED, bundles[1], Job.Status.RUNNING);
        AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.FEED, bundles[0], Job.Status.SUSPENDED);
        AssertUtil.checkNotStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);
    }
}
