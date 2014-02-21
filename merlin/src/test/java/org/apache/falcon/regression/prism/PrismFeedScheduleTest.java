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
import org.apache.falcon.regression.core.supportClasses.ENTITY_TYPE;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.testng.TestNGException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

public class PrismFeedScheduleTest extends BaseTestClass {

    ColoHelper cluster1;
    ColoHelper cluster2;
    OozieClient cluster1OC;
    OozieClient cluster2OC;

    public PrismFeedScheduleTest(){
        super();
        cluster1 = servers.get(0);
        cluster2 = servers.get(1);
        cluster1OC = serverOC.get(0);
        cluster2OC = serverOC.get(1);
    }

    @BeforeMethod(alwaysRun = true)
    public void testName(Method method) {
        Util.print("test name: " + method.getName());
    }

    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testFeedScheduleOn1ColoWhileAnotherColoHasSuspendedFeed(Bundle bundle)
    throws Exception {
        try {
            Bundle UA1Bundle = new Bundle(bundle, cluster1.getEnvFileName(), cluster1.getPrefix());
            Bundle UA2Bundle = new Bundle(bundle, cluster2.getEnvFileName(), cluster2.getPrefix());

            UA1Bundle.generateUniqueBundle();
            UA2Bundle.generateUniqueBundle();

            System.out.println("cluster: " + UA1Bundle.getClusters().get(0));
            System.out.println("feed: " + UA1Bundle.getDataSets().get(0));

            UA1Bundle.submitAndScheduleFeed();
            Util.assertSucceeded(prism.getFeedHelper()
                    .suspend(URLS.SUSPEND_URL, UA1Bundle.getDataSets().get(0)));
            AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.FEED, UA1Bundle, Job.Status.SUSPENDED);
            UA2Bundle.submitAndScheduleFeed();
            AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.FEED, UA2Bundle, Job.Status.RUNNING);
            AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);
            AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.FEED, UA1Bundle, Job.Status.SUSPENDED);
            AssertUtil.checkNotStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        }

    }


    @DataProvider(name = "DP")
    public Object[][] getData() throws Exception {
        return Util.readBundles("LateDataBundles");
    }
}
