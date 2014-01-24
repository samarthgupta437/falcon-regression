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
import org.apache.falcon.regression.core.generated.feed.ActionType;
import org.apache.falcon.regression.core.generated.feed.ClusterType;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.core.util.XmlUtil;
import org.apache.falcon.regression.testHelper.BaseMultiClusterTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

public class FeedDelayParallelTimeoutTest extends BaseMultiClusterTests {

    String baseTestDir = baseHDFSDir + "/FeedDelayParallelTimeoutTest";
    String feedInputPath = baseTestDir + "/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/";
    String feedOutputPath = baseTestDir + "Target/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/";
    Bundle b1 = new Bundle();
    Bundle b2 = new Bundle();


    @BeforeMethod(alwaysRun = true)
    public void setup(Method method) throws Exception {
        Util.print("test name: " + method.getName());
        b1 = (Bundle) Util.readELBundles()[0][0];
        b1.generateUniqueBundle();
        b2 = (Bundle) Util.readELBundles()[0][0];
        b2.generateUniqueBundle();
        b1 = new Bundle(b1, server3.getEnvFileName(), server3.getPrefix());
        b2 = new Bundle(b2, server2.getEnvFileName(), server2.getPrefix());
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        b1.deleteBundle(prism);
    }

    @Test(enabled = true, timeOut = 12000000)
    public void timeoutTest() throws Exception {
        b1.setInputFeedDataPath(feedInputPath);

        Bundle.submitCluster(b1, b2);
        String feedOutput01 = b1.getDataSets().get(0);
        org.apache.falcon.regression.core.generated.dependencies.Frequency delay =
                new org.apache.falcon.regression.core.generated.dependencies.Frequency(
                        "hours(5)");

        feedOutput01 = InstanceUtil
                .setFeedCluster(feedOutput01,
                        XmlUtil.createValidity("2010-10-01T12:00Z", "2099-01-01T00:00Z"),
                        XmlUtil.createRtention("days(10000)", ActionType.DELETE), null,
                        ClusterType.SOURCE, null,
                        null);

        // uncomment below 2 line when falcon in sync with ivory

        //	feedOutput01 = instanceUtil.setFeedCluster(feedOutput01,
        // XmlUtil.createValidity("2013-04-21T00:00Z",
        // "2099-10-01T12:10Z"),XmlUtil.createRtention("hours(15)",ActionType.DELETE),
        // Util.readClusterName(b2.getClusters().get(0)),ClusterType.SOURCE,"",delay,
        // feedInputPath);
        //	feedOutput01 = instanceUtil.setFeedCluster(feedOutput01,
        // XmlUtil.createValidity("2013-04-21T00:00Z",
        // "2099-10-01T12:25Z"),XmlUtil.createRtention("hours(15)",ActionType.DELETE),
        // Util.readClusterName(b1.getClusters().get(0)),ClusterType.TARGET,"",delay,
        // feedOutputPath);

        //feedOutput01 = instanceUtil.setFeedCluster(feedOutput01,
        // XmlUtil.createValidity("2013-04-21T00:00Z",
        // "2099-10-01T12:10Z"),XmlUtil.createRtention("hours(15)",ActionType.DELETE),
        // Util.readClusterName(b2.getClusters().get(0)),ClusterType.SOURCE,"",
        // feedInputPath);
        //feedOutput01 = instanceUtil.setFeedCluster(feedOutput01,
        // XmlUtil.createValidity("2013-04-21T00:00Z",
        // "2099-10-01T12:25Z"),XmlUtil.createRtention("hours(15)",ActionType.DELETE),
        // Util.readClusterName(b1.getClusters().get(0)),ClusterType.TARGET,"",
        // feedOutputPath);

        feedOutput01 = Util.setFeedProperty(feedOutput01, "timeout", "minutes(35)");
        feedOutput01 = Util.setFeedProperty(feedOutput01, "parallel", "3");

        System.out.println(feedOutput01);
        prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feedOutput01);
    }
}
