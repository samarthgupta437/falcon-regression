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
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.helpers.PrismHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.core.util.XmlUtil;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

public class FeedRetentionTest {
    PrismHelper prismHelper = new PrismHelper("prism.properties");
    ColoHelper gs1001 = new ColoHelper("gs1001.config.properties");
    ColoHelper ivoryqa1 = new ColoHelper("ivoryqa-1.config.properties");


    @BeforeMethod(alwaysRun = true)
    public void testName(Method method) {
        Util.print("test name: " + method.getName());
    }

    @Test(enabled = true)
    public void testRetentionClickRC_2Colo() throws Throwable {

        //submit 2 clusters
        // submit and schedule feed on above 2 clusters, both having different locations
        // submit and schedule process having the above feed as output feed and running on 2
        // clusters


        //getImpressionRC bundle
        Bundle b1 = new Bundle();
        Bundle b2 = new Bundle();

        try {
            b1 = (Bundle) Bundle.readBundle("impressionRC")[0][0];
            b1.generateUniqueBundle();
            b1 = new Bundle(b1, gs1001.getEnvFileName());

            b2 = (Bundle) Bundle.readBundle("impressionRC")[0][0];
            b2.generateUniqueBundle();
            b2 = new Bundle(b2, ivoryqa1.getEnvFileName());


            InstanceUtil.putFileInFolders(gs1001,
                    InstanceUtil.createEmptyDirWithinDatesAndPrefix(gs1001,
                            InstanceUtil.oozieDateToDate(InstanceUtil.getTimeWrtSystemTime(-5)),
                            InstanceUtil.oozieDateToDate(InstanceUtil.getTimeWrtSystemTime(10)),
                            "/testInput/ivoryRetention01/data/", 1), "thriftRRMar0602.gz");
            InstanceUtil.putFileInFolders(ivoryqa1,
                    InstanceUtil.createEmptyDirWithinDatesAndPrefix(ivoryqa1,
                            InstanceUtil.oozieDateToDate(InstanceUtil.getTimeWrtSystemTime(-5)),
                            InstanceUtil.oozieDateToDate(InstanceUtil.getTimeWrtSystemTime(10)),
                            "/testInput/ivoryRetention01/data/", 1), "thriftRRMar0602.gz");

//			Bundle.submitCluster(b1,b2);
            prismHelper.getClusterHelper().submitEntity(URLS.SUBMIT_URL, b1.getClusters().get(0));
            prismHelper.getClusterHelper().submitEntity(URLS.SUBMIT_URL, b2.getClusters().get(0));

            String feedOutput01 = b1.getFeed("FETL-RequestRC");
            feedOutput01 = InstanceUtil
                    .setFeedCluster(feedOutput01,
                            XmlUtil.createValidity("2010-10-01T12:00Z", "2099-01-01T00:00Z"),
                            XmlUtil.createRtention("days(10000)", ActionType.DELETE), null,
                            ClusterType.SOURCE, null,
                            null);
            feedOutput01 = InstanceUtil
                    .setFeedCluster(feedOutput01,
                            XmlUtil.createValidity("2010-10-01T12:00Z", "2099-10-01T12:10Z"),
                            XmlUtil.createRtention("minutes(5)", ActionType.DELETE),
                            Util.readClusterName(b1.getClusters().get(0)), ClusterType.SOURCE,
                            "${cluster.colo}",
                            "/testOutput/op1/ivoryRetention01/data/${YEAR}/${MONTH}/${DAY}/${HOUR" +
                                    "}/${MINUTE}",
                            "/testOutput/op1/ivoryRetention01/stats/${YEAR}/${MONTH}/${DAY}/$" +
                                    "{HOUR}/${MINUTE}",
                            "/testOutput/op1/ivoryRetention01/meta/${YEAR}/${MONTH}/${DAY}/${HOUR" +
                                    "}/${MINUTE}",
                            "/testOutput/op1/ivoryRetention01/tmp/${YEAR}/${MONTH}/${DAY}/${HOUR" +
                                    "}/${MINUTE}");
            feedOutput01 = InstanceUtil
                    .setFeedCluster(feedOutput01,
                            XmlUtil.createValidity("2010-10-01T12:00Z", "2099-10-01T12:25Z"),
                            XmlUtil.createRtention("minutes(5)", ActionType.DELETE),
                            Util.readClusterName(b2.getClusters().get(0)), ClusterType.SOURCE,
                            "${cluster.colo}",
                            "/testOutput/op1/ivoryRetention02/data/${YEAR}/${MONTH}/${DAY}/${HOUR" +
                                    "}/${MINUTE}",
                            "/testOutput/op1/ivoryRetention02/stats/${YEAR}/${MONTH}/${DAY}/$" +
                                    "{HOUR}/${MINUTE}",
                            "/testOutput/op1/ivoryRetention02/meta/${YEAR}/${MONTH}/${DAY}/${HOUR" +
                                    "}/${MINUTE}",
                            "/testOutput/op1/ivoryRetention02/tmp/${YEAR}/${MONTH}/${DAY}/${HOUR" +
                                    "}/${MINUTE}");

            //submit the new output feed
            ServiceResponse r =
                    prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feedOutput01);
            Thread.sleep(10000);
            AssertUtil.assertSucceeded(r);


            String feedOutput02 = b1.getFeed("FETL-ImpressionRC");
            feedOutput02 = InstanceUtil
                    .setFeedCluster(feedOutput02,
                            XmlUtil.createValidity("2010-10-01T12:00Z", "2099-01-01T00:00Z"),
                            XmlUtil.createRtention("days(10000)", ActionType.DELETE), null,
                            ClusterType.SOURCE, null,
                            null);
            feedOutput02 = InstanceUtil
                    .setFeedCluster(feedOutput02,
                            XmlUtil.createValidity("2010-10-01T12:00Z", "2099-10-01T12:10Z"),
                            XmlUtil.createRtention("minutes(5)", ActionType.DELETE),
                            Util.readClusterName(b1.getClusters().get(0)), ClusterType.SOURCE,
                            "${cluster.colo}",
                            "/testOutput/op2/ivoryRetention01/data/${YEAR}/${MONTH}/${DAY}/${HOUR" +
                                    "}/${MINUTE}",
                            "/testOutput/op2/ivoryRetention01/stats/${YEAR}/${MONTH}/${DAY}/$" +
                                    "{HOUR}/${MINUTE}",
                            "/testOutput/op2/ivoryRetention01/meta/${YEAR}/${MONTH}/${DAY}/${HOUR" +
                                    "}/${MINUTE}",
                            "/testOutput/op2/ivoryRetention01/tmp/${YEAR}/${MONTH}/${DAY}/${HOUR" +
                                    "}/${MINUTE}");
            feedOutput02 = InstanceUtil
                    .setFeedCluster(feedOutput02,
                            XmlUtil.createValidity("2010-10-01T12:00Z", "2099-10-01T12:25Z"),
                            XmlUtil.createRtention("minutes(5)", ActionType.DELETE),
                            Util.readClusterName(b2.getClusters().get(0)), ClusterType.SOURCE,
                            "${cluster.colo}",
                            "/testOutput/op2/ivoryRetention02/data/${YEAR}/${MONTH}/${DAY}/${HOUR" +
                                    "}/${MINUTE}",
                            "/testOutput/op2/ivoryRetention02/stats/${YEAR}/${MONTH}/${DAY}/$" +
                                    "{HOUR}/${MINUTE}",
                            "/testOutput/op2/ivoryRetention02/meta/${YEAR}/${MONTH}/${DAY}/${HOUR" +
                                    "}/${MINUTE}",
                            "/testOutput/op2/ivoryRetention02/tmp/${YEAR}/${MONTH}/${DAY}/${HOUR" +
                                    "}/${MINUTE}");

            //submit the new output feed
            r = prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feedOutput02);
            Thread.sleep(10000);
            AssertUtil.assertSucceeded(r);


            String feedInput = b1.getFeed("FETL2-RRLog");
            feedInput = InstanceUtil
                    .setFeedCluster(feedInput,
                            XmlUtil.createValidity("2010-10-01T12:00Z", "2099-01-01T00:00Z"),
                            XmlUtil.createRtention("days(10000)", ActionType.DELETE), null,
                            ClusterType.SOURCE, null,
                            null);

            feedInput = InstanceUtil
                    .setFeedCluster(feedInput,
                            XmlUtil.createValidity("2010-10-01T12:00Z", "2099-10-01T12:10Z"),
                            XmlUtil.createRtention("minutes(5)", ActionType.DELETE),
                            Util.readClusterName(b1.getClusters().get(0)), ClusterType.SOURCE,
                            "${cluster.colo}",
                            "/testInput/ivoryRetention01/data/${YEAR}/${MONTH}/${DAY}/${HOUR}/$" +
                                    "{MINUTE}");
            feedInput = InstanceUtil
                    .setFeedCluster(feedInput,
                            XmlUtil.createValidity("2010-10-01T12:00Z", "2099-10-01T12:25Z"),
                            XmlUtil.createRtention("minutes(5)", ActionType.DELETE),
                            Util.readClusterName(b2.getClusters().get(0)), ClusterType.SOURCE,
                            "${cluster.colo}",
                            "/testInput/ivoryRetention01/data/${YEAR}/${MONTH}/${DAY}/${HOUR}/$" +
                                    "{MINUTE}");

            r = prismHelper.getFeedHelper()
                    .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feedInput);

            Thread.sleep(10000);
            AssertUtil.assertSucceeded(r);


            String process = b1.getProcessData();
            process = InstanceUtil.setProcessCluster(process, null,
                    XmlUtil.createProcessValidity("2012-10-01T12:00Z", "2012-10-01T12:10Z"));

            process = InstanceUtil
                    .setProcessCluster(process, Util.readClusterName(b1.getClusters().get(0)),
                            XmlUtil.createProcessValidity(InstanceUtil.getTimeWrtSystemTime(-2),
                                    InstanceUtil.getTimeWrtSystemTime(5)));
            process = InstanceUtil
                    .setProcessCluster(process, Util.readClusterName(b2.getClusters().get(0)),
                            XmlUtil.createProcessValidity(InstanceUtil.getTimeWrtSystemTime(-2),
                                    InstanceUtil.getTimeWrtSystemTime(5)));

            Util.print("process: " + process);


            r = prismHelper.getProcessHelper()
                    .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, process);
            //	Thread.sleep(10000);
            AssertUtil.assertSucceeded(r);


            r = prismHelper.getFeedHelper().schedule(URLS.SCHEDULE_URL, feedOutput01);
            r = prismHelper.getFeedHelper().schedule(URLS.SCHEDULE_URL, feedOutput02);

        } finally {
            prismHelper.getProcessHelper().delete(URLS.DELETE_URL, b1.getProcessData());
            prismHelper.getFeedHelper().delete(URLS.DELETE_URL, b1.getDataSets().get(0));
            prismHelper.getFeedHelper().delete(URLS.DELETE_URL, b1.getDataSets().get(1));
            Bundle.deleteCluster(b1, b2);
        }


    }

}
