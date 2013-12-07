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
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.XmlUtil;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;


public class PrismFeedUpdateTest {


    PrismHelper prismHelper = new PrismHelper("prism.properties");

    ColoHelper ua1 = new ColoHelper("mk-qa.config.properties");

    ColoHelper ua2 = new ColoHelper("ivoryqa-1.config.properties");

    ColoHelper ua3 = new ColoHelper("gs1001.config.properties");

    @Test(enabled = true, timeOut = 1200000)
    public void updateFeedQueue_dependentMultipleProcess_oneProcessZeroInput() throws Exception {
        //ua1 and ua3 are source. feed01 on ua1 target ua3, feed02 on ua3 target ua1

        //get 3 unique bundles
        Bundle b1 = (Bundle) Util.readELBundles()[0][0];
        b1.generateUniqueBundle();

        Bundle b3 = (Bundle) Util.readELBundles()[0][0];
        b3.generateUniqueBundle();

        try {
            //generate bundles according to config files
            b1 = new Bundle(b1, ua1.getEnvFileName());
            b3 = new Bundle(b3, ua3.getEnvFileName());

            //set cluster colos
            b1.setCLusterColo("ua1");
            Util.print("cluster b1: " + b1.getClusters().get(0));

            b3.setCLusterColo("ua3");
            Util.print("cluster b3: " + b3.getClusters().get(0));


            //submit 3 clusters
            //	Bundle.submitCluster(b1,b3);

            //get 2 unique feeds
            String feed01 = Util.getInputFeedFromBundle(b1);
            String outputFeed = Util.getOutputFeedFromBundle(b1);

            //set source and target for the 2 feeds

            //set clusters to null;
            feed01 = InstanceUtil
                    .setFeedCluster(feed01,
                            XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                            XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
                            ClusterType.SOURCE, null);
            outputFeed = InstanceUtil
                    .setFeedCluster(outputFeed,
                            XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                            XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
                            ClusterType.SOURCE, null);


            //set new feed input data
            feed01 = Util.setFeedPathValue(feed01,
                    "/feedUpdateTest/feed01/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/");


            //generate data in both the colos ua1 and ua3
            String prefix = InstanceUtil.getFeedPrefix(feed01);
            Util.HDFSCleanup(ua1, prefix.substring(1));
            Util.lateDataReplenish(ua1, 70, 0, 1, prefix);

            String startTime = InstanceUtil.getTimeWrtSystemTime(-50);

            //set clusters for feed01
            feed01 = InstanceUtil
                    .setFeedCluster(feed01, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                            Util.readClusterName(b1.getClusters().get(0)), ClusterType.SOURCE,
                            null);
            feed01 = InstanceUtil
                    .setFeedCluster(feed01, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                            Util.readClusterName(b3.getClusters().get(0)), ClusterType.TARGET,
                            null);


            //set clusters for output feed
            outputFeed = InstanceUtil.setFeedCluster(outputFeed,
                    XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                    XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                    Util.readClusterName(b1.getClusters().get(0)), ClusterType.SOURCE, null);
            outputFeed = InstanceUtil.setFeedCluster(outputFeed,
                    XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                    XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                    Util.readClusterName(b3.getClusters().get(0)), ClusterType.TARGET, null);


            //submit and schedule feeds
            Util.print("feed01: " + feed01);
            Util.print("outputFeed: " + outputFeed);

            //	ServiceResponse r = prismHelper.getFeedHelper().submitAndSchedule(URLS
            // .SUBMIT_AND_SCHEDULE_URL,
            // feed01);
            //	r = prismHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_URL, outputFeed);


            //create 2 process with 2 clusters

            //get first process
            String process01 = b1.getProcessData();

            //add clusters to process

            String processStartTime = InstanceUtil.getTimeWrtSystemTime(-11);
            String processEndTime = InstanceUtil.getTimeWrtSystemTime(70);


            process01 = InstanceUtil
                    .setProcessCluster(process01, null,
                            XmlUtil.createProcessValidity(startTime, "2099-01-01T00:00Z"));
            process01 = InstanceUtil
                    .setProcessCluster(process01, Util.readClusterName(b1.getClusters().get(0)),
                            XmlUtil.createProcessValidity(processStartTime, processEndTime));
            process01 = InstanceUtil
                    .setProcessCluster(process01, Util.readClusterName(b3.getClusters().get(0)),
                            XmlUtil.createProcessValidity(processStartTime, processEndTime));
            //		process = instanceUtil.addProcessInputFeed(process,
            // Util.readDatasetName(feed02),
            // Util.readDatasetName(feed02));


            //get 2nd process :
            String process02 = process01;
            process02 = InstanceUtil
                    .setProcessName(process02, "zeroInputProcess" + new Random().nextInt());
            //	b1.setProcessWorkflow("/examples/apps/noInput/");
            List<String> feed = new ArrayList<String>();
            feed.add(outputFeed);
            process02 = b1.setProcessFeeds(process02, feed, 0, 0, 1);


            //submit and schedule both process
            Util.print("process: " + process01);
            Util.print("process: " + process02);


            //	r = prismHelper.getProcessHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL,
            // process01);
            //	r = prismHelper.getProcessHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL,
            // process02);


            Util.print("Wait till process goes into running ");

			/*	for(int i = 0 ; i < 30 ; i++)
            {
				Status sUa1 = 	instanceUtil.getInstanceStatus(ua1, Util.getProcessName(process01)
				,0, 0);
				Status sUa2 =	instanceUtil.getInstanceStatus(ua3, Util.getProcessName(process01)
				,0, 0);
				if((sUa1.toString().equals("RUNNING") || (sUa1.toString().equals("SUCCEEDED ")))
				 &&  (sUa2.toString
				().equals("RUNNING") || sUa2.toString().equals("SUCCEEDED")) )
					break;
				Thread.sleep(20000);

			}*/

            //update outputFeed
            //change feed location path
            outputFeed = Util.setFeedProperty(outputFeed, "queueName", "myQueue");
            //	outputFeed =	instanceUtil.setFeedFilePath(outputFeed,
            // "/newFeedPath/input-data/rawLogs/oozieExample/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");


            Util.print("updated feed: " + outputFeed);

            //update feed first time
            prismHelper.getFeedHelper().update(outputFeed, outputFeed);

        } finally {
            b1.deleteBundle(prismHelper);
            b3.deleteBundle(prismHelper);

        }
    }

}
