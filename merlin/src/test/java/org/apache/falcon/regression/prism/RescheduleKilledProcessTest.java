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
import org.apache.falcon.regression.core.helpers.PrismHelper;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.core.util.XmlUtil;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RescheduleKilledProcessTest {

    PrismHelper prismHelper = new PrismHelper("prism.properties");

    ColoHelper ua1 = new ColoHelper("gs1001.config.properties");

    @Test(enabled = false, timeOut = 1200000)
    public void recheduleKilledProcess() throws Exception {
        // submit and schedule a process with error in workflow .
        //it will get killed
        Bundle b1 = (Bundle) Util.readELBundles()[0][0];
        b1.generateUniqueBundle();
        try {
            //generate bundles according to config files
            b1 = new Bundle(b1, ua1.getEnvFileName());
            String processStartTime = InstanceUtil.getTimeWrtSystemTime(-11);
            String processEndTime = InstanceUtil.getTimeWrtSystemTime(06);


            String process = b1.getProcessData();
            process = InstanceUtil
                    .setProcessName(process, "zeroInputProcess" + new Random().nextInt());
            List<String> feed = new ArrayList<String>();
            feed.add(Util.getOutputFeedFromBundle(b1));
            process = b1.setProcessFeeds(process, feed, 0, 0, 1);

            process = InstanceUtil.setProcessCluster(process, null,
                    XmlUtil.createProcessValidity(processStartTime, "2099-01-01T00:00Z"));
            process = InstanceUtil
                    .setProcessCluster(process, Util.readClusterName(b1.getClusters().get(0)),
                            XmlUtil.createProcessValidity(processStartTime, processEndTime));
            b1.setProcessData(process);

            b1.submitAndScheduleBundle(prismHelper);

            prismHelper.getProcessHelper().delete(URLS.DELETE_URL, b1.getProcessData());

            //		prismHelper.getProcessHelper().submitEntity(URLS.SUBMIT_URL,
            // b1.getProcessData());

            //		prismHelper.getProcessHelper().delete(URLS.DELETE_URL, b1.getProcessData());

            prismHelper.getProcessHelper().submitEntity(URLS.SUBMIT_URL, b1.getProcessData());

            prismHelper.getProcessHelper().schedule(URLS.SCHEDULE_URL, b1.getProcessData());


        } finally {
            b1.deleteBundle(prismHelper);
        }


    }


    @Test(enabled = true, timeOut = 1200000)
    public void recheduleKilledProcess02() throws Exception {
        // submit and schedule a process with error in workflow .
        //it will get killed
        Bundle b1 = (Bundle) Util.readELBundles()[0][0];
        b1.generateUniqueBundle();
        try {
            //generate bundles according to config files
            b1 = new Bundle(b1, ua1.getEnvFileName());
            String processStartTime = InstanceUtil.getTimeWrtSystemTime(-11);
            String processEndTime = InstanceUtil.getTimeWrtSystemTime(06);
            b1.setProcessValidity(processStartTime, processEndTime);
            b1.setInputFeedDataPath(
                    "/samarth/input-data/rawLogs/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");

            String prefix = InstanceUtil.getFeedPrefix(Util.getInputFeedFromBundle(b1));
            Util.HDFSCleanup(ua1, prefix.substring(1));
            Util.lateDataReplenish(ua1, 40, 0, 1, prefix);

            System.out.println("process: " + b1.getProcessData());

            b1.submitAndScheduleBundle(prismHelper);

            prismHelper.getProcessHelper().delete(URLS.DELETE_URL, b1.getProcessData());

            prismHelper.getProcessHelper().submitEntity(URLS.SUBMIT_URL, b1.getProcessData());
            prismHelper.getProcessHelper().schedule(URLS.SCHEDULE_URL, b1.getProcessData());

            prismHelper.getProcessHelper().delete(URLS.DELETE_URL, b1.getProcessData());

            prismHelper.getProcessHelper().submitEntity(URLS.SUBMIT_URL, b1.getProcessData());
            prismHelper.getProcessHelper().schedule(URLS.SCHEDULE_URL, b1.getProcessData());

        } finally {
            b1.deleteBundle(prismHelper);
        }
    }
}
