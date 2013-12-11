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
import org.apache.falcon.regression.core.generated.dependencies.Frequency.TimeUnit;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.helpers.PrismHelper;
import org.apache.falcon.regression.core.supportClasses.Consumer;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.oozie.client.CoordinatorAction;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;


/**
 * Null output process tests.
 */
public class NoOutputProcessTest {

    private final PrismHelper prismHelper = new PrismHelper("prism.properties");
    private final ColoHelper ivoryqa1 = new ColoHelper("ivoryqa-1.config.properties");

    @BeforeClass(alwaysRun = true)
    public void createTestData() throws Exception {

        Util.print("in @BeforeClass");

        System.setProperty("java.security.krb5.realm", "");
        System.setProperty("java.security.krb5.kdc", "");


        Bundle b = (Bundle) Util.readELBundles()[0][0];
        b.generateUniqueBundle();
        b = new Bundle(b, ivoryqa1.getEnvFileName());

        String startDate = "2010-01-03T00:00Z";
        String endDate = "2010-01-03T03:00Z";

        b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
        String prefix = b.getFeedDataPathPrefix();
        Util.HDFSCleanup(ivoryqa1, prefix.substring(1));

        DateTime startDateJoda = new DateTime(InstanceUtil.oozieDateToDate(startDate));
        DateTime endDateJoda = new DateTime(InstanceUtil.oozieDateToDate(endDate));

        List<String> dataDates = Util.getMinuteDatesOnEitherSide(startDateJoda, endDateJoda, 20);

        for (int i = 0; i < dataDates.size(); i++)
            dataDates.set(i, prefix + dataDates.get(i));

        ArrayList<String> dataFolder = new ArrayList<String>();

        for (String dataDate : dataDates) {
            dataFolder.add(dataDate);
        }

        InstanceUtil.putDataInFolders(ivoryqa1, dataFolder);
    }


    @BeforeMethod(alwaysRun = true)
    public void testName(Method method) {
        Util.print("test name: " + method.getName());
    }


    @Test(enabled = true, groups = {"singleCluster"})
    public void checkForJMSMsgWhenNoOutput() throws Exception {

        Bundle b = new Bundle();
        try {
            b = (Bundle) Util.readNoOutputBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessValidity("2010-01-03T02:30Z", "2010-01-03T02:45Z");
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.submitAndScheduleBundle(prismHelper);


            Util.print("attaching consumer to:   " + "FALCON.ENTITY.TOPIC");
            Consumer consumer =
                    new Consumer("FALCON.ENTITY.TOPIC", ivoryqa1.getClusterHelper().getActiveMQ());
            consumer.start();
            Thread.sleep(15000);

            //wait for all the instances to complete
            InstanceUtil.waitTillInstanceReachState(ivoryqa1, b.getProcessName(), 3,
                    CoordinatorAction.Status.SUCCEEDED,
                    20);

            Assert.assertEquals(consumer.getMessageData().size(), 3,
                    " Message for all the 3 instance not found");

            consumer.stop();

            Util.dumpConsumerData(consumer);
        } finally {
            b.deleteBundle(prismHelper);

        }
    }


    @Test(enabled = true, groups = {"singleCluster"})
    public void rm() throws Exception {

        Bundle b = new Bundle();
        try {
            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessValidity("2010-01-03T02:30Z", "2010-01-03T02:45Z");
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.submitAndScheduleBundle(prismHelper);

            Consumer consumerInternalMsg =
                    new Consumer("FALCON.ENTITY.TOPIC", ivoryqa1.getClusterHelper().getActiveMQ());
            Consumer consumerProcess =
                    new Consumer("FALCON." + b.getProcessName(),
                            ivoryqa1.getClusterHelper().getActiveMQ());


            consumerInternalMsg.start();
            consumerProcess.start();

            Thread.sleep(15000);

            //wait for all the instances to complete
            InstanceUtil.waitTillInstanceReachState(ivoryqa1, b.getProcessName(), 3,
                    CoordinatorAction.Status.SUCCEEDED,
                    20);

            Assert.assertEquals(consumerInternalMsg.getMessageData().size(), 3,
                    " Message for all the 3 instance not found");
            Assert.assertEquals(consumerProcess.getMessageData().size(), 3,
                    " Message for all the 3 instance not found");

            consumerInternalMsg.stop();
            consumerProcess.stop();

            Util.dumpConsumerData(consumerInternalMsg);
            Util.dumpConsumerData(consumerProcess);

        } finally {
            b.deleteBundle(prismHelper);
        }
    }
}
