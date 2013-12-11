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
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.oozie.client.Job.Status;
import org.joda.time.DateTime;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * Process lib path tests.
 */
public class ProcessLibPath {


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

        String startDate = "2010-01-01T22:00Z";
        String endDate = "2010-01-02T03:00Z";

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

    @Test(groups = {"singleCluster"})
    public void setDifferentLibPathWithNoLibFolderInWorkflowfLocaltion() throws Exception {

        Bundle b = new Bundle();
        try {

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:04Z");
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedLocationData(
                    "/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/$" +
                            "{HOUR}/${MINUTE}");
            b.setProcessConcurrency(1);
            b.setProcessLibPath("/examples/apps/testLib/");
            b.setProcessWorkflow("/examples/apps/aggregatorLib/");
            Util.print("processData: " + b.getProcessData());
            b.submitAndScheduleBundle(prismHelper);
            InstanceUtil
                    .waitForBundleToReachState(ivoryqa1, b.getProcessName(), Status.SUCCEEDED, 20);


        } finally {
            b.deleteBundle(prismHelper);

        }
    }

    @Test(groups = {"singleCluster"})
    public void setDifferentLibPathWithWrongJarInWorkflowLib() throws Exception {

        Bundle b = null;
        try {

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:04Z");
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedLocationData(
                    "/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/$" +
                            "{HOUR}/${MINUTE}");
            b.setProcessConcurrency(1);
            b.setProcessLibPath("/examples/apps/testLib/");
            b.setProcessWorkflow("/examples/apps/aggregatorLib02/");
            Util.print("processData: " + b.getProcessData());
            b.submitAndScheduleBundle(prismHelper);
            InstanceUtil
                    .waitForBundleToReachState(ivoryqa1, b.getProcessName(), Status.SUCCEEDED, 20);
        } finally {
            if (b != null) {
                b.deleteBundle(prismHelper);
            }
        }
    }
}
