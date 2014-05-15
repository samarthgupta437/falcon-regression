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
import org.apache.falcon.regression.core.generated.process.Property;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.enumsAndConstants.ENTITY_TYPE;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;


@Test(groups = "embedded")
public class ProcessPartitionExpVariableTest extends BaseTestClass {
    static Logger logger = Logger.getLogger(ProcessPartitionExpVariableTest.class);

    ColoHelper cluster = servers.get(0);
    FileSystem clusterFS = serverFS.get(0);
    OozieClient clusterOC = serverOC.get(0);
    private String baseTestDir = baseHDFSDir + "/ProcessPartitionExpVariableTest";
    String aggregateWorkflowDir = baseTestDir + "/aggregator";

    @BeforeClass(alwaysRun = true)
    public void uploadWorkflow() throws Exception {
        HadoopUtil.uploadDir(clusterFS, aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        logger.info("test name: " + method.getName());
        bundles[0] = BundleUtil.readELBundles()[0][0];
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].generateUniqueBundle();
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        removeBundles();
        HadoopUtil.deleteDirIfExists(baseTestDir, clusterFS);
    }

    @Test(enabled = true)
    public void ProcessPartitionExpVariableTest_OptionalCompulsaryPartition() throws Exception {
        String startTime = TimeUtil.getTimeWrtSystemTime(-4);
        String endTime = TimeUtil.getTimeWrtSystemTime(30);

        bundles[0] = bundles[0].getRequiredBundle(bundles[0], 1, 2, 1, baseTestDir, 1, startTime,
            endTime);
        bundles[0].setProcessData(bundles[0]
            .setProcessInputNames(bundles[0].getProcessData(), "inputData0", "inputData"));
        Property p = new Property();
        p.setName("var1");
        p.setValue("hardCoded");

        bundles[0].setProcessData(bundles[0].addProcessProperty(bundles[0].getProcessData(), p));

        bundles[0].setProcessData(bundles[0]
            .setProcessInputPartition(bundles[0].getProcessData(), "${var1}", "${fileTime}"));


        for (int i = 0; i < bundles[0].getDataSets().size(); i++)
            logger.info(bundles[0].getDataSets().get(i));

        logger.info(bundles[0].getProcessData());

        createDataWithinDatesAndPrefix(cluster,
            TimeUtil.oozieDateToDate(TimeUtil.addMinsToTime(startTime, -25)),
            TimeUtil.oozieDateToDate(TimeUtil.addMinsToTime(endTime, 25)),
            baseTestDir + "/input1/", 1);

        bundles[0].submitAndScheduleBundle(bundles[0], prism, false);
        TimeUnit.SECONDS.sleep(20);

        InstanceUtil.waitTillInstanceReachState(clusterOC,
            Util.getProcessName(bundles[0].getProcessData()), 2,
            CoordinatorAction.Status.SUCCEEDED, 20, ENTITY_TYPE.PROCESS);
    }

    private static void createDataWithinDatesAndPrefix(ColoHelper colo, DateTime startDateJoda,
                                                       DateTime endDateJoda, String prefix,
                                                       int interval) throws IOException {
        List<String> dataDates =
            generateDateAndOneDayAfter(startDateJoda, endDateJoda, interval);

        for (int i = 0; i < dataDates.size(); i++)
            dataDates.set(i, prefix + dataDates.get(i));

        List<String> dataFolder = new ArrayList<String>();

        for (String dataDate : dataDates) dataFolder.add(dataDate);

        InstanceUtil.putDataInFolders(colo, dataFolder, "");

    }

    private static List<String> generateDateAndOneDayAfter(DateTime startDate, DateTime endDate,
                                                           int minuteSkip) {
        //we want to generate patterns of the form .../2014/03/06/21/57/2014-Mar-07
        //note there are two dates and the second date is one day after the first one
        final DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy/MM/dd/HH/mm/");
        final DateTimeFormatter formatter2 = DateTimeFormat.forPattern("yyyy-MMM-dd");
        logger.info("generating data between " + formatter.print(startDate) + " and " +
            formatter.print(endDate));

        List<String> dates = new ArrayList<String>();


        while (!startDate.isAfter(endDate)) {
            final DateTime nextDate = startDate.plusMinutes(minuteSkip);
            dates.add(formatter.print(nextDate) + formatter2.print(nextDate.plusDays(1)));
            if (minuteSkip == 0) {
                minuteSkip = 1;
            }
            startDate = nextDate;
        }

        return dates;
    }

    //TODO: ProcessPartitionExpVariableTest_OptionalPartition()
    //TODO: ProcessPartitionExpVariableTest_CompulsaryPartition()
    //TODO: ProcessPartitionExpVariableTest_moreThanOnceVariable()
}
