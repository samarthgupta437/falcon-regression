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

package org.apache.falcon.regression.lineage;

import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.oozie.client.Job;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Test(groups = "lineage-rest")
public class LineageApiProcessInstanceTest extends BaseTestClass {

    ColoHelper cluster = servers.get(0);
    FileSystem clusterFS = serverFS.get(0);
    String baseTestHDFSDir = baseHDFSDir + "/LineageApiInstanceTest";
    String aggregateWorkflowDir = baseTestHDFSDir + "/aggregator";
    String feedInputPrefix = baseTestHDFSDir + "/input";
    String feedInputPath = feedInputPrefix + "/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
    String feedOutputPath =
        baseTestHDFSDir + "/output-data/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
    private static final Logger logger = Logger.getLogger(LineageApiProcessInstanceTest.class);
    String processName = null;

    @BeforeMethod(alwaysRun = true, firstTimeOnly = true)
    public void setup(Method method) throws Exception {
        HadoopUtil.deleteDirIfExists(baseTestHDFSDir, clusterFS);
        HadoopUtil.uploadDir(clusterFS, aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);

        bundles[0] = new Bundle(BundleUtil.readELBundles()[0][0], cluster);
        bundles[0].generateUniqueBundle();

        final String dataStartDate = "2010-01-02T09:00Z";
        final String processStartDate = "2010-01-02T09:50Z";
        final String endDate = "2010-01-02T10:00Z";

        bundles[0].setInputFeedDataPath(feedInputPath);

        /* data set creation */
        List<String> dataDates = TimeUtil.getMinuteDatesOnEitherSide(
            new DateTime(TimeUtil.oozieDateToDate(dataStartDate)),
            new DateTime(TimeUtil.oozieDateToDate(endDate)), 5);
        logger.info("dataDates = " + dataDates);
        HadoopUtil.createPeriodicDataset(dataDates, OSUtil.NORMAL_INPUT, clusterFS,
            feedInputPrefix);

        /* running process */
        bundles[0].setInputFeedDataPath(feedInputPath);
        bundles[0].setInputFeedPeriodicity(5, Frequency.TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setOutputFeedPeriodicity(5, Frequency.TimeUnit.minutes);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        bundles[0].setProcessValidity(processStartDate, endDate);
        bundles[0].setProcessPeriodicity(5, Frequency.TimeUnit.minutes);
        bundles[0].submitAndScheduleBundle(prism);
        processName = bundles[0].getProcessName();
        Job.Status status = null;
        for (int i = 0; i < 20; i++) {
            status = InstanceUtil.getDefaultCoordinatorStatus(cluster,
                Util.getProcessName(bundles[0].getProcessData()), 0);
            if (status == Job.Status.SUCCEEDED || status == Job.Status.KILLED)
                break;
            TimeUnit.SECONDS.sleep(30);
        }
        Assert.assertNotNull(status);
        Assert.assertEquals(status, Job.Status.SUCCEEDED,
            "The job did not succeeded even in long time");
    }
    @AfterMethod(alwaysRun = true, lastTimeOnly = true)
    public void tearDown() {
        removeBundles();
    }

    @Test
    public void processToProcessInstanceNodes() {

    }

}
