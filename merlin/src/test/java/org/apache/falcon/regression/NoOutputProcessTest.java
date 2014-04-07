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
import org.apache.falcon.regression.core.supportClasses.Consumer;
import org.apache.falcon.regression.core.enumsAndConstants.ENTITY_TYPE;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;


/**
 * Null output process tests.
 */
@Test(groups = "embedded")
public class NoOutputProcessTest extends BaseTestClass {

    ColoHelper cluster = servers.get(0);
    FileSystem clusterFS = serverFS.get(0);
    OozieClient clusterOC = serverOC.get(0);
    String aggregateWorkflowDir = baseHDFSDir + "/NoOutputProcessTest/aggregator";
    private static final Logger logger = Logger.getLogger(NoOutputProcessTest.class);

    @BeforeClass(alwaysRun = true)
    public void createTestData() throws Exception {

        logger.info("in @BeforeClass");
        HadoopUtil.uploadDir(clusterFS, aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);

        Bundle b = Util.readELBundles()[0][0];
        b.generateUniqueBundle();
        b = new Bundle(b, cluster);

        String startDate = "2010-01-03T00:00Z";
        String endDate = "2010-01-03T03:00Z";

        b.setInputFeedDataPath(baseHDFSDir + "/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
        String prefix = b.getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), clusterFS);

        DateTime startDateJoda = new DateTime(InstanceUtil.oozieDateToDate(startDate));
        DateTime endDateJoda = new DateTime(InstanceUtil.oozieDateToDate(endDate));

        List<String> dataDates = Util.getMinuteDatesOnEitherSide(startDateJoda, endDateJoda, 20);

        for (int i = 0; i < dataDates.size(); i++)
            dataDates.set(i, prefix + dataDates.get(i));

        ArrayList<String> dataFolder = new ArrayList<String>();

        for (String dataDate : dataDates) {
            dataFolder.add(dataDate);
        }

        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.NORMAL_INPUT, dataFolder);
    }


    @BeforeMethod(alwaysRun = true)
    public void testName(Method method) throws Exception {
        logger.info("test name: " + method.getName());
        bundles[0] = Util.readELBundles()[0][0];
        bundles[0].generateUniqueBundle();
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        bundles[0].setInputFeedDataPath(baseHDFSDir + "/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
        bundles[0].setProcessValidity("2010-01-03T02:30Z", "2010-01-03T02:45Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].submitAndScheduleBundle(prism);
    }

    @AfterMethod
    public void tearDown() throws Exception {
        removeBundles();
    }

    @Test(enabled = true, groups = {"singleCluster"})
    public void checkForJMSMsgWhenNoOutput() throws Exception {
        logger.info("attaching consumer to:   " + "FALCON.ENTITY.TOPIC");
        Consumer consumer =
                new Consumer("FALCON.ENTITY.TOPIC", cluster.getClusterHelper().getActiveMQ());
        consumer.start();
        Thread.sleep(15000);

        //wait for all the instances to complete
        InstanceUtil.waitTillInstanceReachState(clusterOC, bundles[0].getProcessName(), 3,
                CoordinatorAction.Status.SUCCEEDED, 20, ENTITY_TYPE.PROCESS);

        Assert.assertEquals(consumer.getMessageData().size(), 3,
                " Message for all the 3 instance not found");

        consumer.interrupt();

        Util.dumpConsumerData(consumer);

    }


    @Test(enabled = true, groups = {"singleCluster"})
    public void rm() throws Exception {
        Consumer consumerInternalMsg =
                new Consumer("FALCON.ENTITY.TOPIC", cluster.getClusterHelper().getActiveMQ());
        Consumer consumerProcess =
                new Consumer("FALCON." + bundles[0].getProcessName(), cluster.getClusterHelper().getActiveMQ());

        consumerInternalMsg.start();
        consumerProcess.start();

        Thread.sleep(15000);

        //wait for all the instances to complete

        InstanceUtil.waitTillInstanceReachState(clusterOC, bundles[0].getProcessName(), 3,
                CoordinatorAction.Status.SUCCEEDED, 20, ENTITY_TYPE.PROCESS);

        Assert.assertEquals(consumerInternalMsg.getMessageData().size(), 3,
                " Message for all the 3 instance not found");
        Assert.assertEquals(consumerProcess.getMessageData().size(), 3,
                " Message for all the 3 instance not found");

        consumerInternalMsg.interrupt();
        consumerProcess.interrupt();

        Util.dumpConsumerData(consumerInternalMsg);
        Util.dumpConsumerData(consumerProcess);
    }

}
