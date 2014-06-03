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

package org.apache.falcon.regression.ui;

import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.enumsAndConstants.ENTITY_TYPE;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Inputs;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.CleanupUtil;
import org.apache.falcon.regression.core.util.Generator;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseUITestClass;
import org.apache.falcon.regression.ui.pages.EntitiesPage;
import org.apache.falcon.regression.ui.pages.ProcessPage;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class ProcessUITest extends BaseUITestClass {

    private ColoHelper cluster = servers.get(0);
    private String baseTestDir = baseHDFSDir + "/TestProcessUI";
    private String aggregateWorkflowDir = baseTestDir + "/aggregator";
    private Logger logger = Logger.getLogger(ProcessUITest.class);
    String datePattern = "/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
    String feedInputPath = baseTestDir + "/input";
    final String feedOutputPath = baseTestDir + "/output";
    private FileSystem clusterFS = serverFS.get(0);
    private OozieClient clusterOC = serverOC.get(0);

    @BeforeMethod
    public void setUp() throws Exception {
        CleanupUtil.cleanAllEntities(prism);
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
        openBrowser();
        bundles[0] = BundleUtil.readELBundles()[0][0];
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].generateUniqueBundle();
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        String startTime = TimeUtil.getTimeWrtSystemTime(0);
        String endTime = TimeUtil.addMinsToTime(startTime, 5);
        logger.info("Start time: " + startTime + "\tEnd time: " + endTime);

        //prepare process definition
        bundles[0].setProcessValidity(startTime, endTime);
        bundles[0].setProcessPeriodicity(1, Frequency.TimeUnit.minutes);
        bundles[0].setProcessConcurrency(5);
        bundles[0].setInputFeedPeriodicity(1, Frequency.TimeUnit.minutes);
        bundles[0].setInputFeedDataPath(feedInputPath + datePattern);
        Process process = InstanceUtil.getProcessElement(bundles[0]);
        Inputs inputs = new Inputs();
        Input input = new Input();
        input.setFeed(Util.readEntityName(BundleUtil.getInputFeedFromBundle(bundles[0])));
        input.setStart("now(0,0)");
        input.setEnd("now(0,4)");
        input.setName("inputData");
        inputs.getInputs().add(input);
        process.setInputs(inputs);

        bundles[0].setProcessData(InstanceUtil.processToString(process));

        //provide necessary data for first 3 instances to run
        logger.info("Creating necessary data...");
        String prefix = bundles[0].getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), clusterFS);
        DateTime startDate = new DateTime(TimeUtil.oozieDateToDate(TimeUtil.addMinsToTime(startTime, -2)));
        DateTime endDate = new DateTime(TimeUtil.oozieDateToDate(endTime));
        List<String> dataDates = TimeUtil.getMinuteDatesOnEitherSide(startDate, endDate, 0);
        List<String> dataPaths = new ArrayList<String>();
        logger.info("Creating data in folders: \n" + dataDates);
        prefix = prefix.substring(0, prefix.length()-1);

        // use 5 <= x < 10 input feeds
        final int numInputFeeds = 5 + new Random().nextInt(5);
        // use 5 <= x < 10 output feeds
        final int numOutputFeeds = 5 + new Random().nextInt(5);

        for (String dataDate : dataDates) {
            dataPaths.add(prefix + "/" + dataDate);
            for (int k = 1; k <= numInputFeeds; k++) {
                dataPaths.add(prefix + "_00" + k + "/" + dataDate);

            }
        }
        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.NORMAL_INPUT, dataPaths);

        logger.info("Process data: " + bundles[0].getProcessData());
        FeedMerlin[] inputFeeds;
        FeedMerlin[] outputFeeds;
        final FeedMerlin inputMerlin = new FeedMerlin(BundleUtil.getInputFeedFromBundle(bundles[0]));
        final FeedMerlin outputMerlin = new FeedMerlin(BundleUtil.getOutputFeedFromBundle(bundles[0]));


        inputFeeds = generateFeeds(numInputFeeds, inputMerlin,
                Generator.getNameGenerator("infeed", inputMerlin.getName()),
                Generator.getHadoopPathGenerator(feedInputPath, datePattern));
        int j = 0;
        for (FeedMerlin feed : inputFeeds) {
            bundles[0].addInputFeedToBundle("inputFeed" + j, feed.toString(), j++);
        }

        outputFeeds = generateFeeds(numOutputFeeds, outputMerlin,
                Generator.getNameGenerator("outfeed", outputMerlin.getName()),
                Generator.getHadoopPathGenerator(feedOutputPath, datePattern));
        j = 0;
        for (FeedMerlin feed : outputFeeds) {
            bundles[0].addOutputFeedToBundle("outputFeed" + j, feed.toString(), j++);
        }

        AssertUtil.assertSucceeded(bundles[0].submitBundle(prism));

    }

    public static FeedMerlin[] generateFeeds(final int numInputFeeds,
                                             final FeedMerlin originalFeedMerlin,
                                             final Generator nameGenerator,
                                             final Generator pathGenerator)
            throws JAXBException, NoSuchMethodException, InvocationTargetException,
            IllegalAccessException, IOException, URISyntaxException, AuthenticationException {
        FeedMerlin[] inputFeeds = new FeedMerlin[numInputFeeds];
        //submit all input feeds
        for(int count = 0; count < numInputFeeds; ++count) {
            final FeedMerlin feed = new FeedMerlin(originalFeedMerlin.toString());
            feed.setName(nameGenerator.generate());
            feed.setLocation(LocationType.DATA, pathGenerator.generate());
            inputFeeds[count] = feed;
        }
        return inputFeeds;
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown(Method method) throws IOException {
        closeBrowser();
        removeBundles();
    }

    @Test
    public void testProcessStatus() throws Exception {

        EntitiesPage page = new EntitiesPage(DRIVER, cluster, ENTITY_TYPE.PROCESS);
        page.navigateTo();

        Assert.assertEquals(page.getEntityStatus(bundles[0].getProcessName()),
                EntitiesPage.EntityStatus.SUBMITTED, "Process status should be SUBMITTED");
        prism.getProcessHelper().schedule(Util.URLS.SCHEDULE_URL, bundles[0].getProcessData());

        InstanceUtil.waitTillInstanceReachState(clusterOC, Util.readEntityName(bundles[0]
                .getProcessData()), 1, CoordinatorAction.Status.RUNNING, 3, ENTITY_TYPE.PROCESS);

        Assert.assertEquals(page.getEntityStatus(bundles[0].getProcessName()),
                EntitiesPage.EntityStatus.RUNNING, "Process status should be RUNNING");

    }

    @Test
    public void testInstances() throws Exception {

        prism.getProcessHelper().schedule(Util.URLS.SCHEDULE_URL, bundles[0].getProcessData());

        InstanceUtil.waitTillInstanceReachState(clusterOC, Util.readEntityName(bundles[0]
                .getProcessData()), 1, CoordinatorAction.Status.RUNNING, 3, ENTITY_TYPE.PROCESS);

        ProcessPage page = new ProcessPage(DRIVER, cluster, bundles[0].getProcessName());
        page.navigateTo();

        String bundleID = InstanceUtil.getLatestBundleID(cluster, bundles[0].getProcessName(), ENTITY_TYPE.PROCESS);
        Map<Date, CoordinatorAction.Status> actions = OozieUtil.getActionsNominalTimeAndStatus(prism, bundleID,
                ENTITY_TYPE.PROCESS);

        checkActions(actions, page);

        InstanceUtil.waitTillInstanceReachState(clusterOC, Util.readEntityName(bundles[0]
                .getProcessData()), 1, CoordinatorAction.Status.SUCCEEDED, 20, ENTITY_TYPE.PROCESS);

        page.refresh();

        actions = OozieUtil.getActionsNominalTimeAndStatus(prism, bundleID, ENTITY_TYPE.PROCESS);

        checkActions(actions, page);

    }

    private void checkActions(Map<Date, CoordinatorAction.Status> actions, ProcessPage page) {
        for(Date date : actions.keySet()) {
            String oozieDate = TimeUtil.dateToOozieDate(date);
            String status = page.getInstanceStatus(oozieDate);
            Assert.assertNotNull(status, oozieDate + " instance not present on UI");
            Assert.assertEquals(status, actions.get(date).toString(), "Status of instance '"
                    + oozieDate + "' is not the same via oozie and via UI");
        }
    }

    @Test
    public void testLineageLink() throws Exception {

        prism.getProcessHelper().schedule(Util.URLS.SCHEDULE_URL, bundles[0].getProcessData());

        InstanceUtil.waitTillInstanceReachState(clusterOC, Util.readEntityName(bundles[0]
                .getProcessData()), 1, CoordinatorAction.Status.SUCCEEDED, 20, ENTITY_TYPE.PROCESS);

        String bundleID = InstanceUtil.getLatestBundleID(cluster, bundles[0].getProcessName(), ENTITY_TYPE.PROCESS);
        Map<Date, CoordinatorAction.Status> actions = OozieUtil.getActionsNominalTimeAndStatus(prism, bundleID,
                ENTITY_TYPE.PROCESS);

        ProcessPage page = new ProcessPage(DRIVER, cluster, bundles[0].getProcessName());
        page.navigateTo();

        for(Date date : actions.keySet()) {
            String oozieDate = TimeUtil.dateToOozieDate(date);
            boolean isPresent = page.isLineageLinkPresent(oozieDate);
            if(actions.get(date) == CoordinatorAction.Status.SUCCEEDED) {
                Assert.assertTrue(isPresent, "Lineage button should be present for instance: " + oozieDate);
            } else {
                Assert.assertFalse(isPresent, "Lineage button should not be present for instance: " + oozieDate);
            }
        }

    }
}
