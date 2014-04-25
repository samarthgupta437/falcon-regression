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

import com.jcraft.jsch.JSchException;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.generated.dependencies.Frequency;
import org.apache.falcon.regression.core.generated.feed.ActionType;
import org.apache.falcon.regression.core.generated.feed.ClusterType;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.enumsAndConstants.ENTITY_TYPE;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.XmlUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClientException;
import org.custommonkey.xmlunit.Diff;
import org.custommonkey.xmlunit.XMLUnit;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.xml.sax.SAXException;
import org.apache.log4j.Logger;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.List;


public class UpdateAtSpecificTimeTest extends BaseTestClass {

    static Logger logger = Logger.getLogger(UpdateAtSpecificTimeTest.class);

    Bundle processBundle = new Bundle();

    ColoHelper cluster_1 = servers.get(0);
    ColoHelper cluster_2 = servers.get(1);
    ColoHelper cluster_3 = servers.get(2);

    private String dateTemplate = "/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
    private final String baseTestDir = baseHDFSDir + "/UpdateAtSpecificTimeTest-data";
    String aggregateWorkflowDir = baseHDFSDir + "/aggregator";

    @BeforeClass(alwaysRun = true)
    public void uploadWorkflow() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }


    @BeforeMethod(alwaysRun = true)
    public void setup(Method method) throws IOException, JAXBException {
        logger.info("test name: " + method.getName());
        Bundle bundle = (Bundle) Bundle.readBundle("LocalDC_feedReplicaltion_BillingRC")[0][0];
        bundles[0] = new Bundle(bundle, cluster_1);
        bundles[1] = new Bundle(bundle, cluster_2);
        bundles[2] = new Bundle(bundle, cluster_3);

        bundles[0].generateUniqueBundle();
        bundles[1].generateUniqueBundle();
        bundles[2].generateUniqueBundle();

        processBundle = BundleUtil.readELBundles()[0][0];
        processBundle = new Bundle(processBundle, cluster_1);
        processBundle.generateUniqueBundle();
        processBundle.setProcessWorkflow(aggregateWorkflowDir);
    }

    public void tearDown() throws Exception {
        removeBundles();
        removeBundles(processBundle);
    }

    @Test(groups = {"singleCluster", "0.3.1"}, timeOut = 1200000,
            enabled = true)
    public void invalidChar_Process()
    throws JAXBException, ParseException, InterruptedException, IOException, URISyntaxException,
    AuthenticationException, OozieClientException {
        processBundle.setProcessValidity(TimeUtil.getTimeWrtSystemTime(0),
                TimeUtil.getTimeWrtSystemTime(20));
        processBundle.submitAndScheduleBundle(prism);
        InstanceUtil.waitTillInstancesAreCreated(cluster_1, processBundle.getProcessData(), 0,
                10);
        String oldProcess =
                processBundle.getProcessData();
        processBundle.setProcessValidity(TimeUtil.getTimeWrtSystemTime(5),
                TimeUtil.getTimeWrtSystemTime(100));
        ServiceResponse r = prism.getProcessHelper().update(oldProcess,
                processBundle.getProcessData(), "abc", null);
        Assert.assertTrue(r.getMessage()
                .contains("java.lang.IllegalArgumentException: abc is not a valid UTC string"));
    }

    @Test(groups = {"singleCluster", "0.3.1"}, timeOut = 1200000,
            enabled = true)
    public void invalidChar_Feed()
    throws ParseException, JAXBException, IOException, URISyntaxException, AuthenticationException,
    OozieClientException {

        String feed = submitAndScheduleFeed(processBundle);
        InstanceUtil.waitTillInstancesAreCreated(cluster_1, feed, 0, 10);
        //update frequency
        Frequency f = new Frequency(21, Frequency.TimeUnit.minutes);
        String updatedFeed = InstanceUtil.setFeedFrequency(feed, f);

        ServiceResponse r = prism.getFeedHelper().update(feed, updatedFeed, "abc", null);
        Assert.assertTrue(r.getMessage()
                .contains("java.lang.IllegalArgumentException: abc is not a valid UTC string"));
    }


    @Test(groups = {"singleCluster", "0.3.1"}, timeOut = 1200000,
            enabled = true)
    public void updateTimeInPast_Process()
    throws JAXBException, ParseException, InterruptedException, IOException, URISyntaxException,
    OozieClientException, IllegalAccessException, NoSuchMethodException, InvocationTargetException,
    AuthenticationException {
        processBundle.setProcessValidity(TimeUtil.getTimeWrtSystemTime(0),
                TimeUtil.getTimeWrtSystemTime(20));
        processBundle.submitAndScheduleBundle(prism);
        Thread.sleep(15000);
        //get old process details
        String oldProcess = processBundle.getProcessData();

        String oldBundleId = InstanceUtil
                .getLatestBundleID(cluster_1,
                        Util.readEntityName(processBundle.getProcessData()), ENTITY_TYPE.PROCESS);

        List<String> initialNominalTimes = OozieUtil.getActionsNominalTime(cluster_1,
                oldBundleId, ENTITY_TYPE.PROCESS);

        InstanceUtil.waitTillInstancesAreCreated(cluster_1, oldProcess, 0, 10);

        // update process by adding property
        processBundle.setProcessProperty("someProp", "someValue");
        ServiceResponse r = prism.getProcessHelper().update(oldProcess,
                processBundle.getProcessData(), TimeUtil.getTimeWrtSystemTime(-10000));
        AssertUtil.assertSucceeded(r);

        //check new coord created with current tim
        OozieUtil.verifyNewBundleCreation(cluster_1, oldBundleId, initialNominalTimes,
                processBundle.getProcessData(), true,
                false);

        InstanceUtil.waitTillInstancesAreCreated(cluster_1, oldProcess, 1, 10);

        OozieUtil.verifyNewBundleCreation(cluster_1, oldBundleId, initialNominalTimes,
                processBundle.getProcessData(), true,
                true);

    }

    @Test(groups = {"MultiCluster", "0.3.1"}, timeOut = 1200000,
            enabled = true)

    public void updateTimeInPast_Feed()
    throws InterruptedException, JAXBException, ParseException, IOException, OozieClientException,
    URISyntaxException, AuthenticationException {


        String startTimeCluster_source = TimeUtil.getTimeWrtSystemTime(-10);
        String startTimeCluster_target = TimeUtil.getTimeWrtSystemTime(10);

        String feed = getMultiClusterFeed(startTimeCluster_source, startTimeCluster_target);

        logger.info("feed: " + feed);

        //submit and schedule feed
        ServiceResponse r =
                prism.getFeedHelper().submitEntity(Util.URLS.SUBMIT_AND_SCHEDULE_URL, feed);
        Thread.sleep(10000);
        AssertUtil.assertSucceeded(r);

        InstanceUtil.waitTillInstancesAreCreated(cluster_1, feed, 0, 10);

        //update frequency
        Frequency f = new Frequency(7, Frequency.TimeUnit.minutes);
        String updatedFeed = InstanceUtil.setFeedFrequency(feed, f);

        r = prism.getFeedHelper().update(feed, updatedFeed,
                TimeUtil.getTimeWrtSystemTime(-10000), null);
        AssertUtil.assertSucceeded(r);

        InstanceUtil.waitTillInstancesAreCreated(cluster_1, feed, 1, 10);

        //check correct number of coord exists or not
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster_1.getFeedHelper(),
                        Util.readDatasetName(feed),
                        "REPLICATION"), 2);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster_2.getFeedHelper(), Util.readDatasetName(feed),
                        "RETENTION"), 2);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster_1.getFeedHelper(), Util.readDatasetName(feed),
                        "RETENTION"), 2);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster_3.getFeedHelper(), Util.readDatasetName(feed),
                        "RETENTION"), 2);

    }


    @Test(groups = {"MultiCluster", "0.3.1"}, timeOut = 1200000,
            enabled = true)
    public void inNextFewMinutesUpdate_RollForward_Process()
    throws JAXBException, ParseException, IOException, URISyntaxException, InterruptedException,
    JSchException, OozieClientException, SAXException, IllegalAccessException,
    NoSuchMethodException, InvocationTargetException, AuthenticationException {
    /*
    submit process on 3 clusters. Schedule on 2 clusters. Bring down one of
    the scheduled cluster. Update with time 5 minutes from now. On running
    cluster new coord should be created with start time +5 and no instance
    should be missing. On 3rd cluster where process was only submit,
    definition should be updated. Bring the down cluster up. Update with same
     definition again, now the recently up cluster should also have new
     coords.
     */

        try {
            String startTime = TimeUtil.getTimeWrtSystemTime(-15);
            processBundle.setProcessValidity(startTime,
                    TimeUtil.getTimeWrtSystemTime(60));
            processBundle.addClusterToBundle(bundles[1].getClusters().get(0),
                    ClusterType.SOURCE, null, null);
            processBundle.addClusterToBundle(bundles[2].getClusters().get(0),
                    ClusterType.SOURCE, null, null);
            processBundle.submitBundle(prism);

            //schedule of 2 cluster
            cluster_1.getProcessHelper().schedule(Util.URLS.SCHEDULE_URL,
                    processBundle.getProcessData());

            cluster_2.getProcessHelper().schedule(Util.URLS.SCHEDULE_URL,
                    processBundle.getProcessData());

            InstanceUtil.waitTillInstancesAreCreated(cluster_2,
                    processBundle.getProcessData(), 0, 10);

            //shut down cluster_2
            Util.shutDownService(cluster_2.getProcessHelper());

            // save old data before update
            String oldProcess = processBundle.getProcessData();
            String oldBundleID_cluster1 = InstanceUtil
                    .getLatestBundleID(cluster_1,
                            Util.readEntityName(oldProcess), ENTITY_TYPE.PROCESS);
            String oldBundleID_cluster2 = InstanceUtil
                    .getLatestBundleID(cluster_2,
                            Util.readEntityName(oldProcess), ENTITY_TYPE.PROCESS);

            List<String> oldNominalTimes_cluster1 = OozieUtil.getActionsNominalTime
                    (cluster_1,
                            oldBundleID_cluster1, ENTITY_TYPE.PROCESS);

            List<String> oldNominalTimes_cluster2 = OozieUtil.getActionsNominalTime
                    (cluster_2,
                            oldBundleID_cluster2, ENTITY_TYPE.PROCESS);

            //update process validity
            processBundle.setProcessProperty("someProp", "someValue");

            //send update request
            String updateTime = TimeUtil.getTimeWrtSystemTime(5);
            ServiceResponse r = prism.getProcessHelper()
                    .update(oldProcess, processBundle.getProcessData(), updateTime
                    );
            AssertUtil.assertPartial(r);

            InstanceUtil.waitTillInstancesAreCreated(cluster_1,
                    processBundle.getProcessData(), 1, 10);

            //verify new bundle on cluster_1 and definition on cluster_3
            OozieUtil.verifyNewBundleCreation(cluster_1, oldBundleID_cluster1,
                    oldNominalTimes_cluster1,
                    oldProcess, true, false);

            OozieUtil.verifyNewBundleCreation(cluster_2, oldBundleID_cluster2,
                    oldNominalTimes_cluster2,
                    oldProcess, false, false);

            String definition_cluster_3 = Util.getEntityDefinition(cluster_3,
                    processBundle.getProcessData(), true);

            Assert.assertTrue(XmlUtil.isIdentical(definition_cluster_3,
                    processBundle.getProcessData()), "Process definitions should be equal");

            //start the stopped cluster_2
            Util.startService(cluster_2.getProcessHelper());
            Thread.sleep(40000);

            String newBundleID_cluster1 = InstanceUtil
                    .getLatestBundleID(cluster_1,
                            Util.readEntityName(oldProcess), ENTITY_TYPE.PROCESS);

            //send second update request
            r = prism.getProcessHelper().update(oldProcess,
                    processBundle.getProcessData(),
                    updateTime);
            AssertUtil.assertSucceeded(r);


            String def_cluster_2 = Util.getEntityDefinition(cluster_2,
                    processBundle.getProcessData(), true);
            logger.info("def_cluster_2 : " + Util.prettyPrintXml(def_cluster_2));

            // verify new bundle in cluster_2 and no new bundle in cluster_1  and
            OozieUtil.verifyNewBundleCreation(cluster_1, newBundleID_cluster1,
                    oldNominalTimes_cluster1,
                    oldProcess, false, false);

            OozieUtil.verifyNewBundleCreation(cluster_2, oldBundleID_cluster2,
                    oldNominalTimes_cluster2,
                    oldProcess, true, false);

            //wait till update time is reached
            TimeUtil.sleepTill(cluster_1, updateTime);

            OozieUtil.verifyNewBundleCreation(cluster_2, oldBundleID_cluster2,
                    oldNominalTimes_cluster2,
                    oldProcess, true, true);

            OozieUtil.verifyNewBundleCreation(cluster_1, oldBundleID_cluster1,
                    oldNominalTimes_cluster1,
                    oldProcess, true, true);
        } finally {
            Util.restartService(cluster_2.getProcessHelper());
        }
    }

    @Test(groups = {"MultiCluster", "0.3.1"}, timeOut = 1200000,
            enabled = true)
    public void inNextFewMinutesUpdate_RollForward_Feed()
    throws InterruptedException, JAXBException, ParseException, IOException, URISyntaxException,
    JSchException, OozieClientException, SAXException, AuthenticationException {
        try {
            String startTimeCluster_source = TimeUtil.getTimeWrtSystemTime(-18);

            String feed = getMultiClusterFeed(startTimeCluster_source, startTimeCluster_source);

            logger.info("feed: " + feed);

            //submit feed on all 3 clusters
            ServiceResponse r = prism.getFeedHelper().submitEntity(Util.URLS.SUBMIT_URL, feed);
            AssertUtil.assertSucceeded(r);

            //schedule feed of cluster_1 and cluster_2
            r = cluster_1.getFeedHelper().schedule(Util.URLS.SCHEDULE_URL, feed);
            AssertUtil.assertSucceeded(r);
            r = cluster_2.getFeedHelper().schedule(Util.URLS.SCHEDULE_URL, feed);
            AssertUtil.assertSucceeded(r);

            InstanceUtil.waitTillInstancesAreCreated(cluster_1,
                    feed, 0, 10);

            //shutdown cluster_2
            Util.shutDownService(cluster_2.getProcessHelper());

            //add some property to feed so that new bundle is created
            String updatedFeed = Util.setFeedProperty(feed, "someProp", "someVal");

            //save old data
            String oldBundle_cluster1 = InstanceUtil.getLatestBundleID(cluster_1,
                    Util.readEntityName(feed), ENTITY_TYPE.FEED);

            List<String> oldNominalTimes_cluster1 = OozieUtil.getActionsNominalTime
                    (cluster_1,
                            oldBundle_cluster1, ENTITY_TYPE.FEED);

            //send update command with +5 mins in future
            String updateTime = TimeUtil.getTimeWrtSystemTime(5);
            r = prism.getFeedHelper().update(feed, updatedFeed, updateTime, null);
            AssertUtil.assertPartial(r);

            //verify new bundle creation on cluster_1 and new definition on cluster_3
            OozieUtil.verifyNewBundleCreation(cluster_1, oldBundle_cluster1,
                    oldNominalTimes_cluster1,
                    feed, true, false);


            String definition = Util.getEntityDefinition(cluster_3, feed, true);
            Diff diff = XMLUnit.compareXML(definition, processBundle.getProcessData());
            logger.info(diff);

            //start stopped cluster_2
            Util.startService(cluster_2.getProcessHelper());

            String newBundle_cluster1 = InstanceUtil.getLatestBundleID(cluster_1,
                    Util.readEntityName(feed), ENTITY_TYPE.FEED);

            //send update again
            r = prism.getFeedHelper().update(feed, updatedFeed, updateTime);
            AssertUtil.assertSucceeded(r);

            //verify new bundle creation on cluster_2 and no new bundle on cluster_1
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(cluster_2.getFeedHelper(), Util.readDatasetName(feed),
                            "RETENTION"), 2);

            OozieUtil.verifyNewBundleCreation(cluster_1, newBundle_cluster1,
                    oldNominalTimes_cluster1,
                    feed, false, false);
            //wait till update time is reached
            TimeUtil.sleepTill(cluster_1, updateTime);

            //verify new bundle creation with instance matching
            OozieUtil.verifyNewBundleCreation(cluster_1, oldBundle_cluster1,
                    oldNominalTimes_cluster1,
                    feed, true, true);

        } finally {
            Util.restartService(cluster_2.getProcessHelper());
        }
    }


    @Test(groups = {"multiCluster", "0.3.1"}, timeOut = 1200000,
            enabled = true)
    public void updateTimeAfterEndTime_Process()
    throws JAXBException, ParseException, InterruptedException, IOException, URISyntaxException,
    OozieClientException, IllegalAccessException, NoSuchMethodException, InvocationTargetException,
    AuthenticationException {

    /*
      submit and schedule process with end time after 60 mins. Set update time
       as with +60 from start mins.
     */
        String startTime = TimeUtil.getTimeWrtSystemTime(-15);
        String endTime = TimeUtil.getTimeWrtSystemTime(60);
        processBundle.setProcessValidity(startTime, endTime);
        processBundle.submitAndScheduleBundle(prism);
        Thread.sleep(10000);

        InstanceUtil.waitTillInstanceReachState(serverOC.get(0),
                Util.readEntityName(processBundle.getProcessData()), 0,
                CoordinatorAction.Status.WAITING, 2, ENTITY_TYPE.PROCESS);

        //save old data
        String oldProcess = processBundle.getProcessData();

        String oldBundleID = InstanceUtil
                .getLatestBundleID(cluster_1,
                        Util.readEntityName(oldProcess), ENTITY_TYPE.PROCESS);

        List<String> oldNominalTimes = OozieUtil.getActionsNominalTime(cluster_1, oldBundleID,
                ENTITY_TYPE.PROCESS);

        //update
        processBundle.setProcessProperty("someProp", "someVal");
        String updateTime = TimeUtil.addMinsToTime(endTime, 60);

        logger.info("Original Feed : " + Util.prettyPrintXml(oldProcess));
        logger.info("Updated Feed :" + Util.prettyPrintXml(processBundle.getProcessData()));
        logger.info("Update Time : " + updateTime);


        ServiceResponse r = prism.getProcessHelper().update(oldProcess,
                processBundle.getProcessData(), updateTime);
        AssertUtil.assertSucceeded(r);

        //verify new bundle creation with instances matching
        OozieUtil.verifyNewBundleCreation(cluster_1, oldBundleID, oldNominalTimes,
                oldProcess, true, false);

        InstanceUtil.waitTillInstancesAreCreated(cluster_1,
                processBundle.getProcessData(), 1, 10);

        OozieUtil.verifyNewBundleCreation(cluster_1, oldBundleID, oldNominalTimes,
                oldProcess, true, true);
    }

    @Test(groups = {"multiCluster", "0.3.1"}, timeOut = 1200000,
            enabled = true)
    public void updateTimeAfterEndTime_Feed()
    throws ParseException, JAXBException, IOException, OozieClientException, InterruptedException,
    URISyntaxException, AuthenticationException {
    /*
    submit and schedule feed with end time 60 mins in future and update with
    +60
     in future.
     */
        String startTime = TimeUtil.getTimeWrtSystemTime(-15);
        String endTime = TimeUtil.getTimeWrtSystemTime(60);

        String feed = processBundle.getDataSets().get(0);
        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2012-10-01T12:00Z", "2010-01-01T00:00Z"),
                XmlUtil.createRtention("days(100000)", ActionType.DELETE), null,
                ClusterType.SOURCE, null);

        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTime, endTime),
                XmlUtil.createRtention("days(100000)", ActionType.DELETE),
                Util.readClusterName(processBundle.getClusters().get(0)), ClusterType.SOURCE,
                null, baseTestDir + "/replication" + dateTemplate);


        ServiceResponse r = prism.getClusterHelper().submitEntity(Util.URLS.SUBMIT_URL,
                processBundle.getClusters().get(0));
        AssertUtil.assertSucceeded(r);
        r = prism.getFeedHelper().submitAndSchedule(Util.URLS
                .SUBMIT_AND_SCHEDULE_URL, feed);
        AssertUtil.assertSucceeded(r);

        InstanceUtil.waitTillInstancesAreCreated(cluster_1, feed, 0, 10);
        //save old data

        String oldBundleID = InstanceUtil
                .getLatestBundleID(cluster_1,
                        Util.readEntityName(feed), ENTITY_TYPE.FEED);

        String updateTime = TimeUtil.addMinsToTime(endTime, 60);
        String updatedFeed = Util.setFeedProperty(feed, "someProp", "someVal");

        logger.info("Original Feed : " + Util.prettyPrintXml(feed));
        logger.info("Updated Feed :" + Util.prettyPrintXml(updatedFeed));
        logger.info("Update Time : " + updateTime);

        r = prism.getFeedHelper().update(feed, updatedFeed, updateTime);
        AssertUtil.assertSucceeded(r);
        InstanceUtil.waitTillInstancesAreCreated(cluster_1, feed, 1, 10);

        //verify new bundle creation
        OozieUtil.verifyNewBundleCreation(cluster_1, oldBundleID, null,
                feed, true, false);
    }

    @Test(groups = {"multiCluster", "0.3.1"}, timeOut = 1200000,
            enabled = true)
    public void updateTimeBeforeStartTime_Process() throws JAXBException,
    ParseException, InterruptedException, IOException, URISyntaxException, OozieClientException,
    AuthenticationException {

    /*
      submit and schedule process with start time +10 mins from now. Update
      with start time -4 and update time +2 mins
     */
        String startTime = TimeUtil.getTimeWrtSystemTime(10);
        String endTime = TimeUtil.getTimeWrtSystemTime(20);
        processBundle.setProcessValidity(startTime, endTime);
        processBundle.submitAndScheduleBundle(prism);
        //save old data
        String oldProcess = processBundle.getProcessData();
        String oldBundleID = InstanceUtil
                .getLatestBundleID(cluster_1,
                        Util.readEntityName(oldProcess), ENTITY_TYPE.PROCESS);
        List<String> oldNominalTimes = OozieUtil.getActionsNominalTime(cluster_1, oldBundleID,
                ENTITY_TYPE.PROCESS);

        processBundle.setProcessValidity(TimeUtil.addMinsToTime(startTime, -4),
                endTime);
        String updateTime = TimeUtil.getTimeWrtSystemTime(2);
        ServiceResponse r = prism.getProcessHelper().update(oldProcess,
                processBundle.getProcessData(), updateTime);
        AssertUtil.assertSucceeded(r);
        Thread.sleep(10000);
        //verify new bundle creation
        OozieUtil.verifyNewBundleCreation(cluster_1, oldBundleID, oldNominalTimes,
                oldProcess, true, false);

    }

    @Test(groups = {"MultiCluster", "0.3.1"}, timeOut = 1200000,
            enabled = true)
    public void updateDiffClusterDiffValidity_Process()
    throws JAXBException, ParseException, InterruptedException, IOException, URISyntaxException,
    OozieClientException, IllegalAccessException, NoSuchMethodException, InvocationTargetException,
    JSchException, AuthenticationException {

        //set start end process time for 3 clusters
        String startTime_cluster1 = TimeUtil.getTimeWrtSystemTime(-40);
        String endTime_cluster1 = TimeUtil.getTimeWrtSystemTime(6);
        String startTime_cluster2 = TimeUtil.getTimeWrtSystemTime(120);
        String endTime_cluster2 = TimeUtil.getTimeWrtSystemTime(240);
        String startTime_cluster3 = TimeUtil.getTimeWrtSystemTime(-30);
        String endTime_cluster3 = TimeUtil.getTimeWrtSystemTime(180);


        //create multi cluster bundle
        processBundle.setProcessValidity(startTime_cluster1,
                endTime_cluster1);
        processBundle.addClusterToBundle(bundles[1].getClusters().get(0),
                ClusterType.SOURCE, startTime_cluster2, endTime_cluster2);
        processBundle.addClusterToBundle(bundles[2].getClusters().get(0),
                ClusterType.SOURCE, startTime_cluster3, endTime_cluster3);

        //submit and schedule
        processBundle.submitAndScheduleBundle(prism);

        //wait for coord to be in running state
        InstanceUtil.waitTillInstancesAreCreated(cluster_1,
                processBundle.getProcessData(), 0, 10);
        InstanceUtil.waitTillInstancesAreCreated(cluster_3,
                processBundle.getProcessData(), 0, 10);

        //save old info
        String oldBundleID_cluster1 = InstanceUtil
                .getLatestBundleID(cluster_1,
                        Util.readEntityName(processBundle.getProcessData()), ENTITY_TYPE.PROCESS);
        List<String> nominalTimes_cluster1 =
                OozieUtil.getActionsNominalTime(cluster_1, oldBundleID_cluster1,
                        ENTITY_TYPE.PROCESS);
        String oldBundleID_cluster2 = InstanceUtil
                .getLatestBundleID(cluster_2,
                        Util.readEntityName(processBundle.getProcessData()), ENTITY_TYPE.PROCESS);
        String oldBundleID_cluster3 = InstanceUtil
                .getLatestBundleID(cluster_3,
                        Util.readEntityName(processBundle.getProcessData()), ENTITY_TYPE.PROCESS);
        List<String> nominalTimes_cluster3 = OozieUtil.getActionsNominalTime
                (cluster_3, oldBundleID_cluster3,
                        ENTITY_TYPE.PROCESS);


        //update process
        String updateTime = TimeUtil.addMinsToTime(endTime_cluster1, 4);
        processBundle.setProcessProperty("someProp", "someVal");
        ServiceResponse r = prism.getProcessHelper().update(processBundle.getProcessData(),
                processBundle.getProcessData(), updateTime);
        AssertUtil.assertSucceeded(r);

        //check for new bundle to be created
        OozieUtil.verifyNewBundleCreation(cluster_1, oldBundleID_cluster1, nominalTimes_cluster1,
                processBundle.getProcessData(), true, false);
        OozieUtil.verifyNewBundleCreation(cluster_3, oldBundleID_cluster3,
                nominalTimes_cluster3,
                processBundle.getProcessData(), true, false);
        OozieUtil.verifyNewBundleCreation(cluster_2, oldBundleID_cluster2,
                nominalTimes_cluster3,
                processBundle.getProcessData(), true, false);

        //wait till new coord are running on Cluster1
        InstanceUtil.waitTillInstancesAreCreated(cluster_1,
                processBundle.getProcessData(), 1, 10);
        OozieUtil.verifyNewBundleCreation(cluster_1, oldBundleID_cluster1, nominalTimes_cluster1,
                processBundle.getProcessData(), true, true);

        //verify
        String coordStartTime_cluster3 = OozieUtil.getCoordStartTime(cluster_3,
                processBundle.getProcessData(), 1);
        String coordStartTime_cluster2 = OozieUtil.getCoordStartTime(cluster_2,
                processBundle.getProcessData(), 1);

        if (!(TimeUtil.oozieDateToDate(coordStartTime_cluster3).isAfter
                (TimeUtil.oozieDateToDate(updateTime)) || TimeUtil
                .oozieDateToDate(coordStartTime_cluster3).isEqual
                        (TimeUtil.oozieDateToDate(updateTime))))
            Assert.assertTrue(false, "new coord start time is not correct");

        if (TimeUtil.oozieDateToDate(coordStartTime_cluster2).isEqual
                (TimeUtil.oozieDateToDate(updateTime)))
            Assert.assertTrue(false, "new coord start time is not correct");

        TimeUtil.sleepTill(cluster_3, updateTime);

        InstanceUtil.waitTillInstancesAreCreated(cluster_3,
                processBundle.getProcessData(), 1, 10);

        //verify that no instance are missing
        OozieUtil.verifyNewBundleCreation(cluster_3, oldBundleID_cluster3,
                nominalTimes_cluster3,
                processBundle.getProcessData(), true, true);
    }

    private String submitAndScheduleFeed(Bundle b)
    throws ParseException, JAXBException, IOException, URISyntaxException, AuthenticationException {
        String feed = b.getDataSets().get(0);
        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2012-10-01T12:00Z", "2010-01-01T00:00Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE), null,
                ClusterType.SOURCE, null);
        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity
                ("2012-10-01T12:10Z", "2099-10-01T12:10Z"),
                XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
                Util.readClusterName(b.getClusters().get(0)), ClusterType.SOURCE, "",
                "/someTestPath" + dateTemplate);
        ServiceResponse r = prism.getClusterHelper().submitEntity(Util.URLS
                .SUBMIT_URL,
                b.getClusters().get(0));
        AssertUtil.assertSucceeded(r);
        r = prism.getFeedHelper().submitAndSchedule(Util.URLS
                .SUBMIT_AND_SCHEDULE_URL, feed);
        AssertUtil.assertSucceeded(r);

        return feed;
    }


    private String getMultiClusterFeed(String startTimeCluster_source,
                                       String startTimeCluster_target)
    throws ParseException, IOException, InterruptedException, JAXBException, URISyntaxException,
    AuthenticationException {
        String testDataDir = baseTestDir + "/replication";

        //create desired feed
        String feed = bundles[0].getDataSets().get(0);

        //cluster_1 is target, cluster_2 is source and cluster_3 is neutral

        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2012-10-01T12:00Z", "2010-01-01T00:00Z"),
                XmlUtil.createRtention("days(100000)", ActionType.DELETE), null,
                ClusterType.SOURCE, null);

        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity(startTimeCluster_source, "2099-10-01T12:10Z"),
                XmlUtil.createRtention("days(100000)", ActionType.DELETE),
                Util.readClusterName(bundles[2].getClusters().get(0)), null, null);

        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity(startTimeCluster_target, "2099-10-01T12:25Z"),
                XmlUtil.createRtention("days(100000)", ActionType.DELETE),
                Util.readClusterName(bundles[0].getClusters().get(0)), ClusterType.TARGET,
                null,
                testDataDir + dateTemplate);

        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity(startTimeCluster_source, "2099-01-01T00:00Z"),
                XmlUtil.createRtention("days(100000)", ActionType.DELETE),
                Util.readClusterName(bundles[1].getClusters().get(0)), ClusterType.SOURCE,
                null, testDataDir + dateTemplate);

        //submit clusters
        Bundle.submitCluster(bundles[0], bundles[1], bundles[2]);

        //create test data on cluster_2
        TimeUtil.createDataWithinDatesAndPrefix(cluster_2,
                TimeUtil.oozieDateToDate(startTimeCluster_source),
                TimeUtil.oozieDateToDate(TimeUtil.getTimeWrtSystemTime(60)),
                testDataDir, 1);

        return feed;
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown(Method method)
    throws JAXBException, IOException, URISyntaxException, JSchException, InterruptedException {
        logger.info("tearDown " + method.getName());
        processBundle.deleteBundle(prism);
        bundles[0].deleteBundle(prism);
        processBundle.deleteBundle(prism);
    }
}