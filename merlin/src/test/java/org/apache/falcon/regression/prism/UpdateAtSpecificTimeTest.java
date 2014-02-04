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
import org.apache.falcon.regression.core.supportClasses.ENTITY_TYPE;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.XmlUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.custommonkey.xmlunit.Diff;
import org.custommonkey.xmlunit.XMLUnit;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.List;


public class UpdateAtSpecificTimeTest extends BaseTestClass {
  Bundle bundle1 = new Bundle();
  Bundle bundle2 = new Bundle();
  Bundle bundle3 = new Bundle();
  Bundle processBundle = new Bundle();

  ColoHelper cluster_1, cluster_2, cluster_3;
  FileSystem clusterFS_1, clusterFS_2, clusterFS_3;

  private String dateTemplate = "/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
  private final String inputPath = baseHDFSDir +
    "/UpdateAtSpecificTimeTest-data";


  public UpdateAtSpecificTimeTest() throws IOException {
    super();
    cluster_1 = servers.get(0);
    clusterFS_1 = serverFS.get(0);
    cluster_2 = servers.get(1);
    clusterFS_2 = serverFS.get(1);
    cluster_3 = servers.get(2);
    clusterFS_3 = serverFS.get(2);

  }

  @BeforeMethod(alwaysRun = true)
  public void setup(Method method) throws IOException, JAXBException {
    Util.print("test name: " + method.getName());
    Bundle bundle = (Bundle) Bundle.readBundle("LocalDC_feedReplicaltion_BillingRC")[0][0];
    bundle1 = new Bundle(bundle, cluster_1.getEnvFileName(), cluster_1.getPrefix());
    bundle2 = new Bundle(bundle, cluster_2.getEnvFileName(), cluster_2.getPrefix());
    bundle3 = new Bundle(bundle, cluster_3.getEnvFileName(), cluster_3.getPrefix());

    bundle1.generateUniqueBundle();
    bundle2.generateUniqueBundle();
    bundle3.generateUniqueBundle();

    processBundle = Util.readELBundles()[0][0];
    processBundle = new Bundle(processBundle, cluster_1.getEnvFileName(),
      cluster_1.getPrefix());
    processBundle.generateUniqueBundle();
  }


  @Test(groups = {"singleCluster", "0.3.1"}, timeOut = 1200000, enabled = false)
  public void invalidChar_Process() throws JAXBException, ParseException, InterruptedException, IOException, URISyntaxException {
    processBundle.setProcessValidity(InstanceUtil.getTimeWrtSystemTime(0),
      InstanceUtil.getTimeWrtSystemTime(20));
    processBundle.submitAndScheduleBundle(prism);
    String oldProcess = processBundle.getProcessData();
    processBundle.setProcessValidity(InstanceUtil.getTimeWrtSystemTime(5),
      InstanceUtil.getTimeWrtSystemTime(100));
    ServiceResponse r = prism.getProcessHelper().update(oldProcess,
      processBundle.getProcessData(), "abc");
    Assert.assertTrue(r.getMessage().contains("java.lang.IllegalArgumentException: abc is not a valid UTC string"));
  }

  @Test(groups = {"singleCluster", "0.3.1"}, timeOut = 1200000, enabled = false)
  public void invalidChar_Feed() throws Exception {

    String feed = submitAndScheduleFeed(processBundle);

    //update frequency
    Frequency f = new Frequency(21, Frequency.TimeUnit.minutes);
    String updatedFeed = InstanceUtil.setFeedFrequency(feed, f);

    ServiceResponse r = prism.getFeedHelper().update(feed, updatedFeed, "abc");
    Assert.assertTrue(r.getMessage().contains("java.lang.IllegalArgumentException: abc is not a valid UTC string"));
  }


  @Test(groups = {"singleCluster", "0.3.1"}, timeOut = 1200000, enabled = false)
  public void updateTimeInPast_Process() throws Exception {
    processBundle.setProcessValidity(InstanceUtil.getTimeWrtSystemTime(0),
      InstanceUtil.getTimeWrtSystemTime(20));
    processBundle.submitAndScheduleBundle(prism);
    Thread.sleep(15000);
    //get old process details
    String oldProcess = processBundle.getProcessData();

    String oldBundleId = InstanceUtil
      .getLatestBundleID(cluster_1,
        Util.readEntityName(processBundle.getProcessData()), ENTITY_TYPE.PROCESS);

    List<String> initialNominalTimes = Util.getActionsNominalTime(cluster_1,
      oldBundleId, ENTITY_TYPE.PROCESS);

    // update process by changing process validity
    processBundle.setProcessValidity(InstanceUtil.getTimeWrtSystemTime(5),
      InstanceUtil.getTimeWrtSystemTime(100));
    ServiceResponse r = prism.getProcessHelper().update(oldProcess,
      processBundle.getProcessData(), InstanceUtil.getTimeWrtSystemTime(-10000));
    AssertUtil.assertSucceeded(r);

    //check new coord created with current time

    Util.verifyNewBundleCreation(cluster_1, oldBundleId, initialNominalTimes,
      Util.readEntityName(processBundle.getProcessData()), true,
      ENTITY_TYPE.PROCESS, true);
  }

  @Test(groups = {"MultiCluster", "0.3.1"}, timeOut = 1200000,
    enabled = false)

  public void updateTimeInPast_Feed() throws Exception {


    String startTimeCluster_source = InstanceUtil.getTimeWrtSystemTime(-10);
    String startTimeCluster_target = InstanceUtil.getTimeWrtSystemTime(5);

    String feed = getMultiClusterFeed(startTimeCluster_source, startTimeCluster_target);

    Util.print("feed: " + feed);

    //submit and schedule feed
    ServiceResponse r = prism.getFeedHelper().submitEntity(Util.URLS.SUBMIT_AND_SCHEDULE_URL, feed);
    Thread.sleep(10000);
    AssertUtil.assertSucceeded(r);


    //save initial bundle info
    String oldBundleId = InstanceUtil
      .getLatestBundleID(cluster_1,
        Util.readEntityName(feed), ENTITY_TYPE.FEED);

    List<String> initialNominalTimes = Util.getActionsNominalTime(cluster_1,
      oldBundleId, ENTITY_TYPE.FEED);


    //update frequency
    Frequency f = new Frequency(7, Frequency.TimeUnit.minutes);
    String updatedFeed = InstanceUtil.setFeedFrequency(feed, f);

    r = prism.getFeedHelper().update(feed, updatedFeed,
      InstanceUtil.getTimeWrtSystemTime(-10000));
    AssertUtil.assertSucceeded(r);
    Thread.sleep(60000);

    //check correct number of coord exists or not
    Assert.assertEquals(InstanceUtil
      .checkIfFeedCoordExist(cluster_2.getFeedHelper(), Util.readDatasetName(feed),
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


    //verify some instance of replication has not gone missing
    Util.verifyNewBundleCreation(cluster_1, oldBundleId, initialNominalTimes,
      Util.readEntityName(feed), true, ENTITY_TYPE.FEED, true);

  }


  @Test(groups = {"MultiCluster", "0.3.1"}, timeOut = 1200000,
    enabled = false)
  public void inNextFewMinutesUpdate_RollForward_Process() throws Exception {
    /*
    submit process on 3 clusters. Schedule on 2 clusters. Bring down one of
    the scheduled cluster. Update with time 5 minutes from now. On running
    cluster new coord should be created with start time +5 and no instance
    should be missing. On 3rd cluster where process was only submit,
    definition should be updated. Bring the down cluster up. Update with same
     definition again, now the recently up cluster should also have new
     coords.
     */

    String startTime = InstanceUtil.getTimeWrtSystemTime(-15);
    processBundle.setProcessValidity(startTime,
      InstanceUtil.getTimeWrtSystemTime(60));
    processBundle.addClusterToBundle(bundle2.getClusters().get(0), ClusterType.SOURCE);
    processBundle.addClusterToBundle(bundle3.getClusters().get(0), ClusterType.SOURCE);
    processBundle.submitBundle(prism);

    //schedule of 2 cluster
    cluster_1.getProcessHelper().schedule(Util.URLS.SCHEDULE_URL,
      processBundle.getProcessData());

    cluster_2.getProcessHelper().schedule(Util.URLS.SCHEDULE_URL,
      processBundle.getProcessData());

    Thread.sleep(30000);

    //shut down cluster_2
    Util.shutDownService(cluster_2.getProcessHelper());
    Thread.sleep(5000);

    // save old data before update
    String oldProcess = processBundle.getProcessData();
    String oldBundleID_cluster1 = InstanceUtil
      .getLatestBundleID(cluster_1,
        Util.readEntityName(oldProcess), ENTITY_TYPE.PROCESS);
    String oldBundleID_cluster2 = InstanceUtil
      .getLatestBundleID(cluster_2,
        Util.readEntityName(oldProcess), ENTITY_TYPE.PROCESS);

    List<String> oldNominalTimes_cluster1 = Util.getActionsNominalTime
      (cluster_1,
        oldBundleID_cluster1, ENTITY_TYPE.PROCESS);

    List<String> oldNominalTimes_cluster2 = Util.getActionsNominalTime
      (cluster_2,
        oldBundleID_cluster2, ENTITY_TYPE.PROCESS);

    //update process validity
    processBundle.setProcessValidity(InstanceUtil.addMinsToTime(startTime, 5),
      InstanceUtil.getTimeWrtSystemTime(80));

    //send update request
    String updateTime = InstanceUtil.getTimeWrtSystemTime(5);
    ServiceResponse r = prism.getProcessHelper().update(oldProcess, processBundle.getProcessData(), updateTime
    );
    AssertUtil.assertPartial(r);

    Thread.sleep(15000);
    //verify new bundle on cluster_1 and definition on cluster_3
    Util.verifyNewBundleCreation(cluster_1, oldBundleID_cluster1, oldNominalTimes_cluster1,
      Util.readEntityName(oldProcess), true, ENTITY_TYPE.PROCESS, false);

    Util.verifyNewBundleCreation(cluster_2, oldBundleID_cluster2,
      oldNominalTimes_cluster2,
      Util.readEntityName(oldProcess), false, ENTITY_TYPE.PROCESS, false);

    String definition = Util.getEntityDefinition(cluster_3,
      processBundle.getProcessData(), true);

    Diff diff = XMLUnit.compareXML(definition, processBundle.getProcessData());
    System.out.println(diff);

    //start the stopped cluster_2
    Util.startService(cluster_2.getProcessHelper());
    Thread.sleep(20000);

    String newBundleID_cluster1 = InstanceUtil
      .getLatestBundleID(cluster_1,
        Util.readEntityName(oldProcess), ENTITY_TYPE.PROCESS);

    //send second update request
    r = prism.getProcessHelper().update(oldProcess,
      processBundle.getProcessData(),
      updateTime);
    AssertUtil.assertSucceeded(r);


    // verify new bundle in cluster_2 and no new bundle in cluster_1  and
    // start time of new coord
    Util.verifyNewBundleCreation(cluster_1, newBundleID_cluster1, oldNominalTimes_cluster1,
      Util.readEntityName(oldProcess), false, ENTITY_TYPE.PROCESS, false);

    Util.verifyNewBundleCreation(cluster_2, oldBundleID_cluster2,
      oldNominalTimes_cluster2,
      Util.readEntityName(oldProcess), true, ENTITY_TYPE.PROCESS, false);

    //wait till update time is reached
    InstanceUtil.sleepTill(cluster_1, updateTime);

    Util.verifyNewBundleCreation(cluster_2, oldBundleID_cluster2,
      oldNominalTimes_cluster2,
      Util.readEntityName(oldProcess), true, ENTITY_TYPE.PROCESS, true);

    Util.verifyNewBundleCreation(cluster_1, oldBundleID_cluster1, oldNominalTimes_cluster1,
      Util.readEntityName(oldProcess), true, ENTITY_TYPE.PROCESS, true);

   /*
      String startTime_cluster1 = Util.getCoordStartTime(cluster_1,
      processBundle.getProcessData(),1);

    String startTime_cluster2 = Util.getCoordStartTime(cluster_2,
      processBundle.getProcessData(),1);
    Assert.assertEquals(startTime_cluster1,startTime_cluster2,
      "start time of coords on both the colos should be same");

    Assert.assertEquals(startTime_cluster1,updateTime,
      "start time of coords is not what was given");*/


  }

  @Test(groups = {"MultiCluster", "0.3.1"}, timeOut = 1200000,
    enabled = false)
  public void inNextFewMinutesUpdate_RollForward_Feed() throws Exception {

    String startTimeCluster_source = InstanceUtil.getTimeWrtSystemTime(-18);

    String feed = getMultiClusterFeed(startTimeCluster_source, startTimeCluster_source);

    Util.print("feed: " + feed);

    //submit feed on all 3 clusters
    ServiceResponse r = prism.getFeedHelper().submitEntity(Util.URLS.SUBMIT_URL, feed);
    AssertUtil.assertSucceeded(r);

    //schedule feed of cluster_1 and cluster_2
    r = cluster_1.getFeedHelper().schedule(Util.URLS.SCHEDULE_URL, feed);
    AssertUtil.assertSucceeded(r);
    r = cluster_2.getFeedHelper().schedule(Util.URLS.SCHEDULE_URL, feed);
    AssertUtil.assertSucceeded(r);

    //shutdown cluster_2
    Util.shutDownService(cluster_2.getProcessHelper());

    //add some property to feed so that new bundle is created
    String updatedFeed = Util.setFeedProperty(feed, "someProp", "someVal");

    //save old data
    String oldBundle_cluster1 = InstanceUtil.getLatestBundleID(cluster_1,
      Util.readEntityName(feed), ENTITY_TYPE.FEED);

    List<String> oldNominalTimes_cluster1 = Util.getActionsNominalTime
      (cluster_1,
        oldBundle_cluster1, ENTITY_TYPE.FEED);

    //send update command with +5 mins in future
    String updateTime = InstanceUtil.getTimeWrtSystemTime(5);
    r = prism.getFeedHelper().update(feed, updatedFeed, updateTime);
    AssertUtil.assertPartial(r);

    //verify new bundle creation on cluster_1 and new definition on cluster_3
    Util.verifyNewBundleCreation(cluster_1, oldBundle_cluster1, oldNominalTimes_cluster1,
      Util.readEntityName(feed), true, ENTITY_TYPE.FEED, false);


    String definition = Util.getEntityDefinition(cluster_3, feed, true);
    Diff diff = XMLUnit.compareXML(definition, processBundle.getProcessData());
    System.out.println(diff);

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

    Util.verifyNewBundleCreation(cluster_1, newBundle_cluster1, oldNominalTimes_cluster1,
      Util.readEntityName(feed), false, ENTITY_TYPE.FEED, false);
    //wait till update time is reached
    InstanceUtil.sleepTill(cluster_1, updateTime);

    //verify new bundle creation with instance matching
    Util.verifyNewBundleCreation(cluster_1, oldBundle_cluster1, oldNominalTimes_cluster1,
      Util.readEntityName(feed), true, ENTITY_TYPE.FEED, true);

  }


  @Test(groups = {"MultiCluster", "0.3.1"}, timeOut = 1200000,
    enabled = false)
  public void updateTimeAfterEndTime_Process() throws JAXBException, ParseException, InterruptedException, IOException, URISyntaxException {

    /*
      submit and schedule process with end time after 3 mins. Set update time
       as with +5 mins from now.
     */
    String startTime = InstanceUtil.getTimeWrtSystemTime(-15);
    String endTime = InstanceUtil.getTimeWrtSystemTime(3);
    processBundle.setProcessValidity(startTime, endTime);
    processBundle.submitAndScheduleBundle(prism);
    Thread.sleep(30000);

    String oldProcess = processBundle.getProcessData();
    processBundle.setProcessValidity(InstanceUtil.addMinsToTime(startTime, 3),
      endTime);
    String updateTime = InstanceUtil.addMinsToTime(endTime, 2);
    ServiceResponse r = prism.getProcessHelper().update(oldProcess,
      processBundle.getProcessData(), updateTime);

     /*
     bug:  getting following error "ua3/jobId cannot be null"  and instances
     on oozie have gone in suspended state.
      */

  }

  @Test(groups = {"MultiCluster", "0.3.1"}, timeOut = 1200000,
    enabled = false)
  public void updateTimeAfterEndTime_Feed() throws ParseException, JAXBException, IOException {
    /*
    submit and schedule feed with end time 3 mins in future and update with 5
     in future.
     */
    String startTime = InstanceUtil.getTimeWrtSystemTime(-15);
    String endTime = InstanceUtil.getTimeWrtSystemTime(3);

    String feed = processBundle.getDataSets().get(0);
    feed = InstanceUtil.setFeedCluster(feed,
      XmlUtil.createValidity("2012-10-01T12:00Z", "2010-01-01T00:00Z"),
      XmlUtil.createRtention("days(100000)", ActionType.DELETE), null,
      ClusterType.SOURCE, null, null);

    feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTime, endTime),
      XmlUtil.createRtention("days(100000)", ActionType.DELETE),
      Util.readClusterName(processBundle.getClusters().get(0)), ClusterType.SOURCE,
      null, inputPath + "/replication" + dateTemplate);


    ServiceResponse r = prism.getClusterHelper().submitEntity(Util.URLS.SUBMIT_URL,
      processBundle.getClusters().get(0));
    AssertUtil.assertSucceeded(r);
    r = prism.getFeedHelper().submitAndSchedule(Util.URLS
      .SUBMIT_AND_SCHEDULE_URL, feed);
    AssertUtil.assertSucceeded(r);

    String updateTime = InstanceUtil.addMinsToTime(endTime, 2);
    String updatedFeed = Util.setFeedProperty(feed, "someProp", "someVal");

    r = prism.getFeedHelper().update(feed, updatedFeed, updateTime);

    /*
     bug:  getting following error "ua3/jobId cannot be null"  and instances
     on oozie have gone in suspended state.
      */

  }

  @Test(groups = {"MultiCluster", "0.3.1"}, timeOut = 1200000,
    enabled = false)
  public void updateTimeBeforeStartTime_Process() throws JAXBException,
    ParseException, InterruptedException, IOException, URISyntaxException {

    /*
      submit and schedule process with end time after 3 mins. Set update time
       as with +5 mins from now.
     */
    String startTime = InstanceUtil.getTimeWrtSystemTime(10);
    String endTime = InstanceUtil.getTimeWrtSystemTime(20);
    processBundle.setProcessValidity(startTime, endTime);
    processBundle.submitAndScheduleBundle(prism);
    Thread.sleep(30000);

    String oldProcess = processBundle.getProcessData();
    processBundle.setProcessValidity(InstanceUtil.addMinsToTime(startTime, -2),
      endTime);
    String updateTime = InstanceUtil.addMinsToTime(endTime, -5);
    ServiceResponse r = prism.getProcessHelper().update(oldProcess,
      processBundle.getProcessData(), updateTime);

      /*
      The web service response is <?xml version="1.0" encoding="UTF-8" standalone="yes"?><result><status>FAILED</status><message>ua3/For Job0001039-140114120113803-oozie-oozi-C, actual statuses: PREP, expected statuses: [RUNNING, SUCCEEDED, FAILED, KILLED]</message><requestId>ua3/cd67fb40-ee3e-42e7-9787-8fdedbe76692</requestId></result>

      new bundle got created , but y ????
       */

  }

  @Test(groups = {"MultiCluster", "0.3.1"}, timeOut = 1200000,
    enabled = true)
  public void udpateDiffClusterDiffValidity_Process(){



  }

  @AfterMethod(alwaysRun = true)
  public void tearDown(Method method) throws JAXBException, IOException, URISyntaxException, JSchException, InterruptedException {
    Util.print("tearDown " + method.getName());
    Util.restartService(cluster_2.getProcessHelper());
    bundle1.deleteBundle(prism);
    processBundle.deleteBundle(prism);
    Thread.sleep(30000);
  }


  private String submitAndScheduleFeed(Bundle b) throws Exception {
    String feed = b.getDataSets().get(0);
    feed = InstanceUtil.setFeedCluster(feed,
      XmlUtil.createValidity("2012-10-01T12:00Z", "2010-01-01T00:00Z"),
      XmlUtil.createRtention("days(1000000)", ActionType.DELETE), null,
      ClusterType.SOURCE, null, null);
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


  private String getMultiClusterFeed(String startTimeCluster_source, String startTimeCluster_target) throws ParseException, IOException, InterruptedException, JAXBException {
    String testDataDir = inputPath + "/replication";

    //create desired feed
    String feed = bundle1.getDataSets().get(0);

    //cluster_1 is target, cluster_2 is source and cluster_3 is neutral

    feed = InstanceUtil.setFeedCluster(feed,
      XmlUtil.createValidity("2012-10-01T12:00Z", "2010-01-01T00:00Z"),
      XmlUtil.createRtention("days(100000)", ActionType.DELETE), null,
      ClusterType.SOURCE, null, null);

    feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTimeCluster_source, "2099-10-01T12:10Z"),
      XmlUtil.createRtention("days(100000)", ActionType.DELETE),
      Util.readClusterName(bundle3.getClusters().get(0)), null, null, null);

    feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTimeCluster_target, "2099-10-01T12:25Z"),
      XmlUtil.createRtention("days(100000)", ActionType.DELETE),
      Util.readClusterName(bundle1.getClusters().get(0)), ClusterType.TARGET,
      null,
      testDataDir + dateTemplate);

    feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTimeCluster_source, "2099-01-01T00:00Z"),
      XmlUtil.createRtention("days(100000)", ActionType.DELETE),
      Util.readClusterName(bundle2.getClusters().get(0)), ClusterType.SOURCE,
      null, testDataDir + dateTemplate);


    //submit clusters
    Bundle.submitCluster(bundle1, bundle2, bundle3);

    //create test data on cluster_2
    InstanceUtil.createDataWithinDatesAndPrefix(cluster_2,
      InstanceUtil.oozieDateToDate(startTimeCluster_source),
      InstanceUtil.oozieDateToDate(InstanceUtil.getTimeWrtSystemTime(60)),
      testDataDir, 1);

    return feed;
  }
}
