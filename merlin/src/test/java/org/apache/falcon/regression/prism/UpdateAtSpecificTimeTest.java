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
import org.apache.hadoop.security.authentication.client.AuthenticationException;
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
  }

  @AfterMethod(alwaysRun = true)
  public void tearDown(Method method) throws JAXBException, IOException, URISyntaxException {
    Util.print("tearDown " + method.getName());
    removeBundles(bundle1, bundle2, bundle3);
  }

  @Test(groups = {"singleCluster", "0.3.1"}, timeOut = 1200000, enabled = false)
  public void invalidChar_Process()
  throws JAXBException, ParseException, InterruptedException, IOException, URISyntaxException,
  AuthenticationException {
    bundle1.setProcessValidity(InstanceUtil.getTimeWrtSystemTime(0),
      InstanceUtil.getTimeWrtSystemTime(20));
    bundle1.submitAndScheduleBundle(prism);
    String oldProcess = bundle1.getProcessData();
    bundle1.setProcessValidity(InstanceUtil.getTimeWrtSystemTime(5),
      InstanceUtil.getTimeWrtSystemTime(100));
    ServiceResponse r = prism.getProcessHelper().update(oldProcess,
      bundle1.getProcessData(), "abc");
    Assert.assertTrue(r.getMessage().contains("java.lang.IllegalArgumentException: abc is not a valid UTC string"));
  }

  @Test(groups = {"singleCluster", "0.3.1"}, timeOut = 1200000, enabled = false)
  public void invalidChar_Feed() throws Exception {

    String feed = submitAndScheduleFeed(bundle1);

    //update frequency
    Frequency f = new Frequency(21, Frequency.TimeUnit.minutes);
    String updatedFeed = InstanceUtil.setFeedFrequency(feed, f);

    ServiceResponse r = prism.getFeedHelper().update(feed, updatedFeed, "abc");
    Assert.assertTrue(r.getMessage().contains("java.lang.IllegalArgumentException: abc is not a valid UTC string"));
  }


  @Test(groups = {"singleCluster", "0.3.1"}, timeOut = 1200000, enabled = false)
  public void updateTimeInPast_Process() throws Exception {
    bundle1.setProcessValidity(InstanceUtil.getTimeWrtSystemTime(0),
      InstanceUtil.getTimeWrtSystemTime(20));
    bundle1.submitAndScheduleBundle(prism);
    Thread.sleep(15000);
    //get old process details
    String oldProcess = bundle1.getProcessData();

    String oldBundleId = InstanceUtil
      .getLatestBundleID(cluster_1,
        Util.readEntityName(bundle1.getProcessData()), ENTITY_TYPE.PROCESS);

    List<String> initialNominalTimes = Util.getActionsNominalTime(cluster_1,
      oldBundleId,ENTITY_TYPE.PROCESS);

    // update process by changing process validity
    bundle1.setProcessValidity(InstanceUtil.getTimeWrtSystemTime(5),
      InstanceUtil.getTimeWrtSystemTime(100));
    ServiceResponse r = prism.getProcessHelper().update(oldProcess,
      bundle1.getProcessData(), InstanceUtil.getTimeWrtSystemTime(-10000));
    AssertUtil.assertSucceeded(r);

    //check new coord created with current time

    Util.verifyNewBundleCreation(cluster_1, oldBundleId, initialNominalTimes,
      Util.readEntityName(bundle1.getProcessData()), true, ENTITY_TYPE.PROCESS);
  }

  @Test(groups = {"MultiCluster", "0.3.1"}, timeOut = 1200000,
    enabled = true)

  public void updateTimeInPast_Feed() throws Exception {


    String startTimeCluster_source = InstanceUtil.getTimeWrtSystemTime(-10);
    String startTimeCluster_target = InstanceUtil.getTimeWrtSystemTime(5);

    String testDataDir = inputPath + "/replication";

    //create test data on cluster_2
    InstanceUtil.createDataWithinDatesAndPrefix(cluster_2,
      InstanceUtil.oozieDateToDate(startTimeCluster_source),
      InstanceUtil.oozieDateToDate(InstanceUtil.getTimeWrtSystemTime(60)),
      testDataDir, 1);

    //submit clusters
    Bundle.submitCluster(bundle1, bundle2, bundle3);

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

    Util.print("feed: " + feed);


    //submit and schedule feed
    ServiceResponse r = prism.getFeedHelper().submitEntity(Util.URLS.SUBMIT_AND_SCHEDULE_URL, feed);
    Thread.sleep(10000);
    AssertUtil.assertSucceeded(r);

   try {
    //save initial bundle info
    String oldBundleId = InstanceUtil
      .getLatestBundleID(cluster_1,
        Util.readEntityName(feed), ENTITY_TYPE.FEED);

    List<String> initialNominalTimes = Util.getActionsNominalTime(cluster_1,
      oldBundleId,ENTITY_TYPE.FEED);


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
      Util.readEntityName(feed), true,ENTITY_TYPE.FEED);
   }
   finally{
     prism.getFeedHelper().delete(Util.URLS.DELETE_URL,feed);
   }

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
}
