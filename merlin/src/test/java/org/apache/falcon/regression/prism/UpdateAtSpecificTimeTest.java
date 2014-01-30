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

import junit.framework.Assert;
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
import org.apache.oozie.client.OozieClientException;
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
  Bundle b = new Bundle();
  ColoHelper cluster;
  FileSystem clusterFS;
  private String dateTemplate = "/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
  private final String inputPath = baseHDFSDir +
    "/UpdateAtSpecificTimeTest-data";


  public UpdateAtSpecificTimeTest() throws IOException {
    super();
    cluster = servers.get(0);
    clusterFS = cluster.getClusterHelper().getHadoopFS();

  }

  @BeforeMethod(alwaysRun = true)
  public void setup(Method method) throws IOException, JAXBException {
    Util.print("test name: " + method.getName());
    b = Util.readELBundles()[0][0];
    b = new Bundle(b, cluster.getEnvFileName(), cluster.getPrefix());
  }


  @Test(groups = {"singleCluster", "0.3.1"}, timeOut = 1200000, enabled = false)
  public void invalidChar_Process() throws JAXBException, ParseException, InterruptedException, IOException, URISyntaxException {
    b.setProcessValidity(InstanceUtil.getTimeWrtSystemTime(0),
      InstanceUtil.getTimeWrtSystemTime(20));
    b.submitAndScheduleBundle(prism);
    String oldProcess = b.getProcessData();
    b.setProcessValidity(InstanceUtil.getTimeWrtSystemTime(5),
      InstanceUtil.getTimeWrtSystemTime(100));
    ServiceResponse r = prism.getProcessHelper().update(oldProcess,
      b.getProcessData(), "abc");
    Assert.assertTrue(r.getMessage().contains("java.lang.IllegalArgumentException: abc is not a valid UTC string"));
  }

  @Test(groups = {"singleCluster", "0.3.1"}, timeOut = 1200000, enabled = false)
  public void invalidChar_Feed() throws Exception {

    String feed = submitAndScheduleFeed(b);

    //update frequency
    Frequency f = new Frequency(21, Frequency.TimeUnit.minutes);
    String updatedFeed = InstanceUtil.setFeedFrequency(feed, f);

    ServiceResponse r = prism.getFeedHelper().update(feed, updatedFeed, "abc");
    Assert.assertTrue(r.getMessage().contains("java.lang.IllegalArgumentException: abc is not a valid UTC string"));
  }


  @Test(groups = {"singleCluster", "0.3.1"}, timeOut = 1200000, enabled = false)
  public void updateTimeInPast_Process() throws Exception {
    b.setProcessValidity(InstanceUtil.getTimeWrtSystemTime(0),
      InstanceUtil.getTimeWrtSystemTime(20));
    b.submitAndScheduleBundle(prism);
    Thread.sleep(15000);
    //get old process details
    String oldProcess = b.getProcessData();

    String oldBundleId = InstanceUtil
      .getLatestBundleID(cluster,
        Util.readEntityName(b.getProcessData()), ENTITY_TYPE.PROCESS);

    List<String> initialNominalTimes = Util.getActionsNominalTime(cluster, oldBundleId);

    // update process by changing process validity
    b.setProcessValidity(InstanceUtil.getTimeWrtSystemTime(5),
      InstanceUtil.getTimeWrtSystemTime(100));
    ServiceResponse r = prism.getProcessHelper().update(oldProcess,
      b.getProcessData(), InstanceUtil.getTimeWrtSystemTime(-10000));
    AssertUtil.assertSucceeded(r);

    //check new coord created with current time

    Util.verifyNewBundleCreation(cluster, oldBundleId, initialNominalTimes,
      Util.readEntityName(b.getProcessData()), true);
  }

 /* @Test(groups = {"MultiCluster", "0.3.1"}, timeOut = 1200000,
 enabled = true)

  public void updateTimeInPast_Feed() throws Exception {

    InstanceUtil.createDataWithinDatesAndPrefix(cluster,
      InstanceUtil.oozieDateToDate(InstanceUtil.getTimeWrtSystemTime(-10)),
      InstanceUtil.oozieDateToDate(InstanceUtil.getTimeWrtSystemTime(60)),
      inputPath+"/replication",1);





    //update frequency
    Frequency f = new Frequency(21, Frequency.TimeUnit.minutes);
    String updatedFeed = InstanceUtil.setFeedFrequency(feed, f);

    ServiceResponse r = prism.getFeedHelper().update(feed, updatedFeed,
      InstanceUtil.getTimeWrtSystemTime(-10000));
    AssertUtil.assertSucceeded(r);
    Thread.sleep(15000);


  }  */

  @AfterMethod(alwaysRun = true)
  public void tearDown(Method method) throws JAXBException, IOException, URISyntaxException {
    Util.print("tearDown " + method.getName());
    b.deleteBundle(prism);
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
