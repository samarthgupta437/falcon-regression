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
import org.apache.falcon.regression.core.generated.feed.ActionType;
import org.apache.falcon.regression.core.generated.feed.ClusterType;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.InstancesSummaryResult;
import org.apache.falcon.regression.core.enumsAndConstants.ENTITY_TYPE;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.XmlUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.oozie.client.CoordinatorAction.Status;
import org.apache.oozie.client.OozieClientException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
  /*
  this test currently provide minimum verification. More detailed test should
   be added
   */
public class InstanceSummaryTest extends BaseTestClass {

  //1. process : test summary single cluster few instance some future some past
  //2. process : test multiple cluster, full past on one cluster,
  // full future on one cluster, half future / past on third one

  // 3. feed : same as test 1 for feed
  // 4. feed : same as test 2 for feed



  String testDir = "/ProcessInstanceKillsTest";
  String baseTestHDFSDir = baseHDFSDir + testDir;
  String feedInputPath = baseTestHDFSDir + "/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
  String startTime ;
  String endTime ;

  ColoHelper cluster1 = servers.get(0);
  ColoHelper cluster2 = servers.get(1);
  ColoHelper cluster3 = servers.get(2);

  Bundle processBundle ;
  Bundle bundle1 ;
  Bundle bundle2 ;
  Bundle bundle3 ;

  @BeforeClass(alwaysRun = true)
  public void createTestData() throws Exception {

    startTime = TimeUtil.get20roundedTime(InstanceUtil
      .getTimeWrtSystemTime
        (-20));
    endTime = InstanceUtil.getTimeWrtSystemTime(60);
    String startTimeData = InstanceUtil.addMinsToTime(startTime,-100);
    List<String> dataDates = Util.getMinuteDatesOnEitherSide(InstanceUtil
      .oozieDateToDate(startTimeData),InstanceUtil.oozieDateToDate(endTime),
      20);

    for (int i = 0; i < dataDates.size(); i++)
      dataDates.set(i, Util.getPathPrefix(feedInputPath) + dataDates.get(i));

    ArrayList<String> dataFolder = new ArrayList<String>();

    for (String dataDate : dataDates) {
      dataFolder.add(dataDate);
    }

    for(FileSystem fs : serverFS) {
      HadoopUtil.deleteDirIfExists(Util.getPathPrefix(feedInputPath),fs);
      HadoopUtil.flattenAndPutDataInFolder(fs, "src/test/resources/OozieExampleInputData/normalInput", dataFolder);
    }
  }

  @BeforeMethod(alwaysRun = true)
  public void setup(Method method) throws Exception {
    Util.print("test name: " + method.getName());
    processBundle = Util.readELBundles()[0][0];
    processBundle = new Bundle(processBundle, cluster3.getEnvFileName(),
      cluster3.getPrefix());
    processBundle.setInputFeedDataPath(feedInputPath);

    bundle1 = new Bundle(processBundle, cluster1.getEnvFileName(), cluster1.getPrefix());
    bundle2 = new Bundle(processBundle, cluster2.getEnvFileName(), cluster2.getPrefix());
    bundle3 = new Bundle(processBundle, cluster3.getEnvFileName(), cluster3.getPrefix());
  }

  @Test(enabled = true,timeOut = 1200000 )
  public void testSummarySingleClusterProcess() throws InterruptedException, URISyntaxException, JAXBException, IOException, ParseException, OozieClientException, AuthenticationException {
    processBundle.setProcessValidity(startTime,endTime);
    processBundle.submitAndScheduleBundle(prism);

    InstanceUtil.waitTillInstancesAreCreated(cluster3,
      processBundle.getProcessData(),0,10);

    InstanceUtil.waitTillInstanceReachState(serverOC.get(2),
      Util.readEntityName(processBundle.getProcessData()), 2,
      Status.SUCCEEDED, 10, ENTITY_TYPE.PROCESS);

    // start only at start time
    InstancesSummaryResult r = prism.getProcessHelper()
      .getInstanceSummary(Util.readEntityName(processBundle.getProcessData()),
        "?start=" + startTime);
    //AssertUtil.assertSucceeded(r);

    //start only before process start
    r = prism.getProcessHelper()
      .getInstanceSummary(Util.readEntityName(processBundle.getProcessData()),
        "?start=" + InstanceUtil.addMinsToTime(startTime, -100));
    //AssertUtil.assertFailed(r,"response should have failed");

    //start only after process end
    r = prism.getProcessHelper()
      .getInstanceSummary(Util.readEntityName(processBundle.getProcessData()),
        "?start=" + InstanceUtil.addMinsToTime(startTime,120));


    //start only at mid specific instance
    r = prism.getProcessHelper()
      .getInstanceSummary(Util.readEntityName(processBundle.getProcessData()),
        "?start=" + InstanceUtil.addMinsToTime(startTime,
          +10));

    //start only in between 2 instance
    r = prism.getProcessHelper()
      .getInstanceSummary(Util.readEntityName(processBundle.getProcessData()),
        "?start=" + InstanceUtil.addMinsToTime(startTime,
          7));

    //start and end at start and end
     r = prism.getProcessHelper()
      .getInstanceSummary(Util.readEntityName(processBundle.getProcessData()),
        "?start=" +startTime + "&end=" +endTime);

    //start in between and end at end
    r = prism.getProcessHelper()
      .getInstanceSummary(Util.readEntityName(processBundle.getProcessData()),
        "?start=" + InstanceUtil.addMinsToTime(startTime,
          14) + "&end=" + endTime);

    //start at start and end between
    r = prism.getProcessHelper()
      .getInstanceSummary(Util.readEntityName(processBundle.getProcessData()),
        "?start=" + startTime + "&end=" + InstanceUtil.addMinsToTime(endTime,
          -20));

    // start and end in between
    r = prism.getProcessHelper()
      .getInstanceSummary(Util.readEntityName(processBundle.getProcessData()),
        "?start=" + InstanceUtil.addMinsToTime(startTime,
          20) + "&end=" + InstanceUtil.addMinsToTime(endTime, -13));

    //start before start with end in between
    r = prism.getProcessHelper()
      .getInstanceSummary(Util.readEntityName(processBundle.getProcessData()),
        "?start=" + InstanceUtil.addMinsToTime(startTime,
          -100) + "&end=" + InstanceUtil.addMinsToTime(endTime, -37));

    //start in between and end after end
    r = prism.getProcessHelper()
      .getInstanceSummary(Util.readEntityName(processBundle.getProcessData()),
        "?start=" + InstanceUtil.addMinsToTime(startTime,
          60) + "&end=" + InstanceUtil.addMinsToTime(endTime, 100));

    // both start end out od range
    r = prism.getProcessHelper()
      .getInstanceSummary(Util.readEntityName(processBundle.getProcessData()),
        "?start=" + InstanceUtil.addMinsToTime(startTime,
          -100) + "&end=" + InstanceUtil.addMinsToTime(endTime, 100));

    // end only
    r = prism.getProcessHelper()
      .getInstanceSummary(Util.readEntityName(processBundle.getProcessData()),
        "?end=" + InstanceUtil.addMinsToTime(endTime, -30));
  }

  @Test(enabled = true, timeOut = 1200000 )
  public void testSummaryMultiClusterProcess() throws JAXBException,
    ParseException, InterruptedException, IOException, URISyntaxException, AuthenticationException {
    processBundle.setProcessValidity(startTime,endTime);
    processBundle.addClusterToBundle(bundle2.getClusters().get(0),
      ClusterType.SOURCE, null, null);
    processBundle.addClusterToBundle(bundle3.getClusters().get(0),
      ClusterType.SOURCE, null, null);
    processBundle.submitAndScheduleBundle(prism);
    InstancesSummaryResult r = prism.getProcessHelper()
      .getInstanceSummary(Util.readEntityName(processBundle.getProcessData()),
        "?start=" + startTime);

    r = prism.getProcessHelper()
      .getInstanceSummary(Util.readEntityName(processBundle.getProcessData()),
        "?start=" + startTime + "&end=" + endTime);


    r = prism.getProcessHelper()
      .getInstanceSummary(Util.readEntityName(processBundle.getProcessData()),
        "?start=" + startTime + "&end=" + endTime);



    r = prism.getProcessHelper()
      .getInstanceSummary(Util.readEntityName(processBundle.getProcessData()),
        "?start=" + startTime + "&end=" + endTime);



    r = prism.getProcessHelper()
      .getInstanceSummary(Util.readEntityName(processBundle.getProcessData()),
        "?start=" + startTime + "&end=" + endTime);


    r = prism.getProcessHelper()
      .getInstanceSummary(Util.readEntityName(processBundle.getProcessData()),
        "?start=" + startTime + "&end=" + endTime);


    r = prism.getProcessHelper()
      .getInstanceSummary(Util.readEntityName(processBundle.getProcessData()),
        "?start=" + startTime + "&end=" + endTime);
  }

    @Test(enabled = true, timeOut = 1200000 )
    public void testSummaryMultiClusterFeed() throws JAXBException,
      ParseException, InterruptedException, IOException, URISyntaxException, OozieClientException, AuthenticationException {
      bundle1.generateUniqueBundle();
      bundle2.generateUniqueBundle();
      bundle3.generateUniqueBundle();

      //create desired feed
      String feed = bundle1.getDataSets().get(0);

      //cluster_1 is target, cluster_2 is source and cluster_3 is neutral

      feed = InstanceUtil.setFeedCluster(feed,
        XmlUtil.createValidity("2012-10-01T12:00Z", "2010-01-01T00:00Z"),
        XmlUtil.createRtention("days(100000)", ActionType.DELETE), null,
        ClusterType.SOURCE, null);

      feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTime, "2099-10-01T12:10Z"),
        XmlUtil.createRtention("days(100000)", ActionType.DELETE),
        Util.readClusterName(bundle3.getClusters().get(0)), null, null);

      feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTime, "2099-10-01T12:25Z"),
        XmlUtil.createRtention("days(100000)", ActionType.DELETE),
        Util.readClusterName(bundle1.getClusters().get(0)), ClusterType.TARGET,
        null,
        feedInputPath);

      feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
        XmlUtil.createRtention("days(100000)", ActionType.DELETE),
        Util.readClusterName(bundle2.getClusters().get(0)), ClusterType.SOURCE,
        null, feedInputPath);

      //submit clusters
      Bundle.submitCluster(bundle1, bundle2, bundle3);

      //create test data on cluster_2
      /*InstanceUtil.createDataWithinDatesAndPrefix(cluster2,
        InstanceUtil.oozieDateToDate(startTime),
        InstanceUtil.oozieDateToDate(InstanceUtil.getTimeWrtSystemTime(60)),
        feedInputPath, 1);*/

      //submit and schedule feed
       prism.getFeedHelper().submitAndSchedule(Util.URLS
        .SUBMIT_AND_SCHEDULE_URL, feed);

      InstancesSummaryResult r = prism.getFeedHelper()
        .getInstanceSummary(Util.readEntityName(feed),
          "?start=" + startTime);

      r = prism.getFeedHelper()
        .getInstanceSummary(Util.readEntityName(feed),
          "?start=" + startTime + "&end=" + InstanceUtil.addMinsToTime(endTime,
            -20));

    }



  @AfterMethod
  public void tearDown() throws IOException {
    processBundle.deleteBundle(prism);
    for(FileSystem fs : serverFS) {
      HadoopUtil.deleteDirIfExists(Util.getPathPrefix(feedInputPath), fs);
    }
  }
}
