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

import org.apache.falcon.regression.Entities.ProcessMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.enumsAndConstants.ENTITY_TYPE;
import org.apache.falcon.regression.core.supportClasses.HadoopFileEditor;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.testng.TestNGException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;

@Test(groups = "distributed")
public class PrismProcessScheduleTest extends BaseTestClass {

  ColoHelper cluster1 = servers.get(0);
  ColoHelper cluster2 = servers.get(1);
  OozieClient cluster1OC = serverOC.get(0);
  OozieClient cluster2OC = serverOC.get(1);

  @BeforeMethod(alwaysRun = true)
  public void setUp(Method method) throws Exception {
    Util.print("test name: " + method.getName());
    Bundle bundle = Util.readBundles("LateDataBundles")[0][0];
    bundles[0] = new Bundle(bundle, cluster2.getEnvFileName(), cluster2.getPrefix());
    bundles[1] = new Bundle(bundle, cluster1.getEnvFileName(), cluster1.getPrefix());
    bundles[0].generateUniqueBundle();
    bundles[1].generateUniqueBundle();
  }

  @AfterMethod
  public void tearDown() throws Exception {
    removeBundles();
  }

  @Test(groups = {"prism", "0.2"})
  public void testProcessScheduleOnBothColos() throws Exception {
    //schedule both bundles
    bundles[0].submitAndScheduleProcess();
    AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.RUNNING);
    AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);

    bundles[1].submitAndScheduleProcess();

    //now check if they have been scheduled correctly or not
    AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);

    //check if there is no criss cross
    AssertUtil.checkNotStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.RUNNING);

  }

  @Test(groups = {"prism", "0.2"})
  public void testScheduleAlreadyScheduledProcessOnBothColos() throws Exception {
    //schedule both bundles
    bundles[0].submitAndScheduleProcess();
    bundles[1].submitAndScheduleProcess();

    //now check if they have been scheduled correctly or not
    AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.RUNNING);
    AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);

    //check if there is no criss cross
    AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);
    AssertUtil.checkNotStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.RUNNING);

    Util.assertSucceeded(cluster2.getProcessHelper()
      .schedule(URLS.SCHEDULE_URL, bundles[0].getProcessData()));
    Util.assertSucceeded(cluster1.getProcessHelper()
      .schedule(URLS.SCHEDULE_URL, bundles[1].getProcessData()));
    //now check if they have been scheduled correctly or not
    AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.RUNNING);
    AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);

  }

  @Test(groups = {"prism", "0.2"})
  public void testScheduleSuspendedProcessOnBothColos() throws Exception {
    //schedule both bundles
    bundles[0].submitAndScheduleProcess();
    bundles[1].submitAndScheduleProcess();

    Util.assertSucceeded(cluster2.getProcessHelper()
      .suspend(URLS.SUSPEND_URL, bundles[0].getProcessData()));
    AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.SUSPENDED);
    AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);
    //now check if they have been scheduled correctly or not

    Util.assertSucceeded(cluster2.getProcessHelper()
      .schedule(URLS.SCHEDULE_URL, bundles[0].getProcessData()));
    AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.SUSPENDED);
    Util.assertSucceeded(cluster2.getProcessHelper()
      .resume(URLS.RESUME_URL, bundles[0].getProcessData()));
    AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.RUNNING);

    Util.assertSucceeded(cluster1.getProcessHelper()
      .suspend(URLS.SUSPEND_URL, bundles[1].getProcessData()));
    AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.SUSPENDED);
    AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.RUNNING);
  }

  @Test(groups = {"prism", "0.2"})
  public void testScheduleDeletedProcessOnBothColos() throws Exception {
    //schedule both bundles
    bundles[0].submitAndScheduleProcess();
    bundles[1].submitAndScheduleProcess();

    Util.assertSucceeded(
      prism.getProcessHelper().delete(URLS.DELETE_URL, bundles[0].getProcessData()));
    AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.KILLED);
    AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);

    Util.assertSucceeded(
      prism.getProcessHelper().delete(URLS.DELETE_URL, bundles[1].getProcessData()));
    AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.KILLED);
    AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.KILLED);

    Util.assertFailed(cluster2.getProcessHelper()
      .schedule(URLS.SCHEDULE_URL, bundles[0].getProcessData()));
    Util.assertFailed(cluster1.getProcessHelper()
      .schedule(URLS.SCHEDULE_URL, bundles[1].getProcessData()));

  }


  @Test(groups = {"prism", "0.2"})
  public void testScheduleNonExistentProcessOnBothColos() throws Exception {
    Util.assertFailed(cluster2.getProcessHelper()
      .schedule(URLS.SCHEDULE_URL, bundles[0].getProcessData()));
    Util.assertFailed(cluster1.getProcessHelper()
      .schedule(URLS.SCHEDULE_URL, bundles[1].getProcessData()));

  }


  @Test(groups = {"prism", "0.2"})
  public void testProcessScheduleOn1ColoWhileOtherColoIsDown() throws Exception {
    try {
      bundles[1].submitProcess(true);

      Util.shutDownService(cluster2.getProcessHelper());

      Util.assertSucceeded(prism.getProcessHelper()
        .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[1].getProcessData()));

      //now check if they have been scheduled correctly or not
      AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);

      //check if there is no criss cross
      AssertUtil.checkNotStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.RUNNING);
    } catch (Exception e) {
      e.printStackTrace();
      throw new TestNGException(e.getMessage());
    } finally {
      Util.restartService(cluster2.getProcessHelper());
    }
  }


  @Test(groups = {"prism", "0.2"})
  public void testProcessScheduleOn1ColoWhileThatColoIsDown() throws Exception {
    try {
      bundles[0].submitProcess(true);

      Util.shutDownService(cluster2.getProcessHelper());

      Util.assertFailed(prism.getProcessHelper()
        .schedule(URLS.SCHEDULE_URL, bundles[0].getProcessData()));
      AssertUtil.checkNotStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.RUNNING);
    } catch (Exception e) {
      e.printStackTrace();
      throw new TestNGException(e.getMessage());
    } finally {
      Util.restartService(cluster2.getProcessHelper());
    }

  }

  @Test(groups = {"prism", "0.2"})
  public void testProcessScheduleOn1ColoWhileAnotherColoHasSuspendedProcess()
    throws Exception {
    try {
      bundles[0].submitAndScheduleProcess();
      Util.assertSucceeded(bundles[0].getProcessHelper()
        .suspend(URLS.SUSPEND_URL, bundles[0].getProcessData()));
      AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.SUSPENDED);

      bundles[1].submitAndScheduleProcess();
      AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);
      AssertUtil.checkNotStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.RUNNING);
      AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.SUSPENDED);
      AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);

    } catch (Exception e) {
      e.printStackTrace();
      throw new TestNGException(e.getMessage());
    }

  }

  @Test(groups = {"prism", "0.2"})
  public void testProcessScheduleOn1ColoWhileAnotherColoHasKilledProcess()
    throws Exception {
    try {
      bundles[0].submitAndScheduleProcess();
      Util.assertSucceeded(prism.getProcessHelper()
        .delete(URLS.DELETE_URL, bundles[0].getProcessData()));
      AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.KILLED);

      bundles[1].submitAndScheduleProcess();
      AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);
      AssertUtil.checkNotStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.RUNNING);
      AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.KILLED);
      AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);
    } catch (Exception e) {
      e.printStackTrace();
      throw new TestNGException(e.getMessage());
    }
  }

  @Test(groups = {"prism", "0.2"}, enabled = true, timeOut = 1800000)
  public void testRescheduleKilledProcess() throws Exception {

    /*
    add test data generator pending
     */

    bundles[0].setProcessValidity(InstanceUtil.getTimeWrtSystemTime(-1),
      InstanceUtil.getTimeWrtSystemTime(1));
    HadoopFileEditor hadoopFileEditor = null;
    try {

      hadoopFileEditor = new HadoopFileEditor(cluster1
        .getClusterHelper().getHadoopFS());

      hadoopFileEditor.edit(new ProcessMerlin(bundles[0]
        .getProcessData()).getWorkflow().getPath()+"/workflow.xml","<value>${outputData}</value>",
        "<property>\n" +
          "                    <name>randomProp</name>\n" +
          "                    <value>randomValue</value>\n" +
          "                </property>");

      bundles[0].submitAndScheduleBundle(prism);

      InstanceUtil.waitForBundleToReachState(cluster1,
        Util.readEntityName(bundles[0].getProcessData()), org.apache.oozie.client.Job.Status.KILLED,10);

      String oldBundleID = InstanceUtil.getLatestBundleID(cluster1,
        Util.readEntityName(bundles[0].getProcessData()), ENTITY_TYPE.PROCESS);

      prism.getProcessHelper().delete(URLS.DELETE_URL,
        bundles[0].getProcessData());

      bundles[0].submitAndScheduleProcess();

      Util.verifyNewBundleCreation(cluster1, oldBundleID,
        new ArrayList<String>(),
        bundles[0].getProcessData(), true,
        false);
    } finally {

      hadoopFileEditor.restore();
    }
  }
}