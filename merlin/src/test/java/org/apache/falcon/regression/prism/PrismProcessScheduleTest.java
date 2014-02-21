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

import org.apache.commons.io.FileUtils;
import org.apache.falcon.regression.Entities.ProcessMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.supportClasses.ENTITY_TYPE;
import org.apache.falcon.regression.core.supportClasses.HadoopFileEditor;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.testng.TestNGException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.lang.reflect.Method;
import java.util.ArrayList;

public class PrismProcessScheduleTest extends BaseTestClass {

  ColoHelper cluster1;
  ColoHelper cluster2;
  OozieClient cluster1OC;
  OozieClient cluster2OC;
  Bundle UA1Bundle = new Bundle();
  Bundle UA2Bundle = new Bundle();

  public PrismProcessScheduleTest() {
    super();
    cluster1 = servers.get(0);
    cluster2 = servers.get(1);
    cluster1OC = serverOC.get(0);
    cluster2OC = serverOC.get(1);
  }

  @BeforeMethod(alwaysRun = true)
  public void setUp(Method method) throws Exception {
    Util.print("test name: " + method.getName());
    Bundle bundle = Util.readBundles("LateDataBundles")[0][0];
    UA1Bundle = new Bundle(bundle, cluster1.getEnvFileName(),
      cluster1.getPrefix());
    UA2Bundle = new Bundle(bundle, cluster2.getEnvFileName(),
      cluster2.getPrefix());
    UA1Bundle.generateUniqueBundle();
    UA2Bundle.generateUniqueBundle();
  }

  @Test(groups = {"prism", "0.2"})
  public void testProcessScheduleOnBothColos() throws Exception {
    //schedule both bundles
    UA1Bundle.submitAndScheduleProcess();
    AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);
    AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);

    UA2Bundle.submitAndScheduleProcess();

    //now check if they have been scheduled correctly or not
    AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);

    //check if there is no criss cross
    AssertUtil.checkNotStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);

  }

  @Test(groups = {"prism", "0.2"})
  public void testScheduleAlreadyScheduledProcessOnBothColos() throws Exception {
    //schedule both bundles
    UA1Bundle.submitAndScheduleProcess();
    UA2Bundle.submitAndScheduleProcess();

    //now check if they have been scheduled correctly or not
    AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);
    AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);

    //check if there is no criss cross
    AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);
    AssertUtil.checkNotStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);

    Util.assertSucceeded(cluster2.getProcessHelper()
      .schedule(URLS.SCHEDULE_URL, UA1Bundle.getProcessData()));
    Util.assertSucceeded(cluster1.getProcessHelper()
      .schedule(URLS.SCHEDULE_URL, UA2Bundle.getProcessData()));
    //now check if they have been scheduled correctly or not
    AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);
    AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);

  }

  @Test(groups = {"prism", "0.2"})
  public void testScheduleSuspendedProcessOnBothColos() throws Exception {
    //schedule both bundles
    UA1Bundle.submitAndScheduleProcess();
    UA2Bundle.submitAndScheduleProcess();

    Util.assertSucceeded(cluster2.getProcessHelper()
      .suspend(URLS.SUSPEND_URL, UA1Bundle.getProcessData()));
    AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.SUSPENDED);
    AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);
    //now check if they have been scheduled correctly or not

    Util.assertSucceeded(cluster2.getProcessHelper()
      .schedule(URLS.SCHEDULE_URL, UA1Bundle.getProcessData()));
    AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.SUSPENDED);
    Util.assertSucceeded(cluster2.getProcessHelper()
      .resume(URLS.RESUME_URL, UA1Bundle.getProcessData()));
    AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);

    Util.assertSucceeded(cluster1.getProcessHelper()
      .suspend(URLS.SUSPEND_URL, UA2Bundle.getProcessData()));
    AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.SUSPENDED);
    AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);
  }

  @Test(groups = {"prism", "0.2"})
  public void testScheduleDeletedProcessOnBothColos() throws Exception {
    //schedule both bundles
    UA1Bundle.submitAndScheduleProcess();
    UA2Bundle.submitAndScheduleProcess();

    Util.assertSucceeded(
      prism.getProcessHelper().delete(URLS.DELETE_URL, UA1Bundle.getProcessData()));
    AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.KILLED);
    AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);

    Util.assertSucceeded(
      prism.getProcessHelper().delete(URLS.DELETE_URL, UA2Bundle.getProcessData()));
    AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.KILLED);
    AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.KILLED);

    Util.assertFailed(cluster2.getProcessHelper()
      .schedule(URLS.SCHEDULE_URL, UA1Bundle.getProcessData()));
    Util.assertFailed(cluster1.getProcessHelper()
      .schedule(URLS.SCHEDULE_URL, UA2Bundle.getProcessData()));

  }


  @Test(groups = {"prism", "0.2"})
  public void testScheduleNonExistentProcessOnBothColos() throws Exception {
    Util.assertFailed(cluster2.getProcessHelper()
      .schedule(URLS.SCHEDULE_URL, UA1Bundle.getProcessData()));
    Util.assertFailed(cluster1.getProcessHelper()
      .schedule(URLS.SCHEDULE_URL, UA2Bundle.getProcessData()));

  }


  @Test(groups = {"prism", "0.2"})
  public void testProcessScheduleOn1ColoWhileOtherColoIsDown() throws Exception {
    try {
      UA2Bundle.submitProcess();

      Util.shutDownService(cluster2.getProcessHelper());

      Util.assertSucceeded(prism.getProcessHelper()
        .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, UA2Bundle.getProcessData()));

      //now check if they have been scheduled correctly or not
      AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);

      //check if there is no criss cross
      AssertUtil.checkNotStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);
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
      UA1Bundle.submitProcess();

      Util.shutDownService(cluster2.getProcessHelper());

      Util.assertFailed(prism.getProcessHelper()
        .schedule(URLS.SCHEDULE_URL, UA1Bundle.getProcessData()));
      AssertUtil.checkNotStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);
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
      UA1Bundle.submitAndScheduleProcess();
      Util.assertSucceeded(UA1Bundle.getProcessHelper()
        .suspend(URLS.SUSPEND_URL, UA1Bundle.getProcessData()));
      AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.SUSPENDED);

      UA2Bundle.submitAndScheduleProcess();
      AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);
      AssertUtil.checkNotStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);
      AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.SUSPENDED);
      AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);

    } catch (Exception e) {
      e.printStackTrace();
      throw new TestNGException(e.getMessage());
    }

  }

  @Test(groups = {"prism", "0.2"})
  public void testProcessScheduleOn1ColoWhileAnotherColoHasKilledProcess()
    throws Exception {
    try {
      UA1Bundle.submitAndScheduleProcess();
      Util.assertSucceeded(prism.getProcessHelper()
        .delete(URLS.DELETE_URL, UA1Bundle.getProcessData()));
      AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.KILLED);

      UA2Bundle.submitAndScheduleProcess();
      AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);
      AssertUtil.checkNotStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);
      AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.KILLED);
      AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);
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

    UA1Bundle.setProcessValidity(InstanceUtil.getTimeWrtSystemTime(-1),
      InstanceUtil.getTimeWrtSystemTime(1));
    HadoopFileEditor hadoopFileEditor = null;
    try {

       hadoopFileEditor = new HadoopFileEditor(cluster1
        .getClusterHelper().getHadoopFS());

      hadoopFileEditor.edit(new ProcessMerlin(UA1Bundle
        .getProcessData()).element
        .getWorkflow().getPath()+"/workflow.xml","<value>${outputData}</value>",
        "<property>\n" +
        "                    <name>randomProp</name>\n" +
        "                    <value>randomValue</value>\n" +
        "                </property>");

      UA1Bundle.submitAndScheduleBundle(prism);

      InstanceUtil.waitForBundleToReachState(cluster1,
        Util.readEntityName(UA1Bundle.getProcessData()), org.apache.oozie.client.Job.Status.KILLED,10);

      String oldBundleID = InstanceUtil.getLatestBundleID(cluster1,
        Util.readEntityName(UA1Bundle.getProcessData()), ENTITY_TYPE.PROCESS);

      prism.getProcessHelper().delete(URLS.DELETE_URL,
        UA1Bundle.getProcessData());

      UA1Bundle.submitAndScheduleProcess();

      Util.verifyNewBundleCreation(cluster1, oldBundleID,
        new ArrayList<String>(),
        Util.readEntityName(UA1Bundle.getProcessData()), true,
        ENTITY_TYPE.PROCESS, false);
    } finally {

      hadoopFileEditor.restore();
    }
  }

  @AfterMethod
  public void tearDown(){
    UA1Bundle.deleteBundle(prism);
  }

}
