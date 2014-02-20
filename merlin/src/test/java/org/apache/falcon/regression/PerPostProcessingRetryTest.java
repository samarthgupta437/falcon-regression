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

import org.apache.falcon.regression.Entities.ProcessMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.supportClasses.ENTITY_TYPE;
import org.apache.falcon.regression.core.supportClasses.OozieActions;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.List;

public class PerPostProcessingRetryTest extends BaseTestClass{

  ColoHelper cluster = servers.get(0);
  FileSystem clusterFS = serverFS.get(0);
  OozieClient clusterOC = serverOC.get(0);
  Bundle processBundle ;

  String JAR_TO_DELETE_PREPROCESSING="/lib/falcon-rerun-0.4" +
    ".6-incubating-SNAPSHOT.jar";
  String JAR_TO_DELETE_POSTPROCESSING="/lib/falcon-messaging-0.4" +
    ".6-incubating-SNAPSHOT.jar";

  @BeforeMethod
  public void setUp() throws IOException, JAXBException {
    processBundle = Util.getBundle(cluster,"LateDataBundles/valid1/bundle1");
    processBundle = new Bundle(processBundle, cluster.getEnvFileName(),
      cluster.getPrefix());
    processBundle.generateUniqueBundle();
  }

  @Test(groups = {"singleCluster", "0.3.1"}, timeOut = 1200000,
    enabled = true)
  public void PreProcessingRetryTest() throws JAXBException, ParseException, InterruptedException, IOException, URISyntaxException, OozieClientException {

    String startTime = InstanceUtil.getTimeWrtSystemTime(1);
    String endTime = InstanceUtil.getTimeWrtSystemTime(5);

    processBundle.setProcessValidity(startTime,endTime);
    processBundle.submitAndScheduleBundle(prism);
    List<String> appPath = Util.getAppPath(cluster, processBundle.getProcessData());
    for(String path : appPath){
      HadoopUtil.deleteFile(cluster,
        new Path(path+JAR_TO_DELETE_PREPROCESSING));
    }

    InstanceUtil.waitTillParticularInstanceReachState(cluster,
      Util.readEntityName(processBundle.getProcessData()),0,
      CoordinatorAction.Status.KILLED,5, ENTITY_TYPE.PROCESS);

    Thread.sleep(180000);
    Assert.assertEquals(Util.getOozieActionRetryCount(cluster,
      processBundle.getProcessData(),0, OozieActions.RECORD_SIZE), 3);

  }

  @AfterMethod
  public void tearDown() throws JAXBException, IOException, URISyntaxException {
    processBundle.deleteBundle(prism);
  }
}
