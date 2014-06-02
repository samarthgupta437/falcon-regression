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

import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.process.*;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.enumsAndConstants.ENTITY_TYPE;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.helpers.LineageHelper;
import org.apache.falcon.regression.core.response.lineage.Vertex;
import org.apache.falcon.regression.core.response.lineage.VerticesResult;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseUITestClass;
import org.apache.falcon.regression.ui.pages.ProcessPage;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.codehaus.jettison.json.JSONException;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Test(groups = "embedded")
public class LineageGraphTest extends BaseUITestClass {

    private ColoHelper cluster = servers.get(0);
    private String baseTestDir = baseHDFSDir + "/LineageGraphTest";
    private String aggregateWorkflowDir = baseTestDir + "/aggregator";
    private Logger logger = Logger.getLogger(LineageGraphTest.class);
    String datePattern = "/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
    String feedInputPath = baseTestDir + datePattern;
    private FileSystem clusterFS = serverFS.get(0);
    private OozieClient clusterOC = serverOC.get(0);
    private String processName = null;
    int inputEnd = 4;
    List<String> processInstances;

    /**
     * Adjusts bundle and schedules it. Provides process with data, waits till some instances got
     * succeeded.
     */
    @BeforeClass
    public void setUp() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
        bundles[0] = BundleUtil.readELBundles()[0][0];
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].generateUniqueBundle();
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        String startTime = TimeUtil.getTimeWrtSystemTime(0);
        String endTime = TimeUtil.addMinsToTime(startTime, 5);
        logger.info("Start time: " + startTime + "\tEnd time: " + endTime);

        /**prepare process definition*/
        bundles[0].setProcessValidity(startTime, endTime);
        bundles[0].setProcessPeriodicity(1, Frequency.TimeUnit.minutes);
        bundles[0].setProcessConcurrency(5);
        bundles[0].setInputFeedPeriodicity(1, Frequency.TimeUnit.minutes);
        bundles[0].setInputFeedDataPath(feedInputPath);
        Process process = InstanceUtil.getProcessElement(bundles[0]);
        Inputs inputs = new Inputs();
        Input input = new Input();
        input.setFeed(Util.readEntityName(BundleUtil.getInputFeedFromBundle(bundles[0])));
        input.setStart("now(0,0)");
        input.setEnd(String.format("now(0,%d)", inputEnd));
        input.setName("inputData");
        inputs.getInputs().add(input);
        process.setInputs(inputs);

        bundles[0].setProcessData(InstanceUtil.processToString(process));

        /**provide necessary data for first 3 instances to run*/
        logger.info("Creating necessary data...");
        String prefix = bundles[0].getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), clusterFS);
        DateTime startDate = new DateTime(TimeUtil.oozieDateToDate(TimeUtil.addMinsToTime
            (startTime, -2)));
        DateTime endDate = new DateTime(TimeUtil.oozieDateToDate(endTime));
        List<String> dataDates = TimeUtil.getMinuteDatesOnEitherSide(startDate, endDate, 0);
        logger.info("Creating data in folders: \n" + dataDates);
        for (int i = 0; i < dataDates.size(); i++)
            dataDates.set(i, prefix + dataDates.get(i));
        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.NORMAL_INPUT, dataDates);
        logger.info("Process data: " + Util.prettyPrintXml(bundles[0].getProcessData()));
        bundles[0].submitBundle(prism);

        processName = bundles[0].getProcessName();
        /**schedule process, wait for instances to succeed*/
        prism.getProcessHelper().schedule(Util.URLS.SCHEDULE_URL, bundles[0].getProcessData());
        InstanceUtil.waitTillInstanceReachState(clusterOC, bundles[0].getProcessName(), 3,
            CoordinatorAction.Status.SUCCEEDED, 8, ENTITY_TYPE.PROCESS);

        /**process instances*/
        LineageHelper graphUtil = new LineageHelper(prism);
        VerticesResult allVertices = graphUtil.getAllVertices();
        processInstances = new ArrayList<String>();
        for (Vertex vertex : allVertices.getResults()) {
            if (!vertex.getName().equals(processName) && vertex.getName().contains(processName)) {
                String instance = vertex.getName().split("/")[1];
                processInstances.add(instance);
            }
        }

        openBrowser();
    }

    @AfterClass
    public void tearDown() throws IOException {
        closeBrowser();
        removeBundles();
    }

    /**
     * Tests the number of vertices on graph and if they match to expected number of instances
     * and their description.
     */
    @Test
    public void testGraphVertices()
        throws URISyntaxException, IOException, AuthenticationException, JAXBException,
        OozieClientException, InterruptedException, NoSuchMethodException, IllegalAccessException,
        InvocationTargetException, ParseException, JSONException {
        String inputFeedName = BundleUtil.getInputFeedNameFromBundle(bundles[0]);
        String outputFeedName = BundleUtil.getOutputFeedNameFromBundle(bundles[0]);
        ProcessPage processPage = new ProcessPage(DRIVER, cluster, processName);
        processPage.navigateTo();
        logger.info("Working with process instances : " + processInstances);
        for (String nominalTime : processInstances) {
            /**get expected feed instances*/
            //input feed instances
            List<String> inputFeedinstances = new ArrayList<String>();
            for (int i = 0; i <= inputEnd; i++) {
                inputFeedinstances.add(TimeUtil.addMinsToTime(nominalTime, i));
            }
            //output feed instance
            String normalPattern = "yyyy'-'MM'-'dd'T'HH':'mm'Z'";
            String WOMinutesPattern = "yyyy'-'MM'-'dd'T'HH'";
            DateTime time = DateTimeFormat.forPattern(normalPattern).parseDateTime(nominalTime);
            String hourlyTime = DateTimeFormat.forPattern(WOMinutesPattern).print(time);
            time = DateTimeFormat.forPattern(WOMinutesPattern).parseDateTime(hourlyTime);
            String outputFeedinstance = DateTimeFormat.forPattern(normalPattern).print(time);
            /**open lineage for particular process instance*/
            boolean isLineagePresent = processPage.openLineage(nominalTime);
            if (!isLineagePresent) continue;
            /**verify if number of vertices and their content is correct*/
            HashMap<String, List<String>> map = processPage.getAllVertices();
            Assert.assertTrue(map.containsKey(processName) && map.containsKey(inputFeedName)
                && map.containsKey(outputFeedName));
            //process validation
            List<String> entityInstances = map.get(processName);
            Assert.assertEquals(entityInstances.size(), 1);
            Assert.assertEquals(entityInstances.get(0), nominalTime);
            //input feed validations
            entityInstances = map.get(inputFeedName);
            logger.info("InputFeed instances on lineage UI : " + entityInstances);
            logger.info("InputFeed instances from API : " + inputFeedinstances);
            Assert.assertEquals(entityInstances.size(), inputFeedinstances.size());
            for (String feedInstance : inputFeedinstances) {
                Assert.assertTrue(entityInstances.contains(feedInstance));
            }
            //output feed validation
            entityInstances = map.get(outputFeedName);
            logger.info("Expected outputFeed instances : " + entityInstances);
            logger.info("Actual instance : " + outputFeedinstance);
            Assert.assertEquals(entityInstances.size(), 1);
            Assert.assertTrue(entityInstances.contains(outputFeedinstance));
            processPage.closeLineage();
            processPage.navigateTo();
        }
    }

    /**
     * Clicks on each vertex and check the content of info panel
     */
    @Test
    public void testVerticesInfo()
        throws InterruptedException, JAXBException, URISyntaxException, AuthenticationException,
        JSONException, IOException {
        String clusterName = Util.readClusterName(bundles[0].getClusters().get(0));
        ProcessPage processPage = new ProcessPage(DRIVER, cluster, processName);
        processPage.navigateTo();

        /**process instances*/
        LineageHelper graphUtil = new LineageHelper(prism);
        VerticesResult allVertices = graphUtil.getAllVertices();
        List<String> processInstances = new ArrayList<String>();
        for (Vertex vertex : allVertices.getResults()) {
            if (!vertex.getName().equals(processName) && vertex.getName().contains(processName)) {
                String instance = vertex.getName().split("/")[1];
                processInstances.add(instance);
            }
        }
        logger.info("Working with process instances : " + processInstances);
        for (String nominalTime : processInstances) {
            /**open lineage for particular process instance*/
            boolean isLineagePresent = processPage.openLineage(nominalTime);
            if (!isLineagePresent) continue;
            HashMap<String, List<String>> map = processPage.getAllVertices();
            /**click on each vertex and check the bottom info*/
            for (Map.Entry<String, List<String>> entry : map.entrySet()) {
                String entityName = entry.getKey();
                List<String> entityInstances = entry.getValue();
                for (String entityInstance : entityInstances) {
                    processPage.clickOnVertex(entityName, entityInstance);
                    HashMap<String, String> info = processPage.getPanelInfo();
                    if (entityName.equals(processName)) {
                        String message = "Lineage info-panel reflects invalid %s for process %s.";
                        String workflow = processName + "-workflow";
                        Assert.assertEquals(info.get("User workflow"), workflow,
                            String.format(message, "workflow", processName));
                        Assert.assertEquals(info.get("User workflow engine"), "oozie",
                            String.format(message, "engine", processName));
                        Assert.assertEquals(info.get("Runs on"), clusterName,
                            String.format(message, "cluster", processName));
                    }
                    Assert.assertEquals(info.get("Owned by"), System.getProperty("user" +
                        ".name"), "Entity should be owned by current system user.");
                }
            }
            processPage.closeLineage();
            processPage.navigateTo();
        }
    }

    @Test
    public void testTitlesAndDescriptions() throws InterruptedException {
        HashMap<String, String> expectedDescriptions = new HashMap<String, String>();
        expectedDescriptions.put("lineage-legend-process-inst", "Process instance");
        expectedDescriptions.put("lineage-legend-process-inst lineage-legend-terminal",
            "Process instance (terminal)");
        expectedDescriptions.put("lineage-legend-feed-inst", "Feed instance");
        expectedDescriptions.put("lineage-legend-feed-inst lineage-legend-terminal",
            "Feed instance (terminal)");
        ProcessPage processPage = new ProcessPage(DRIVER, prism, processName);
        processPage.navigateTo();
        for(String instance : processInstances){
            boolean isLineageOpened = processPage.openLineage(instance);
            if(!isLineageOpened) continue;
            //check the main lineage title
            Assert.assertEquals(processPage.getLineageTitle(), "Lineage information");
            //check legends title
            Assert.assertEquals(processPage.getLegendsTitile(), "Legends");
            //check that all legends are present and match to expected
            HashMap<String, String> legends = processPage.getLegends();
            for (Map.Entry<String, String> entry : legends.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                Assert.assertEquals(expectedDescriptions.get(key), value);
            }
            processPage.closeLineage();
            processPage.navigateTo();
        }
    }
}
