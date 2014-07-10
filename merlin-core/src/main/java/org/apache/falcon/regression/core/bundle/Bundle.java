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

package org.apache.falcon.regression.core.bundle;

import org.apache.commons.lang.StringUtils;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.Frequency.TimeUnit;
import org.apache.falcon.entity.v0.cluster.Interface;
import org.apache.falcon.entity.v0.cluster.Interfaces;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.entity.v0.feed.ActionType;
import org.apache.falcon.entity.v0.feed.CatalogTable;
import org.apache.falcon.entity.v0.feed.ClusterType;
import org.apache.falcon.entity.v0.feed.Clusters;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.Location;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.feed.Locations;
import org.apache.falcon.entity.v0.feed.Retention;
import org.apache.falcon.entity.v0.feed.RetentionType;
import org.apache.falcon.entity.v0.feed.Validity;
import org.apache.falcon.entity.v0.process.Cluster;
import org.apache.falcon.entity.v0.process.EngineType;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Inputs;
import org.apache.falcon.entity.v0.process.LateInput;
import org.apache.falcon.entity.v0.process.LateProcess;
import org.apache.falcon.entity.v0.process.Output;
import org.apache.falcon.entity.v0.process.Outputs;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.entity.v0.process.Property;
import org.apache.falcon.entity.v0.process.Retry;
import org.apache.falcon.entity.v0.process.Workflow;
import org.apache.falcon.regression.Entities.ClusterMerlin;
import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.Entities.ProcessMerlin;
import org.apache.falcon.regression.core.enumsAndConstants.ENTITY_TYPE;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.interfaces.EntityHelperFactory;
import org.apache.falcon.regression.core.interfaces.IEntityManagerHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.Assert;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * A bundle abstraction.
 */
public class Bundle {

    public static final String PRISM_PREFIX = "prism";
    static ColoHelper prismHelper = new ColoHelper(PRISM_PREFIX);
    private static final Logger logger = Logger.getLogger(Bundle.class);

    public List<String> dataSets;
    String processData;
    String clusterData;

    String processFilePath;
    List<String> clusters;

    private static String sBundleLocation;

    public void submitFeed() throws Exception {
        submitClusters(prismHelper);

        AssertUtil.assertSucceeded(
            prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, dataSets.get(0)));
    }

    public void submitAndScheduleFeed() throws Exception {
        submitClusters(prismHelper);

        AssertUtil.assertSucceeded(prismHelper.getFeedHelper().submitAndSchedule(
            URLS.SUBMIT_AND_SCHEDULE_URL, dataSets.get(0)));
    }

    public void submitAndScheduleFeedUsingColoHelper(ColoHelper coloHelper) throws Exception {
        submitFeed();

        AssertUtil.assertSucceeded(
            coloHelper.getFeedHelper().schedule(Util.URLS.SCHEDULE_URL, dataSets.get(0)));
    }

    public void submitAndScheduleAllFeeds()
        throws JAXBException, IOException, URISyntaxException, AuthenticationException {
        submitClusters(prismHelper);

        for (String feed : dataSets) {
            AssertUtil.assertSucceeded(
                prismHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed));
        }
    }

    public ServiceResponse submitProcess(boolean shouldSucceed) throws JAXBException,
        IOException, URISyntaxException, AuthenticationException {
        submitAndScheduleAllFeeds();
        ServiceResponse r = prismHelper.getProcessHelper().submitEntity(URLS.SUBMIT_URL,
            processData);
        if (shouldSucceed)
            AssertUtil.assertSucceeded(r);
        return r;
    }

    public void submitFeedsScheduleProcess() throws Exception {
        submitClusters(prismHelper);

        submitFeeds(prismHelper);

        AssertUtil.assertSucceeded(prismHelper.getProcessHelper().submitAndSchedule(
            URLS.SUBMIT_AND_SCHEDULE_URL, processData));
    }


    public void submitAndScheduleProcess() throws Exception {
        submitAndScheduleAllFeeds();

        AssertUtil.assertSucceeded(prismHelper.getProcessHelper().submitAndSchedule(
            URLS.SUBMIT_AND_SCHEDULE_URL, processData));
    }

    public void submitAndScheduleProcessUsingColoHelper(ColoHelper coloHelper) throws Exception {
        submitProcess(true);

        AssertUtil.assertSucceeded(
            coloHelper.getProcessHelper().schedule(URLS.SCHEDULE_URL, processData));
    }

    public List<String> getClusters() {
        return clusters;
    }

    List<String> oldClusters;

    IEntityManagerHelper clusterHelper;
    IEntityManagerHelper processHelper;
    IEntityManagerHelper feedHelper;

    private ColoHelper colohelper;

    public IEntityManagerHelper getClusterHelper() {
        return clusterHelper;
    }

    public IEntityManagerHelper getFeedHelper() {
        return feedHelper;
    }

    public IEntityManagerHelper getProcessHelper() {
        return processHelper;
    }

    public String getProcessFilePath() {
        return processFilePath;
    }

    public Bundle(List<String> dataSets, String processData, String clusterData) {
        this.dataSets = dataSets;
        this.processData = processData;
        this.clusters = new ArrayList<String>();
        this.clusters.add(clusterData);
    }

    public Bundle(Bundle bundle, String prefix) {
        this.dataSets = new ArrayList<String>(bundle.getDataSets());
        this.processData = bundle.getProcessData();
        this.clusters = new ArrayList<String>();
        colohelper = new ColoHelper(prefix);
        for (String cluster : bundle.getClusters()) {
            this.clusters.add(Util.getEnvClusterXML(cluster, prefix));
        }

        if (null == bundle.getClusterHelper()) {
            this.clusterHelper =
                EntityHelperFactory.getEntityHelper(ENTITY_TYPE.CLUSTER, prefix);
        } else {
            this.clusterHelper = bundle.getClusterHelper();
        }

        if (null == bundle.getProcessHelper()) {
            this.processHelper =
                EntityHelperFactory.getEntityHelper(ENTITY_TYPE.PROCESS, prefix);
        } else {
            this.processHelper = bundle.getProcessHelper();
        }

        if (null == bundle.getFeedHelper()) {
            this.feedHelper =
                EntityHelperFactory.getEntityHelper(ENTITY_TYPE.FEED, prefix);
        } else {
            this.feedHelper = bundle.getFeedHelper();
        }
    }

    public Bundle(Bundle bundle, ColoHelper prismHelper) {
        this.dataSets = new ArrayList<String>(bundle.getDataSets());
        this.processData = bundle.getProcessData();
        this.clusters = new ArrayList<String>();
        for (String cluster : bundle.getClusters()) {
            this.clusters
                .add(Util.getEnvClusterXML(cluster,
                    prismHelper.getPrefix()));
        }
        this.clusterHelper = prismHelper.getClusterHelper();
        this.processHelper = prismHelper.getProcessHelper();
        this.feedHelper = prismHelper.getFeedHelper();
    }

    public void setClusterData(List<String> clusters) {
        this.clusters = new ArrayList<String>(clusters);
    }

    public List<String> getClusterNames() {
        List<String> clusterNames = new ArrayList<String>();
        for (String cluster : clusters) {
            final org.apache.falcon.entity.v0.cluster.Cluster clusterObject =
                Util.getClusterObject(cluster);
            clusterNames.add(clusterObject.getName());
        }
        return clusterNames;
    }

    public void setClusterData(String clusterData) {
        this.clusterData = clusterData;
    }

    public List<String> getDataSets() {
        return dataSets;
    }

    public void setDataSets(List<String> dataSets) {
        this.dataSets = dataSets;
    }

    public String getProcessData() {
        return processData;
    }

    public void setProcessData(String processData) {
        this.processData = processData;
    }

    /**
     * Generates unique entities within a bundle changing their names and names of dependant items
     * to unique.
     */
    public void generateUniqueBundle() {

        this.oldClusters = new ArrayList<String>(this.clusters);
        this.clusters = Util.generateUniqueClusterEntity(clusters);

        List<String> newDataSet = new ArrayList<String>();
        for (String dataSet : getDataSets()) {
            String uniqueDataEntity = Util.generateUniqueDataEntity(dataSet);
            for (int i = 0; i < clusters.size(); i++) {
                String oldCluster = oldClusters.get(i);
                String uniqueCluster = clusters.get(i);

                uniqueDataEntity =
                    injectNewDataIntoFeed(uniqueDataEntity, Util.readClusterName(uniqueCluster),
                        Util.readClusterName(oldCluster));
                this.processData =
                    injectNewDataIntoProcess(getProcessData(), Util.readDatasetName(dataSet),
                        Util.readDatasetName(uniqueDataEntity),
                        Util.readClusterName(uniqueCluster),
                        Util.readClusterName(oldCluster));
            }
            newDataSet.add(uniqueDataEntity);
        }
        if (getDataSets().size() == 0) {

            for (int i = 0; i < clusters.size(); i++) {
                String oldCluster = oldClusters.get(i);
                String uniqueCluster = clusters.get(i);
                this.processData =
                    injectNewDataIntoProcess(getProcessData(), null, null,
                        Util.readClusterName(uniqueCluster),
                        Util.readClusterName(oldCluster));
            }
        }
        this.dataSets = newDataSet;

        if (!processData.equals("")) {
            this.processData = Util.generateUniqueProcessEntity(processData);
            this.processData = injectLateDataBasedOnInputs(processData);
        }
    }

    private String injectLateDataBasedOnInputs(String processData) {

        Process processElement = (Process) Entity.fromString(EntityType.PROCESS, processData);

        if (processElement.getLateProcess() != null) {

            ArrayList<LateInput> lateInput = new ArrayList<LateInput>();

            for (Input input : processElement.getInputs().getInputs()) {
                LateInput temp = new LateInput();
                temp.setInput(input.getName());
                temp.setWorkflowPath(processElement.getWorkflow().getPath());
                lateInput.add(temp);
            }


            processElement.getLateProcess().getLateInputs().clear();
            processElement.getLateProcess().getLateInputs().addAll(lateInput);

            logger.info("process after late input set: " + processElement.toString());

            return processElement.toString();
        }

        return processData;
    }

    /**
     * Renames feed cluster with matching name
     *
     * @param dataSet feed definition to be modified
     * @param uniqueCluster new cluster name
     * @param oldCluster old cluster name
     * @return feed definition with new cluster name
     */
    private String injectNewDataIntoFeed(String dataSet, String uniqueCluster, String oldCluster) {
        Feed feedElement = (Feed) Entity.fromString(EntityType.FEED, dataSet);
        for (org.apache.falcon.entity.v0.feed.Cluster cluster : feedElement
            .getClusters().getClusters()) {
            if (cluster.getName().equalsIgnoreCase(oldCluster)) {
                cluster.setName(uniqueCluster);
            }
        }
        return feedElement.toString();
    }

    /**
     * Injects new data source into process: input or output.
     * Replaces old process input/output feed name with new one as well as an appropriate process
     * cluster.
     *
     * @param processData process definition to be modified
     * @param oldDataName old feed name
     * @param newDataName new feed name
     * @param uniqueCluster new cluster name
     * @param oldCluster old cluster name
     * @return modified process definition
     */
    private String injectNewDataIntoProcess(String processData, String oldDataName,
                                            String newDataName,
                                            String uniqueCluster, String oldCluster) {
        if (processData.equals(""))
            return "";
        Process processElement = (Process) Entity.fromString(EntityType.PROCESS, processData);
        if (processElement.getInputs() != null)
            for (Input input : processElement.getInputs().getInputs()) {
                if (input.getFeed().equals(oldDataName)) {
                    input.setFeed(newDataName);
                }
            }
        if (processElement.getOutputs() != null)
            for (Output output : processElement.getOutputs().getOutputs()) {
                if (output.getFeed().equalsIgnoreCase(oldDataName)) {
                    output.setFeed(newDataName);
                }
            }
        for (Cluster cluster : processElement.getClusters().getClusters()) {
            if (cluster.getName().equalsIgnoreCase(oldCluster)) {
                cluster.setName(uniqueCluster);
            }
        }

        //now just wrap the process back!
        return processElement.toString();
    }


    public ServiceResponse submitBundle(ColoHelper prismHelper)
        throws JAXBException, IOException, URISyntaxException, AuthenticationException {

        submitClusters(prismHelper);

        //lets submit all data first
        submitFeeds(prismHelper);

        return prismHelper.getProcessHelper().submitEntity(URLS.SUBMIT_URL, getProcessData());
    }

    public void updateWorkFlowFile() throws IOException {
        Process processElement = InstanceUtil.getProcessElement(this);
        Workflow wf = processElement.getWorkflow();
        File wfFile = new File(sBundleLocation + "/workflow/workflow.xml");
        if (!wfFile.exists()) {
            logger.info("workflow not provided along with process and feed xmls");
            return;
        }
        //is folder present
        if (!HadoopUtil.isDirPresent(colohelper.getClusterHelper().getHadoopFS(), wf.getPath())) {
            logger.info("workflowPath does not exists: creating path: " + wf.getPath());
            HadoopUtil.createDir(wf.getPath(), colohelper.getClusterHelper().getHadoopFS());
        }

        // If File is present in hdfs check for contents and replace if found different
        if (HadoopUtil.isFilePresentHDFS(colohelper, wf.getPath(), "workflow.xml")) {

            HadoopUtil.deleteFile(colohelper, new Path(wf.getPath() + "/workflow.xml"));
        }
        // If there is no file in hdfs , replace it anyways
        HadoopUtil.copyDataToFolder(colohelper, new Path(wf.getPath() + "/workflow.xml"),
            wfFile.getAbsolutePath());
    }

    /**
     * Submits bundle and schedules process.
     *
     * @param prismHelper prismHelper of prism host
     * @return message from schedule response
     * @throws IOException
     * @throws JAXBException
     * @throws URISyntaxException
     * @throws AuthenticationException
     */
    public String submitAndScheduleBundle(ColoHelper prismHelper)
        throws IOException, JAXBException, URISyntaxException,
        AuthenticationException {
        if (colohelper != null) {
            updateWorkFlowFile();
        }
        ServiceResponse submitResponse = submitBundle(prismHelper);
        if (submitResponse.getCode() == 400)
            return submitResponse.getMessage();

        //lets schedule the damn thing now :)
        ServiceResponse scheduleResult =
            prismHelper.getProcessHelper().schedule(URLS.SCHEDULE_URL, getProcessData());
        logger.info("process schedule result=" + scheduleResult.getMessage());
        AssertUtil.assertSucceeded(scheduleResult);
        try {
            Thread.sleep(7000);
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        }
        return scheduleResult.getMessage();
    }

    /**
     * Sets the only process input
     *
     * @param startEl its start in terms of EL expression
     * @param endEl its end in terms of EL expression
     * @return modified process
     */
    public void setProcessInput(String startEl, String endEl) {
        Process process = InstanceUtil.getProcessElement(this);
        Inputs inputs = new Inputs();
        Input input = new Input();
        input.setFeed(Util.readEntityName(BundleUtil.getInputFeedFromBundle(this)));
        input.setStart(startEl);
        input.setEnd(endEl);
        input.setName("inputData");
        inputs.getInputs().add(input);
        process.setInputs(inputs);
        this.setProcessData(process.toString());
    }

    public void setInvalidData() {
        int index = 0;
        Feed dataElement = (Feed) Entity.fromString(EntityType.FEED, dataSets.get(0));
        if (!dataElement.getName().contains("raaw-logs16")) {
            dataElement = (Feed) Entity.fromString(EntityType.FEED, dataSets.get(1));
            index = 1;
        }


        String oldLocation = dataElement.getLocations().getLocations().get(0).getPath();
        logger.info("oldlocation: " + oldLocation);
        dataElement.getLocations().getLocations().get(0).setPath(
            oldLocation.substring(0, oldLocation.indexOf('$')) + "invalid/" +
                oldLocation.substring(oldLocation.indexOf('$')));
        logger.info("new location: " + dataElement.getLocations().getLocations().get(0).getPath());
        dataSets.set(index, dataElement.toString());
    }


    public void setFeedValidity(String feedStart, String feedEnd, String feedName) {
        Feed feedElement = InstanceUtil.getFeedElement(this, feedName);
        feedElement.getClusters().getClusters().get(0).getValidity()
            .setStart(TimeUtil.oozieDateToDate(feedStart).toDate());
        feedElement.getClusters().getClusters().get(0).getValidity()
            .setEnd(TimeUtil.oozieDateToDate(feedEnd).toDate());
        InstanceUtil.writeFeedElement(this, feedElement, feedName);
    }

    public int getInitialDatasetFrequency() {
        Feed dataElement = (Feed) Entity.fromString(EntityType.FEED, dataSets.get(0));
        if (!dataElement.getName().contains("raaw-logs16")) {
            dataElement = (Feed) Entity.fromString(EntityType.FEED, dataSets.get(1));
        }
        if (dataElement.getFrequency().getTimeUnit() == TimeUnit.hours)
            return (Integer.parseInt(dataElement.getFrequency().getFrequency())) * 60;
        else return (Integer.parseInt(dataElement.getFrequency().getFrequency()));
    }

    public Date getStartInstanceProcess(Calendar time) {
        Process processElement = InstanceUtil.getProcessElement(this);
        logger.info("start instance: " + processElement.getInputs().getInputs().get(0).getStart());
        return TimeUtil.getMinutes(processElement.getInputs().getInputs().get(0).getStart(), time);
    }

    public Date getEndInstanceProcess(Calendar time) {
        Process processElement = InstanceUtil.getProcessElement(this);
        logger.info("end instance: " + processElement.getInputs().getInputs().get(0).getEnd());
        logger.info("timezone in getendinstance: " + time.getTimeZone().toString());
        logger.info("time in getendinstance: " + time.getTime());
        return TimeUtil.getMinutes(processElement.getInputs().getInputs().get(0).getEnd(), time);
    }

    public void setDatasetInstances(String startInstance, String endInstance) {
        Process processElement = InstanceUtil.getProcessElement(this);
        processElement.getInputs().getInputs().get(0).setStart(startInstance);
        processElement.getInputs().getInputs().get(0).setEnd(endInstance);
        InstanceUtil.writeProcessElement(this, processElement);
    }

    public void setProcessPeriodicity(int frequency, TimeUnit periodicity) {
        Process processElement = InstanceUtil.getProcessElement(this);
        Frequency frq = new Frequency("" + frequency, periodicity);
        processElement.setFrequency(frq);
        InstanceUtil.writeProcessElement(this, processElement);
    }

    public void setProcessInputStartEnd(String start, String end) {
        Process processElement = InstanceUtil.getProcessElement(this);
        for (Input input : processElement.getInputs().getInputs()) {
            input.setStart(start);
            input.setEnd(end);
        }
        InstanceUtil.writeProcessElement(this, processElement);
    }

    public void setOutputFeedPeriodicity(int frequency, TimeUnit periodicity) {
        Process processElement = (Process) Entity.fromString(EntityType.PROCESS, processData);
        String outputDataset = null;
        int datasetIndex;
        for (datasetIndex = 0; datasetIndex < dataSets.size(); datasetIndex++) {
            outputDataset = dataSets.get(datasetIndex);
            if (outputDataset.contains(processElement.getOutputs().getOutputs().get(0).getFeed())) {
                break;
            }
        }

        Feed feedElement = (Feed) Entity.fromString(EntityType.FEED, outputDataset);

        feedElement.setFrequency(new Frequency("" + frequency, periodicity));
        dataSets.set(datasetIndex, feedElement.toString());
        logger.info("modified o/p dataSet is: " + dataSets.get(datasetIndex));
    }

    public int getProcessConcurrency() {
        return InstanceUtil.getProcessElement(this).getParallel();
    }

    public void setOutputFeedLocationData(String path) {
        Process processElement = (Process) Entity.fromString(EntityType.PROCESS, processData);
        String outputDataset = null;
        int datasetIndex;
        for (datasetIndex = 0; datasetIndex < dataSets.size(); datasetIndex++) {
            outputDataset = dataSets.get(datasetIndex);
            if (outputDataset.contains(processElement.getOutputs().getOutputs().get(0).getFeed())) {
                break;
            }
        }

        Feed feedElement = (Feed) Entity.fromString(EntityType.FEED, outputDataset);
        Location l = new Location();
        l.setPath(path);
        l.setType(LocationType.DATA);
        feedElement.getLocations().getLocations().set(0, l);
        dataSets.set(datasetIndex, feedElement.toString());
        logger.info("modified location path dataSet is: " + dataSets.get(datasetIndex));
    }

    public void setProcessConcurrency(int concurrency) {
        Process processElement = InstanceUtil.getProcessElement(this);
        processElement.setParallel((concurrency));
        InstanceUtil.writeProcessElement(this, processElement);
    }

    public void setProcessWorkflow(String wfPath) {
        setProcessWorkflow(wfPath, null);
    }

    public void setProcessWorkflow(String wfPath, EngineType engineType) {
        setProcessWorkflow(wfPath, null, engineType);
    }

    public void setProcessWorkflow(String wfPath, String libPath, EngineType engineType) {
        Process processElement = InstanceUtil.getProcessElement(this);
        Workflow w = processElement.getWorkflow();
        if (engineType != null) {
            w.setEngine(engineType);
        }
        if (libPath != null) {
            w.setLib(libPath);
        }
        w.setPath(wfPath);
        processElement.setWorkflow(w);
        InstanceUtil.writeProcessElement(this, processElement);
    }

    public Process getProcessObject() {
        return (Process) Entity.fromString(EntityType.PROCESS, getProcessData());
    }


    public String getFeed(String feedName) {
        for (String feed : getDataSets()) {
            if (Util.readDatasetName(feed).contains(feedName)) {
                return feed;
            }
        }

        return null;
    }

    public void setInputFeedPeriodicity(int frequency, TimeUnit periodicity) {
        String feedName = BundleUtil.getInputFeedNameFromBundle(this);
        Feed feedElement = InstanceUtil.getFeedElement(this, feedName);
        Frequency frq = new Frequency("" + frequency, periodicity);
        feedElement.setFrequency(frq);
        InstanceUtil.writeFeedElement(this, feedElement, feedName);

    }

    public void setInputFeedValidity(String startInstance, String endInstance) {
        String feedName = BundleUtil.getInputFeedNameFromBundle(this);
        this.setFeedValidity(startInstance, endInstance, feedName);
    }

    public void setOutputFeedValidity(String startInstance, String endInstance) {
        String feedName = BundleUtil.getOutputFeedNameFromBundle(this);
        this.setFeedValidity(startInstance, endInstance, feedName);
    }

    public void setInputFeedDataPath(String path) {
        String feedName = BundleUtil.getInputFeedNameFromBundle(this);
        Feed feedElement = InstanceUtil.getFeedElement(this, feedName);
        feedElement.getLocations().getLocations().get(0).setPath(path);
        InstanceUtil.writeFeedElement(this, feedElement, feedName);
    }

    public String getFeedDataPathPrefix() {
        Feed feedElement =
            InstanceUtil.getFeedElement(this, BundleUtil.getInputFeedNameFromBundle(this));
        return Util.getPathPrefix(feedElement.getLocations().getLocations().get(0)
            .getPath());
    }

    public void setProcessValidity(DateTime startDate, DateTime endDate) {

        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd/HH:mm");

        String start = formatter.print(startDate).replace("/", "T") + "Z";
        String end = formatter.print(endDate).replace("/", "T") + "Z";

        Process processElement = (Process) Entity.fromString(EntityType.PROCESS, processData);

        for (Cluster cluster : processElement.getClusters().getClusters()) {

            org.apache.falcon.entity.v0.process.Validity validity =
                new org.apache.falcon.entity.v0.process.Validity();
            validity.setStart(TimeUtil.oozieDateToDate(start).toDate());
            validity.setEnd(TimeUtil.oozieDateToDate(end).toDate());
            cluster.setValidity(validity);

        }

        processData = processElement.toString();
    }

    public void setProcessValidity(String startDate, String endDate) {
        Process processElement = (Process) Entity.fromString(EntityType.PROCESS, processData);

        for (Cluster cluster : processElement.getClusters().getClusters()) {
            org.apache.falcon.entity.v0.process.Validity validity =
                new org.apache.falcon.entity.v0.process.Validity();
            validity.setStart(TimeUtil.oozieDateToDate(startDate).toDate());
            validity.setEnd(TimeUtil.oozieDateToDate(endDate).toDate());
            cluster.setValidity(validity);

        }

        processData = processElement.toString();
    }

    public void setProcessLatePolicy(LateProcess lateProcess) {
        Process processElement = (Process) Entity.fromString(EntityType.PROCESS, processData);
        processElement.setLateProcess(lateProcess);
        processData = processElement.toString();
    }


    public void verifyDependencyListing() {
        //display dependencies of process:
        String dependencies = processHelper.getDependencies(Util.readEntityName(getProcessData()));

        //verify presence
        for (String cluster : clusters) {
            Assert.assertTrue(dependencies.contains("(cluster) " + Util.readClusterName(cluster)));
        }
        for (String feed : getDataSets()) {
            Assert.assertTrue(dependencies.contains("(feed) " + Util.readDatasetName(feed)));
            for (String cluster : clusters) {
                Assert.assertTrue(feedHelper.getDependencies(Util.readDatasetName(feed))
                    .contains("(cluster) " + Util.readClusterName(cluster)));
            }
            Assert.assertFalse(feedHelper.getDependencies(Util.readDatasetName(feed))
                .contains("(process)" + Util.readEntityName(getProcessData())));
        }


    }

    public void addProcessInput(String feed, String feedName) {
        Process processElement = InstanceUtil.getProcessElement(this);
        Input in1 = processElement.getInputs().getInputs().get(0);
        Input in2 = new Input();
        in2.setEnd(in1.getEnd());
        in2.setFeed(feed);
        in2.setName(feedName);
        in2.setPartition(in1.getPartition());
        in2.setStart(in1.getStart());
        processElement.getInputs().getInputs().add(in2);
        InstanceUtil.writeProcessElement(this, processElement);
    }

    public void setProcessName(String newName) {
        Process processElement = InstanceUtil.getProcessElement(this);
        processElement.setName(newName);
        InstanceUtil.writeProcessElement(this, processElement);

    }

    public void setRetry(Retry retry) {
        logger.info("old process: " + Util.prettyPrintXml(processData));
        Process processObject = getProcessObject();
        processObject.setRetry(retry);
        processData = processObject.toString();
        logger.info("updated process: " + Util.prettyPrintXml(processData));
    }

    public void setInputFeedAvailabilityFlag(String flag) {
        String feedName = BundleUtil.getInputFeedNameFromBundle(this);
        Feed feedElement = InstanceUtil.getFeedElement(this, feedName);
        feedElement.setAvailabilityFlag(flag);
        InstanceUtil.writeFeedElement(this, feedElement, feedName);
    }

    public void setCLusterColo(String colo) {
        org.apache.falcon.entity.v0.cluster.Cluster c =
            InstanceUtil.getClusterElement(this);
        c.setColo(colo);
        InstanceUtil.writeClusterElement(this, c);

    }

    public void setClusterInterface(Interfacetype interfacetype, String value) {
        org.apache.falcon.entity.v0.cluster.Cluster c =
            InstanceUtil.getClusterElement(this);
        final Interfaces interfaces = c.getInterfaces();
        final List<Interface> interfaceList = interfaces.getInterfaces();
        for (final Interface anInterface : interfaceList) {
            if (anInterface.getType() == interfacetype) {
                anInterface.setEndpoint(value);
            }
        }
        InstanceUtil.writeClusterElement(this, c);
        clusters.set(0, clusterData);
    }

    public void setInputFeedTableUri(String tableUri) {
        final String feedStr = BundleUtil.getInputFeedFromBundle(this);
        Feed feed = (Feed) Entity.fromString(EntityType.FEED, feedStr);
        final CatalogTable catalogTable = new CatalogTable();
        catalogTable.setUri(tableUri);
        feed.setTable(catalogTable);
        InstanceUtil.writeFeedElement(this, feed, feed.getName());
    }

    public void setOutputFeedTableUri(String tableUri) {
        final String feedStr = BundleUtil.getOutputFeedFromBundle(this);
        Feed feed = (Feed) Entity.fromString(EntityType.FEED, feedStr);
        final CatalogTable catalogTable = new CatalogTable();
        catalogTable.setUri(tableUri);
        feed.setTable(catalogTable);
        InstanceUtil.writeFeedElement(this, feed, feed.getName());
    }

    public void setCLusterWorkingPath(String clusterData, String path) {

        org.apache.falcon.entity.v0.cluster.Cluster c =
            (org.apache.falcon.entity.v0.cluster.Cluster)
                Entity.fromString(EntityType.CLUSTER, clusterData);

        for (int i = 0; i < c.getLocations().getLocations().size(); i++) {
            if (c.getLocations().getLocations().get(i).getName().contains("working"))
                c.getLocations().getLocations().get(i).setPath(path);
        }

        //this.setClusterData(clusterData)
        InstanceUtil.writeClusterElement(this, c);
    }


    public void submitClusters(ColoHelper prismHelper)
        throws JAXBException, IOException, URISyntaxException, AuthenticationException {
        submitClusters(prismHelper, null);
    }

    public void submitClusters(ColoHelper prismHelper, String user)
        throws JAXBException, IOException, URISyntaxException, AuthenticationException {
        for (String cluster : this.clusters) {
            AssertUtil.assertSucceeded(
                prismHelper.getClusterHelper().submitEntity(URLS.SUBMIT_URL, cluster, user));
        }
    }

    public void submitFeeds(ColoHelper prismHelper)
        throws JAXBException, IOException, URISyntaxException, AuthenticationException {
        for (String feed : this.dataSets) {
            AssertUtil.assertSucceeded(
                prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed));
        }
    }

    public void addClusterToBundle(String clusterData, ClusterType type,
                                   String startTime, String endTime
    ) {

        clusterData = setNewClusterName(clusterData);

        this.clusters.add(clusterData);
        //now to add clusters to feeds
        for (int i = 0; i < dataSets.size(); i++) {
            FeedMerlin feedObject = new FeedMerlin(dataSets.get(i));
            org.apache.falcon.entity.v0.feed.Cluster cluster =
                new org.apache.falcon.entity.v0.feed.Cluster();
            cluster.setName(Util.getClusterObject(clusterData).getName());
            cluster.setValidity(feedObject.getClusters().getClusters().get(0).getValidity());
            cluster.setType(type);
            cluster.setRetention(feedObject.getClusters().getClusters().get(0).getRetention());
            feedObject.getClusters().getClusters().add(cluster);

            dataSets.remove(i);
            dataSets.add(i, feedObject.toString());

        }

        //now to add cluster to process
        ProcessMerlin processObject = new ProcessMerlin(processData);
        Cluster cluster = new Cluster();
        cluster.setName(Util.getClusterObject(clusterData).getName());
        org.apache.falcon.entity.v0.process.Validity v =
            processObject.getClusters().getClusters().get(0).getValidity();
        if (StringUtils.isNotEmpty(startTime))
            v.setStart(TimeUtil.oozieDateToDate(startTime).toDate());
        if (StringUtils.isNotEmpty(endTime))
            v.setEnd(TimeUtil.oozieDateToDate(endTime).toDate());
        cluster.setValidity(v);
        processObject.getClusters().getClusters().add(cluster);
        this.processData = processObject.toString();

    }

    private String setNewClusterName(String clusterData) {
        ClusterMerlin clusterObj = new ClusterMerlin(clusterData);
        clusterObj.setName(clusterObj.getName() + this.clusters.size() + 1);
        return clusterObj.toString();
    }

    public void deleteBundle(ColoHelper prismHelper) {

        try {
            prismHelper.getProcessHelper().delete(URLS.DELETE_URL, getProcessData());
        } catch (Exception e) {
        }

        for (String dataset : getDataSets()) {
            try {
                prismHelper.getFeedHelper().delete(URLS.DELETE_URL, dataset);
            } catch (Exception e) {
            }
        }

        for (String cluster : this.getClusters()) {
            try {
                prismHelper.getClusterHelper().delete(URLS.DELETE_URL, cluster);
            } catch (Exception e) {
            }
        }


    }

    public String getProcessName() {

        return Util.getProcessName(this.getProcessData());
    }

    public void setProcessLibPath(String libPath) {
        Process processElement = InstanceUtil.getProcessElement(this);
        Workflow wf = processElement.getWorkflow();
        wf.setLib(libPath);
        processElement.setWorkflow(wf);
        InstanceUtil.writeProcessElement(this, processElement);

    }

    public void setProcessTimeOut(int magnitude, TimeUnit unit) {
        Process processElement = InstanceUtil.getProcessElement(this);
        Frequency frq = new Frequency("" + magnitude, unit);
        processElement.setTimeout(frq);
        InstanceUtil.writeProcessElement(this, processElement);
    }

    public static void submitCluster(Bundle... bundles)
        throws IOException, URISyntaxException, AuthenticationException {

        for (Bundle bundle : bundles) {
            ServiceResponse r =
                prismHelper.getClusterHelper()
                    .submitEntity(URLS.SUBMIT_URL, bundle.getClusters().get(0));
            Assert.assertTrue(r.getMessage().contains("SUCCEEDED"), r.getMessage());
        }


    }

    /**
     * Generates unique entities definitions: clusters, feeds and process, populating them with
     * desired values of different properties.
     *
     * @param b bundle to be modified
     * @param numberOfClusters number of clusters on which feeds and process should run
     * @param numberOfInputs number of desired inputs in process definition
     * @param numberOfOptionalInput how many inputs should be optional
     * @param inputBasePaths base data path for inputs
     * @param numberOfOutputs number of outputs
     * @param startTime start of feeds and process validity on every cluster
     * @param endTime end of feeds and process validity on every cluster
     * @return modified bundle
     */
    public Bundle getRequiredBundle(Bundle b, int numberOfClusters, int numberOfInputs,
                                    int numberOfOptionalInput,
                                    String inputBasePaths, int numberOfOutputs, String startTime,
                                    String endTime) {

        //generate and set clusters
        org.apache.falcon.entity.v0.cluster.Cluster c =
            (org.apache.falcon.entity.v0.cluster.Cluster)
                Entity.fromString(EntityType.CLUSTER,
                    Util.generateUniqueClusterEntity(b.getClusters().get(0)));
        List<String> newClusters = new ArrayList<String>();
        List<String> newDataSets = new ArrayList<String>();

        for (int i = 0; i < numberOfClusters; i++) {
            String clusterName = c.getName() + i;
            c.setName(clusterName);
            newClusters.add(i, c.toString());
        }
        b.setClusterData(newClusters);

        //generate and set newDataSets
        for (int i = 0; i < numberOfInputs; i++) {
            String referenceFeed = Util.generateUniqueDataEntity(b.getDataSets().get(0));
            referenceFeed =
                b.setFeedClusters(referenceFeed, newClusters, inputBasePaths + "/input" + i,
                    startTime, endTime);
            newDataSets.add(referenceFeed);
        }
        for (int i = 0; i < numberOfOutputs; i++) {
            String referenceFeed = Util.generateUniqueDataEntity(b.getDataSets().get(0));
            referenceFeed =
                b.setFeedClusters(referenceFeed, newClusters, inputBasePaths + "/output" + i,
                    startTime, endTime);
            newDataSets.add(referenceFeed);
        }
        b.setDataSets(newDataSets);

        //add clusters and feed to process
        String process = b.getProcessData();
        process = Util.generateUniqueProcessEntity(process);
        process = b.setProcessClusters(process, newClusters, startTime, endTime);
        process = b.setProcessFeeds(process, newDataSets, numberOfInputs, numberOfOptionalInput,
            numberOfOutputs);
        b.setProcessData(process);
        return b;
    }

    /**
     * Method sets optional/compulsory inputs and outputs of process according to list of feed
     * definitions and matching numeric parameters. Optional inputs are set first and then
     * compulsory ones.
     *
     * @param process process definition to be modified
     * @param newDataSets list of feed definitions
     * @param numberOfInputs number of desired inputs
     * @param numberOfOptionalInput how many inputs should be optional
     * @param numberOfOutputs number of outputs
     * @return modified process
     */
    public String setProcessFeeds(String process, List<String> newDataSets,
                                  int numberOfInputs, int numberOfOptionalInput,
                                  int numberOfOutputs) {

        Process p = (Process) Entity.fromString(EntityType.PROCESS, process);
        int numberOfOptionalSet = 0;
        boolean isFirst = true;

        Inputs is = new Inputs();
        for (int i = 0; i < numberOfInputs; i++) {
            Input in = new Input();
            in.setEnd("now(0,0)");
            in.setStart("now(0,-20)");
            if (numberOfOptionalSet < numberOfOptionalInput) {
                in.setOptional(true);
                in.setName("inputData" + i);
                numberOfOptionalSet++;
            } else {
                in.setOptional(false);
                if (isFirst) {
                    in.setName("inputData");
                    isFirst = false;
                } else
                    in.setName("inputData" + i);
            }
            in.setFeed(Util.readDatasetName(newDataSets.get(i)));
            is.getInputs().add(in);
        }

        p.setInputs(is);
        if (numberOfInputs == 0) {
            p.setInputs(null);
        }

        Outputs os = new Outputs();
        for (int i = 0; i < numberOfOutputs; i++) {
            Output op = new Output();
            op.setFeed(Util.readDatasetName(newDataSets.get(numberOfInputs - i)));
            op.setName("outputData");
            op.setInstance("now(0,0)");
            os.getOutputs().add(op);
        }
        p.setOutputs(os);
        p.setLateProcess(null);
        return p.toString();
    }

    /**
     * Method sets a number of clusters to process definition
     *
     * @param process process definition to be modified
     * @param newClusters list of definitions of clusters which are to be set to process
     *                    (clusters on which process should run)
     * @param startTime start of process validity on every cluster
     * @param endTime end of process validity on every cluster
     * @return modified process definition
     */
    public String setProcessClusters(String process, List<String> newClusters, String startTime,
                                     String endTime) {

        Process p = (Process) Entity.fromString(EntityType.PROCESS, process);
        org.apache.falcon.entity.v0.process.Clusters cs =
            new org.apache.falcon.entity.v0.process.Clusters();
        for (String newCluster : newClusters) {
            Cluster c = new Cluster();
            c.setName(Util.readClusterName(newCluster));
            org.apache.falcon.entity.v0.process.Validity v =
                new org.apache.falcon.entity.v0.process.Validity();
            v.setStart(TimeUtil.oozieDateToDate(startTime).toDate());
            v.setEnd(TimeUtil.oozieDateToDate(endTime).toDate());
            c.setValidity(v);
            cs.getClusters().add(c);
        }
        p.setClusters(cs);
        return p.toString();
    }

    /**
     * Method sets a number of clusters to feed definition
     *
     * @param referenceFeed feed definition to be changed
     * @param newClusters list of definitions of clusters which are to be set to feed
     * @param location location of data on every cluster
     * @param startTime start of feed validity on every cluster
     * @param endTime end of feed validity on every cluster
     * @return modified feed definition
     */
    public String setFeedClusters(String referenceFeed,
                                  List<String> newClusters, String location, String startTime,
                                  String endTime) {

        Feed f = (Feed) Entity.fromString(EntityType.FEED, referenceFeed);
        Clusters cs = new Clusters();
        f.setFrequency(new Frequency("" + 5, TimeUnit.minutes));

        for (String newCluster : newClusters) {
            org.apache.falcon.entity.v0.feed.Cluster c =
                new org.apache.falcon.entity.v0.feed.Cluster();
            c.setName(Util.readClusterName(newCluster));
            Location l = new Location();
            l.setType(LocationType.DATA);
            l.setPath(location + "/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            Locations ls = new Locations();
            ls.getLocations().add(l);
            c.setLocations(ls);
            Validity v = new Validity();
            startTime = TimeUtil.addMinsToTime(startTime, -180);
            endTime = TimeUtil.addMinsToTime(endTime, 180);
            v.setStart(TimeUtil.oozieDateToDate(startTime).toDate());
            v.setEnd(TimeUtil.oozieDateToDate(endTime).toDate());
            c.setValidity(v);
            Retention r = new Retention();
            r.setAction(ActionType.DELETE);
            Frequency f1 = new Frequency("" + 20, TimeUnit.hours);
            r.setLimit(f1);
            r.setType(RetentionType.INSTANCE);
            c.setRetention(r);
            cs.getClusters().add(c);
        }
        f.setClusters(cs);
        return f.toString();
    }

    public void submitAndScheduleBundle(Bundle b, ColoHelper prismHelper,
                                        boolean checkSuccess)
        throws IOException, JAXBException, URISyntaxException, AuthenticationException {

        for (int i = 0; i < b.getClusters().size(); i++) {
            ServiceResponse r = prismHelper.getClusterHelper()
                .submitEntity(URLS.SUBMIT_URL, b.getClusters().get(i));
            if (checkSuccess)
                AssertUtil.assertSucceeded(r);
        }
        for (int i = 0; i < b.getDataSets().size(); i++) {
            ServiceResponse r =
                prismHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL,
                    b.getDataSets().get(i));
            if (checkSuccess)
                AssertUtil.assertSucceeded(r);
        }
        ServiceResponse r =
            prismHelper.getProcessHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, b.getProcessData());
        if (checkSuccess)
            AssertUtil.assertSucceeded(r);
    }

    /**
     * Changes names of process inputs
     *
     * @param process process definition to be modified
     * @param names desired names of inputs
     * @return modified process definition
     */
    public String setProcessInputNames(String process, String... names) {
        Process p = (Process) Entity.fromString(EntityType.PROCESS, process);
        for (int i = 0; i < names.length; i++) {
            p.getInputs().getInputs().get(i).setName(names[i]);
        }
        return p.toString();
    }

    /**
     * Adds optional property to process definition
     *
     * @param process process definition to be modified
     * @param properties desired properties to be added
     * @return modified process definition
     */
    public String addProcessProperty(String process, Property... properties) {
        Process p = (Process) Entity.fromString(EntityType.PROCESS, process);
        for (Property property : properties) {
            p.getProperties().getProperties().add(property);
        }
        return p.toString();
    }

    /**
     * Sets partition for each input, according to number of supplied partitions.
     *
     * @param process process definition to be modified
     * @param partition partitions to be set
     * @return modified process definition
     */
    public String setProcessInputPartition(String process, String... partition) {
        Process p = (Process) Entity.fromString(EntityType.PROCESS, process);
        for (int i = 0; i < partition.length; i++) {
            p.getInputs().getInputs().get(i).setPartition(partition[i]);
        }
        return p.toString();
    }

    public static Object[][] readBundle(String bundleLocation) throws IOException {
        sBundleLocation = bundleLocation;

        List<Bundle> bundleSet = BundleUtil.getDataFromFolder(bundleLocation);

        Object[][] testData = new Object[bundleSet.size()][1];

        for (int i = 0; i < bundleSet.size(); i++) {
            testData[i][0] = bundleSet.get(i);
        }
        return testData;
    }

    public String setProcessOutputNames(String process, String... names) {
        Process p = (Process) Entity.fromString(EntityType.PROCESS, process);
        Outputs outputs = p.getOutputs();
        if (outputs.getOutputs().size() != names.length) {
            logger.info("Number of output names not equal to output in processdef");
            return null;
        }
        for (int i = 0; i < names.length; i++) {
            outputs.getOutputs().get(i).setName(names[i]);
        }
        p.setOutputs(outputs);
        return p.toString();
    }

    public void addInputFeedToBundle(String feedRefName, String feed, int templateInputIdx) {
        this.getDataSets().add(feed);
        String feedName = Util.readEntityName(feed);
        String processData = getProcessData();

        Process processObject = (Process) Entity.fromString(EntityType.PROCESS, processData);
        final List<Input> processInputs = processObject.getInputs().getInputs();
        Input templateInput = processInputs.get(templateInputIdx);
        Input newInput = new Input();
        newInput.setFeed(feedName);
        newInput.setName(feedRefName);
        newInput.setOptional(templateInput.isOptional());
        newInput.setStart(templateInput.getStart());
        newInput.setEnd(templateInput.getEnd());
        newInput.setPartition(templateInput.getPartition());
        processInputs.add(newInput);
        InstanceUtil.writeProcessElement(this, processObject);
    }

    public void addOutputFeedToBundle(String feedRefName, String feed, int templateOutputIdx) {
        this.getDataSets().add(feed);
        String feedName = Util.readEntityName(feed);
        Process processObject = getProcessObject();
        final List<Output> processOutputs = processObject.getOutputs().getOutputs();
        Output templateOutput = processOutputs.get(templateOutputIdx);
        Output newOutput = new Output();
        newOutput.setFeed(feedName);
        newOutput.setName(feedRefName);
        newOutput.setInstance(templateOutput.getInstance());
        processOutputs.add(newOutput);
        InstanceUtil.writeProcessElement(this, processObject);
    }

    public void setProcessProperty(String property, String value) {

        ProcessMerlin process = new ProcessMerlin(this.getProcessData());
        process.setProperty(property, value);
        this.setProcessData(process.toString());

    }
}
