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

import org.apache.falcon.regression.core.generated.dependencies.Frequency;
import org.apache.falcon.regression.core.generated.dependencies.Frequency.TimeUnit;
import org.apache.falcon.regression.core.generated.feed.ActionType;
import org.apache.falcon.regression.core.generated.feed.ClusterType;
import org.apache.falcon.regression.core.generated.feed.Clusters;
import org.apache.falcon.regression.core.generated.feed.Feed;
import org.apache.falcon.regression.core.generated.feed.Location;
import org.apache.falcon.regression.core.generated.feed.LocationType;
import org.apache.falcon.regression.core.generated.feed.Locations;
import org.apache.falcon.regression.core.generated.feed.Retention;
import org.apache.falcon.regression.core.generated.feed.RetentionType;
import org.apache.falcon.regression.core.generated.feed.Validity;
import org.apache.falcon.regression.core.generated.process.Cluster;
import org.apache.falcon.regression.core.generated.process.Input;
import org.apache.falcon.regression.core.generated.process.Inputs;
import org.apache.falcon.regression.core.generated.process.LateInput;
import org.apache.falcon.regression.core.generated.process.LateProcess;
import org.apache.falcon.regression.core.generated.process.Output;
import org.apache.falcon.regression.core.generated.process.Outputs;
import org.apache.falcon.regression.core.generated.process.Process;
import org.apache.falcon.regression.core.generated.process.Properties;
import org.apache.falcon.regression.core.generated.process.Property;
import org.apache.falcon.regression.core.generated.process.Retry;
import org.apache.falcon.regression.core.generated.process.Workflow;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.helpers.PrismHelper;
import org.apache.falcon.regression.core.interfaces.EntityHelperFactory;
import org.apache.falcon.regression.core.interfaces.IEntityManagerHelper;
import org.apache.falcon.regression.core.response.APIResult;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.supportClasses.ENTITY_TYPE;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.ELUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.log4testng.Logger;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import java.io.File;
import java.io.StringReader;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * A bundle abstraction.
 */
public class Bundle {

    static PrismHelper prismHelper = new PrismHelper("prism.properties");

    public List<String> dataSets;
    String processData;
    String clusterData;

    //List<String> feedFilePaths;
    String processFilePath;
    String envFileName;
    List<String> clusters;

    private static String sBundleLocation;

    public List<String> getClusters() {
        return clusters;
    }

    List<String> oldClusters;
    //List<String> clusterFilePaths;

    IEntityManagerHelper clusterHelper;
    IEntityManagerHelper processHelper;
    IEntityManagerHelper feedHelper;

    private ColoHelper colohelper;
    private Logger logger = Logger.getLogger(this.getClass());

    public IEntityManagerHelper getClusterHelper() {
        return clusterHelper;
    }

    public IEntityManagerHelper getFeedHelper() {
        return feedHelper;
    }

    public IEntityManagerHelper getProcessHelper() {
        return processHelper;
    }

    public String getEnvFileName() {
        return envFileName;
    }

    /*public void addClusterFilePath(String clusterFilePath) {

        if (null == this.clusterFilePaths) {
            this.clusterFilePaths = new ArrayList<String>();
        }
        this.clusterFilePaths.add(clusterFilePath);
    }*/

/*
    public void addFeedFilePaths(String feedFilePath) {

        if (null == this.feedFilePaths) {
            this.feedFilePaths = new ArrayList<String>();
        }
        this.feedFilePaths.add(feedFilePath);
    }

*/
    public String getProcessFilePath() {
        return processFilePath;
    }

    /*public void setProcessFilePath(String processFilePath) {
        this.processFilePath = processFilePath;
    }*/

    public Bundle(Bundle bundle) {
        this.dataSets = new ArrayList<String>(bundle.getDataSets());
        this.processData = bundle.getProcessData();
        this.clusters = bundle.getClusters();
        this.clusterHelper = bundle.getClusterHelper();
        this.processHelper = bundle.getProcessHelper();
        this.feedHelper = bundle.getFeedHelper();
        this.envFileName = bundle.getEnvFileName();
    }

    public Bundle(List<String> dataSets, String processData, String clusterData, String envFileName)
    throws Exception {
        this.dataSets = dataSets;
        this.processData = processData;
        this.envFileName = envFileName;
        this.clusters = new ArrayList<String>();
        this.clusters.add(Util.getEnvClusterXML(envFileName, clusterData));
        this.processHelper = EntityHelperFactory.getEntityHelper(ENTITY_TYPE.PROCESS, envFileName);
        this.feedHelper = EntityHelperFactory.getEntityHelper(ENTITY_TYPE.DATA, envFileName);
    }

    public Bundle(List<String> dataSets, String processData, List<String> clusterData,
                  String envFileName)
    throws Exception {
        this.dataSets = dataSets;
        this.processData = processData;
        this.clusters = new ArrayList<String>();
        for (String cluster : clusterData) {
            this.clusters.add(Util.getEnvClusterXML(envFileName, cluster));
        }
        this.envFileName = envFileName;
        this.clusterHelper = EntityHelperFactory.getEntityHelper(ENTITY_TYPE.CLUSTER, envFileName);
        this.processHelper = EntityHelperFactory.getEntityHelper(ENTITY_TYPE.PROCESS, envFileName);
        this.feedHelper = EntityHelperFactory.getEntityHelper(ENTITY_TYPE.DATA, envFileName);
    }

    public Bundle(List<String> dataSets, String processData, String clusterData) throws Exception {
        this.dataSets = dataSets;
        this.processData = processData;
        this.clusters = new ArrayList<String>();
        this.clusters.add(clusterData);
    }

    public Bundle(Bundle bundle, String envFileName) throws Exception {
        this.dataSets = new ArrayList<String>(bundle.getDataSets());
        this.processData = bundle.getProcessData();
        this.clusters = new ArrayList<String>();
        colohelper = new ColoHelper(envFileName);
        for (String cluster : bundle.getClusters()) {
            this.clusters.add(Util.getEnvClusterXML(envFileName, cluster));
        }

        if (null == bundle.getClusterHelper()) {
            this.clusterHelper =
                    EntityHelperFactory.getEntityHelper(ENTITY_TYPE.CLUSTER, envFileName);
        } else {
            this.clusterHelper = bundle.getClusterHelper();
        }

        if (null == bundle.getProcessHelper()) {
            this.processHelper =
                    EntityHelperFactory.getEntityHelper(ENTITY_TYPE.PROCESS, envFileName);
        } else {
            this.processHelper = bundle.getProcessHelper();
        }

        if (null == bundle.getFeedHelper()) {
            this.feedHelper = EntityHelperFactory.getEntityHelper(ENTITY_TYPE.DATA, envFileName);
        } else {
            this.feedHelper = bundle.getFeedHelper();
        }

        this.envFileName = envFileName;
    }

    public Bundle(Bundle bundle, PrismHelper prismHelper) throws Exception {
        this.dataSets = new ArrayList<String>(bundle.getDataSets());
        this.processData = bundle.getProcessData();
        this.clusters = new ArrayList<String>();
        for (String cluster : bundle.getClusters()) {
            this.clusters
                    .add(Util.getEnvClusterXML(prismHelper.getEnvFileName(), cluster));
        }
        this.clusterHelper = prismHelper.getClusterHelper();
        this.processHelper = prismHelper.getProcessHelper();
        this.feedHelper = prismHelper.getFeedHelper();
    }

    public String getClusterData() {
        return clusterData;
    }

    public void setClusterData(List<String> clusters) throws Exception {
        this.clusters = new ArrayList<String>(clusters);
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

    public Bundle(List<String> dataSets, String processData) {
        this.dataSets = dataSets;
        this.processData = processData;
    }


    public void generateUniqueBundle() throws Exception {

        //String newCluster=Util.generateUniqueClusterEntity(clusterData);
        //this.oldCluster=this.clusterData;
        //this.clusterData=newCluster;

        this.oldClusters = new ArrayList<String>(this.clusters);
        this.clusters = Util.generateUniqueClusterEntity(clusters);

        List<String> newDataSet = new ArrayList<String>();
        //
        //                for(int i=0;i<clusters.size();i++)
        //                {
        //                    String oldCluster=oldClusters.get(i);
        //                    String uniqueCluster=clusters.get(i);

        for (String dataset : getDataSets()) {
            String uniqueEntityName = Util.generateUniqueDataEntity(dataset);
            for (int i = 0; i < clusters.size(); i++) {
                String oldCluster = oldClusters.get(i);
                String uniqueCluster = clusters.get(i);


                //processData.replace(Util.readDatasetName(dataset),
                // Util.readDatasetName(uniqueEntityName));
                uniqueEntityName =
                        injectNewDataIntoFeed(uniqueEntityName, Util.readClusterName(uniqueCluster),
                                Util.readClusterName(oldCluster));
                this.processData =
                        injectNewDataIntoProcess(getProcessData(), Util.readDatasetName(dataset),
                                Util.readDatasetName(uniqueEntityName),
                                Util.readClusterName(uniqueCluster),
                                Util.readClusterName(oldCluster));
            }
            newDataSet.add(uniqueEntityName);
        }
        //}

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

    private String injectLateDataBasedOnInputs(String processData) throws Exception {

       // Util.print("process before late input set: " + processData);

        JAXBContext jc = JAXBContext.newInstance(Process.class);
        Unmarshaller u = jc.createUnmarshaller();

        Process processElement = (Process) u.unmarshal((new StringReader(processData)));

        if (processElement.getLateProcess() != null) {

            ArrayList<LateInput> lateInput = new ArrayList<LateInput>();

            for (Input input : processElement.getInputs().getInput()) {
                LateInput temp = new LateInput();
                temp.setInput(input.getName());
                temp.setWorkflowPath(processElement.getWorkflow().getPath());
                lateInput.add(temp);
            }


            processElement.getLateProcess().setLateInput(lateInput);

            //	processElement.setLateProcess(value);

            java.io.StringWriter sw = new StringWriter();

            Marshaller marshaller = jc.createMarshaller();
            //marshaller.setProperty(Marshaller.JAXB_ENCODING, "UTF-8");
            marshaller.marshal(processElement, sw);

            Util.print("process after late input set: " + sw.toString());

            return sw.toString();
        }

        return processData;
    }


    private String injectNewDataIntoFeed(String dataset, String uniqueCluster, String oldCluster)
    throws Exception {
        JAXBContext jc = JAXBContext.newInstance(Feed.class);

        Unmarshaller uc = jc.createUnmarshaller();

        Feed feedElement = (Feed) uc.unmarshal(new StringReader(dataset));

        for (org.apache.falcon.regression.core.generated.feed.Cluster cluster : feedElement
                .getClusters()
                .getCluster()) {
            if (cluster.getName().equalsIgnoreCase(oldCluster)) {
                cluster.setName(uniqueCluster);
                //break;
            }
        }

        java.io.StringWriter sw = new StringWriter();

        Marshaller marshaller = jc.createMarshaller();
        //marshaller.setProperty(Marshaller.JAXB_ENCODING, "UTF-8");
        marshaller.marshal(feedElement, sw);

        return sw.toString();
    }

    private String injectNewDataIntoProcess(String processData, String oldDataName,
                                            String newDataName,
                                            String uniqueCluster, String oldCluster)
    throws Exception {

        if (processData.equals(""))
            return "";
        JAXBContext jc = JAXBContext.newInstance(Process.class);

        Unmarshaller u = jc.createUnmarshaller();

        Process processElement = (Process) u.unmarshal((new StringReader(processData)));

        //List<LateInput> lateInputList=new ArrayList<LateInput>();
        if (processElement.getInputs() != null)
            for (Input input : processElement.getInputs().getInput()) {
                if (input.getFeed().equals(oldDataName)) {
                    input.setFeed(newDataName);
                }

                //also set process' late data
                //                        LateInput temp=new LateInput();
                //                        temp.setFeed(newDataName);
                //                        temp.setWorkflowPath(processElement.getWorkflow()
                // .getPath());
                //                        if(!isPresent(temp, lateInputList))
                //                        {
                //                            lateInputList.add(temp);
                //                        }
            }

        if (processElement.getOutputs() != null)
            for (Output output : processElement.getOutputs().getOutput()) {
                if (output.getFeed().equalsIgnoreCase(oldDataName)) {
                    output.setFeed(newDataName);
                }
                //                        LateInput temp=new LateInput();
                //                        temp.setFeed(newDataName);
                //                        temp.setWorkflowPath(processElement.getWorkflow()
                // .getPath());
                //                        if(!isPresent(temp, lateInputList))
                //                        {
                //                            lateInputList.add(temp);
                //                        }
            }

        //		for(com.inmobi.qa.ivory.generated.Cluster cluster:processElement.getClusters()
        // .getCluster())
        //		{
        //			if(cluster.getName().equalsIgnoreCase(oldCluster))
        //			{
        //				cluster.setName(uniqueCluster);
        //			}
        //		}

        for (Cluster cluster : processElement.getClusters().getCluster()) {
            if (cluster.getName().equalsIgnoreCase(oldCluster)) {
                cluster.setName(uniqueCluster);
            }
        }

        //		if(processElement.getCluster().getName().equalsIgnoreCase(oldCluster))
        //		{
        //			processElement.getCluster().setName(uniqueCluster);
        //		}

        //processElement.getLateProcess().setLateInput((lateInputList));


        //now just wrap the process back!
        java.io.StringWriter sw = new StringWriter();

        Marshaller marshaller = jc.createMarshaller();
        //marshaller.setProperty(Marshaller.JAXB_ENCODING, "UTF-8");
        marshaller.marshal(processElement, sw);

        return sw.toString();
    }


    public ServiceResponse submitBundle(PrismHelper prismHelper) throws Exception {

        //make sure bundle is unique
        generateUniqueBundle();

        submitClusters(prismHelper);

        //lets submit all data first
        submitFeeds(prismHelper);

        return prismHelper.getProcessHelper().submitEntity(URLS.SUBMIT_URL, getProcessData());
    }

    public ServiceResponse submitBundle(boolean isUnique) throws Exception {

        //make sure bundle is unique
        if (isUnique)
            generateUniqueBundle();

        //submit the cluster first
        for (String clusterData : clusters) {
            clusterHelper.submitEntity(URLS.SUBMIT_URL, clusterData);
        }

        //lets submit all data first
        for (String dataset : getDataSets()) {

         feedHelper.submitEntity(URLS.SUBMIT_URL, dataset);


        }

        return processHelper.submitEntity(URLS.SUBMIT_URL, getProcessData());
    }

    public String submitAndScheduleBundle(PrismHelper prismHelper, boolean isUnique)
    throws Exception {
        if (isUnique) {
            ServiceResponse submitResponse = submitBundle(prismHelper);
            if (submitResponse.getCode() == 400)
                return submitResponse.getMessage();
        } else {
            ServiceResponse submitResponse = submitBundle(false);
            if (submitResponse.getCode() == 400)
                return submitResponse.getMessage();
        }
        //lets schedule the damn thing now :)
        ServiceResponse scheduleResult =
                processHelper.schedule(URLS.SCHEDULE_URL, getProcessData());
        logger.info("process schedule result=" + scheduleResult);

        Assert.assertEquals(Util.parseResponse(scheduleResult).getStatus(),
                APIResult.Status.SUCCEEDED);
        Assert.assertEquals(Util.parseResponse(scheduleResult).getStatusCode(), 200);

        Thread.sleep(7000);
        //also fetch the coordinator info
        //return Util.getOozieCoordinator(Util.readEntityName(processData));
       /* List<String> coordinatorStatus = Util.getOozieJobStatus(Util.readEntityName
        // (processData));

        if (!coordinatorStatus.isEmpty()) {
            //validate that the coordinator is up and RUNNING in state after submission
            return coordinatorStatus.get(0);
        } else return null;*/
        return null;
    }

    public void updateWorkFlowFile() throws Exception {
        Process processElement = InstanceUtil.getProcessElement(this);
        Workflow wf = processElement.getWorkflow();
        File wfFile = new File(sBundleLocation + "/workflow/workflow.xml");
        if (!wfFile.exists()) {
            System.out.println("workflow not provided along with process and feed xmls");
            return;
        }
        //is folder present
        if (!HadoopUtil.isDirPresent(colohelper, wf.getPath())) {
            System.out.println("workflowPath does not exists: creating path: " + wf.getPath());
            HadoopUtil.createDir(colohelper, wf.getPath());
        }

        // If File is present in hdfs check for contents and replace if found different


        if (HadoopUtil.isFilePresentHDFS(colohelper, wf.getPath(), "workflow.xml")) {

            HadoopUtil.deleteFile(colohelper, new Path(wf.getPath() + "/workflow.xml"));
            /*	File tmpWorkflow = HadoopUtil.getFileFromHDFSFolder(colohelper,
            wf.getPath()+"/workflow.xml",
            "target/tmpWorkflow.xml");
				byte[] wfFileBytes = org.apache.commons.io.FileUtils.readFileToByteArray(wfFile);
				byte[] tmpWorkflowBytes = org.apache.commons.io.FileUtils.readFileToByteArray
				(tmpWorkflow);

				if(!Arrays.equals(wfFileBytes,tmpWorkflowBytes)) {
					HadoopUtil.copyDataToFolder(colohelper, new Path(wf.getPath()+"/workflow.xml"),
					sBundleLocation+"/workflow/workflow.xml");
				}*/
        }
        // If there is no file in hdfs , replace it anyways

        HadoopUtil.copyDataToFolder(colohelper, new Path(wf.getPath() + "/workflow.xml"),
                wfFile.getAbsolutePath());
    }

    public String submitAndScheduleBundle(PrismHelper prismHelper) throws Exception {

        if (colohelper != null) {
            updateWorkFlowFile();
            //	updateLibFile();
        }
        ServiceResponse submitResponse = submitBundle(prismHelper);
        if (submitResponse.getCode() == 400)
            return submitResponse.getMessage();

        //lets schedule the damn thing now :)
        ServiceResponse scheduleResult =
                prismHelper.getProcessHelper().schedule(URLS.SCHEDULE_URL, getProcessData());
        logger.info("process schedule result=" + scheduleResult);
        Assert.assertEquals(Util.parseResponse(scheduleResult).getStatus(),
                APIResult.Status.SUCCEEDED);
        Assert.assertEquals(Util.parseResponse(scheduleResult).getStatusCode(), 200);

        Thread.sleep(7000);
        //also fetch the coordinator info
        //return Util.getOozieCoordinator(Util.readEntityName(processData));
        /*	ArrayList<String> coordinatorStatus=Util.getOozieJobStatus(this.getFeedHelper(),
        Util.readEntityName(processData));

		if(!coordinatorStatus.isEmpty())
		{
			//validate that the coordinator is up and RUNNING in state after submission
			return coordinatorStatus.get(0);
		}
		else return null;*/

        return scheduleResult.getMessage();
    }

    /*public String submitAndScheduleBundleWithFeedScheduled(PrismHelper prismHelper)
    throws Exception {
        ServiceResponse submitResponse = submitBundle(prismHelper);
        if (submitResponse.getCode() == 400)
            return submitResponse.getMessage();

        //schedule the feeds also

        for (String feed : getDataSets()) {
            ServiceResponse scheduleResult = feedHelper.schedule(URLS.SCHEDULE_URL, feed);
            Assert.assertEquals(Util.parseResponse(scheduleResult).getStatus(),
                    APIResult.Status.SUCCEEDED);
            Assert.assertEquals(Util.parseResponse(scheduleResult).getStatusCode(), 200,
                    "could not schedule feed:" + Util.readDatasetName(feed));
        }

        //lets schedule the damn thing now :)
        ServiceResponse scheduleResult =
                processHelper.schedule(URLS.SCHEDULE_URL, getProcessData());
        logger.info("process schedule result=" + scheduleResult);

        Assert.assertEquals(Util.parseResponse(scheduleResult).getStatus(),
                APIResult.Status.SUCCEEDED);
        Assert.assertEquals(Util.parseResponse(scheduleResult).getStatusCode(), 200);

        Thread.sleep(7000);
        //also fetch the coordinator info
        //return Util.getOozieCoordinator(Util.readEntityName(processData));
        List<String> coordinatorStatus = Util.getOozieJobStatus(Util.readEntityName(processData));

        if (!coordinatorStatus.isEmpty()) {
            //validate that the coordinator is up and RUNNING in state after submission
            return coordinatorStatus.get(0);
        } else return null;

    }*/

    public Bundle() {
    }

    @DataProvider(name = "DP")
    public static Object[][] getTestData(Method m) throws Exception {

        return Util.readBundles();
    }

    @DataProvider(name = "EL-DP")
    public static Object[][] getELTestData(Method m) throws Exception {

        return Util.readELBundles();
    }

    public void setInvalidData() throws Exception {
        //File f = new File("src/test/resources/ELbundle/valid/bundle1/feed-template1.xml");

        JAXBContext jc = JAXBContext.newInstance(Feed.class);

        Unmarshaller u = jc.createUnmarshaller();

        int index = 0;
        Feed dataElement = (Feed) u.unmarshal(new StringReader(dataSets.get(0)));
        if (!dataElement.getName().contains("raaw-logs16")) {
            dataElement = (Feed) u.unmarshal(new StringReader(dataSets.get(1)));
            index = 1;
        }

        //Util.print("dataFeed: "+dataSets.get(0));

        String oldLocation = dataElement.getLocations().getLocation().get(0).getPath();
        Util.print("oldlocation: " + oldLocation);
        dataElement.getLocations().getLocation().get(0).setPath(
                oldLocation.substring(0, oldLocation.indexOf('$')) + "invalid/" +
                        oldLocation.substring(oldLocation.indexOf('$')));
        Util.print("new location: " + dataElement.getLocations().getLocation().get(0).getPath());

        //lets marshall it back and return
        java.io.StringWriter sw = new StringWriter();

        Marshaller marshaller = jc.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_ENCODING, "UTF-8");
        marshaller.marshal(dataElement, sw);

        dataSets.set(index, sw.toString());

    }


    public void setFeedValidity(String feedStart, String feedEnd, String feedName)
    throws Exception {

        Feed feedElement = InstanceUtil.getFeedElement(this, feedName);
        feedElement.getClusters().getCluster().get(0).getValidity()
                .setStart(InstanceUtil.oozieDateToDate(feedStart).toDate());
        feedElement.getClusters().getCluster().get(0).getValidity()
                .setEnd(InstanceUtil.oozieDateToDate(feedEnd).toDate());
        InstanceUtil.writeFeedElement(this, feedElement, feedName);


    }


    /*public Date getInitialDatasetTime() throws Exception {
        JAXBContext jc = JAXBContext.newInstance(Feed.class);

        Unmarshaller u = jc.createUnmarshaller();

        Feed dataElement = (Feed) u.unmarshal((new StringReader(dataSets.get(0))));
        if (!dataElement.getName().contains("raaw-logs16")) {
            dataElement = (Feed) u.unmarshal(new StringReader(dataSets.get(1)));

        }

        //2010-01-01T00:00Z new SimpleDateFormat("yyyy'-'MM'-'dd'T'HH':'mm':'ss'Z'");

        DateFormat formatter = new SimpleDateFormat("yyyy'-'MM'-'dd'T'HH':'mm'Z'");
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        formatter.setCalendar(cal);

        //	Date dt = formatter.parse(dataElement.getClusters().getCluster().get(0).getValidity()
        // .getStart());
        Date dt = dataElement.getClusters().getCluster().get(0).getValidity().getStart();
        return dt;
    }*/


    public int getInitialDatasetFrequency() throws Exception {
        JAXBContext jc = JAXBContext.newInstance(Feed.class);

        Unmarshaller u = jc.createUnmarshaller();

        Feed dataElement = (Feed) u.unmarshal((new StringReader(dataSets.get(0))));
        if (!dataElement.getName().contains("raaw-logs16")) {
            dataElement = (Feed) u.unmarshal(new StringReader(dataSets.get(1)));

        }
        //Util.print("cluster start time: " +dataElement.getClusters().getCluster().get(0)
        // .getValidity().getStart());
        if (dataElement.getFrequency().getTimeUnit().equals(TimeUnit.hours))
            return (dataElement.getFrequency().getFrequency()) * 60;
        else return (dataElement.getFrequency().getFrequency());

    }

    public Date getStartInstanceProcess(Calendar time) throws Exception {
        Process processElement = InstanceUtil.getProcessElement(this);
        Util.print("start instance: " + processElement.getInputs().getInput().get(0).getStart());
        return ELUtil.getMinutes(processElement.getInputs().getInput().get(0).getStart(), time);
    }

    public Date getEndInstanceProcess(Calendar time) throws Exception {
        Process processElement = InstanceUtil.getProcessElement(this);
        Util.print("end instance: " + processElement.getInputs().getInput().get(0).getEnd());
        Util.print("timezone in getendinstance: " + time.getTimeZone().toString());
        Util.print("time in getendinstance: " + time.getTime());
        return ELUtil.getMinutes(processElement.getInputs().getInput().get(0).getEnd(), time);
    }

    public void setDatasetInstances(String startInstance, String endInstance) throws Exception {
        Process processElement = InstanceUtil.getProcessElement(this);
        processElement.getInputs().getInput().get(0).setStart(startInstance);
        processElement.getInputs().getInput().get(0).setEnd(endInstance);
        InstanceUtil.writeProcessElement(this, processElement);
    }

    public void setProcessPeriodicity(int frequency, TimeUnit periodicity) throws Exception {
        Process processElement = InstanceUtil.getProcessElement(this);
        Frequency frq = new Frequency(frequency, periodicity);
        processElement.setFrequency(frq);
        InstanceUtil.writeProcessElement(this, processElement);
    }

    public void setOutputFeedPeriodicity(int frequency, TimeUnit periodicity) throws JAXBException {
        JAXBContext jc = JAXBContext.newInstance(Process.class);
        Unmarshaller u = jc.createUnmarshaller();
        Process processElement = (Process) u.unmarshal((new StringReader(processData)));
        String outputDataset = null;
        int datasetIndex;
        for (datasetIndex = 0; datasetIndex < dataSets.size(); datasetIndex++) {
            outputDataset = dataSets.get(datasetIndex);
            if (outputDataset.contains(processElement.getOutputs().getOutput().get(0).getFeed())) {
                break;
            }
        }

        jc = JAXBContext.newInstance(Feed.class);
        u = jc.createUnmarshaller();
        Feed feedElement = (Feed) u.unmarshal((new StringReader(outputDataset)));

        feedElement.setFrequency(new Frequency(frequency, periodicity));
        java.io.StringWriter sw = new StringWriter();
        Marshaller marshaller = jc.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_ENCODING, "UTF-8");
        marshaller.marshal(feedElement, sw);
        dataSets.set(datasetIndex, sw.toString());
        Util.print("modified o/p dataSet is: " + dataSets.get(datasetIndex));
    }

    public int getProcessConcurrency() throws Exception {
        return InstanceUtil.getProcessElement(this).getParallel();
    }

    public void setOutputFeedLocationData(String path) throws JAXBException {
        JAXBContext jc = JAXBContext.newInstance(Process.class);
        Unmarshaller u = jc.createUnmarshaller();
        Process processElement = (Process) u.unmarshal((new StringReader(processData)));
        String outputDataset = null;
        int datasetIndex;
        for (datasetIndex = 0; datasetIndex < dataSets.size(); datasetIndex++) {
            outputDataset = dataSets.get(datasetIndex);
            if (outputDataset.contains(processElement.getOutputs().getOutput().get(0).getFeed())) {
                break;
            }
        }

        jc = JAXBContext.newInstance(Feed.class);
        u = jc.createUnmarshaller();
        Feed feedElement = (Feed) u.unmarshal((new StringReader(outputDataset)));
        Location l = new Location();
        l.setPath(path);
        l.setType(LocationType.DATA);
        feedElement.getLocations().getLocation().set(0, l);
        java.io.StringWriter sw = new StringWriter();
        Marshaller marshaller = jc.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_ENCODING, "UTF-8");
        marshaller.marshal(feedElement, sw);
        dataSets.set(datasetIndex, sw.toString());
        Util.print("modified location path dataSet is: " + dataSets.get(datasetIndex));
    }

    public void setProcessConcurrency(int concurrency) throws Exception {
        Process processElement = InstanceUtil.getProcessElement(this);
        processElement.setParallel((concurrency));
        InstanceUtil.writeProcessElement(this, processElement);
    }

    public void setProcessWorkflow(String wfPath) throws Exception {
        Process processElement = InstanceUtil.getProcessElement(this);
        Workflow w = processElement.getWorkflow();
        w.setPath(wfPath);
        processElement.setWorkflow(w);
        InstanceUtil.writeProcessElement(this, processElement);
    }

    public Process getProcessObject() throws Exception {
        JAXBContext context = JAXBContext.newInstance(Process.class);
        Unmarshaller um = context.createUnmarshaller();
        return (Process) um.unmarshal(new StringReader(getProcessData()));
    }

    /*public org.apache.falcon.regression.core.generated.cluster.Cluster getClusterObject()
    throws Exception {
        JAXBContext context =
                JAXBContext.newInstance(
                        org.apache.falcon.regression.core.generated.cluster.Cluster.class);
        Unmarshaller um = context.createUnmarshaller();
        return (org.apache.falcon.regression.core.generated.cluster.Cluster) um
                .unmarshal(new StringReader(getClusterData()));
    }*/

    /*public Feed getFeedObject(String name) throws Exception {
        JAXBContext context = JAXBContext.newInstance(Feed.class);
        Unmarshaller um = context.createUnmarshaller();
        for (String feed : getDataSets()) {
            if (Util.readDatasetName(feed).equalsIgnoreCase(name)) {
                return (Feed) um.unmarshal(new StringReader(feed));
            }
        }

        return null;
    }*/

    public String getFeed(String feedName) throws Exception {
        for (String feed : getDataSets()) {
            if (Util.readDatasetName(feed).contains(feedName)) {
                return feed;
            }
        }

        return null;
    }

    /*public void writeBundleToFiles() throws Exception {
        for (String cluster : this.clusters) {
            addClusterFilePath(clusterHelper.writeEntityToFile(cluster));
        }

        for (String dataset : dataSets) {
            addFeedFilePaths(feedHelper.writeEntityToFile(dataset));
        }

        setProcessFilePath(processHelper.writeEntityToFile(processData));
    }*/

    //	public void submitBundleViaCLI() throws Exception
    //	{
    //		//submit cluster
    //
    //                for(String clusterPath:clusterFilePaths)
    //                {
    //                    Assert.assertTrue(clusterHelper.submitEntityViaCLI(URLS.SUBMIT_URL,
    // clusterPath).contains("Submit successful"));
    //                }
    //		//submit feed
    //		for(String dataset:getFeedFilePaths())
    //		{
    //			Assert.assertTrue(feedHelper.submitEntityViaCLI(URLS.SUBMIT_URL,
    // dataset).contains("Submit successful"));
    //		}
    //		//submit process
    //		Assert.assertTrue(processHelper.submitEntityViaCLI(URLS.SUBMIT_URL,
    // getProcessFilePath()).contains("Submit
    // successful"));
    //	}

    public void setInputFeedPeriodicity(int frequency, TimeUnit periodicity) throws Exception {
        String feedName = Util.getInputFeedNameFromBundle(this);
        Feed feedElement = InstanceUtil.getFeedElement(this, feedName);
        Frequency frq = new Frequency(frequency, periodicity);
        feedElement.setFrequency(frq);
        InstanceUtil.writeFeedElement(this, feedElement, feedName);

    }

    /*public String readFeedNameFromFile(String feedFilePath) throws Exception {

        BufferedReader br = new BufferedReader(
                new InputStreamReader(new FileInputStream(new File(feedFilePath))));
        String feed = "";
        String line;
        while ((line = br.readLine()) != null) {
            feed += line;
        }

        return Util.readDatasetName(feed);

    }*/

    public void setInputFeedValidity(String startInstance, String endInstance) throws Exception {
        String feedName = Util.getInputFeedNameFromBundle(this);
        this.setFeedValidity(startInstance, endInstance, feedName);
    }

    public void setInputFeedDataPath(String path) throws Exception {
        String feedName = Util.getInputFeedNameFromBundle(this);
        Feed feedElement = InstanceUtil.getFeedElement(this, feedName);
        feedElement.getLocations().getLocation().get(0).setPath(path);
        InstanceUtil.writeFeedElement(this, feedElement, feedName);
    }

    /*public void setInputFeedRetentionLimit(int frquency, TimeUnit timeUnit) throws Exception {
        String feedName = Util.getInputFeedNameFromBundle(this);
        Feed feedElement = InstanceUtil.getFeedElement(this, feedName);
        Retention retention = feedElement.getClusters().getCluster().get(0).getRetention();
        retention.setLimit(new Frequency(frquency, timeUnit));
        feedElement.getClusters().getCluster().get(0).setRetention(retention);
        InstanceUtil.writeFeedElement(this, feedElement, feedName);


    }*/

    public String getFeedDataPathPrefix() throws Exception {
        Feed feedElement = InstanceUtil.getFeedElement(this, Util.getInputFeedNameFromBundle(this));
        String p = feedElement.getLocations().getLocation().get(0).getPath();
        p = p.substring(0, p.indexOf("$"));
        return p;
    }

    public void setProcessValidity(DateTime startDate, DateTime endDate, String clusterName)
    throws Exception {

        JAXBContext jc = JAXBContext.newInstance(Process.class);

        Unmarshaller u = jc.createUnmarshaller();

        Process processElement = (Process) u.unmarshal((new StringReader(processData)));

        for (Cluster cluster : processElement.getClusters().getCluster()) {
            if (cluster.getName().equalsIgnoreCase(clusterName)) {
                org.apache.falcon.regression.core.generated.process.Validity validity =
                        new org.apache.falcon.regression.core.generated.process.Validity();
                validity.setStart(startDate.toDate());
                validity.setEnd(endDate.toDate());
                cluster.setValidity(validity);
            }
        }


        java.io.StringWriter sw = new StringWriter();
        Marshaller marshaller = jc.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_ENCODING, "UTF-8");
        marshaller.marshal(processElement, sw);
        processData = sw.toString();
    }

    public void setProcessValidity(DateTime startDate, DateTime endDate) throws Exception {

        JAXBContext jc = JAXBContext.newInstance(Process.class);

        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd/HH:mm");

        String start = formatter.print(startDate).replace("/", "T") + "Z";
        String end = formatter.print(endDate).replace("/", "T") + "Z";

        Unmarshaller u = jc.createUnmarshaller();

        Process processElement = (Process) u.unmarshal((new StringReader(processData)));

        for (Cluster cluster : processElement.getClusters().getCluster()) {

            org.apache.falcon.regression.core.generated.process.Validity validity =
                    new org.apache.falcon.regression.core.generated.process.Validity();
            validity.setStart(InstanceUtil.oozieDateToDate(start).toDate());
            validity.setEnd(InstanceUtil.oozieDateToDate(end).toDate());
            cluster.setValidity(validity);

        }


        java.io.StringWriter sw = new StringWriter();
        Marshaller marshaller = jc.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_ENCODING, "UTF-8");
        marshaller.marshal(processElement, sw);
        processData = sw.toString();
    }

    public void setProcessValidity(String startDate, String endDate) throws Exception {

        JAXBContext jc = JAXBContext.newInstance(Process.class);


        Unmarshaller u = jc.createUnmarshaller();

        Process processElement = (Process) u.unmarshal((new StringReader(processData)));

        for (Cluster cluster : processElement.getClusters().getCluster()) {

            org.apache.falcon.regression.core.generated.process.Validity validity =
                    new org.apache.falcon.regression.core.generated.process.Validity();
            validity.setStart(InstanceUtil.oozieDateToDate(startDate).toDate());
            validity.setEnd(InstanceUtil.oozieDateToDate(endDate).toDate());
            cluster.setValidity(validity);

        }


        java.io.StringWriter sw = new StringWriter();
        Marshaller marshaller = jc.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_ENCODING, "UTF-8");
        marshaller.marshal(processElement, sw);
        processData = sw.toString();
    }

    public void setProcessLatePolicy(LateProcess lateProcess) throws Exception {
        JAXBContext jc = JAXBContext.newInstance(Process.class);
        Unmarshaller u = jc.createUnmarshaller();

        Process processElement = (Process) u.unmarshal((new StringReader(processData)));
        processElement.setLateProcess(lateProcess);

        java.io.StringWriter sw = new StringWriter();
        Marshaller marshaller = jc.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_ENCODING, "UTF-8");
        marshaller.marshal(processElement, sw);
        processData = sw.toString();
    }

    /*public void setProcessLatePolicy(PolicyType policyType, String delay) throws Exception {
        JAXBContext jc = JAXBContext.newInstance(Process.class);
        Unmarshaller u = jc.createUnmarshaller();

        Process processElement = (Process) u.unmarshal((new StringReader(processData)));


        LateProcess lateProcess = processElement.getLateProcess();
        lateProcess.setDelay(new Frequency(delay));
        lateProcess.setPolicy(policyType);

		*//*<LateInput> lateList=new ArrayList<LateInput>();

		for(Input input:processElement.getInputs().getInput())
		{
			LateInput late=new LateInput();
			late.setInput(input.getName());
			late.setWorkflowPath(processElement.getWorkflow().getPath());
			lateList.add(late);
		}

		//	lateProcess.setLateInput(lateList);*//*

        processElement.setLateProcess(lateProcess);

        java.io.StringWriter sw = new StringWriter();
        Marshaller marshaller = jc.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_ENCODING, "UTF-8");
        marshaller.marshal(processElement, sw);
        processData = sw.toString();
    }*/

    /*public String getFeedFilePath(String feedName) throws Exception {
        for (String feedPath : this.feedFilePaths) {
            BufferedReader reader = new BufferedReader(new FileReader(new File(feedPath)));
            String data = "";
            String feed = "";
            while ((data = reader.readLine()) != null) {
                feed += data;
            }
            if (feed.contains(feedName)) {
                return feedPath;
            }
        }
        return null;

    }*/

    /*public String getClusterFilePath(String clusterName) throws Exception {
        for (String clusterPath : this.clusterFilePaths) {
            BufferedReader reader = new BufferedReader(new FileReader(new File(clusterPath)));
            String data = "";
            String feed = "";
            while ((data = reader.readLine()) != null) {
                feed += data;
            }
            if (feed.contains(clusterName)) {
                return clusterPath;
            }
        }
        return null;

    }*/


    /*public void listBundle() throws Exception {
        Assert.assertTrue(processHelper.list().contains(Util.readEntityName(processData)),
                "Process was not listed post submission!");
        Assert.assertTrue(clusterHelper.list().contains(Util.readClusterName(clusterData)),
                "CLuster was not listed post submission!");

        String dataList = feedHelper.list();

        for (String data : dataSets) {
            Assert.assertTrue(dataList.contains(Util.readDatasetName(data)),
                    "Feed " + Util.readDatasetName(data) + " was not listed post submission!");
        }
    }*/

    public void verifyDependencyListing() throws Exception {
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

/*
    public void verifyFeedDependencyListing() throws Exception {
        for (String feed : getDataSets()) {
            for (String cluster : clusters) {
                Assert.assertTrue(feedHelper.getDependencies(Util.readDatasetName(feed))
                        .contains("(cluster) " + Util.readClusterName(cluster)));
            }
            Assert.assertFalse(feedHelper.getDependencies(Util.readDatasetName(feed))
                    .contains("(process)" + Util.readEntityName(getProcessData())));
        }
    }
*/

    public void addProcessInput(String feed, String feedName) throws Exception {
        Process processElement = InstanceUtil.getProcessElement(this);
        Input in1 = processElement.getInputs().getInput().get(0);
        Input in2 = new Input();
        in2.setEnd(in1.getEnd());
        in2.setFeed(feed);
        in2.setName(feedName);
        in2.setPartition(in1.getPartition());
        in2.setStart(in1.getStart());
        processElement.getInputs().getInput().add(in2);
        InstanceUtil.writeProcessElement(this, processElement);
    }

    public void setProcessName(String newName) throws Exception {
        Process processElement = InstanceUtil.getProcessElement(this);
        processElement.setName(newName);
        InstanceUtil.writeProcessElement(this, processElement);

    }

    /*public void setProcessExecution(ExecutionType exeType) throws Exception {
        Process processElement = InstanceUtil.getProcessElement(this);
        processElement.setOrder(exeType);
        InstanceUtil.writeProcessElement(this, processElement);
    }*/

    /*public void setOutputDataInstance(String instance) throws Exception {
        Process processElement = InstanceUtil.getProcessElement(this);
        processElement.getOutputs().getOutput().get(0).setInstance(instance);
        InstanceUtil.writeProcessElement(this, processElement);
    }*/

    public void setRetry(Retry retry) throws Exception {
        logger.info("old process: " + processData);
        Process processObject = getProcessObject();
        processObject.setRetry(retry);
        java.io.StringWriter sw = new StringWriter();
        Marshaller marshaller = JAXBContext.newInstance(Process.class).createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_ENCODING, "UTF-8");
        marshaller.marshal(processObject, sw);
        processData = sw.toString();
        logger.info("updated process: " + processData);
    }

    public void setInputFeedAvailabilityFlag(String flag) throws Exception {
        String feedName = Util.getInputFeedNameFromBundle(this);
        Feed feedElement = InstanceUtil.getFeedElement(this, feedName);
        feedElement.setAvailabilityFlag(flag);
        InstanceUtil.writeFeedElement(this, feedElement, feedName);
    }

/*
    public void setProcessLatePolicy(PolicyType policy, int delay, TimeUnit periodicity,
                                     Frequency delayUnit)
    throws Exception {
        Process process = getProcessObject();

        process.getLateProcess().setDelay(new Frequency(delay, periodicity));
        process.getLateProcess().setPolicy(policy);
        InstanceUtil.writeProcessElement(this, process);

    }
*/

    public Cluster getClusterObjectFromProcess(String clusterName) throws Exception {
        for (Cluster cluster : getProcessObject().getClusters().getCluster()) {
            if (cluster.getName().equalsIgnoreCase(clusterName)) {
                return cluster;
            }
        }
        return null;
    }

    /*public Cluster getClusterObjectFromProcess(Process processObject, String clusterName)
    throws Exception {
        for (Cluster cluster : processObject.getClusters().getCluster()) {
            if (cluster.getName().equalsIgnoreCase(clusterName)) {
                return cluster;
            }
        }
        return null;
    }*/

    public void setCLusterColo(String colo) throws Exception {
        org.apache.falcon.regression.core.generated.cluster.Cluster c =
                InstanceUtil.getClusterElement(this);
        c.setColo(colo);
        InstanceUtil.writeClusterElement(this, c);

    }

    public void setCLusterWorkingPath(String clusterData, String path) throws Exception {

        org.apache.falcon.regression.core.generated.cluster.Cluster c =
                InstanceUtil.getClusterElement(clusterData);

        for (int i = 0; i < c.getLocations().getLocation().size(); i++) {
            if (c.getLocations().getLocation().get(i).getName().contains("working"))
                c.getLocations().getLocation().get(i).setPath(path);
        }

        //this.setClusterData(clusterData)
        InstanceUtil.writeClusterElement(this, c);
    }


    public void submitClusters(PrismHelper prismHelper) throws Exception {
        for (String cluster : this.clusters) {
            Util.assertSucceeded(
                    prismHelper.getClusterHelper().submitEntity(URLS.SUBMIT_URL, cluster));
        }
    }

    public void submitFeeds(PrismHelper prismHelper) throws Exception {
        for (String feed : this.dataSets) {
            Util.assertSucceeded(prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed));
        }
    }

    public void addClusterToBundle(String clusterData, ClusterType type) throws Exception {

        clusterData = setNewClusterName(clusterData);

        this.clusters.add(clusterData);
        //now to add clusters to feeds
        for (int i = 0; i < dataSets.size(); i++) {
            Feed feedObject = Util.getFeedObject(dataSets.get(i));
            org.apache.falcon.regression.core.generated.feed.Cluster cluster =
                    new org.apache.falcon.regression.core.generated.feed.Cluster();
            cluster.setName(Util.getClusterObject(clusterData).getName());
            cluster.setValidity(feedObject.getClusters().getCluster().get(0).getValidity());
            cluster.setType(type);
            cluster.setRetention(feedObject.getClusters().getCluster().get(0).getRetention());
            feedObject.getClusters().getCluster().add(cluster);

            dataSets.remove(i);
            dataSets.add(i, this.feedHelper.toString(feedObject));
            //feed=this.feedHelper.toString(feedObject);

        }


        //now to add cluster to process
        Process processObject = Util.getProcessObject(processData);
        Cluster cluster = new Cluster();
        cluster.setName(Util.getClusterObject(clusterData).getName());
        cluster.setValidity(processObject.getClusters().getCluster().get(0).getValidity());
        processObject.getClusters().getCluster().add(cluster);
        this.processData = processHelper.toString(processObject);

    }

    private String setNewClusterName(String clusterData) throws Exception {
        org.apache.falcon.regression.core.generated.cluster.Cluster clusterObj =
                Util.getClusterObject(clusterData);
        clusterObj.setName(clusterObj.getName() + this.clusters.size() + 1);
        return clusterHelper.toString(clusterObj);
    }

    public void deleteBundle(PrismHelper prismHelper) throws Exception {

        prismHelper.getProcessHelper().delete(URLS.DELETE_URL, getProcessData());

        for (String dataset : getDataSets()) {

             prismHelper.getFeedHelper().delete(URLS.DELETE_URL, dataset);
        }

        for (String cluster : this.getClusters()) {

            prismHelper.getClusterHelper().delete(URLS.DELETE_URL, cluster);
        }


    }

    public String getProcessName() throws Exception {

        return Util.getProcessName(this.getProcessData());
    }

    public void setProcessQueueName(String queueName) throws Exception {
        Process processElement = InstanceUtil.getProcessElement(this);
        Property p = new Property();
        p.setName("mapred.job.queue.name");
        p.setValue(queueName);
        Properties propList = processElement.getProperties();
        propList.addProperty(p);

        processElement.setProperties(propList);
        InstanceUtil.writeProcessElement(this, processElement);

    }

    /*public void addProcessProperty(String propertyName, String propertyValue) throws Exception {
        Process processElement = InstanceUtil.getProcessElement(this);
        Property p = new Property();
        p.setName(propertyName);
        p.setValue(propertyValue);
        Properties propList = processElement.getProperties();
        propList.addProperty(p);

        processElement.setProperties(propList);
        InstanceUtil.writeProcessElement(this, processElement);

    }*/

    public void setProcessPriority(String priority) throws Exception {
        Process processElement = InstanceUtil.getProcessElement(this);
        Property p = new Property();
        p.setName("mapred.job.priority");
        p.setValue(priority);
        Properties propList = processElement.getProperties();
        propList.addProperty(p);
        processElement.setProperties(propList);
        InstanceUtil.writeProcessElement(this, processElement);
    }

    public void setProcessLibPath(String libPath) throws Exception {
        Process processElement = InstanceUtil.getProcessElement(this);
        Workflow wf = processElement.getWorkflow();
        wf.setLib(libPath);
        processElement.setWorkflow(wf);
        InstanceUtil.writeProcessElement(this, processElement);

    }

    public void setProcessTimeOut(int magnitude, TimeUnit unit) throws Exception {
        Process processElement = InstanceUtil.getProcessElement(this);
        Frequency frq = new Frequency(magnitude, unit);
        processElement.setTimeout(frq);
        InstanceUtil.writeProcessElement(this, processElement);
    }

    public static void submitCluster(Bundle... bundles) throws Exception {

        for (Bundle bundle : bundles) {
            Util.print("cluster b1: " + bundle.getClusters().get(0));
            ServiceResponse r =
                    prismHelper.getClusterHelper()
                            .submitEntity(URLS.SUBMIT_URL, bundle.getClusters().get(0));
            Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));
        }


    }

    public static void deleteCluster(Bundle... bundles) throws Exception {

        for (Bundle bundle : bundles) {
            Util.print("cluster b1: " + bundle.getClusters().get(0));
            prismHelper.getClusterHelper().delete(URLS.DELETE_URL, bundle.getClusters().get(0));
        }

    }

    public List<Output> getAllOutputs() throws Exception {

        Process p = InstanceUtil.getProcessElement(processData);

        return p.getOutputs().getOutput();

    }

    /*public void addFeedPartition(String feedName, String partition) throws Exception {

        Feed feedElement = InstanceUtil.getFeedElement( this, feedName);
        Partitions p = feedElement.getPartitions();
        Partition newPartiton = new Partition();
        newPartiton.setName(partition);
        p.addPartition(newPartiton);
        feedElement.setPartitions(p);
        InstanceUtil.writeFeedElement(this, feedElement, feedName);

    }*/

    public Bundle getRequiredBundle(Bundle b, int numberOfClusters, int numberOfInputs,
                                    int numberOfOptionalInput,
                                    String inputBasePaths, int numberOfOutputs, String startTime,
                                    String endTime)
    throws Exception {


        //generate clusters And setCluster
        org.apache.falcon.regression.core.generated.cluster.Cluster c = InstanceUtil
                .getClusterElement(Util.generateUniqueClusterEntity(b.getClusters().get(0)));
        List<String> newClusters = new ArrayList<String>();
        List<String> newDataSets = new ArrayList<String>();


        for (int i = 0; i < numberOfClusters; i++) {
            String clusterName = c.getName() + i;
            c.setName(clusterName);
            newClusters.add(i, InstanceUtil.ClusterElementToString(c));
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


    /*public Bundle getRequiredBundle(Bundle b, int numberOfClusters, int numberOfInputs,
                                    int numberOfOptionalInput,
                                    String inputBasePaths, int numberOfOutputs, String startTime,
                                    String endTime,
                                    String... propFiles)
    throws Exception {


        //generate clusters And setCluster
        List<String> newClusters = new ArrayList<String>();
        List<String> newDataSets = new ArrayList<String>();


        for (int i = 0; i < numberOfClusters; i++) {
            Bundle temp = (Bundle) Util.readELBundles()[0][0];
            ColoHelper coloHelper = new ColoHelper(propFiles[i]);
            temp = new Bundle(temp, coloHelper.getEnvFileName());
            org.apache.falcon.regression.core.generated.cluster.Cluster c = InstanceUtil
                    .getClusterElement(Util.generateUniqueClusterEntity(temp.getClusters().get(0)));
            String clusterName = c.getName() + i;
            c.setName(clusterName);
            newClusters.add(i, InstanceUtil.ClusterElementToString(c));
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
    }*/

    public String setProcessFeeds(String process, List<String> newDataSets,
                                  int numberOfInputs, int numberOfOptionalInput,
                                  int numberOfOutputs) throws Exception {

        //	process = Util.generateUniqueProcessEntity(process);
        Process p = InstanceUtil.getProcessElement(process);
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

            } else {
                in.setOptional(false);
                if (isFirst) {
                    in.setName("inputData");
                    isFirst = false;
                } else
                    in.setName("inputData" + i);

            }

            numberOfOptionalSet++;


            in.setFeed(Util.readDatasetName(newDataSets.get(i)));
            is.getInput().add(in);
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
            os.getOutput().add(op);
        }

        p.setOutputs(os);

        p.setLateProcess(null);

		/*	LateProcess lp = p.getLateProcess();
		for(int i = 0 ; i < lp.getLateInput().size();i++){

			LateInput li = lp.getLateInput().get(i);
			li.setInput("impression");
			lp.getLateInput().set(i, li);

		}
		 */

        return InstanceUtil.processToString(p);
    }

    public String setProcessClusters(String process, List<String> newClusters, String startTime,
                                     String endTime)
    throws Exception {

        Process p = InstanceUtil.getProcessElement(process);
        org.apache.falcon.regression.core.generated.process.Clusters cs =
                new org.apache.falcon.regression.core.generated.process.Clusters();
        for (String newCluster : newClusters) {
            Cluster c = new Cluster();
            c.setName(Util.readClusterName(newCluster));
            org.apache.falcon.regression.core.generated.process.Validity v =
                    new org.apache.falcon.regression.core.generated.process.Validity();
            v.setStart(InstanceUtil.oozieDateToDate(startTime).toDate());
            v.setEnd(InstanceUtil.oozieDateToDate(endTime).toDate());
            c.setValidity(v);
            cs.getCluster().add(c);
        }

        p.setClusters(cs);

        return InstanceUtil.processToString(p);
    }

    public String setFeedClusters(String referenceFeed,
                                  List<String> newClusters, String location, String startTime,
                                  String endTime)
    throws Exception {

        Feed f = InstanceUtil.getFeedElement(referenceFeed);
        Clusters cs = new Clusters();
        f.setFrequency(new Frequency(5, TimeUnit.minutes));

        for (String newCluster : newClusters) {
            org.apache.falcon.regression.core.generated.feed.Cluster c =
                    new org.apache.falcon.regression.core.generated.feed.Cluster();
            c.setName(Util.readClusterName(newCluster));
            Location l = new Location();
            l.setType(LocationType.DATA);
            l.setPath(location + "/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            Locations ls = new Locations();
            ls.getLocation().add(l);
            c.setLocations(ls);
            Validity v = new Validity();
            startTime = InstanceUtil.addMinsToTime(startTime, -180);
            endTime = InstanceUtil.addMinsToTime(endTime, 180);
            v.setStart(InstanceUtil.oozieDateToDate(startTime).toDate());
            v.setEnd(InstanceUtil.oozieDateToDate(endTime).toDate());
            c.setValidity(v);
            Retention r = new Retention();
            r.setAction(ActionType.DELETE);
            Frequency f1 = new Frequency(20, TimeUnit.hours);
            r.setLimit(f1);
            r.setType(RetentionType.INSTANCE);
            c.setRetention(r);
            cs.getCluster().add(c);
        }

        f.setClusters(cs);
        return InstanceUtil.feedElementToString(f);
    }

    public void submitAndScheduleBundle(Bundle b, PrismHelper prismHelper,
                                        boolean checkSuccess) throws Exception {

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

    public String setProcessInputNames(String process, String... names) throws Exception {
        Process p = InstanceUtil.getProcessElement(process);

        for (int i = 0; i < names.length; i++) {
            p.getInputs().getInput().get(i).setName(names[i]);
        }

        return InstanceUtil.processToString(p);
    }

    public String addProcessProperty(String process, Property... properties)
    throws Exception {

        Process p = InstanceUtil.getProcessElement(process);

        for (Property property : properties) {
            p.getProperties().getProperty().add(property);
        }

        return InstanceUtil.processToString(p);

    }

    public String setProcessInputPartition(String process, String... partition) throws Exception {
        Process p = InstanceUtil.getProcessElement(process);

        for (int i = 0; i < partition.length; i++) {
            p.getInputs().getInput().get(i).setPartition(partition[i]);
        }

        return InstanceUtil.processToString(p);
    }

    public static Object[][] readBundle(String bundleLocation) throws Exception {
        sBundleLocation = bundleLocation;
        Util u = new Util();

        List<Bundle> bundleSet = u.getDataFromFolder(bundleLocation);

        Object[][] testData = new Object[bundleSet.size()][1];

        for (int i = 0; i < bundleSet.size(); i++) {
            testData[i][0] = bundleSet.get(i);
        }

        return testData;
    }

    public String setProcessOutputNames(String process, String... names) throws Exception {
        Process p = InstanceUtil.getProcessElement(process);
        Outputs outputs = p.getOutputs();
        if (outputs.getOutput().size() != names.length) {
            System.out.println("Number of output names not equal to output in processdef");
            return null;
        }

        for (int i = 0; i < names.length; i++) {
            outputs.getOutput().get(i).setName(names[i]);
        }
        p.setOutputs(outputs);
        return InstanceUtil.processToString(p);
    }
}




