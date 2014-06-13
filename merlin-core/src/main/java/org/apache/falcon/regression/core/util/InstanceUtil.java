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

package org.apache.falcon.regression.core.util;

import com.google.gson.GsonBuilder;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.falcon.entity.v0.feed.Cluster;
import org.apache.falcon.entity.v0.feed.Location;
import org.apache.falcon.entity.v0.feed.Locations;
import org.apache.falcon.entity.v0.feed.Validity;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.entity.v0.feed.CatalogTable;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.feed.ClusterType;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.feed.Retention;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.helpers.PrismHelper;
import org.apache.falcon.regression.core.interfaces.IEntityManagerHelper;
import org.apache.falcon.regression.core.response.APIResult;
import org.apache.falcon.regression.core.response.InstancesSummaryResult;
import org.apache.falcon.regression.core.response.ProcessInstancesResult;
import org.apache.falcon.regression.core.response.ResponseKeys;
import org.apache.falcon.regression.core.enumsAndConstants.ENTITY_TYPE;
import org.apache.falcon.request.BaseRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.http.HttpResponse;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job.Status;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;
import org.testng.Assert;
import org.apache.log4j.Logger;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class InstanceUtil {

    static Logger logger = Logger.getLogger(InstanceUtil.class);

    public static APIResult sendRequestProcessInstance(String
                                                           url, String user)
        throws IOException, URISyntaxException, AuthenticationException {
        return hitUrl(url, Util.getMethodType(url), user);
    }

    public static APIResult hitUrl(String url,
                                   String method, String user) throws URISyntaxException,
        IOException, AuthenticationException {
        BaseRequest request = new BaseRequest(url, method, user);
        HttpResponse response = request.run();

        BufferedReader reader = new BufferedReader(
            new InputStreamReader(response.getEntity().getContent(), "UTF-8"));
        StringBuilder string_response = new StringBuilder();
        for (String line; (line = reader.readLine()) != null; ) {
            string_response.append(line).append("\n");
        }
        String jsonString = string_response.toString();
        logger.info("The web service response is:\n" + Util.prettyPrintXmlOrJson(jsonString));
        APIResult r = null;
        try {
            if (url.contains("/summary/")) {
                //Order is not guaranteed in the getDeclaredConstructors() call
                Constructor<?> constructors[] = InstancesSummaryResult.class
                    .getDeclaredConstructors();
                for (Constructor<?> constructor : constructors) {
                    //we want to invoke the constructor that has no parameters
                    if (constructor.getParameterTypes().length == 0) {
                        constructor.setAccessible(true);
                        r = (InstancesSummaryResult) constructor.newInstance();
                        break;
                    }
                }
            } else {
                //Order is not guaranteed in the getDeclaredConstructors() call
                Constructor<?> constructors[] = ProcessInstancesResult.class
                    .getDeclaredConstructors();
                for (Constructor<?> constructor : constructors) {
                    //we want to invoke the constructor that has no parameters
                    if (constructor.getParameterTypes().length == 0) {
                        constructor.setAccessible(true);
                        r = (ProcessInstancesResult) constructor.newInstance();
                        break;
                    }
                }
            }
        } catch (IllegalAccessException e) {
            Assert.fail("Could not create InstancesSummaryResult or " +
                "ProcessInstancesResult constructor\n" + ExceptionUtils.getStackTrace(e));
        } catch (InstantiationException e) {
            Assert.fail("Could not create InstancesSummaryResult or " +
                "ProcessInstancesResult constructor\n" + ExceptionUtils.getStackTrace(e));
        } catch (InvocationTargetException e) {
            Assert.fail("Could not create InstancesSummaryResult or " +
                "ProcessInstancesResult constructor\n" + ExceptionUtils.getStackTrace(e));
        }
        Assert.assertNotNull(r, "APIResult is null");
        if (jsonString.contains("(PROCESS) not found")) {
            r.setStatusCode(ResponseKeys.PROCESS_NOT_FOUND);
            return r;
        } else if (jsonString.contains("Parameter start is empty") ||
            jsonString.contains("Unparseable date:")) {
            r.setStatusCode(ResponseKeys.UNPARSEABLE_DATE);
            return r;
        } else if (response.getStatusLine().getStatusCode() == 400 &&
            jsonString.contains("(FEED) not found")) {
            r.setStatusCode(400);
            return r;
        } else if (
            (response.getStatusLine().getStatusCode() == 400 &&
                jsonString.contains("is beforePROCESS  start")) ||
                response.getStatusLine().getStatusCode() == 400 &&
                    jsonString.contains("is after end date")
                || (response.getStatusLine().getStatusCode() == 400 &&
                jsonString.contains("is after PROCESS's end")) ||
                (response.getStatusLine().getStatusCode() == 400 &&
                    jsonString.contains("is before PROCESS's  start"))) {
            r.setStatusCode(400);
            return r;
        }
        if (url.contains("/summary/"))
            r = new GsonBuilder().setPrettyPrinting().create()
                .fromJson(jsonString, InstancesSummaryResult.class);
        else
            r = new GsonBuilder().setPrettyPrinting().create()
                .fromJson(jsonString, ProcessInstancesResult.class);

        logger.info("r.getMessage(): " + r.getMessage());
        logger.info("r.getStatusCode(): " + r.getStatusCode());
        logger.info("r.getStatus() " + r.getStatus());
        return r;
    }

    /**
     * Checks if API response reflects success and if it's instances match to expected status.
     *
     * @param r  - kind of response from API which should contain information about instances
     * @param b  - bundle from which process instances are being analyzed
     * @param ws - - expected status of instances
     * @throws JAXBException
     */
    public static void validateSuccess(ProcessInstancesResult r, Bundle b,
                                       ProcessInstancesResult.WorkflowStatus ws)
        throws JAXBException {
        Assert.assertEquals(r.getStatus(), APIResult.Status.SUCCEEDED);
        Assert.assertEquals(runningInstancesInResult(r, ws), b.getProcessConcurrency());
    }

    /**
     * Check the number of instances in response which have the same status as expected.
     *
     * @param r  - kind of response from API which should contain information about instances
     * @param ws - expected status of instances
     * @return - number of instances which have expected status
     */
    public static int runningInstancesInResult(ProcessInstancesResult r,
                                               ProcessInstancesResult.WorkflowStatus ws) {
        ProcessInstancesResult.ProcessInstance[] pArray = r.getInstances();
        int runningCount = 0;
        logger.info("pArray: " + Arrays.toString(pArray));
        for (int instanceIndex = 0; instanceIndex < pArray.length; instanceIndex++) {
            logger.info(
                "pArray[" + instanceIndex + "]: " + pArray[instanceIndex].getStatus() + " , " +
                    pArray[instanceIndex].getInstance()
            );

            if (pArray[instanceIndex].getStatus().equals(ws)) {
                runningCount++;
            }
        }
        return runningCount;
    }

    public static void validateSuccessWOInstances(ProcessInstancesResult r) {
        AssertUtil.assertSucceeded(r);
        Assert.assertNull(r.getInstances(), "Unexpected :" + Arrays.toString(r.getInstances()));
    }

    public static void validateSuccessWithStatusCode(ProcessInstancesResult r,
                                                     int expectedErrorCode) {
        Assert.assertEquals(r.getStatusCode(), expectedErrorCode,
            "Parameter start is empty should have the response");
    }

    public static void writeProcessElement(Bundle bundle, Process processElement)
        throws JAXBException {
        JAXBContext jc = JAXBContext.newInstance(Process.class);
        java.io.StringWriter sw = new StringWriter();
        Marshaller marshaller = jc.createMarshaller();
        marshaller.marshal(processElement, sw);
        //logger.info("modified process is: " + sw);
        bundle.setProcessData(sw.toString());
    }

    public static Process getProcessElement(Bundle bundle) throws JAXBException {
        JAXBContext jc = JAXBContext.newInstance(Process.class);
        Unmarshaller u = jc.createUnmarshaller();
        return (Process) u.unmarshal((new StringReader(bundle.getProcessData())));

    }

    public static Feed getFeedElement(Bundle bundle, String feedName) throws JAXBException {
        JAXBContext jc = JAXBContext.newInstance(Feed.class);
        Unmarshaller u = jc.createUnmarshaller();
        Feed feedElement = (Feed) u.unmarshal((new StringReader(bundle.dataSets.get(0))));
        if (!feedElement.getName().contains(feedName)) {
            feedElement = (Feed) u.unmarshal(new StringReader(bundle.dataSets.get(1)));

        }
        return feedElement;
    }

    public static void writeFeedElement(Bundle bundle, Feed feedElement,
                                        String feedName) throws JAXBException {
        JAXBContext jc = JAXBContext.newInstance(Feed.class);
        java.io.StringWriter sw = new StringWriter();
        Marshaller marshaller = jc.createMarshaller();
        marshaller.marshal(feedElement, sw);
        //logger.info("feed to be written is: "+sw);
        writeFeedElement(bundle, sw.toString(), feedName);
    }

    public static void writeFeedElement(Bundle bundle, String feedString,
                                        String feedName) throws JAXBException {
        JAXBContext jc = JAXBContext.newInstance(Feed.class);
        Unmarshaller u = jc.createUnmarshaller();
        int index = 0;
        Feed dataElement = (Feed) u.unmarshal(new StringReader(bundle.dataSets.get(0)));
        if (!dataElement.getName().contains(feedName)) {
            index = 1;
        }
        bundle.getDataSets().set(index, feedString);
    }

    /**
     * Checks that API action succeed and the instance on which it has been performed on has
     * expected status.
     *
     * @param r  - - kind of response from API which should contain information about instance
     * @param ws - - expected status of instance
     */
    public static void validateSuccessOnlyStart(ProcessInstancesResult r,
                                                ProcessInstancesResult.WorkflowStatus ws) {
        Assert.assertEquals(r.getStatus(), APIResult.Status.SUCCEEDED);
        Assert.assertEquals(1, runningInstancesInResult(r, ws));
    }

    /**
     * Checks that actual number of instances with different statuses are equal to expected number
     * of instances with matching statuses.
     *
     * @param r                  - - kind of response from API which should contain information
     *                           about instances
     *                           All parameters below reflect number of expected instances of
     *                           some kind of status.
     * @param totalInstances
     * @param runningInstances
     * @param suspendedInstances
     * @param waitingInstances
     * @param killedInstances
     */
    public static void validateResponse(ProcessInstancesResult r, int totalInstances,
                                        int runningInstances,
                                        int suspendedInstances, int waitingInstances,
                                        int killedInstances) {

        int actualRunningInstances = 0;
        int actualSuspendedInstances = 0;
        int actualWaitingInstances = 0;
        int actualKilledInstances = 0;
        ProcessInstancesResult.ProcessInstance[] pArray = r.getInstances();
        logger.info("pArray: " + Arrays.toString(pArray));
        Assert.assertNotNull(pArray, "pArray should be not null");
        Assert.assertEquals(pArray.length, totalInstances, "Total Instances");
        for (int instanceIndex = 0; instanceIndex < pArray.length; instanceIndex++) {
            logger.info(
                "pArray[" + instanceIndex + "]: " + pArray[instanceIndex].getStatus() + " , " +
                    pArray[instanceIndex].getInstance());

            if (pArray[instanceIndex].getStatus()
                .equals(ProcessInstancesResult.WorkflowStatus.RUNNING))
                actualRunningInstances++;
            else if (pArray[instanceIndex].getStatus()
                .equals(ProcessInstancesResult.WorkflowStatus.SUSPENDED))
                actualSuspendedInstances++;
            else if (pArray[instanceIndex].getStatus()
                .equals(ProcessInstancesResult.WorkflowStatus.WAITING))
                actualWaitingInstances++;
            else if (pArray[instanceIndex].getStatus()
                .equals(ProcessInstancesResult.WorkflowStatus.KILLED))
                actualKilledInstances++;
        }

        Assert.assertEquals(actualRunningInstances, runningInstances, "Running Instances");
        Assert.assertEquals(actualSuspendedInstances, suspendedInstances, "Suspended Instances");
        Assert.assertEquals(actualWaitingInstances, waitingInstances, "Waiting Instances");
        Assert.assertEquals(actualKilledInstances, killedInstances, "Killed Instances");
    }

    /**
     * Checks that expected number of failed instances matches actual number of failed ones.
     *
     * @param r         - - kind of response from API which should contain information about
     *                  instances
     * @param failCount - number of instances which should be failed.
     */
    public static void validateFailedInstances(ProcessInstancesResult r, int failCount) {
        AssertUtil.assertSucceeded(r);
        int counter = 0;
        for (ProcessInstancesResult.ProcessInstance processInstance : r.getInstances()) {
            if (processInstance.getStatus() == ProcessInstancesResult.WorkflowStatus.FAILED)
                counter++;
        }
        Assert.assertEquals(counter, failCount, "Actual number of failed instances does not " +
            "match expected number of failed instances.");
    }

    public static List<String> getWorkflows(PrismHelper prismHelper, String processName,
                                            WorkflowJob.Status... ws) throws OozieClientException {

        String bundleID = OozieUtil.getBundles(prismHelper.getFeedHelper().getOozieClient(),
            processName, ENTITY_TYPE.PROCESS).get(0);
        OozieClient oozieClient = prismHelper.getClusterHelper().getOozieClient();

        List<String> workflows = OozieUtil.getCoordinatorJobs(prismHelper, bundleID);

        List<String> toBeReturned = new ArrayList<String>();
        for (String jobID : workflows) {
            WorkflowJob wfJob = oozieClient.getJobInfo(jobID);
            logger.info("wa.getExternalId(): " + wfJob.getId() + " wa" +
                ".getExternalStatus" +
                "():  " +
                wfJob.getStartTime());
            logger.info("wf id: " + jobID + "  wf status: " + wfJob.getStatus());
            if (ws.length == 0)
                toBeReturned.add(jobID);
            else {
                for (WorkflowJob.Status status : ws) {
                    if (wfJob.getStatus().name().equals(status.name()))
                        toBeReturned.add(jobID);
                }
            }
        }
        return toBeReturned;
    }


    public static boolean isWorkflowRunning(OozieClient OC, String workflowID) throws
        OozieClientException {
        String status = OC.getJobInfo(workflowID).getStatus().toString();
        return status.equals("RUNNING");
    }

    public static void areWorkflowsRunning(OozieClient OC, List<String> wfIDs,
                                           int totalWorkflows,
                                           int runningWorkflows, int killedWorkflows,
                                           int succeededWorkflows) throws OozieClientException {

        List<WorkflowJob> wfJobs = new ArrayList<WorkflowJob>();
        for (String wdID : wfIDs)
            wfJobs.add(OC.getJobInfo(wdID));
        if (totalWorkflows != -1)
            Assert.assertEquals(wfJobs.size(), totalWorkflows);
        int actualRunningWorkflows = 0;
        int actualKilledWorkflows = 0;
        int actualSucceededWorkflows = 0;
        logger.info("wfJobs: " + wfJobs);
        for (int instanceIndex = 0; instanceIndex < wfJobs.size(); instanceIndex++) {
            logger.info("was.get(" + instanceIndex + ").getStatus(): " +
                wfJobs.get(instanceIndex).getStatus());

            if (wfJobs.get(instanceIndex).getStatus().toString().equals("RUNNING"))
                actualRunningWorkflows++;
            else if (wfJobs.get(instanceIndex).getStatus().toString().equals("KILLED"))
                actualKilledWorkflows++;
            else if (wfJobs.get(instanceIndex).getStatus().toString().equals("SUCCEEDED"))
                actualSucceededWorkflows++;
        }
        if (runningWorkflows != -1)
            Assert.assertEquals(actualRunningWorkflows, runningWorkflows);
        if (killedWorkflows != -1)
            Assert.assertEquals(actualKilledWorkflows, killedWorkflows);
        if (succeededWorkflows != -1)
            Assert.assertEquals(actualSucceededWorkflows, succeededWorkflows);

    }

    public static List<CoordinatorAction> getProcessInstanceList(ColoHelper coloHelper,
                                                                 String processName,
                                                                 ENTITY_TYPE entityType)
        throws OozieClientException {

        OozieClient oozieClient = coloHelper.getProcessHelper().getOozieClient();
        String coordId = getLatestCoordinatorID(coloHelper, processName, entityType);
        //String coordId = getDefaultCoordinatorFromProcessName(processName);
        logger.info("default coordID: " + coordId);
        return oozieClient.getCoordJobInfo(coordId).getActions();
    }

    public static String getLatestCoordinatorID(ColoHelper coloHelper, String processName,
                                                ENTITY_TYPE entityType)
        throws OozieClientException {
        return getDefaultCoordIDFromBundle(coloHelper,
            getLatestBundleID(coloHelper, processName, entityType));
    }

    public static String getDefaultCoordIDFromBundle(ColoHelper coloHelper, String bundleId)
        throws OozieClientException {

        OozieClient oozieClient = coloHelper.getProcessHelper().getOozieClient();
        OozieUtil.waitForCoordinatorJobCreation(oozieClient, bundleId);
        BundleJob bundleInfo = oozieClient.getBundleJobInfo(bundleId);
        List<CoordinatorJob> coords = bundleInfo.getCoordinators();
        int min = 100000;
        String minString = "";
        for (CoordinatorJob coord : coords) {
            String strID = coord.getId();
            if (min > Integer.parseInt(strID.substring(0, strID.indexOf("-")))) {
                min = Integer.parseInt(strID.substring(0, strID.indexOf("-")));
                minString = coord.getId();
            }
        }

        logger.info("function getDefaultCoordIDFromBundle: minString: " + minString);
        return minString;

    }


    public static int getInstanceCountWithStatus(ColoHelper coloHelper, String processName,
                                                 org.apache.oozie.client.CoordinatorAction.Status
                                                     status,
                                                 ENTITY_TYPE entityType)
        throws OozieClientException {
        List<CoordinatorAction> list = getProcessInstanceList(coloHelper, processName, entityType);
        int instanceCount = 0;
        for (CoordinatorAction aList : list) {

            if (aList.getStatus().equals(status))
                instanceCount++;
        }
        return instanceCount;

    }


    public static Status getDefaultCoordinatorStatus(ColoHelper colohelper, String processName,
                                                     int bundleNumber) throws OozieClientException {

        OozieClient oozieClient = colohelper.getProcessHelper().getOozieClient();
        String coordId =
            getDefaultCoordinatorFromProcessName(colohelper, processName, bundleNumber);
        return oozieClient.getCoordJobInfo(coordId).getStatus();
    }


    public static String getDefaultCoordinatorFromProcessName(
        ColoHelper coloHelper, String processName, int bundleNumber) throws OozieClientException {
        //String bundleId = Util.getCoordID(Util.getOozieJobStatus(processName,"NONE").get(0));
        String bundleID =
            getSequenceBundleID(coloHelper, processName, ENTITY_TYPE.PROCESS, bundleNumber);
        return getDefaultCoordIDFromBundle(coloHelper, bundleID);
    }

    public static List<CoordinatorJob> getBundleCoordinators(String bundleID,
                                                             IEntityManagerHelper helper)
        throws OozieClientException {
        OozieClient localOozieClient = helper.getOozieClient();
        BundleJob bundleInfo = localOozieClient.getBundleJobInfo(bundleID);
        return bundleInfo.getCoordinators();
    }

    public static String getLatestBundleID(ColoHelper coloHelper,
                                           String entityName, ENTITY_TYPE entityType)
        throws OozieClientException {

        List<String> bundleIds = OozieUtil.getBundles(coloHelper.getFeedHelper().getOozieClient(),
            entityName, entityType);

        String max = "0";
        int maxID = -1;
        for (String strID : bundleIds) {
            if (maxID < Integer.parseInt(strID.substring(0, strID.indexOf("-")))) {
                maxID = Integer.parseInt(strID.substring(0, strID.indexOf("-")));
                max = strID;
            }
        }
        return max;
    }

    /**
     * Retrieves ID of bundle related to some process/feed using its ordinal number.
     *
     * @param entityName   - name of entity bundle is related to
     * @param entityType   - feed or process
     * @param bundleNumber - ordinal number of bundle
     * @return bundle ID
     * @throws OozieClientException
     */
    public static String getSequenceBundleID(PrismHelper prismHelper, String entityName,
                                             ENTITY_TYPE entityType, int bundleNumber)
        throws OozieClientException {

        //sequence start from 0
        List<String> bundleIds = OozieUtil.getBundles(prismHelper.getFeedHelper().getOozieClient(),
            entityName, entityType);
        Map<Integer, String> bundleMap = new TreeMap<Integer, String>();
        String bundleID;
        for (String strID : bundleIds) {
            logger.info("getSequenceBundleID: " + strID);
            int key = Integer.parseInt(strID.substring(0, strID.indexOf("-")));
            bundleMap.put(key, strID);
        }

        for (Map.Entry<Integer, String> entry : bundleMap.entrySet()) {
            logger.info("Key = " + entry.getKey() + ", Value = " + entry.getValue());
        }

        int i = 0;
        for (Integer key : bundleMap.keySet()) {
            bundleID = bundleMap.get(key);
            if (i == bundleNumber)
                return bundleID;
            i++;
        }
        return null;
    }

    /**
     * Retrieves status of one instance.
     *
     * @param coloHelper     - server from which instance status will be retrieved.
     * @param processName    - name of process which mentioned instance belongs to.
     * @param bundleNumber   - ordinal number of one of the bundle which are related to that
     *                       process.
     * @param instanceNumber - ordinal number of instance which state will be returned.
     * @return - state of mentioned instance.
     * @throws OozieClientException
     */
    public static CoordinatorAction.Status getInstanceStatus(ColoHelper coloHelper,
                                                             String processName,
                                                             int bundleNumber, int
        instanceNumber) throws OozieClientException {
        String bundleID = InstanceUtil
            .getSequenceBundleID(coloHelper, processName, ENTITY_TYPE.PROCESS, bundleNumber);
        if (StringUtils.isEmpty(bundleID)) {
            return null;
        }
        String coordID = InstanceUtil.getDefaultCoordIDFromBundle(coloHelper, bundleID);
        if (StringUtils.isEmpty(coordID)) {
            return null;
        }
        OozieClient oozieClient = coloHelper.getProcessHelper().getOozieClient();
        CoordinatorJob coordInfo = oozieClient.getCoordJobInfo(coordID);
        if (coordInfo == null) {
            return null;
        }
        logger.info("coordInfo = " + coordInfo);
        List<CoordinatorAction> actions = coordInfo.getActions();
        if (actions.size() == 0) {
            return null;
        }
        logger.info("actions = " + actions);
        return actions.get(instanceNumber).getStatus();
    }

    public static void putDataInFolders(ColoHelper colo,
                                        final List<String> inputFoldersForInstance,
                                        String type) throws IOException {

        for (String anInputFoldersForInstance : inputFoldersForInstance)
            putDataInFolder(colo.getClusterHelper().getHadoopFS(),
                anInputFoldersForInstance, type);

    }


    public static void putDataInFolder(FileSystem fs, final String remoteLocation, String type)
        throws IOException {
        String inputPath = OSUtil.NORMAL_INPUT;
        if ((null != type) && type.equals("late")) {
            inputPath = OSUtil.OOZIE_EXAMPLE_INPUT_DATA + "lateData";
        }
        else if ((null !=type) && type.equals("oneFile")) {
             inputPath = OSUtil.SINGLE_FILE  ;
        }

        File[] files = new File(inputPath).listFiles();
        assert files != null;

        Path remotePath = new Path(remoteLocation);
        if (!fs.exists(remotePath))
            fs.mkdirs(remotePath);

        List<Path> localPaths = new ArrayList<Path>();
        for (final File file : files) {
            if (!file.isDirectory()) {
                localPaths.add(new Path(file.getAbsolutePath()));
            }
        }
        logger.info(
            "putting: " + Arrays.toString(files) + " to hdfs " + fs.getUri() + remoteLocation);
        fs.copyFromLocalFile(false, false, localPaths.toArray(new Path[localPaths.size()]),
            new Path(remoteLocation));
    }

    public static void createHDFSFolders(PrismHelper helper, List<String> folderList)
        throws IOException {
        logger.info("creating folders.....");


        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://" + helper.getFeedHelper().getHadoopURL());

        final FileSystem fs = FileSystem.get(conf);

        for (final String folder : folderList) {
            fs.mkdirs(new Path(folder));
        }
        logger.info("created folders.....");

    }


    public static void putFileInFolders(ColoHelper colo, List<String> folderList,
                                        final String... fileName) throws IOException {
        final FileSystem fs = colo.getClusterHelper().getHadoopFS();

        for (final String folder : folderList) {
            for (String aFileName : fileName) {
                logger.info("copying  " + aFileName + " to " + folder);
                if (aFileName.equals("_SUCCESS"))
                    fs.mkdirs(new Path(folder + "/_SUCCESS"));
                else
                    fs.copyFromLocalFile(new Path(aFileName), new Path(folder));
            }
        }
    }

    public static org.apache.falcon.entity.v0.cluster.Cluster getClusterElement(
        Bundle bundle)
        throws JAXBException {
        JAXBContext jc = JAXBContext
            .newInstance(org.apache.falcon.entity.v0.cluster.Cluster.class);
        Unmarshaller u = jc.createUnmarshaller();

        return (org.apache.falcon.entity.v0.cluster.Cluster) u
            .unmarshal((new StringReader(bundle.getClusters().get(0))));
    }

    public static void writeClusterElement(Bundle bundle,
                                           org.apache.falcon.entity.v0.cluster
                                               .Cluster c)
        throws JAXBException {
        JAXBContext jc = JAXBContext
            .newInstance(org.apache.falcon.entity.v0.cluster.Cluster.class);
        java.io.StringWriter sw = new StringWriter();
        Marshaller marshaller = jc.createMarshaller();
        marshaller.marshal(c, sw);
        bundle.setClusterData(sw.toString());
    }

    /**
     * Sets one more cluster to feed.
     *
     * @param feed feed which is to be modified
     * @param feedValidity validity of the feed on the cluster
     * @param feedRetention set retention of the feed on the cluster
     * @param clusterName cluster name, if null would erase all the cluster details from the feed
     * @param clusterType cluster type
     * @param partition - partition where data is available for feed
     * @param locations - location where data is picked
     * @return - string representation of the modified feed
     * @throws JAXBException
     */
    public static String setFeedCluster(String feed, Validity feedValidity, Retention feedRetention,
                                        String clusterName,
                                        ClusterType clusterType, String partition,
                                        String... locations) throws JAXBException {
        return setFeedClusterWithTable(feed, feedValidity, feedRetention, clusterName, clusterType,
            partition, null, locations);
    }

    public static String setFeedClusterWithTable(String feed, Validity feedValidity,
                                                 Retention feedRetention, String clusterName,
                                                 ClusterType clusterType, String partition,
                                                 String tableUri, String... locations)
        throws JAXBException {
        Feed f = getFeedElement(feed);
        if (clusterName == null) {
            f.getClusters().getClusters().clear();
        } else {
            Cluster feedCluster = createFeedCluster(feedValidity, feedRetention, clusterName,
                clusterType, partition, tableUri, locations);
            f.getClusters().getClusters().add(feedCluster);
        }
        return feedElementToString(f);
    }

    private static CatalogTable getCatalogTable(String tableUri) {
        CatalogTable catalogTable = new CatalogTable();
        catalogTable.setUri(tableUri);
        return catalogTable;
    }

    private static Cluster createFeedCluster(
        Validity feedValidity, Retention feedRetention, String clusterName, ClusterType clusterType,
        String partition, String tableUri, String[] locations) {

        Cluster cluster = new Cluster();
        cluster.setName(clusterName);
        cluster.setRetention(feedRetention);
        if (clusterType != null)
            cluster.setType(clusterType);
        cluster.setValidity(feedValidity);
        if (partition != null)
            cluster.setPartition(partition);

        // if table uri is not empty or null then set it.
        if (StringUtils.isNotEmpty(tableUri)) {
            cluster.setTable(getCatalogTable(tableUri));
        }


        Locations feedLocations = new Locations();
        if (ArrayUtils.isNotEmpty(locations)) {
            for (int i = 0; i < locations.length; i++) {
                Location oneLocation = new Location();
                oneLocation.setPath(locations[i]);
                if (i == 0)
                    oneLocation.setType(LocationType.DATA);
                else if (i == 1)
                    oneLocation.setType(LocationType.STATS);
                else if (i == 2)
                    oneLocation.setType(LocationType.META);
                else if (i == 3)
                    oneLocation.setType(LocationType.TMP);
                else
                    Assert.fail("unexpected value of locations: " + locations);

                feedLocations.getLocations().add(oneLocation);
            }

            cluster.setLocations(feedLocations);
        }
        return cluster;
    }

    /**
     * Converts string feed representation to XML form
     */
    public static Feed getFeedElement(String feed) throws JAXBException {
        JAXBContext jc = JAXBContext.newInstance(Feed.class);
        Unmarshaller u = jc.createUnmarshaller();
        return (Feed) u.unmarshal((new StringReader(feed)));
    }

    /**
     * Converts XML feed representation to string form
     */
    public static String feedElementToString(Feed feedElement) throws JAXBException {
        JAXBContext jc = JAXBContext.newInstance(Feed.class);
        java.io.StringWriter sw = new StringWriter();
        Marshaller marshaller = jc.createMarshaller();
        marshaller.marshal(feedElement, sw);
        return sw.toString();
    }

    /**
     * Retrieves replication coordinatorID from bundle of coordinators
     */
    public static List<String> getReplicationCoordID(String bundlID,
                                                     IEntityManagerHelper helper)
        throws OozieClientException {
        List<CoordinatorJob> coords = InstanceUtil.getBundleCoordinators(bundlID, helper);
        List<String> ReplicationCoordID = new ArrayList<String>();
        for (CoordinatorJob coord : coords) {
            if (coord.getAppName().contains("FEED_REPLICATION"))
                ReplicationCoordID.add(coord.getId());
        }

        return ReplicationCoordID;
    }

    /**
     * Forms and sends process instance request based on url of action to be performed and it's
     * parameters
     *
     * @param colo - servers on which action should be performed
     * @param user - whose credentials will be used for this action
     * @return
     */
    public static APIResult createAndsendRequestProcessInstance(
        String url, String params, String colo, String user)
        throws IOException, URISyntaxException, AuthenticationException {

        if (params != null && !colo.equals("")) {
            url = url + params + "&" + colo.substring(1);
        } else if (params != null) {
            url = url + params;
        } else
            url = url + colo;

        return InstanceUtil.sendRequestProcessInstance(url, user);

    }


    /**
     * Retrieves prefix (main sub-folders) of feed data path.
     */
    public static String getFeedPrefix(String feed) throws JAXBException {
        Feed feedElement = InstanceUtil.getFeedElement(feed);
        String p = feedElement.getLocations().getLocations().get(0).getPath();
        p = p.substring(0, p.indexOf("$"));
        return p;
    }

    /**
     * Sets one more cluster to process definition.
     *
     * @param process     - process definition string representation
     * @param clusterName - name of cluster
     * @param validity    - cluster validity
     * @return - string representation of modified process
     * @throws JAXBException
     */
    public static String setProcessCluster(String process,
                                           String clusterName,
                                           org.apache.falcon.entity.v0.process
                                               .Validity validity) throws JAXBException {


        org.apache.falcon.entity.v0.process.Cluster c =
            new org.apache.falcon.entity.v0.process.Cluster();

        c.setName(clusterName);
        c.setValidity(validity);

        Process p = InstanceUtil.getProcessElement(process);


        if (clusterName == null)
            p.getClusters().getClusters().set(0, null);
        else {
            p.getClusters().getClusters().add(c);
        }
        return processToString(p);

    }

    /**
     * Represents process XML definition as string
     *
     * @param p - process XML definition which is to be converted
     * @throws JAXBException
     */
    public static String processToString(Process p) throws JAXBException {
        JAXBContext jc = JAXBContext.newInstance(Process.class);
        java.io.StringWriter sw = new StringWriter();
        Marshaller marshaller = jc.createMarshaller();
        marshaller.marshal(p, sw);
        return sw.toString();
    }

    /**
     * Converts process string representation to XML
     */
    public static Process getProcessElement(String process) throws JAXBException {
        JAXBContext jc = JAXBContext.newInstance(Process.class);
        Unmarshaller u = jc.createUnmarshaller();

        return (Process) u.unmarshal((new StringReader(process)));
    }

    /**
     * Adds one input into process.
     *
     * @param process - where input should be inserted
     * @param feed    - feed which will be used as input feed
     * @return - string representation of process definition
     * @throws JAXBException
     */
    public static String addProcessInputFeed(String process, String feed,
                                             String feedName) throws JAXBException {
        Process processElement = InstanceUtil.getProcessElement(process);
        Input in1 = processElement.getInputs().getInputs().get(0);
        Input in2 = new Input();
        in2.setEnd(in1.getEnd());
        in2.setFeed(feed);
        in2.setName(feedName);
        in2.setPartition(in1.getPartition());
        in2.setStart(in1.getStart());
        processElement.getInputs().getInputs().add(in2);
        return processToString(processElement);
    }

    public static org.apache.oozie.client.WorkflowJob.Status getInstanceStatusFromCoord(
        ColoHelper ua1,
        String coordID,
        int instanceNumber) throws OozieClientException {
        OozieClient oozieClient = ua1.getProcessHelper().getOozieClient();
        CoordinatorJob coordInfo = oozieClient.getCoordJobInfo(coordID);
        String jobId = coordInfo.getActions().get(instanceNumber).getExternalId();
        logger.info("jobId = " + jobId);
        if (jobId == null)
            return null;
        WorkflowJob actionInfo = oozieClient.getJobInfo(jobId);
        return actionInfo.getStatus();
    }

    public static List<String> getInputFoldersForInstanceForReplication(
        ColoHelper coloHelper, String coordID, int instanceNumber) throws OozieClientException {
        OozieClient oozieClient = coloHelper.getProcessHelper().getOozieClient();
        CoordinatorAction x = oozieClient.getCoordActionInfo(coordID + "@" + instanceNumber);
        String jobId = x.getExternalId();
        WorkflowJob wfJob = oozieClient.getJobInfo(jobId);
        return InstanceUtil.getReplicationFolderFromInstanceRunConf(wfJob.getConf());
    }

    public static List<String> getReplicationFolderFromInstanceRunConf(
        String runConf) {
        String conf;
        conf = runConf.substring(runConf.indexOf("falconInPaths</name>") + 20);
        conf = conf.substring(conf.indexOf("<value>") + 7);
        conf = conf.substring(0, conf.indexOf("</value>"));
        return new ArrayList<String>(Arrays.asList(conf.split(",")));
    }

    public static int getInstanceRunIdFromCoord(ColoHelper colo,
                                                String coordID, int instanceNumber)
        throws OozieClientException {
        OozieClient oozieClient = colo.getProcessHelper().getOozieClient();
        CoordinatorJob coordInfo = oozieClient.getCoordJobInfo(coordID);

        WorkflowJob actionInfo =
            oozieClient.getJobInfo(coordInfo.getActions().get(instanceNumber).getExternalId());
        return actionInfo.getRun();
    }

    public static void putLateDataInFolders(ColoHelper helper,
                                            List<String> inputFolderList,
                                            int lateDataFolderNumber) throws IOException {

        for (String anInputFolderList : inputFolderList)
            putLateDataInFolder(helper, anInputFolderList, lateDataFolderNumber);
    }

    public static void putLateDataInFolder(ColoHelper helper, final String remoteLocation,
                                           int lateDataFolderNumber)
        throws IOException {

        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://" + helper.getFeedHelper().getHadoopURL());


        final FileSystem fs = FileSystem.get(conf);

        File[] files = new File(OSUtil.NORMAL_INPUT).listFiles();
        if (lateDataFolderNumber == 2) {
            files = new File(OSUtil.OOZIE_EXAMPLE_INPUT_DATA + "2ndLateData").listFiles();
        }

        assert files != null;
        for (final File file : files) {
            if (!file.isDirectory()) {
                fs.copyFromLocalFile(new Path(file.getAbsolutePath()),
                    new Path(remoteLocation));
            }
        }
    }

    public static String setFeedFilePath(String feed, String path) throws JAXBException {
        Feed feedElement = InstanceUtil.getFeedElement(feed);
        feedElement.getLocations().getLocations().get(0).setPath(path);
        return InstanceUtil.feedElementToString(feedElement);

    }

    public static int checkIfFeedCoordExist(IEntityManagerHelper helper,
                                            String feedName, String coordType)
        throws OozieClientException, InterruptedException {
        logger.info("feedName: " + feedName);
        int numberOfCoord = 0;

        if (OozieUtil.getBundles(helper.getOozieClient(), feedName, ENTITY_TYPE.FEED).size() == 0)
            return 0;
        List<String> bundleID =
            OozieUtil.getBundles(helper.getOozieClient(), feedName, ENTITY_TYPE.FEED);
        logger.info("bundleID: " + bundleID);

        for (String aBundleID : bundleID) {
            logger.info("aBundleID: " + aBundleID);
            OozieUtil.waitForCoordinatorJobCreation(helper.getOozieClient(), aBundleID);
            List<CoordinatorJob> coords =
                InstanceUtil.getBundleCoordinators(aBundleID, helper);
            logger.info("coords: " + coords);
            for (CoordinatorJob coord : coords) {
                if (coord.getAppName().contains(coordType))
                    numberOfCoord++;
            }
        }
        return numberOfCoord;
    }


    public static String setProcessFrequency(String process,
                                             Frequency frequency) throws JAXBException {
        Process p = InstanceUtil.getProcessElement(process);

        p.setFrequency(frequency);

        return InstanceUtil.processToString(p);
    }

    public static String setProcessName(String process, String newName) throws JAXBException {
        Process p = InstanceUtil.getProcessElement(process);

        p.setName(newName);

        return InstanceUtil.processToString(p);
    }


    public static String setProcessValidity(String process,
                                            String startTime, String endTime) throws JAXBException {

        Process processElement = InstanceUtil.getProcessElement(process);

        for (int i = 0; i < processElement.getClusters().getClusters().size(); i++) {
            processElement.getClusters().getClusters().get(i).getValidity().setStart(
                TimeUtil.oozieDateToDate(startTime).toDate());
            processElement.getClusters().getClusters().get(i).getValidity()
                .setEnd(TimeUtil.oozieDateToDate(endTime).toDate());

        }

        return InstanceUtil.processToString(processElement);
    }

    public static List<CoordinatorAction> getProcessInstanceListFromAllBundles(
        ColoHelper coloHelper, String processName, ENTITY_TYPE entityType)
        throws OozieClientException {

        OozieClient oozieClient = coloHelper.getProcessHelper().getOozieClient();

        List<CoordinatorAction> list = new ArrayList<CoordinatorAction>();

        logger.info("bundle size for process is " +
            OozieUtil.getBundles(coloHelper.getFeedHelper().getOozieClient(), processName,
                entityType).size());

        for (String bundleId : OozieUtil.getBundles(coloHelper.getFeedHelper().getOozieClient(),
            processName, entityType)) {
            BundleJob bundleInfo = oozieClient.getBundleJobInfo(bundleId);
            List<CoordinatorJob> coords = bundleInfo.getCoordinators();

            logger.info("number of coords in bundle " + bundleId + "=" + coords.size());

            for (CoordinatorJob coord : coords) {
                List<CoordinatorAction> actions =
                    oozieClient.getCoordJobInfo(coord.getId()).getActions();
                logger.info("number of actions in coordinator " + coord.getId() + " is " +
                    actions.size());
                list.addAll(actions);
            }
        }

        String coordId = getLatestCoordinatorID(coloHelper, processName, entityType);
        //String coordId = getDefaultCoordinatorFromProcessName(processName);
        logger.info("default coordID: " + coordId);

        return list;
    }

    public static String getOutputFolderForInstanceForReplication(ColoHelper coloHelper,
                                                                  String coordID,
                                                                  int instanceNumber)
        throws OozieClientException {
        OozieClient oozieClient = coloHelper.getProcessHelper().getOozieClient();
        CoordinatorJob coordInfo = oozieClient.getCoordJobInfo(coordID);

        return InstanceUtil.getReplicatedFolderFromInstanceRunConf(
            oozieClient.getJobInfo(coordInfo.getActions().get(instanceNumber).getExternalId())
                .getConf()
        );
    }

    private static String getReplicatedFolderFromInstanceRunConf(
        String runConf) {

        String inputPathExample =
            InstanceUtil.getReplicationFolderFromInstanceRunConf(runConf).get(0);
        String postFix = inputPathExample
            .substring(inputPathExample.length() - 7, inputPathExample.length());

        return getReplicatedFolderBaseFromInstanceRunConf(runConf) + postFix;
    }

    public static String getOutputFolderBaseForInstanceForReplication(
        ColoHelper coloHelper, String coordID, int instanceNumber) throws OozieClientException {
        OozieClient oozieClient = coloHelper.getProcessHelper().getOozieClient();
        CoordinatorJob coordInfo = oozieClient.getCoordJobInfo(coordID);

        return InstanceUtil.getReplicatedFolderBaseFromInstanceRunConf(
            oozieClient.getJobInfo(coordInfo.getActions().get(instanceNumber).getExternalId())
                .getConf());
    }

    private static String getReplicatedFolderBaseFromInstanceRunConf(String runConf) {
        String conf = runConf.substring(runConf.indexOf("distcpTargetPaths</name>") + 24);
        conf = conf.substring(conf.indexOf("<value>") + 7);
        conf = conf.substring(0, conf.indexOf("</value>"));
        return conf;
    }

    public static String ClusterElementToString(
        org.apache.falcon.entity.v0.cluster.Cluster c)
        throws JAXBException {
        JAXBContext jc = JAXBContext
            .newInstance(org.apache.falcon.entity.v0.cluster.Cluster.class);
        java.io.StringWriter sw = new StringWriter();
        Marshaller marshaller = jc.createMarshaller();
        marshaller.marshal(c, sw);
        return sw.toString();
    }

    public static org.apache.falcon.entity.v0.cluster.Cluster getClusterElement(
        String clusterData) throws JAXBException {
        JAXBContext jc = JAXBContext
            .newInstance(org.apache.falcon.entity.v0.cluster.Cluster.class);
        Unmarshaller u = jc.createUnmarshaller();

        return (org.apache.falcon.entity.v0.cluster.Cluster) u
            .unmarshal((new StringReader(clusterData)));
    }

    public static void waitTillInstanceReachState(OozieClient client, String entityName,
                                                  int numberOfInstance,
                                                  org.apache.oozie.client.CoordinatorAction
                                                      .Status expectedStatus,
                                                  int totalMinutesToWait, ENTITY_TYPE entityType)
        throws InterruptedException, OozieClientException {
        String filter;
        // get the bunlde ids
        if (entityType.equals(ENTITY_TYPE.FEED)) {
            filter = "name=FALCON_FEED_" + entityName;
        } else {
            filter = "name=FALCON_PROCESS_" + entityName;
        }
        List<BundleJob> bundleJobs = new ArrayList<BundleJob>();
        for (int retries = 0; retries < 20; ++retries) {
            bundleJobs = OozieUtil.getBundles(client, filter, 0, 10);
            if (bundleJobs.size() > 0) {
                break;
            }
            Thread.sleep(5000);
        }
        if (bundleJobs.size() == 0) {
            Assert.assertTrue(false, "Could not retrieve bundles");
        }
        List<String> bundleIds = OozieUtil.getBundleIds(bundleJobs);
        String bundleId = OozieUtil.getMaxId(bundleIds);
        logger.info(String.format("Using bundle %s", bundleId));
        final String coordId;
        final Status bundleStatus = client.getBundleJobInfo(bundleId).getStatus();
        Assert.assertTrue(bundleStatus == Status.RUNNING || bundleStatus == Status.PREP ||
            bundleStatus == Status.SUCCEEDED,
            String.format("Bundle job %s is should be prep/running but is %s", bundleId,
                bundleStatus));
        OozieUtil.waitForCoordinatorJobCreation(client, bundleId);
        List<CoordinatorJob> coords = client.getBundleJobInfo(bundleId).getCoordinators();
        List<String> cIds = new ArrayList<String>();
        if (entityType.equals(ENTITY_TYPE.PROCESS)) {
            for (CoordinatorJob coord : coords) {
                cIds.add(coord.getId());
            }
            coordId = OozieUtil.getMinId(cIds);
        } else {
            for (CoordinatorJob coord : coords) {
                if (coord.getAppName().contains("FEED_REPLICATION")) {
                    cIds.add(coord.getId());
                }
            }
            coordId = cIds.get(0);
        }
        logger.info(String.format("Using coordinator id: %s", coordId));
        int maxTries = 50;
        int totalSleepTime = totalMinutesToWait * 60 * 1000;
        int sleepTime = totalSleepTime / maxTries;
        logger.info(String.format("Sleep for %d seconds", sleepTime / 1000));
        for (int i = 0; i < maxTries; i++) {
            logger.info(String.format("Try %d of %d", (i + 1), maxTries));
            int instanceWithStatus = 0;
            CoordinatorJob coordinatorJob = client.getCoordJobInfo(coordId);
            final Status coordinatorStatus = coordinatorJob.getStatus();
            Assert.assertTrue(
                coordinatorStatus == Status.RUNNING || coordinatorStatus == Status.PREP ||
                    coordinatorStatus == Status.SUCCEEDED,
                String.format("Coordinator %s should be running/prep but is %s.", coordId,
                    coordinatorStatus));
            List<CoordinatorAction> coordinatorActions = coordinatorJob.getActions();
            for (CoordinatorAction coordinatorAction : coordinatorActions) {
                logger.info(String.format("Coordinator Action %s status is %s on oozie %s",
                    coordinatorAction.getId(), coordinatorAction.getStatus(), client.getOozieUrl()));
                if (expectedStatus == coordinatorAction.getStatus()) {
                    instanceWithStatus++;
                }
            }

            if (instanceWithStatus >= numberOfInstance)
                return;
            Thread.sleep(sleepTime);
        }
        Assert.assertTrue(false, "expected state of instance was never reached");
    }

    public static void waitForBundleToReachState(
        ColoHelper coloHelper,
        String processName,
        org.apache.oozie.client.Job.Status expectedStatus,
        int totalMinutesToWait) throws OozieClientException {

        int sleep = totalMinutesToWait * 60 / 20;

        for (int sleepCount = 0; sleepCount < sleep; sleepCount++) {

            String BundleID =
                InstanceUtil.getLatestBundleID(coloHelper, processName, ENTITY_TYPE.PROCESS);

            OozieClient oozieClient =
                coloHelper.getProcessHelper().getOozieClient();

            BundleJob j = oozieClient.getBundleJobInfo(BundleID);


            if (j.getStatus().equals(expectedStatus))
                break;

            try {
                Thread.sleep(20000);
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }
        }
    }

    public static String setFeedFrequency(String feed, Frequency f) throws JAXBException {
        Feed feedElement = InstanceUtil.getFeedElement(feed);
        feedElement.setFrequency(f);
        return InstanceUtil.feedElementToString(feedElement);
    }

    public static void waitTillInstancesAreCreated(ColoHelper coloHelper,
                                                   String entity,
                                                   int bundleSeqNo,
                                                   int totalMinutesToWait
    ) throws JAXBException, OozieClientException {
        int sleep = totalMinutesToWait * 60 / 5;
        String entityName = Util.readEntityName(entity);
        ENTITY_TYPE type = Util.getEntityType(entity);
        String bundleID = getSequenceBundleID(coloHelper, entityName, type,
            bundleSeqNo);
        String coordID = getDefaultCoordIDFromBundle(coloHelper, bundleID);

        for (int sleepCount = 0; sleepCount < sleep; sleepCount++) {
            CoordinatorJob coordInfo = coloHelper.getProcessHelper().getOozieClient()
                .getCoordJobInfo(coordID);

            if (coordInfo.getActions().size() > 0)
                break;
            logger.info("Coord " + coordInfo.getId() + " still doesn't have " +
                "instance created on oozie: " + coloHelper.getProcessHelper()
                .getOozieClient().getOozieUrl());
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }

        }

    }
}

