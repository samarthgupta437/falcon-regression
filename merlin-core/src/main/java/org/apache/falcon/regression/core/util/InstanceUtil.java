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
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.generated.dependencies.Frequency;
import org.apache.falcon.regression.core.generated.feed.ClusterType;
import org.apache.falcon.regression.core.generated.feed.Feed;
import org.apache.falcon.regression.core.generated.feed.LocationType;
import org.apache.falcon.regression.core.generated.feed.Retention;
import org.apache.falcon.regression.core.generated.process.Input;
import org.apache.falcon.regression.core.generated.process.Process;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.helpers.PrismHelper;
import org.apache.falcon.regression.core.interfaces.IEntityManagerHelper;
import org.apache.falcon.regression.core.response.APIResult;
import org.apache.falcon.regression.core.response.ProcessInstancesResult;
import org.apache.falcon.regression.core.supportClasses.ENTITY_TYPE;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job.Status;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.XOozieClient;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.Assert;
import org.testng.log4testng.Logger;

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
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class InstanceUtil {


    static XOozieClient oozieClient = null;
    static String hdfs_url = null;


    public InstanceUtil(String envFileName) throws Exception {
        oozieClient = new XOozieClient(Util.readPropertiesFile(envFileName, "oozie_url"));
        hdfs_url = "hdfs://" + Util.readPropertiesFile(envFileName, "hadoop_url");

    }

    static Logger logger = Logger.getLogger(InstanceUtil.class);

    public static ProcessInstancesResult sendRequestProcessInstance(String
                                                                            url) throws Exception {
        HttpRequestBase request;
        if (Thread.currentThread().getStackTrace()[3].getMethodName().contains("Suspend") ||
                Thread.currentThread().getStackTrace()[3].getMethodName().contains("Resume") ||
                Thread.currentThread().getStackTrace()[3].getMethodName().contains("Kill") ||
                Thread.currentThread().getStackTrace()[3].getMethodName().contains("Rerun")) {
            request = new HttpPost();

        } else
            request = new HttpGet();
        request.setHeader("Remote-User", System.getProperty("user.name"));
        return hitUrl(url, request);
    }

    public static ProcessInstancesResult sendRequestProcessInstance(String
                                                                            url,
                                                                    String user
    ) throws Exception {

        HttpRequestBase request;
        if (Thread.currentThread().getStackTrace()[3].getMethodName().contains("Suspend") ||
                Thread.currentThread().getStackTrace()[3].getMethodName().contains("Resume") ||
                Thread.currentThread().getStackTrace()[3].getMethodName().contains("Kill") ||
                Thread.currentThread().getStackTrace()[3].getMethodName().contains("Rerun")) {
            request = new HttpPost();

        } else
            request = new HttpGet();
        request.setHeader("Remote-User", user);
        return hitUrl(url, request);
    }

    public static ProcessInstancesResult hitUrl(String url, HttpRequestBase request)
    throws Exception {
        logger.info("hitting the url: " + url);

        request.setURI(new URI(url));
        HttpClient client = new DefaultHttpClient();

        HttpResponse response = client.execute(request);


        BufferedReader reader = new BufferedReader(
                new InputStreamReader(response.getEntity().getContent(), "UTF-8"));
        StringBuilder string_response = new StringBuilder();
        for (String line; (line = reader.readLine()) != null; ) {
            string_response.append(line).append("\n");
        }
        String jsonString = string_response.toString();
        logger.info(
                "The web service response status is " + response.getStatusLine().getStatusCode());
        logger.info("The web service response is: " + string_response.toString() + "\n");
        ProcessInstancesResult r = new ProcessInstancesResult();
        if (jsonString.contains("(PROCESS) not found")) {
            r.setStatusCode(777);
            return r;
        } else if (jsonString.contains("Parameter start is empty") ||
                jsonString.contains("Unparseable date:")) {
            r.setStatusCode(2);
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
        r = new GsonBuilder().setPrettyPrinting().create()
                .fromJson(jsonString, ProcessInstancesResult.class);

        Util.print("r.getMessage(): " + r.getMessage());
        Util.print("r.getStatusCode(): " + r.getStatusCode());
        Util.print("r.getStatus() " + r.getStatus());
        Util.print("r.getInstances()" + Arrays.toString(r.getInstances()));
        return r;
    }


    public static void validateSuccess(ProcessInstancesResult r, Bundle b,
                                       ProcessInstancesResult.WorkflowStatus ws)
    throws Exception {
        Assert.assertEquals(r.getStatus(), APIResult.Status.SUCCEEDED);
        Assert.assertEquals(b.getProcessConcurrency(), runningInstancesInResult(r, ws));
    }

    public static int runningInstancesInResult(ProcessInstancesResult r,
                                               ProcessInstancesResult.WorkflowStatus ws) {
        ProcessInstancesResult.ProcessInstance[] pArray = r.getInstances();
        int runningCount = 0;
        //Util.print("function runningInstancesInResult: Start");
        Util.print("pArray: " + Arrays.toString(pArray));
        for (int instanceIndex = 0; instanceIndex < pArray.length; instanceIndex++) {
            Util.print(
                    "pArray[" + instanceIndex + "]: " + pArray[instanceIndex].getStatus() + " , " +
                            pArray[instanceIndex].getInstance());

            if (pArray[instanceIndex].getStatus().equals(ws)) {
                runningCount++;
            }
        }
        return runningCount;
    }

    public static void validateSuccessWOInstances(ProcessInstancesResult r) {
        Assert.assertTrue(r.getMessage().contains("is successful"));
        Assert.assertEquals(r.getStatus(), APIResult.Status.SUCCEEDED);
        if (r.getInstances() != null)
            Assert.assertTrue(false);
    }

    public static void validateSuccessWithStatusCode(ProcessInstancesResult r,
                                                     int expectedErrorCode) {
        Assert.assertEquals(r.getStatusCode(), expectedErrorCode,
                "Parameter start is empty should have the response");
    }

    public static void writeProcessElement(Bundle bundle, Process processElement) throws Exception {
        JAXBContext jc = JAXBContext.newInstance(Process.class);
        java.io.StringWriter sw = new StringWriter();
        Marshaller marshaller = jc.createMarshaller();
        marshaller.marshal(processElement, sw);
        //logger.info("modified process is: " + sw);
        bundle.setProcessData(sw.toString());
    }

    public static Process getProcessElement(Bundle bundle) throws Exception {
        JAXBContext jc = JAXBContext.newInstance(Process.class);
        Unmarshaller u = jc.createUnmarshaller();
        return (Process) u.unmarshal((new StringReader(bundle.getProcessData())));

    }

    public static Feed getFeedElement(Bundle bundle, String feedName) throws Exception {
        JAXBContext jc = JAXBContext.newInstance(Feed.class);
        Unmarshaller u = jc.createUnmarshaller();
        Feed feedElement = (Feed) u.unmarshal((new StringReader(bundle.dataSets.get(0))));
        if (!feedElement.getName().contains(feedName)) {
            feedElement = (Feed) u.unmarshal(new StringReader(bundle.dataSets.get(1)));

        }
        return feedElement;
    }

    public static void writeFeedElement(Bundle bundle, Feed feedElement,
                                        String feedName) throws Exception {
        JAXBContext jc = JAXBContext.newInstance(Feed.class);
        Unmarshaller u = jc.createUnmarshaller();
        java.io.StringWriter sw = new StringWriter();
        Marshaller marshaller = jc.createMarshaller();
        marshaller.marshal(feedElement, sw);
        //logger.info("feed to be written is: "+sw);
        int index = 0;
        Feed dataElement = (Feed) u.unmarshal(new StringReader(bundle.dataSets.get(0)));
        if (!dataElement.getName().contains(feedName)) {
            index = 1;
        }
        bundle.getDataSets().set(index, sw.toString());
    }

    public static void validateSuccessOnlyStart(ProcessInstancesResult r,
                                                ProcessInstancesResult.WorkflowStatus ws)
    throws Exception {
        Assert.assertEquals(r.getStatus(), APIResult.Status.SUCCEEDED);
        Assert.assertEquals(1, runningInstancesInResult(r, ws));
    }

    public static void validateResponse(ProcessInstancesResult r, int totalInstances,
                                        int runningInstances,
                                        int suspendedInstances, int waitingInstances,
                                        int killedInstances) {

        int actualRunningInstances = 0;
        int actualSuspendedInstances = 0;
        int actualWaitingInstances = 0;
        int actualKilledInstances = 0;
        ProcessInstancesResult.ProcessInstance[] pArray = r.getInstances();
        Util.print("pArray: " + Arrays.toString(pArray));
        Assert.assertEquals(pArray.length, totalInstances);
        for (int instanceIndex = 0; instanceIndex < pArray.length; instanceIndex++) {
            Util.print(
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

        Assert.assertEquals(actualRunningInstances, runningInstances);
        Assert.assertEquals(actualSuspendedInstances, suspendedInstances);
        Assert.assertEquals(actualWaitingInstances, waitingInstances);
        Assert.assertEquals(actualKilledInstances, killedInstances);
    }

    public static ArrayList<String> getWorkflows(PrismHelper prismHelper, String processName,
                                                 WorkflowAction.Status ws) throws Exception {

        String bundleID =
                Util.getCoordID(Util.getOozieJobStatus(prismHelper, processName, "NONE").get(0));
        oozieClient = new XOozieClient(prismHelper.getClusterHelper().getOozieURL());

        List<String> workflows = Util.getCoordinatorJobs(prismHelper, bundleID);

        ArrayList<String> toBeReturned = new ArrayList<String>();
        for (String jobID : workflows) {
            CoordinatorAction wfJob = oozieClient.getCoordActionInfo(jobID);
            WorkflowAction wa = oozieClient.getWorkflowActionInfo(wfJob.getExternalId());
            Util.print("wa.getExternalId(): " + wa.getExternalId() + " wa.getExternalStatus():  " +
                    wa.getExternalStatus());
            Util.print("wf id: " + jobID + "  wf status: " + wa.getStatus());
            //org.apache.oozie.client.WorkflowAction.Status.
            if (wa.getStatus().equals(ws))
                toBeReturned.add(jobID);
        }
        return toBeReturned;
    }


    public static boolean isWorkflowRunning(String workflowID) throws Exception {
        CoordinatorAction wfJob = oozieClient.getCoordActionInfo(workflowID);
        WorkflowAction wa = oozieClient.getWorkflowActionInfo(wfJob.getExternalId());
        return wa.getStatus().toString().equals("RUNNING");
    }

    public static void areWorkflowsRunning(PrismHelper prismHelper, String ProcessName,
                                           int totalWorkflows,
                                           int runningWorkflows, int killedWorkflows,
                                           int succeededWorkflows)
    throws Exception {

        ArrayList<WorkflowAction> was = getWorkflowActions(prismHelper, ProcessName);
        if (totalWorkflows != -1)
            Assert.assertEquals(was.size(), totalWorkflows);
        int actualRunningWorkflows = 0;
        int actualKilledWorkflows = 0;
        int actualSucceededWorkflows = 0;
        Util.print("was: " + was);
        for (int instanceIndex = 0; instanceIndex < was.size(); instanceIndex++) {
            Util.print("was.get(" + instanceIndex + ").getStatus(): " +
                    was.get(instanceIndex).getStatus() + " , " +
                    was.get(instanceIndex).getName());

            if (was.get(instanceIndex).getStatus().toString().equals("RUNNING"))
                actualRunningWorkflows++;
            else if (was.get(instanceIndex).getStatus().toString().equals("KILLED"))
                actualKilledWorkflows++;
            else if (was.get(instanceIndex).getStatus().toString().equals("SUCCEEDED"))
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
    throws Exception {

        XOozieClient oozieClient = new XOozieClient(coloHelper.getProcessHelper().getOozieURL());
        String coordId = getLatestCoordinatorID(coloHelper, processName, entityType);
        //String coordId = getDefaultCoordinatorFromProcessName(processName);
        Util.print("default coordID: " + coordId);
        return oozieClient.getCoordJobInfo(coordId).getActions();
    }

    public static String getLatestCoordinatorID(ColoHelper coloHelper, String processName,
                                                ENTITY_TYPE entityType)
    throws Exception {
        return getDefaultCoordIDFromBundle(coloHelper,
                getLatestBundleID(coloHelper, processName, entityType));
    }

    public static String getDefaultCoordIDFromBundle(ColoHelper coloHelper, String bundleId)
    throws OozieClientException {

        XOozieClient oozieClient = new XOozieClient(coloHelper.getProcessHelper().getOozieURL());
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

        Util.print("function getDefaultCoordIDFromBundle: minString: " + minString);
        return minString;

    }


    public static ArrayList<WorkflowAction> getWorkflowActions(PrismHelper prismHelper,
                                                               String processName)
    throws Exception {
        XOozieClient oozieClient = new XOozieClient(prismHelper.getProcessHelper().getOozieURL());

        String bundleID =
                Util.getCoordID(Util.getOozieJobStatus(prismHelper, processName, "NONE").get(0));
        ArrayList<WorkflowAction> was = new ArrayList<WorkflowAction>();
        List<String> workflows = Util.getCoordinatorJobs(prismHelper, bundleID);

        for (String jobID : workflows) {

            was.add(oozieClient
                    .getWorkflowActionInfo(oozieClient.getCoordActionInfo(jobID).getExternalId()));
        }
        return was;
    }

    public static int getInstanceCountWithStatus(ColoHelper coloHelper, String processName,
                                                 org.apache.oozie.client.CoordinatorAction.Status
                                                         status,
                                                 ENTITY_TYPE entityType)
    throws Exception {
        List<CoordinatorAction> list = getProcessInstanceList(coloHelper, processName, entityType);
        int instanceCount = 0;
        for (CoordinatorAction aList : list) {

            if (aList.getStatus().equals(status))
                instanceCount++;
        }
        return instanceCount;

    }


    public static String getTimeWrtSystemTime(int minutes) throws Exception {

        DateTime jodaTime = new DateTime(DateTimeZone.UTC);
        if (minutes > 0)
            jodaTime = jodaTime.plusMinutes(minutes);
        else
            jodaTime = jodaTime.minusMinutes(-1 * minutes);

        DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy'-'MM'-'dd'T'HH':'mm'Z'");
        return fmt.print(jodaTime);
    }

    public static String addMinsToTime(String time, int minutes) throws ParseException {

        DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy'-'MM'-'dd'T'HH':'mm'Z'");
        DateTime jodaTime = fmt.parseDateTime(time);

        if (minutes > 0)
            jodaTime = jodaTime.plusMinutes(minutes);
        else
            jodaTime = jodaTime.minusMinutes(-1 * minutes);

        return fmt.print(jodaTime);
    }


    public static Status getDefaultCoordinatorStatus(ColoHelper colohelper, String processName,
                                                     int bundleNumber)
    throws Exception {

        XOozieClient oozieClient = new XOozieClient(colohelper.getProcessHelper().getOozieURL());
        String coordId =
                getDefaultCoordinatorFromProcessName(colohelper, processName, bundleNumber);
        return oozieClient.getCoordJobInfo(coordId).getStatus();
    }


    public static String getDefaultCoordinatorFromProcessName(
            ColoHelper coloHelper, String processName, int bundleNumber) throws Exception {
        //String bundleId = Util.getCoordID(Util.getOozieJobStatus(processName,"NONE").get(0));
        String bundleID =
                getSequenceBundleID(coloHelper, processName, ENTITY_TYPE.PROCESS, bundleNumber);
        return getDefaultCoordIDFromBundle(coloHelper, bundleID);
    }

    public static List<CoordinatorJob> getBundleCoordinators(String bundleID,
                                                             IEntityManagerHelper helper)
    throws Exception {
        XOozieClient localOozieClient = new XOozieClient(helper.getOozieURL());
        BundleJob bundleInfo = localOozieClient.getBundleJobInfo(bundleID);
        return bundleInfo.getCoordinators();
    }

    public static String getLatestBundleID(ColoHelper coloHelper, String processName,
                                           ENTITY_TYPE entityType)
    throws Exception {
        List<String> bundleIds = Util.getBundles(coloHelper.getFeedHelper().getOozieClient(), processName, entityType);

        String max = "";
        int maxID = -1;
        for (String strID : bundleIds) {
            if (maxID < Integer.parseInt(strID.substring(0, strID.indexOf("-")))) {
                maxID = Integer.parseInt(strID.substring(0, strID.indexOf("-")));
                max = strID;
            }
        }
        return max;
    }

    @Deprecated
    public static String getLatestBundleID(ColoHelper coloHelper, String processName,
                                           String entityType)
    throws Exception {
        List<String> bundleIds = Util.getBundles(coloHelper, processName, entityType);

        String max = "";
        int maxID = -1;
        for (String strID : bundleIds) {
            if (maxID < Integer.parseInt(strID.substring(0, strID.indexOf("-")))) {
                maxID = Integer.parseInt(strID.substring(0, strID.indexOf("-")));
                max = strID;
            }
        }
        return max;
    }

    @Deprecated
    public static String getLatestBundleID(String processName, String entityType,
                                           IEntityManagerHelper helper)
    throws Exception {
        List<String> bundleIds = Util.getBundles(processName, entityType, helper);

        String max = "";
        int maxID = -1;
        for (String strID : bundleIds) {
            if (maxID < Integer.parseInt(strID.substring(0, strID.indexOf("-")))) {
                maxID = Integer.parseInt(strID.substring(0, strID.indexOf("-")));
                max = strID;
            }
        }
        return max;
    }

    public static String getSequenceBundleID(PrismHelper prismHelper, String entityName,
                                             ENTITY_TYPE entityType, int bundleNumber)
    throws Exception {

        List<String> bundleIds = Util.getBundles(prismHelper.getFeedHelper().getOozieClient(), entityName, entityType);
        Map<Integer, String> bundleMap = new TreeMap<Integer, String>();
        String bundleID;
        for (String strID : bundleIds) {
            Util.print("getSequenceBundleID: " + strID);
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

    public static String dateToOozieDate(Date dt) throws ParseException {

        DateTime jodaTime = new DateTime(dt, DateTimeZone.UTC);
        Util.print("jadaSystemTime: " + jodaTime);
        DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy'-'MM'-'dd'T'HH':'mm'Z'");
        return fmt.print(jodaTime);
    }


    public static CoordinatorAction.Status getInstanceStatus(ColoHelper coloHelper,
                                                             String processName,
                                                             int bundleNumber, int instanceNumber)
    throws Exception {
        String bundleID = InstanceUtil
                .getSequenceBundleID(coloHelper, processName, ENTITY_TYPE.PROCESS, bundleNumber);
        String coordID = InstanceUtil.getDefaultCoordIDFromBundle(coloHelper, bundleID);
        XOozieClient oozieClient = new XOozieClient(coloHelper.getProcessHelper().getOozieURL());
        CoordinatorJob coordInfo = oozieClient.getCoordJobInfo(coordID);
        return coordInfo.getActions().get(instanceNumber).getStatus();

    }

    @Deprecated
    public static void putDataInFolders(ColoHelper colo,
                                        final ArrayList<String> inputFoldersForInstance)
    throws Exception {

        for (String anInputFoldersForInstance : inputFoldersForInstance)
            putDataInFolder(colo, anInputFoldersForInstance);

    }

    @Deprecated
    public static void putDataInFolders(FileSystem fs,
                                        final ArrayList<String> inputFoldersForInstance)
    throws Exception {

        for (String anInputFoldersForInstance : inputFoldersForInstance)
            putDataInFolder(fs, anInputFoldersForInstance, null);

    }

    public static void putDataInFolders(ColoHelper colo,
                                        final ArrayList<String> inputFoldersForInstance,
                                        String type) throws Exception {

        for (String anInputFoldersForInstance : inputFoldersForInstance)
            putDataInFolder(colo, anInputFoldersForInstance, type);

    }


    public static void putDataInFolder(ColoHelper colo, final String remoteLocation)
    throws Exception {

        Configuration conf = new Configuration();
        conf.set("fs.default.name",
                "hdfs://" + Util.readPropertiesFile(colo.getEnvFileName(), "hadoop_url"));
        //System.out.println("prop: "+conf.get("fs.default.name"));

        final FileSystem fs = FileSystem.get(conf);
        //System.out.println("fs uri: "+fs.getUri());

        UserGroupInformation user = UserGroupInformation.createRemoteUser("hdfs");


        File[] files = new File("src/test/resources/OozieExampleInputData/normalInput").listFiles();
        //System.out.println("files: "+files);
        assert files != null;
        for (final File file : files) {
            if (!file.isDirectory()) {
                // System.out.println("inside if block");
                user.doAs(new PrivilegedExceptionAction<Boolean>() {

                    @Override

                    public Boolean run() throws Exception {

                        Util.print("putDataInFolder: " + remoteLocation);
                        fs.copyFromLocalFile(new Path(file.getAbsolutePath()),
                                new Path(remoteLocation));
                        return true;

                    }
                });
            }
        }

    }

    @Deprecated
    public static void putDataInFolder(ColoHelper colo, final String remoteLocation, String type)
    throws Exception {

        Configuration conf = new Configuration();
        conf.set("fs.default.name",
                "hdfs://" + Util.readPropertiesFile(colo.getEnvFileName(), "hadoop_url"));
        File[] files;

        final FileSystem fs = FileSystem.get(conf);

        UserGroupInformation user = UserGroupInformation.createRemoteUser("hdfs");

        if ("late".equals(type))
            files = new File("src/test/resources/lateData").listFiles();
        else
            files = new File("src/test/resources/OozieExampleInputData/normalInput").listFiles();

        assert files != null;
        for (final File file : files) {
            if (!file.isDirectory()) {
                user.doAs(new PrivilegedExceptionAction<Boolean>() {

                    @Override
                    public Boolean run() throws Exception {
                        Util.print("putDataInFolder: " + remoteLocation);
                        fs.copyFromLocalFile(new Path(file.getAbsolutePath()),
                                new Path(remoteLocation));
                        return true;

                    }
                });
            }
        }

    }


    public static void putDataInFolder(FileSystem fs, final String remoteLocation, String type)
    throws Exception {
        String inputPath = "src/test/resources/OozieExampleInputData/normalInput";
        if ((null != type) && type.equals("late")) {
            inputPath = "src/test/resources/lateData";
        }
        File[] files = new File(inputPath).listFiles();
        assert files != null;
        for (final File file : files) {
            if (!file.isDirectory()) {
                Util.print("putDataInFolder: " + remoteLocation);
                fs.copyFromLocalFile(new Path(file.getAbsolutePath()), new Path(remoteLocation));
            }
        }
    }

    public static void sleepTill(PrismHelper prismHelper, String startTimeOfLateCoord)
    throws Exception {

        DateTime finalDate = new DateTime(InstanceUtil.oozieDateToDate(startTimeOfLateCoord));

        while (true) {
            DateTime sysDate = new DateTime(Util.getSystemDate(prismHelper));
            Util.print("sysDate: " + sysDate + "  finalDate: " + finalDate);
            if (sysDate.compareTo(finalDate) > 0)
                break;

            Thread.sleep(15000);
        }

    }

    public static DateTime oozieDateToDate(String time) throws ParseException {
        DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy'-'MM'-'dd'T'HH':'mm'Z'");
        fmt = fmt.withZoneUTC();
        return fmt.parseDateTime(time);
    }

    public static void createHDFSFolders(PrismHelper helper, ArrayList<String> folderList)
    throws Exception {
        logger.info("creating folders.....");


        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://" + helper.getFeedHelper().getHadoopURL());

        final FileSystem fs = FileSystem.get(conf);


        UserGroupInformation user = UserGroupInformation.createRemoteUser("rishu");

        for (final String folder : folderList) {
            user.doAs(new PrivilegedExceptionAction<Boolean>() {

                @Override
                public Boolean run() throws Exception {
                    return fs.mkdirs(new Path(folder));

                }
            });
        }

        logger.info("created folders.....");

    }


    public static void putFileInFolders(ColoHelper colo, ArrayList<String> folderList,
                                        final String... fileName) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.default.name",
                Util.readPropertiesFile(colo.getEnvFileName(), "cluster_write"));

        final FileSystem fs = FileSystem.get(conf);

        UserGroupInformation user = UserGroupInformation.createRemoteUser("rishu");


        for (final String folder : folderList) {

            user.doAs(new PrivilegedExceptionAction<Boolean>() {

                @Override
                public Boolean run() throws Exception {
                    for (String aFileName : fileName) {
                        logger.info("copying  " + aFileName + " to " + folder);
                        if (aFileName.equals("_SUCCESS"))
                            fs.mkdirs(new Path(folder + "/_SUCCESS"));
                        else
                            fs.copyFromLocalFile(new Path(aFileName), new Path(folder));
                    }
                    return true;

                }
            });
        }
    }

    public static void createDataWithinDatesAndPrefix(ColoHelper colo, DateTime startDateJoda,
                                                      DateTime endDateJoda, String prefix,
                                                      int interval)
            throws Exception {
        List<String> dataDates =
                Util.getMinuteDatesOnEitherSide(startDateJoda, endDateJoda, interval);

        for (int i = 0; i < dataDates.size(); i++)
            dataDates.set(i, prefix + dataDates.get(i));

        ArrayList<String> dataFolder = new ArrayList<String>();

        for (String dataDate : dataDates) dataFolder.add(dataDate);

        InstanceUtil.putDataInFolders(colo, dataFolder);

    }

    public static org.apache.falcon.regression.core.generated.cluster.Cluster getClusterElement(
            Bundle bundle)
    throws JAXBException {
        JAXBContext jc = JAXBContext
                .newInstance(org.apache.falcon.regression.core.generated.cluster.Cluster.class);
        Unmarshaller u = jc.createUnmarshaller();

        return (org.apache.falcon.regression.core.generated.cluster.Cluster) u
                .unmarshal((new StringReader(bundle.getClusters().get(0))));
    }

    public static void writeClusterElement(Bundle bundle,
                                           org.apache.falcon.regression.core.generated.cluster
                                                   .Cluster c)
    throws JAXBException {
        JAXBContext jc = JAXBContext
                .newInstance(org.apache.falcon.regression.core.generated.cluster.Cluster.class);
        java.io.StringWriter sw = new StringWriter();
        Marshaller marshaller = jc.createMarshaller();
        marshaller.marshal(c, sw);
        //logger.info("modified process is: " + sw);
        bundle.setClusterData(sw.toString());
    }


    @Deprecated
    /**
     * method has been replaced
     */
    public static String setFeedCluster(String feed,
                                        org.apache.falcon.regression.core.generated.feed.Validity
                                                v1,
                                        Retention r1, String n1, ClusterType t1, String partition)
    throws Exception {

        org.apache.falcon.regression.core.generated.feed.Cluster c1 =
                new org.apache.falcon.regression.core.generated.feed.Cluster();
        c1.setName(n1);
        c1.setRetention(r1);
        c1.setType(t1);
        c1.setValidity(v1);
        if (partition != null)
            c1.setPartition(partition);

        Feed f = getFeedElement(feed);

        int numberOfInitialClusters = f.getClusters().getCluster().size();
        if (n1 == null)
            for (int i = 0; i < numberOfInitialClusters; i++)
                f.getClusters().getCluster().set(i, null);
        else {
            f.getClusters().getCluster().add(c1);
        }
        return feedElementToString(f);

    }

    public static String setFeedCluster(String feed,
                                        org.apache.falcon.regression.core.generated.feed.Validity
                                                v1,
                                        Retention r1, String n1, ClusterType t1, String partition,
                                        String... locations)
    throws Exception {

        org.apache.falcon.regression.core.generated.feed.Cluster c1 =
                new org.apache.falcon.regression.core.generated.feed.Cluster();
        c1.setName(n1);
        c1.setRetention(r1);
        if (t1 != null)
            c1.setType(t1);
        c1.setValidity(v1);
        if (partition != null)
            c1.setPartition(partition);


        org.apache.falcon.regression.core.generated.feed.Locations ls =
                new org.apache.falcon.regression.core.generated.feed.Locations();
        if (null != locations) {
            for (int i = 0; i < locations.length; i++) {
                org.apache.falcon.regression.core.generated.feed.Location l =
                        new org.apache.falcon.regression.core.generated.feed.Location();
                l.setPath(locations[i]);
                if (i == 0)
                    l.setType(LocationType.DATA);
                else if (i == 1)
                    l.setType(LocationType.STATS);
                else if (i == 2)
                    l.setType(LocationType.META);
                else if (i == 3)
                    l.setType(LocationType.TMP);
                else
                    Assert.assertTrue(false, "correct value of localtions were not passed");

                ls.getLocation().add(l);
            }

            c1.setLocations(ls);
        }
        Feed f = getFeedElement(feed);

        int numberOfInitialClusters = f.getClusters().getCluster().size();
        if (n1 == null)
            for (int i = 0; i < numberOfInitialClusters; i++)
                f.getClusters().getCluster().set(i, null);
        else {
            f.getClusters().getCluster().add(c1);
        }
        return feedElementToString(f);
    }

    public static Feed getFeedElement(String feed) throws Exception {
        JAXBContext jc = JAXBContext.newInstance(Feed.class);
        Unmarshaller u = jc.createUnmarshaller();
        return (Feed) u.unmarshal((new StringReader(feed)));
    }

    public static String feedElementToString(Feed feedElement) throws Exception {
        JAXBContext jc = JAXBContext.newInstance(Feed.class);
        java.io.StringWriter sw = new StringWriter();
        Marshaller marshaller = jc.createMarshaller();
        marshaller.marshal(feedElement, sw);
        return sw.toString();
    }

    public static ArrayList<String> getReplicationCoordName(String bundleID,
                                                            IEntityManagerHelper helper)
    throws Exception {
        List<CoordinatorJob> cords = InstanceUtil.getBundleCoordinators(bundleID, helper);

        ArrayList<String> ReplicationCordName = new ArrayList<String>();
        for (CoordinatorJob cord : cords) {
            if (cord.getAppName().contains("FEED_REPLICATION"))
                ReplicationCordName.add(cord.getAppName());
        }

        return ReplicationCordName;
    }

    public static String getRetentionCoordName(String bundlID,
                                               IEntityManagerHelper helper) throws Exception {
        List<CoordinatorJob> coords = InstanceUtil.getBundleCoordinators(bundlID, helper);
        String RetentionCoordName = null;
        for (CoordinatorJob coord : coords) {
            if (coord.getAppName().contains("FEED_RETENTION"))
                return coord.getAppName();
        }

        return RetentionCoordName;
    }

    public static ArrayList<String> getReplicationCoordID(String bundlID,
                                                          IEntityManagerHelper helper)
    throws Exception {
        List<CoordinatorJob> coords = InstanceUtil.getBundleCoordinators(bundlID, helper);
        ArrayList<String> ReplicationCoordID = new ArrayList<String>();
        for (CoordinatorJob coord : coords) {
            if (coord.getAppName().contains("FEED_REPLICATION"))
                ReplicationCoordID.add(coord.getId());
        }

        return ReplicationCoordID;
    }

    public static String getRetentionCoordID(String bundlID,
                                             IEntityManagerHelper helper) throws Exception {
        List<CoordinatorJob> coords = InstanceUtil.getBundleCoordinators(bundlID, helper);
        String RetentionCoordID = null;
        for (CoordinatorJob coord : coords) {
            if (coord.getAppName().contains("FEED_RETENTION"))
                return coord.getId();
        }

        return RetentionCoordID;
    }

    public static void putDataInFolders(PrismHelper helper,
                                        final ArrayList<String> inputFoldersForInstance)
    throws Exception {

        for (String anInputFoldersForInstance : inputFoldersForInstance)
            putDataInFolder(helper, anInputFoldersForInstance);

    }

    public static void putDataInFolder(PrismHelper helper, final String remoteLocation)
    throws Exception {

        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://" + helper.getFeedHelper().getHadoopURL());


        final FileSystem fs = FileSystem.get(FileSystem.getDefaultUri(conf), conf, "hdfs");

        UserGroupInformation user = UserGroupInformation.createRemoteUser("hdfs");

        if (!fs.exists(new Path(remoteLocation)))
            fs.mkdirs(new Path(remoteLocation));

        File[] files = new File("src/test/resources/OozieExampleInputData/normalInput").listFiles();
        assert files != null;
        for (final File file : files) {
            if (!file.isDirectory()) {
                user.doAs(new PrivilegedExceptionAction<Boolean>() {

                    @Override
                    public Boolean run() throws Exception {
                        //Util.print("file.getAbsolutePath(): "+file.getAbsolutePath());
                        //Util.print("remoteLocation: "+remoteLocation);


                        fs.copyFromLocalFile(new Path(file.getAbsolutePath()),
                                new Path(remoteLocation));
                        return true;

                    }
                });
            }
        }

    }

    public static ProcessInstancesResult createAndsendRequestProcessInstance(
            String url, String params, String colo) throws Exception {

        if (params != null && !colo.equals("")) {
            url = url + params + "&" + colo.substring(1);
        } else if (params != null) {
            url = url + params;
        } else
            url = url + colo;


        return InstanceUtil.sendRequestProcessInstance(url);

    }

    public static String getFeedPrefix(String feed) throws Exception {
        Feed feedElement = InstanceUtil.getFeedElement(feed);
        String p = feedElement.getLocations().getLocation().get(0).getPath();
        p = p.substring(0, p.indexOf("$"));
        return p;
    }

    public static String setProcessCluster(String process,
                                           String clusterName,
                                           org.apache.falcon.regression.core.generated.process
                                                   .Validity validity)
    throws Exception {


        org.apache.falcon.regression.core.generated.process.Cluster c =
                new org.apache.falcon.regression.core.generated.process.Cluster();

        c.setName(clusterName);
        c.setValidity(validity);

        Process p = InstanceUtil.getProcessElement(process);


        if (clusterName == null)
            p.getClusters().getCluster().set(0, null);
        else {
            p.getClusters().getCluster().add(c);
        }
        return processToString(p);

    }

    public static String processToString(Process p) throws Exception {
        JAXBContext jc = JAXBContext.newInstance(Process.class);
        java.io.StringWriter sw = new StringWriter();
        Marshaller marshaller = jc.createMarshaller();
        marshaller.marshal(p, sw);
        return sw.toString();
    }


    public static Process getProcessElement(String process) throws JAXBException {
        JAXBContext jc = JAXBContext.newInstance(Process.class);
        Unmarshaller u = jc.createUnmarshaller();

        return (Process) u.unmarshal((new StringReader(process)));
    }

    public static String addProcessInputFeed(String process, String feed,
                                             String feedName) throws Exception {
        Process processElement = InstanceUtil.getProcessElement(process);
        Input in1 = processElement.getInputs().getInput().get(0);
        Input in2 = new Input();
        in2.setEnd(in1.getEnd());
        in2.setFeed(feed);
        in2.setName(feedName);
        in2.setPartition(in1.getPartition());
        in2.setStart(in1.getStart());
        processElement.getInputs().getInput().add(in2);
        return processToString(processElement);
    }

    public static org.apache.oozie.client.WorkflowJob.Status getInstanceStatusFromCoord(
            ColoHelper ua1,
            String coordID,
            int instanceNumber)
    throws Exception {
        XOozieClient oozieClient = new XOozieClient(ua1.getProcessHelper().getOozieURL());
        CoordinatorJob coordInfo = oozieClient.getCoordJobInfo(coordID);
        WorkflowJob actionInfo =
                oozieClient.getJobInfo(coordInfo.getActions().get(instanceNumber).getExternalId());
        return actionInfo.getStatus();
        //return coordInfo.getActions().get(instanceNumber).getStatus();
    }

    public static ArrayList<String> getInputFoldersForInstanceForReplication(
            ColoHelper coloHelper, String coordID, int instanceNumber) throws OozieClientException {
        XOozieClient oozieClient = new XOozieClient(coloHelper.getProcessHelper().getOozieURL());
        CoordinatorAction x = oozieClient.getCoordActionInfo(coordID + "@" + instanceNumber);
        return InstanceUtil.getReplicationFolderFromInstanceRunConf(x.getRunConf());
    }

    public static ArrayList<String> getReplicationFolderFromInstanceRunConf(
            String runConf) {
        String conf;
        conf = runConf.substring(runConf.indexOf("ivoryInPaths</name>") + 19);
        //	Util.print("conf1: "+conf);

        conf = conf.substring(conf.indexOf("<value>") + 7);
        //Util.print("conf2: "+conf);

        conf = conf.substring(0, conf.indexOf("</value>"));
        //Util.print("conf3: "+conf);

        return new ArrayList<String>(Arrays.asList(conf.split(",")));
    }

    public static int getInstanceRunIdFromCoord(ColoHelper colo,
                                                String coordID, int instanceNumber)
    throws Exception {
        XOozieClient oozieClient = new XOozieClient(colo.getProcessHelper().getOozieURL());
        CoordinatorJob coordInfo = oozieClient.getCoordJobInfo(coordID);

        WorkflowJob actionInfo =
                oozieClient.getJobInfo(coordInfo.getActions().get(instanceNumber).getExternalId());
        return actionInfo.getRun();
    }

    public static void putLateDataInFolders(ColoHelper helper,
                                            ArrayList<String> inputFolderList,
                                            int lateDataFolderNumber)
    throws Exception {

        for (String anInputFolderList : inputFolderList)
            putLateDataInFolder(helper, anInputFolderList, lateDataFolderNumber);
    }

    public static void putLateDataInFolder(ColoHelper helper, final String remoteLocation,
                                           int lateDataFolderNumber)
    throws IOException, InterruptedException {

        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://" + helper.getFeedHelper().getHadoopURL());


        final FileSystem fs = FileSystem.get(conf);

        UserGroupInformation user = UserGroupInformation.createRemoteUser("hdfs");


        File[] files = new File("src/test/resources/OozieExampleInputData/normalInput").listFiles();
        if (lateDataFolderNumber == 2) {
            files = new File("src/test/resources/OozieExampleInputData/2ndLateData").listFiles();
        }

        assert files != null;
        for (final File file : files) {
            if (!file.isDirectory()) {
                user.doAs(new PrivilegedExceptionAction<Boolean>() {

                    @Override
                    public Boolean run() throws Exception {
                        //	Util.print("putDataInFolder: "+remoteLocation);
                        fs.copyFromLocalFile(new Path(file.getAbsolutePath()),
                                new Path(remoteLocation));
                        return true;

                    }
                });
            }
        }
    }

    public static String setFeedFilePath(String feed, String path) throws Exception {
        Feed feedElement = InstanceUtil.getFeedElement(feed);
        feedElement.getLocations().getLocation().get(0).setPath(path);
        return InstanceUtil.feedElementToString(feedElement);

    }

    public static int checkIfFeedCoordExist(IEntityManagerHelper helper,
                                            String feedName, String coordType) throws Exception {

        int numberOfCoord = 0;

        if (Util.getBundles(helper.getOozieClient(), feedName, ENTITY_TYPE.FEED).size() == 0)
            return 0;


        List<String> bundleID = Util.getBundles(helper.getOozieClient(), feedName, ENTITY_TYPE.FEED);

        for (String aBundleID : bundleID) {

            List<CoordinatorJob> coords =
                    InstanceUtil.getBundleCoordinators(aBundleID, helper);

            for (CoordinatorJob coord : coords) {
                if (coord.getAppName().contains(coordType))
                    numberOfCoord++;
            }
        }
        return numberOfCoord;
    }


    public static String setProcessFrequency(String process,
                                             Frequency frequency) throws Exception {
        Process p = InstanceUtil.getProcessElement(process);

        p.setFrequency(frequency);

        return InstanceUtil.processToString(p);
    }

    public static String setProcessName(String process, String newName) throws Exception {
        Process p = InstanceUtil.getProcessElement(process);

        p.setName(newName);

        return InstanceUtil.processToString(p);
    }


    public static String setProcessValidity(String process,
                                            String startTime, String endTime) throws Exception {

        Process processElement = InstanceUtil.getProcessElement(process);

        for (int i = 0; i < processElement.getClusters().getCluster().size(); i++) {
            processElement.getClusters().getCluster().get(i).getValidity().setStart(
                    InstanceUtil.oozieDateToDate(startTime).toDate());
            processElement.getClusters().getCluster().get(i).getValidity()
                    .setEnd(InstanceUtil.oozieDateToDate(endTime).toDate());

        }

        return InstanceUtil.processToString(processElement);
    }

    public static List<CoordinatorAction> getProcessInstanceListFromAllBundles(
            ColoHelper coloHelper, String processName, ENTITY_TYPE entityType)
    throws Exception {

        XOozieClient oozieClient = new XOozieClient(coloHelper.getProcessHelper().getOozieURL());

        List<CoordinatorAction> list = new ArrayList<CoordinatorAction>();

        System.out.println("bundle size for process is " +
                Util.getBundles(coloHelper.getFeedHelper().getOozieClient(), processName, entityType).size());

        for (String bundleId : Util.getBundles(coloHelper.getFeedHelper().getOozieClient(), processName, entityType)) {
            BundleJob bundleInfo = oozieClient.getBundleJobInfo(bundleId);
            List<CoordinatorJob> coords = bundleInfo.getCoordinators();

            System.out.println("number of coords in bundle " + bundleId + "=" + coords.size());

            for (CoordinatorJob coord : coords) {
                List<CoordinatorAction> actions =
                        oozieClient.getCoordJobInfo(coord.getId()).getActions();
                System.out.println("number of actions in coordinator " + coord.getId() + " is " +
                        actions.size());
                list.addAll(actions);
            }
        }

        String coordId = getLatestCoordinatorID(coloHelper, processName, entityType);
        //String coordId = getDefaultCoordinatorFromProcessName(processName);
        Util.print("default coordID: " + coordId);

        return list;
    }

    public static String getOutputFolderForInstanceForReplication(ColoHelper coloHelper,
                                                                  String coordID,
                                                                  int instanceNumber)
    throws OozieClientException {
        XOozieClient oozieClient = new XOozieClient(coloHelper.getProcessHelper().getOozieURL());
        CoordinatorJob coordInfo = oozieClient.getCoordJobInfo(coordID);

        return InstanceUtil.getReplicatedFolderFromInstanceRunConf(
                oozieClient.getJobInfo(coordInfo.getActions().get(instanceNumber).getExternalId())
                        .getConf());
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
            ColoHelper coloHelper, String coordID, int instanceNumber) throws Exception {
        XOozieClient oozieClient = new XOozieClient(coloHelper.getProcessHelper().getOozieURL());
        CoordinatorJob coordInfo = oozieClient.getCoordJobInfo(coordID);

        return InstanceUtil.getReplicatedFolderBaseFromInstanceRunConf(
                oozieClient.getJobInfo(coordInfo.getActions().get(instanceNumber).getExternalId())
                        .getConf());
    }

    private static String getReplicatedFolderBaseFromInstanceRunConf(String runConf) {
        String conf = runConf.substring(runConf.indexOf("distcpTargetPaths</name>") + 24);
        //	Util.print("conf1: "+conf);

        conf = conf.substring(conf.indexOf("<value>") + 7);
        //Util.print("conf2: "+conf);

        conf = conf.substring(0, conf.indexOf("</value>"));

        return conf;
    }

    public static String ClusterElementToString(
            org.apache.falcon.regression.core.generated.cluster.Cluster c)
    throws JAXBException {
        JAXBContext jc = JAXBContext
                .newInstance(org.apache.falcon.regression.core.generated.cluster.Cluster.class);
        java.io.StringWriter sw = new StringWriter();
        Marshaller marshaller = jc.createMarshaller();
        marshaller.marshal(c, sw);
        //logger.info("modified process is: " + sw);
        return sw.toString();
    }

    public static org.apache.falcon.regression.core.generated.cluster.Cluster getClusterElement(
            String clusterData)
    throws Exception {
        JAXBContext jc = JAXBContext
                .newInstance(org.apache.falcon.regression.core.generated.cluster.Cluster.class);
        Unmarshaller u = jc.createUnmarshaller();

        return (org.apache.falcon.regression.core.generated.cluster.Cluster) u
                .unmarshal((new StringReader(clusterData)));
    }

    @Deprecated
    public static void waitTillInstanceReachState(ColoHelper coloHelper,
                                                  String processName, int numberOfInstance,
                                                  org.apache.oozie.client.CoordinatorAction
                                                          .Status expectedStatus,
                                                  int minutes)
    throws Exception {


        int sleep = minutes * 60 / 20;

        for (int sleepCount = 0; sleepCount < sleep; sleepCount++) {

            ArrayList<org.apache.oozie.client.CoordinatorAction.Status> statusList = InstanceUtil
                    .getStatusAllInstanceStatusForProcess(coloHelper, processName);
            int instanceWithStatus = 0;
            for (CoordinatorAction.Status aStatusList : statusList) {

                if (aStatusList.equals(expectedStatus))
                    instanceWithStatus++;

            }

            if (instanceWithStatus == numberOfInstance)
                break;

            Thread.sleep(20000);

        }
    }

    @Deprecated
    public static void waitTillInstanceReachState(ColoHelper coloHelper,
                                                  String entityName, int numberOfInstance,
                                                  org.apache.oozie.client.CoordinatorAction
                                                          .Status expectedStatus,
                                                  int totalMinutesToWait, ENTITY_TYPE entityType)
    throws Exception {

        int sleep = totalMinutesToWait * 60 / 20;

        for (int sleepCount = 0; sleepCount < sleep; sleepCount++) {

            ArrayList<org.apache.oozie.client.CoordinatorAction.Status> statusList = InstanceUtil
                    .getStatusAllInstance(coloHelper, entityName, entityType);

            int instanceWithStatus = 0;
            for (CoordinatorAction.Status aStatusList : statusList) {

                if (aStatusList.equals(expectedStatus))
                    instanceWithStatus++;

            }

            if (instanceWithStatus >= numberOfInstance)
                return;

            Thread.sleep(20000);

        }

        Assert.assertTrue(false, "expceted state of instance was never reached");
    }

    public static void waitTillInstanceReachState(OozieClient client, String entityName,
                                                  int numberOfInstance,
                                                  org.apache.oozie.client.CoordinatorAction
                                                          .Status expectedStatus,
                                                  int totalMinutesToWait, ENTITY_TYPE entityType)
    throws InterruptedException, OozieClientException {
        String filter = "";
        // get the bunlde ids
        if (entityType.equals(ENTITY_TYPE.FEED)) {
            filter = "name=FALCON_FEED_" + entityName;
        } else {
            filter = "name=FALCON_PROCESS_" + entityName;
        }
        List<BundleJob> bundleJobs = new ArrayList<BundleJob>();
        int retries = 0;
        while ((bundleJobs.size() == 0) && (retries < 20)) {
            bundleJobs = OozieUtil.getBundles(client, filter, 0, 10);
            retries++;
            Thread.sleep(5000);
        }
        if (bundleJobs.size() == 0) {
            Assert.assertTrue(false, "Could not retrieve bundles");
        }
        List<String> bundleIds = OozieUtil.getBundleIds(bundleJobs);
        String bundleId = OozieUtil.getMaxId(bundleIds);
        logger.info(String.format("Using bundle %s", bundleId));
        String coordId = null;
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
        int maxTries = 20;
        int totalSleepTime = totalMinutesToWait * 60 * 1000;
        int sleepTime = totalSleepTime / maxTries;
        logger.info(String.format("Sleep for %d seconds", sleepTime / 1000));
        for (int i = 0; i < maxTries; i++) {
            logger.info(String.format("Try %d of %d", (i + 1), maxTries));
            int instanceWithStatus = 0;
            CoordinatorJob coordinatorJob = client.getCoordJobInfo(coordId);
            List<CoordinatorAction> coordinatorActions = coordinatorJob.getActions();
            for (CoordinatorAction coordinatorAction : coordinatorActions) {
                logger.info(String.format("Coordinator Action %s status is %s",
                        coordinatorAction.getId(), coordinatorAction.getStatus()));
                if (expectedStatus == coordinatorAction.getStatus()) {
                    instanceWithStatus++;
                }
            }

            if (instanceWithStatus >= numberOfInstance)
                return;
            Thread.sleep(sleepTime);
        }
        Assert.assertTrue(false, "expceted state of instance was never reached");
    }

    private static ArrayList<org.apache.oozie.client.CoordinatorAction.Status>
    getStatusAllInstanceStatusForProcess(
            ColoHelper coloHelper, String processName) throws Exception {

        CoordinatorJob coordInfo = InstanceUtil.getCoordJobForProcess(coloHelper, processName);

        ArrayList<org.apache.oozie.client.CoordinatorAction.Status> statusList =
                new ArrayList<org.apache.oozie.client.CoordinatorAction.Status>();
        for (int count = 0; count < coordInfo.getActions().size(); count++)
            statusList.add(coordInfo.getActions().get(count).getStatus());

        return statusList;
    }

    private static ArrayList<org.apache.oozie.client.CoordinatorAction.Status> getStatusAllInstance(
            ColoHelper coloHelper, String entityName, ENTITY_TYPE entityType) throws Exception {

        CoordinatorJob coordInfo =
                InstanceUtil.getCoordJobForProcess(coloHelper, entityName, entityType);

        ArrayList<org.apache.oozie.client.CoordinatorAction.Status> statusList =
                new ArrayList<org.apache.oozie.client.CoordinatorAction.Status>();
        for (int count = 0; count < coordInfo.getActions().size(); count++)
            statusList.add(coordInfo.getActions().get(count).getStatus());

        return statusList;
    }

    private static CoordinatorJob getCoordJobForProcess(
            ColoHelper coloHelper, String processName) throws Exception {

        String bundleID =
                InstanceUtil.getSequenceBundleID(coloHelper, processName, ENTITY_TYPE.PROCESS, 0);
        String coordID = InstanceUtil.getDefaultCoordIDFromBundle(coloHelper, bundleID);
        XOozieClient oozieClient = new XOozieClient(coloHelper.getProcessHelper().getOozieURL());
        return oozieClient.getCoordJobInfo(coordID);
    }

    private static CoordinatorJob getCoordJobForProcess(
            ColoHelper coloHelper, String processName, ENTITY_TYPE entityType) throws Exception {

        String bundleID = InstanceUtil.getLatestBundleID(coloHelper, processName, entityType);
        //instanceUtil.getSequenceBundleID(coloHelper,processName,entityType, 0);
        String coordID;
        if (entityType.equals(ENTITY_TYPE.PROCESS))
            coordID = InstanceUtil.getDefaultCoordIDFromBundle(coloHelper, bundleID);
        else
            coordID =
                    InstanceUtil.getReplicationCoordID(bundleID, coloHelper.getFeedHelper()).get(0);
        XOozieClient oozieClient = new XOozieClient(coloHelper.getProcessHelper().getOozieURL());
        return oozieClient.getCoordJobInfo(coordID);
    }

    public static String removeFeedPartitionsTag(String feed) throws Exception {
        Feed f = getFeedElement(feed);

        f.setPartitions(null);

        return feedElementToString(f);

    }

    public static void waitForBundleToReachState(
            ColoHelper coloHelper,
            String processName,
            org.apache.oozie.client.Job.Status expectedStatus,
            int totalMinutesToWait) throws Exception {

        int sleep = totalMinutesToWait * 60 / 20;

        for (int sleepCount = 0; sleepCount < sleep; sleepCount++) {

            String BundleID = InstanceUtil.getLatestBundleID(coloHelper, processName, ENTITY_TYPE.PROCESS);

            XOozieClient oozieClient =
                    new XOozieClient(coloHelper.getProcessHelper().getOozieURL());

            BundleJob j = oozieClient.getBundleJobInfo(BundleID);


            if (j.getStatus().equals(expectedStatus))
                break;

            Thread.sleep(20000);
        }
    }

    public static void waitTillParticularInstanceReachState(ColoHelper coloHelper,
                                                            String entityName, int instanceNumber,
                                                            org.apache.oozie.client.CoordinatorAction.Status
                                                                    expectedStatus,
                                                            int totalMinutesToWait,
                                                            ENTITY_TYPE entityType)
    throws Exception {

        int sleep = totalMinutesToWait * 60 / 20;

        for (int sleepCount = 0; sleepCount < sleep; sleepCount++) {

            ArrayList<org.apache.oozie.client.CoordinatorAction.Status> statusList = InstanceUtil
                    .getStatusAllInstance(coloHelper, entityName, entityType);

            if (statusList.get(instanceNumber).equals(expectedStatus))
                break;
            Thread.sleep(20000);

        }


    }

    public static ArrayList<String> createEmptyDirWithinDatesAndPrefix(ColoHelper colo,
                                                                       DateTime startDateJoda,
                                                                       DateTime endDateJoda,
                                                                       String prefix,
                                                                       int interval)
    throws Exception {
        List<String> dataDates =
                Util.getMinuteDatesOnEitherSide(startDateJoda, endDateJoda, interval);

        for (int i = 0; i < dataDates.size(); i++)
            dataDates.set(i, prefix + dataDates.get(i));

        ArrayList<String> dataFolder = new ArrayList<String>();

        for (String dataDate : dataDates) dataFolder.add(dataDate);

        InstanceUtil.createHDFSFolders(colo, dataFolder);
        return dataFolder;
    }
}

