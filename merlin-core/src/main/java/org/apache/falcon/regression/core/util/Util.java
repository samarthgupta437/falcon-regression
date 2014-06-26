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


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.UserInfo;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.falcon.regression.core.enumsAndConstants.MerlinConstants;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.Interface;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.feed.Location;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.feed.Property;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.helpers.PrismHelper;
import org.apache.falcon.regression.core.interfaces.IEntityManagerHelper;
import org.apache.falcon.regression.core.response.APIResult;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.supportClasses.Consumer;
import org.apache.falcon.regression.core.enumsAndConstants.ENTITY_TYPE;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.http.HttpResponse;
import org.apache.falcon.request.BaseRequest;
import org.apache.falcon.request.RequestKeys;
import org.apache.hadoop.security.authentication.client.AuthenticationException;


import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.Assert;
import org.apache.log4j.Logger;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringReader;
import java.io.StringWriter;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class Util {


    static Logger logger = Logger.getLogger(Util.class);
    static final String MERLIN_PROPERTIES = "Merlin.properties";
    static final String PRISM_PREFIX = "prism";

    static PrismHelper prismHelper = new PrismHelper(MERLIN_PROPERTIES, PRISM_PREFIX);

    public static ServiceResponse sendRequest(String url, String method)
        throws IOException, URISyntaxException, AuthenticationException {
        return sendRequest(url, method, null, null);
    }

    public static ServiceResponse sendRequest(String url, String method, String user)
        throws IOException, URISyntaxException, AuthenticationException {
        return sendRequest(url, method, null, user);
    }

    public static ServiceResponse sendRequest(String url, String method, String data,
                                              String user)
        throws IOException, URISyntaxException, AuthenticationException {
        BaseRequest request = new BaseRequest(url, method, user, data);
        request.addHeader(RequestKeys.CONTENT_TYPE_HEADER, RequestKeys.XML_CONTENT_TYPE);
        HttpResponse response = request.run();
        return new ServiceResponse(response);
    }

    public static String getExpectedErrorMessage(String filename) throws IOException {

        Properties properties = new Properties();
        final InputStream resourceAsStream =
            Util.class.getResourceAsStream("/" + "errorMapping.properties");
        properties.load(resourceAsStream);
        resourceAsStream.close();
        return properties.getProperty(filename);
    }

    public static String getProcessName(String data) throws JAXBException {
        JAXBContext jc = JAXBContext.newInstance(Process.class);
        Unmarshaller u = jc.createUnmarshaller();
        Process processElement = (Process) u.unmarshal((new StringReader(data)));
        return processElement.getName();
    }

    private static boolean isXML(String data) {

        if (data != null && data.trim().length() > 0) {
            if (data.trim().startsWith("<")) {
                return true; //find a better way of validation
            }
        }

        return false;
    }

    public static APIResult parseResponse(ServiceResponse response) throws JAXBException {

        if (!isXML(response.getMessage())) {
            return new APIResult(APIResult.Status.FAILED, response.getMessage(), "somerandomstring",
                response.getCode());
        }

        JAXBContext jc = JAXBContext.newInstance(APIResult.class);
        Unmarshaller u = jc.createUnmarshaller();
        APIResult temp;
        if (response.getMessage().contains("requestId")) {
            temp = (APIResult) u
                .unmarshal(new InputSource(new StringReader(response.getMessage())));
            temp.setStatusCode(response.getCode());
        } else {
            temp = new APIResult();
            temp.setStatusCode(response.getCode());
            temp.setMessage(response.getMessage());
            temp.setRequestId("");
            if (response.getCode() == 200) {
                temp.setStatus(APIResult.Status.SUCCEEDED);
            } else {
                temp.setStatus(APIResult.Status.FAILED);
            }
        }

        return temp;
    }

    public static List<String> getProcessStoreInfo(IEntityManagerHelper helper)
        throws IOException, JSchException {
        return getStoreInfo(helper, "/PROCESS");
    }

    public static List<String> getDataSetStoreInfo(IEntityManagerHelper helper)
        throws IOException, JSchException {
        return getStoreInfo(helper, "/FEED");
    }

    public static List<String> getDataSetArchiveInfo(IEntityManagerHelper helper)
        throws IOException, JSchException {
        return getStoreInfo(helper, "/archive/FEED");
    }

    public static List<String> getArchiveStoreInfo(IEntityManagerHelper helper)
        throws IOException, JSchException {
        return getStoreInfo(helper, "/archive/PROCESS");
    }

    public static List<String> getClusterStoreInfo(IEntityManagerHelper helper)
        throws IOException, JSchException {
        return getStoreInfo(helper, "/CLUSTER");
    }

    public static List<String> getClusterArchiveInfo(IEntityManagerHelper helper)
        throws IOException, JSchException {
        return getStoreInfo(helper, "/archive/CLUSTER");
    }

    private static List<String> getStoreInfo(IEntityManagerHelper helper, String subPath)
        throws IOException, JSchException {
        if (helper.getStoreLocation().startsWith("hdfs:")) {
            return HadoopUtil.getAllFilesHDFS(helper.getStoreLocation(),
                helper.getStoreLocation() + subPath);
        } else {
            return runRemoteScriptAsSudo(helper.getQaHost(), helper.getUsername(),
                helper.getPassword(), "ls " + helper.getStoreLocation() + "/store" + subPath,
                helper.getUsername(), helper.getIdentityFile());
        }
    }

    public static File[] getFiles(String directoryPath) throws URISyntaxException {
        directoryPath = directoryPath.replaceFirst("^.*test-classes[\\\\/]", "");
        logger.info("directoryPath: " + directoryPath);
        URL url = Util.class.getResource("/" + directoryPath);
        logger.info("url" + url);
        File dir = new File(url.toURI());
        File[] files = dir.listFiles();
        if (files != null) Arrays.sort(files);
        return files;
    }

    public static String readEntityName(String data) throws JAXBException {

        if (data.contains("uri:falcon:feed"))
            return InstanceUtil.getFeedElement(data).getName();
        else if (data.contains("uri:falcon:process"))
            return InstanceUtil.getProcessElement(data).getName();
        else
            return InstanceUtil.getClusterElement(data).getName();
    }

    public static String readClusterName(String data) throws JAXBException {
        JAXBContext jc = JAXBContext.newInstance(Cluster.class);
        Unmarshaller u = jc.createUnmarshaller();

        Cluster clusterElement = (Cluster) u.unmarshal(new StringReader(data));

        return clusterElement.getName();
    }

    public static String readDatasetName(String data) throws JAXBException {
        JAXBContext jc = JAXBContext.newInstance(Feed.class);
        Unmarshaller u = jc.createUnmarshaller();

        Feed datasetElement = (Feed) u.unmarshal((new StringReader(data)));

        return datasetElement.getName();

    }

    public static String generateUniqueProcessEntity(String data) throws JAXBException {

        JAXBContext jc = JAXBContext.newInstance(Process.class);
        Unmarshaller u = jc.createUnmarshaller();
        Process processElement = (Process) u.unmarshal((new StringReader(data)));
        processElement.setName(processElement.getName() + "-" + UUID.randomUUID());
        java.io.StringWriter sw = new StringWriter();
        Marshaller marshaller = jc.createMarshaller();
        marshaller.marshal(processElement, sw);

        return sw.toString();

    }


    public static String generateUniqueClusterEntity(String data) throws JAXBException {

        JAXBContext jc = JAXBContext.newInstance(Cluster.class);
        Unmarshaller u = jc.createUnmarshaller();
        Cluster clusterElement = (Cluster) u.unmarshal((new StringReader(data)));
        clusterElement.setName(clusterElement.getName() + "-" + UUID.randomUUID());

        //lets marshall it back and return
        java.io.StringWriter sw = new StringWriter();
        Marshaller marshaller = jc.createMarshaller();
        marshaller.marshal(clusterElement, sw);

        return sw.toString();
    }

    public static String generateUniqueDataEntity(String data) throws JAXBException {

        JAXBContext jc = JAXBContext.newInstance(Feed.class);
        Unmarshaller u = jc.createUnmarshaller();
        Feed dataElement = (Feed) u.unmarshal((new StringReader(data)));
        dataElement.setName(dataElement.getName() + "-" + UUID.randomUUID());

        return InstanceUtil.feedElementToString(dataElement);
    }

    public static String readPropertiesFile(String filename, String property) {
        return readPropertiesFile(filename, property, null);
    }

    public static String readPropertiesFile(String filename, String property, String defaultValue) {
        String desired_property;

        try {
            InputStream conf_stream = Util.class.getResourceAsStream("/" + filename);

            Properties properties = new Properties();
            properties.load(conf_stream);
            desired_property = properties.getProperty(property, defaultValue);
            conf_stream.close();

            return desired_property;
        } catch (Exception e) {
            logger.info(e.getStackTrace());
        }
        return null;
    }

    public static List<String> getHadoopDataFromDir(ColoHelper helper, String feed, String dir)
        throws JAXBException, IOException {
        List<String> finalResult = new ArrayList<String>();

        String feedPath = getFeedPath(feed);
        int depth = feedPath.split(dir)[1].split("/").length - 1;
        List<Path> results = HadoopUtil.getAllDirsRecursivelyHDFS(helper,
            new Path(dir), depth);

        for (Path result : results) {
            int pathDepth = result.toString().split(dir)[1].split("/").length - 1;
            if (pathDepth == depth) {
                finalResult.add(result.toString().split(dir)[1]);
            }
        }

        return finalResult;
    }


    public static int executeCommandGetExitCode(String command) {
        return executeCommand(command).getExitVal();
    }


    public static String executeCommandGetOutput(String command) {
        return executeCommand(command).getOutput();
    }

    public static String setFeedProperty(String feed, String propertyName, String propertyValue)
        throws JAXBException {

        Feed feedObject = InstanceUtil.getFeedElement(feed);

        boolean found = false;
        for (Property prop : feedObject.getProperties().getProperties()) {
            //check if it is present
            if (prop.getName().equalsIgnoreCase(propertyName)) {
                prop.setValue(propertyValue);
                found = true;
                break;
            }
        }

        if (!found) {
            Property property = new Property();
            property.setName(propertyName);
            property.setValue(propertyValue);
            feedObject.getProperties().getProperties().add(property);
        }


        return InstanceUtil.feedElementToString(feedObject);

    }


    public static String getFeedPath(String feed) throws JAXBException {
        JAXBContext context = JAXBContext.newInstance(Feed.class);
        Unmarshaller um = context.createUnmarshaller();
        Feed feedObject = (Feed) um.unmarshal(new StringReader(feed));

        for (Location location : feedObject.getLocations().getLocations()) {
            if (location.getType().equals(LocationType.DATA)) {
                return location.getPath();
            }
        }

        return null;
    }

    public static ExecResult executeCommand(String command) {
        logger.info("Command to be executed: " + command);
        StringBuilder errors = new StringBuilder();
        StringBuilder output = new StringBuilder();

        try {
            java.lang.Process process = Runtime.getRuntime().exec(command);

            BufferedReader errorReader =
                new BufferedReader(new InputStreamReader(process.getErrorStream()));
            BufferedReader consoleReader =
                new BufferedReader(new InputStreamReader(process.getInputStream()));

            String line;
            while ((line = errorReader.readLine()) != null) {
                errors.append(line).append("\n");
            }

            while ((line = consoleReader.readLine()) != null) {
                output.append(line).append("\n");
            }
            final int exitVal = process.waitFor();
            logger.info("exitVal: " + exitVal);
            logger.info("output: " + output);
            logger.info("errors: " + errors);
            return new ExecResult(exitVal, output.toString().trim(), errors.toString().trim());
        } catch (InterruptedException e) {
            Assert.fail("Process execution failed:" + ExceptionUtils.getStackTrace(e));
        } catch (IOException e) {
            Assert.fail("Process execution failed:" + ExceptionUtils.getStackTrace(e));
        }
        return null;
    }

    public static String insertLateFeedValue(String feed, Frequency frequency)
        throws JAXBException {
        JAXBContext context = JAXBContext.newInstance(Feed.class);
        Unmarshaller um = context.createUnmarshaller();
        Feed feedObject = (Feed) um.unmarshal(new StringReader(feed));

        feedObject.getLateArrival().setCutOff(frequency);

        Marshaller m = context.createMarshaller();
        StringWriter sw = new StringWriter();

        m.marshal(feedObject, sw);

        return sw.toString();
    }

    public static void createLateDataFoldersWithRandom(PrismHelper prismHelper, String folderPrefix,
                                                       List<String> folderList)
        throws IOException {
        logger.info("creating late data folders.....");
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://" + prismHelper.getProcessHelper().getHadoopURL() + "");

        final FileSystem fs = FileSystem.get(conf);

        folderList.add("somethingRandom");

        for (final String folder : folderList) {
            fs.mkdirs(new Path(folderPrefix + folder));
        }

        logger.info("created all late data folders.....");
    }

    public static void copyDataToFolders(PrismHelper prismHelper, List<String> folderList,
                                         String directory, String folderPrefix)
        throws IOException {
        logger.info("copying data into folders....");
        List<String> fileLocations = new ArrayList<String>();
        File[] files = new File(directory).listFiles();
        if (files != null) {
            for (final File file : files) {
                fileLocations.add(file.toString());
            }
        }
        copyDataToFolders(prismHelper, folderPrefix, folderList,
            fileLocations.toArray(new String[fileLocations.size()]));
    }

    public static void copyDataToFolders(PrismHelper prismHelper, final String folderPrefix,
                                         List<String> folderList,
                                         String... fileLocations)
        throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://" + prismHelper.getProcessHelper().getHadoopURL());

        final FileSystem fs = FileSystem.get(conf);

        for (final String folder : folderList) {
            boolean r;
            String folder_space = folder.replaceAll("/", "_");
            File f = new File(
                OSUtil.NORMAL_INPUT + folder_space +
                    ".txt");
            if (!f.exists()) {
                r = f.createNewFile();
                if (!r)
                    logger.info("file could not be created");
            }


            FileWriter fr = new FileWriter(f);
            fr.append("folder");
            fr.close();
            fs.copyFromLocalFile(new Path(f.getAbsolutePath()), new Path(folderPrefix + folder));
            r = f.delete();
            if (!r)
                logger.info("delete was not successful");

            Path[] srcPaths = new Path[fileLocations.length];
            for (int i = 0; i < srcPaths.length; ++i) {
                srcPaths[i] = new Path(fileLocations[i]);
            }
            logger.info("copying  " + Arrays.toString(srcPaths) + " to " + folderPrefix + folder);
            fs.copyFromLocalFile(false, true, srcPaths, new Path(folderPrefix + folder));
        }
    }


    public static String setFeedPathValue(String feed, String pathValue) throws JAXBException {
        JAXBContext feedContext = JAXBContext.newInstance(Feed.class);
        Feed feedObject = (Feed) feedContext.createUnmarshaller().unmarshal(new StringReader(feed));

        //set the value
        for (Location location : feedObject.getLocations().getLocations()) {
            if (location.getType().equals(LocationType.DATA)) {
                location.setPath(pathValue);
            }
        }

        StringWriter feedWriter = new StringWriter();
        feedContext.createMarshaller().marshal(feedObject, feedWriter);
        return feedWriter.toString();
    }


    public static String findFolderBetweenGivenTimeStamps(DateTime startTime, DateTime endTime,
                                                          List<String> folderList) {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy/MM/dd/HH/mm");

        for (String folder : folderList) {
            if (folder.compareTo(formatter.print(startTime)) >= 0 &&
                folder.compareTo(formatter.print(endTime)) <= 0) {
                return folder;
            }
        }
        return null;
    }

    public static void lateDataReplenish(PrismHelper prismHelper, int interval,
                                         int minuteSkip, String folderPrefix) throws IOException {
        List<String> folderData = TimeUtil.getMinuteDatesOnEitherSide(interval, minuteSkip);

        Util.createLateDataFoldersWithRandom(prismHelper, folderPrefix, folderData);
        Util.copyDataToFolders(prismHelper, folderData,
            OSUtil.NORMAL_INPUT, folderPrefix);
    }

    public static void createLateDataFolders(PrismHelper prismHelper, List<String> folderList,
                                             final String FolderPrefix)
        throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://" + prismHelper.getProcessHelper().getHadoopURL() + "");

        final FileSystem fs = FileSystem.get(conf);

        for (final String folder : folderList) {
            fs.mkdirs(new Path(FolderPrefix + folder));
        }
    }

    public static void injectMoreData(PrismHelper prismHelper, final String remoteLocation,
                                      String localLocation)
        throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://" + prismHelper.getClusterHelper().getHadoopURL() + "");

        final FileSystem fs = FileSystem.get(conf);

        File[] files = new File(localLocation).listFiles();
        assert files != null;
        for (final File file : files) {
            if (!file.isDirectory()) {
                String path = remoteLocation + "/" +
                    System.currentTimeMillis() / 1000 + "/";
                logger.info("inserting data@ " + path);
                fs.copyFromLocalFile(new Path(file.getAbsolutePath()), new Path(path));
            }
        }

    }

    public static String setFeedName(String feedString, String newName) throws JAXBException {
        JAXBContext feedContext = JAXBContext.newInstance(Feed.class);
        Feed feedObject =
            (Feed) feedContext.createUnmarshaller().unmarshal(new StringReader(feedString));

        //set the value
        feedObject.setName(newName);
        StringWriter feedWriter = new StringWriter();
        feedContext.createMarshaller().marshal(feedObject, feedWriter);
        return feedWriter.toString().trim();
    }

    public static String setClusterNameInFeed(String feedString, String clusterName,
                                              int clusterIndex) throws JAXBException {
        JAXBContext feedContext = JAXBContext.newInstance(Feed.class);
        Feed feedObject =
            (Feed) feedContext.createUnmarshaller().unmarshal(new StringReader(feedString));
        //set the value
        feedObject.getClusters().getClusters().get(clusterIndex).setName(clusterName);
        StringWriter feedWriter = new StringWriter();
        feedContext.createMarshaller().marshal(feedObject, feedWriter);
        return feedWriter.toString().trim();
    }

    public static Cluster getClusterObject(
        String clusterXML) throws JAXBException {
        JAXBContext context = JAXBContext.newInstance(Cluster.class);
        Unmarshaller um = context.createUnmarshaller();
        return (Cluster) um.unmarshal(new StringReader(clusterXML));
    }

    public static List<String> getInstanceFinishTimes(ColoHelper coloHelper, String workflowId)
        throws IOException, JSchException {
        List<String> raw = runRemoteScriptAsSudo(coloHelper.getProcessHelper()
                .getQaHost(), coloHelper.getProcessHelper().getUsername(),
            coloHelper.getProcessHelper().getPassword(),
            "cat /var/log/ivory/application.* | grep \"" + workflowId + "\" | grep " +
                "\"Received\" | awk '{print $2}'",
            coloHelper.getProcessHelper().getUsername(),
            coloHelper.getProcessHelper().getIdentityFile()
        );
        List<String> finalList = new ArrayList<String>();
        for (String line : raw) {
            finalList.add(line.split(",")[0]);

        }
        return finalList;
    }

    public static List<String> getInstanceRetryTimes(ColoHelper coloHelper, String workflowId)
        throws IOException, JSchException {
        List<String> raw = runRemoteScriptAsSudo(coloHelper.getProcessHelper()
                .getQaHost(), coloHelper.getProcessHelper().getUsername(),
            coloHelper.getProcessHelper().getPassword(),
            "cat /var/log/ivory/application.* | grep \"" + workflowId + "\" | grep " +
                "\"Retrying attempt\" | awk '{print $2}'",
            coloHelper.getProcessHelper().getUsername(),
            coloHelper.getProcessHelper().getIdentityFile()
        );
        List<String> finalList = new ArrayList<String>();
        for (String line : raw) {
            finalList.add(line.split(",")[0]);
        }

        return finalList;
    }

    public static void shutDownService(IEntityManagerHelper helper)
        throws IOException, JSchException {
        runRemoteScriptAsSudo(helper.getQaHost(), helper.getUsername(),
            helper.getPassword(), helper.getServiceStopCmd(),
            helper.getServiceUser(), helper.getIdentityFile());
        try {
            TimeUnit.SECONDS.sleep(10);
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        }
    }

    public static void startService(IEntityManagerHelper helper)
        throws IOException, JSchException, AuthenticationException, URISyntaxException {

        //putting shutdown to stop multiple start, to take care of bug https://issues.apache.org/jira/browse/FALCON-442
        shutDownService(helper);
        runRemoteScriptAsSudo(helper.getQaHost(), helper.getUsername(),
            helper.getPassword(), helper.getServiceStartCmd(), helper.getServiceUser(),
            helper.getIdentityFile());
        int statusCode = 0;
        for (int tries = 20; tries > 0; tries--) {
            try {
                statusCode = Util.sendRequest(helper.getHostname(), "get").getCode();
            } catch (IOException e) {
                logger.info(e.getMessage());
            }
            if (statusCode == 200) return;
            try {
                TimeUnit.SECONDS.sleep(5);
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }
        }
        throw new RuntimeException("Service on" + helper.getHostname() + " did not start!");
    }

    public static void restartService(IEntityManagerHelper helper)
        throws IOException, JSchException, AuthenticationException, URISyntaxException {
        logger.info("restarting service for: " + helper.getQaHost());

        shutDownService(helper);
        startService(helper);
    }

    private static List<String> runRemoteScriptAsSudo(String hostName,
                                                      String userName,
                                                      String password,
                                                      String command,
                                                      String runAs,
                                                      String identityFile
    ) throws JSchException, IOException {
        JSch jsch = new JSch();
        Session session = jsch.getSession(userName, hostName, 22);
        // only set the password if its not empty
        if (null != password && !password.isEmpty()) {
            session.setUserInfo(new HardcodedUserInfo(password));
        }
        Properties config = new Properties();
        config.setProperty("StrictHostKeyChecking", "no");
        config.setProperty("UserKnownHostsFile", "/dev/null");
        // only set the password if its not empty
        if (null == password || password.isEmpty()) {
            jsch.addIdentity(identityFile);
        }
        session.setConfig(config);
        session.connect();
        Assert.assertTrue(session.isConnected(), "The session was not connected correctly!");

        List<String> data = new ArrayList<String>();

        ChannelExec channel = (ChannelExec) session.openChannel("exec");
        channel.setPty(true);
        String runCmd;
        if (null == runAs || runAs.isEmpty()) {
            runCmd = "sudo -S -p '' " + command;
        } else {
            runCmd = String.format("sudo su - %s -c '%s'", runAs, command);
        }
        if (userName.equals(runAs)) runCmd = command;
        logger.info(
            "host_name: " + hostName + " user_name: " + userName + " password: " + password +
                " command: " +
                runCmd);
        channel.setCommand(runCmd);
        InputStream in = channel.getInputStream();
        OutputStream out = channel.getOutputStream();
        channel.setErrStream(System.err);
        channel.connect();
        try {
            Thread.sleep(20000);
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        }
        // only print the password if its not empty
        if (null != password && !password.isEmpty()) {
            out.write((password + "\n").getBytes());
            out.flush();
        }

        //save console output to data
        BufferedReader r = new BufferedReader(new InputStreamReader(in));
        String line;
        while (true) {
            while ((line=r.readLine())!=null) {
                logger.debug(line);
                data.add(line);
            }
            if (channel.isClosed()) {
                break;
            }
        }

        byte[] tmp = new byte[1024];
        while (true) {
            while (in.available() > 0) {
                int i = in.read(tmp, 0, 1024);
                if (i < 0) break;
                logger.info(new String(tmp, 0, i));
            }
            if (channel.isClosed()) {
                logger.info("exit-status: " + channel.getExitStatus());
                break;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                logger.info(e.getMessage());
            }
        }

        in.close();
        channel.disconnect();
        session.disconnect();
        out.close();
        return data;
    }

    public static Process getProcessObject(String processData) throws JAXBException {
        JAXBContext context = JAXBContext.newInstance(Process.class);
        Unmarshaller um = context.createUnmarshaller();
        return (Process) um.unmarshal(new StringReader(processData));
    }

    public static Feed getFeedObject(String feedData) throws JAXBException {
        JAXBContext context = JAXBContext.newInstance(Feed.class);
        Unmarshaller um = context.createUnmarshaller();

        return (Feed) um.unmarshal(new StringReader(feedData));
    }

    public static List<String> generateUniqueClusterEntity(List<String> clusterData)
        throws JAXBException {
        List<String> newList = new ArrayList<String>();
        for (String cluster : clusterData) {
            newList.add(generateUniqueClusterEntity(cluster));
        }

        return newList;
    }

    public static void dumpConsumerData(Consumer consumer) {
        logger.info("dumping all queue data:");

        for (HashMap<String, String> data : consumer.getMessageData()) {
            logger.info("*************************************");
            for (String key : data.keySet()) {
                logger.info(key + "=" + data.get(key));
            }
            logger.info("*************************************");
        }
    }

    public static void lateDataReplenish(PrismHelper prismHelper, int interval,
                                         int minuteSkip,
                                         String folderPrefix, String postFix)
        throws IOException {
        List<String> folderPaths = TimeUtil.getMinuteDatesOnEitherSide(interval, minuteSkip);
        logger.info("folderData: " + folderPaths.toString());

        if (postFix != null) {
            for (int i = 0; i < folderPaths.size(); i++)
                folderPaths.set(i, folderPaths.get(i) + postFix);
        }

        Util.createLateDataFolders(prismHelper, folderPaths, folderPrefix);
        Util.copyDataToFolders(prismHelper, folderPrefix, folderPaths,
            OSUtil.NORMAL_INPUT + "_SUCCESS",
            OSUtil.NORMAL_INPUT + "log_01.txt");
    }

    public static void lateDataReplenishWithout_Success(PrismHelper prismHelper, int interval,
                                                        int minuteSkip, String folderPrefix,
                                                        String postFix)
        throws IOException {
        List<String> folderPaths = TimeUtil.getMinuteDatesOnEitherSide(interval, minuteSkip);
        logger.info("folderData: " + folderPaths.toString());

        if (postFix != null) {
            for (int i = 0; i < folderPaths.size(); i++)
                folderPaths.set(i, folderPaths.get(i) + postFix);
        }

        Util.createLateDataFolders(prismHelper, folderPaths, folderPrefix);
        Util.copyDataToFolders(prismHelper, folderPrefix, folderPaths,
            OSUtil.NORMAL_INPUT + "log_01.txt");
    }


    public static void putFileInFolderHDFS(PrismHelper prismHelper, int interval, int minuteSkip,
                                           String folderPrefix, String fileToBePut)
        throws IOException {
        List<String> folderPaths = TimeUtil.getMinuteDatesOnEitherSide(interval, minuteSkip);
        logger.info("folderData: " + folderPaths.toString());

        Util.createLateDataFolders(prismHelper, folderPaths, folderPrefix);

        if (fileToBePut.equals("_SUCCESS"))
            Util.copyDataToFolders(prismHelper, folderPrefix, folderPaths,
                OSUtil.NORMAL_INPUT + "_SUCCESS");

        else
            Util.copyDataToFolders(prismHelper, folderPrefix, folderPaths,
                OSUtil.NORMAL_INPUT + "log_01.txt");

    }

    public static Properties getPropertiesObj(String filename) {
        try {
            Properties properties = new Properties();

            logger.info("filename: " + filename);
            InputStream conf_stream =
                Util.class.getResourceAsStream("/" + filename);
            properties.load(conf_stream);
            conf_stream.close();
            return properties;

        } catch (Exception e) {
            logger.info(e.getStackTrace());
        }
        return null;
    }


    public static String getEnvClusterXML(String filename, String cluster, String prefix)
        throws JAXBException {

        Cluster clusterObject =
            getClusterObject(cluster);
        if ((null == prefix) || prefix.isEmpty())
            prefix = "";
        else prefix = prefix + ".";

        String hcat_endpoint = readPropertiesFile(filename, prefix + "hcat_endpoint");

        //now read and set relevant values
        for (Interface iface : clusterObject.getInterfaces().getInterfaces()) {
            if (iface.getType().equals(Interfacetype.READONLY)) {
                iface.setEndpoint(readPropertiesFile(filename, prefix + "cluster_readonly"));
            } else if (iface.getType().equals(Interfacetype.WRITE)) {
                iface.setEndpoint(readPropertiesFile(filename, prefix + "cluster_write"));
            } else if (iface.getType().equals(Interfacetype.EXECUTE)) {
                iface.setEndpoint(readPropertiesFile(filename, prefix + "cluster_execute"));
            } else if (iface.getType().equals(Interfacetype.WORKFLOW)) {
                iface.setEndpoint(readPropertiesFile(filename, prefix + "oozie_url"));
            } else if (iface.getType().equals(Interfacetype.MESSAGING)) {
                iface.setEndpoint(readPropertiesFile(filename, prefix + "activemq_url"));
            } else if (iface.getType().equals(Interfacetype.REGISTRY)) {
                iface.setEndpoint(hcat_endpoint);
            }
        }

        //set colo name:
        clusterObject.setColo(readPropertiesFile(filename, prefix + "colo"));
        // properties in the cluster needed when secure mode is on
        if (MerlinConstants.IS_SECURE) {
            // get the properties object for the cluster
            org.apache.falcon.entity.v0.cluster.Properties clusterProperties =
                clusterObject.getProperties();
            // add the namenode principal to the properties object
            clusterProperties.getProperties().add(getFalconClusterPropertyObject(
                "dfs.namenode.kerberos.principal",
                readPropertiesFile(filename, prefix + "namenode.kerberos.principal", "none")));

            // add the hive meta store principal to the properties object
            clusterProperties.getProperties().add(getFalconClusterPropertyObject(
                "hive.metastore.kerberos" +
                    ".principal",
                readPropertiesFile(filename, prefix + "hive.metastore.kerberos" +
                    ".principal", "none")
            ));

            // Until oozie has better integration with secure hive we need to send the properites to
            // falcon.
            // hive.metastore.sasl.enabled = true
            clusterProperties.getProperties()
                .add(getFalconClusterPropertyObject("hive.metastore.sasl" +
                    ".enabled", "true"));
            // Only set the metastore uri if its not empty or null.
            if (null != hcat_endpoint && !hcat_endpoint.isEmpty()) {
                //hive.metastore.uris
                clusterProperties.getProperties()
                    .add(getFalconClusterPropertyObject("hive.metastore.uris", hcat_endpoint));
            }
        }


        JAXBContext context = JAXBContext.newInstance(Cluster.class);
        Marshaller m = context.createMarshaller();
        StringWriter writer = new StringWriter();

        m.marshal(clusterObject, writer);
        return writer.toString();
    }

    public static org.apache.falcon.entity.v0.cluster.Property
    getFalconClusterPropertyObject
        (String name, String value) {
        org.apache.falcon.entity.v0.cluster.Property property = new org
            .apache.falcon.entity.v0.cluster.Property();
        property.setName(name);
        property.setValue(value);
        return property;
    }

    public static ENTITY_TYPE getEntityType(String entity) {
        if (
            entity.contains("uri:falcon:process:0.1"))
            return ENTITY_TYPE.PROCESS;
        else if (
            entity.contains("uri:falcon:cluster:0.1"))
            return ENTITY_TYPE.CLUSTER;
        else if (
            entity.contains("uri:falcon:feed:0.1")) {
            return ENTITY_TYPE.FEED;
        }
        return null;
    }

    public static boolean isDefinitionSame(PrismHelper server1, PrismHelper server2,
                                           String entity)
        throws URISyntaxException, IOException, AuthenticationException, JAXBException,
        SAXException {
        return XmlUtil.isIdentical(getEntityDefinition(server1, entity, true),
            getEntityDefinition(server2, entity, true));
    }

    public enum URLS {

        LIST_URL("/api/entities/list"),
        SUBMIT_URL("/api/entities/submit"),
        GET_ENTITY_DEFINITION("/api/entities/definition"),
        DELETE_URL("/api/entities/delete"),
        SCHEDULE_URL("/api/entities/schedule"),
        VALIDATE_URL("/api/entities/validate"),
        SUSPEND_URL("/api/entities/suspend"),
        RESUME_URL("/api/entities/resume"),
        STATUS_URL("/api/entities/status"),
        SUBMIT_AND_SCHEDULE_URL("/api/entities/submitAndSchedule"),
        INSTANCE_RUNNING("/api/instance/running"),
        INSTANCE_STATUS("/api/instance/status"),
        INSTANCE_KILL("/api/instance/kill"),
        INSTANCE_RESUME("/api/instance/resume"),
        INSTANCE_SUSPEND("/api/instance/suspend"),
        PROCESS_UPDATE("/api/entities/update/process"),
        INSTANCE_RERUN("/api/instance/rerun"),
        FEED_UPDATE("/api/entities/update/feed"),
        INSTANCE_SUMMARY("/api/instance/summary");
        private final String url;

        URLS(String url) {
            this.url = url;
        }

        public String getValue() {
            return this.url;
        }
    }


    public static String getPathPrefix(String pathString) {
        return pathString.substring(0, pathString.indexOf("$"));
    }

    public static String getFileNameFromPath(String path) {

        return path.substring(path.lastIndexOf("/") + 1, path.length());
    }


    private static class HardcodedUserInfo implements UserInfo {

        private final String password;

        private HardcodedUserInfo(String password) {
            this.password = password;
        }

        public String getPassphrase() {
            return null;
        }

        public String getPassword() {
            return password;
        }

        public boolean promptPassword(String s) {
            return true;
        }

        public boolean promptPassphrase(String s) {
            return true;
        }

        public boolean promptYesNo(String s) {
            return true;
        }

        public void showMessage(String s) {
            logger.info("message = " + s);
        }
    }


    public static String getMethodType(String url) {
        List<String> postList = new ArrayList<String>();
        postList.add("/entities/validate");
        postList.add("/entities/submit");
        postList.add("/entities/submitAndSchedule");
        postList.add("/entities/suspend");
        postList.add("/entities/resume");
        postList.add("/instance/kill");
        postList.add("/instance/suspend");
        postList.add("/instance/resume");
        postList.add("/instance/rerun");
        for (String item : postList) {
            if (url.toLowerCase().contains(item)) {
                return "post";
            }
        }
        List<String> deleteList = new ArrayList<String>();
        deleteList.add("/entities/delete");
        for (String item : deleteList) {
            if (url.toLowerCase().contains(item)) {
                return "delete";
            }
        }

        return "get";
    }

    public static String prettyPrintXml(final String xmlString) {
        if (xmlString == null) {
            return null;
        }
        try {
            Source xmlInput = new StreamSource(new StringReader(xmlString));
            StringWriter stringWriter = new StringWriter();
            StreamResult xmlOutput = new StreamResult(stringWriter);
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            transformerFactory.setAttribute("indent-number", "2");
            Transformer transformer = transformerFactory.newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            transformer.transform(xmlInput, xmlOutput);
            return xmlOutput.getWriter().toString();
        } catch (TransformerConfigurationException e) {
            return xmlString;
        } catch (TransformerException e) {
            return xmlString;
        }

    }

    public static String prettyPrintJson(final String jsonString) {
        if (jsonString == null) {
            return null;
        }
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        JsonElement json = new JsonParser().parse(jsonString);

        return gson.toJson(json);
    }

    public static String prettyPrintXmlOrJson(final String str) {
        if (str == null) {
            return null;
        }
        String cleanStr = str.trim();
        //taken from http://stackoverflow.com/questions/7256142/way-to-quickly-check-if-string-is-xml-or-json-in-c-sharp
        if (cleanStr.startsWith("{") || cleanStr.startsWith("["))
            return prettyPrintJson(cleanStr);
        if (cleanStr.startsWith("<"))
            return prettyPrintXml(cleanStr);
        logger.warn("The string does not seem to be either json or xml: " + cleanStr);
        return str;
    }

    public static String getEntityDefinition(PrismHelper cluster,
                                             String entity,
                                             boolean shouldReturn) throws
        JAXBException,
        IOException, URISyntaxException, AuthenticationException {
        ENTITY_TYPE type = getEntityType(entity);
        IEntityManagerHelper helper;
        if (ENTITY_TYPE.PROCESS.equals(type))
            helper = cluster.getProcessHelper();
        else if (ENTITY_TYPE.FEED.equals(type))
            helper = cluster.getFeedHelper();
        else
            helper = cluster.getClusterHelper();

        ServiceResponse response = helper.getEntityDefinition(URLS
            .GET_ENTITY_DEFINITION, entity);

        if (shouldReturn)
            AssertUtil.assertSucceeded(response);
        else
            AssertUtil.assertFailed(response);
        String result = response.getMessage();
        Assert.assertNotNull(result);

        return result;
    }
}
