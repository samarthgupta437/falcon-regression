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

import org.apache.commons.io.IOUtils;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.*;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.testng.Assert;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BundleUtil {
    public static Bundle[][] readBundles(String path) throws IOException {

        List<Bundle> bundleSet = getDataFromFolder(path);

        Bundle[][] testData = new Bundle[bundleSet.size()][1];

        for (int i = 0; i < bundleSet.size(); i++) {
            testData[i][0] = bundleSet.get(i);
        }

        return testData;
    }

    public static Bundle readHCatBundle() throws IOException {
        return readBundles("hcat")[0][0];
    }

    public static Bundle getHCat2Bundle() throws IOException {
        return getBundleData("hcat_2")[0];
    }

    public static List<Bundle> getDataFromFolder(String folderPath) throws IOException {

        List<Bundle> bundleList = new ArrayList<Bundle>();
        File[] files;
        try {
            files = Util.getFiles(folderPath);
        } catch (URISyntaxException e) {
            return bundleList;
        }

        List<String> dataSets = new ArrayList<String>();
        String processData = "";
        String clusterData = "";

        for (File file : files) {

            if (!file.getName().contains("svn") && !file.getName().startsWith(".DS")) {
                Util.logger.info("Loading data from path: " + file.getAbsolutePath());
                if (file.isDirectory()) {
                    bundleList.addAll(getDataFromFolder(file.getAbsolutePath()));
                } else {

                    String data = IOUtils.toString(file.toURI());

                    if (data.contains("uri:ivory:process:0.1") ||
                        data.contains("uri:falcon:process:0.1")) {
                        Util.logger.info("data been added to process");
                        processData = data;
                    } else if (data.contains("uri:ivory:cluster:0.1") ||
                        data.contains("uri:falcon:cluster:0.1")) {
                        Util.logger.info("data been added to cluster");
                        clusterData = data;
                    } else if (data.contains("uri:ivory:feed:0.1") ||
                        data.contains("uri:falcon:feed:0.1")) {
                        Util.logger.info("data been added to feed");
                        dataSets.add(data);
                    }
                }
            }

        }
        if (!clusterData.isEmpty() && !dataSets.isEmpty()) {
            bundleList.add(new Bundle(dataSets, processData, clusterData));
        }

        return bundleList;

    }

    public static Bundle[][] readELBundles() throws IOException {
        return readBundles("ELbundle");
    }

    public static Bundle[] getBundleData(String path) throws IOException {

        List<Bundle> bundleSet = getDataFromFolder(path);

        return bundleSet.toArray(new Bundle[bundleSet.size()]);
    }

    public static void submitAllClusters(Bundle... b)
        throws IOException, URISyntaxException, AuthenticationException {
        for (Bundle aB : b) {
            ServiceResponse r = Util.prismHelper.getClusterHelper()
                .submitEntity(Util.URLS.SUBMIT_URL, aB.getClusters().get(0));
            Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        }
    }

    public static String getInputFeedNameFromBundle(Bundle b) throws JAXBException {
        String feedData = getInputFeedFromBundle(b);

        JAXBContext processContext = JAXBContext.newInstance(Feed.class);
        Unmarshaller unmarshaller = processContext.createUnmarshaller();
        Feed feedObject = (Feed) unmarshaller.unmarshal(new StringReader(feedData));

        return feedObject.getName();
    }

    public static String getOutputFeedNameFromBundle(Bundle b) throws JAXBException {
        String feedData = getOutputFeedFromBundle(b);

        JAXBContext processContext = JAXBContext.newInstance(Feed.class);
        Unmarshaller unmarshaller = processContext.createUnmarshaller();
        Feed feedObject = (Feed) unmarshaller.unmarshal(new StringReader(feedData));

        return feedObject.getName();
    }

    public static String getOutputFeedFromBundle(Bundle bundle) throws JAXBException {
        String processData = bundle.getProcessData();

        JAXBContext processContext = JAXBContext.newInstance(
            org.apache.falcon.entity.v0.process.Process.class);
        Unmarshaller unmarshaller = processContext.createUnmarshaller();
        Process processObject = (Process) unmarshaller.unmarshal(new StringReader(processData));

        for (Output output : processObject.getOutputs().getOutputs()) {
            for (String feed : bundle.getDataSets()) {
                if (Util.readDatasetName(feed).equalsIgnoreCase(output.getFeed())) {
                    return feed;
                }
            }
        }
        return null;
    }

    public static String getDatasetPath(Bundle bundle) throws JAXBException {
        JAXBContext jc = JAXBContext.newInstance(Feed.class);

        Unmarshaller u = jc.createUnmarshaller();
        Feed dataElement = (Feed) u.unmarshal((new StringReader(bundle.dataSets.get(0))));
        if (!dataElement.getName().contains("raaw-logs16")) {
            dataElement = (Feed) u.unmarshal(new StringReader(bundle.dataSets.get(1)));
        }

        return dataElement.getLocations().getLocations().get(0).getPath();

    }

    //needs to be rewritten to randomly pick an input feed
    public static String getInputFeedFromBundle(Bundle bundle) throws JAXBException {
        String processData = bundle.getProcessData();

        JAXBContext processContext = JAXBContext.newInstance(Process.class);
        Unmarshaller unmarshaller = processContext.createUnmarshaller();
        Process processObject = (Process) unmarshaller.unmarshal(new StringReader(processData));

        for (Input input : processObject.getInputs().getInputs()) {
            for (String feed : bundle.getDataSets()) {
                if (Util.readDatasetName(feed).equalsIgnoreCase(input.getFeed())) {
                    return feed;
                }
            }
        }
        return null;
    }
}
