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

package org.apache.falcon.regression.Entities;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.generated.process.Input;
import org.apache.falcon.regression.core.generated.process.Process;
import org.apache.falcon.regression.core.generated.process.Properties;
import org.apache.falcon.regression.core.generated.process.Property;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hive.hcatalog.api.HCatClient;

import javax.xml.bind.JAXBException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class ProcessMerlin extends org.apache.falcon.regression.core.generated
        .process.Process {
    public ProcessMerlin(String processData)
    throws JAXBException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        Process element = InstanceUtil.getProcessElement(processData);
        Field[] fields = Process.class.getDeclaredFields();
        for (Field fld : fields) {
            PropertyUtils.setProperty(this, fld.getName(),
                    PropertyUtils.getProperty(element, fld.getName()));
        }
    }

    public final void setProperty(String name, String value) {
        Property p = new Property();
        p.setName(name);
        p.setValue(value);
        if (null == getProperties() || null == getProperties()
                .getProperty() || getProperties().getProperty().size()
                <= 0) {
            Properties props = new Properties();
            props.addProperty(p);
            setProperties(props);
        } else {
            getProperties().getProperty().add(p);
        }
    }

    @Override
    public String toString() {
        try {
            return InstanceUtil.processToString(this);
        } catch (JAXBException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void generateData(FileSystem fs, Bundle b, String... copyFrom) throws Exception {
        Bundle b1 = new Bundle(b);
        b1 = setFeedsToGenerateData(fs, b1);
        Map<String, FeedMerlin> inpFeeds = getInputFeeds(b1);
        for (FeedMerlin feedElement : inpFeeds.values()) {
            feedElement.generateData(fs, copyFrom);
        }
    }

    public void generateData(FileSystem fs, Bundle b, HCatClient client, String... copyFrom)
    throws Exception {
        Bundle b1 = new Bundle(b);
        b1 = setFeedsToGenerateData(fs, b1);
        Map<String, FeedMerlin> inpFeeds = getInputFeeds(b1);
        for (FeedMerlin feedElement : inpFeeds.values()) {
            feedElement.generateData(client, fs, copyFrom);
        }
    }

    public Bundle setFeedsToGenerateData(FileSystem fs, Bundle b) throws Exception {
        Date start = getClusters().getCluster().get(0).getValidity().getStart();
        Format formatter = new SimpleDateFormat("yyyy'-'MM'-'dd'T'HH':'mm'Z'");
        String startDate = formatter.format(start);
        Date end = getClusters().getCluster().get(0).getValidity().getEnd();
        String endDate = formatter.format(end);

        Map<String, FeedMerlin> inpFeeds = getInputFeeds(b);
        for (FeedMerlin feedElement : inpFeeds.values()) {
            feedElement.getClusters().getCluster().get(0).getValidity()
                    .setStart(InstanceUtil.oozieDateToDate(startDate).toDate());
            feedElement.getClusters().getCluster().get(0).getValidity()
                    .setEnd(InstanceUtil.oozieDateToDate(endDate).toDate());
            InstanceUtil.writeFeedElement(b, feedElement, feedElement.getName());
        }
        return b;
    }

    public Map<String, FeedMerlin> getInputFeeds(Bundle b) throws Exception {
        Map<String, FeedMerlin> inpFeeds = new HashMap<String, FeedMerlin>();
        for (Input input : getInputs().getInput()) {
            for (String feed : b.getDataSets()) {
                if (Util.readDatasetName(feed).equalsIgnoreCase(input.getFeed())) {
                    FeedMerlin feedO = new FeedMerlin(feed);
                    inpFeeds.put(Util.readDatasetName(feed), feedO);
                    break;
                }
            }
        }
        return inpFeeds;
    }

}


