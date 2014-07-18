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
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.process.Cluster;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Output;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.entity.v0.process.Properties;
import org.apache.falcon.entity.v0.process.Property;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.hadoop.fs.FileSystem;
import org.testng.Assert;

import javax.xml.bind.JAXBException;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class ProcessMerlin extends Process {
    public ProcessMerlin(String processData) {
        this((Process) fromString(EntityType.PROCESS, processData));
    }

    public ProcessMerlin(final Process process) {
        try {
            PropertyUtils.copyProperties(this, process);
        } catch (IllegalAccessException e) {
            Assert.fail("Can't create ClusterMerlin: " + ExceptionUtils.getStackTrace(e));
        } catch (InvocationTargetException e) {
            Assert.fail("Can't create ClusterMerlin: " + ExceptionUtils.getStackTrace(e));
        } catch (NoSuchMethodException e) {
            Assert.fail("Can't create ClusterMerlin: " + ExceptionUtils.getStackTrace(e));
        }
    }

    public Bundle setFeedsToGenerateData(FileSystem fs, Bundle b) {
        Date start = getClusters().getClusters().get(0).getValidity().getStart();
        Format formatter = new SimpleDateFormat("yyyy'-'MM'-'dd'T'HH':'mm'Z'");
        String startDate = formatter.format(start);
        Date end = getClusters().getClusters().get(0).getValidity().getEnd();
        String endDate = formatter.format(end);

        Map<String, FeedMerlin> inpFeeds = getInputFeeds(b);
        for (FeedMerlin feedElement : inpFeeds.values()) {
            feedElement.getClusters().getClusters().get(0).getValidity()
                .setStart(TimeUtil.oozieDateToDate(startDate).toDate());
            feedElement.getClusters().getClusters().get(0).getValidity()
                .setEnd(TimeUtil.oozieDateToDate(endDate).toDate());
            InstanceUtil.writeFeedElement(b, feedElement, feedElement.getName());
        }
        return b;
    }

    public Map<String, FeedMerlin> getInputFeeds(Bundle b) {
        Map<String, FeedMerlin> inpFeeds = new HashMap<String, FeedMerlin>();
        for (Input input : getInputs().getInputs()) {
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

    public final void setProperty(String name, String value) {
        Property p = new Property();
        p.setName(name);
        p.setValue(value);
        if (null == getProperties() || null == getProperties()
            .getProperties() || getProperties().getProperties().size()
            <= 0) {
            Properties props = new Properties();
            props.getProperties().add(p);
            setProperties(props);
        } else {
            getProperties().getProperties().add(p);
        }
    }

    @Override
    public String toString() {
        try {
            StringWriter sw = new StringWriter();
            EntityType.PROCESS.getMarshaller().marshal(this, sw);
            return sw.toString();
        } catch (JAXBException e) {
            throw new RuntimeException(e);
        }
    }

    public void renameClusters(Map<String, String> clusterNameMap) {
        for (Cluster cluster : getClusters().getClusters()) {
            final String oldName = cluster.getName();
            final String newName = clusterNameMap.get(oldName);
            if (!StringUtils.isEmpty(newName)) {
                cluster.setName(newName);
            }
        }
    }

    public void renameFeeds(Map<String, String> feedNameMap) {
        for(Input input : getInputs().getInputs()) {
            final String oldName = input.getFeed();
            final String newName = feedNameMap.get(oldName);
            if (!StringUtils.isEmpty(newName)) {
                input.setFeed(newName);
            }
        }
        for(Output output : getOutputs().getOutputs()) {
            final String oldName = output.getFeed();
            final String newName = feedNameMap.get(oldName);
            if (!StringUtils.isEmpty(newName)) {
                output.setFeed(newName);
            }
        }
    }

    /**
     * Sets unique names for the process
     * @return mapping of old name to new name
     */
    public Map<? extends String, ? extends String> setUniqueName() {
        final String oldName = getName();
        final String newName =  oldName + Util.getUniqueString();
        setName(newName);
        final HashMap<String, String> nameMap = new HashMap<String, String>(1);
        nameMap.put(oldName, newName);
        return nameMap;
    }

}


