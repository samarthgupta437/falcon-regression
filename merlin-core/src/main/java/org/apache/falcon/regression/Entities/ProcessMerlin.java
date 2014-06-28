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
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.entity.v0.process.Properties;
import org.apache.falcon.entity.v0.process.Property;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hive.hcatalog.api.HCatClient;
import org.testng.Assert;

import javax.xml.bind.JAXBException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class ProcessMerlin extends Process {
    public ProcessMerlin(String processData)
        throws JAXBException {
        this(InstanceUtil.getProcessElement(processData));
    }

    public ProcessMerlin(Process processObj) {
        Field[] fields = Process.class.getDeclaredFields();
        for (Field fld : fields) {
            try {
                PropertyUtils.setProperty(this, fld.getName(),
                    PropertyUtils.getProperty(processObj, fld.getName()));
            } catch (IllegalAccessException e) {
                Assert.fail("Can't create ProcessMerlin: " + ExceptionUtils.getStackTrace(e));
            } catch (InvocationTargetException e) {
                Assert.fail("Can't create ProcessMerlin: " + ExceptionUtils.getStackTrace(e));
            } catch (NoSuchMethodException e) {
                Assert.fail("Can't create ProcessMerlin: " + ExceptionUtils.getStackTrace(e));
            }
        }
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
            return InstanceUtil.processToString(this);
        } catch (JAXBException e) {
            e.printStackTrace();
        }
        return null;
    }

    public Bundle setFeedsToGenerateData(FileSystem fs, Bundle b) throws Exception {
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

    public Map<String, FeedMerlin> getInputFeeds(Bundle b) throws Exception {
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

}


