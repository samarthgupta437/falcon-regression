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
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.feed.Cluster;
import org.apache.falcon.entity.v0.feed.ClusterType;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.Location;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.log4j.Logger;
import org.testng.Assert;

import javax.xml.bind.JAXBException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

public class FeedMerlin extends Feed {

    private static Logger logger = Logger.getLogger(FeedMerlin.class);

    public FeedMerlin(String entity) throws JAXBException {
        this(InstanceUtil.getFeedElement(entity));
    }

    public FeedMerlin(Feed element) {
        Field[] fields = Feed.class.getDeclaredFields();
        for (Field fld : fields) {
            if ("acl".equals(fld.getName())) {
                this.setACL(element.getACL());
                continue;
            }
            try {
                PropertyUtils.setProperty(this, fld.getName(),
                    PropertyUtils.getProperty(element, fld.getName()));
            } catch (IllegalAccessException e) {
                Assert.fail("Can't create FeedMerlin: " + ExceptionUtils.getStackTrace(e));
            } catch (InvocationTargetException e) {
                Assert.fail("Can't create FeedMerlin: " + ExceptionUtils.getStackTrace(e));
            } catch (NoSuchMethodException e) {
                Assert.fail("Can't create FeedMerlin: " + ExceptionUtils.getStackTrace(e));
            }
        }
    }

    /*
    all Merlin specific operations
     */
    public String getTargetCluster() {
        for (Cluster c : getClusters().getClusters()) {
            if (c.getType() == ClusterType.TARGET)
                return c.getName();
        }
        return "";
    }

    public String insertRetentionValueInFeed(String retentionValue) {
        //insert retentionclause
        getClusters().getClusters().get(0).getRetention()
            .setLimit(new Frequency(retentionValue));

        for (org.apache.falcon.entity.v0.feed.Cluster cluster :
            getClusters().getClusters()) {
            cluster.getRetention().setLimit(new Frequency(retentionValue));
        }
        return toString();
    }

    public String setTableValue(String dBName, String tableName, String pathValue) {
        getTable().setUri("catalog:" + dBName + ":" + tableName + "#" + pathValue);
        //set the value
        return toString();
    }

    @Override
    public String toString() {
        try {
            return InstanceUtil.feedElementToString(this);
        } catch (JAXBException e) {
            e.printStackTrace();
        }
        return null;
    }

    public String getLocation(LocationType locationType) {
        for (Location location : getLocations().getLocations()) {
            if(location.getType() == locationType) {
                return location.getPath();
            }
        }
        Assert.fail("Unexpected locationType: " + locationType);
        return null;
    }

    public void setLocation(LocationType locationType, String feedInputPath) {
        for (Location location : getLocations().getLocations()) {
            if(location.getType() == locationType) {
                location.setPath(feedInputPath);
            }
        }
    }
}