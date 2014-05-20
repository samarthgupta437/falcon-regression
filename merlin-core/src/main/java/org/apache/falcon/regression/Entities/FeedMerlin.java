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
import org.apache.falcon.regression.core.enumsAndConstants.FEED_TYPE;
import org.apache.falcon.regression.core.generated.dependencies.Frequency;
import org.apache.falcon.regression.core.generated.feed.Cluster;
import org.apache.falcon.regression.core.generated.feed.ClusterType;
import org.apache.falcon.regression.core.generated.feed.Feed;
import org.apache.falcon.regression.core.generated.feed.Location;
import org.apache.falcon.regression.core.generated.feed.LocationType;
import org.apache.falcon.regression.core.util.HCatUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import javax.xml.bind.JAXBException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class FeedMerlin extends Feed {

    private static Logger logger = Logger.getLogger(FeedMerlin.class);

    public FeedMerlin(String entity)
        throws JAXBException, NoSuchMethodException, InvocationTargetException,
        IllegalAccessException {
        this(InstanceUtil.getFeedElement(entity));
    }

    public FeedMerlin(Feed element)
        throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        Field[] fields = Feed.class.getDeclaredFields();
        for (Field fld : fields) {
            if ("acl".equals(fld.getName())) {
                this.setACL(element.getACL());
                continue;
            }
            PropertyUtils.setProperty(this, fld.getName(),
                PropertyUtils.getProperty(element, fld.getName()));
        }
    }

    /*
    all Merlin specific operations
     */
    public String getTargetCluster() {
        for (Cluster c : getClusters().getCluster()) {
            if (c.getType().equals(ClusterType.TARGET))
                return c.getName();
        }
        return "";
    }

    public String insertRetentionValueInFeed(String retentionValue) {
        //insert retentionclause
        getClusters().getCluster().get(0).getRetention()
            .setLimit(new Frequency(retentionValue));

        for (org.apache.falcon.regression.core.generated.feed.Cluster cluster :
            getClusters().getCluster()) {
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

    public void setLocation(LocationType lacationType, String feedInputPath) {
        for (Location location : getLocations().getLocation()) {
            if(location.getType() == lacationType) {
                location.setPath(feedInputPath);
            }
        }
    }
}