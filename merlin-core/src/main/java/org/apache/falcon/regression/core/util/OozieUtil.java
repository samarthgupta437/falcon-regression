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

import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.oozie.client.AuthOozieClient;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.CoordinatorJob;
import org.joda.time.DateTime;
import org.testng.log4testng.Logger;

import javax.xml.bind.JAXBException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

public class OozieUtil {

  private static final Logger logger = Logger.getLogger(OozieUtil.class);

    public static AuthOozieClient getClient(String url) {
        return new AuthOozieClient(url);
  }

  public static List<BundleJob> getBundles(OozieClient client, String filter, int start, int len)
    throws OozieClientException {
    return client.getBundleJobsInfo(filter, start, len);
  }

  public static List<String> getBundleIds(OozieClient client, String filter, int start, int len)
    throws OozieClientException {
    List<BundleJob> bundles = getBundles(client, filter, start, len);
    return getBundleIds(bundles);
  }

  public static List<String> getBundleIds(List<BundleJob> bundles) {
    List<String> ids = new ArrayList<String>();
    for (BundleJob bundle : bundles) {
      logger.info("Bundle Id: " + bundle.getId());
      ids.add(bundle.getId());
    }
    return ids;
  }

  public static List<Job.Status> getBundleStatuses(OozieClient client, String filter, int start, int len) throws OozieClientException {
    List<BundleJob> bundles = getBundles(client, filter, start, len);
    return getBundleStatuses(bundles);
  }

  public static List<Job.Status> getBundleStatuses(List<BundleJob> bundles) {
    List<Job.Status> statuses = new ArrayList<Job.Status>();
    for (BundleJob bundle : bundles) {
      logger.info("Bundle Id: " + bundle.getId());
      statuses.add(bundle.getStatus());
    }
    return statuses;
  }

  public static String getMaxId(List<String> ids) {
    String oozieId = ids.get(0);
    int maxInt = Integer.valueOf(oozieId.split("-")[0]);
    for (int i = 1; i < ids.size(); i++) {
      String currentId = ids.get(i);
      int currInt = Integer.valueOf(currentId.split("-")[0]);
      if (currInt > maxInt) {
        oozieId = currentId;
      }
    }
    return oozieId;
  }

  public static String getMinId(List<String> ids) {
    String oozieId = ids.get(0);
    int minInt = Integer.valueOf(oozieId.split("-")[0]);
    for (int i = 1; i < ids.size(); i++) {
      String currentId = ids.get(i);
      int currInt = Integer.valueOf(currentId.split("-")[0]);
      if (currInt < minInt) {
        oozieId = currentId;
      }
    }
    return oozieId;
  }

  public static List<CoordinatorJob> getAllCoordIds(ColoHelper cluster, String entityData) throws JAXBException, OozieClientException {

    List<String> bundleIds = Util.getBundles(cluster.getFeedHelper()
      .getOozieClient(), Util.readEntityName(entityData),
      Util.getEntityType(entityData));
    List<CoordinatorJob> coords = new ArrayList<CoordinatorJob>();
    for(String bundleID : bundleIds) {

      coords.addAll(cluster.getClusterHelper().getOozieClient().getBundleJobInfo
        (bundleID).getCoordinators());
    }

    return coords ;
  }

  public static String addMinsToTime(DateTime time, int difference) throws ParseException {
    return InstanceUtil.addMinsToTime(InstanceUtil.dateToOozieDate(time.toDate()),
      difference
    );
  }


}
