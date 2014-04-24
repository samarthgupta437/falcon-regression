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

import org.apache.falcon.regression.core.enumsAndConstants.ENTITY_TYPE;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.helpers.PrismHelper;
import org.apache.oozie.client.AuthOozieClient;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.XOozieClient;
import org.joda.time.DateTime;
import org.apache.log4j.Logger;
import org.joda.time.DateTimeZone;
import org.testng.Assert;

import javax.xml.bind.JAXBException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class OozieUtil {

  private static final Logger logger = Logger.getLogger(OozieUtil.class);

    public static AuthOozieClient getClient(String url) {
        return new AuthOozieClient(url);
  }

  public static List<BundleJob> getBundles(OozieClient client, String filter, int start, int len)
    throws OozieClientException {
    logger.info("Connecting to oozie: " + client.getOozieUrl());
    return client.getBundleJobsInfo(filter, start, len);
  }

  public static List<String> getBundleIds(OozieClient client, String filter, int start, int len)
    throws OozieClientException {
    logger.info("Connecting to oozie: " + client.getOozieUrl());
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
      logger.info("bundle: " + bundle);
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

    List<String> bundleIds = getBundles(cluster.getFeedHelper()
                    .getOozieClient(), Util.readEntityName(entityData),
            Util.getEntityType(entityData)
    );
    List<CoordinatorJob> coords = new ArrayList<CoordinatorJob>();
    for(String bundleID : bundleIds) {

      coords.addAll(cluster.getClusterHelper().getOozieClient().getBundleJobInfo
        (bundleID).getCoordinators());
    }

    return coords ;
  }

  public static String addMinsToTime(DateTime time, int difference) throws ParseException {
    return TimeUtil.addMinsToTime(TimeUtil.dateToOozieDate(time.toDate()),
            difference
    );
  }

    /**
     *
     * @param bundleID
     * @param oozieClient
     * @return list of action ids of the succeeded retention workflow
     * @throws OozieClientException
     * @throws InterruptedException
     */
    public static List<String> waitForRetentionWorkflowToSucceed(String bundleID, OozieClient oozieClient)
    throws OozieClientException, InterruptedException {
        logger.info("Connecting to oozie: " + oozieClient.getOozieUrl());
        List<String> jobIds = new ArrayList<String>();
        logger.info("using bundleId:" + bundleID);
        waitForCoordinatorJobCreation(oozieClient, bundleID);
        final String coordinatorId = oozieClient.getBundleJobInfo(bundleID).getCoordinators().get(0).getId();
        logger.info("using coordinatorId: " + coordinatorId);

        for(int i=0; i < 120 && oozieClient.getCoordJobInfo(coordinatorId).getActions().isEmpty(); ++i) {
            Thread.sleep(4000);
        }
        Assert.assertFalse(oozieClient.getCoordJobInfo(coordinatorId).getActions().isEmpty(),
                "Coordinator actions should have got created by now.");

        final List<CoordinatorAction> actions = oozieClient.getCoordJobInfo(coordinatorId).getActions();
        logger.info("actions: " + actions);

        for (CoordinatorAction action : actions) {
            for(int i=0; i < 180; ++i) {
                CoordinatorAction actionInfo = oozieClient.getCoordActionInfo(action.getId());
                logger.info("actionInfo: " + actionInfo);
                if(actionInfo.getStatus() == CoordinatorAction.Status.SUCCEEDED ||
                        actionInfo.getStatus() == CoordinatorAction.Status.KILLED ||
                        actionInfo.getStatus() == CoordinatorAction.Status.FAILED ) {
                    break;
                }
                Thread.sleep(10000);
            }
            Assert.assertEquals(
                    oozieClient.getCoordActionInfo(action.getId()).getStatus(),
                    CoordinatorAction.Status.SUCCEEDED,
                    "Action did not succeed.");
            jobIds.add(action.getId());

        }

        return jobIds;

    }

    public static void waitForCoordinatorJobCreation(OozieClient oozieClient, String bundleID) throws OozieClientException, InterruptedException {
        logger.info("Connecting to oozie: " + oozieClient.getOozieUrl());
        for(int i=0; i < 60 && oozieClient.getBundleJobInfo(bundleID).getCoordinators().isEmpty(); ++i) {
            Thread.sleep(2000);
        }
        Assert.assertFalse(oozieClient.getBundleJobInfo(bundleID).getCoordinators().isEmpty(),
                "Coordinator job should have got created by now.");
    }

    public static String getBundleStatus(PrismHelper prismHelper, String bundleId)
      throws OozieClientException {
          XOozieClient oozieClient = prismHelper.getClusterHelper().getOozieClient();
      BundleJob bundleJob = oozieClient.getBundleJobInfo(bundleId);
      return bundleJob.getStatus().toString();
    }

    public static Job.Status getOozieJobStatus(OozieClient client, String processName,
                                               ENTITY_TYPE entityType)
      throws OozieClientException, InterruptedException {
      String filter = String.format("name=FALCON_%s_%s", entityType, processName);
      List<Job.Status> statuses = getBundleStatuses(client, filter, 0, 10);
      if (statuses.isEmpty()) {
        return null;
      } else {
        return statuses.get(0);
      }
    }

    public static List<String> getBundles(OozieClient client, String entityName,
                                          ENTITY_TYPE entityType)
      throws OozieClientException {
      String filter = "name=FALCON_" + entityType + "_" + entityName;
      return getBundleIds(client, filter, 0, 10);
    }

    public static List<DateTime> getStartTimeForRunningCoordinators(PrismHelper prismHelper,
                                                                    String bundleID)
      throws OozieClientException {
      List<DateTime> startTimes = new ArrayList<DateTime>();
  
      XOozieClient oozieClient = prismHelper.getClusterHelper().getOozieClient();
      BundleJob bundleJob = oozieClient.getBundleJobInfo(bundleID);
      CoordinatorJob jobInfo;
  
  
      for (CoordinatorJob job : bundleJob.getCoordinators()) {
  
        if (job.getAppName().contains("DEFAULT")) {
          jobInfo = oozieClient.getCoordJobInfo(job.getId());
          for (CoordinatorAction action : jobInfo.getActions()) {
            DateTime temp = new DateTime(action.getCreatedTime(), DateTimeZone.UTC);
            logger.info(temp);
            startTimes.add(temp);
          }
        }
  
        Collections.sort(startTimes);
  
        if (!(startTimes.isEmpty())) {
          return startTimes;
        }
      }
  
      return null;
    }

    public static boolean verifyOozieJobStatus(OozieClient client, String processName,
                                             ENTITY_TYPE entityType, Job.Status expectedStatus)
    throws OozieClientException, InterruptedException {
    for (int seconds = 0; seconds < 20; seconds++) {
      Job.Status status = getOozieJobStatus(client, processName, entityType);
      logger.info("Current status: " + status);
      if (status == expectedStatus) {
        return true;
      }
      TimeUnit.SECONDS.sleep(5);
    }
    return false;
  }

    public static List<String> getMissingDependencies(PrismHelper helper, String bundleID)
    throws OozieClientException {
        BundleJob bundleJob = helper.getClusterHelper().getOozieClient().getBundleJobInfo(bundleID);
    CoordinatorJob jobInfo =
                helper.getClusterHelper().getOozieClient().getCoordJobInfo(
                        bundleJob.getCoordinators().get(0).getId());
    List<CoordinatorAction> actions = jobInfo.getActions();

        if(actions.size() < 1) {
            return null;
        }
        logger.info("conf from event: " + actions.get(0).getMissingDependencies());

    String[] missingDependencies = actions.get(0).getMissingDependencies().split("#");
    return new ArrayList<String>(Arrays.asList(missingDependencies));
  }

    public static List<String> getCoordinatorJobs(PrismHelper prismHelper, String bundleID)
    throws OozieClientException {
    List<String> jobIds = new ArrayList<String>();
        XOozieClient oozieClient = prismHelper.getClusterHelper().getOozieClient();
    BundleJob bundleJob = oozieClient.getBundleJobInfo(bundleID);
    CoordinatorJob jobInfo =
      oozieClient.getCoordJobInfo(bundleJob.getCoordinators().get(0).getId());

    for (CoordinatorAction action : jobInfo.getActions()) {
      jobIds.add(action.getExternalId());
    }


    return jobIds;

  }

    public static Date getNominalTime(PrismHelper prismHelper, String bundleID)
    throws OozieClientException {
        XOozieClient oozieClient = prismHelper.getClusterHelper().getOozieClient();
    BundleJob bundleJob = oozieClient.getBundleJobInfo(bundleID);
    CoordinatorJob jobInfo =
      oozieClient.getCoordJobInfo(bundleJob.getCoordinators().get(0).getId());
    List<CoordinatorAction> actions = jobInfo.getActions();

    return actions.get(0).getNominalTime();

  }

    public static CoordinatorJob getDefaultOozieCoord(PrismHelper prismHelper, String bundleId,
                                                      ENTITY_TYPE type)
    throws OozieClientException {
        XOozieClient client = prismHelper.getClusterHelper().getOozieClient();
    BundleJob bundlejob = client.getBundleJobInfo(bundleId);

    for (CoordinatorJob coord : bundlejob.getCoordinators()) {
      if ((coord.getAppName().contains("DEFAULT") && ENTITY_TYPE.PROCESS
        .equals(type)) || (coord.getAppName().contains("REPLICATION") && ENTITY_TYPE
        .FEED
        .equals(type))) {
        return client.getCoordJobInfo(coord.getId());
      } else {
        logger.info("Desired coord does not exists on "+ client.getOozieUrl());
      }
    }

    return null;
  }

    public static int getNumberOfWorkflowInstances(PrismHelper prismHelper, String bundleId)
      throws OozieClientException {
      return getDefaultOozieCoord(prismHelper, bundleId,
              ENTITY_TYPE.PROCESS).getActions().size();
    }

    public static List<String> getActionsNominalTime(PrismHelper prismHelper,
                                                   String bundleId,
                                                   ENTITY_TYPE type)
    throws OozieClientException {
    List<String> nominalTime = new ArrayList<String>();
    List<CoordinatorAction> actions = getDefaultOozieCoord(prismHelper,
            bundleId, type).getActions();
    for (CoordinatorAction action : actions) {
      nominalTime.add(action.getNominalTime().toString());
    }
    return nominalTime;
  }

    public static boolean isBundleOver(ColoHelper coloHelper, String bundleId)
     throws OozieClientException {
         XOozieClient client = coloHelper.getClusterHelper().getOozieClient();
 
         BundleJob bundleJob = client.getBundleJobInfo(bundleId);
 
         if (bundleJob.getStatus().equals(BundleJob.Status.DONEWITHERROR) ||
                 bundleJob.getStatus().equals(BundleJob.Status.FAILED) ||
                 bundleJob.getStatus().equals(BundleJob.Status.SUCCEEDED) ||
                 bundleJob.getStatus().equals(BundleJob.Status.KILLED)) {
             return true;
         }
 
 
         try {
             TimeUnit.SECONDS.sleep(20);
         } catch (InterruptedException e) {
             logger.error(e.getMessage());
         }
         return false;
     }

    public static void verifyNewBundleCreation(ColoHelper cluster,
                                               String originalBundleId,
                                               List<String>
                                                 initialNominalTimes,
                                               String entity,
                                               boolean shouldBeCreated,
  
                                               boolean matchInstances) throws OozieClientException, ParseException, JAXBException {
      String entityName = Util.readEntityName(entity);
      ENTITY_TYPE entityType = Util.getEntityType(entity);
      String newBundleId = InstanceUtil.getLatestBundleID(cluster, entityName,
        entityType);
      if (shouldBeCreated) {
        Assert.assertTrue(!newBundleId.equalsIgnoreCase(originalBundleId),
          "eeks! new bundle is not getting created!!!!");
        logger.info("old bundleId=" + originalBundleId + " on oozie: " +
          ""+cluster.getProcessHelper().getOozieClient().getOozieUrl());
        logger.info("new bundleId=" + newBundleId + " on oozie: " +
          ""+cluster.getProcessHelper().getOozieClient().getOozieUrl());
        if(matchInstances)
          validateNumberOfWorkflowInstances(cluster,
                  initialNominalTimes, originalBundleId, newBundleId, entityType);
      } else {
        Assert.assertEquals(newBundleId,
          originalBundleId, "eeks! new bundle is getting created!!!!");
      }
    }

    private static void validateNumberOfWorkflowInstances(ColoHelper cluster, List<String> initialNominalTimes, String originalBundleId, String newBundleId, ENTITY_TYPE type) throws OozieClientException, ParseException {
  
      List<String> nominalTimesOriginalAndNew = getActionsNominalTime
              (cluster,
                      originalBundleId, type);
  
      nominalTimesOriginalAndNew.addAll(getActionsNominalTime(cluster,
              newBundleId, type));
  
      initialNominalTimes.removeAll(nominalTimesOriginalAndNew);
  
      if (initialNominalTimes.size() != 0){
        logger.info("Missing instance are : "+ getListElements(initialNominalTimes));
        logger.info("Original Bundle ID   : "+originalBundleId);
        logger.info("New Bundle ID        : "+newBundleId);
  
        Assert.assertFalse(true, "some instances have gone missing after " +
          "update");
      }
    }

    private static String getListElements(List<String> list) {
  
      String concatenated ="";
      for(String curr : list)
        concatenated = concatenated + " , " + curr;
      return concatenated;
    }

    public static String getCoordStartTime(ColoHelper colo, String entity,
                                           int bundleNo) throws JAXBException, OozieClientException, ParseException {
      String bundleID = InstanceUtil.getSequenceBundleID(colo,
              Util.readEntityName(entity), Util.getEntityType(entity), bundleNo);
  
      CoordinatorJob coord = getDefaultOozieCoord(colo, bundleID,
              Util.getEntityType(entity));
  
      return TimeUtil.dateToOozieDate(coord.getStartTime()
      );
    }
}
