package org.apache.falcon.regression.core.util;

import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.XOozieClient;
import org.testng.log4testng.Logger;

import java.util.ArrayList;
import java.util.List;

public class OozieUtil {

    private static final Logger logger = Logger.getLogger(OozieUtil.class);

    public static OozieClient getClient(String url) {
        return new XOozieClient(url);
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
}
