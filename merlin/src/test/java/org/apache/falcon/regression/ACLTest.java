package org.apache.falcon.regression;

import org.apache.commons.httpclient.HttpStatus;
import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.enumsAndConstants.MerlinConstants;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.KerberosHelper;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.oozie.client.OozieClient;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

public class ACLTest extends BaseTestClass{
    private static final Logger logger = Logger.getLogger(ACLTest.class);

    ColoHelper cluster = servers.get(0);
    FileSystem clusterFS = serverFS.get(0);
    OozieClient clusterOC = serverOC.get(0);
    String baseTestDir = baseHDFSDir + "/ACLTest";
    String aggregateWorkflowDir = baseTestDir + "/aggregator";

    @BeforeClass(alwaysRun = true)
    public void uploadWorkflow() throws Exception {
        HadoopUtil.uploadDir(clusterFS, aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setup(Method method) throws Exception {
        logger.info("test name: " + method.getName());
        Bundle bundle = BundleUtil.readELBundles()[0][0];
        bundles[0] = new Bundle(bundle, cluster);
        bundles[0].generateUniqueBundle();
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
    }

    @Test(enabled=true)
    public void UpdateFeedACL()throws Exception{
        //submit feed as current user
        bundles[0].submitClusters(prism);
        bundles[0].submitFeed();
        String feed =bundles[0].getDataSets().get(0);
        FeedMerlin feedElement = new FeedMerlin(feed);

        //U1 tries to change ACL owner from U1 to a random user
        feedElement.getACL().setOwner("anythingRandom");
        ServiceResponse serviceResponse = prism.getFeedHelper().update(feed, feedElement.toString(),
                TimeUtil.getTimeWrtSystemTime(0),
                MerlinConstants.CURRENT_USER_NAME);
        AssertUtil.assertFailedWithStatus(serviceResponse, HttpStatus.SC_BAD_REQUEST,
                "A user cannot change the ACL of the feed");

        //change user and let new user try updating ACL
        KerberosHelper.loginFromKeytab(MerlinConstants.USER2_NAME);

        //U2 changes ACL owner from U1 to U2
        feedElement.getACL().setOwner(MerlinConstants.USER2_NAME);
        serviceResponse = prism.getFeedHelper().update(feed, feedElement.toString(),
                TimeUtil.getTimeWrtSystemTime(0),
                MerlinConstants.USER2_NAME);
        AssertUtil.assertFailedWithStatus(serviceResponse, HttpStatus.SC_BAD_REQUEST,
                "Feed submitted by first user should not be updated by second user");

        //U2 changes ACL owner from U1 to a third random user
        feedElement.getACL().setOwner("anythingRandom");
        serviceResponse = prism.getFeedHelper().update(feed, feedElement.toString(),
                TimeUtil.getTimeWrtSystemTime(0),
                MerlinConstants.USER2_NAME);
        AssertUtil.assertFailedWithStatus(serviceResponse, HttpStatus.SC_BAD_REQUEST,
                "Feed submitted by first user should not be updated by second user");
  }
}
