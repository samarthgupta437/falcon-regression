package com.inmobi.qa.falcon.util;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import com.inmobi.qa.falcon.bundle.Bundle;
import com.inmobi.qa.falcon.generated.feed.Feed;
import com.inmobi.qa.falcon.generated.feed.Retention;
import com.inmobi.qa.falcon.generated.feed.Validity;
import org.apache.commons.lang.StringUtils;
import org.testng.Assert;

import com.inmobi.qa.falcon.generated.feed.ClusterType;
import com.inmobi.qa.falcon.response.ServiceResponse;
/**
 * 
 * @author samarth.gupta
 *
 */
public class prismUtil {

	String cluster2Data = null;
	public static void verifyClusterSubmission(ServiceResponse r, String clusterData, String env, String expectedStatus,ArrayList<String> beforeSubmit, ArrayList<String> afterSubmit) throws Exception
	{

		if(expectedStatus.equals("SUCCEEDED"))
		{
			Assert.assertEquals(r.getMessage().toString().contains("SUCCEEDED"),true,"submit cluster status was not succeeded");
			compareDataStoreStates(beforeSubmit,afterSubmit,Util.readClusterName(clusterData));
			Assert.assertEquals(StringUtils.countMatches(r.getMessage().toString(),"(cluster)"),1,"exactly one colo cluster submission should have been there");
		}
	}

	public static void compareDataStoreStates(ArrayList<String> initialState,ArrayList<String> finalState,String filename) throws Exception
	{
		finalState.removeAll(initialState)  ;

		Assert.assertEquals(finalState.size(),1);
		Assert.assertTrue(finalState.get(0).contains(filename));

	}

	public static void compareDataStoreStates(ArrayList<String> initialState,
			ArrayList<String> finalState, String filename, int expectedDiff) {

		if(expectedDiff>-1){
			finalState.removeAll(initialState)  ;
			Assert.assertEquals(finalState.size(),expectedDiff);
			if(expectedDiff!=0)
				Assert.assertTrue(finalState.get(0).contains(filename));
		}
		else
		{
			expectedDiff = expectedDiff*-1;
			initialState.removeAll(finalState)  ;
			Assert.assertEquals(initialState.size(),expectedDiff);
			if(expectedDiff!=0)
				Assert.assertTrue(initialState.get(0).contains(filename));
		}


	}

	public static void compareDataStoreStates(ArrayList<String> initialState,
			ArrayList<String> finalState, int expectedDiff) {

		if(expectedDiff>-1){
			finalState.removeAll(initialState)  ;
			Assert.assertEquals(finalState.size(),expectedDiff);

		}
		else
		{
			expectedDiff = expectedDiff*-1;
			initialState.removeAll(finalState)  ;
			Assert.assertEquals(initialState.size(),expectedDiff);

		}


	}

	private com.inmobi.qa.falcon.generated.cluster.Cluster getClusterElement(
			Bundle bundle) throws Exception {
		JAXBContext jc=JAXBContext.newInstance(com.inmobi.qa.falcon.generated.cluster.Cluster.class);
		Unmarshaller u=jc.createUnmarshaller();
		return (com.inmobi.qa.falcon.generated.cluster.Cluster)u.unmarshal((new StringReader(bundle.getClusterData())));
	}



	public Bundle setFeedCluster(Validity v1,Retention r1,String n1,ClusterType t1,Validity v2,Retention r2,String n2,ClusterType t2) throws Exception
	{
		Bundle bundle = (Bundle)Util.readELBundles()[0][0];
		bundle.generateUniqueBundle();
		com.inmobi.qa.falcon.generated.feed.Cluster c1 = new com.inmobi.qa.falcon.generated.feed.Cluster();
		c1.setName(n1);
		c1.setRetention(r1);
		c1.setType(t1);
		c1.setValidity(v1);

		com.inmobi.qa.falcon.generated.feed.Cluster c2 = new com.inmobi.qa.falcon.generated.feed.Cluster();
		c2.setName(n2);
		c2.setRetention(r2);
		c2.setType(t2);
		c2.setValidity(v2);

		if(v1.getStart().equals("allNull"))
			c1=c2=null;

		com.inmobi.qa.falcon.generated.cluster.Cluster clusterElement = getClusterElement(bundle);
		clusterElement.setName(n1);
		writeClusterElement(bundle, clusterElement);

		Feed feedElement = getFeedElement(bundle);
		feedElement.getClusters().getCluster().set(0, c1);
		feedElement.getClusters().getCluster().add(c2);
		writeFeedElement(bundle,feedElement);


		clusterElement.setName(n2);
		JAXBContext jc=JAXBContext.newInstance(com.inmobi.qa.falcon.generated.cluster.Cluster.class);
		java.io.StringWriter sw = new StringWriter();
		Marshaller marshaller = jc.createMarshaller();
		marshaller.marshal(clusterElement,sw);
		System.out.println("modified cluster 2 is: "+sw);
		cluster2Data = sw.toString();
		return bundle;
	}
	private void writeClusterElement(Bundle bundle,
			com.inmobi.qa.falcon.generated.cluster.Cluster clusterElement) throws Exception{
		JAXBContext jc=JAXBContext.newInstance(com.inmobi.qa.falcon.generated.cluster.Cluster.class);
		java.io.StringWriter sw = new StringWriter();
		Marshaller marshaller = jc.createMarshaller();
		marshaller.marshal(clusterElement,sw);
		System.out.println("modified cluster is: "+sw);
		bundle.setClusterData(sw.toString());
	}

	public Feed getFeedElement(Bundle bundle) throws Exception
	{
		JAXBContext jc=JAXBContext.newInstance(Feed.class); 
		Unmarshaller u=jc.createUnmarshaller();
		return (Feed)u.unmarshal((new StringReader(bundle.getDataSets().get(0))));
	}

	public void writeFeedElement(Bundle bundle, Feed feedElement) throws Exception
	{
		JAXBContext jc=JAXBContext.newInstance(Feed.class); 
		java.io.StringWriter sw = new StringWriter();
		Marshaller marshaller = jc.createMarshaller();
		marshaller.marshal(feedElement,sw);
		List<String> bundleData = new ArrayList<String>();
		System.out.println("modified dataset is: "+sw);
		bundleData.add(sw.toString());
		bundleData.add(bundle.getDataSets().get(1));
		bundle.setDataSets(bundleData);
	}

}
