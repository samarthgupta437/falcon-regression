/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.inmobi.qa.falcon.prism;


import java.io.StringReader;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.util.Random;
import javax.xml.bind.JAXBContext;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.TestNGException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.inmobi.qa.falcon.bundle.Bundle;
import com.inmobi.qa.falcon.generated.feed.Feed;
import com.inmobi.qa.falcon.generated.feed.LocationType;
import com.inmobi.qa.falcon.helpers.ColoHelper;
import com.inmobi.qa.falcon.helpers.PrismHelper;
import com.inmobi.qa.falcon.supportClasses.Consumer;
import com.inmobi.qa.falcon.util.Util;
import com.inmobi.qa.falcon.util.Util.URLS;

/**
 *
 * @author rishu.mehrotra
 */
public class RetentionTest {
//	@BeforeClass(alwaysRun=true)
	public void createTestData() throws Exception
	{
		Util.restartService(UA1coloHelper.getProcessHelper());
		Util.restartService(UA2ColoHelper.getProcessHelper());
		Util.restartService(UA3ColoHelper.getProcessHelper());
	}

	@BeforeMethod(alwaysRun=true)
	public void testName(Method method)
	{
		Util.print("test name: "+method.getName());
	}


	Consumer consumer;

	PrismHelper prismHelper=new PrismHelper("prism.properties");
	ColoHelper UA1coloHelper=new ColoHelper("mk-qa.config.properties");
	ColoHelper UA2ColoHelper = new ColoHelper("gs1001.config.properties");
    ColoHelper UA3ColoHelper = new ColoHelper("ivoryqa-1.config.properties");


	DateTimeFormatter formatter=DateTimeFormat.forPattern("yyyy/MM/dd/HH/mm");

	@Test(groups={"0.1","0.2","prism"},dataProvider="betterDP",priority=-1)
	public void testRetention(Bundle bundle,String period,String unit,boolean gaps,String dataType) throws Exception
	{
		Bundle ua2Bundle=new Bundle(bundle,UA2ColoHelper);
		try {

			displayDetails(period,unit,gaps,dataType);


			System.setProperty("java.security.krb5.realm", "");
			System.setProperty("java.security.krb5.kdc", ""); 
			String feed=setFeedPathValue(Util.getInputFeedFromBundle(ua2Bundle),getFeedPathValue(dataType));
			feed=Util.insertRetentionValueInFeed(feed,unit+"("+period+")");
			ua2Bundle.getDataSets().remove(Util.getInputFeedFromBundle(ua2Bundle));
			ua2Bundle.getDataSets().add(feed);
			ua2Bundle.generateUniqueBundle();

			ua2Bundle.submitClusters(prismHelper);

			if(Integer.parseInt(period)>0)
			{
				Util.assertSucceeded(prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL,Util.getInputFeedFromBundle(ua2Bundle)));

				replenishData(UA2ColoHelper, dataType, gaps);

				Util.CommonDataRetentionWorkflow(UA2ColoHelper, ua2Bundle,Integer.parseInt(period), unit);
			}
			else
			{    
				Util.assertFailed(prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL,Util.getInputFeedFromBundle(ua2Bundle)));
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
			throw new TestNGException(e.getCause());
		}
		finally {

			finalCheck(ua2Bundle);
		}
	}


	@DataProvider(name="betterDP")
	public Object[][] getTestData(Method m) throws Exception
	{
		Bundle[] bundles=Util.getBundleData("src/test/resources/RetentionBundles/valid/bundle1");
		String [] units=new String[]{"hours","days"};// "minutes","hours","days",
		String [] periods=new String[]{"0","10080","60","8","24"};// "0","10080","60","1","24"  //not using a negative value like -4 since the system does not allow setting it. Should be covered in validation scenarios.
		Boolean [] gaps=new Boolean[]{false,true}; //,true
		String[] dataTypes=new String[]{"daily","yearly","monthly"}; // 

/*		Object[][] testData=new Object[1][5];
		testData[0][0]=bundles[0];
		testData[0][1]=periods[1];
		testData[0][2]=units[0];
		testData[0][3]=gaps[1];
		testData[0][4]=dataTypes[1]; 
*/
		Object[][] testData=new Object[bundles.length*units.length*periods.length*gaps.length*dataTypes.length][5];

		int i=0;

		for(int b=0;b<bundles.length;b++)
		{
			for(int u=0;u<units.length;u++)
			{
				for(int p=0;p<periods.length;p++)
				{
					for(int g=0;g<gaps.length;g++)
					{
						for(int d=0;d<dataTypes.length;d++)
						{
							testData[i][0]=bundles[b];
							testData[i][1]=periods[p];
							testData[i][2]=units[u];
							testData[i][3]=gaps[g];
							testData[i][4]=dataTypes[d];           
							i++;
						}
					}
				}
			}
		}

		return testData;
	}    

	@DataProvider(name="DP")
	public Object[][] getBundles(Method m) throws Exception
	{
		return Util.readBundles("src/test/resources/RetentionBundles/valid/bundle1");
	}

	private String setFeedPathValue(String feed,String pathValue) throws Exception
	{
		JAXBContext feedContext=JAXBContext.newInstance(Feed.class);
		Feed feedObject=(Feed)feedContext.createUnmarshaller().unmarshal(new StringReader(feed));

		//set the value
		for( com.inmobi.qa.falcon.generated.feed.Location location: feedObject.getLocations().getLocation())
		{
			if(location.getType().equals(LocationType.DATA))
			{
				location.setPath(pathValue);
			}
		}

		StringWriter feedWriter =new StringWriter();
		feedContext.createMarshaller().marshal(feedObject,feedWriter);
		return feedWriter.toString();
	}

	private void finalCheck(Bundle bundle) throws Exception
	{
		prismHelper.getFeedHelper().delete(URLS.DELETE_URL,Util.getInputFeedFromBundle(bundle));
		Util.verifyFeedDeletion(Util.getInputFeedFromBundle(bundle),UA2ColoHelper);
	}

	private void displayDetails(String period,String unit,boolean gaps,String dataType) throws Exception
	{
		System.out.println("***********************************************");
		System.out.println("executing for:");
		System.out.println(unit+"("+period+")");
		System.out.println("gaps="+gaps);
		System.out.println("dataType="+dataType);
		System.out.println("***********************************************");
	}

	private void replenishData(PrismHelper prismHelper,String dataType,boolean gap) throws Exception
	{
		int skip=0;

		if(gap)
		{
			Random r=new Random();
			skip=gaps[r.nextInt(gaps.length)];
		}

		if(dataType.equalsIgnoreCase("daily"))
		{
			Util.replenishData(prismHelper,Util.convertDatesToFolders(Util.getDailyDatesOnEitherSide(36, skip),skip));
		}
		else if(dataType.equalsIgnoreCase("yearly"))
		{
			Util.replenishData(prismHelper,Util.getYearlyDatesOnEitherSide(10, skip)); 
		}
		else if(dataType.equalsIgnoreCase("monthly"))
		{
			Util.replenishData(prismHelper,Util.getMonthlyDatesOnEitherSide(30, skip)); 
		}
	}

	private String getFeedPathValue(String dataType) throws Exception
	{
		if(dataType.equalsIgnoreCase("monthly"))
		{
			return "/retention/testFolders/${YEAR}/${MONTH}";
		}
		if(dataType.equalsIgnoreCase("daily"))
		{
			return "/retention/testFolders/${YEAR}/${MONTH}/${DAY}/${HOUR}";
		}
		if(dataType.equalsIgnoreCase("yearly"))
		{
			return "/retention/testFolders/${YEAR}";
		}
		return null;
	}

	final static int [] gaps=new int[]{2,4,5,1};    

}
