package com.inmobi.qa.falcon;
//package com.inmobi.qa.airavatqa;
//
//import java.lang.reflect.Method;
//import java.util.ArrayList;
//import java.util.List;
//
//import org.joda.time.DateTime;
//import org.testng.annotations.BeforeClass;
//import org.testng.annotations.BeforeMethod;
//
//import com.inmobi.qa.ivory.bundle.Bundle;
//import com.inmobi.qa.ivory.interfaces.EntityHelperFactory;
//import com.inmobi.qa.ivory.interfaces.IEntityManagerHelper;
//import com.inmobi.qa.ivory.supportClasses.ENTITY_TYPE;
//import com.inmobi.qa.ivory.util.Util;
//import com.inmobi.qa.ivory.util.instanceUtil;
//
//
//
//public class ignoreFeedTest {
//
//	
//	
// @BeforeClass(alwaysRun=true)
//public void createTestData() throws Exception
//{
//
//	Util.print("in @BeforeClass");
//	
//	System.setProperty("java.security.krb5.realm", "");
//	System.setProperty("java.security.krb5.kdc", "");
//	
//	
//	Bundle b = new Bundle();
//	b = (Bundle)Util.readELBundles()[0][0];
//	b.generateUniqueBundle();
////	String startDate = "2010-01-01T20:00Z";
////	String endDate = "2010-01-03T01:04Z";
//
//	
//	String startDate = instanceUtil.getTimeWrtSystemTime(-200);
//	String endDate = instanceUtil.getTimeWrtSystemTime(100);
//	
//	b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
//	String prefix = b.getFeedDataPathPrefix();
//	Util.HDFSCleanup(prefix.substring(1));
//	
//	DateTime startDateJoda = new DateTime(instanceUtil.oozieDateToDate(startDate));
//	DateTime endDateJoda = new DateTime(instanceUtil.oozieDateToDate(endDate));
//
//	List<String> dataDates = Util.getMinuteDatesOnEitherSide(startDateJoda,endDateJoda,1);
//
//	for(int i = 0 ; i < dataDates.size(); i++)
//		dataDates.set(i, prefix + dataDates.get(i));
//	
//	ArrayList<String> dataFolder = new ArrayList<String>();
//	
//	for(int i = 0 ; i < dataDates.size(); i++)
//		dataFolder.add(dataDates.get(i));
//	
//	instanceUtil.putDataInFolders(dataFolder);
//	
//	
//	
//	String prefixIgnore = "/ignoreFeed";
//	for(int i = 0 ; i < dataDates.size(); i++)
//		dataDates.set(i, prefixIgnore + dataDates.get(i));
//	
//	ArrayList<String> dataFolderIgnore = new ArrayList<String>();
//	
//	for(int i = 0 ; i < dataDates.size(); i++)
//		dataFolderIgnore.add(dataDates.get(i));
//	
//	//instanceUtil.putDataInFolders(dataFolderIgnore);
//	
//	
//	String prefixIgnore2 = "/ignoreFeed2";
//	for(int i = 0 ; i < dataDates.size(); i++)
//		dataDates.set(i, prefixIgnore2 + dataDates.get(i));
//	
//	ArrayList<String> dataFolderIgnore2 = new ArrayList<String>();
//	
//	for(int i = 0 ; i < dataDates.size(); i++)
//		dataFolderIgnore2.add(dataDates.get(i));
//	
//	instanceUtil.putDataInFolders(dataFolderIgnore2);
//	
//}
//
//
//@BeforeMethod(alwaysRun=true)
//public void testName(Method method)
//{
//	Util.print("test name: "+method.getName());
//}
//IEntityManagerHelper processHelper=EntityHelperFactory.getEntityHelper(ENTITY_TYPE.PROCESS);
//
//
//
//
///*@Test
//public void testIgnoredFeed() throws Exception
//{
//	Bundle b = new Bundle();
//
//	try{
//
//		b = (Bundle)Util.readIgnoreBundles()[0][0];
//		b.generateUniqueBundle();
//		
//		
//		b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
//		
//		b.setProcessValidity(instanceUtil.getTimeWrtSystemTime(-3),instanceUtil.getTimeWrtSystemTime(20));
//		b.setProcessPeriodicity(FrequencyType.MINUTES,5);
//		b.setOutputFeedPeriodicity("minutes","5");
//		b.submitAndScheduleBundle();
//		
//		Thread.sleep(15000);			
//		
//		ArrayList<String> lateData = instanceUtil.getInputFoldersForInstance(Util.getProcessName(b.getProcessData()), 0, 1);
//		
//		//instanceUtil.putDataInFolders(lateData,"late");
//
//		
//		ArrayList<String> lateData2 = new ArrayList<String>();
//		
//		for(int i =0 ; i < lateData.size() ; i++)
//		{
//			lateData2.add(lateData.get(i).substring(0,lateData.get(i).indexOf("/samarthData/"))+"/ignoreFeed"+lateData.get(i).substring(lateData.get(i).indexOf("/samarthData/")));
//		}
//		
//		
//		instanceUtil.putDataInFolders(lateData2,"late");
//
//	}
//	finally{
//		b.deleteBundle();
//	}
//	
//}
//	*/
//}
