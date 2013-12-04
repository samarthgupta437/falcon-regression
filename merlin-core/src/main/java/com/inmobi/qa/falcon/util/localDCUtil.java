package com.inmobi.qa.falcon.util;

//import com.inmobi.qa.ivory.supportClasses.DSTCounts;
//import com.inmobi.qa.ivory.supportClasses.EnhancedDataDetails;

public class localDCUtil {

/*	public static String getHBaseTableName(Impression impression) {

		SimpleDateFormat defaultFormat = new SimpleDateFormat("yyyyMMddHHmm");
		defaultFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

		UUID uuid  = new UUID(impression.getId().getId_high(), impression.getId().getId_low());
		return "IMPRESSION_" +
		defaultFormat.format(new Date(uuid.timestamp()));
	}

	public static void compareImpressionAndImpressonData(AdRR adrr,
			int  impNumber, ImpressionData obj) throws Exception{

		QA_RRLogLineParser q = new QA_RRLogLineParser(adrr);


		Util.print(obj.getAd() + " " + q.getAdmeta().get(impNumber));
		Util.print(obj.getAddn_handset_info() + " " + q.getHandsetExtras());
		Util.print(obj.getContext() + " " + q.getRequestContext());
		Util.print(obj.getImpression() + " " + q.getDpImpression().get(impNumber));
		Util.print(obj.getSource() + " " + q.getSource());
		Util.print(obj.getUser() + " " + q.getUser());




		Assert.assertEquals(obj.getAd(), q.getAdmeta().get(impNumber));
		Assert.assertEquals(obj.getAddn_handset_info(), q.getHandsetExtras());
		Assert.assertEquals(obj.getContext(), q.getRequestContext());
		Assert.assertEquals(obj.getImpression(),  q.getDpImpression().get(impNumber));
		Assert.assertEquals(obj.getSource(), q.getSource());
		Assert.assertEquals(obj.getUser(), q.getUser());
	}*/
/*
	public static String getImpressionOriDataCenter(GUID impression_id) throws Exception {

		return intToDC(LogParserHelper.getDataCenterID(getUUIDFromGUID(impression_id)));

	}

	private static String intToDC(int dataCenterID) throws FileNotFoundException, IOException {
		Properties prop = new Properties();
		prop.load(new FileInputStream("src/main/resources/intToDCMapping.properties"));
		return prop.getProperty(new Integer(dataCenterID).toString());


	}

	public static UUID getUUIDFromGUID(GUID impression_id) {

		return new UUID(impression_id.getId_high(), impression_id.getId_low());
	}

	public static String getImpressionOriDataCenter(ClickRequestResponse r) throws Exception {

		return intToDC(LogParserHelper.getDataCenterID(getUUIDFromGUID(r.getRequest_response().getRequest().getImpression_id())));
	}

	public static String getDestFolderForCRR (
			ClickRequestResponse clickRequestResponse, String processingColo,
			String baseOutputDir)throws Exception {
		QA_ClickLogParser parsedClick = new QA_ClickLogParser();
		parsedClick.setClickLog(clickRequestResponse);

		Util.print("clickTime: "+parsedClick.getClickInfo().getClickTime());
		Util.print("clickTime after LogParsed: "+LogParserHelper.toHour(parsedClick.getClickInfo().getClickTime()));
		Util.print("impressionTIme: "+ LogParserHelper.toHour(localDCUtil.getUUID(clickRequestResponse))); 
		Util.print("impressionTIme time stamp: "+ LogParserHelper.toHour(localDCUtil.getUUID(clickRequestResponse).timestamp()));

		UUID id = UUID.fromString("9c87dda3-0138-1000-e0f7-3fa318590043");

		Util.print("hardcoded ID: "+LogParserHelper.toHour(id));
		Util.print("hardcoded ID ts: "+LogParserHelper.toHour(id.timestamp()));





		UUID uuid = UUID.fromString(parsedClick.clickInfo.getImpression_id()); 
		Util.print("dev code clickInfo.getClickTime(): "+parsedClick.clickInfo.getClickTime());
		Util.print("dev code uuid.timestamp(): "+uuid.timestamp());
		Util.print("impressionTIme: "+ LogParserHelper.toHour(uuid.timestamp()));


		if(!clickRequestResponse.getRequest_response().is_fraudulent)
			return baseOutputDir+"/"+getImpressionOriDataCenter(clickRequestResponse)+"/"+processingColo+"/"+clickRequestResponse.getRequest_response().getAd().getMeta().getPricing()+"/"+LogParserHelper.toHour(localDCUtil.getUUID(clickRequestResponse));

		return baseOutputDir+"/"+getImpressionOriDataCenter(clickRequestResponse)+"/"+processingColo+"/NULL/"+LogParserHelper.toHour(localDCUtil.getUUID(clickRequestResponse));
	}

	public static UUID getUUID(ClickRequestResponse clickRequestResponse) {

		Util.print("QA UUID: "+ new UUID(clickRequestResponse.getRequest_response().getRequest().getImpression_id().getId_high(), clickRequestResponse.getRequest_response().getRequest().getImpression_id().getId_low()));
		return new UUID(clickRequestResponse.getRequest_response().getRequest().getImpression_id().getId_high(), clickRequestResponse.getRequest_response().getRequest().getImpression_id().getId_low());
	}

	public static ArrayList<Path> getAllInputFilesForProcess(
			ColoHelper coloHelper, String processName, int bundleNumber,boolean isGated) throws Exception{

		String bundleID = instanceUtil.getSequenceBundleID(coloHelper,processName,ENTITY_TYPE.PROCESS, bundleNumber);
		String coordID = instanceUtil.getDefaultCoordIDFromBundle(coloHelper,bundleID);
		XOozieClient oozieClient=new XOozieClient(coloHelper.getProcessHelper().getOozieURL());
		CoordinatorJob coordInfo = oozieClient.getCoordJobInfo(coordID);

		ArrayList<Path> returnObject = new ArrayList<Path>();

		for(int i = 0 ; i < coordInfo.getActions().size(); i++){

			ArrayList<String> inputPath = instanceUtil.getInputFoldersForInstance(coloHelper, processName, bundleNumber, i,isGated);
			for(String currentFolder : inputPath)
				returnObject.addAll(hadoopUtil.getAllFilesRecursivelyHDFS(coloHelper,Util.stringToPath(currentFolder) ));
		}

		return returnObject;
	}

	public static int getNumberOFRecordsFromFilesHDFS(ColoHelper coloHelper,
			ArrayList<Path> inputFiles, InputFormats format) throws Exception{

		int totalRecords = 0 ; 



		for(Path p : inputFiles)
		{
			//Util.print("records getting counted for file: "+p.toString());
			ColumnarDataReader impressionReader_click = null;

			if(format!=null)
				impressionReader_click = new ColumnarDataReader(hadoopUtil.getHadoopConfiguration(coloHelper),p,format,null);

			ColumnarDataReader impressionReader_clickEnhanced = new ColumnarDataReader(hadoopUtil.getHadoopConfiguration(coloHelper),p,InputFormats.CLICK_ENHANCED_LOG,null);

			if(format!=null){
				while (impressionReader_click.next()) {
					System.out.println(Arrays.toString(impressionReader_click.getFields()));
					totalRecords++;
				}
				impressionReader_click.close();
			}


			else {

				if(p.toUri().toString().contains("deferred"))
				{
					impressionReader_click = new ColumnarDataReader(hadoopUtil.getHadoopConfiguration(coloHelper),p,InputFormats.CLICK_LOG,null);

					while (impressionReader_click.next()) {
						//System.out.println(Arrays.toString(impressionReader_click.getFields()));
						Arrays.toString(impressionReader_click.getFields());
						totalRecords++;
					}
					impressionReader_click.close();
				}
				else{
					while (impressionReader_clickEnhanced.next()) {
						//Util.print("form file: "+p.toUri().toString());
						//System.out.println(Arrays.toString(impressionReader_clickEnhanced.getFields()));
						Arrays.toString(impressionReader_clickEnhanced.getFields());
						totalRecords++;
					}
					impressionReader_clickEnhanced.close();
				}
			}

		}

		Util.print("total records found in all"+ totalRecords);
		Util.print("total Files: "+inputFiles.size());
		return totalRecords;
	}

	public static int getNumberOfEnhancedRecords(ColoHelper coloHelper,
			String processData, String typeOfReord,DemandSource dst)throws Exception {

		//get output localtion
		String outputLocation = localDCUtil.getOutputFromEnrichmentProcess(processData,typeOfReord);

		ArrayList<Path> allFiles = new ArrayList<Path>();

		if(typeOfReord.equals("ENRICHED"))
			allFiles = hadoopUtil.getAllFilesRecursivelyHDFS(coloHelper, Util.stringToPath(outputLocation),"data/fetlrc/clickenhance/deferred","data/fetlrc/clickenhance/stats","_SUCCESS","data/fetlrc/clickenhance/staging","ifd-clickenhance/stats/");
		else
			allFiles = hadoopUtil.getAllFilesRecursivelyHDFS(coloHelper, Util.stringToPath(outputLocation),"data/fetlrc/clickenhance/staging","_SUCCESS","ifd-clickenhance/stats/");

		if(dst==null)
			return getNumberOFRecordsFromFilesHDFS(coloHelper,allFiles,null);
		else 
			return getNumberOfEnhancedRecordsWithDST(coloHelper,allFiles,dst);

	}

	private static int getNumberOfEnhancedRecordsWithDST(ColoHelper coloHelper,
			ArrayList<Path> allFiles, DemandSource dst) throws Exception{

		int totalRecords = 0 ; 



		for(Path p : allFiles)
		{


			ColumnarDataReader impressionReader_clickEnhanced = new ColumnarDataReader(hadoopUtil.getHadoopConfiguration(coloHelper),p,InputFormats.CLICK_ENHANCED_LOG,null);


			while (impressionReader_clickEnhanced.next()) {
				System.out.println(Arrays.toString(impressionReader_clickEnhanced.getFields()));
				DPClickInfo clickInfo = impressionReader_clickEnhanced.getField(1);
				if(clickInfo.getDemand_source_type().equals(dst))
					totalRecords++;
			}
			impressionReader_clickEnhanced.close();
		}
		return totalRecords;

	}

	public static String getOutputFromEnrichmentProcess(String processData,
			String typeOfReord) throws JAXBException {

		com.inmobi.qa.ivory.generated.process.Process p = instanceUtil.getProcessElement(processData);

		/*	<properties>
			<property name="enhancer.out.path" value="/data/clickenhance" />
			<property name="enhancer.deferred.path" value="/data/clickenhance/deferred" />
			<property name="enhancer.stats.path" value="/data/clickenhance/stats" />
			<property name="enhancer.processing.colo.name" value="lhr1" />
		</properties>

		for(int i = 0 ; i < p.getProperties().getProperty().size(); i++){
			Property  property = p.getProperties().getProperty().get(i);
			if(property.getName().equals("enhancer.out.path") && typeOfReord.equals("ENRICHED"))
				return property.getValue();
			else if(property.getName().equals("enhancer.deferred.path") && typeOfReord.equals("DEFERRED"))
				return property.getValue();
		}
		Util.print("no matching proeprty found");
		return null;
	}

	public static int getNumberOfEnhancedRecordsWithFlag(ColoHelper coloHelper,String processData,
			boolean flag) throws Exception{
		//get output localtion
		String outputLocation = localDCUtil.getOutputFromEnrichmentProcess(processData,"ENRICHED");

		ArrayList<Path> allFiles = new ArrayList<Path>();

		allFiles = hadoopUtil.getAllFilesRecursivelyHDFS(coloHelper, Util.stringToPath(outputLocation),"data/fetlrc/clickenhance/deferred","data/fetlrc/clickenhance/stats","_SUCCESS","data/fetlrc/clickenhance/staging");

		return getNumberOFRecordsFromFilesHDFS_enhancedFlag(coloHelper,allFiles,flag);


	}

	private static int getNumberOFRecordsFromFilesHDFS_enhancedFlag(
			ColoHelper coloHelper, ArrayList<Path> allFiles, boolean isEnriched) throws Exception{

		int totalRecords = 0 ; 



		for(Path p : allFiles)
		{


			ColumnarDataReader impressionReader_clickEnhanced = new ColumnarDataReader(hadoopUtil.getHadoopConfiguration(coloHelper),p,InputFormats.CLICK_ENHANCED_LOG,null);


			while (impressionReader_clickEnhanced.next()) {
				Util.print("form file: "+p.toUri().toString());
				System.out.println(Arrays.toString(impressionReader_clickEnhanced.getFields()));
				DPClickInfo clickInfo = impressionReader_clickEnhanced.getField(1);
				if(isEnriched){
					if(clickInfo.isEnriched())
						totalRecords++;
				}
				else{
					if(!clickInfo.isEnriched())
						totalRecords++;
				}

			}
			impressionReader_clickEnhanced.close();

		}
		return totalRecords;

	}


	private static void insertNonNetworkDataInHDFSClickFile(ColoHelper coloHelper,Path fileHDFSLocaltion,boolean isGz) throws Exception{

		int  maxInsertedRecodrs = 5;
		int minInsertedRecodrs = 1;

		int counter = 0 ; 

		File fileUnzipped =hadoopUtil.getFileFromHDFSFolder(coloHelper, fileHDFSLocaltion.toUri().toString(),"");

		if(isGz){
			File fileZipped =	hadoopUtil.getFileFromHDFSFolder(coloHelper, fileHDFSLocaltion.toUri().toString(),"");
			fileUnzipped = ZipUtil.unzipFileToAnotherFile(fileZipped);
			fileZipped.delete();

		}

		ArrayList<ClickRequestResponse> requestResponse = new ArrayList<ClickRequestResponse>();
		FileReader r = new FileReader(fileUnzipped);
		BufferedReader reader = new BufferedReader (r);
		String line = reader.readLine();

		while(line!=null)
		{


			ClickRequestResponse crrDefault = new ClickRequestResponse();
			Base64 base64 = new Base64();
			TDeserializer ud= new TDeserializer();
			ud.deserialize((TBase) crrDefault,base64.decode(line.getBytes()));


			requestResponse.add(crrDefault);



			if(getRandomBoolean() && (counter==0 || counter%7==0))
			{
				int numberOfRecordsToBeInserted = minInsertedRecodrs + (int)(Math.random() * ((maxInsertedRecodrs - minInsertedRecodrs) + 1));
				for(int i =0 ; i < numberOfRecordsToBeInserted ; i++)
				{
					line = reader.readLine();

					if(line==null)
						break;
					ClickRequestResponse crrTemp = new ClickRequestResponse();
					ClickRequestResponse crr = crrDefault;



					base64 = new Base64();
					ud= new TDeserializer();
					ud.deserialize((TBase) crrTemp,base64.decode(line.getBytes()));
					crr.getRequest_response().getRequest().setImpression_id(crrTemp.getRequest_response().getRequest().getImpression_id());
					crr.getRequest_response().setDemand_source_type(DemandSource.findByValue(1 + (int)(Math.random() * ((4 - 1) + 1))));

					if(crr.getRequest_response().getRequest().getImpression_id()!=null)
						requestResponse.add(crr);


					if(getRandomBoolean()){
						crr.getRequest_response().setDemand_source_type(DemandSource.IFD);

						if(crr.getRequest_response().getRequest().getImpression_id()!=null)
							requestResponse.add(crr);

					}

					if(crrTemp.getRequest_response().getRequest().getImpression_id()!=null)

						requestResponse.add(crrTemp);
				}

			}

			if(line!=null)
				line = reader.readLine();
			else 
				break;
			counter++;
		}

		String fileName = fileUnzipped.getName();
		fileUnzipped.delete();

		hadoopUtil.deleteFile(coloHelper,fileHDFSLocaltion);


		for(int i = 0 ; i < requestResponse.size();i++)
		{
			ClickRequestResponse crrPrint = requestResponse.get(i);
			if(crrPrint.getRequest_response().getRequest().getImpression_id()==null && !crrPrint.getRequest_response().is_fraudulent && !crrPrint.getRequest_response().is_terminated)
			{
				requestResponse.get(i).getRequest_response().setIs_fraudulent(true);
				requestResponse.get(i).getRequest_response().setIs_terminated(true);

			}
		}

		for(int i = 0 ; i < requestResponse.size();i++)
		{
			ClickRequestResponse crrPrint = requestResponse.get(i);
			if(crrPrint.getRequest_response().getRequest().getImpression_id()==null && !crrPrint.getRequest_response().is_fraudulent && !crrPrint.getRequest_response().is_terminated)

				Util.print("bad lines: "+crrPrint.toString());

		}

		Util.WriteToFileCRR(fileName,requestResponse);
		ZipUtil.zipFile(fileName);

		hadoopUtil.copyDataToFolders(coloHelper, "", fileHDFSLocaltion,  fileName+".gz");

		//delete zippedFile
		File f= new File(fileName+".gz");
		f.delete();
		//	hadoopUtil.putGZAdRRInHdfs(fileName, numberOfImpressions, numberOfRequest, ivoryqa1, folderForHDFS)


	}
	public static boolean getRandomBoolean() {
		Random random = new Random();
		return random.nextBoolean();
	}

	public static void putNonNetworkDataInHDFSFolder(ColoHelper coloHelper,
			String inputPath,int skipFile) throws Exception{

		ArrayList<Path> allFiles = hadoopUtil.getAllFilesRecursivelyHDFS(coloHelper, new Path(inputPath));
		int count = 0 ;
		for(Path p: allFiles){
			count++;
			if(count==skipFile){
				localDCUtil.insertNonNetworkDataInHDFSClickFile(coloHelper, p,true);
				count=0;
			}
		}

	}

	public static ArrayList<ClickRequestResponse> getAllNonNetworkRequestFromHDFSRecursively(
			ColoHelper ivoryqa1, Path path) throws Exception{

		ArrayList<Path> allFile = hadoopUtil.getAllFilesRecursivelyHDFS(ivoryqa1, path);
		ArrayList<ClickRequestResponse> returnObject = new ArrayList<ClickRequestResponse>();

		for(Path p :allFile)
			returnObject.addAll(hadoopUtil.getAllnonNetworkRequestFromHDFS(ivoryqa1,p));

		return returnObject;
	}

	public static int getDstClickCountFromFolders(ColoHelper coloHelper,
			ArrayList<Path> baseFolder, DemandSource dst) throws Exception{

		return getCrrFromRawClick(coloHelper,baseFolder,dst).size();
	}

	public static int getDstClickCountFromClickRCFolders(ColoHelper coloHelper,
			ArrayList<Path> outputLocations, DemandSource dst,String postFix) throws Exception{


		int totalRecords = 0 ; 

		ArrayList<Path> allFiles = new ArrayList<Path>();

		for(int i = 0 ;i < outputLocations.size(); i++){
			if(!outputLocations.get(i).toUri().toString().contains("nn-click"))
				outputLocations.set(i,outputLocations.get(i).suffix(postFix));
		}

		for(Path p : outputLocations )
		{
			allFiles.addAll(hadoopUtil.getAllFilesRecursivelyHDFS(coloHelper, p,"META","_SUCCESS"));
		}


		for(Path p : allFiles)
		{
			//Util.print("records getting counted for file: "+p.toString());
			ColumnarDataReader impressionReader_clickEnhanced = new ColumnarDataReader(hadoopUtil.getHadoopConfiguration(coloHelper),p,InputFormats.CLICK_LOG,null);

			while (impressionReader_clickEnhanced.next()) {
				//System.out.println(Arrays.toString(impressionReader_clickEnhanced.getFields()));
				Arrays.toString(impressionReader_clickEnhanced.getFields());
				DPClickInfo c = (DPClickInfo)impressionReader_clickEnhanced.getField(1);
				if(c.getDemand_source_type().equals(dst))
					totalRecords++;

			}
			impressionReader_clickEnhanced.close();
		}

		return totalRecords;
	}

	public static ArrayList<ClickRequestResponse> getAllNonNetworkRequestFromHDFSRecursively(
			ColoHelper ivoryqa1, ArrayList<Path> clickRCInput) throws Exception {

		ArrayList<ClickRequestResponse> returnList = new ArrayList<ClickRequestResponse>();
		for(Path p: clickRCInput)
			returnList.addAll(localDCUtil.getAllNonNetworkRequestFromHDFSRecursively(ivoryqa1, p));

		return returnList;
	}

	public static int getAllNonNetworkRequestFromClickRCFoldersHDFSRecursively(
			ColoHelper coloHelper, ArrayList<Path> folders) throws Exception{


		int totalRecords = 0 ; 

		ArrayList<Path> allFiles = new ArrayList<Path>();

		for(Path p : folders )
		{
			allFiles.addAll(hadoopUtil.getAllFilesRecursivelyHDFS(coloHelper, p,"META","_SUCCESS"));
		}


		for(Path p : allFiles)
		{
			//Util.print("records getting counted for file: "+p.toString());
			ColumnarDataReader impressionReader_clickEnhanced = new ColumnarDataReader(hadoopUtil.getHadoopConfiguration(coloHelper),p,InputFormats.CLICK_LOG,null);

			while (impressionReader_clickEnhanced.next()) {
				System.out.println(Arrays.toString(impressionReader_clickEnhanced.getFields()));

				DPClickInfo c = (DPClickInfo)impressionReader_clickEnhanced.getField(1);
				if(!c.getDemand_source_type().equals(DemandSource.NETWORK))
					totalRecords++;

			}
			impressionReader_clickEnhanced.close();
		}

		return totalRecords;
	}

	public static void verifyEnrichedRecords(ColoHelper coloHelper,
			String processData) {
		// TODO Auto-generated method stub

	}

	public static void printAllRecordsFrom(ColoHelper coloHelper, Path baseFolder,InputFormats format) throws Exception{

		ArrayList<Path> allFiles = new ArrayList<Path>();

		allFiles.addAll(hadoopUtil.getAllFilesRecursivelyHDFS(coloHelper, baseFolder,"META","_SUCCESS","/clickenhance/stats","clickenhance/deferred","clickenhance/deferred-staging","clickenhance/staging"));

		for(Path p : allFiles)
		{
			Util.print("records getting printed from file: "+p.toString());
			ColumnarDataReader impressionReader_clickEnhanced = new ColumnarDataReader(hadoopUtil.getHadoopConfiguration(coloHelper),p,format,null);
			while (impressionReader_clickEnhanced.next()) {
				System.out.println(Arrays.toString(impressionReader_clickEnhanced.getFields()));

			}
		}
	}

	
	
	public static ArrayList<ClickRequestResponse> getCrrFromRawClick(
			ColoHelper coloHelper, ArrayList<Path> baseFolder,
			DemandSource dst)throws Exception {
		ArrayList<ClickRequestResponse> crr = new ArrayList<ClickRequestResponse>();

		ArrayList<Path> allFiles = new ArrayList<Path>();

		for(Path p : baseFolder )
		{
			allFiles.addAll(hadoopUtil.getAllFilesRecursivelyHDFS(coloHelper, p));
		}

		for(Path p : allFiles)
			crr.addAll(hadoopUtil.getAllRequestForDst(coloHelper,p,dst));

		return crr;

	}

	public static Hashtable<String,ClickRequestResponse> putCRRInHashImpressionKey(
			ArrayList<ClickRequestResponse> crrFromInput) {

		Hashtable<String,ClickRequestResponse> h = new Hashtable<String,ClickRequestResponse>();
		int count = 0 ;
		for(ClickRequestResponse c : crrFromInput)
		{
			if(c.getRequest_response().getRequest().getImpression_id()!=null)
				h.put(c.getRequest_response().getRequest().getImpression_id().toString(), c);

			Util.print(""+count++);
		}

		return h;
	}


	public static ArrayList<AdRR> getAdRRFromRawRR(
			ColoHelper coloHelper, ArrayList<Path> baseFolder)throws Exception {
		ArrayList<AdRR> adrr = new ArrayList<AdRR>();

		ArrayList<Path> allFiles = new ArrayList<Path>();

		for(Path p : baseFolder )
		{
			allFiles.addAll(hadoopUtil.getAllFilesRecursivelyHDFS(coloHelper, p));
		}

		for(Path p : allFiles)
			adrr.addAll(hadoopUtil.getAllRRFromHDFS(coloHelper,p));

		return adrr;

	}

	public static DSTCounts getDstClickCountFromFolders(ColoHelper coloHelper,
			ArrayList<Path> folders,String postFix,RawOrRC type) throws Exception {

		DSTCounts returnObject = new DSTCounts();

		for(int i = 0 ;i < folders.size(); i++){
			if(!folders.get(i).toUri().toString().contains("nn-click"))
				folders.set(i,folders.get(i).suffix(postFix));
		}

		ArrayList<Path> allFiles = new ArrayList<Path>();

		for(Path p : folders )
		{
			allFiles.addAll(hadoopUtil.getAllFilesRecursivelyHDFS(coloHelper, p,"META","_SUCCESS"));
		}

		if(RawOrRC.RAW.equals(type))
			for(Path p : allFiles)
				returnObject.add(hadoopUtil.getDSTCountsForFile(coloHelper,p));

		else if(RawOrRC.RC.equals(type))
		{
			for(Path p : allFiles)
			{
				//Util.print("records getting counted for file: "+p.toString());
				ColumnarDataReader impressionReader_clickEnhanced = new ColumnarDataReader(hadoopUtil.getHadoopConfiguration(coloHelper),p,InputFormats.CLICK_LOG,null);

				while (impressionReader_clickEnhanced.next()) {
					
					
					//System.out.println(Arrays.toString(impressionReader_clickEnhanced.getFields()));
					Arrays.toString(impressionReader_clickEnhanced.getFields());
					DPClickInfo c = (DPClickInfo)impressionReader_clickEnhanced.getField(1);
					if(c.getDemand_source_type().equals(DemandSource.CHANNEL_PARTNERSHIP))
						returnObject.incrementChannelPartnership(p);
					else if(c.getDemand_source_type().equals(DemandSource.HOSTED))
						returnObject.incrementHosted(p);
					else if(c.getDemand_source_type().equals(DemandSource.HOUSE))
						returnObject.incrementHouse(p);
					else if(c.getDemand_source_type().equals(DemandSource.IFD))
						returnObject.incrementIFD(p);
					else if(c.getDemand_source_type().equals(DemandSource.NETWORK))
						returnObject.incrementNetwork(p);

				}
				impressionReader_clickEnhanced.close();
			}

		}
		return returnObject;

	}

	public static EnhancedDataDetails getEnhancedDataDetails(ColoHelper coloHelper,
			String processData) throws Exception{


		//get output localtion
		String outputLocationEnriched = localDCUtil.getOutputFromEnrichmentProcess(processData,"ENRICHED");
		String outputLocationDeferred = localDCUtil.getOutputFromEnrichmentProcess(processData,"DEFERRED");

		ArrayList<Path> allFilesEnriched = new ArrayList<Path>();
		ArrayList<Path> allFilesDeferred = new ArrayList<Path>();



		allFilesEnriched = hadoopUtil.getAllFilesRecursivelyHDFS(coloHelper, Util.stringToPath(outputLocationEnriched),"data/fetlrc/clickenhance/deferred","data/fetlrc/clickenhance/stats","_SUCCESS","data/fetlrc/clickenhance/staging","nn-clickenhance/stats/","nn-clickenhance/deferred","nn-clickenhance/staging");

		allFilesDeferred = hadoopUtil.getAllFilesRecursivelyHDFS(coloHelper, Util.stringToPath(outputLocationDeferred),"data/fetlrc/clickenhance/staging","_SUCCESS","nn-clickenhance/stats/","data/fetlrc/clickenhance/stats","nn-clickenhance/staging");

		return getEnhancedDataDetailsFromFiles(coloHelper,allFilesEnriched,allFilesDeferred);



	}

	private static EnhancedDataDetails getEnhancedDataDetailsFromFiles(
			ColoHelper coloHelper, ArrayList<Path> allFilesEnriched,
			ArrayList<Path> allFilesDeferred) throws Exception{


		EnhancedDataDetails returnObject = new EnhancedDataDetails();


		for(Path p : allFilesEnriched)
		{
			//Util.print("records getting counted for file: "+p.toString());

			

			ColumnarDataReader impressionReader_clickEnhanced = new ColumnarDataReader(hadoopUtil.getHadoopConfiguration(coloHelper),p,InputFormats.CLICK_ENHANCED_LOG,null);

			while (impressionReader_clickEnhanced.next()) {
								
				System.out.println(Arrays.toString(impressionReader_clickEnhanced.getFields()));
				DPClickInfo clickInfo = impressionReader_clickEnhanced.getField(1);
				if(clickInfo.getDemand_source_type().equals(DemandSource.CHANNEL_PARTNERSHIP))
					returnObject.incrementChannelPartnership(p);
				else if(clickInfo.getDemand_source_type().equals(DemandSource.HOSTED))
					returnObject.incrementHosted(p);
				else if(clickInfo.getDemand_source_type().equals(DemandSource.HOUSE))
					returnObject.incrementHouse(p);
				else if(clickInfo.getDemand_source_type().equals(DemandSource.IFD))
					returnObject.incrementIFD(p);
				else if(clickInfo.getDemand_source_type().equals(DemandSource.NETWORK))
					returnObject.incrementNetwork(p);

				if(clickInfo.isEnriched())
					returnObject.incrementEnhancedTrue();
				else
					returnObject.incrementEnhancedFalse();					
			}
			impressionReader_clickEnhanced.close();

		}


		for(Path p : allFilesDeferred)
		{
			if(p.toUri().toString().contains("deferred"))
			{
				ColumnarDataReader impressionReader_click  = new ColumnarDataReader(hadoopUtil.getHadoopConfiguration(coloHelper),p,InputFormats.CLICK_LOG,null);

				while (impressionReader_click.next()) {
					//System.out.println(Arrays.toString(impressionReader_click.getFields()));
					Arrays.toString(impressionReader_click.getFields());
					returnObject.incrementTotalDeferred();
				}
				impressionReader_click.close();
			}

		}

		return returnObject;
	}
*/

}

