package sph.supermap.isc;

import java.awt.Color;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.stream.Stream;

//import org.apache.logging.log4j.Level;
//
//import com.sph.log.LogManager;

//import com.sph.log.LogManager;

import org.json.JSONObject;

import com.supermap.data.ColorDictionary;
import com.supermap.data.ColorGradientType;
import com.supermap.data.Colors;
import com.supermap.data.CursorType;
import com.supermap.data.Dataset;
import com.supermap.data.DatasetGrid;
import com.supermap.data.DatasetImage;
import com.supermap.data.DatasetImageInfo;
import com.supermap.data.DatasetType;
import com.supermap.data.DatasetVector;
import com.supermap.data.DatasetVectorInfo;
import com.supermap.data.Datasets;
import com.supermap.data.Datasource;
import com.supermap.data.EncodeType;
import com.supermap.data.FieldInfo;
import com.supermap.data.FieldType;
import com.supermap.data.GeoRectangle;
import com.supermap.data.Geometry;
import com.supermap.data.PrjCoordSys;
import com.supermap.data.QueryParameter;
import com.supermap.data.Recordset;
import com.supermap.data.Rectangle2D;
import com.supermap.data.SpatialIndexType;
import com.supermap.data.conversion.DataImport;
import com.supermap.data.conversion.ExportSetting;
import com.supermap.data.conversion.ImportMode;
import com.supermap.data.conversion.ImportResult;
import com.supermap.data.conversion.ImportSetting;
import com.supermap.data.conversion.ImportSettingJPG;
import com.supermap.data.conversion.ImportSettingPNG;
import com.supermap.data.conversion.ImportSettingTIF;
import com.supermap.data.conversion.MultiBandImportMode;
import com.supermap.machinelearning.commontypes.FileSystemOutputSetting;
import com.supermap.services.protocols.wfs.v_1_0_0.TransactionStatus;

import sph.supermap.isc.VectorProcess;
import sph.supermap.isc.config.Config;
import sph.supermap.isc.config.Config.Settings;
import sph.supermap.isc.execute.KMLtoSHPThread;
import sph.supermap.isc.file.Compressor;
import sph.supermap.isc.manage.SNUKMLThread;
import sph.supermap.isc.manage.SphDataManage;
import sph.supermap.isc.manage.SphManangeAgent;
import sph.supermap.isc.service.SphService;
import sph.supermap.isc.util.DatasourceType;
import sph.supermap.isc.util.Disaster;
import sph.supermap.isc.util.KMLReader;
import sph.supermap.isc.util.KMLReader.LatLonBox;
import sph.supermap.isc.util.PublishItem;
import sph.supermap.isc.util.ResultItem;
import sph.supermap.isc.util.ServiceStatus;
import sph.supermap.isc.util.SphUtil;

public class VectorProcess {
	private static VectorProcess process = null;
	private static Settings config = null;

	private static SphService service = new SphService();
	private static SphManangeAgent agent = SphManangeAgent.getInstance();
	private static SphDataManage manage = null;

	private int maxThreadCount = 20;
	private static ArrayList<Thread> threadList = new ArrayList<>();
	

	private static String fileDir = "/data/workspaces";
	private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss");

	public static void main(String[] args) {
		ServiceStatus status = ServiceStatus.Fail;
		process = new VectorProcess();
		/**
		 * Config Class 에서 ./src/config/config.xml file Read
		 */
		config = new Config().settings;
		manage = new SphDataManage();
		
		/**
		 * Shell Sciprt를 통해 인자값 전달 
		 * [0] Params : Table SMID 값
		 * [1] Params : tableName , ex) UPLOADED, WEATHER , .....
		 */
		if (args.length == 0)
			return;
		String pk = args[0];
		String tableName = args[1];
		
		
		try {
			//Config.xml 파일에 설정한 DB Connection 정보를 통해 Datasource로 DB 연결
			Datasource postDS = agent.connectionPostgre(config.getPostgre().getId(), config.getPostgre().getPassword(),
					config.getPostgre().getServer() + ":" + config.getPostgre().getPort(), config.getPostgre().getName(),
					false);
			//결과 반환 Object 
			PublishItem transManageObj = null;

			//Datasource에 'TableName'+ _DATA Dataset
			DatasetVector tabular = (DatasetVector) postDS.getDatasets().get(tableName + "_DATA");
			/**
			 * Recordset으로 PK 값 Record 조회 && 수정은 하지않으므로 CursorType.STATIC
			 * CursorType -> 수정 o (DYNAMIC) // 수정 x (STATIC)
			 * PK값에 FILE_PATH, DATA_NAME, DATA_TYPE (소문자) 조회 
			 */
			Recordset rs = tabular.query("SMID = " + pk, CursorType.STATIC);
			String filePath = rs.getString("FILE_PATH");
			String dataName = rs.getString("DATA_NAME");
			String dataType = rs.getString("DATA_TYPE").toLowerCase();
			StringBuilder builder = new StringBuilder();
			//조회 후 Recordset Close (중요)
			rs.close();
			if (pk != null) {
				/**
				 * dataType값에 따라서 tif, kmz, shp, kml 작업 
				 * tif : tifProcess
				 * kmz : kmzProcess
				 * shp : shpProcess
				 * kml : kmlProcess
				 * 
				 * 변환 작업이 완료 된 후(Sucess일 경우) 해당 PK값에 Record 값 Update [setRecordsetUpdate]
				 */
				if (dataType.toLowerCase().equals("tif"))
					transManageObj = process.tifProcess(filePath, dataName);
				else if (dataType.toLowerCase().equals("kmz")) {
					transManageObj = process.kmzProcess(filePath, dataName, builder);
					transManageObj.setMessage(builder);
				} 
				else if (dataType.toLowerCase().equals("shp"))
				{
					transManageObj = process.shpProcess(filePath, dataName);
				}
				else {
					transManageObj = process.kmlProcess(filePath, dataName);
				}
				if (transManageObj.isStatus())
					process.setRecordsetUpdate(pk, tabular, dataType, transManageObj, false);
			}
			
			/**
			 * tableName 값이 WEATHER or SUBSD_PERIODIC 일 경우 
			 * 1. tableName 값에 맞는 Workspace를 생성 
			 * 2. 업로드 된 파일 (변환 완료)을 Datasource 생성 
			 * 3. Datasource를 MapService로 발행  [ process.generateItem(Params .....) ]
			 * 4. 정상적으로 Service가 발행 되면 기존에 생서되어있는 MapService와 Workspace 파일 삭제
			 * 5. MapService 발행까지 완료 되었을 경우 기존에 발행되어있던 Workspace 파일 삭제.
			 * 
			 * [5] 까지의 작업이 모두 완료 되었을때 Shell Script에 'S' or 'F' 값을 출력
			 *  
			 * ☆ Shell에 결과를 출력해야 Server에서 해당 결과값을 참조하여 후 처리 진행 .
			 * ############################################################# 
			 * ☆ Shell 결과값을 출력하는건 필수
			 * ############################################################# 
			 */
			if (tableName.equals("WEATHER") || tableName.equals("SUBSD_PERIODIC")) {
				String typeName = tableName.equals("WEATHER") ? "WEATHER" : "SUBSD";
				boolean typeRslt = process.generateItem(typeName, transManageObj, pk, tabular);
				if (typeRslt)
				{
					process.fsDelete(typeName);
					System.out.println(ServiceStatus.Service.getStatus());
				}
				else
					System.out.println(ServiceStatus.Fail.getStatus());
				return;
			}
			System.out.println(status.Service.getStatus());
			
		}catch (Exception e) {
			// TODO: handle exception
			System.out.println(e.getMessage());
			System.out.println(ServiceStatus.Fail.getStatus());
		}
	}
	/**
	 * Execute TIF Process
	 * @param String filePath : Tif File Path
	 */
	private PublishItem tifProcess(String filePath, String data_name) {

		//업로드 된 파일의 확장자가 zip인지 확인
		int idx = filePath.lastIndexOf('.');
		String fileExt = filePath.substring(idx + 1, filePath.length());

		//업로드 된 파일이 zip일 경우 압축해제 작업, filePath에 TIF File경로 반환
		if (fileExt.equals("zip")) {
			String deCompPath = Compressor.unzip(filePath);
			filePath = Compressor.getFileName(deCompPath, "TIF");
		}

		PublishItem rtnItem = null;
		/**
		 * 해당 TIF 파일의 경로에 UDBX Temp 생성
		 * 해당 TIF 파일로 DataSource [udbx] 생성 
		 */
		Datasource targetDataSource = agent.createDS(filePath, null, DatasourceType.UDBX, false, false);
		Datasource imageDataSource = agent.OpenTifDatasource(filePath, null);

		//Upload된 데이터에 Band가 SingleBand , MultiBand에 따라서 DatasetImage or DatasetGrid 사용
		DatasetImage imageDataset = (DatasetImage) imageDataSource.getDatasets().get(0);
		DatasetImage targetImage = null;

		DatasetGrid targetGrid = null;
		Datasource ds = targetDataSource;

		//TIF 파일의 좌표계가 3857,4326,32652 or (GCS가 GRS80 or WGS84) 면 true
		if (!agent.isAvailableCoord(imageDataset.getPrjCoordSys())) {
			System.out.println("ERROR");
		}

		//3857 좌표계로 변환 && 확인
		boolean isTransfer = agent.isTransfer(imageDataset.getPrjCoordSys()); 
		if (isTransfer)
		{
			//좌표계가 3857일 경우 tempDatasource 생성 [ 이거는 memoryDatasource로 해도 사실 상관은없음 .. 어떤..문제로인해 memory에서 temp로 수정했는데 memory문제가 아니였음..... ]
			ds = agent.tempDatasource(filePath);
//			ds = agent.createMemoryDatasource();
		}

		try {
			//Tif 파일 Import 설정 [singleBand, multiBand 확인 ] 
			ImportSetting importSetting = null;
			importSetting = new ImportSettingTIF(filePath, ds);

			((ImportSettingTIF) importSetting).setPyramidBuilt(false);
			importSetting.setTargetDatasetName(ds.getDatasets().getAvailableDatasetName(data_name));
			importSetting.setImportMode(ImportMode.NONE);
			importSetting.setEncodeType(EncodeType.LZW);
			System.out.println("BandCNT : " + imageDataset.getBandCount());
			if (imageDataset.getBandCount() == 1) {
				((ImportSettingTIF) importSetting).setMultiBandImportMode(MultiBandImportMode.SINGLEBAND);
				((ImportSettingTIF) importSetting).setImportingAsGrid(true);
			} else {
				((ImportSettingTIF) importSetting).setMultiBandImportMode(MultiBandImportMode.MULTIBAND);
			}

			DataImport dataImport = new DataImport();
			dataImport.getImportSettings().add(importSetting);
			ImportResult result = dataImport.run();

			String names[] = result.getSucceedDatasetNames(importSetting);

			System.out.println("Import Success");
			
			rtnItem = new PublishItem(filePath, data_name, targetDataSource.getConnectionInfo().getServer(), "", "tif");
			System.out.println("DATA NAME : " + data_name);
			String splitName = data_name.substring(data_name.lastIndexOf("_") + 1, data_name.length());
			int strLength = splitName.length();

			int length = splitName.lastIndexOf("A");
			if (length == -1)
				length = splitName.lastIndexOf("B") + 1;

			//RGB Band값 split RecordSet 설정
			String bandIndexsStr = splitName.substring(length, strLength);

			String r = bandIndexsStr.substring(0, 2);
			String g = bandIndexsStr.substring(2, 4);
			String b = bandIndexsStr.substring(4, 6);
			String rgbIndexs = r + "," + g + "," + b;

			rtnItem.setRgb_band_idx(rgbIndexs);
			rtnItem.setRgb_band_count(imageDataset.getBandCount() + "");

			// SingleBand... -> DatasetGrid -> setPyramid -> Extent 설정
			if (imageDataset.getBandCount() == 1) {
				targetGrid = (DatasetGrid) ds.getDatasets().get(0);
				
				if (isTransfer) {
					System.out.println("TRANS");
					targetGrid = (DatasetGrid) agent.CoordSysTranslator(ds.getDatasets().get(0), targetDataSource);
					/**
					 * FileRemove Temp Datasource
					 * 1. Datasource Close 
					 * 2. Datasource File Remove 
					 */
					String removePath = ds.getConnectionInfo().getServer();
					ds.close();
					System.out.println("RemoveFilePath : " + removePath);
					manage.removeFile(removePath);
					
				}
				
				if (!targetGrid.getHasPyramid()) {
					System.out.println("TARGET GRID PYRAMID");
					targetGrid.buildPyramid();
					System.out.println("PYRAMID SUCCESS");
				}
				
				rtnItem.setTop(targetGrid.getBounds().getTop());
				rtnItem.setLeft(targetGrid.getBounds().getLeft());
				rtnItem.setRight(targetGrid.getBounds().getRight());
				rtnItem.setBottom(targetGrid.getBounds().getBottom());
			}
			// MultiBand... -> DatasetImage -> setPyramid -> Extent 설정 
			else {
				targetImage = (DatasetImage) ds.getDatasets().get(0);
				if (isTransfer) {
					targetImage = (DatasetImage) agent.CoordSysTranslator(targetImage, targetDataSource);
					/**
					 * FileRemove Temp Datasource
					 * 1. Datasource Close 
					 * 2. Datasource File Remove 
					 */
					String removePath = ds.getConnectionInfo().getServer();
					ds.close();
					System.out.println("RemoveFilePath : " + removePath);
					manage.removeFile(removePath);
				}

				System.out.println("HAS PYRAMID : " + ((DatasetImage) targetImage).getHasPyramid());
				if (!((DatasetImage) targetImage).getHasPyramid()) {
					System.out.println("StartPyramid");
					((DatasetImage) targetImage).buildPyramid();
					System.out.println("End Pyramid");
				}
				((DatasetImage) targetImage).buildStatistics();

				rtnItem.setTop(targetImage.getBounds().getTop());
				rtnItem.setLeft(targetImage.getBounds().getLeft());
				rtnItem.setRight(targetImage.getBounds().getRight());
				rtnItem.setBottom(targetImage.getBounds().getBottom());
			}

			rtnItem.setStatus(true);
		} catch (Exception e) {
			// TODO: handle exception
			System.out.println(e.getMessage());
		}
		finally {
			System.out.println("DatasetCount : " + targetDataSource.getDatasets().getCount());
			targetDataSource.close();
		}

		return rtnItem;
	}

	/**
	 * Execute KMZ Process
	 * 
	 * @param String filePath : KMZ File Directory
	 * 
	 */
	private PublishItem kmzProcess(String filePath, String data_name, StringBuilder builder) {
		//KMZ 파일 압축해제, KML, TIF 파일명 조회
		PublishItem rtnItem = null;
		String deCompPath = Compressor.unzip(filePath);
		String kmlFile = Compressor.getFileName(deCompPath, "KML");
		String tifFilePath = Compressor.getFileName(deCompPath, "TIF");
		//TIF 파일경로에 Datasource 생성
		Datasource targetDataSource = agent.createDS(tifFilePath, null, DatasourceType.UDBX, false, false);

		//KML 속성정보 read [kmlFile] , imageFile , Path 설정
		KMLReader reader = new KMLReader(kmlFile);
		String imageFile = reader.kml.Folder.GroundOverlay.Icon.href;
		String imageFilePath = deCompPath + File.separator + imageFile;
		builder.append(deCompPath + File.separator + reader.kml.Folder.ScreenOverlay.Icon.href);

		//Tif File Datasource Open -> width, height 설정 , 설정 후 tif Dataset, Datasource는 사용하지 않으므로 close 
		// DataSet Close -> DataSource Close 
		Datasource imageDs = agent.OpenTifDatasource(imageFilePath, null);
		DatasetImage imageDataset = (DatasetImage) imageDs.getDatasets().get(0);
		int imageWidth = imageDataset.getWidth();
		int imageHeight = imageDataset.getHeight();
		imageDataset.close();
		imageDs.close();

		//LatLonBox값 설정 
		LatLonBox box = reader.kml.Folder.GroundOverlay.LatLonBox;
		//Rectangle2D Draw
		Rectangle2D rectangle = new Rectangle2D(box.west, box.south, box.east, box.north);
		//Geometry 생성 [rectangle2D]
		Geometry geom = new GeoRectangle(rectangle, 0.0);
		// 3857 projection
		GeoRectangle geoRect = (GeoRectangle) agent.CoordSysTranslator(geom, new PrjCoordSys(4326));
		String worldFilePath = deCompPath + File.separator + reader.kml.Folder.GroundOverlay.name + ".wld";

		try {
			FileWriter writer = new FileWriter(worldFilePath);
			writer.write(geoRect.getWidth() / imageWidth + "\n0\n0\n-" + geoRect.getHeight() / imageHeight + "\n"
					+ geoRect.getBounds().getLeft() + "\n" + geoRect.getBounds().getTop());
			writer.close();
		} catch (Exception e) {

		}

		//imageFile 형식에 맞는 ImportSetting 생성
		ImportSetting importSetting = null;
		imageFile = imageFile.toLowerCase();
		if (imageFile.contains(".png")) {
			importSetting = new ImportSettingPNG(imageFilePath, targetDataSource);
			((ImportSettingPNG) importSetting).setPyramidBuilt(true);
			((ImportSettingPNG) importSetting).setMultiBandImportMode(MultiBandImportMode.MULTIBAND);
			((ImportSettingPNG) importSetting).setWorldFilePath(worldFilePath);

		} else if (imageFile.contains(".tif")) {
			importSetting = new ImportSettingTIF(imageFilePath, targetDataSource);
			((ImportSettingTIF) importSetting).setPyramidBuilt(true);
			((ImportSettingTIF) importSetting).setMultiBandImportMode(MultiBandImportMode.MULTIBAND);
			((ImportSettingTIF) importSetting).setWorldFilePath(worldFilePath);
		} else if (imageFile.contains(".jpg") || imageFile.contains(".jpeg")) {
			importSetting = new ImportSettingJPG(imageFilePath, targetDataSource);
			((ImportSettingJPG) importSetting).setPyramidBuilt(true);
			((ImportSettingJPG) importSetting).setMultiBandImportMode(MultiBandImportMode.MULTIBAND);
			((ImportSettingJPG) importSetting).setWorldFilePath(worldFilePath);
		}
		imageFile = targetDataSource.getDatasets().getAvailableDatasetName(imageFile);
		importSetting.setTargetDatasetName(imageFile);
		importSetting.setImportMode(ImportMode.NONE);
		importSetting.setEncodeType(EncodeType.LZW);

		DatasetImage targetImage = null;
		//DataImport에 ImportSetting 추가
		DataImport dataImport = new DataImport();
		dataImport.getImportSettings().add(importSetting);
		//Import 실행
		ImportResult result = dataImport.run();
		//Import 성공 시 DatasetNames 반환 이후 후처리 진행
		if (result.getSucceedDatasetNames(importSetting) != null) {
			String datasetName = result.getSucceedDatasetNames(importSetting)[0];
			//DatasetImage 좌표계 변환
			targetImage = (DatasetImage) targetDataSource.getDatasets().get(datasetName);
			targetImage.setPrjCoordSys(new PrjCoordSys(3857));

			//NoValue 설정
			int noValueData = 0;
			if (imageFile.contains("png") || imageFile.contains("PNG")) {
				noValueData = 16777215;
			}
			// 각 밴드의 NoData값 설정
			int bandCnt = targetImage.getBandCount();
			for (int i = 0; i < bandCnt; i++) {
				targetImage.setNoData(noValueData, i);
			}
			// 작업 완료 후 값 설정 후 PublishItem 반환
			rtnItem = new PublishItem(filePath, data_name, targetDataSource.getConnectionInfo().getServer(), "", "kmz");

			rtnItem.setRgb_band_count(targetImage.getBandCount() + "");
			rtnItem.setTop(targetImage.getBounds().getTop());
			rtnItem.setLeft(targetImage.getBounds().getLeft());
			rtnItem.setRight(targetImage.getBounds().getRight());
			rtnItem.setBottom(targetImage.getBounds().getBottom());
			rtnItem.setStatus(true);
		}

		return rtnItem;
	}

	/**
	 * Execute SHP Process
	 * 
	 * @param String filePath : SHP File Directory
	 * 
	 */
	private PublishItem shpProcess(String filePath, String data_name) {
		PublishItem rtnItem = null;
		//SHP 파일 압축해제, SHP 파일경로 조회
		String deCompPath = Compressor.unzip(filePath);
		String shpFilePath = Compressor.getFileName(deCompPath, "SHP");

		//SHP파일 경로에 DataSource 생성
		Datasource targetDataSource = agent.createDS(shpFilePath, null, DatasourceType.UDBX, false, false);
		//Shp 파일로 DataSource Open
		Datasource vectorSource = agent.OpenShpDatasource(shpFilePath, null);
		//Vector Datset 조회
		DatasetVector vector = (DatasetVector) vectorSource.getDatasets().get(0);
		DatasetVector targetVector = null;

		//VectorDataset의 좌표계 검증
		boolean isTransfer = agent.isTransfer(vector.getPrjCoordSys());
		if (isTransfer) {
			//targetVector로 좌표계 변환, buildSpatialIndex (RTREE) 설정 후 
			// vector -> targetVector로 Copy하는 개념으로 이해..
			targetVector = (DatasetVector) agent.CoordSysTranslator(vector, targetDataSource);
			targetVector.buildSpatialIndex(SpatialIndexType.RTREE);
			//Copy 완료 후 vectorSource는 사용하지 않으므로 close
			vectorSource.close();
		} else {
			targetVector = vector;
		}
		// 작업 완료 후 값 설정 후 PublishItem 반환		
		rtnItem = new PublishItem(filePath, data_name, targetDataSource.getConnectionInfo().getServer(), "", "shp");

		rtnItem.setTop(targetVector.getBounds().getTop());
		rtnItem.setLeft(targetVector.getBounds().getLeft());
		rtnItem.setRight(targetVector.getBounds().getRight());
		rtnItem.setBottom(targetVector.getBounds().getBottom());
		rtnItem.setStatus(true);

		return rtnItem;
	}

	/**
	 * Execute KML Process
	 * 
	 * @param String filePath : KML File Directory
	 * 
	 */
	private PublishItem kmlProcess(String filePath, String data_name) {
		/**
		 * KML이 처리되는 형식은 
		 * 각각의 kml 파일을 read해서 createKMLDataset으로 생성한 vector Dataset에 데이터를 append하는 형식의 개념
		 * 
		 * 그래서 하나의 kml파일에 데이터를 통째로 넣으면 에러가 발생하는 것
		 * 만약 client가 하나의 File에 Data를 통째로 넣어서 처리하려고하면 해당 부분에 대해서는 수정이 필요함.
		 * ※ 1. 어떤 형식으로 데이터가 들어오는지 확인
		 * ※ 2. 데이터를 Read하는 부분을 해당 파일(kml)에 맞게 수정을해야함
		 */
		PublishItem rtnItem = null;
		//KML 파일 압축해제, KML폴더 경로 조회
		String deCompPath = Compressor.unzip(filePath);// Compress file path
		String datasetName = filePath.substring(filePath.lastIndexOf(File.separator) + 1, filePath.lastIndexOf("."));
		
		//압축해제 경로에 Datasource 생성
		Datasource targetDataSource = agent.createDS(deCompPath, null, DatasourceType.UDBX, false, true);
		// CAD 형식의 DatasetVector 생성 
		DatasetVector vector = createKMLDataset(targetDataSource, datasetName);
		//File Object생성 압축해제 경로 
		File kmlDirectory = new File(deCompPath);
		
		//transform 메서드에서 Thread MAX COUNT 확인 -> MAX COUNT : 20 

		try {
			//Directory일 경우 
			if (kmlDirectory.isDirectory()) {
				//KML FileList에서 KML Read
				for (File kmlFile : kmlDirectory.listFiles()) {
					//File 일 경우 Thread 실행 tarsnform 참고
					transform(kmlFile, vector);
				}
			} else {
				//File 일 경우 Thread 실행 tarsnform 참고
				transform(kmlDirectory, vector);
			}
		} catch (Exception e) {
//         return false;
			// TODO: handle exception
		}
		while (threadList.size() > 0) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		vector.buildSpatialIndex(SpatialIndexType.RTREE);
		// 작업 완료 후 값 설정 후 PublishItem 반환
		rtnItem = new PublishItem(filePath, data_name, targetDataSource.getConnectionInfo().getServer(), "", "kml");

		rtnItem.setTop(vector.getBounds().getTop());
		rtnItem.setLeft(vector.getBounds().getLeft());
		rtnItem.setRight(vector.getBounds().getRight());
		rtnItem.setBottom(vector.getBounds().getBottom());
		rtnItem.setStatus(true);
		
		return rtnItem;
	}

	private DatasetVector createKMLDataset(Datasource datasource, String datasetName) {
		//Vector Datset을 생성 DatasetType은 CAD , 좌표계 3857 , description(TEXT), zvalue(DOUBLE) Column ADD
		Datasets datasets = datasource.getDatasets();
		datasetName = datasets.getAvailableDatasetName(datasetName);
		DatasetVectorInfo info = new DatasetVectorInfo(datasetName, DatasetType.CAD);
		DatasetVector vector = datasets.create(info);
		vector.setPrjCoordSys(new PrjCoordSys(3857));
		vector.getFieldInfos().add(new FieldInfo("description", FieldType.TEXT));
		vector.getFieldInfos().add(new FieldInfo("zvalue", FieldType.DOUBLE));
		return vector;
	}

	private void transform(File kmlFile, DatasetVector vector) {
		//kmlFile이 File이 아닐경우 재귀하는 형식
		if (threadList.size() > maxThreadCount) {
			try {
				while (threadList.size() > maxThreadCount) {
					Thread.sleep(1000);
				}
				transform(kmlFile, vector);
			} catch (InterruptedException e) {
			}
		} else {
			if (kmlFile.isDirectory()) {
				for (File file : kmlFile.listFiles()) {
					transform(file, vector);
				}
			}
			if (kmlFile.isFile()) {
				SNUKMLThread thread = new SNUKMLThread(kmlFile.getAbsolutePath(), vector, agent, threadList);
				threadList.add(thread);
				thread.start();
			}
		}
	}

	public void removeFile(String filePath) {
		try {
			//filePath에 extList에 있는 확장자들 삭제  
			String[] extList = { "cpg", "dbf", "prj", "shp", "shx" };
			for (String ext : extList) {
				File file = new File(filePath + "." + ext);
				if (file.exists()) {
					if (file.delete())
						System.out.println(filePath + "." + ext + " File Remove");
				} else {
					System.out.println("Not Found File");
				}
			}

		} catch (Exception e) {
			// TODO: handle exception
		}
	}

	private PublishItem publishMapService(String workspacePath, String mapName, String disasterType, String type) {
		PublishItem rslt = new PublishItem();

		try {
			String mapServiceJSON = null;
			//Token 생성 REST 호출
			String token = service.getToken(config.getIServer().getUser(), config.getIServer().getPassword());
			//TOKEN 값, workspacePath 값 으로 맵서비스 발행 
			mapServiceJSON = service.createSerivce(token, workspacePath);

			//발행이 정상적으로 되었을경우 TEXT값 반환
			if (!mapServiceJSON.equals("")) {
				mapServiceJSON = mapServiceJSON.substring(1, mapServiceJSON.length() - 1);
				//MapService반환 Object 생성 
				JSONObject item = new JSONObject(mapServiceJSON);
				//Config.xml 파일에 PostgreSQL -> publishURL 값에 있는 IP 주소로 반환
				//iServer내에서 발행하면 localhost로 떨어짐, 외부에서 접속을 해야하는 url주소가 되어야하기때문에 replace하는 것
				rslt.setMapServiceURL(item.getString("serviceAddress").replace("localhost", config.getPostgre().getPublishUrl()) + "/maps/" + mapName);
				rslt.setWorkspacePath(workspacePath);
				rslt.setDisasterType(disasterType);
				rslt.setType(type);
				rslt.setPublish_date(new Date());

				if (!rslt.getMapServiceURL().equals("")) {
					rslt.setStatus(true);
				}
			}
		} catch (Exception e) {
			// TODO: handle exception
		}
		return rslt;
	}

	private boolean setRecordsetUpdate(String pk, DatasetVector dataset, String data_type, PublishItem rsltObj,
			boolean isPublish) {
		try {
			Recordset rs = dataset.query("SMID = " + pk, CursorType.DYNAMIC);
			rs.edit();
			if (!isPublish) {
				rs.setObject("b_left", rsltObj.getLeft());
				rs.setObject("b_right", rsltObj.getRight());
				rs.setObject("b_top", rsltObj.getTop());
				rs.setObject("b_bottom", rsltObj.getBottom());

				if (rsltObj.getMessage() != null) {
					rs.setObject("message", rsltObj.getMessage());
				}

				if (rsltObj.getData_type().equals("tif")) {
					rs.setObject("rgb_band_idx", rsltObj.getRgb_band_idx());
					rs.setObject("rgb_band_count", rsltObj.getRgb_band_count());
				}
				if (rsltObj.getData_type().equals("kmz")) {
					rs.setObject("rgb_band_count", rsltObj.getRgb_band_count());
				}
				rs.setObject("DATASOURCE_PATH", rsltObj.getDatasource_path());
				rs.setString("STATUS", rsltObj.isStatus() == true ? "S" : "F");
				rs.setDateTime("PUBLISH_DATE", new Date());
			} else {
				rs.setObject("WORKSPACE_PATH", rsltObj.getWorkspacePath());
				rs.setObject("SERVICE_URL", rsltObj.getMapServiceURL());
			}
			boolean rslt = rs.update();
			rs.close();

			return rslt;
		} catch (Exception e) {
			e.printStackTrace();
			// TODO: handle exception
		}

		return true;
	}

	private boolean generateItem(String commonNm, PublishItem transManageObj, String pk, DatasetVector tabular) {
		if (!commonNm.equals("EARQK")) {
			//발행하려는 유형 ex) WEATHER or SUBSD + 년월일시분초 형태로 WorkspaceName으로 Workspace 생성
			String worksapceNm = commonNm + SphUtil.getName();
			String liveWorkspacePath = "/data/workspaces/" + worksapceNm + ".smwu";
			try {
				agent.createWorkspace(agent.setDisasterWorkspaceConnectionInfo(liveWorkspacePath, worksapceNm));
				//좌표계가 3857 인 WEATHER or SUBSD Map Name 생성
				agent.CreateNewMap(commonNm, "3857");

				//업로드 한 파일의 DataSource Open
				Datasource source = agent.openDatasource(transManageObj.getDatasource_path(),
						transManageObj.getData_name(), DatasourceType.UDBX, false);

				//DataType 조회
				String dttype = transManageObj.getData_type();

				//DataType 값에 따라 DatasetImage or DatasetVector 위에서 생성한 Map에 Layer 추가
				if (dttype.equals("tif")) {
					DatasetImage image = (DatasetImage) source.getDatasets().get(0);
					agent.addLayerFromDatasetImage(image, SphUtil.parseBandIndexes(transManageObj.getRgb_band_idx()),
							transManageObj.getData_name(), commonNm);
				} else if (dttype.equals("kmz")) {
					DatasetImage image = (DatasetImage) source.getDatasets().get(0);
					agent.addLayerFromDatasetImageKMZ(image, transManageObj.getData_name(), commonNm);
				} else {
					DatasetVector vector = (DatasetVector) source.getDatasets().get(0);
					agent.addLayerFromDatasetVector(vector, transManageObj.getData_name(), commonNm);
				}

				agent.ReleaseInstance();
				//맵 서비스 발행 
				PublishItem rslt = process.publishMapService(liveWorkspacePath, commonNm, "", "");
				//맵서비스 발행 후 ServiceList에 Data Insert
				process.setRecordsetUpdate(pk, tabular, commonNm, rslt, true);

			} catch (Exception e) {
				// TODO: handle exception
			}
		} else {
			PublishItem rslt = new PublishItem();
			rslt.setWorkspacePath("");
			rslt.setPublish_date(new Date());
			rslt.setMapServiceURL("");
			process.setRecordsetUpdate(pk, tabular, commonNm, rslt, true);
		}
		return true;
	}
	
	private void fsDelete(String typeNm) {
		List<Path> fileList;
		String token = service.getToken(config.getIServer().getUser(), config.getIServer().getPassword());
		
		try {
			Path paths = Paths.get(fileDir);
			Stream<Path> walk = Files.walk(paths);
			fileList = walk.filter(path -> path.getFileName().toString().startsWith(typeNm)).collect(Collectors.toList());
			Collections.reverse(fileList);
			HashMap<String, String> hash = new HashMap<String, String>();
			Map<Long, String> hashItemList = new HashMap<Long, String>();
			
			for (Path filePath : fileList) {
				String fileName = filePath.getFileName().toString();
				String type = fileName.substring(0, typeNm.length());
				
				if (!hash.containsKey(type)) {
					hash.put(type, fileName);
				}
				
				BasicFileAttributes attr = Files.readAttributes(filePath, BasicFileAttributes.class);
				FileTime creationTime = attr.creationTime();

				LocalDateTime ldt = creationTime.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
				String date = ldt.format(DATE_FORMATTER);

				Date newDate = new Date(date);
				Calendar fileCal = Calendar.getInstance();
				fileCal.setTime(newDate);

				Calendar nowCal = Calendar.getInstance();
				nowCal.setTime(new Date());
				long sec = (nowCal.getTimeInMillis() - fileCal.getTimeInMillis()) / (1000);
				long min = (nowCal.getTimeInMillis() - fileCal.getTimeInMillis()) / (60 * 1000);
				long hour = (nowCal.getTimeInMillis() - fileCal.getTimeInMillis()) / (60 * 60 * 1000);
				
				hashItemList.put(sec, fileName);
			}
			
			for (String key : hash.keySet()) {
				Map<Long, String> typeMap =  hashItemList.entrySet().stream().filter(
							map -> map.getValue().toString().startsWith(key)).collect(
									Collectors.toMap(map -> map.getKey(), map -> map.getValue()
							)
				);
				
				if(typeMap.size() > 1)
				{
					long min = Collections.min(typeMap.keySet());
					for (Long secKey : typeMap.keySet()) {
						if(secKey != min) {
							service.deleteService(token, fileDir + "/" + typeMap.get(secKey));
							manage.removeFile(fileDir + "/" + typeMap.get(secKey));
						}
					}
				}
			}
		}catch (Exception e) {
			// TODO: handle exception
		}
	}
}