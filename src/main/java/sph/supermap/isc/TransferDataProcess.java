package sph.supermap.isc;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;

import javax.activation.DataSource;

import org.geotools.data.DataTestCase;

import com.supermap.data.ColorGradientType;
import com.supermap.data.Colors;
import com.supermap.data.CursorType;
import com.supermap.data.Dataset;
import com.supermap.data.DatasetGrid;
import com.supermap.data.DatasetImage;
import com.supermap.data.DatasetVector;
import com.supermap.data.Datasource;
import com.supermap.data.EncodeType;
import com.supermap.data.FieldInfo;
import com.supermap.data.GeoRectangle;
import com.supermap.data.Geometry;
import com.supermap.data.PrjCoordSys;
import com.supermap.data.Recordset;
import com.supermap.data.Rectangle2D;
import com.supermap.data.Recordset.BatchEditor;
import com.supermap.data.conversion.DataImport;
import com.supermap.data.conversion.ImportMode;
import com.supermap.data.conversion.ImportResult;
import com.supermap.data.conversion.ImportSetting;
import com.supermap.data.conversion.ImportSettingJPG;
import com.supermap.data.conversion.ImportSettingPNG;
import com.supermap.data.conversion.ImportSettingTIF;
import com.supermap.data.conversion.MultiBandImportMode;
import com.supermap.data.Workspace;

import sph.supermap.isc.file.Compressor;
import sph.supermap.isc.manage.SphManangeAgent;
import sph.supermap.isc.manage.TransferDataThread;
import sph.supermap.isc.service.SphService;
import sph.supermap.isc.util.DatasourceType;
import sph.supermap.isc.util.KMLReader;
import sph.supermap.isc.util.PublishItem;
import sph.supermap.isc.util.ServiceStatus;
import sph.supermap.isc.util.KMLReader.LatLonBox;

public class TransferDataProcess {
	private static TransferDataProcess process = null;

	private static int threadMaxCount = 4;
	private static ArrayList<Thread> threadList = new ArrayList<>();

	private static SphService service = new SphService();
	private static SphManangeAgent agent = SphManangeAgent.getInstance();

	private static String workspaceFilePath = "";
	private static String workspaceName = "";

	/// home/sphinfo/SuperMap/DB
//	private static String datasourceFilePath = "D:\\서울대\\SATELLITEDATA.udbx";
	private static String datasourceFilePath = "/home/sphinfo/SuperMap/DB/SATELLITEDATA.udbx";

	private static String datasourceName = "SATELLITEDATA";
	private static String dpColNm = "datasource_path";

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String sourceName = "";
		String filePathColNm = "";

//		if(args.length > 0 )
//			sourceName = args[0].toUpperCase();
//		else
//			sourceName = "IMAGE_DATA";

		sourceName = "IMAGE_DATA";
//		sourceName = "VECTOR_DATA";
		filePathColNm = sourceName.equals("IMAGE_DATA") == true ? "image_path" : "file_path";

		// BASE UDBX Open [SATELLITEDATA UDBX targetworkspace value]
		Datasource source = agent.openDatasource(datasourceFilePath, null, DatasourceType.UDBX, false);
		DatasetVector dataset = (DatasetVector) source.getDatasets().get(sourceName);

		// TARGET UDBX Open [Postgre SQL workspace value]
		Datasource target_source = agent.connectionPostgre("sphinfo", "sphinfo", "localhost:5432", "sphinfo", false);
		DatasetVector target_vector = (DatasetVector) target_source.getDatasets().get("UPLOADED_DATA");
//		DatasetVector target_vector = (DatasetVector) target_source.getDatasets().get("EARQK_PERIODIC_DATA");

		System.out.println("POSTGRE DATASET CNT : " + target_source.getDatasets().getCount());

		Recordset rs = dataset.query("STATUS = 'S'", CursorType.STATIC);
		//Recordset rs = dataset.query("STATUS = 'S' and data_name like '%EARQK%'", CursorType.STATIC);
//		Recordset rs = dataset.query("STATUS = 'S'", CursorType.STATIC);
		try {
			while (!rs.isEOF()) {
				String data_name = rs.getString("data_name");
				String path = rs.getString(filePathColNm);
				int lastIdx = path.lastIndexOf('.');
				String fileType = path.substring(lastIdx + 1, path.length());
//				
//				String data_type = rs.getString("data_type");

				HashMap<String, Object> hash = new HashMap<String, Object>();

				for (int idx = 0; idx < rs.getFieldCount(); idx++) {
					if (!rs.getFieldInfos().get(idx).getName().startsWith("Sm")) {
						// Column Name
						String name = rs.getFieldInfos().get(idx).getName();
						if (name.equals(filePathColNm) || name.equals(filePathColNm.toUpperCase()))
							name = "file_path";
						if (name.equals("LEFT") || name.equals("TOP") || name.equals("RIGHT") || name.equals("BOTTOM"))
							name = "b_" + name.toLowerCase();
						if (name.equals("DATA_ID") || name.equals("data_id") )
						{
							System.out.println("DATA_ID COLUMN : " + name);
							name = "data_id";
						}

						// Column Value
						Object value = rs.getFieldValue(idx) == null ? "" : rs.getFieldValue(idx);
						hash.put(name.toLowerCase(), value);
					}
				}
				hash.put("mig_status", "");
				hash.put("data_type", fileType);
//				hash.put("data_type", data_type);

				PublishItem transManageObj = null;

				
				StringBuilder builder = new StringBuilder();
				if (fileType.equals("tif"))
					transManageObj = tifProcess(path, data_name);
				else if(fileType.equals("kmz")) {
					transManageObj = kmzProcess(path, data_name, builder);
				}

				if (transManageObj != null) {
					if (transManageObj.isStatus()) {
						System.out.println("FILE PATH : " + path);
						System.out.println("DATA NAME : " + data_name);
						System.out.println("DATA TYPE : " + fileType);

						boolean rslt = appendRecordset(target_vector, hash, transManageObj);
						if (rslt)
							System.out.println("TRANSLATE & IMPORT SUCCESS : " + path);
					}
				} else {
					System.out.println("NOT EXISTS " + path + "Remove this Thread");
				}

				rs.moveNext();
			}
		} catch (Exception e) {
			// TODO: handle exception
		} finally {
			rs.close();
			target_source.close();
			source.close();
		}
	}

	private static PublishItem tifProcess(String filePath, String data_name) {

		int idx = filePath.lastIndexOf('.');
		String fileExt = filePath.substring(idx + 1, filePath.length());

		if (fileExt.equals("zip")) {
			String deCompPath = Compressor.unzip(filePath);
			System.out.println("DECOMPPATH : " + deCompPath);
			filePath = Compressor.getFileName(deCompPath, "TIF");
			System.out.println("DECOMP FILE PATH : " + filePath);
		} else {
			System.out.println("UNZIP File!!!!");
		}
		///////////////////////////////////////////////
		// File 존재하는지
		// -> 존재하면 process 진행
		// -> 없을경우 return null....
		///////////////////////////////////////////////
		File file = new File(filePath);
		if(!file.exists()) {
			System.out.println("FILE NOT FOUND");
			return null;
		}
		
		String udbxPath = filePath.substring(0, filePath.lastIndexOf("."));
		File udbxFile = new File(udbxPath +".udbx");
		
		if(udbxFile.exists()) {
			System.out.println(udbxPath +".udbx는 이미 변환된 파일입니다.");
			return null;
		}

		PublishItem rtnItem = null;
		Datasource targetDataSource = agent.createDS(filePath, null, DatasourceType.UDBX, false, false);

		Datasource imageDataSource = agent.OpenTifDatasource(filePath, null);
		DatasetImage imageDataset = (DatasetImage) imageDataSource.getDatasets().get(0);

		DatasetImage targetImage = null;
		DatasetGrid targetGrid = null;

		// ************
		Datasource ds = targetDataSource;
		// ************

		if (!agent.isAvailableCoord(imageDataset.getPrjCoordSys())) {
			System.out.println("ERROR");
		}

		boolean isTransfer = agent.isTransfer(imageDataset.getPrjCoordSys());
//				//Projection 3857 Check 
		if (isTransfer)
			ds = agent.createMemoryDatasource();

		try {
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

			if (imageDataset.getBandCount() == 1) {
				targetGrid = (DatasetGrid) ds.getDatasets().get(0);

				if (isTransfer) {
					System.out.println("TRANS");
					targetGrid = (DatasetGrid) agent.CoordSysTranslator(targetGrid, targetDataSource);
					ds.close();
				}
//				targetGrid.setNoValue(0);
//				targetGrid.setColorTable(Colors.makeGradient(32, ColorGradientType.BLACKWHITE, true));
				targetGrid.setColorTable(Colors.makeGradient(32, ColorGradientType.BLACKWHITE, false));

				if (!targetGrid.getHasPyramid()) {
					System.out.println("TARGET GRID PYRAMID");
					targetGrid.buildPyramid();
					System.out.println("PYRAMID SUCCESS");
				}
			}
			// MultiBand
			else {
				targetImage = (DatasetImage) ds.getDatasets().get(0);

				if (isTransfer) {
					targetImage = (DatasetImage) agent.CoordSysTranslator(targetImage, targetDataSource);
					ds.close();
				}

				if (!((DatasetImage) targetImage).getHasPyramid()) {
					((DatasetImage) targetImage).buildPyramid();
				}
				((DatasetImage) targetImage).buildStatistics();
			}

			System.out.println("Import Success");
		} catch (Exception e) {
			// TODO: handle exception
			System.out.println(e.getMessage());
		}

		rtnItem = new PublishItem(filePath, data_name, targetDataSource.getConnectionInfo().getServer(), "", "tif");
		rtnItem.setStatus(true);

		targetDataSource.close();
		imageDataSource.close();
		return rtnItem;
	}
	
	private static PublishItem kmzProcess(String filePath, String data_name, StringBuilder builder) {
		
		File file = new File(filePath);
		if(!file.exists())
		{
			System.out.println("FILE NOT FOUND");
			return null;
		}
		
		PublishItem rtnItem = null;
		String deCompPath = Compressor.unzip(filePath);
		String kmlFile = Compressor.getFileName(deCompPath, "KML");
		String tifFilePath = Compressor.getFileName(deCompPath, "TIF");
		Datasource targetDataSource = agent.createDS(tifFilePath, null, DatasourceType.UDBX, false, false);
		
		KMLReader reader = new KMLReader(kmlFile);

		String imageFile = reader.kml.Folder.GroundOverlay.Icon.href;
		String imageFilePath = deCompPath + File.separator + imageFile;
//		builder.append(deCompPath + File.separator + reader.kml.Folder.ScreenOverlay.Icon.href);
		
		Datasource imageDs = agent.OpenTifDatasource(imageFilePath, null);
		DatasetImage imageDataset = (DatasetImage) imageDs.getDatasets().get(0);
		int imageWidth = imageDataset.getWidth();
		int imageHeight = imageDataset.getHeight();
		imageDataset.close();
		imageDs.close();

		LatLonBox box = reader.kml.Folder.GroundOverlay.LatLonBox;
		Rectangle2D rectangle = new Rectangle2D(box.west, box.south, box.east, box.north);
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
		DataImport dataImport = new DataImport();
		dataImport.getImportSettings().add(importSetting);
		ImportResult result = dataImport.run();
		
		if (result.getSucceedDatasetNames(importSetting) != null) {
			String datasetName = result.getSucceedDatasetNames(importSetting)[0];
			targetImage = (DatasetImage) targetDataSource.getDatasets().get(datasetName);
			targetImage.setPrjCoordSys(new PrjCoordSys(3857));

			int noValueData = 0;
			if (imageFile.contains("png") || imageFile.contains("PNG")) {
				noValueData = 16777215;
			}
			// nodata value to 0
			int bandCnt = targetImage.getBandCount();
			for (int i = 0; i < bandCnt; i++) {
				targetImage.setNoData(noValueData, i);
			}
		}
		
		rtnItem = new PublishItem(filePath, data_name, targetDataSource.getConnectionInfo().getServer(), "", "tif");
		rtnItem.setStatus(true);

		targetDataSource.close();
		return rtnItem;
	}

	private static boolean appendRecordset(DatasetVector vector, HashMap<String, Object> hash, PublishItem item) {
		boolean rslt = false;
		Recordset rs = vector.query("", CursorType.DYNAMIC);
		BatchEditor editor = rs.getBatch();
		try {
			editor.begin();
			rs.addNew(null);
			for (String key : hash.keySet()) {
				if (key.toUpperCase().equals("mig_status") || key.toLowerCase().equals("mig_status")) {
					rs.setObject(key, ServiceStatus.Service.getStatus());
				} else if (key.toUpperCase().equals(dpColNm) || key.toLowerCase().equals(dpColNm)) {
					rs.setObject(key, item.getDatasource_path());
				} 
				else if(key.toUpperCase().equals("ogc_url") || key.toLowerCase().equals("ogc_url")) { }
				else if(key.toUpperCase().equals("data_id") || key.toLowerCase().equals("data_id")) { }
				else {
					rs.setObject(key, hash.get(key));
				}
			}
			editor.update();
			rslt = true;
		} catch (Exception e) {
			// TODO: handle exception
			System.out.println(e.getMessage());
			editor.cancel();
		} finally {
			editor = null;
			rs.close();
			return rslt;
		}
	}

}
