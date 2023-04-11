package sph.supermap.isc.manage;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

import com.supermap.data.ColorGradientType;
import com.supermap.data.Colors;
import com.supermap.data.CursorType;
import com.supermap.data.DatasetGrid;
import com.supermap.data.DatasetImage;
import com.supermap.data.DatasetVector;
import com.supermap.data.Datasource;
import com.supermap.data.EncodeType;
import com.supermap.data.Recordset;
import com.supermap.data.Recordset.BatchEditor;
import com.supermap.data.conversion.DataImport;
import com.supermap.data.conversion.ImportMode;
import com.supermap.data.conversion.ImportResult;
import com.supermap.data.conversion.ImportSetting;
import com.supermap.data.conversion.ImportSettingTIF;
import com.supermap.data.conversion.MultiBandImportMode;
import com.supermap.machinelearning.commontypes.FileSystemOutputSetting;

import sph.supermap.isc.file.Compressor;
import sph.supermap.isc.util.DatasourceType;
import sph.supermap.isc.util.PublishItem;
import sph.supermap.isc.util.ServiceStatus;

public class TransferDataThread extends Thread {
	private ArrayList<Thread> threadList;
	private SphManangeAgent agent;
	private String filePath;
	private String dataName;
	private String fileType;

	private DatasetVector vector;
	private HashMap<String, Object> hash;
	
	//dpColNm -> datasource_path Column Nmae 
	private String dpColNm ="datasource_path"; 

	public TransferDataThread() {

	}

	public TransferDataThread(DatasetVector vector, HashMap<String, Object> hash, SphManangeAgent agent,
			String filePath, String dataName, String fileType, ArrayList<Thread> threadList) {
		System.out.println("<<<<<<<<<<<<<<< TRANS THREAD INSTANCE >>>>>>>>>>>>>>>>>>");
		this.vector = vector;
		this.hash = hash;
		this.agent = agent;
		this.filePath = filePath;
		this.dataName = dataName;
		this.fileType = fileType;
		this.threadList = threadList;
	}

	public void run() {
		try {
			PublishItem transManageObj = null;
			System.out.println("###################THREAD START###################");
			System.out.println("FILE PATH : " + filePath);
			System.out.println("DATA NAME : " + dataName);
			System.out.println("FILE TYPE : " + fileType);

			if (this.fileType.equals("tif"))
				transManageObj = this.tifProcess(filePath, dataName);

			if(transManageObj != null) {
				if (transManageObj.isStatus()) {
					System.out.println("TARNS SUCCESS");
					boolean rslt = appendRecordset(vector, hash, transManageObj);
					System.out.println(rslt);
				}
			}
			else {
				System.out.println("NOT EXISTS " + filePath + "Remove this Thread");
			}
		} catch (Exception e) {
			// TODO: handle exception
		} finally {
			threadList.remove(this);
			System.out.println("CALL REMOVE THREAD");
		}
	}

	private PublishItem tifProcess(String filePath, String data_name) {
		
		int idx = filePath.lastIndexOf('.');
		String fileExt = filePath.substring(idx + 1, filePath.length());

		if (fileExt.equals("zip")) {
			String deCompPath = Compressor.unzip(filePath);
			System.out.println("DECOMPPATH : " + deCompPath);
			filePath = Compressor.getFileName(deCompPath, "TIF");
			System.out.println("DECOMP FILE PATH : " + filePath);
		}
		else {
			System.out.println("UNZIP File!!!!");
		}
		///////////////////////////////////////////////
		//	File 존재하는지 
		//	-> 존재하면 process 진행
		//	-> 없을경우 return null....
		///////////////////////////////////////////////
		File file = new File(filePath);
		if(!file.exists())
			return null;
		else {
			boolean delete = file.delete();
			System.out.println(delete == true ? "REMOVEPREV Datasoucre file : " + filePath : "");
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
//			targetImage = (DatasetImage) ds.getDatasets().get(0);

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

		System.out.println("DatasetCount : " + targetDataSource.getDatasets().getCount());
		targetDataSource.close();
		return rtnItem;
	}

	private boolean appendRecordset(DatasetVector vector,  HashMap<String, Object> hash, PublishItem item) {
		boolean rslt = false;
		Recordset rs = vector.query("", CursorType.DYNAMIC);
		BatchEditor editor = rs.getBatch();
		try {
			editor.begin();
			rs.addNew(null);
			for (String key : hash.keySet()) {
				if(key.toUpperCase().equals("mig_status") || key.toLowerCase().equals("mig_status")) {
					rs.setObject(key, ServiceStatus.Service.getStatus());
				}
				else if(key.toUpperCase().equals(dpColNm) || key.toLowerCase().equals(dpColNm)) {
					rs.setObject(key, item.getDatasource_path());
				}
				else {
					rs.setObject(key, hash.get(key));
				}
			}
			editor.update();
			rslt = true;
		}catch (Exception e) {
			// TODO: handle exception
			System.out.println(e.getMessage());
			editor.cancel();
		}
		finally {
			editor = null;
			rs.close();
			return rslt;
		}
	}
	
	private boolean setRecordsetUpdate(String pk, DatasetVector dataset, String data_type, PublishItem rsltObj, boolean isPublish) {
		try {
			Recordset rs = dataset.query("SMID = " + pk, CursorType.DYNAMIC);
			rs.edit();
			if (!isPublish) {
				rs.setObject("b_left", rsltObj.getLeft());
				rs.setObject("b_right", rsltObj.getRight());
				rs.setObject("b_top", rsltObj.getTop());
				rs.setObject("b_bottom", rsltObj.getBottom());
				if (rsltObj.getData_type().equals("tif")) {
					rs.setObject("rgb_band_idx", rsltObj.getRgb_band_idx());
					rs.setObject("rgb_band_count", rsltObj.getRgb_band_count());
				}
				if (rsltObj.getData_type().equals("kmz")) {
					rs.setObject("rgb_band_count", rsltObj.getRgb_band_count());
				}
				rs.setObject("DATASOURCE_PATH", rsltObj.getDatasource_path());
				rs.setString("STATUS", rsltObj.isStatus() == true ? "S" : "F");
			} else {
				rs.setDateTime("PUBLISH_DATE", rsltObj.getPublish_date());
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

}
