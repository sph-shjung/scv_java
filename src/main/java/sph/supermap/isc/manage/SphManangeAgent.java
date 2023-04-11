package sph.supermap.isc.manage;

import java.awt.Color;
import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;

import com.supermap.data.ColorDictionary;
import com.supermap.data.ColorGradientType;
import com.supermap.data.ColorSpaceType;
import com.supermap.data.Colors;
import com.supermap.data.CoordSysTransMethod;
import com.supermap.data.CoordSysTransParameter;
import com.supermap.data.CoordSysTranslator;
import com.supermap.data.CursorType;
import com.supermap.data.Dataset;
import com.supermap.data.DatasetGrid;
import com.supermap.data.DatasetImage;
import com.supermap.data.DatasetType;
import com.supermap.data.DatasetVector;
import com.supermap.data.Datasource;
import com.supermap.data.DatasourceConnectionInfo;
import com.supermap.data.Datasources;
import com.supermap.data.EngineType;
import com.supermap.data.GeoPoint;
import com.supermap.data.GeoSpheroidType;
import com.supermap.data.GeoStyle;
import com.supermap.data.Geometry;
import com.supermap.data.PixelFormat;
import com.supermap.data.Point2D;
import com.supermap.data.PrjCoordSys;
import com.supermap.data.QueryParameter;
import com.supermap.data.Recordset;
import com.supermap.data.Size2D;
import com.supermap.data.Toolkit;
import com.supermap.data.Workspace;
import com.supermap.data.WorkspaceConnectionInfo;
import com.supermap.data.WorkspaceType;
import com.supermap.machinelearning.commontypes.FileSystemOutputSetting;
import com.supermap.mapping.ImageDisplayMode;
import com.supermap.mapping.ImageStretchOption;
import com.supermap.mapping.ImageStretchType;
import com.supermap.mapping.Layer;
import com.supermap.mapping.LayerSettingGrid;
import com.supermap.mapping.LayerSettingImage;
import com.supermap.mapping.LayerSettingVector;
import com.supermap.mapping.Map;

import sph.supermap.isc.util.DatasourceType;
import sph.supermap.isc.util.Disaster;
import sph.supermap.isc.util.PublishItem;
import sph.supermap.isc.util.SphUtil;

public class SphManangeAgent {
	private static SphManangeAgent manager = null;
	
	private Workspace workspace = null;
	private Workspace targetWorkspace = null;
	
	private PrjCoordSys toPrj = new PrjCoordSys(3857);
	
	private CoordSysTransParameter transParam = new CoordSysTransParameter();
	private int[] epsgArr = {3857, 4326, 32652};
	
	private SphManangeAgent() {
		workspace = new Workspace();
		targetWorkspace = new Workspace();
		Toolkit.setDtNameAsTableName(true);
	}
	
	public static SphManangeAgent getInstance() {
//		if(manager == null)
		manager = new SphManangeAgent();
		
		return manager;
	}
	
	/**
	 * File WorkSpace Create
	 * @param WorkspaceConnectionInfo conn
	 */
	public boolean createWorkspace(WorkspaceConnectionInfo conn) {
		if(conn!= null) {
			if(targetWorkspace == null) {
				System.out.println("TARGET WORKSPACE NULL");
			}
			return targetWorkspace.create(conn);
		}
		else {
			System.out.println("ERROR");
			return false;
		}
	}
	
	public Datasource createDS(String filePath, String alias, DatasourceType sourceType, boolean readonly, boolean isKml) {
		Datasource source = null;
		try {
			if(alias == null || alias.length() == 0) {
				if(!isKml)
					alias = filePath.substring(filePath.lastIndexOf(File.separator)+1, filePath.lastIndexOf("."));
				else
					alias = filePath.substring(filePath.lastIndexOf(File.separator)+1, filePath.length());
			}
			
			String udbxPath = isKml == true ? filePath : filePath.substring(0, filePath.lastIndexOf("."));
			File file = new File(udbxPath +".udbx");
			if(file.exists()) {
				file.delete();
				System.out.println("REMOVE PREV UDBX FILE : " + udbxPath +".udbx");
			}
			DatasourceConnectionInfo conn = new DatasourceConnectionInfo();
			conn.setServer(udbxPath + ".udbx");
			System.out.println("UDBX PATH : " + udbxPath);
			System.out.println("ALIAS NAME : " + alias);
//			conn.setServer("D:/download/kmlsampleudbx.udbx");
			conn.setEngineType(sourceType.getEngineType());
			conn.setAlias(alias);
			conn.setReadOnly(readonly);
			source = workspace.getDatasources().create(conn);
			if (source != null) {
				System.out.println("Success 'CreateDS Function' Create Target DataSource");
			} else {
				System.out.println("Error 'CreateDS Function' Create Target Datasource");
			}
		}catch (Exception e) {
			// TODO: handle exception
			System.out.println(e.getMessage());
			return null;
		}
		
		return source;
	}
	/**
	 * Set WorkSpaceConnectionInfo 
	 */
	public WorkspaceConnectionInfo setDisasterWorkspaceConnectionInfo(String path, String disasterName) {
		targetWorkspace = new Workspace();
		
		WorkspaceConnectionInfo conn = new WorkspaceConnectionInfo();
		conn.setServer(path);
		conn.setName(disasterName);
		conn.setType(WorkspaceType.SMWU);
		
		return conn;
	}
	
	/**
	 * Set WorkSpaceConnectionInfo 
	 */
	public WorkspaceConnectionInfo setWorkspaceConnectionInfo(String path, String name) {
		targetWorkspace = new Workspace();
		
		WorkspaceConnectionInfo conn = new WorkspaceConnectionInfo();
		conn.setName(name);
		conn.setServer(path);
		conn.setType(WorkspaceType.SMWU);
		
		return conn;
	}
	
	/**
	 * Connection UDBX[FileManage Datasource] 
	 */
	public Datasource openDatasource(String filePath, String alias, DatasourceType sourceType, boolean readOnly) {
		try {
			if(alias == null || alias.length() == 0) {
				alias = filePath.substring(filePath.lastIndexOf(File.separator)+1, filePath.lastIndexOf("."));
			}
			
			Datasources datasources = targetWorkspace.getDatasources();
			boolean isNew = true;
			
			if(datasources.contains(alias)) {
				for(int i = 0; i < datasources.getCount(); i++) {
					Datasource datasource = datasources.get(i);
					if(filePath.equals(datasource.getConnectionInfo().getServer())) {
						isNew = false;
						break;
					}
				}
			}
			Datasource datasource = null;
			if(!isNew) {
				datasource = targetWorkspace.getDatasources().get(alias);
			}else {
				DatasourceConnectionInfo connInfo = new DatasourceConnectionInfo();
				connInfo.setServer(filePath);
				connInfo.setEngineType(sourceType.getEngineType());
				connInfo.setReadOnly(readOnly);
				connInfo.setAlias(this.getDatasourceName(datasources, alias));
				datasource = datasources.open(connInfo);
			}
			System.out.println("ALIAS : " + datasource.getAlias());
			return datasource;
		}catch(Exception e) {
			//LogManager.log("error", e);
			System.out.println("ERROR" + filePath);
		}
		return null;
	}
	
	public String getDatasourceName(Datasources datasources, String alias) {
		int idx = 1;
		String tmpAlias = alias;
		if(datasources.contains(alias)){
			do {
				tmpAlias = alias+"_"+idx;
				idx++;
			}while(datasources.contains(tmpAlias));
		}
		return tmpAlias;
	}
	
	public Datasource tempDatasource(String path) {
		Datasource tempSource = null;
		String udbxPath = path.substring(0, path.lastIndexOf("."));
		System.out.println("UDBX PATH : " + udbxPath);
		if(workspace.getDatasources().contains("temp"))
			tempSource = workspace.getDatasources().get("temp");
		else {
			DatasourceConnectionInfo connInfo = new DatasourceConnectionInfo();
			/**
			 * [EX]
			 * FilePath :  /data/upload/YYYYMMDD/164423232323.tif
			 * udbxPAth :  /data/upload/YYYYMMDD/164423232323Temp.udbx
			 */
			System.out.println("Temp Datasource Path : [" + udbxPath + "Temp.udbx]");
			connInfo.setServer(udbxPath +"Temp"+ ".udbx");
			connInfo.setEngineType(EngineType.UDBX);
			connInfo.setAlias("temp");
			tempSource = workspace.getDatasources().create(connInfo);
			
			if(tempSource != null)
				System.out.println("Create Temp Datasources");
		}
		return tempSource;
	}
	
	public Datasource createMemoryDatasource() {
		Datasource datasource = null;
		if(workspace.getDatasources().contains("memory")) {
			datasource = workspace.getDatasources().get("memory");
		}else {
			DatasourceConnectionInfo connInfo = new DatasourceConnectionInfo();
			connInfo.setServer(":memory:");
			connInfo.setEngineType(EngineType.UDBX);
			connInfo.setAlias("memory");
			datasource = workspace.getDatasources().create(connInfo);
			
			if(datasource != null) {
				System.out.println("Create Memory Datasource.");
			}
		}
		return datasource;
	}
	
	public Datasource OpenShpDatasource(String filePath, String alias) {
		Datasource targetSource = null;
		Datasources sources = targetWorkspace.getDatasources();
		DatasourceConnectionInfo conn = new DatasourceConnectionInfo();
		System.out.println("FILESERVER :" + filePath);
		conn.setEngineType(EngineType.VECTORFILE);
		conn.setServer(filePath);
		if(alias == null || alias.length() == 0) {
			alias = filePath.substring(filePath.lastIndexOf(File.separator)+1, filePath.lastIndexOf("."));
		}
		System.out.println(alias);
		conn.setAlias(alias);
		conn.setReadOnly(false);
//		conn.setReadOnly(true);
		
		System.out.println("SET");
		try {
			
			targetSource = sources.open(conn);
			if (targetSource != null) {
				System.out.println("Success 'OpenDS Function' Open targetSource DataSource");
			} else {
				System.out.println("Error 'OpenDS Function' Open targetSource Datasource");
			}
		}catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return targetSource;
	}
	
	/**
	 * Open Tif to Datasource 
	 */
	public Datasource OpenTifDatasource(String filePath, String alias) {

		Datasource targetSource = null;
		Datasources sources = targetWorkspace.getDatasources();

		DatasourceConnectionInfo conn = new DatasourceConnectionInfo();
		// path = "F:\\\\image\\\\1651035211544.tif";
		conn.setEngineType(EngineType.IMAGEPLUGINS);
		conn.setServer(filePath);
		
		if(alias == null || alias.length() == 0) {
			alias = filePath.substring(filePath.lastIndexOf(File.separator)+1, filePath.lastIndexOf("."));
		}
		conn.setAlias(alias);
		conn.setReadOnly(false);

		targetSource = sources.open(conn);
		if (targetSource != null) {
			System.out.println("Success 'OpenDS Function' Open targetSource DataSource");
		} else {
			System.out.println("Error 'OpenDS Function' Open targetSource Datasource");
		}

		return targetSource;
	}
	
	/**
	 * 
	 */
	public Datasource connectionPostgre(String user, String password, String server, String database, boolean readonly) {
		Datasource postDS = null;
		try {
			System.out.println(workspace == null ? "WORKSPACE NULL " : "IS WORKSPACE");
			Datasources datasources = workspace.getDatasources();
			
			System.out.println(datasources == null ? "datasources NULL " : "IS datasources");
			DatasourceConnectionInfo conn = new DatasourceConnectionInfo();
			
			conn.setEngineType(EngineType.PGGIS);
			conn.setUser(user);
			conn.setPassword(password);
			conn.setServer(server);
			conn.setDatabase(database);
			conn.setReadOnly(readonly);
			
			postDS = datasources.open(conn);
						
		}catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
			// TODO: handle exception
		}
		return postDS;
	}
	
	/**
	 * 좌표변환 GCS 변환 안됨
	 * @param dataset
	 * @param targetDatasource
	 * @return
	 */
	public Dataset CoordSysTranslator(Dataset dataset, Datasource targetDatasource) {
		System.out.println(toPrj.getProjection());
		System.out.println(toPrj.getEPSGCode());
		return CoordSysTranslator.convert(dataset, toPrj, targetDatasource, targetDatasource.getDatasets().getAvailableDatasetName(dataset.getName()),
				transParam, CoordSysTransMethod.MTH_GEOCENTRIC_TRANSLATION);
	}
	
	public Geometry CoordSysTranslator(Geometry geom, PrjCoordSys fromCoordSys) {
		
		CoordSysTranslator.convert(geom, fromCoordSys, toPrj, transParam, CoordSysTransMethod.MTH_GEOCENTRIC_TRANSLATION);
		return geom;
	}
	
	public boolean CoordSysTranslator(Dataset dataset) {
		
		return CoordSysTranslator.convert(dataset, toPrj, transParam, CoordSysTransMethod.MTH_GEOCENTRIC_TRANSLATION);
	}
	
	// create New Map
	public void CreateNewMap(String mapName, String prjCoordSys) {

		Map map = new Map(targetWorkspace);
		if (prjCoordSys.equals("")) {
			prjCoordSys = "3857";
		}

		map.setPrjCoordSys(new PrjCoordSys(Integer.parseInt(prjCoordSys)));
		targetWorkspace.getMaps().add(mapName, map.toXML());
		map.close();
	}
	
	public void dummyMap(String mapName) {
		Map map = new Map(targetWorkspace);
		map.open(mapName);
		targetWorkspace.getMaps().setMapXML(mapName, map.toXML());
		System.out.println("MAP NAME : " + mapName);
		System.out.println("MAP CNT : " + targetWorkspace.getMaps().getCount());
		map.close();
	}
	
	public void addLayerFromSingleBandImage(DatasetGrid dataset, String caption, String mapName) {
		try {
			System.out.println("MAP : " + caption);
			Map map = new Map(targetWorkspace);
			map.open(mapName);
			
			LayerSettingGrid layerSetting = new LayerSettingGrid();
			layerSetting.setSpecialValue(0.0);
			layerSetting.setSpecialValueTransparent(true);
			
			Layer layer = map.getLayers().add(dataset, layerSetting, false);
			layerSetting = (LayerSettingGrid)layer.getAdditionalSetting();
			
			
			layerSetting.setSpecialValueColor(Color.white);
//			layerSetting.setColorDictionary(getDatasetColor(caption));
			
			layerSetting.setDisplayMode(ImageDisplayMode.STRETCHED);
//			ColorDictionary dic = new ColorDictionary();
//	    	
//			Colors cols = new Colors();
//			for(int i=0; i<32; i++)
//				dic.setColor(i, new Color(8*i, 8*i, 8*i));
//			
//			layerSetting.setColorDictionary(dic);
			
//			layerSetting.setColorTable(Colors.makeGradient(32, ColorGradientType.BLACKWHITE, true));
			layerSetting.setImageStretchOption(getImageStretchOption(3.5, true));
		  	
		  	if(caption != null) {
				layer.setCaption(caption);
			}
			map.viewEntire();
			layer.setVisible(true);
			targetWorkspace.getMaps().setMapXML(mapName, map.toXML());
			map.close();
		}catch (Exception e) {
			// TODO: handle exception
			System.out.println(e.getMessage());
		}
	}
	
	public void addLayerFromDatasetImage(DatasetImage dataset, int[] bandIndexes, String caption, String mapName) {
		Map map = new Map(targetWorkspace);
		map.open(mapName);
		
		int bandCount = dataset.getBandCount();
		if (bandCount == 3) {
			boolean isSameValue = false;
			int tmpIdx = bandIndexes[0];
			for (int i = 1; i < 3; i++) {
				if (tmpIdx != bandIndexes[i]) {
					isSameValue = false;
					break;
				} else {
					isSameValue = true;
				}
			}
			if (isSameValue) 
				bandCount += 1;
		}
		int[] indexes = new int[bandCount];

		for (int i = 0; i < indexes.length; i++) {
			if (i < 3) 
				indexes[i] = bandIndexes[i];
			else 
				indexes[i] = i;
		}

		LayerSettingImage layerSetting = getLayerSettingImage();
		
		Layer layer = map.getLayers().add(dataset, layerSetting, false);
		if (caption != null) {
			layer.setCaption(caption);
		}

		layerSetting = (LayerSettingImage) layer.getAdditionalSetting();
		layerSetting.setDisplayBandIndexes(indexes);
		layerSetting.setImageStretchOption(getImageStretchOption(3.5, isStretch(dataset)));
		map.viewEntire();
		layer.setVisible(true);
		targetWorkspace.getMaps().setMapXML(mapName, map.toXML());
		map.close();
	}
	
	public ImageStretchOption getImageStretchOption(double devialtionFactor, boolean isStretch) {
		ImageStretchOption option = new ImageStretchOption();
		if (isStretch) {
			option.setStretchType(ImageStretchType.STANDARDDEVIATION);
			option.setStandardDeviationStretchFactor(devialtionFactor);
		} else {
			option.setStretchType(ImageStretchType.NONE);
		}
		return option;
	}
	
	public boolean isStretch(DatasetImage dsImage) {
		boolean isStretch = true;
		int bandCnt = dsImage.getBandCount();

		if (bandCnt >= 3) {
			isStretch = false;
			for (int idx = 0; idx < bandCnt; idx++) {
				if (!dsImage.getPixelFormat(idx).equals(PixelFormat.UBIT8)) {
					isStretch = true;
					break;
				}
			}
		}

		return isStretch;
	}
	
	// set LayerSettingVector
	public LayerSettingImage getLayerSettingImage() {
			LayerSettingImage layerSetting = new LayerSettingImage();
			layerSetting.setDisplayColorSpace(ColorSpaceType.RGB);
//			layerSetting.setDisplayColorSpace(ColorSpaceType.RGBA);
			layerSetting.setSpecialValue(0.0);
			layerSetting.setSpecialValueTransparent(true);

			return layerSetting;
		}
	
	public void addLayerFromDatasetImageKMZ(DatasetImage dataset, String caption, String mapName) {
		Map map = new Map(targetWorkspace);
		map.open(mapName);
		
		LayerSettingImage lsi = new LayerSettingImage();
		lsi.setSpecialValue(0.0);
		lsi.setSpecialValueTransparent(true);
		
		Layer layer = map.getLayers().add(dataset, lsi, false);
		if(caption != null) {
			layer.setCaption(caption);
		}
		
		map.viewEntire();
		layer.setVisible(true);
		targetWorkspace.getMaps().setMapXML(mapName, map.toXML());
		map.close();
	}
	
	public void addLayerFromDatasetVector(DatasetVector dataset, String caption, String mapName) {
		Map map = new Map(targetWorkspace);
		map.open(mapName);
		LayerSettingVector layerSetting = new LayerSettingVector();		
		if(dataset != null) {
			if(!dataset.getType().equals(DatasetType.CAD)) {
				GeoStyle style = new GeoStyle();		
				style.setMarkerSymbolID(314);
				style.setMarkerSize(new Size2D(2, 2));
				style.setFillForeColor(Color.RED);
				style.setFillBackColor(Color.RED);
				style.setLineColor(Color.RED);
				layerSetting.setStyle(style);
			}
			Layer layer = map.getLayers().add(dataset, layerSetting, false);
			layer.setVisible(true);
			if(caption != null) {
				layer.setCaption(caption);
			}
			map.viewEntire();
			targetWorkspace.getMaps().setMapXML(mapName, map.toXML());
			map.close();
		}else {
			//LogManager.log(Level.FATAL, "Dataset Null" + caption);
		}
	}
	
	/**
	 * EPSG 코드 확인 3857,4326,32652 or (GCS가 GRS80 or WGS84) 면 true 
	 * @param prj
	 * @return
	 */
	public boolean isAvailableCoord(PrjCoordSys prj) {
		if(prj != null) {
			for(int epsg : epsgArr) {
				if(epsg == prj.getEPSGCode()) {
					return true;
				}
			}
			GeoSpheroidType type = prj.getGeoCoordSys().getGeoDatum().getGeoSpheroid().getType();
			if(GeoSpheroidType.SPHEROID_GRS_1980.equals(type) || GeoSpheroidType.SPHEROID_WGS_1984.equals(type)) {
				return true;
			}
		}
		return false;
	}

	public boolean isTransfer(PrjCoordSys prj) {
		if(prj.getEPSGCode() == toPrj.getEPSGCode()) {
			return false;
		}
		GeoPoint p = new GeoPoint(0.0, 0.0);
		CoordSysTranslator.convert(p,prj,toPrj,transParam,CoordSysTransMethod.MTH_GEOCENTRIC_TRANSLATION);
		if(p.getX() == 0.0 && p.getY() == 0.0) {
			return false;
		}
		return true;
	}
	
	public void ReleaseInstance() {
		try {
			targetWorkspace.save();
			System.out.println("release Instance");
		} catch (Exception e) {
			// TODO: handle exception
		}
		finally { 
			targetWorkspace.dispose();
		} 
	}
	
	public HashMap<String, String> CheckDisasterType(Datasource source, String datasetNm, String dataNm, String queryParams, String statusCol) {
		HashMap<String, String> rtnMap = new HashMap<>();
		DatasetVector vector = (DatasetVector)source.getDatasets().get(datasetNm);
		Recordset rs_vector = vector.query(queryParams +" and type_order > 0 ", CursorType.STATIC);
		
		int lastIndexOf = dataNm.lastIndexOf('_');
		dataNm = dataNm.substring(lastIndexOf - 5, lastIndexOf);
		
		rtnMap.put(statusCol, "");
		try {
			if(rs_vector.getRecordCount() > 0)
			{
				do {
					String disaster = rs_vector.getObject("disaster_type").toString();
					if(statusCol.equals("type") && dataNm.contains(disaster))
					{
						rtnMap.put(statusCol, "default");
						rtnMap.put("disaster_type", disaster);
						break;
					}
					
					if(statusCol.equals("disaster_type") && dataNm.contains(disaster)) {
						rtnMap.put(statusCol, disaster);
						break;
					}
		         		   
	            }while(rs_vector.moveNext());
			}
		}
		catch (Exception e) {
			// TODO: handle exception
		}
		finally {
			rs_vector.close();
			vector.close();
		}
		return rtnMap;
	}
	
	public ArrayList<Disaster> getDisasterInfos(Datasource source, String datasetNm, String queryParams){
		ArrayList<Disaster> arrItem = new ArrayList<Disaster>();
		DatasetVector vector = (DatasetVector)source.getDatasets().get(datasetNm);
		Recordset rs_vector = vector.query(queryParams +" and type_order > 0 ", CursorType.STATIC);
		
		try {
			if(rs_vector.getRecordCount() > 0)
			{
				do {
					Disaster item = new Disaster();
					
					item.setDisaster_type(rs_vector.getObject("disaster_type").toString());
					item.setType_order(rs_vector.getObject("type_order").toString());
					item.setSearch_date(rs_vector.getObject("search_date").toString());
					
					arrItem.add(item);
					
					if(rs_vector.getRecordCount() == arrItem.size())
						break;
	            }while(rs_vector.moveNext());
			}
		}
		catch (Exception e) {
			// TODO: handle exception
		}
		finally {
			rs_vector.close();
			vector.close();
		}
		
		return arrItem;
	}
	
	public ArrayList<PublishItem> getDisasterTypeItem(ArrayList<PublishItem> rtnItems, Datasource source, String datasetNm, String queryParams)
	{
//		ArrayList<PublishItem> rtnItems = new ArrayList<PublishItem>();
		DatasetVector vector = (DatasetVector)source.getDatasets().get(datasetNm);
		Recordset rs_vector = vector.query(queryParams, CursorType.STATIC);
		
		try {
			System.out.println("getDisasterTypeItem : " + rs_vector.getRecordCount());
			if(rs_vector.getRecordCount() > 0)
			{
				rs_vector.moveFirst();
				while(!rs_vector.isEOF()) {
					
					String file_path = rs_vector.getObject("FILE_PATH").toString();
					String data_name = rs_vector.getObject("DATA_NAME").toString();
					String data_type = rs_vector.getObject("DATA_TYPE").toString();
					String datasource_path = rs_vector.getObject("DATASOURCE_PATH").toString();
					String rgb_band_idx = "";
					
					PublishItem item = new PublishItem(file_path, data_name, datasource_path, rgb_band_idx, data_type);
					
					if(data_type.equals("tif")) 
					{
						item.setRgb_band_idx(rs_vector.getObject("RGB_BAND_IDX").toString());
						item.setRgb_band_count(rs_vector.getObject("RGB_BAND_COUNT").toString());
					}
					
					rtnItems.add(item);
					
					rs_vector.moveNext();
				}
			}
			return rtnItems;
		}
		catch (Exception e) {
			return null;
			// TODO: handle exception
		}
		finally {
			rs_vector.close();
			vector.close();
		}
		
	}
	
	public PublishItem isServiceList(Datasource source, String type) {
		
		PublishItem rsltItem = new PublishItem();
		String queryParams = type.equals("DEFAULT") ? "type=" : "DISASTER_TYPE=";
		System.out.println("TYPE:" + type +  "\tQUERY : "+queryParams);
		DatasetVector slVector = (DatasetVector)source.getDatasets().get("SERVICE_LIST");
		Recordset rs_slVector = slVector.query(queryParams + "'" + type + "'", CursorType.STATIC);
		try {
			System.out.println(rs_slVector.getRecordCount());
			if(rs_slVector.getRecordCount() > 0 ) {
				//PUBLISH_DATE
				rsltItem.setMapServiceURL(rs_slVector.getObject("SERVICE_URL").toString());
				rsltItem.setWorkspacePath(rs_slVector.getObject("WORKSPACE_PATH").toString());
				rsltItem.setPublish_date(new Date());
				rsltItem.setStatus(true);
			}
			else {
				rsltItem.setStatus(false);
			}
		}catch (Exception e) {
			// TODO: handle exception
		}finally {
			rs_slVector.close();
		}
		return rsltItem;
	}
}
