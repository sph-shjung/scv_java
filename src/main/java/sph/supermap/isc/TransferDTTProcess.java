package sph.supermap.isc;

import java.io.File;

import com.supermap.data.CursorType;
import com.supermap.data.Dataset;
import com.supermap.data.DatasetVector;
import com.supermap.data.Datasource;
import com.supermap.data.DatasourceConnectionInfo;
import com.supermap.data.Datasources;
import com.supermap.data.Recordset;
import com.supermap.data.Workspace;
import com.supermap.data.conversion.DataExport;
import com.supermap.data.conversion.ExportResult;
import com.supermap.data.conversion.ExportSetting;
import com.supermap.data.conversion.ExportSettingTIF;
import com.supermap.data.conversion.ExportSettings;
import com.supermap.data.conversion.FileType;

import sph.supermap.isc.manage.SphManangeAgent;
import sph.supermap.isc.util.DatasourceType;

public class TransferDTTProcess {
	private static Workspace workspace;
	private static SphManangeAgent agent = SphManangeAgent.getInstance();

	private static String datasourceFilePath = "/data/udb/image/image.udbx";
	
	private static String targetSourceFilePath = "/home/sphinfo/SuperMap/DB/SATELLITEDATA.udbx";
	private static String targetDatasetName = "IMAGE_DATA";
	

	public static void main(String[] args) {
		init();
		// TODO Auto-generated method stub
		Datasource source = agent.openDatasource(datasourceFilePath, null, DatasourceType.UDBX, false);
		
		Datasource targetSource = openDatasource(targetSourceFilePath, null, DatasourceType.UDBX, false);
		System.out.println(targetSource.getDatasets().getCount());
		DatasetVector targetDataset = (DatasetVector)targetSource.getDatasets().get(targetDatasetName);
		
		try {
			for(int idx = 0; idx < source.getDatasets().getCount(); idx++) {
				//###############################################
				Dataset dataset = source.getDatasets().get(idx);
				String datasetNm = dataset.getName();
				String resultNm = "";
				System.out.println("DATASET NAME : " + datasetNm);
				//image
				if(datasetNm.startsWith("D")) 
					resultNm = datasetNm.replace("Dataset_", "");
				//raster
				if(datasetNm.startsWith("T")) 
					resultNm = datasetNm.replace("T", "");
				
				System.out.println("DATASET REPLACE NAME : " + resultNm);
				//###############################################
				
				String queryParmas = "STATUS ='S' and IMAGE_PATH like '%" + resultNm + ".tif'";
				Recordset rs = targetDataset.query(queryParmas, CursorType.STATIC);
				
				if(rs.getRecordCount() > 0 ) {
					String tifRsltPath = rs.getObject("IMAGE_PATH").toString();
					
					int lastIndex = tifRsltPath.lastIndexOf('/');
					String tifRsltPathDir = tifRsltPath.substring(0, lastIndex);
					
					File file = new File(tifRsltPathDir);
					if(!file.exists())
					{
						boolean rsltBoolean = file.mkdirs();
						if(rsltBoolean) {
							System.out.println("MAKE THE DIRECTORY");
						}
					}
					
					
					
					ExportSetting exportSetting = null;
					exportSetting = new ExportSettingTIF(dataset, tifRsltPath, FileType.TIF);
					
					DataExport dataExport = new DataExport();
					dataExport.getExportSettings().add(exportSetting);
					
					ExportResult rslt = dataExport.run();
					
					if(rslt.getSucceedSettings() != null) {
						System.out.println("TRANSFER DATASET TO TIF SUCCESS ITEM : " + tifRsltPath);
						rs.close();
					}	
				}
			}
		}catch (Exception e) {
			// TODO: handle exception
			System.out.println(e.getMessage());
		}
		finally {
			targetSource.close();
			source.close();
		}
	}
	
	private static void init() {
		workspace = new Workspace();
		System.out.println("Workspace INSTACNE Generate");
	}
	
	private static Datasource openDatasource(String filePath, String alias, DatasourceType sourceType, boolean readOnly) {
		try {
			if(alias == null || alias.length() == 0) {
				alias = filePath.substring(filePath.lastIndexOf(File.separator)+1, filePath.lastIndexOf("."));
			}
			
			Datasources datasources = workspace.getDatasources();
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
				datasource = workspace.getDatasources().get(alias);
			}else {
				DatasourceConnectionInfo connInfo = new DatasourceConnectionInfo();
				connInfo.setServer(filePath);
				connInfo.setEngineType(sourceType.getEngineType());
				connInfo.setReadOnly(readOnly);
				connInfo.setAlias(getDatasourceName(datasources, alias));
				datasource = datasources.open(connInfo);
			}
			return datasource;
		}catch(Exception e) {
			System.out.println("ERROR" + filePath);
		}
		return null;
	}
	
	private static String getDatasourceName(Datasources datasources, String alias) {
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
	
	
}
