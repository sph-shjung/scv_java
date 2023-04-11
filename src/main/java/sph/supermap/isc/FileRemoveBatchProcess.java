package sph.supermap.isc;

import com.supermap.data.CursorType;
import com.supermap.data.DatasetVector;
import com.supermap.data.Datasource;
import com.supermap.data.Recordset;
import com.supermap.data.Recordset.BatchEditor;

import sph.supermap.isc.config.Config;
import sph.supermap.isc.config.Config.Settings;
import sph.supermap.isc.manage.SphDataManage;
import sph.supermap.isc.manage.SphManangeAgent;
import sph.supermap.isc.service.SphService;

public class FileRemoveBatchProcess {
	private static FileRemoveBatchProcess process = null;
	private static SphService service = new SphService();
	private static SphManangeAgent agent = SphManangeAgent.getInstance();

	private static SphDataManage manage = null;
	private static Settings config = null;

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		if (args.length <= 0)
			return;

		String tableName = args[0];
		process = new FileRemoveBatchProcess();
		config = new Config().settings;
		manage = new SphDataManage();

		Datasource postDS = agent.connectionPostgre(config.getPostgre().getId(), config.getPostgre().getPassword(),
				config.getPostgre().getServer() + ":" + config.getPostgre().getPort(), config.getPostgre().getName(),
				false);

		DatasetVector serviceDV = (DatasetVector) postDS.getDatasets().get(tableName + "_DATA");
		
		// 오늘 기준 한달 전 Data && 상태값 S && datasource_path not null
		String searchCondition = tableName.equals("UPLOADED") ? "1 month" : "1 week";
		String queryParams = "status IN('S', 'OLD') and publish_date < (now() - interval '" + searchCondition + "') and datasource_path is not null";
		Recordset rs = serviceDV.query(queryParams, CursorType.DYNAMIC);
		try {
			if (rs.getRecordCount() <= 0) {
				return;
			}
			System.out.println("Total Record Count : " + rs.getRecordCount());
			/**
			 * 삭제 순서 1. 원본 파일(File_Path 확인) -> Zip 파일 일 경우 Zip파일 삭제 & Zip파일 압축해제 폴더 통 삭제 ->
			 * 1-1. Zip 파일이 아닌 경우 원본파일 삭제 -> 1-2. UDBX 파일 삭제 2. 파일 삭제 완료 후 STATUS 값 'D'로 변경
			 */

			while (!rs.isEOF()) {
				String smID = rs.getObject("smid").toString();
				String filePath = rs.getObject("file_path").toString();
				String dataName = rs.getObject("data_name").toString();

				int idx = filePath.lastIndexOf('.');
				String fileExt = filePath.substring(idx + 1, filePath.length());

				
				boolean status = false;
				String dirName = filePath.substring(0, idx);
				System.out.println("FILEPATH : " + filePath);
				System.out.println("DIR NAME : " + dirName);
				System.out.println("EXT NAME : " + fileExt);
				
				// 원본파일 삭제
				status = manage.removeFile(filePath, true);
				if(status) {
					System.out.println("REMOVE : " + filePath +" File" );
				}
				
				// txt 파일 삭제 
				status = manage.removeFile(dirName + ".txt", true);				
				if(status) {
					System.out.println("REMOVE : " + dirName +".txt File" );
				}
				
				// tif.xml 파일 삭제
				status = manage.removeFile(dirName + ".tif.xml", true);				
				if(status) {
					System.out.println("REMOVE : " + dirName +".tif.xml File" );
				}
				
				
				// Zip파일 압축 해제 폴더 삭제
				if (fileExt.equals("zip") || fileExt.equals("ZIP") || fileExt.equals("kmz") || fileExt.equals("KMZ"))
				{
					status = manage.removeFile(dirName, true);
				}
				// UDBX 파일 삭제
				else
					status = manage.removeFile(dirName + ".udbx", true);
				
				System.out.println("STATUS:" + status);

				if (status) {
					BatchEditor editor = rs.getBatch();
					editor.begin();
					rs.setObject("status", "D");
					editor.update();
				}
				
				rs.moveNext();
			}
		} catch (Exception e) {
			// TODO: handle exception
		} finally {
			rs.close();
			serviceDV.close();
			postDS.close();
		}
	}

}
