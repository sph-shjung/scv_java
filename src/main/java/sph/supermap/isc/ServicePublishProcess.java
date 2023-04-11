package sph.supermap.isc;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.json.JSONObject;

import com.supermap.data.ColorGradientType;
import com.supermap.data.Colors;
import com.supermap.data.CursorType;
import com.supermap.data.DatasetGrid;
import com.supermap.data.DatasetImage;
import com.supermap.data.DatasetVector;
import com.supermap.data.Datasource;
import com.supermap.data.Recordset;

import sph.supermap.isc.config.Config;
import sph.supermap.isc.config.Config.Settings;
import sph.supermap.isc.manage.SphDataManage;
import sph.supermap.isc.manage.SphManangeAgent;
import sph.supermap.isc.service.SphService;
import sph.supermap.isc.util.DatasourceType;
import sph.supermap.isc.util.Disaster;
import sph.supermap.isc.util.PublishItem;
import sph.supermap.isc.util.ServiceStatus;
import sph.supermap.isc.util.SphUtil;

public class ServicePublishProcess {
	private static Settings config = null;
	private static ServicePublishProcess process = null;
	
	private static SphService service = new SphService();
	private static SphManangeAgent agent = SphManangeAgent.getInstance();
	private static SphDataManage manage = null;
	
	private static String fileDir = "/data/workspaces";
	private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss");

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ServiceStatus status = ServiceStatus.Fail;
		process = new ServicePublishProcess();
		config = new Config().settings;
		manage = new SphDataManage();
		PublishItem transManageObj = null;

		
		if (args.length == 0)
			return;
		String pk = args[0];
		String tableName = args[1];

		Datasource postDS = agent.connectionPostgre(config.getPostgre().getId(), config.getPostgre().getPassword(),
				config.getPostgre().getServer() + ":" + config.getPostgre().getPort(), config.getPostgre().getName(), false);

		DatasetVector tabular = (DatasetVector) postDS.getDatasets().get(tableName + "_DATA");
		Recordset rs = tabular.query("SMID = " + pk, CursorType.STATIC);
		
		String filePath = rs.getString("FILE_PATH");
		String dataName = rs.getString("DATA_NAME");
		String dataType = rs.getString("DATA_TYPE").toLowerCase();
		
		rs.close();
		
		String qColumn = config.getPostgre().getDisasterquery();
		String qCnt = config.getPostgre().getDisastercount();
		HashMap<String, String> checkType = agent.CheckDisasterType(postDS, config.getPostgre().getDisaster(), dataName, qColumn + "<" + qCnt, "type");
		
		String type = "", disasterType = "";
		if (checkType.get("type").equals("default")) {
			type = checkType.get("type");
			disasterType = checkType.get("disaster_type");
		} else {
			disasterType = agent.CheckDisasterType(postDS, config.getPostgre().getDisaster(), dataName, qColumn + ">=" + qCnt, "disaster_type").get("disaster_type");
		}
		
		try {
			/**
			 * 20220811 -> 기존 DEFAULT or DisasterType 으로 Workspace이름을 고정했으나 연속적으로 파일업로드 할 경우
			 * 해당 작업이 일어날때마다 MapserVice를 발행해야하므로 Workspace도 새로 생성 기존에 commonNm 은 MapName으로
			 * 활용대체 신규로 DefaultWorkspaceNm 생성 : 기존 네이밍 규칙 + yyyyMMddhhmmsss 추가
			 */
			String commonNm = type.equals("default") ? "DEFAULT" : disasterType;

			String deafultWorkspaceNm = commonNm + SphUtil.getName();
			String disasterWorksapceNm = disasterType + SphUtil.getName();

			if (type.equals("default")) {
				// String liveWorkspacePath = "/data/workspaces/" + commonNm + ".smwu";
				String liveWorkspacePath = "/data/workspaces/" + deafultWorkspaceNm + ".smwu";
				ArrayList<Disaster> arrItem = agent.getDisasterInfos(postDS, config.getPostgre().getDisaster(),
						qColumn + "<" + qCnt);
				ArrayList<PublishItem> publishItems = new ArrayList<PublishItem>();

				for (Disaster disaster : arrItem) {
					String queryParams = "reverse(split_part(reverse(data_name),'_',2)) LIKE '%"
							+ disaster.getDisaster_type() + "%' and status = 'S' and group_id ='2'";
					queryParams += "ORDER BY publish_date DESC LIMIT 2 ";
//					queryParams += "ORDER BY publish_date DESC";
					agent.getDisasterTypeItem(publishItems, postDS, config.getPostgre().getTablename(), queryParams);
				}

				agent.createWorkspace(agent.setWorkspaceConnectionInfo(liveWorkspacePath, deafultWorkspaceNm));
				agent.CreateNewMap(commonNm, "3857");

				for (PublishItem publishItem : publishItems) {
					Datasource source = null;
					String dt = publishItem.getData_type();

					source = agent.openDatasource(publishItem.getDatasource_path(), publishItem.getData_name(),
							DatasourceType.UDBX, false);
					if (dt.equals("tif")) {
						if(!publishItem.getRgb_band_count().equals("1")) {
							DatasetImage image = (DatasetImage) source.getDatasets().get(0);
							agent.addLayerFromDatasetImage(image, SphUtil.parseBandIndexes(publishItem.getRgb_band_idx()), publishItem.getData_name(), commonNm);
						}
						else {
							DatasetGrid grid =(DatasetGrid) source.getDatasets().get(0);
							grid.setColorTable(Colors.makeGradient(32, ColorGradientType.BLACKWHITE, false));
							agent.addLayerFromSingleBandImage(grid, publishItem.getData_name(), commonNm);
						}
					} else if (dt.equals("kmz")) {
						DatasetImage image = (DatasetImage) source.getDatasets().get(0);
						agent.addLayerFromDatasetImageKMZ(image, publishItem.getData_name(), commonNm);
					} else {
						DatasetVector vector = (DatasetVector) source.getDatasets().get(0);
						agent.addLayerFromDatasetVector(vector, publishItem.getData_name(), commonNm);
					}
				}
				agent.ReleaseInstance();
				/**
				 * 수정순서 (3) 20220811 현재 수정 X 내용 : ServiceList 테이블에 데이터 계속 쌓아야함.
				 */

				transManageObj = process.publishMapService(liveWorkspacePath, commonNm, type, disasterType);

				if (transManageObj.isStatus()) {
					process.setRecordsetUpdate(pk, tabular, dataType, transManageObj, true);
					process.setSLRecordtUpdate(transManageObj, commonNm, disasterType, postDS);
					
					process.fsDelete("DEFAULT");
				}
			}

			if (!disasterType.equals("") && type.equals("default")) {
				String search_date = process.getSearchDate(postDS, disasterType, "search_date");
				String from_date = SphUtil.getDate(search_date);
				String to_date = SphUtil.getDate();
				ArrayList<PublishItem> arr = process.getSearchItemList(postDS, disasterType, search_date);

//	         	String liveWorkspacePath = "/data/workspaces/" + disasterType + ".smwu";
				String liveWorkspacePath = "/data/workspaces/" + disasterWorksapceNm + ".smwu";
				// agent.createWorkspace(agent.setDisasterWorkspaceConnectionInfo(liveWorkspacePath,
				// disasterType));
				agent.createWorkspace(agent.setDisasterWorkspaceConnectionInfo(liveWorkspacePath, disasterWorksapceNm));
				agent.CreateNewMap(disasterType, "3857");
				for (PublishItem publishItem : arr) {
					Datasource source = agent.openDatasource(publishItem.getDatasource_path(), publishItem.getData_name(),
							DatasourceType.UDBX, false);

					String dttype = publishItem.getData_type();
					if (dttype.equals("tif")) {
						if(!publishItem.getRgb_band_count().equals("1")) {
							DatasetImage image = (DatasetImage) source.getDatasets().get(0);
							agent.addLayerFromDatasetImage(image, SphUtil.parseBandIndexes(publishItem.getRgb_band_idx()), publishItem.getData_name(), disasterType);
						}
						else {
							DatasetGrid grid = (DatasetGrid) source.getDatasets().get(0);
							
							agent.addLayerFromSingleBandImage(grid, publishItem.getData_name(), disasterType);
						}
					} else if (dttype.equals("kmz")) {
						DatasetImage image = (DatasetImage) source.getDatasets().get(0);
						agent.addLayerFromDatasetImageKMZ(image, publishItem.getData_name(), disasterType);
					} else {
						DatasetVector vector = (DatasetVector) source.getDatasets().get(0);
						agent.addLayerFromDatasetVector(vector, publishItem.getData_name(), disasterType);
					}
				}
				agent.ReleaseInstance();

				transManageObj = process.publishMapService(liveWorkspacePath, disasterType, null, disasterType);

				if (transManageObj.isStatus())
				{
					process.setSLRecordtUpdate(transManageObj, disasterType, disasterType, postDS);
					process.fsDelete(disasterType);
				}
			}

			System.out.println(status.Service.getStatus());
		}catch (Exception e) {
			// TODO: handle exception
		}
	}
	

	private String getSearchDate(Datasource source, String disasterName, String selectColumn) {
		String r_searchDate = null;
		DatasetVector disasterVector = null;
		Recordset rs = null;
		try {
			disasterVector = (DatasetVector) source.getDatasets().get(config.getPostgre().getDisaster());
			rs = disasterVector.query("disaster_type = '" + disasterName + "'", CursorType.STATIC);
			r_searchDate = rs.getString(selectColumn);
		} catch (Exception e) {
			// TODO: handle exception
		} finally {
			rs.close();
			disasterVector.close();
		}
		return r_searchDate;
	}
	
	private ArrayList<PublishItem> getSearchItemList(Datasource source, String disasterType, String searchDate) {
		ArrayList<PublishItem> arr = new ArrayList<PublishItem>();
		DatasetVector uploadedVector = null;
		Recordset rs = null;
		try {
//         reverse(split_part(reverse(data_name),'_',2))
//         String QueryParams = "data_name like '%" + disasterType + "%' and status ='S'";
			String QueryParams = "reverse(split_part(reverse(data_name),'_',2)) like '%" + disasterType
					+ "%' and status ='S' and group_id ='2' and publish_date is not null and b_left is not null ";
			//QueryParams += "and (publish_date between '" + fDate + "' and '" + tDate + "')";
			QueryParams += "and publish_date > (current_date - interval '" + searchDate + " day') order by publish_date desc";

			uploadedVector = (DatasetVector) source.getDatasets().get(config.getPostgre().getTablename());
			rs = uploadedVector.query(QueryParams, CursorType.STATIC);

			rs.moveFirst();
//			int maxCnt = rs.getRecordCount() <= 20 ? rs.getRecordCount() : 20;
			
			for (int idx = 0; idx < rs.getRecordCount(); idx++) {
//			for (int idx = 0; idx < maxCnt; idx++) {
				String file_path = rs.getObject("file_path").toString();
				String data_name = rs.getObject("data_name").toString();
				String datasource_path = rs.getObject("datasource_path").toString();
				String data_type = rs.getObject("data_type").toString();

				String rgb_band_idx = "";
				
				PublishItem item = new PublishItem(file_path, data_name, datasource_path, rgb_band_idx, data_type);
				
				if(data_type.equals("tif")) 
				{
					item.setRgb_band_idx(rs.getObject("RGB_BAND_IDX").toString());
					item.setRgb_band_count(rs.getObject("RGB_BAND_COUNT").toString());
				}
				
				boolean check = false;
				
				if(arr.size() > 1) {
					for (PublishItem publishItem : arr) {
						if(publishItem.getData_name().equals(data_name)) {
							check = true;		
						}
					}
					
					if(!check)
						arr.add(item);
				}
				else {
					arr.add(item);
				}
				
				if(arr.size() == 20)
					break;
				
//				arr.add(item);

				rs.moveNext();
			}
		} catch (Exception e) {
			// TODO: handle exception
		} finally {
			uploadedVector.close();
			rs.close();
		}
		return arr;
	}
	
	private PublishItem publishMapService(String workspacePath, String mapName, String disasterType, String type) {
		PublishItem rslt = new PublishItem();

		try {
			String mapServiceJSON = null;

			String token = service.getToken(config.getIServer().getUser(), config.getIServer().getPassword());
			System.out.println("TOKEN :" + token);
			mapServiceJSON = service.createSerivce(token, workspacePath);
			System.out.println("WORKSPACE :" + workspacePath + "\t\t PUBLISH RSLT : " + mapServiceJSON);

			if (!mapServiceJSON.equals("")) {
				mapServiceJSON = mapServiceJSON.substring(1, mapServiceJSON.length() - 1);
				JSONObject item = new JSONObject(mapServiceJSON);
				
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
	
	private boolean setRecordsetUpdate(String pk, DatasetVector dataset, String data_type, PublishItem rsltObj, boolean isPublish) {
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
	
	private boolean setSLRecordtUpdate(PublishItem transManageObj, String commonNm, String disasterType, Datasource source) {
		DatasetVector dataset = (DatasetVector) source.getDatasets().get("SERVICE_LIST");
		Recordset rs = dataset.query("", CursorType.DYNAMIC);
		try {
			System.out.println(rs.getRecordCount());

			Recordset.BatchEditor editor = rs.getBatch();
			editor.begin();

			rs.addNew(null);
			rs.setObject("SERVICE_URL", transManageObj.getMapServiceURL());
			rs.setObject("STATUS", ServiceStatus.Service.getStatus());
			rs.setObject("TYPE", commonNm.equals("DEFAULT") ? commonNm : "");
			rs.setObject("DISASTER_TYPE", commonNm.equals("DEFAULT") ? "" : disasterType);
			rs.setObject("WORKSPACE_PATH", transManageObj.getWorkspacePath());
			rs.setObject("GENERATED_DATE", new Date());
			editor.update();
			System.out.println("SUCCESS SERVICELIST DATA UPDATE");
		} catch (Exception e) {
			// TODO: handle exception
		} finally {
			rs.close();
			dataset.close();
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
				//Stream<Entry<Long, String>> obj =  hashItemList.entrySet().stream().filter(map -> map.getValue().toString().startsWith(key));
				Map<Long, String> typeMap =  hashItemList.entrySet().stream().filter(map -> map.getValue().toString().startsWith(key)).collect(Collectors.toMap(map -> map.getKey(), map -> map.getValue()));
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
