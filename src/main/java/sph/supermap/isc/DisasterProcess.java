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

public class DisasterProcess {

	private static DisasterProcess process = null;

	private static Settings config = null;

	private static SphService service = new SphService();
	private static SphManangeAgent agent = SphManangeAgent.getInstance();
	private static SphDataManage manage = null;

	private static String fileDir = "/data/workspaces";
	private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss");

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		ServiceStatus status = ServiceStatus.Fail;
		process = new DisasterProcess();
		config = new Config().settings;
		manage = new SphDataManage();

		if (args.length == 0)
			return;
		/**
		 * divisionCD 1. CAL 2. SEQ
		 * 
		 * disasterType 1. divisionCD == CAL -> ex) FFIRE 2. divisionCD == SEQ -> ex) ""
		 */
		System.out.println("ARGS LENGTH : " + args.length);
		String divisionCD = args[0];
		String disasterType = args.length == 1 ? "" : args[1];

		System.out.println(divisionCD);
		System.out.println(disasterType);

		Datasource postDS = agent.connectionPostgre(config.getPostgre().getId(), config.getPostgre().getPassword(),
				config.getPostgre().getServer() + ":" + config.getPostgre().getPort(), config.getPostgre().getName(),
				false);

		/**
		 * query기준은 config.xml 파일에 disasterquery 값을 기준으로 조회됨.
		 * qCnt는 DEFAULT를 생성하는데 몇개의 재난유형을 사용할지 config.xml 파일에 disastercount 값을 기준으로 생성됨.
		 * 서울대에서는 김덕진 교수님이 5개로 정해주셧음
		 * 5개일 경우 disastercount 6 ...
		 * 
		 * arrItem에서 조회할때 type_order < 6  값으로 조회됨.
		 */
		String qColumn = config.getPostgre().getDisasterquery();
		String qCnt = config.getPostgre().getDisastercount();
		ArrayList<Disaster> arrItem = agent.getDisasterInfos(postDS, config.getPostgre().getDisaster(),
				qColumn + "<" + qCnt);

		//divisionCD[구분 값 코드] 가 SEQ 일 경우 재난유 형의 순서가 변경되었음
		//재난유형의 순서가 변경되었을때는 arrItem에 조회 된 값을 기준으로 새로운 재난유형 리스트를 생성
		//arrItem의 재난유형이 모두 생성 된 후 DEFAULT 유형을 생성하게됨
		// 재난유형, DEFAULT 가 모두 생성되면 Shell Script 결과로 'S' or 'F' 를 출력.
		if (divisionCD.toUpperCase().equals("SEQ")) {
			// 1. 유형별 Workspace 생성 후 맵 서비스 발행
			// 2. DEFAULT Workspace 생성 후 맵 서비스 발행

			boolean disaserRslt = process.generateDisasterItem(postDS, arrItem);
			boolean commonRslt = process.generateDefaultItem(postDS, arrItem);

			if (!disaserRslt)
				System.out.println("DISASTER WORKSPACE GENERATE FAILED");

			if (!commonRslt)
				System.out.println("COMMON WORKSPACE GENERATE FAILED");

			// 둘 다 성공 시 Success
			if (disaserRslt && commonRslt)
				System.out.println(ServiceStatus.Service.getStatus());
			else
				System.out.println(ServiceStatus.Fail.getStatus());
		} else {
			//divisionCD[구분 값]이 "CAL"일 경우 재난유형의 날짜가 변경되었음을 뜻함.
			//arrItem에서 조회된 재난유형과, 날짜가 변경된 재난유형의 날짜를 검색 
			String searchDate = "";
			for (Disaster item : arrItem) {
				if (item.getDisaster_type().equals(disasterType)) {
					searchDate = item.getSearch_date();
					break;
				}
			}
			
			//검색일 기준으로 데이터를 검색하고 해당 재난유형의 Map Service를 생성
			boolean rslt = process.generateDisasterItem(postDS, searchDate, disasterType, disasterType);
			
			//MapService생성 작업이 완료되면 해당 결과에 따라서 Shell에 'S' or 'F' 출력.
			if (rslt)
				System.out.println(ServiceStatus.Service.getStatus());
			else
				System.out.println(ServiceStatus.Fail.getStatus());
		}
	}

	private boolean generateDefaultItem(Datasource postDS, ArrayList<Disaster> arrItem) {
		boolean rslt = false;
		try {
			String commonNm = "DEFAULT";
			String workspaceName = commonNm + SphUtil.getName();
			String liveWorkspacePath = "/data/workspaces/" + workspaceName + ".smwu";
			agent.createWorkspace(agent.setWorkspaceConnectionInfo(liveWorkspacePath, workspaceName));
			agent.CreateNewMap(commonNm, "3857");

			ArrayList<PublishItem> publishItems = new ArrayList<>();

			//재난유형에 GROUP_ID 값이 2, Status 값이 'S' 인 데이터 조회
			for (Disaster disaster : arrItem) {
				String queryParams = "reverse(split_part(reverse(data_name),'_',2)) LIKE '%"
						+ disaster.getDisaster_type() + "%' and status = 'S' and group_id ='2' ";
//				queryParams += "ORDER BY publish_date DESC LIMIT 2 ";
				queryParams += "ORDER BY publish_date DESC";
				agent.getDisasterTypeItem(publishItems, postDS, config.getPostgre().getTablename(), queryParams);
			}

			//재난유형 List loof
			for (PublishItem publishItem : publishItems) {
				Datasource source = null;
				String dt = publishItem.getData_type();
				System.out.println("PATH : " + publishItem.getDatasource_path());
				System.out.println("DATA NAME : " + publishItem.getData_name());

				
				source = agent.openDatasource(publishItem.getDatasource_path(), publishItem.getData_name(), DatasourceType.UDBX, false);
				if (dt.equals("tif")) {
					if (!publishItem.getRgb_band_count().equals("1")) {
						System.out.println("TIF DATASETIMAGE");
						DatasetImage image = (DatasetImage) source.getDatasets().get(0);
						agent.addLayerFromDatasetImage(image, SphUtil.parseBandIndexes(publishItem.getRgb_band_idx()),
								publishItem.getData_name(), commonNm);
					} else {
						System.out.println("TIF DATASET GRID");
						DatasetGrid grid = (DatasetGrid) source.getDatasets().get(0);
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

			PublishItem transManageObj = process.publishMapService(liveWorkspacePath, commonNm, commonNm, "");
			if (transManageObj.isStatus()) {
				rslt = process.setSLRecordtUpdate(transManageObj, commonNm, "", postDS);
				process.fsDelete("DEFAULT");
			}
		} catch (Exception e) {
			// TODO: handle exception
			System.out.println(e.getMessage());
		}

		return rslt;
	}

	private boolean generateDisasterItem(Datasource postDS, ArrayList<Disaster> arrItem) {
		boolean resultStatus = false;
		try {
			for (Disaster item : arrItem) {
				String from_date = SphUtil.getDate(item.getSearch_date());
				String to_date = SphUtil.getDate();
				String disasterType = item.getDisaster_type();

				System.out.println("DISASTER TYPE : " + disasterType + "\t\t SEARCHDATE : " + item.getSearch_date()
						+ "\t\t F DATE : " + from_date + "\t\t T_DATE : " + to_date);

				ArrayList<PublishItem> arr = process.getSearchItemList(postDS, disasterType, item.getSearch_date());

				if (arr.size() > 0) {
					if (!resultStatus)
						resultStatus = process.generateDisasterItem(postDS, item.getSearch_date(), disasterType,
								disasterType);
					else
						process.generateDisasterItem(postDS, item.getSearch_date(), disasterType, disasterType);
				} else {
					resultStatus = true;
				}

			}
		} catch (Exception e) {
			// TODO: handle exception
		}
		return resultStatus;
	}

	private boolean generateDisasterItem(Datasource postDS, String searchDate, String commonNm, String disasterType) {
		boolean resultStatus = false;
		try {
			ArrayList<PublishItem> arr = process.getSearchItemList(postDS, disasterType, searchDate);
			System.out.println("########################");
			System.out.println(disasterType + " \t\t\t " + arr.size());
			System.out.println("########################");

			if (arr.size() == 0)
				return true;

			String disasterWorkspacePath = "/data/workspaces/" + commonNm + SphUtil.getName() + ".smwu";
			agent.createWorkspace(
					agent.setDisasterWorkspaceConnectionInfo(disasterWorkspacePath, disasterType + SphUtil.getName())
			);
			agent.CreateNewMap(commonNm, "3857");

			for (PublishItem publishItem : arr) {
				Datasource source = agent.openDatasource(publishItem.getDatasource_path(), publishItem.getData_name(),
						DatasourceType.UDBX, false);

				String dttype = publishItem.getData_type();
				if (dttype.equals("tif")) {
					if (!publishItem.getRgb_band_count().equals("1")) {
						DatasetImage image = (DatasetImage) source.getDatasets().get(0);
						agent.addLayerFromDatasetImage(image, SphUtil.parseBandIndexes(publishItem.getRgb_band_idx()),
								publishItem.getData_name(), commonNm);
					} else {
						DatasetGrid grid = (DatasetGrid) source.getDatasets().get(0);
						agent.addLayerFromSingleBandImage(grid, publishItem.getData_name(), commonNm);
					}
				} else if (dttype.equals("kmz")) {
					DatasetImage image = (DatasetImage) source.getDatasets().get(0);
					agent.addLayerFromDatasetImageKMZ(image, publishItem.getData_name(), commonNm);
				} else {
					DatasetVector vector = (DatasetVector) source.getDatasets().get(0);
					agent.addLayerFromDatasetVector(vector, publishItem.getData_name(), commonNm);
				}
			}

			if (arr.size() == 0) {
				agent.dummyMap(commonNm);
			}
			agent.ReleaseInstance();
			System.out.println("PUBLISH MAP NAME : " + disasterType);
			PublishItem transManageObj = process.publishMapService(disasterWorkspacePath, disasterType, null,
					disasterType);
			resultStatus = process.setSLRecordtUpdate(transManageObj, commonNm, disasterType, postDS);

			process.fsDelete(disasterType);

		} catch (Exception e) {
			// TODO: handle exception
		}
		return resultStatus;
	}

	private ArrayList<PublishItem> getSearchItemList(Datasource source, String disasterType, String searchDate) {
		ArrayList<PublishItem> arr = new ArrayList<PublishItem>();
		DatasetVector uploadedVector = null;
		Recordset rs = null;
		try {
			String QueryParams = "reverse(split_part(reverse(data_name),'_',2)) like '%" + disasterType
					+ "%' and status ='S' and group_id ='2' and publish_date is not null and b_left is not null ";
			QueryParams += "and publish_date > (current_date - interval '" + searchDate
					+ " day') order by publish_date desc";

			System.out.println("START####SELECT QUERY WHERE#### ");
			System.out.println(QueryParams);
			System.out.println("END ####SELECT QUERY WHERE#### ");

			uploadedVector = (DatasetVector) source.getDatasets().get(config.getPostgre().getTablename());
			rs = uploadedVector.query(QueryParams, CursorType.STATIC);

			rs.moveFirst();
			for (int idx = 0; idx < rs.getRecordCount(); idx++) {
				String file_path = rs.getObject("file_path").toString();
				String data_name = rs.getObject("data_name").toString();
				String datasource_path = rs.getObject("datasource_path").toString();
				String data_type = rs.getObject("data_type").toString();

				String rgb_band_idx = "";

				PublishItem item = new PublishItem(file_path, data_name, datasource_path, rgb_band_idx, data_type);

				if (data_type.equals("tif")) {
					item.setRgb_band_idx(rs.getObject("RGB_BAND_IDX").toString());
					item.setRgb_band_count(rs.getObject("RGB_BAND_COUNT").toString());
				}

				boolean check = false;

				if (arr.size() > 1) {
					for (PublishItem publishItem : arr) {
						if (publishItem.getData_name().equals(data_name)) {
							check = true;
						}
					}

					if (!check)
						arr.add(item);
				} else {
					arr.add(item);
				}

				if (arr.size() == 20)
					break;

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
			mapServiceJSON = service.createSerivce(token, workspacePath);

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

	private boolean setSLRecordtUpdate(PublishItem transManageObj, String commonNm, String disasterType,
			Datasource source) {
		DatasetVector dataset = (DatasetVector) source.getDatasets().get("SERVICE_LIST");
		String queryParams = commonNm.toUpperCase().equals("DEFAULT") ? "TYPE = '" + commonNm + "'"
				: "DISASTER_TYPE = '" + disasterType + "'";
		System.out.println("QUERY PARAMS : " + queryParams);

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
			System.out.println("RECORD COUNT : " + rs.getRecordCount());
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
			fileList = walk.filter(path -> path.getFileName().toString().startsWith(typeNm))
					.collect(Collectors.toList());
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
				// Stream<Entry<Long, String>> obj = hashItemList.entrySet().stream().filter(map
				// -> map.getValue().toString().startsWith(key));
				Map<Long, String> typeMap = hashItemList.entrySet().stream()
						.filter(map -> map.getValue().toString().startsWith(key))
						.collect(Collectors.toMap(map -> map.getKey(), map -> map.getValue()));
				System.out.println("ServiceCount : " + typeMap.size());

				if (typeMap.size() > 1) {
					long min = Collections.min(typeMap.keySet());
					for (Long secKey : typeMap.keySet()) {
						if (secKey != min) {
							System.out.println("DELETE FILE LIST : " + typeMap.get(secKey) + "\t\t" + key);
							service.deleteService(token, fileDir + "/" + typeMap.get(secKey));
							manage.removeFile(fileDir + "/" + typeMap.get(secKey));
						}
					}
				}

			}

		} catch (Exception e) {
			// TODO: handle exception
		}

	}
}
