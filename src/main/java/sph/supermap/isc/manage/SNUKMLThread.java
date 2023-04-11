package sph.supermap.isc.manage;

import java.awt.Color;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;

import javax.xml.namespace.QName;

import org.geotools.kml.KML;
import org.geotools.kml.v22.KMLConfiguration;
import org.geotools.xml.PullParser;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;

import com.supermap.data.CoordSysTransParameter;
import com.supermap.data.CursorType;
import com.supermap.data.DatasetVector;
import com.supermap.data.GeoPoint;
import com.supermap.data.GeoStyle;
import com.supermap.data.PrjCoordSys;
import com.supermap.data.Recordset;
import com.supermap.data.Size2D;
import com.supermap.data.Recordset.BatchEditor;

public class SNUKMLThread extends Thread{
	private String filePath;
	private Recordset recordset;
	private DatasetVector vector;
	private BatchEditor editor;
	//private SuperMapAgent agent;
	private SphManangeAgent agent;
	private ArrayList<Thread> threadList;
	public SNUKMLThread(String filePath, DatasetVector vector, SphManangeAgent agent) {
		this.filePath = filePath;
		this.vector = vector;
		this.recordset = vector.getRecordset(true, CursorType.DYNAMIC);
		this.editor = recordset.getBatch();
		this.editor.setMaxRecordCount(1000);
		this.editor.begin();
		this.agent = agent;
	}
	
	public SNUKMLThread(String filePath, DatasetVector vector, SphManangeAgent agent, ArrayList<Thread> threadList) {
		this.filePath = filePath;
		this.vector = vector;
		this.recordset = vector.getRecordset(true, CursorType.DYNAMIC);
		this.editor = recordset.getBatch();
		this.editor.setMaxRecordCount(1000);
		this.editor.begin();
		this.agent = agent;
		this.threadList = threadList;
	}
	
	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}
	
	public void run() {
		FileInputStream fis = null;
		PrjCoordSys sourcePrj = new PrjCoordSys(4326);
		PrjCoordSys targetPrj = new PrjCoordSys(3857);
		CoordSysTransParameter param = new CoordSysTransParameter();
		try {
			fis = new FileInputStream(filePath);
			QName[] qNames = new QName[] {KML.IconStyle,KML.Placemark};
			PullParser parser = new PullParser(new KMLConfiguration(), fis, qNames);
			Object obj = null;
			try {
				GeoStyle style = new GeoStyle();
				style.setMarkerSymbolID(323);
				style.setMarkerSize(new Size2D(3, 3));
				while((obj = parser.parse()) != null) {						
					if(obj instanceof SimpleFeature) {						
						SimpleFeature f = (SimpleFeature)obj;
						Geometry jtsGeom = (Geometry)f.getAttribute("Geometry");
						com.supermap.data.Geometry smGeom = new GeoPoint(jtsGeom.getCoordinate().getX(), jtsGeom.getCoordinate().getY());
						
						smGeom = agent.CoordSysTranslator(smGeom, sourcePrj);
						smGeom.setStyle(style);
						recordset.addNew(smGeom);
						recordset.setObject("description", f.getAttribute("description"));
						recordset.setObject("zvalue", jtsGeom.getCoordinate().getZ());
					}else if(obj instanceof Color) {
						Color c = (Color)obj;
						style.setLineColor(new Color(c.getBlue(), c.getGreen(), c.getRed()));
					}
				}
			}catch(Exception e) {
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}finally {
			if(fis != null) {
				try {
					fis.close();
					this.editor.update();
					this.recordset.close();
					threadList.remove(this);
				} catch (IOException e) {
				}
			}
		}
	}
}
