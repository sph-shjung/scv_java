package sph.supermap.isc.util;

import com.supermap.data.EngineType;

public enum DatasourceType {

	Raster(EngineType.IMAGEPLUGINS), Vector(EngineType.VECTORFILE), UDB(EngineType.UDB), UDBX(EngineType.UDBX);
	
	private EngineType engineType;
	
	private DatasourceType(EngineType type) {
		engineType = type;
	}
	
	public EngineType getEngineType() {
		return engineType;
	}
}
