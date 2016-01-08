/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.runtime;


import java.util.List;

import io.bigdime.core.runtimeinfo.RuntimeInfo;
import io.bigdime.runtimeinfo.DTO.RuntimeInfoDTO;

public interface ObjectEntityMapper {
	
	public List<RuntimeInfo> mapObjectList(List<RuntimeInfoDTO> runtimeInfoDAO);
	
	public RuntimeInfo mapObject(RuntimeInfoDTO runtimeInfoDAO);
	
	public RuntimeInfoDTO mapEntityObject(RuntimeInfo runtimeInfo);

}
