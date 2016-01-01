/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.runtimeinfo.impl;

import java.util.List;

import io.bigdime.runtimeinfo.DTO.RuntimeInfoDTO;
import io.bigdime.runtimeinfo.repositories.RuntimeInfoRepository;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import io.bigdime.alert.LoggerFactory;
import io.bigdime.alert.Logger;

@Component
public class RuntimeInfoRepositoryService {

	private Logger logger = LoggerFactory.getLogger(RuntimeInfoRepositoryService.class);

	private static final String SOURCENAME = "RUNTIME_INFO-API";

	@Autowired
	private RuntimeInfoRepository runtimeInfoRepository;

	public boolean create(RuntimeInfoDTO adaptorRuntimeInfo) {
		boolean isCreatedOrUpdated = false;
		Assert.notNull(adaptorRuntimeInfo);
		RuntimeInfoDTO adaptorRuntimeInformation = runtimeInfoRepository
				.findByAdaptorNameAndEntityNameAndInputDescriptor(adaptorRuntimeInfo.getAdaptorName(),
						adaptorRuntimeInfo.getEntityName(), adaptorRuntimeInfo.getInputDescriptor());
		if (adaptorRuntimeInformation == null) {
			logger.debug(SOURCENAME, "creating new Runtime Info entry",
					"Creaiting new Runtime Info enrty for adaptorName: {}", adaptorRuntimeInfo.getAdaptorName());
			adaptorRuntimeInfo.setCreatedAt();
			adaptorRuntimeInfo.setUpdatedAt();
			runtimeInfoRepository.save(adaptorRuntimeInfo);
			isCreatedOrUpdated = true;
		} else {
			logger.debug(SOURCENAME, "Updating existing Runtime Info entry",
					"Updating existing Runtime Info entry for adaproName:{}", adaptorRuntimeInfo.getAdaptorName());
			adaptorRuntimeInformation.setAdaptorName(adaptorRuntimeInfo.getAdaptorName());
			adaptorRuntimeInformation.setEntityName(adaptorRuntimeInfo.getEntityName());
			adaptorRuntimeInformation.setInputDescriptor(adaptorRuntimeInfo.getInputDescriptor());
			adaptorRuntimeInformation.setNumOfAttempts(adaptorRuntimeInfo.getNumOfAttempts());
			adaptorRuntimeInformation.setStatus(adaptorRuntimeInfo.getStatus());
			adaptorRuntimeInformation.setRuntimeProperties(adaptorRuntimeInfo.getRuntimeProperties());
			adaptorRuntimeInformation.setUpdatedAt();
			runtimeInfoRepository.save(adaptorRuntimeInformation);
			isCreatedOrUpdated = true;
		}
		return isCreatedOrUpdated;

	}

	/*
	 * public boolean isExists(String adaptorName, String entityName) {
	 * 
	 * Assert.notNull(adaptorName); Assert.notNull(entityName);
	 * 
	 * List<RuntimeInfo> adaptorRuntimeInformationList = runtimeInfoRepository
	 * .findByAdaptorNameAndEntityName(adaptorName, entityName);
	 * 
	 * if (adaptorRuntimeInformationList == null ||
	 * adaptorRuntimeInformationList.size() == 0) return false; else return
	 * true;
	 * 
	 * }
	 */

	public List<RuntimeInfoDTO> get(String adaptorName, String entityName) {

		Assert.notNull(adaptorName);
		Assert.notNull(entityName);
		return runtimeInfoRepository.findByAdaptorNameAndEntityName(adaptorName, entityName);

	}

	public RuntimeInfoDTO get(String adaptorName, String entityName, String descriptor) {

		Assert.notNull(adaptorName);
		Assert.notNull(entityName);
		Assert.notNull(descriptor);

		return runtimeInfoRepository.findByAdaptorNameAndEntityNameAndInputDescriptor(adaptorName, entityName,
				descriptor);

	}

	public RuntimeInfoDTO getLatestRecord(String adaptorName, String entityName) {
		Assert.notNull(adaptorName);
		Assert.notNull(entityName);
		return runtimeInfoRepository.findFirstByAdaptorNameAndEntityNameOrderByRuntimeIdDesc(adaptorName, entityName);
	}

}
