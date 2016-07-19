/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.runtimeinfo.impl;

import java.util.List;

import io.bigdime.runtimeinfo.DTO.RuntimeInfoDTO;
import io.bigdime.runtimeinfo.DTO.RuntimePropertyDTO;
import io.bigdime.runtimeinfo.repositories.RuntimeInfoRepository;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import io.bigdime.alert.LoggerFactory;
import io.bigdime.alert.Logger;

@Component
public class RuntimeInfoRepositoryService {

	private static Logger logger = LoggerFactory
			.getLogger(RuntimeInfoRepositoryService.class);

	private static final String SOURCENAME = "RUNTIME_INFO-API";

	@Autowired
	private RuntimeInfoRepository runtimeInfoRepository;

	public synchronized boolean create(RuntimeInfoDTO adaptorRuntimeInfo) {
		boolean isCreatedOrUpdated = false;
		Assert.notNull(adaptorRuntimeInfo);
		RuntimeInfoDTO adaptorRuntimeInformation = runtimeInfoRepository
				.findByAdaptorNameAndEntityNameAndInputDescriptor(
						adaptorRuntimeInfo.getAdaptorName(),
						adaptorRuntimeInfo.getEntityName(),
						adaptorRuntimeInfo.getInputDescriptor());
		if (adaptorRuntimeInformation == null) {
			logger.debug(SOURCENAME, "creating new Runtime Info entry",
					"Creaiting new Runtime Info enrty for adaptorName: {}",
					adaptorRuntimeInfo.getAdaptorName());
			adaptorRuntimeInfo.setCreatedAt();
			adaptorRuntimeInfo.setUpdatedAt();
			runtimeInfoRepository.save(adaptorRuntimeInfo);
			isCreatedOrUpdated = true;
		} else {
			if(adaptorRuntimeInfo.getRuntimeProperties()!=null)
			for(RuntimePropertyDTO runtimePropertyDTO: adaptorRuntimeInfo.getRuntimeProperties()) {
			logger.debug(SOURCENAME, "Updating existing Runtime Info entry",
					"Updating existing Runtime Info entry for adaproName:{}",
					adaptorRuntimeInfo.getAdaptorName());
			RuntimeInfoDTO repoAdaptorRuntimeInformation = runtimeInfoRepository
					.findByAdaptorNameAndEntityNameAndInputDescriptor(
							adaptorRuntimeInfo.getAdaptorName(),
							adaptorRuntimeInfo.getEntityName(),
							adaptorRuntimeInfo.getInputDescriptor());
			
			repoAdaptorRuntimeInformation.setAdaptorName(adaptorRuntimeInfo.getAdaptorName());
			repoAdaptorRuntimeInformation.setEntityName(adaptorRuntimeInfo.getEntityName());
			repoAdaptorRuntimeInformation.setInputDescriptor(adaptorRuntimeInfo.getInputDescriptor());
			repoAdaptorRuntimeInformation.setNumOfAttempts(adaptorRuntimeInfo.getNumOfAttempts());
			repoAdaptorRuntimeInformation.setStatus(adaptorRuntimeInfo.getStatus());
			
			if(repoAdaptorRuntimeInformation.getRuntimeProperties()!= null
					 && repoAdaptorRuntimeInformation.getRuntimeProperties().size() > 0) {
				for(RuntimePropertyDTO repoRuntimePropertyDTO: repoAdaptorRuntimeInformation.getRuntimeProperties()){
					if(repoRuntimePropertyDTO.getKey().equalsIgnoreCase(runtimePropertyDTO.getKey())){
						runtimePropertyDTO.setRuntimePropertyId(repoRuntimePropertyDTO.getRuntimePropertyId());
						repoAdaptorRuntimeInformation.getRuntimeProperties().remove(repoRuntimePropertyDTO);
						break;
					}
				}
			}
			
			repoAdaptorRuntimeInformation.getRuntimeProperties().add(runtimePropertyDTO);
			repoAdaptorRuntimeInformation.setUpdatedAt();
			runtimeInfoRepository.save(repoAdaptorRuntimeInformation);
			isCreatedOrUpdated = true;
			}
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
		return runtimeInfoRepository.findByAdaptorNameAndEntityName(
				adaptorName, entityName);

	}

	public RuntimeInfoDTO get(String adaptorName, String entityName,
			String descriptor) {

		Assert.notNull(adaptorName);
		Assert.notNull(entityName);
		Assert.notNull(descriptor);

		return runtimeInfoRepository
				.findByAdaptorNameAndEntityNameAndInputDescriptor(adaptorName,
						entityName, descriptor);

	}

	public RuntimeInfoDTO getLatestRecord(String adaptorName, String entityName) {
		Assert.notNull(adaptorName);
		Assert.notNull(entityName);
		return runtimeInfoRepository
				.findFirstByAdaptorNameAndEntityNameOrderByRuntimeIdDesc(
						adaptorName, entityName);
	}

}