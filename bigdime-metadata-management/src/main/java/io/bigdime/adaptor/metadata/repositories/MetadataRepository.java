/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.adaptor.metadata.repositories;

import java.util.List;
import java.util.Set;

import io.bigdime.adaptor.metadata.dto.MetasegmentDTO;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Transactional;

/**
 * Class MetadataRepository
 * 
 * This is a repository for Metasegment which extends JpaRepository. It provides
 * an elegant way to access the Metasegment table available in repository.
 * 
 * @author Neeraj jain, psabinikari
 * @version 1.0
 * 
 */
@Transactional
public interface MetadataRepository extends JpaRepository<MetasegmentDTO, Integer> {

	MetasegmentDTO findByAdaptorNameAndSchemaType(String adaptorName,
			String schemaType);

	@Query("select distinct m.adaptorName from MetasegmentDTO m")
	Set<String> findAllDataSources();
	
	List<MetasegmentDTO> findByAdaptorName(String adaptorName);

}
