/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.adaptor.metadata;

import io.bigdime.adaptor.metadata.model.Entitee;
import io.bigdime.adaptor.metadata.model.Metasegment;

import java.util.List;
import java.util.Set;

/**
 * Interface MetadataStore This interface defines the method which are exposed
 * to end client.
 * 
 * @author Neeraj Jain, psabinikari
 * 
 */
public interface MetadataStore {

	/**
	 * Inserts/Updates MetaSchema details into the datastore and cache
	 * 
	 * @param metasegment
	 */

	public void put(Metasegment metasegment) throws MetadataAccessException;

	/**
	 * Get Adaptor Metasegment details for a given adaptor.
	 * 
	 * @param adaptorName
	 * @param schemaType
	 * @param entityName
	 * @return
	 */
	public Metasegment getAdaptorMetasegment(String adaptorName,
			String schemaType, String entityName)
			throws MetadataAccessException;

	/**
	 * Get Adaptor entity details for a given adaptor.
	 * 
	 * @param adaptorName
	 * @param schemaType
	 * @param entityName
	 * @return
	 */
	public Entitee getAdaptorEntity(String adaptorName, String schemaType,
			String entityName) throws MetadataAccessException;

	/**
	 * Get distinct data sources list.
	 * 
	 * @return
	 */
	public Set<String> getDataSources() throws MetadataAccessException;

	/**
	 * Get schema list for a given adaptor.
	 * 
	 * @param adaptorName
	 * @param schemaType
	 * @return
	 */
	public List<Metasegment> getAdaptorMetasegments(String adaptorName,
			String schemaType) throws MetadataAccessException;

	/**
	 * Get entity list for a given Adaptor.
	 * 
	 * @param adaptorName
	 * @param schemaType
	 * @return
	 */
	public Set<String> getAdaptorEntityList(String adaptorName,
			String schemaType) throws MetadataAccessException;

	/**
	 * returns all the entity objects for a given adaptor name and schema type
	 * 
	 * @param adaptorName
	 * @param schemaName
	 * @return unique entities for a given adaptor name and schema type.
	 */
	public Set<Entitee> getAdaptorEntities(String adaptorName, String schemaName)
			throws MetadataAccessException;

	public void remove(Metasegment metasegment) throws MetadataAccessException;

	public List<Metasegment> createDatasourceIfNotExist(String adaptorName,
			String schemaType) throws MetadataAccessException;


}
