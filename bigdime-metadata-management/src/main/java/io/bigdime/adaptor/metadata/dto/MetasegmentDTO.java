/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.adaptor.metadata.dto;

import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToMany;
import javax.persistence.Table;

import org.springframework.util.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Class Metasegment(adaptor/Database)
 * 
 * This Metasegment class represents a base-level adaptor/database details
 * available in big dime application. The Metasegment can be used to instantiate
 * a Metasegment object.
 * 
 * <pre>
 *       Example:
 * Metasegment metasegment = new Metasegment();
 * 
 * <pre>
 * 
 * @author Neeraj Jain,
 *         psabinikari
 * 
 * 
 * @version 1.0
 * 
 */
@Entity
@Table(name = "METASEGMENT")
public class MetasegmentDTO {

	/**
	 * Metasegment's primary key
	 */
	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	@Column(name = "METASEGMENT_ID")
	private Integer Id;

	/**
	 * adaptor's Name
	 */
	@Column(name = "ADAPTOR_NAME")
	private String adaptorName;

	/**
	 * Type of Schema Ex: Oracle(RDBMS) type or File type
	 */
	@Column(name = "SCHEMA_TYPE")
	private String schemaType;

	/**
	 * adaptor's data base name
	 */
	@Column(name = "DATABASE_NAME")
	private String databaseName;

	/**
	 * adaptor's data base location
	 */
	@Column(name = "DATABASE_LOCATION")
	private String databaseLocation;

	/**
	 * Represents whether source or target type of data source
	 */
	@Column(name = "SOURCE_TARGET_TYPE")
	private String repositoryType;

	/**
	 * Contains 'Y' or 'N'.Y represents data source. 'N' represents not a data
	 * source.
	 */
	@Column(name = "IS_DATASOURCE")
	private String isDataSource;

	/**
	 * Provides description for an adaptor entry.
	 */
	@Column(name = "DESCRIPTION")
	private String description;

	/**
	 * Maps adaptor to entities.
	 */
	@OneToMany(cascade = CascadeType.ALL, fetch = FetchType.EAGER)
	@JoinColumn(name = "METASEGMENT_ID")
	private Set<EntiteeDTO> entitees = new HashSet<EntiteeDTO>();

	/**
	 * adaptor entry created date information.
	 */
	@Column(name = "CREATED_AT")
	private Date createdAt;

	/**
	 * adaptor entry created by information.
	 */
	@Column(name = "CREATED_BY")
	private String createdBy;

	/**
	 * adaptor entry updated by information.
	 */
	@Column(name = "UPDATED_AT")
	private Date updatedAt;

	/**
	 * adaptor entry updated by information.
	 */
	@Column(name = "UPDATED_BY")
	private String updatedBy;

	/**
	 * Class constructor. This is default no arguments constructor
	 * 
	 * <pre>
	 *    Example: Metasegment metasegment = new Metasegment();
	 * </pre>
	 */
	public MetasegmentDTO() {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * Class constructor. This is arguments constructor.
	 * 
	 * <pre>
	 *     Example: Metasegment metasegment = new Metasegment(argument1 datatype, ....,argumentn datatype);
	 * </pre>
	 * 
	 * @param adaptorName
	 * @param schemaType
	 * @param databaseName
	 * @param databaseLocation
	 * @param repositoryType
	 * @param isDataSource
	 * @param description
	 * @param entity
	 *            Set
	 * @param createdBy
	 * @param updatedBy
	 */
	public MetasegmentDTO(String adaptorName, String schemaType,
			String databaseName, String databaseLocation,
			String repositoryType, String isDataSource, String description,
			Set<EntiteeDTO> entitees, String createdBy, String updatedBy) {
		super();

		this.adaptorName = adaptorName;
		this.schemaType = schemaType;
		this.databaseName = databaseName;
		this.databaseLocation = databaseLocation;
		this.repositoryType = repositoryType;
		this.isDataSource = isDataSource;
		this.description = description;
        this.createdAt = new Date();
		this.createdBy = createdBy;
		this.updatedAt = new Date();
		this.updatedBy = updatedBy;
		Iterator<EntiteeDTO> iterator =entitees.iterator();
	    while(iterator.hasNext()){
	    	this.entitees.add(iterator.next().clone());
	    }
	}

	/**
	 * Get Id of an adaptor.
	 * 
	 * @return integer which is primary key.
	 */
	@JsonIgnore
	public Integer getId() {
		return Id;
	}

	/**
	 * Set Id for an adaptor
	 * 
	 * @param id
	 */
	public void setId(Integer id) {
		Id = id;
	}

	/**
	 * Get adaptor Name
	 * 
	 * @return String which is an application name.
	 */
	public String getAdaptorName() {
		return adaptorName;
	}

	/**
	 * Set adaptor Name
	 * 
	 * @param applicationName
	 */
	public void setAdaptorName(String adaptorName) {
		this.adaptorName = adaptorName;
	}

	/**
	 * Get Schema Type(Ex: whether oracle, file type)
	 * 
	 * @return String
	 */
	public String getSchemaType() {
		return schemaType;
	}

	/**
	 * Set Schema Type(Ex: whether oracle, file type)
	 * 
	 * @param schemaType
	 */
	public void setSchemaType(String schemaType) {
		this.schemaType = schemaType;
	}

	/**
	 * Get adaptor data base name.
	 * 
	 * @return
	 */
	public String getDatabaseName() {
		return databaseName;
	}

	/**
	 * Set adaptor data base name.
	 * 
	 * @param databaseName
	 */
	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	/**
	 * Get adaptor data base location.
	 * 
	 * @return String
	 */
	public String getDatabaseLocation() {
		return databaseLocation;
	}

	/**
	 * Set adaptor data base location.
	 * 
	 * @param databaseLocation
	 */
	public void setDatabaseLocation(String databaseLocation) {
		this.databaseLocation = databaseLocation;
	}

	/**
	 * Get adaptor source or target type.
	 * 
	 * @return String
	 */
	public String getRepositoryType() {
		return repositoryType;
	}

	/**
	 * Set adaptor source or target type.
	 * 
	 * @param repositoryType
	 */
	public void setRepositoryType(String repositoryType) {
		this.repositoryType = repositoryType;
	}

	/**
	 * Get isDataSource(Values are 'Y' or 'N') Y represent data source type. N
	 * represents not a data source type. For Example: Alert is not a data
	 * source type.
	 * 
	 * @return String
	 */
	public String getIsDataSource() {
		return isDataSource;
	}

	/**
	 * Set isDataSource(Values are 'Y' or 'N') Y represents data source. N
	 * represents not a data source type.
	 * 
	 * @param isDataSource
	 */
	public void setIsDataSource(String isDataSource) {
		this.isDataSource = isDataSource;
	}

	/**
	 * Gets set of entities associated with Metasegment
	 * 
	 * @return
	 */
	public Set<EntiteeDTO> getEntitees() {
		Set<EntiteeDTO> entitees=new HashSet<EntiteeDTO>();
		Iterator<EntiteeDTO> iterator =this.entitees.iterator();
	    while(iterator.hasNext()){
	    	entitees.add(iterator.next().clone());
	    }
		return entitees;
	}

	/**
	 * Set entities associated with Metasegment
	 * 
	 * @param entities
	 */
	public void setEntitees(Set<EntiteeDTO> entitees) {
		Iterator<EntiteeDTO> iterator =entitees.iterator();
	    while(iterator.hasNext()){
	    	this.entitees.add(iterator.next().clone());
	    }
	}

	/**
	 * Get Description for an adaptor entry.
	 * 
	 * @return String
	 */
	public String getDescription() {
		return description;
	}

	/**
	 * Set Description for an adaptor entry.
	 * 
	 * @param description
	 */
	public void setDescription(String description) {
		this.description = description;
	}

	/**
	 * Get Created date for an adaptor.
	 * 
	 * @return Date
	 */
	public Date getCreatedAt() {
		return (Date) createdAt.clone();
	}

	/**
	 * Set Created date for an adaptor
	 * 
	 * @param createdAt
	 */
	public void setCreatedAt(Date createdAt) {
		 this.createdAt = (Date) createdAt.clone();
		//this.createdAt = new Date();
	}

	/**
	 * Get Create by for an adaptor.
	 * 
	 * @return String
	 */
	public String getCreatedBy() {
		return createdBy;
	}

	/**
	 * Set Created by for an adaptor.
	 * 
	 * @param createdBy
	 */
	public void setCreatedBy(String createdBy) {
		this.createdBy = createdBy;
	}

	/**
	 * Get Updated date for an adaptor
	 * 
	 * @return Date
	 */
	public Date getUpdatedAt() {
		return (Date) updatedAt.clone();
	}

	/**
	 * Set updated date for an adaptor
	 * 
	 * @param updatedAt
	 */
	public void setUpdatedAt(Date updatedAt) {
		 this.updatedAt = (Date) updatedAt.clone();
		//this.updatedAt = new Date();
	}

	/**
	 * Get updated by for an adaptor
	 * 
	 * @return String
	 */
	public String getUpdatedBy() {
		return updatedBy;
	}

	/**
	 * Set Updated by for adaptor.
	 * 
	 * @param updatedBy
	 */
	public void setUpdatedBy(String updatedBy) {
		this.updatedBy = updatedBy;
	}

	/**
	 * This method gets the given entity details for current metasegment object.
	 * 
	 * @param entityName
	 * @return Entitee object if exists else null object.
	 */
	public EntiteeDTO getEntity(String entityName) {

		if (StringUtils.hasText(entityName)) {
			if(this.getEntitees() != null)
			for (final EntiteeDTO entity : this.getEntitees()) {
				if (entity.getEntityName().equalsIgnoreCase(entityName))
					return entity;
			}
		}
		return null;

	}

	@Override
	public String toString() {
		return "Metasegment [Id=" + Id + ", adaptorName=" + adaptorName
				+ ", schemaType=" + schemaType + ", databaseName="
				+ databaseName + ", databaseLocation=" + databaseLocation
				+ ", repositoryType=" + repositoryType + ", isDataSource="
				+ isDataSource + ", description=" + description + ", entitees="
				+ entitees + ", createdAt=" + createdAt + ", createdBy="
				+ createdBy + ", updatedAt=" + updatedAt + ", updatedBy="
				+ updatedBy + "]";
	}

   

}

