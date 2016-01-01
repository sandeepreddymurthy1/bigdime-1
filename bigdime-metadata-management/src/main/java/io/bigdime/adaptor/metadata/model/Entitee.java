/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.adaptor.metadata.model;

import java.util.LinkedHashSet;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Class Entitee It represents base-level entity details available in bigdime
 * application. This Entity can be used to instantiate Entity object.
 * 
 * <pre>
 *       Example:
 * Entitee entitee = new Entitee();
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
public class Entitee {

	/**
	 * Entity primary id
	 */
	private Integer id;

	/**
	 * Table Name
	 */
	private String entityName;

	/**
	 * Table Location
	 */
	private String entityLocation;

	/**
	 * Entity object version
	 */
	private double version;

	/**
	 * Description this entity object
	 */
	private String description;

	/**
	 * Attribute relation ship
	 */
	private Set<Attribute> attributes = new LinkedHashSet<Attribute>();

	/**
	 * Class constructor. This is default no arguments constructor
	 * 
	 * <pre>
	 *    Example: Entitee entitee = new Entitee();
	 * </pre>
	 */
	public Entitee() {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * Class constructor. This is arguments constructor.
	 * 
	 * <pre>
	 *     Example:  Entitee entitee = new Entitee(argument1 datatype,....,argumentn datatype)
	 * </pre>
	 * 
	 * @param metasegment
	 * @param entityName
	 * @param entityLocation
	 * @param version
	 * @param description
	 * @param attributes
	 * 
	 */
	public Entitee(String entityName, String entityLocation, double version,
			String description, Set<Attribute> attributes) {
		super();
		this.entityName = entityName;
		this.entityLocation = entityLocation;
		this.version = version;
		this.description = description;
		this.attributes = attributes;

	}

	/**
	 * Get primary key, Id
	 * 
	 * @return
	 */
	@JsonIgnore
	public Integer getId() {
		return id;
	}

	/**
	 * Set primary key, id
	 * 
	 * @param id
	 */
	public void setId(Integer id) {
		this.id = id;
	}

	/**
	 * Get Entity(Table) name
	 * 
	 * @return
	 */
	public String getEntityName() {
		return entityName;
	}

	/**
	 * Set Entity(Table) name
	 * 
	 * @param entityName
	 */
	public void setEntityName(String entityName) {
		this.entityName = entityName;
	}

	/**
	 * Get Entity(Table) location
	 * 
	 * @return
	 */
	public String getEntityLocation() {
		return entityLocation;
	}

	/**
	 * Set Entity(Table) location
	 * 
	 * @param entityLocation
	 */
	public void setEntityLocation(String entityLocation) {
		this.entityLocation = entityLocation;
	}

	/**
	 * Get Version for current object
	 * 
	 * @return
	 */
	public double getVersion() {
		return version;
	}

	/**
	 * Set Version for current object
	 * 
	 * @param version
	 */
	public void setVersion(double version) {
		this.version = version;
	}

	/**
	 * Get Description for current object
	 * 
	 * @return
	 */
	public String getDescription() {
		return description;
	}

	/**
	 * Set Description for for current object
	 * 
	 * @param description
	 */
	public void setDescription(String description) {
		this.description = description;
	}

	/**
	 * Get Attribute object foreign key
	 * 
	 * @return
	 */
	public Set<Attribute> getAttributes() {
		return attributes;
	}

	/**
	 * Set Attribute object foreign key
	 * 
	 * @param attributes
	 */
	public void setAttributes(Set<Attribute> attributes) {
		this.attributes = attributes;
	}

	@Override
	public String toString() {
		return "Entitee [id=" + id + ", entityName=" + entityName
				+ ", entityLocation=" + entityLocation + ", version=" + version
				+ ", description=" + description + ", attributes=" + attributes
				+ "]";
	}

}
