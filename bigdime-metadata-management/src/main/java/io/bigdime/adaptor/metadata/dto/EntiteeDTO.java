/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.adaptor.metadata.dto;


import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
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
import javax.persistence.OrderBy;
import javax.persistence.Table;

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
@Entity
@Table(name = "ENTITY")
public class EntiteeDTO implements Cloneable{

	private static final int PRECISIONNUMBER = 5;
	private static final int SCALENUMBER = 1;

	/**
	 * Entity primary id
	 */
	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	@Column(name = "ENTITY_ID")
	private Integer id;

	/**
	 * Table Name
	 */
	@Column(name = "ENTITY_NAME")
	private String entityName;

	/**
	 * Table Location
	 */
	@Column(name = "ENTITY_LOCATION")
	private String entityLocation;

	/**
	 * Entity object version
	 */
	@Column(name = "VERSION", precision = PRECISIONNUMBER, scale = SCALENUMBER)
	private double version;

	/**
	 * Description this entity object
	 */
	@Column(name = "DESCRIPTION")
	private String description;

	/**
	 * Attribute relation ship
	 */
	@OneToMany(cascade = CascadeType.ALL, fetch = FetchType.EAGER)
	@JoinColumn(name = "ENTITY_ID")
	@OrderBy("id")
	private Set<AttributeDTO> attributes = new LinkedHashSet<AttributeDTO>();

	/**
	 * Class constructor. This is default no arguments constructor
	 * 
	 * <pre>
	 *    Example: Entitee entitee = new Entitee();
	 * </pre>
	 */
	public EntiteeDTO() {
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
	public EntiteeDTO(String entityName, String entityLocation, double version,
			String description, Set<AttributeDTO> attributes) {
		super();
		this.entityName = entityName;
		this.entityLocation = entityLocation;
		this.version = version;
		this.description = description;
	    Iterator<AttributeDTO> iterator =attributes.iterator();
	    while(iterator.hasNext()){
	    	this.attributes.add(iterator.next().clone());
	    }

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
	public Set<AttributeDTO> getAttributes() {
		Set<AttributeDTO> attributeSet = new LinkedHashSet<AttributeDTO>();
		Iterator<AttributeDTO> iterator =this.attributes.iterator();
	    while(iterator.hasNext()){
	    	attributeSet.add(iterator.next().clone());
	    	}
		return attributeSet;
	}

	/**
	 * Set Attribute object foreign key
	 * 
	 * @param attributes
	 */
	public void setAttributes(Set<AttributeDTO> attributes) {
		 Iterator<AttributeDTO> iterator =attributes.iterator();
		    while(iterator.hasNext()){
		    	this.attributes.add(iterator.next().clone());
		    	}
	}

	@Override
	public String toString() {
		return "Entitee [id=" + id + ", entityName=" + entityName
				+ ", entityLocation=" + entityLocation + ", version=" + version
				+ ", description=" + description + ", attributes=" + attributes
				+ "]";
	}
	
	@Override
	public EntiteeDTO clone() {
		EntiteeDTO clone=null;
		try{ 
			clone = (EntiteeDTO) super.clone(); 
		}catch(CloneNotSupportedException e){
			throw new RuntimeException(e); 
			} 
		return clone;
		}

}

