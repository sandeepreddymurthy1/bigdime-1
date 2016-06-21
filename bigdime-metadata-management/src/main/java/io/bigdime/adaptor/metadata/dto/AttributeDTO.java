/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.adaptor.metadata.dto;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.PostLoad;
import javax.persistence.Table;
import javax.persistence.Transient;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Class Attribute Attibute(Colum) Information associated to Entity. This plain
 * POJO Attribute can be used to instantiate Attribute Object in order to set
 * and get the values for the current instance.
 * 
 * <pre>
 *     Example: Attribtue attribute = new Attribute()
 * </pre>
 * 
 * @author Neeraj Jain, psabinikari
 * 
 * @version 1.0
 * 
 */
@Entity
@Table(name = "ATTRIBUTE")
public class AttributeDTO implements Cloneable{

	/**
	 * Attribute's primary key
	 */
	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	@Column(name = "ATTRIBUTE_ID")
	private Integer id;

	
	/**
	 * Attribute(column) name
	 */
	@Column(name = "ATTRIBUTE_NAME")
	private String attributeName;

	/**
	 * Attribute(column) type This does not persist into repository
	 */
	@Transient
	private String attributeType;

	@ManyToOne
	@JoinColumn(name = "DATA_TYPE_ID")
	private DataTypeDTO dataType;

	/**
	 * Integer part of data type size
	 */
	@Column(name = "INTPART")
	private String intPart;

	/**
	 * Fractional part of data type size
	 */
	@Column(name = "FRACTIONAL_PART")
	private String fractionalPart;

	/**
	 * Comment for current attribute object
	 */
	@Column(name = "COMMENT")
	private String comment;

	/**
	 * Provides whether current attribute is null or not
	 */
	@Column(name = "NULLABLE")
	private String nullable;

	/**
	 * Provides what type of attribute is current object
	 */
	@Column(name = "FIELD_TYPE")
	private String fieldType;

	@Column(name = "MAPPED_ATTRIBUTE_NAME")
	private String mappedAttributeName;

	@Column(name = "DEFAULT_VALUE")
	private String defaultValue;

	/**
	 * Class constructor. This is default no argument constructor.
	 * 
	 * <pre>
	 *    Example: Attribute attribute = new Attribute();
	 * </pre>
	 */
	public AttributeDTO() {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * Class constructor. This is arguments constructor. This is used to create
	 * Attribute object with corresponding values.
	 * 
	 * <pre>
	 *    Example: Attribute attribute = new Attribute(argument1 datatype,....,argumentn datatype);
	 * </pre>
	 * 
	 * 
	 * @param attributeName
	 * @param attributeType
	 * @param intPart
	 * @param fractionalPart
	 * @param comment
	 * @param nullable
	 * @param fieldType
	 * @param mappedAttributeName
	 * @param defaultValue
	 */
	public AttributeDTO(String attributeName, String attributeType,
			String intPart, String fractionalPart, String comment,
			String nullable, String fieldType, String mappedAttributeName,
			String defaultValue) {
		super();

		this.attributeName = attributeName;
		this.attributeType = attributeType;
		this.intPart = intPart;
		this.fractionalPart = fractionalPart;
		this.comment = comment;
		this.nullable = nullable;
		this.fieldType = fieldType;
		this.mappedAttributeName = mappedAttributeName;
		this.defaultValue = defaultValue;
	}

	/**
	 * Get attribute primary key id
	 * 
	 * @return Integer
	 */
	@JsonIgnore
	public Integer getId() {
		return id;
	}

	/**
	 * Set attribute primary key id
	 * 
	 * @param id
	 */
	public void setId(Integer id) {
		this.id = id;
	}

	/**
	 * Get attribute name
	 * 
	 * @return String
	 */
	public String getAttributeName() {
		return attributeName;
	}

	/**
	 * Set attribute name
	 * 
	 * @param attributeName
	 */
	public void setAttributeName(String attributeName) {
		this.attributeName = attributeName;

	}

	/**
	 * Get attribute type
	 * 
	 * @return String
	 */

	public String getAttributeType() {
		return attributeType;
	}

	/**
	 * Set attibute type
	 * 
	 * @param attributeType
	 */
	public void setAttributeType(String attributeType) {
		this.attributeType = attributeType;
	}

	/**
	 * Get Integer part of data type size.
	 * 
	 * @return String
	 */
	public String getIntPart() {
		return intPart;
	}

	/**
	 * Set integer part of data type size.
	 * 
	 * @param intPart
	 */
	public void setIntPart(String intPart) {
		this.intPart = intPart;
	}

	/**
	 * Get fractional part of data type size.
	 * 
	 * @return String
	 */
	public String getFractionalPart() {
		return fractionalPart;
	}

	/**
	 * Set fractional part of data type size.
	 * 
	 * @param fractionalPart
	 */
	public void setFractionalPart(String fractionalPart) {
		this.fractionalPart = fractionalPart;
	}

	public DataTypeDTO getDataType() {

		return dataType;
	}

	public void setDataType(DataTypeDTO dataType) {

		this.dataType = dataType;

	}

	/**
	 * Get comment available for current object
	 * 
	 * @return String
	 */
	public String getComment() {
		return comment;
	}

	/**
	 * Set comment for current object
	 * 
	 * @param comment
	 */
	public void setComment(String comment) {
		this.comment = comment;
	}

	/**
	 * Get nullable values 'YES or 'NO'. Yes represents current object entry can
	 * be null. No represents current object entry can not be null.
	 * 
	 * @return String
	 */
	public String getNullable() {
		return nullable;
	}

	/**
	 * Set nullable values 'YES or 'NO'. Yes represents current object entry can
	 * be null. No represents current object entry can not be null.
	 * 
	 * @param nullable
	 */
	public void setNullable(String nullable) {
		this.nullable = nullable;
	}

	/**
	 * Get field type associate to current object
	 * 
	 * @return String
	 */
	public String getFieldType() {
		return fieldType;
	}

	/**
	 * Set field type associated with current object
	 * 
	 * @param fieldType
	 */
	public void setFieldType(String fieldType) {
		this.fieldType = fieldType;
	}

	/**
	 * Get target mapped attribute name
	 * 
	 * @return
	 */

	public String getMappedAttributeName() {
		return mappedAttributeName;
	}

	/**
	 * Set target mapped attribute name
	 * 
	 * @param mappedAttributeName
	 */
	public void setMappedAttributeName(String mappedAttributeName) {
		this.mappedAttributeName = mappedAttributeName;
	}

	/**
	 * Get default value for target mapped attribute name
	 * 
	 * @return String
	 */
	public String getDefaultValue() {
		return defaultValue;
	}

	/**
	 * Set default value for target mapped attribute name
	 * 
	 * @param defaultValue
	 */
	public void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
	}

	/**
	 * This method sets the attributeType after loading the data from
	 * repository.
	 */
	@PostLoad
	public void onPostLoad() {
		if (this.dataType.getDataType() != null)
			this.attributeType = this.dataType.getDataType();
	}

	@Override
	public String toString() {
		return "Attribute [id=" + id + ", attributeName=" + attributeName
				+ ", attributeType=" + attributeType + ", dataType=" + dataType
				+ ", intPart=" + intPart + ", fractionalPart=" + fractionalPart
				+ ", comment=" + comment + ", nullable=" + nullable
				+ ", fieldType=" + fieldType + ", mappedAttributeName="
				+ mappedAttributeName + ", defaultValue=" + defaultValue + "]";
	}

	@Override
	public AttributeDTO clone() {
		AttributeDTO clone=null;
		try{ 
			clone = (AttributeDTO) super.clone(); 
		}catch(CloneNotSupportedException e){
			throw new RuntimeException(e); 
			} 
		return clone;
		}

}

