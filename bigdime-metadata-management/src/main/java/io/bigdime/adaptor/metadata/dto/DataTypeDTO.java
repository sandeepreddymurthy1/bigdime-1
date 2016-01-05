/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.adaptor.metadata.dto;


import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * 
 * Class DataType information is associated to Attribute. This object acts as a
 * master information for all the data types available in big-dime. Column data
 * type refers to this object id. These data types can be pre-populated in the
 * table or can be dynamically populated during data process. This DataType can
 * be used to instantiate DataType Object.
 * 
 * <pre>
 *     Example: DataType dataType = new DataType();
 * </pre>
 * 
 * @author Neeraj Jain, psabinikari
 */
@Entity
@Table(name = "DATA_TYPE")
public class DataTypeDTO {
	/**
	 * DataType's primary key
	 */

	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	@Column(name = "DATA_TYPE_ID")
	private int dataTypeId;

	/**
	 * Defined the type of data
	 */
	@Column(name = "DATA_TYPE")
	private String dataType;

	/**
	 * Associated description for data type.
	 */
	@Column(name = "DESCRIPTION")
	private String description;

	/**
	 * Class constructor. This is arguments constructor. This is used to create
	 * DataType object with corresponding values.
	 * 
	 * <pre>
	 *    Example: DataType dataType = new DataType(argument1 datatype,....,argumentn datatype);
	 * </pre>
	 * 
	 * @param dataType
	 * @param description
	 */

	public DataTypeDTO(String dataType, String description) {
		super();
		this.dataType = dataType;
		this.description = description;
	}

	public DataTypeDTO() {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * Get data type primary key id
	 * 
	 * @return int
	 */
	public int getDataTypeId() {
		return dataTypeId;
	}

	/**
	 * Set data type primary key id
	 * 
	 * @param dataTypeId
	 */
	public void setDataTypeId(int dataTypeId) {
		this.dataTypeId = dataTypeId;
	}

	/**
	 * Get data type
	 * 
	 * @return
	 */
	public String getDataType() {
		return dataType;
	}

	/**
	 * set data type
	 * 
	 * @param dataType
	 */
	public void setDataType(String dataType) {
		// (new Attribute()).setAttributeType(dataType);
		this.dataType = dataType;
	}

	/**
	 * Get description
	 * 
	 * @return
	 */
	public String getDescription() {
		return description;
	}

	/**
	 * set Description
	 * 
	 * @param description
	 */
	public void setDescription(String description) {
		this.description = description;
	}



}

