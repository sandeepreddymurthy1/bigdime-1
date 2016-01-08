/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.adaptor.metadata.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.springframework.stereotype.Component;

import io.bigdime.adaptor.metadata.ObjectEntityMapper;
import io.bigdime.adaptor.metadata.dto.AttributeDTO;
import io.bigdime.adaptor.metadata.dto.DataTypeDTO;
import io.bigdime.adaptor.metadata.dto.EntiteeDTO;
import io.bigdime.adaptor.metadata.dto.MetasegmentDTO;
import io.bigdime.adaptor.metadata.model.Attribute;
import io.bigdime.adaptor.metadata.model.DataType;
import io.bigdime.adaptor.metadata.model.Entitee;
import io.bigdime.adaptor.metadata.model.Metasegment;

@Component("MetadataObjectEntityMapper")
public class ObjectEntityMapperImpl implements ObjectEntityMapper {

	public Metasegment mapMetasegmentObject(MetasegmentDTO metasegmentDTO) {
		if (metasegmentDTO != null) {
			Metasegment metasegment = new Metasegment();
			metasegment.setId(metasegmentDTO.getId());
			metasegment.setAdaptorName(metasegmentDTO.getAdaptorName());
			metasegment.setSchemaType(metasegmentDTO.getSchemaType());
			metasegment.setDatabaseName(metasegmentDTO.getDatabaseName());
			metasegment.setDatabaseLocation(metasegmentDTO
					.getDatabaseLocation());
			metasegment.setDescription(metasegmentDTO.getDescription());
			metasegment.setIsDataSource(metasegmentDTO.getIsDataSource());
			metasegment.setCreatedAt(metasegmentDTO.getCreatedAt());
			metasegment.setCreatedBy(metasegmentDTO.getCreatedBy());
			metasegment.setUpdatedAt(metasegmentDTO.getUpdatedAt());
			metasegment.setUpdatedBy(metasegmentDTO.getUpdatedBy());
			if(metasegmentDTO.getEntitees()!=null)
			metasegment.setEntitees(mapEntiteeSetObject(metasegmentDTO
					.getEntitees()));
			return metasegment;
		}

		return null;

	}

	public MetasegmentDTO mapMetasegmentEntity(Metasegment metasegment) {
		if (metasegment != null) {
			MetasegmentDTO metasegmentDTO = new MetasegmentDTO();
			metasegmentDTO.setAdaptorName(metasegment.getAdaptorName());
			metasegmentDTO.setSchemaType(metasegment.getSchemaType());
			metasegmentDTO.setDatabaseLocation(metasegment
					.getDatabaseLocation());
			metasegmentDTO.setDatabaseName(metasegment.getDatabaseName());
			metasegmentDTO.setDescription(metasegment.getDescription());
			metasegmentDTO.setIsDataSource(metasegment.getIsDataSource());
			metasegmentDTO.setCreatedAt(metasegment.getCreatedAt());
			metasegmentDTO.setCreatedBy(metasegment.getCreatedBy());
			metasegmentDTO.setUpdatedAt(metasegment.getUpdatedAt());
			metasegmentDTO.setUpdatedBy(metasegment.getUpdatedBy());
			metasegmentDTO.setEntitees(mapEntiteeSetEntity(metasegment
					.getEntitees()));
			return metasegmentDTO;
		}
		return null;

	}

	public Set<Entitee> mapEntiteeSetObject(Set<EntiteeDTO> entiteeDTOSet) {
		if (entiteeDTOSet.size() > 0) {
			Set<Entitee> entiteeSet = new LinkedHashSet<Entitee>();
			for (EntiteeDTO entiteeDTO : entiteeDTOSet) {
				entiteeSet.add(mapEntiteeObject(entiteeDTO));
			}
			return Collections.unmodifiableSet(entiteeSet);
		}
		return null;

	}

	public Set<EntiteeDTO> mapEntiteeSetEntity(Set<Entitee> entiteeSet) {

		if (entiteeSet!=null && entiteeSet.size() > 0) {
			Set<EntiteeDTO> entiteeDTOSet = new LinkedHashSet<EntiteeDTO>();
			for (Entitee entity : entiteeSet) {
				entiteeDTOSet.add(mapEntiteeDTO(entity));

			}
			return entiteeDTOSet;
		}
		return null;
	}

	public EntiteeDTO mapEntiteeDTO(Entitee entity) {
		if (entity != null) {
			EntiteeDTO entiteeDTO = new EntiteeDTO();
			entiteeDTO.setEntityName(entity.getEntityName());
			entiteeDTO.setEntityLocation(entity.getEntityLocation());
			entiteeDTO.setVersion(entity.getVersion());
			entiteeDTO.setDescription(entity.getDescription());
			entiteeDTO.setAttributes(mapAttributeEntitySet(entity
					.getAttributes()));
			return entiteeDTO;
		}
		return null;
	}

	public Entitee mapEntiteeObject(EntiteeDTO entiteeDTO) {
		if (entiteeDTO != null) {
			Entitee entitee = new Entitee();
			entitee.setEntityName(entiteeDTO.getEntityName());
			entitee.setEntityLocation(entiteeDTO.getEntityLocation());
			entitee.setDescription(entiteeDTO.getDescription());
			entitee.setVersion(entiteeDTO.getVersion());
			entitee.setAttributes(mapAttributeObjectSet(entiteeDTO
					.getAttributes()));
			return entitee;
		}
		return null;
	}

	public Set<AttributeDTO> mapAttributeEntitySet(Set<Attribute> attributeSet) {
		if (attributeSet.size() > 0) {
			Set<AttributeDTO> attributeDTOSet = new LinkedHashSet<AttributeDTO>();
			for (Attribute attribute : attributeSet) {
				attributeDTOSet.add(mapAttributeEntity(attribute));
			}
			return attributeDTOSet;
		}

		return null;
	}

	public Set<Attribute> mapAttributeObjectSet(
			Set<AttributeDTO> attributeDTOSet) {
		if (attributeDTOSet.size() > 0) {
			Set<Attribute> attributeSet = new LinkedHashSet<Attribute>();
			for (AttributeDTO attributeDTO : attributeDTOSet) {
				attributeSet.add(mapAttributeObject(attributeDTO));
			}
			return Collections.unmodifiableSet(attributeSet);
		}
		return null;
	}

	public AttributeDTO mapAttributeEntity(Attribute attribute) {
		if (attribute != null) {
			AttributeDTO attributeDTO = new AttributeDTO();
			attributeDTO.setAttributeName(attribute.getAttributeName());
			attributeDTO.setAttributeType(attribute.getAttributeType());
			attributeDTO.setDefaultValue(attribute.getDefaultValue());
			attributeDTO.setFieldType(attribute.getFieldType());
			attributeDTO.setFractionalPart(attribute.getFractionalPart());
			attributeDTO.setIntPart(attribute.getIntPart());
			attributeDTO.setMappedAttributeName(attribute
					.getMappedAttributeName());
			attributeDTO.setNullable(attribute.getNullable());
			attributeDTO.setComment(attribute.getComment());
			attributeDTO
					.setDataType(mapDataTypeEntity(attribute.getDataType()));

			return attributeDTO;
		}

		return null;

	}

	public DataTypeDTO mapDataTypeEntity(DataType dataType) {

		if (dataType != null) {
			DataTypeDTO dataTypeDTO = new DataTypeDTO();
			dataTypeDTO.setDataType(dataType.getDataType());
			dataTypeDTO.setDescription(dataType.getDescription());
			dataTypeDTO.setDataTypeId(dataType.getDataTypeId());
			return dataTypeDTO;
		}

		return null;

	}

	public DataType mapDataTypeObject(DataTypeDTO dataTypeDTO) {
		if (dataTypeDTO != null) {
			DataType dataType = new DataType();
			dataType.setDataTypeId(dataTypeDTO.getDataTypeId());
			dataType.setDataType(dataTypeDTO.getDataType());
			dataType.setDescription(dataTypeDTO.getDescription());
			return dataType;
		}
		return null;
	}

	public Attribute mapAttributeObject(AttributeDTO attributeDTO) {
		if (attributeDTO != null) {

			Attribute attribute = new Attribute();
			attribute.setAttributeName(attributeDTO.getAttributeName());
			attribute.setAttributeType(attributeDTO.getAttributeType());
			attribute.setDefaultValue(attributeDTO.getDefaultValue());
			attribute.setFieldType(attributeDTO.getFieldType());
			attribute.setFractionalPart(attributeDTO.getFractionalPart());
			attribute.setIntPart(attributeDTO.getIntPart());
			attribute.setMappedAttributeName(attributeDTO
					.getMappedAttributeName());
			attribute.setNullable(attributeDTO.getNullable());
			attribute.setComment(attributeDTO.getComment());
			attribute
					.setDataType(mapDataTypeObject(attributeDTO.getDataType()));

			return attribute;

		}
		return null;
	}

	public List<Metasegment> mapMetasegmentListObject(
			List<MetasegmentDTO> allSegments) {
		// TODO Auto-generated method stub
		if (allSegments.size() > 0) {
			List<Metasegment> metasegmentList = new ArrayList<Metasegment>();
			for (MetasegmentDTO metasegmentDTO : allSegments) {
				metasegmentList.add(mapMetasegmentObject(metasegmentDTO));
			}
			return Collections.unmodifiableList(metasegmentList);
		}
		return null;
	}

}
