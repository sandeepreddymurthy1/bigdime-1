/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.adaptor.metadata;


import java.util.List;
import java.util.Set;


import io.bigdime.adaptor.metadata.dto.AttributeDTO;
import io.bigdime.adaptor.metadata.dto.DataTypeDTO;
import io.bigdime.adaptor.metadata.dto.EntiteeDTO;
import io.bigdime.adaptor.metadata.dto.MetasegmentDTO;
import io.bigdime.adaptor.metadata.model.Attribute;
import io.bigdime.adaptor.metadata.model.DataType;
import io.bigdime.adaptor.metadata.model.Entitee;
import io.bigdime.adaptor.metadata.model.Metasegment;

public interface ObjectEntityMapper {

	public Metasegment mapMetasegmentObject(MetasegmentDTO metasegmentDTO);

	public MetasegmentDTO mapMetasegmentEntity(Metasegment metasegment);

	public Set<Entitee> mapEntiteeSetObject(Set<EntiteeDTO> entiteeDTOSet);

	public Set<EntiteeDTO> mapEntiteeSetEntity(Set<Entitee> entiteeSet);

	public EntiteeDTO mapEntiteeDTO(Entitee entity);

	public Entitee mapEntiteeObject(EntiteeDTO entiteeDTO);

	public Set<AttributeDTO> mapAttributeEntitySet(Set<Attribute> attributeSet);

	public Set<Attribute> mapAttributeObjectSet(
			Set<AttributeDTO> attributeDTOSet);

	public AttributeDTO mapAttributeEntity(Attribute attribute);

	public DataTypeDTO mapDataTypeEntity(DataType dataType);

	public DataType mapDataTypeObject(DataTypeDTO dataTypeDTO);

	public Attribute mapAttributeObject(AttributeDTO attributeDTO);

	public List<Metasegment> mapMetasegmentListObject(
			List<MetasegmentDTO> allSegments);

}
