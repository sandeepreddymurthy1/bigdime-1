/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.line;

import static io.bigdime.core.commons.DataConstants.SCHEMA_FILE_NAME;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;

import io.bigdime.adaptor.metadata.MetadataAccessException;
import io.bigdime.adaptor.metadata.MetadataStore;
import io.bigdime.adaptor.metadata.model.Attribute;
import io.bigdime.adaptor.metadata.model.Entitee;
import io.bigdime.adaptor.metadata.model.Metasegment;
import io.bigdime.adaptor.metadata.utils.MetaDataJsonUtils;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.HandlerException;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.JsonHelper;
import io.bigdime.core.commons.PropertyHelper;
import io.bigdime.core.config.AdaptorConfig;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.handler.AbstractHandler;
import io.bigdime.core.handler.HandlerJournal;
import io.bigdime.core.handler.SimpleJournal;

/**
 * Reads a line and maps to a record according to targeted hive schema.
 * <p>
 * <ul>
 * <li>input can be col1,col2,col3,col4,col5
 * <li>output can be col1,col2,col3,col4,col5
 * 
 * 
 * <li>output may be col1,col2,col5,col4,col3
 * <li>if input changes to col1,col2,col8,col9,col5
 * <li>output should become col1,col2,col3,col4,col5,col8,col9
 * </ul>
 * 
 * @author Neeraj Jain
 *
 */
@Component
@Scope("prototype")
public class TextHiveMapperHandler extends AbstractHandler {
	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(TextHiveMapperHandler.class));

	private String handlerPhase = "";
	private String schemaFileName;
	@Autowired
	private MetadataStore metadataStore;
	@Autowired
	private MetaDataJsonUtils metaDataJsonUtils;

	private List<Map<String, Metasegment>> metasegments;

	// private Metasegment metaSegment;

	private String fieldDelimiter;

	@Autowired
	private JsonHelper jsonHelper;
	// Map<String, Metasegment> entityToMetaSegmentMap = new HashMap<>();

	@Override
	public void build() throws AdaptorConfigurationException {
		super.build();
		handlerPhase = "building TextHiveMapperHandler";
		logger.info(handlerPhase, "properties={}", getPropertyMap());
		schemaFileName = PropertyHelper.getStringProperty(getPropertyMap(), SCHEMA_FILE_NAME);
		fieldDelimiter = PropertyHelper.getStringProperty(getPropertyMap(), "field-delimiter");
		logger.info(handlerPhase, "output_channels={} schemaFileName={} fieldDelimiter={}", getOutputChannel(),
				schemaFileName, fieldDelimiter);

		/*
		 * read schema file and create map of entityName->schema During the
		 * process, use the schema for the entity for which the process method
		 * is being executed.
		 */
		try {
			synchronized (Metasegment.class) {
				metasegments = metaDataJsonUtils.readSchemaAndConvertToMetasegment(metadataStore,
						AdaptorConfig.getInstance().getName(), "HIVE", schemaFileName);
			}
		} catch (IOException | MetadataAccessException e) {
			throw new AdaptorConfigurationException(e);
		}
		logger.info(handlerPhase, "built TextHiveMapperHandler");
	}

	@Override
	public Status process() throws HandlerException {
		/*
		 * Read the ordered list of columns.
		 */
		handlerPhase = "processing TextHiveMapperHandler";
		logger.debug(handlerPhase, "processing TextHiveMapperHandler");

		if (getSimpleJournal().getEventList() != null && !getSimpleJournal().getEventList().isEmpty()) {
			// process for CALLBACK status.
			/*
			 * @formatter:off
			 * Get the list from journal
			 * remove one from the list
			 * submit to channel if needed
			 * set one in context
			 * if more available in journal list, return CALLBACK
			 * @formatter:on
			 */
			List<ActionEvent> actionEvents = getSimpleJournal().getEventList();
			logger.debug(handlerPhase, "_message=\"journal not empty\" list_size={}", actionEvents.size());
			return processIt(actionEvents);

		} else {
			// process for ready status.
			/*
			 * @formatter:off
			 * Get the list from context
			 * remove one from the list
			 * submit to channel if needed
			 * set one in context
			 * if more available in context list, return CALLBACK
			 * @formatter:on
			 */
			List<ActionEvent> actionEvents = getHandlerContext().getEventList();
			logger.debug(handlerPhase, "_message=\"journal empty, will process from context\" actionEvents={}",
					actionEvents);

			Preconditions.checkNotNull(actionEvents, "eventList in HandlerContext must be not null");
			Preconditions.checkArgument(!actionEvents.isEmpty(),
					"eventList in HandlerContext must contain at least one ActionEvent");
			return processIt(actionEvents);
		}
		// List<ActionEvent> actionEvents = getHandlerContext().getEventList();
		// Preconditions.checkNotNull(actionEvents, "eventList in HandlerContext
		// must be not null");
		// Preconditions.checkArgument(!actionEvents.isEmpty(),
		// "eventList in HandlerContext must contain at least one ActionEvent");
		// return processIt(actionEvents);
	}

	private Status processIt(final List<ActionEvent> actionEvents) throws HandlerException {

		Status statusToReturn = Status.READY;
		try {
			ActionEvent actionEvent = actionEvents.remove(0);

			// for (ActionEvent actionEvent : actionEvents) {
			logger.debug(handlerPhase, "event_headers={}", actionEvent.getHeaders());
			String entityName = actionEvent.getHeaders().get(ActionEventHeaderConstants.ENTITY_NAME);
			logger.debug(handlerPhase, "entity_name={}", entityName);
			// try {
			String stringBody = new String(actionEvent.getBody(), Charset.defaultCharset());
			logger.debug(handlerPhase, "stringBody={}", stringBody);
			byte[] newBody = parseBody(stringBody, entityName);
			actionEvent.setBody(newBody);
			logger.debug(handlerPhase, "checking channel submission, output_channel={}", getOutputChannel());
			if (getOutputChannel() != null) {
				logger.debug(handlerPhase, "submitting to channel");
				getOutputChannel().put(actionEvent);
			}

			getHandlerContext().createSingleItemEventList(actionEvent);
			if (!actionEvents.isEmpty()) {
				getSimpleJournal().setEventList(actionEvents);
				statusToReturn = Status.CALLBACK;
			} else {
				getSimpleJournal().setEventList(null);
				statusToReturn = Status.READY;
			}
		} catch (Exception ex) {
			throw new HandlerException(ex);
		}
		// }
		return statusToReturn;
	}

	private byte[] parseBody(String stringBody, String entityName) throws IOException {
		String[] fields = stringBody.split(fieldDelimiter);
		Metasegment schemaMetasegment = metasegments.get(0).get(entityName);
		logger.debug(handlerPhase, "db_segment={}", metasegments.get(1));
		Metasegment dbMetasegment = metasegments.get(1).get(entityName);
		Entitee dbEntity = dbMetasegment.getEntity(entityName);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();

		Set<Attribute> dbAttributes = dbEntity.getAttributes();
		Map<String, String> attributeNameValueMap = MetaDataJsonUtils.createAttributeNameValueMap(schemaMetasegment,
				entityName, fields);

		for (final Attribute attribute : dbAttributes) {
			// String value = getAttributeValue(attribute.getAttributeName(),
			// fields, schemaMetasegment, entityName);
			String value = attributeNameValueMap.get(attribute.getAttributeName());
			logger.debug(handlerPhase, "_message=\"parsing body\" attribute_name={} value={}",
					attribute.getAttributeName(), value);
			if (value == null) {
				logger.debug(handlerPhase, "no value found for attribute_name={}, will set it null",
						attribute.getAttributeName());
				value = "null";
			}
			baos.write(value.getBytes(Charset.defaultCharset()));
		}
		baos.write("\n".getBytes(Charset.defaultCharset()));
		byte[] bytes = baos.toByteArray();
		baos.close();
		return bytes;
	}

	/*
	 * For the given db attribute, find it's value from payload fields. To do
	 * this, iterate through the attributes from input schema and see for which
	 * index, the dbAttribute name matches with schemaAttribute name. Get the
	 * value from that index in fields array.
	 * 
	 */

	private HandlerJournal getSimpleJournal() throws HandlerException {
		return getNonNullJournal(SimpleJournal.class);
	}

}
