/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.jdbc;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.HandlerException;
import io.bigdime.core.commons.PropertyHelper;
import io.bigdime.core.handler.AbstractHandler;
@Component
@Scope("prototype")
/**
 * 
 * @author mnamburi
 *
 */
public class JDBCReaderHandler extends AbstractHandler{


	@Override
	public void build() throws AdaptorConfigurationException {

	}
	
	@Override
	public Status process() throws HandlerException {
		return null;
	}

}
