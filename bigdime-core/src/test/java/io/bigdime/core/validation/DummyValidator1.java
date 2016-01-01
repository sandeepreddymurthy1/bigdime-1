/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.validation;

import org.apache.commons.lang.NotImplementedException;
import org.springframework.stereotype.Component;

import io.bigdime.core.ActionEvent;

@Factory(id = "_dummy1", type = DummyValidator1.class)

@Component
public class DummyValidator1 implements Validator {

	private String name;

	@Override
	public ValidationResponse validate(ActionEvent actionEvent) throws DataValidationException {

		throw new NotImplementedException();
	}

	@Override
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

}
