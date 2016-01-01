/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.validation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Use this annotation with classes implementing {@link Validator} interface.
 * 
 * @author Neeraj Jain
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Inherited
public @interface Factory {
	Class<? extends Validator> type();

	String id();
}
