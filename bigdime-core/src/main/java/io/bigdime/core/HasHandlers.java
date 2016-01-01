/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core;

import java.util.LinkedHashSet;

public interface HasHandlers {
	LinkedHashSet<Handler> getHandlers();

	void setHandlers(LinkedHashSet<Handler> handlers);

}
