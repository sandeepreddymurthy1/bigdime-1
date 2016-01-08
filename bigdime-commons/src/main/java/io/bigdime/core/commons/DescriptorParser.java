package io.bigdime.core.commons;

import java.util.Map;

public interface DescriptorParser {

	public Map<Object, String> parseDescriptor(String inputKey, Object descriptor);
}
