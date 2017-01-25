package io.bigdime.handler.webservice;

import io.bigdime.core.InputDescriptor;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

/**
 * Encapsulates the fields(filePath, fileName) that are needed to fetch data from a webservice.
 * 
 * @author Sandeep Reddy,Murthy
 *
 */
public class WebserviceInputDescriptor implements InputDescriptor<String> {
	private String path;
	private String fileName;
	private String entityName;
//	private String applicationContext;
//	private Map<String,String> headers;
	
	public String[] parseSourceDescriptor(String descriptor) {
		if (descriptor.contains(":")) {
			String[] stringParts = descriptor.split(":");
			if (stringParts != null && stringParts.length == 2) {
				return stringParts;
			}
		}
		throw new IllegalArgumentException("descriptor must be in entityName:fileNamePrefix format");
	}
	@Override
	public String getNext(List<String> availableInputDescriptors,
			String lastInputDescriptor) {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public void parseDescriptor(String descriptor) {
		// TODO Auto-generated method stub
		
	}
}
