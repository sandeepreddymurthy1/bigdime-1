/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.file;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;

import io.bigdime.alert.Logger.ALERT_CAUSE;
import io.bigdime.alert.Logger.ALERT_SEVERITY;
import io.bigdime.alert.Logger.ALERT_TYPE;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.HandlerException;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.PropertyHelper;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.handler.AbstractHandler;

/**
 * 
 * @author Neeraj Jain
 *
 */

@Component
@Scope("prototype")
public class FileArchiveHandler extends AbstractHandler {
	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(FileArchiveHandler.class));

	private String handlerPhase = "";
	private String archivePath = "";

	@Override
	public void build() throws AdaptorConfigurationException {
		super.build();
		handlerPhase = "building FileArchiveHandler";
		logger.info(handlerPhase, "properties={}", getPropertyMap());
		archivePath = PropertyHelper.getStringProperty(getPropertyMap(), FileArchiveHandlerConstants.ARCHIVE_PATH, "/");
	}

	@Override
	public Status process() throws HandlerException {
		handlerPhase = "processing FileArchiveHandler";
		logger.info(handlerPhase, "processing FileArchiveHandler");
		String sourceFile = "";
		try {
			List<ActionEvent> actionEvents = getHandlerContext().getEventList();
			Preconditions.checkNotNull(actionEvents, "eventList in HandlerContext can't be null");

			Preconditions.checkArgument(!actionEvents.isEmpty(), "eventList in HandlerContext can't be empty");
			ActionEvent actionEvent = actionEvents.get(0);

			String fileReadComplete = actionEvent.getHeaders().get(ActionEventHeaderConstants.READ_COMPLETE);
			if (fileReadComplete == null || !fileReadComplete.equalsIgnoreCase("true")) {
				logger.debug(handlerPhase, "FileArchiveHandler skipping archive operation");
				return Status.READY;
			}

			sourceFile = actionEvent.getHeaders().get(ActionEventHeaderConstants.SOURCE_FILE_PATH);
			// String sourceFileLocation =
			// actionEvent.getHeaders().get(ActionEventHeaderConstants.SOURCE_FILE_LOCATION);

			Path destPath = new File(archivePath + sourceFile).toPath().toAbsolutePath();
			if (!Files.exists(destPath)) {
				logger.debug(handlerPhase, "FileArchiveHandler creating directory \"{}\"", destPath);
				Path newDir = Files.createDirectories(destPath);
				logger.debug(handlerPhase, "FileArchiveHandler created directory \"{}\"", newDir);
			}
			// sourceFile =
			// actionEvent.getHeaders().get(ActionEventHeaderConstants.SOURCE_FILE_PATH);
			logger.debug("FileArchiveHandler archiving file", "sourceFile=\"{}\" archivePath=\"{}\"", sourceFile,
					archivePath);
			Path fromPath = new File(sourceFile).toPath().toAbsolutePath();
			// Path toPath = new File(archivePath + sourceFile).toPath();
			logger.debug("FileArchiveHandler archiving file", "sourceFile=\"{}\" archivePath=\"{}\"", fromPath,
					destPath);
			Path p = Files.move(fromPath, destPath, StandardCopyOption.REPLACE_EXISTING);
			logger.debug("FileArchiveHandler archived file", "sourceFile=\"{}\" archivePath=\"{}\" returnedPath=\"{}\"",
					sourceFile, archivePath + sourceFile, p.getFileName());
		} catch (IOException e) {
			logger.alert(ALERT_TYPE.FILE_ARCHIVE_FAILED, ALERT_CAUSE.APPLICATION_INTERNAL_ERROR, ALERT_SEVERITY.MAJOR,
					"\"file could not be archived\" sourceFile=\"{}\" archivePath=\"{}\"", sourceFile, archivePath, e);
		}
		return Status.READY;
	}
}
