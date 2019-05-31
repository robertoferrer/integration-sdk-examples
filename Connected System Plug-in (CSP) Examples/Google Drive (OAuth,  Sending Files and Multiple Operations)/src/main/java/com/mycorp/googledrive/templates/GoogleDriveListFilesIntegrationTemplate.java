package com.mycorp.googledrive.templates;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.appian.connectedsystems.simplified.sdk.SimpleIntegrationTemplate;
import com.appian.connectedsystems.simplified.sdk.configuration.SimpleConfiguration;
import com.appian.connectedsystems.templateframework.sdk.ExecutionContext;
import com.appian.connectedsystems.templateframework.sdk.IntegrationResponse;
import com.appian.connectedsystems.templateframework.sdk.TemplateId;
import com.appian.connectedsystems.templateframework.sdk.configuration.PropertyPath;
import com.appian.connectedsystems.templateframework.sdk.diagnostics.IntegrationDesignerDiagnostic;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.common.base.Stopwatch;

@TemplateId(name = "GoogleDriveListFilesIntegrationTemplate")
public class GoogleDriveListFilesIntegrationTemplate extends SimpleIntegrationTemplate {
  private static final String FOLDER_ID_KEY = "folderId";

  @Override
  protected SimpleConfiguration getConfiguration(
      SimpleConfiguration integrationConfiguration,
      SimpleConfiguration connectedSystemConfiguration,
      PropertyPath updatedProperty,
      ExecutionContext executionContext) {
    return integrationConfiguration.setProperties(
        textProperty(FOLDER_ID_KEY).label("Folder Id")
            .instructionText("If left blank, it will list all existing files. Otherwise it will list the " +
                "children files in the folder")
            .build()
    );
  }

  @Override
  protected IntegrationResponse execute(
      SimpleConfiguration integrationConfiguration,
      SimpleConfiguration connectedSystemConfiguration,
      ExecutionContext executionContext) {
    String folderId = integrationConfiguration.getValue(FOLDER_ID_KEY);

    IntegrationDesignerDiagnostic.IntegrationDesignerDiagnosticBuilder diagnosticBuilder = IntegrationDesignerDiagnostic
        .builder();
    Stopwatch stopwatch = Stopwatch.createStarted();

    //GoogleCredential is used to create a Google client with Drive. To send a file, you need to supply a File
    //and an InputStreamContent.
    GoogleCredential credential = new GoogleCredential().setAccessToken(
        executionContext.getAccessToken().get());
    Drive drive = new Drive.Builder(new NetHttpTransport(), JacksonFactory.getDefaultInstance(),
        credential).build();

    ArrayList<File> queriedFiles = new ArrayList<File>();
    Drive.Files.List request;
    try {
      request = drive.files().list();
      if (folderId != null && !folderId.isEmpty()) {
        request.setQ("'" + folderId + "' in parents");
      }
      do {
        try {
          FileList files = request.execute();

          queriedFiles.addAll(files.getFiles());
          request.setPageToken(files.getNextPageToken());
        } catch (GoogleJsonResponseException e) {
          Map<String,Object> requestDiagnostics = getRequestDiagnostics(folderId,
              connectedSystemConfiguration, integrationConfiguration);
          return IntegrationExecutionUtils.handleException(e, diagnosticBuilder, requestDiagnostics,
              stopwatch);
        }
      } while (request.getPageToken() != null && request.getPageToken().length() > 0);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    HashMap<String,Object> resultMap = new HashMap<>();
    resultMap.put("files", queriedFiles);
    return IntegrationResponse.forSuccess(resultMap).withDiagnostic(diagnosticBuilder.build()).build();
  }

  private Map<String,Object> getRequestDiagnostics(
      String folderId,
      SimpleConfiguration connectedSystemConfiguration,
      SimpleConfiguration integrationConfiguration) {
    Map<String,Object> requestDiagnostics = IntegrationExecutionUtils.getRequestDiagnostics(
        connectedSystemConfiguration);
    requestDiagnostics.put("Folder ID", folderId);
    return requestDiagnostics;
  }
}
