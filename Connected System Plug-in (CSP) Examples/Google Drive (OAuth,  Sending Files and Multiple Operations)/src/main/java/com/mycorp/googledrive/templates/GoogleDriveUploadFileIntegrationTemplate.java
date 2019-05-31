package com.mycorp.googledrive.templates;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;

import com.appian.connectedsystems.simplified.sdk.SimpleIntegrationTemplate;
import com.appian.connectedsystems.simplified.sdk.configuration.SimpleConfiguration;
import com.appian.connectedsystems.templateframework.sdk.ExecutionContext;
import com.appian.connectedsystems.templateframework.sdk.IntegrationResponse;
import com.appian.connectedsystems.templateframework.sdk.TemplateId;
import com.appian.connectedsystems.templateframework.sdk.configuration.Document;
import com.appian.connectedsystems.templateframework.sdk.configuration.FolderPropertyDescriptor;
import com.appian.connectedsystems.templateframework.sdk.configuration.PropertyPath;
import com.appian.connectedsystems.templateframework.sdk.diagnostics.IntegrationDesignerDiagnostic;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.common.base.Stopwatch;

@TemplateId(name = "GoogleDriveUploadFileIntegrationTemplate")
public class GoogleDriveUploadFileIntegrationTemplate extends SimpleIntegrationTemplate {

  public static final String FILE_ID_KEY = "fileId";
  public static final String FOLDER_ID_KEY = "folderID";

  @Override
  protected SimpleConfiguration getConfiguration(
      SimpleConfiguration integrationConfiguration,
      SimpleConfiguration connectedSystemConfiguration,
      PropertyPath updatedProperty,
      ExecutionContext executionContext) {
    return integrationConfiguration.setProperties(
        textProperty(FILE_ID_KEY)
            .label("File ID")
            .instructionText("This can be found in a shareable link for a file as part of the URL, e.g. " +
                "https://drive.google.com/open?id=<FileID>")
            .build(),
        FolderPropertyDescriptor.builder()
            .key(FOLDER_ID_KEY)
            .label("Save to Folder")
            .build()
    );
  }

  @Override
  protected IntegrationResponse execute(
      SimpleConfiguration integrationConfiguration,
      SimpleConfiguration connectedSystemConfiguration,
      ExecutionContext executionContext) {
    String fileId = integrationConfiguration.getValue(FILE_ID_KEY);
    Long folderId = integrationConfiguration.getValue(FOLDER_ID_KEY);

    IntegrationDesignerDiagnostic.IntegrationDesignerDiagnosticBuilder diagnosticBuilder = IntegrationDesignerDiagnostic
        .builder();
    Stopwatch stopwatch = Stopwatch.createStarted();

    //GoogleCredential is used to create a Google client with Drive. To send a file, you need to supply a File
    //and an InputStreamContent.
    GoogleCredential credential = new GoogleCredential().setAccessToken(
        executionContext.getAccessToken().get());
    Drive drive = new Drive.Builder(new NetHttpTransport(), JacksonFactory.getDefaultInstance(),
        credential).build();

    File fileMetadata;
    OutputStream outputStream = new ByteArrayOutputStream();
    try {
      fileMetadata = drive.files().get(fileId).execute();
      drive.files().get(fileId).executeMediaAndDownloadTo(outputStream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    long elapsed = stopwatch.stop().elapsed(TimeUnit.MILLISECONDS);
    Map<String,Object> requestDiagnostics = getRequestDiagnostics(connectedSystemConfiguration,
        integrationConfiguration);

    byte[] buffer = new byte[4096];
    InputStream inputStream = new ByteArrayInputStream(buffer);
    try {
      IOUtils.copyLarge(inputStream, outputStream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    String fileName = fileMetadata.getName();
    Document document = executionContext.getDocumentDownloadService()
        .downloadDocument(inputStream, folderId, fileName);
    Map<String,Object> result = new HashMap<>();
    result.put("File uploaded", document);

    Map<String,Object> responseDiagnostics = new HashMap<>();
    responseDiagnostics.put("File Name", fileMetadata.getName());
    responseDiagnostics.put("File Type", fileMetadata.getMimeType());
    responseDiagnostics.put("File Description", fileMetadata.getDescription());

    IntegrationDesignerDiagnostic diagnostic = diagnosticBuilder.addExecutionTimeDiagnostic(elapsed)
        .addRequestDiagnostic(requestDiagnostics)
        .addResponseDiagnostic(responseDiagnostics)
        .build();

    return IntegrationResponse.forSuccess(result).withDiagnostic(diagnostic).build();
  }

  private Map<String,Object> getRequestDiagnostics(
      SimpleConfiguration connectedSystemConfiguration,
      SimpleConfiguration integrationConfiguration) {
    Map<String,Object> requestDiagnostics = IntegrationExecutionUtils.getRequestDiagnostics(
        connectedSystemConfiguration);
    String fileId = integrationConfiguration.getValue(FILE_ID_KEY);
    requestDiagnostics.put("File ID", fileId);
    return requestDiagnostics;
  }

}
