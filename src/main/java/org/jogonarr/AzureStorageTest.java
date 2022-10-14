package org.jogonarr;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;

public class AzureStorageTest {
    private static final String SOURCE_DIRECTORY = "./data/source/";
    private static final String DOWNLOAD_DIRECTORY = "./data/downloads/";
    private static final String CONTAINER_NAME = "muguiwaras";

    private String _connectionString;
    private BlobContainerClient _containerClient;

    public AzureStorageTest() {
    }

    public void RunTest() {
        System.out.println("Starting Azure Test");

        GetConnectionString();
        CreateContainer();

        try {
            UploadFileToContainer("Monkey D Luffy.jpg");
            UploadFileToContainer("Roronoa Zoro.jpg");
            UploadFileToContainer("Go D Ussop.jpg");
            UploadFileToContainer("Vinsmoke Sanji.jpg");
            UploadFileToContainer("Nami.jpg");
            UploadFileToContainer("Tony Tony Chopper.jpg");
            UploadFileToContainer("Nico Robin.jpg");
            UploadFileToContainer("Franky.jpg");
            UploadFileToContainer("Brook.jpg");
            UploadFileToContainer("Jimbe.jpg");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        ListBlobsInContainer();
        DownloadBlobsFromContainer();

        try {
            DeleteContainer();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        System.out.println("Done!!!");
    }

    private void GetConnectionString() {
        _connectionString = System.getenv("AZURE_STORAGE_CONNECTION_STRING");
    }

    private void CreateContainer() {
        // Create a BlobServiceClient object which will be used to create a container client
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder().connectionString(_connectionString).buildClient();

        // Create the container and return a container client object
        _containerClient = blobServiceClient.createBlobContainerIfNotExists(CONTAINER_NAME);
    }

    private void UploadFileToContainer(String fileName) throws IOException {
        // Create a local file in the ./data/ directory for uploading and downloading

        // Get a reference to a blob
        BlobClient blobClient = _containerClient.getBlobClient(fileName);

        System.out.println("\nUploading to Blob storage as blob:\n\t" + blobClient.getBlobUrl());

        // Upload the blob
        blobClient.uploadFromFile(SOURCE_DIRECTORY + fileName, true);
    }

    private void ListBlobsInContainer() {
        // List the blob(s) in the container.
        for (BlobItem blobItem : _containerClient.listBlobs()) {
            System.out.println("\t" + blobItem.getName());
        }
    }

    private void DownloadBlobsFromContainer() {
        // List the blob(s) in the container.
        for (BlobItem blobItem : _containerClient.listBlobs()) {
            // Download the blob to a local file
            String localPath = DOWNLOAD_DIRECTORY;
            String blobName = blobItem.getName();
            String downloadFileName = blobName.replace(".jpg", "_DOWNLOAD.jpg");

            // Get a reference to a blob
            BlobClient blobClient = _containerClient.getBlobClient(blobName);

            System.out.println("\nDownloading blob to\n\t " + localPath + downloadFileName);
            blobClient.downloadToFile(localPath + downloadFileName);
        }
    }

    public void DeleteContainer() throws IOException {
        // Clean up
        /*System.out.println("\nPress the Enter key to begin clean up");
        System.console().readLine();*/

        System.out.println("Deleting blob container...");
        _containerClient.delete();

        System.out.println("Deleting the downloaded files...");
        FileUtils.cleanDirectory(new File(DOWNLOAD_DIRECTORY));
    }
}
