package org.sagebionetworks.bridge.sdk.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.sagebionetworks.client.SynapseAdminClientImpl;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.repo.model.Folder;
import org.sagebionetworks.repo.model.Project;
import org.sagebionetworks.repo.model.Team;
import org.sagebionetworks.repo.model.project.StsStorageLocationSetting;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.config.PropertiesConfig;
import org.sagebionetworks.bridge.rest.api.ForAdminsApi;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.Exporter3Configuration;
import org.sagebionetworks.bridge.rest.model.Role;
import org.sagebionetworks.bridge.user.TestUserHelper;

public class Exporter3Test {
    private static final String USER_NAME = "synapse.user";
    private static final String SYNAPSE_API_KEY_NAME = "synapse.api.key";

    private static final String CONFIG_FILE = "bridge-sdk-test.properties";
    private static final String DEFAULT_CONFIG_FILE = CONFIG_FILE;
    private static final String USER_CONFIG_FILE = System.getProperty("user.home") + "/" + CONFIG_FILE;

    private static TestUserHelper.TestUser admin;
    private static SynapseClient synapseClient;

    @BeforeClass
    public static void beforeClass() throws Exception {
        // Set up SynapseClient.
        Config config;
        Path localConfigPath = Paths.get(USER_CONFIG_FILE);
        if (Files.exists(localConfigPath)) {
            config = new PropertiesConfig(DEFAULT_CONFIG_FILE, localConfigPath);
        } else {
            config = new PropertiesConfig(DEFAULT_CONFIG_FILE);
        }

        String synapseUserName = config.get(USER_NAME);
        String synapseApiKey = config.get(SYNAPSE_API_KEY_NAME);

        synapseClient = new SynapseAdminClientImpl();
        synapseClient.setUsername(synapseUserName);
        synapseClient.setApiKey(synapseApiKey);

        // Create admin account.
        admin = TestUserHelper.createAndSignInUser(Exporter3Test.class, false, Role.ADMIN);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        if (admin != null) {
            admin.signOutAndDeleteUser();
        }
    }

    @Ignore
    @Test
    public void test() throws Exception {
        ForAdminsApi adminsApi = admin.getClient(ForAdminsApi.class);

        // Reset the Exporter 3 Config.
        App app = adminsApi.getUsersApp().execute().body();
        app.setExporter3Configuration(null);
        app.setExporter3Enabled(false);
        adminsApi.updateUsersApp(app).execute();

        // Init Exporter 3.
        Exporter3Configuration ex3Config = adminsApi.initExporter3().execute().body();

        // Verify that app has also been updated.
        App updatedApp = adminsApi.getUsersApp().execute().body();
        assertEquals(ex3Config, updatedApp.getExporter3Configuration());
        assertTrue(updatedApp.isExporter3Enabled());

        // Verify Synapse.
        long dataAccessTeamId = ex3Config.getDataAccessTeamId();
        Team dataAccessTeam = synapseClient.getTeam(String.valueOf(dataAccessTeamId));
        assertNotNull(dataAccessTeam);

        String projectId = ex3Config.getProjectId();
        Project project = synapseClient.getEntity(projectId, Project.class);
        assertNotNull(project);

        String rawFolderId = ex3Config.getRawDataFolderId();
        Folder rawFolder = synapseClient.getEntity(rawFolderId, Folder.class);
        assertNotNull(rawFolder);
        assertEquals(projectId, rawFolder.getParentId());

        long storageLocationId = ex3Config.getStorageLocationId();
        StsStorageLocationSetting storageLocation = synapseClient.getMyStorageLocationSetting(storageLocationId);
        assertNotNull(storageLocation);
        assertTrue(storageLocation.getStsEnabled());
    }
}
