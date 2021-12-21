package org.sagebionetworks.bridge.sdk.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.Folder;
import org.sagebionetworks.repo.model.Project;
import org.sagebionetworks.repo.model.Team;
import org.sagebionetworks.repo.model.project.StsStorageLocationSetting;
import org.sagebionetworks.repo.model.table.TableEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.sagebionetworks.bridge.rest.api.ForAdminsApi;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.Exporter3Configuration;
import org.sagebionetworks.bridge.user.TestUserHelper;

public class Exporter3Test {
    private static final Logger LOG = LoggerFactory.getLogger(Exporter3Test.class);

    private static TestUserHelper.TestUser admin;
    private static SynapseClient synapseClient;

    @BeforeClass
    public static void beforeClass() throws Exception {
        synapseClient = Tests.getSynapseClient();
        admin = TestUserHelper.getSignedInAdmin();

        // Clean up stray Synapse resources before test.
        deleteEx3Resources();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        // Clean up Synapse resources.
        deleteEx3Resources();
    }

    private static void deleteEx3Resources() throws IOException {
        ForAdminsApi adminsApi = admin.getClient(ForAdminsApi.class);
        App app = adminsApi.getUsersApp().execute().body();
        Exporter3Configuration ex3Config = app.getExporter3Configuration();
        if (ex3Config == null) {
            // Exporter 3 is not configured on this app. We can skip this step.
            return;
        }

        // Delete the project. This automatically deletes the folder too.
        String projectId = ex3Config.getProjectId();
        if (projectId != null) {
            try {
                synapseClient.deleteEntityById(projectId, true);
            } catch (SynapseException ex) {
                LOG.error("Error deleting project " + projectId, ex);
            }
        }

        // Delete the data access team.
        Long dataAccessTeamId = ex3Config.getDataAccessTeamId();
        if (dataAccessTeamId != null) {
            try {
                synapseClient.deleteTeam(String.valueOf(dataAccessTeamId));
            } catch (SynapseException ex) {
                LOG.error("Error deleting team " + dataAccessTeamId, ex);
            }
        }

        // Storage locations are idempotent, so no need to delete that.

        // Reset the Exporter 3 Config.
        app.setExporter3Configuration(null);
        app.setExporter3Enabled(false);
        adminsApi.updateUsersApp(app).execute();
    }

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

        String participantVersionTableId = ex3Config.getParticipantVersionTableId();;
        TableEntity participantVersionTable = synapseClient.getEntity(participantVersionTableId, TableEntity.class);
        assertNotNull(participantVersionTable);
        assertEquals(projectId, participantVersionTable.getParentId());

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
