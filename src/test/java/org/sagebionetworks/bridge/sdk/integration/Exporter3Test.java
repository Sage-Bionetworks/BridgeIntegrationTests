package org.sagebionetworks.bridge.sdk.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.sagebionetworks.bridge.sdk.integration.Tests.PASSWORD;
import static org.sagebionetworks.bridge.sdk.integration.Tests.STUDY_ID_1;
import static org.sagebionetworks.bridge.util.IntegTestUtils.TEST_APP_ID;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
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

import org.sagebionetworks.bridge.rest.ApiClientProvider;
import org.sagebionetworks.bridge.rest.api.AuthenticationApi;
import org.sagebionetworks.bridge.rest.api.ConsentsApi;
import org.sagebionetworks.bridge.rest.api.ForAdminsApi;
import org.sagebionetworks.bridge.rest.api.ForConsentedUsersApi;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.api.InternalApi;
import org.sagebionetworks.bridge.rest.api.ParticipantsApi;
import org.sagebionetworks.bridge.rest.api.StudyParticipantsApi;
import org.sagebionetworks.bridge.rest.exceptions.ConsentRequiredException;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.ConsentSignature;
import org.sagebionetworks.bridge.rest.model.Exporter3Configuration;
import org.sagebionetworks.bridge.rest.model.ParticipantVersion;
import org.sagebionetworks.bridge.rest.model.Role;
import org.sagebionetworks.bridge.rest.model.SharingScope;
import org.sagebionetworks.bridge.rest.model.SharingScopeForm;
import org.sagebionetworks.bridge.rest.model.SignIn;
import org.sagebionetworks.bridge.rest.model.SignUp;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.rest.model.Withdrawal;
import org.sagebionetworks.bridge.user.TestUser;
import org.sagebionetworks.bridge.user.TestUserHelper;

public class Exporter3Test {
    private static final Logger LOG = LoggerFactory.getLogger(Exporter3Test.class);

    private static TestUser admin;
    private static ForAdminsApi adminsApi;
    private static DateTime oneHourAgo;
    private static SynapseClient synapseClient;
    private static ForWorkersApi workersApi;

    private String extId;
    private String userId;

    @BeforeClass
    public static void beforeClass() throws Exception {
        synapseClient = Tests.getSynapseClient();
        admin = TestUserHelper.getSignedInAdmin();
        adminsApi = admin.getClient(ForAdminsApi.class);
        oneHourAgo = DateTime.now().minusHours(1);
        workersApi = admin.getClient(ForWorkersApi.class);

        // Clean up stray Synapse resources before test.
        deleteEx3Resources();

        // Init Exporter 3.
        adminsApi.initExporter3().execute().body();
    }

    @Before
    public void before() {
        extId = Tests.randomIdentifier(Exporter3Test.class);
    }

    @After
    public void after() throws Exception {
        if (userId != null) {
            admin.getClient(InternalApi.class).deleteAllParticipantVersionsForUser(userId).execute();
            adminsApi.deleteUser(userId).execute();
        }
    }

    @AfterClass
    public static void afterClass() throws Exception {
        // Clean up Synapse resources.
        deleteEx3Resources();
    }

    private static void deleteEx3Resources() throws IOException {
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
    public void verifyInitExporter3() throws Exception {
        // Verify that app has also been updated.
        App updatedApp = adminsApi.getUsersApp().execute().body();
        assertTrue(updatedApp.isExporter3Enabled());

        // Verify Synapse.
        Exporter3Configuration ex3Config = updatedApp.getExporter3Configuration();
        long dataAccessTeamId = ex3Config.getDataAccessTeamId();
        Team dataAccessTeam = synapseClient.getTeam(String.valueOf(dataAccessTeamId));
        assertNotNull(dataAccessTeam);

        String projectId = ex3Config.getProjectId();
        Project project = synapseClient.getEntity(projectId, Project.class);
        assertNotNull(project);

        String participantVersionTableId = ex3Config.getParticipantVersionTableId();
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

    // PARTICIPANT VERSION TESTS
    // This is part of Exporter3Test because Participant Versions need Exporter 3.0 to be enabled. I figured it was
    // better for Exporter 3.0 to be enabled and torn down in just one place instead of setting it up and tearing it
    // down multiple times.

    @Test
    public void selfSignedUpUser() throws Exception {
        ApiClientProvider unauthenticatedProvider = Tests.getUnauthenticatedClientProvider(admin.getClientManager(), TEST_APP_ID);
        AuthenticationApi authApi = unauthenticatedProvider.getAuthenticationApi();

        // Create user via sign up. Use external ID so we can bypass email verification.
        SignUp signUp = new SignUp().appId(TEST_APP_ID).addDataGroupsItem("test_user")
                .putExternalIdsItem(STUDY_ID_1, extId).password(PASSWORD);
        authApi.signUp(signUp).execute();

        // Sign in. This throws because we're not consented yet.
        SignIn signIn = new SignIn().appId(TEST_APP_ID).externalId(extId).password(PASSWORD);
        try {
            authApi.signInV4(signIn).execute();
            fail("signIn should have thrown ConsentRequiredException");
        } catch (ConsentRequiredException ex) {
            userId = ex.getSession().getId();
        }
        assertNotNull(userId);

        // Because the user is not consented, there should be no participant versions.
        List<ParticipantVersion> participantVersionList = workersApi.getAllParticipantVersionsForUser(TEST_APP_ID,
                userId).execute().body().getItems();
        assertTrue(participantVersionList.isEmpty());

        // Consent w/ sponsors_and_partners.
        ApiClientProvider.AuthenticatedClientProvider authenticatedProvider = unauthenticatedProvider
                .getAuthenticatedClientProviderBuilder().withExternalId(extId).withPassword(PASSWORD).build();
        ConsentSignature signature = new ConsentSignature().name("Eggplant McTester")
                .birthdate(LocalDate.parse("1970-04-04")).scope(SharingScope.SPONSORS_AND_PARTNERS);
        authenticatedProvider.getClient(ConsentsApi.class).createConsentSignature(TEST_APP_ID, signature).execute();

        // There is now one participant version.
        participantVersionList = workersApi.getAllParticipantVersionsForUser(TEST_APP_ID,
                userId).execute().body().getItems();
        assertEquals(1, participantVersionList.size());
        ParticipantVersion participantVersion1 = participantVersionList.get(0);
        verifyCommonParams(participantVersion1);
        assertEquals(1, participantVersion1.getParticipantVersion().intValue());
        assertEquals(SharingScope.SPONSORS_AND_PARTNERS, participantVersion1.getSharingScope());
        assertNull(participantVersion1.getTimeZone());

        Map<String, String> studyMembershipMap = participantVersion1.getStudyMemberships();
        assertEquals(1, studyMembershipMap.size());
        assertEquals(extId, studyMembershipMap.get(STUDY_ID_1));

        // Update participant by updating time zone.
        ParticipantsApi participantsApi = admin.getClient(ParticipantsApi.class);
        StudyParticipant participant = participantsApi.getParticipantById(userId, false).execute().body();
        participant.setClientTimeZone("America/Los_Angeles");
        participantsApi.updateParticipant(userId, participant).execute();

        // There is now version 2.
        participantVersionList = workersApi.getAllParticipantVersionsForUser(TEST_APP_ID,
                userId).execute().body().getItems();
        assertEquals(2, participantVersionList.size());
        ParticipantVersion participantVersion2 = participantVersionList.get(1);
        verifyCommonParams(participantVersion2);
        assertEquals(2, participantVersion2.getParticipantVersion().intValue());
        assertEquals(participantVersion1.getCreatedOn().getMillis(), participantVersion2.getCreatedOn().getMillis());
        assertNotEquals(participantVersion1.getModifiedOn().getMillis(), participantVersion2.getModifiedOn()
                .getMillis());
        assertEquals(SharingScope.SPONSORS_AND_PARTNERS, participantVersion2.getSharingScope());
        assertEquals("America/Los_Angeles", participantVersion2.getTimeZone());
        assertEquals(studyMembershipMap, participantVersion2.getStudyMemberships());

        // Participant updates itself.
        TestUser user = TestUserHelper.getSignedInUser(signIn);
        ForConsentedUsersApi consentedUsersApi = user.getClient(ForConsentedUsersApi.class);
        participant = consentedUsersApi.getUsersParticipantRecord(false).execute().body();
        participant.setClientTimeZone("Asia/Tokyo");
        consentedUsersApi.updateUsersParticipantRecord(participant).execute();

        // There is now version 3.
        participantVersionList = workersApi.getAllParticipantVersionsForUser(TEST_APP_ID,
                userId).execute().body().getItems();
        assertEquals(3, participantVersionList.size());
        ParticipantVersion participantVersion3 = participantVersionList.get(2);
        verifyCommonParams(participantVersion3);
        assertEquals(3, participantVersion3.getParticipantVersion().intValue());
        assertEquals(participantVersion2.getCreatedOn().getMillis(), participantVersion3.getCreatedOn().getMillis());
        assertNotEquals(participantVersion2.getModifiedOn().getMillis(), participantVersion3.getModifiedOn()
                .getMillis());
        assertEquals(SharingScope.SPONSORS_AND_PARTNERS, participantVersion3.getSharingScope());
        assertEquals("Asia/Tokyo", participantVersion3.getTimeZone());
        assertEquals(studyMembershipMap, participantVersion3.getStudyMemberships());

        // Update first/last name.
        participant = consentedUsersApi.getUsersParticipantRecord(false).execute().body();
        participant.setFirstName("Eggplant");
        participant.setLastName("McTester");
        consentedUsersApi.updateUsersParticipantRecord(participant).execute();

        // This doesn't create a new version.
        participantVersionList = workersApi.getAllParticipantVersionsForUser(TEST_APP_ID,
                userId).execute().body().getItems();
        assertEquals(3, participantVersionList.size());

        // Participant updates sharing scope to all_qualified_researchers.
        consentedUsersApi.changeSharingScope(new SharingScopeForm().scope(SharingScope.ALL_QUALIFIED_RESEARCHERS))
                .execute();

        // There is now version 4.
        participantVersionList = workersApi.getAllParticipantVersionsForUser(TEST_APP_ID,
                userId).execute().body().getItems();
        assertEquals(4, participantVersionList.size());
        ParticipantVersion participantVersion4 = participantVersionList.get(3);
        verifyCommonParams(participantVersion4);
        assertEquals(4, participantVersion4.getParticipantVersion().intValue());
        assertEquals(participantVersion3.getCreatedOn().getMillis(), participantVersion4.getCreatedOn().getMillis());
        assertNotEquals(participantVersion3.getModifiedOn().getMillis(), participantVersion4.getModifiedOn()
                .getMillis());
        assertEquals(SharingScope.ALL_QUALIFIED_RESEARCHERS, participantVersion4.getSharingScope());
        assertEquals("Asia/Tokyo", participantVersion4.getTimeZone());
        assertEquals(studyMembershipMap, participantVersion4.getStudyMemberships());

        // Toggle to no_sharing and back.
        consentedUsersApi.changeSharingScope(new SharingScopeForm().scope(SharingScope.NO_SHARING))
                .execute();
        consentedUsersApi.changeSharingScope(new SharingScopeForm().scope(SharingScope.ALL_QUALIFIED_RESEARCHERS))
                .execute();

        // This doesn't create a new version.
        participantVersionList = workersApi.getAllParticipantVersionsForUser(TEST_APP_ID,
                userId).execute().body().getItems();
        assertEquals(4, participantVersionList.size());

        // Toggle to no_sharing and update the participant again.
        consentedUsersApi.changeSharingScope(new SharingScopeForm().scope(SharingScope.NO_SHARING))
                .execute();
        participant = consentedUsersApi.getUsersParticipantRecord(false).execute().body();
        participant.setClientTimeZone("America/New_York");
        consentedUsersApi.updateUsersParticipantRecord(participant).execute();

        // This doesn't create a new version... yet.
        participantVersionList = workersApi.getAllParticipantVersionsForUser(TEST_APP_ID,
                userId).execute().body().getItems();
        assertEquals(4, participantVersionList.size());

        // Toggle back to all_qualified_researchers.
        consentedUsersApi.changeSharingScope(new SharingScopeForm().scope(SharingScope.ALL_QUALIFIED_RESEARCHERS))
                .execute();

        // There is now version 5.
        participantVersionList = workersApi.getAllParticipantVersionsForUser(TEST_APP_ID,
                userId).execute().body().getItems();
        assertEquals(5, participantVersionList.size());
        ParticipantVersion participantVersion5 = participantVersionList.get(4);
        verifyCommonParams(participantVersion5);
        assertEquals(5, participantVersion5.getParticipantVersion().intValue());
        assertEquals(participantVersion4.getCreatedOn().getMillis(), participantVersion5.getCreatedOn().getMillis());
        assertNotEquals(participantVersion4.getModifiedOn().getMillis(), participantVersion5.getModifiedOn()
                .getMillis());
        assertEquals(SharingScope.ALL_QUALIFIED_RESEARCHERS, participantVersion5.getSharingScope());
        assertEquals("America/New_York", participantVersion5.getTimeZone());
        assertEquals(studyMembershipMap, participantVersion5.getStudyMemberships());

        // Withdraw consent.
        consentedUsersApi.withdrawFromApp(new Withdrawal().reason("Testing")).execute();

        // Worker updates the user again.
        participant = participantsApi.getParticipantById(userId, false).execute().body();
        participant.setClientTimeZone("Europe/London");
        participantsApi.updateParticipant(userId, participant).execute();

        // Participant is withdrawn. There is no new version.
        participantVersionList = workersApi.getAllParticipantVersionsForUser(TEST_APP_ID,
                userId).execute().body().getItems();
        assertEquals(5, participantVersionList.size());

        // Test get by userId and participant version.
        ParticipantVersion result = workersApi.getParticipantVersion(TEST_APP_ID, userId, 1).execute().body();
        assertEquals(participantVersion1, result);

        result = workersApi.getParticipantVersion(TEST_APP_ID, userId, 2).execute().body();
        assertEquals(participantVersion2, result);

        result = workersApi.getParticipantVersion(TEST_APP_ID, userId, 3).execute().body();
        assertEquals(participantVersion3, result);

        result = workersApi.getParticipantVersion(TEST_APP_ID, userId, 4).execute().body();
        assertEquals(participantVersion4, result);

        result = workersApi.getParticipantVersion(TEST_APP_ID, userId, 5).execute().body();
        assertEquals(participantVersion5, result);
    }

    @Test
    public void adminCreatedUser() throws Exception {
        // This is a simpler test than the previous test. Go ahead and create a user that's already consented. However,
        // createUser() API automatically initially sets user to no_sharing.
        TestUser user = TestUserHelper.createAndSignInUser(Exporter3Test.class, true);
        userId = user.getUserId();

        // Add test_user data group.
        ParticipantsApi participantsApi = admin.getClient(ParticipantsApi.class);
        StudyParticipant participant = participantsApi.getParticipantById(userId, false).execute().body();
        participant.addDataGroupsItem("test_user");
        participantsApi.updateParticipant(userId, participant).execute();

        // Because user has no sharing scope, there is no participant version.
        List<ParticipantVersion> participantVersionList = workersApi.getAllParticipantVersionsForUser(TEST_APP_ID,
                userId).execute().body().getItems();
        assertTrue(participantVersionList.isEmpty());

        // Set sharing scope to all_qualified_researchers.
        user.getClient(ForConsentedUsersApi.class).changeSharingScope(new SharingScopeForm()
                .scope(SharingScope.ALL_QUALIFIED_RESEARCHERS)).execute();

        // There is now one participant version.
        participantVersionList = workersApi.getAllParticipantVersionsForUser(TEST_APP_ID,
                userId).execute().body().getItems();
        assertEquals(1, participantVersionList.size());
        ParticipantVersion participantVersion1 = participantVersionList.get(0);
        verifyCommonParams(participantVersion1);
        assertEquals(1, participantVersion1.getParticipantVersion().intValue());
        assertEquals(SharingScope.ALL_QUALIFIED_RESEARCHERS, participantVersion1.getSharingScope());

        Map<String, String> studyMembershipMap = participantVersion1.getStudyMemberships();
        assertEquals(1, studyMembershipMap.size());
        assertEquals("<none>", studyMembershipMap.get(STUDY_ID_1));

        // Unenrolling the participant will also prevent the participant from creating participant versions. (And this
        // is different from withdrawing consent.)
        StudyParticipantsApi studyParticipantsApi = admin.getClient(StudyParticipantsApi.class);
        studyParticipantsApi.withdrawParticipant(STUDY_ID_1, userId, "Testing").execute();

        // Update the participant again.
        participant = participantsApi.getParticipantById(userId, false).execute().body();
        participant.setClientTimeZone("America/Los_Angeles");
        participantsApi.updateParticipant(userId, participant).execute();

        // No new version is created.
        participantVersionList = workersApi.getAllParticipantVersionsForUser(TEST_APP_ID,
                userId).execute().body().getItems();
        assertEquals(1, participantVersionList.size());
    }

    @Test
    public void accountWithRoleHasNoVersions() throws Exception {
        // Create a developer w/ consent. This should never happen in real life, but we'll test it in case it happens.
        TestUser developer = TestUserHelper.createAndSignInUser(Exporter3Test.class, true, Role.DEVELOPER);
        userId = developer.getUserId();

        // Add test_user data group.
        ParticipantsApi participantsApi = admin.getClient(ParticipantsApi.class);
        StudyParticipant participant = participantsApi.getParticipantById(userId, false).execute().body();
        participant.addDataGroupsItem("test_user");
        participantsApi.updateParticipant(userId, participant).execute();

        // Give it a sharing scope.
        developer.getClient(ForConsentedUsersApi.class).changeSharingScope(new SharingScopeForm()
                .scope(SharingScope.ALL_QUALIFIED_RESEARCHERS)).execute();

        // Accounts with roles never have versions.
        List<ParticipantVersion> participantVersionList = workersApi.getAllParticipantVersionsForUser(TEST_APP_ID,
                userId).execute().body().getItems();
        assertTrue(participantVersionList.isEmpty());
    }

    private void verifyCommonParams(ParticipantVersion participantVersion) {
        assertEquals(TEST_APP_ID, participantVersion.getAppId());
        // Health code exists (but we don't know what it is).
        assertNotNull(participantVersion.getHealthCode());

        // createdOn and modifiedOn exist and are recent.
        assertTrue(participantVersion.getCreatedOn().isAfter(oneHourAgo));
        assertTrue(participantVersion.getModifiedOn().isAfter(oneHourAgo));

        assertEquals(1, participantVersion.getDataGroups().size());
        assertEquals("test_user", participantVersion.getDataGroups().get(0));
    }
}
