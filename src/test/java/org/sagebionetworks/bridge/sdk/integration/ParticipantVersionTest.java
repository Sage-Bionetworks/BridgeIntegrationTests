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

import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

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
import org.sagebionetworks.bridge.rest.model.ConsentSignature;
import org.sagebionetworks.bridge.rest.model.ParticipantVersion;
import org.sagebionetworks.bridge.rest.model.Role;
import org.sagebionetworks.bridge.rest.model.SharingScope;
import org.sagebionetworks.bridge.rest.model.SharingScopeForm;
import org.sagebionetworks.bridge.rest.model.SignIn;
import org.sagebionetworks.bridge.rest.model.SignUp;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.rest.model.Withdrawal;
import org.sagebionetworks.bridge.user.TestUserHelper;

public class ParticipantVersionTest {
    private static TestUserHelper.TestUser admin;
    private static DateTime oneHourAgo;
    private static ForWorkersApi workersApi;

    private String extId;
    private String userId;

    @BeforeClass
    public static void beforeClass() {
        admin = TestUserHelper.getSignedInAdmin();
        oneHourAgo = DateTime.now().minusHours(1);
        workersApi = admin.getClient(ForWorkersApi.class);
    }

    @Before
    public void before() {
        extId = Tests.randomIdentifier(ParticipantVersionTest.class);
    }

    @After
    public void after() throws Exception {
        if (userId != null) {
            admin.getClient(InternalApi.class).deleteAllParticipantVersionsForUser(userId).execute();
            admin.getClient(ForAdminsApi.class).deleteUser(userId).execute();
        }
    }

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
        TestUserHelper.TestUser user = TestUserHelper.getSignedInUser(signIn);
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

        // getParticipantVersionByHealthcode is a helpful alias.
        String healthCode = participantVersion5.getHealthCode();
        result = workersApi.getParticipantVersionForHealthCode(TEST_APP_ID, healthCode, 5).execute().body();
        assertEquals(participantVersion5, result);
    }

    @Test
    public void adminCreatedUser() throws Exception {
        // This is a simpler test than the previous test. Go ahead and create a user that's already consented. However,
        // createUser() API automatically initially sets user to no_sharing.
        TestUserHelper.TestUser user = TestUserHelper.createAndSignInUser(ParticipantVersionTest.class,
                true);
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
        TestUserHelper.TestUser developer = TestUserHelper.createAndSignInUser(ParticipantVersionTest.class,
                true, Role.DEVELOPER);
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
