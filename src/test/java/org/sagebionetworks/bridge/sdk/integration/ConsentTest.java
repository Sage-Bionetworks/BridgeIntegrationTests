package org.sagebionetworks.bridge.sdk.integration;

import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.sagebionetworks.bridge.rest.model.ActivityEventUpdateType.IMMUTABLE;
import static org.sagebionetworks.bridge.rest.model.PerformanceOrder.SEQUENTIAL;
import static org.sagebionetworks.bridge.rest.model.Role.DEVELOPER;
import static org.sagebionetworks.bridge.rest.model.Role.RESEARCHER;
import static org.sagebionetworks.bridge.rest.model.SharingScope.ALL_QUALIFIED_RESEARCHERS;
import static org.sagebionetworks.bridge.rest.model.SharingScope.NO_SHARING;
import static org.sagebionetworks.bridge.rest.model.SharingScope.SPONSORS_AND_PARTNERS;
import static org.sagebionetworks.bridge.rest.model.SmsType.TRANSACTIONAL;
import static org.sagebionetworks.bridge.sdk.integration.InitListener.FAKE_ENROLLMENT;
import static org.sagebionetworks.bridge.sdk.integration.Tests.PASSWORD;
import static org.sagebionetworks.bridge.sdk.integration.Tests.STUDY_ID_1;
import static org.sagebionetworks.bridge.sdk.integration.Tests.STUDY_ID_2;
import static org.sagebionetworks.bridge.sdk.integration.Tests.randomIdentifier;
import static org.sagebionetworks.bridge.util.IntegTestUtils.PHONE;
import static org.sagebionetworks.bridge.util.IntegTestUtils.TEST_APP_ID;

import org.joda.time.DateTime;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.joda.time.LocalDate;
import org.joda.time.format.ISODateTimeFormat;
import retrofit2.Response;

import org.sagebionetworks.bridge.rest.ApiClientProvider;
import org.sagebionetworks.bridge.rest.RestUtils;
import org.sagebionetworks.bridge.rest.api.AppsApi;
import org.sagebionetworks.bridge.rest.api.AssessmentsApi;
import org.sagebionetworks.bridge.rest.api.AuthenticationApi;
import org.sagebionetworks.bridge.rest.api.ConsentsApi;
import org.sagebionetworks.bridge.rest.api.ForAdminsApi;
import org.sagebionetworks.bridge.rest.api.ForConsentedUsersApi;
import org.sagebionetworks.bridge.rest.api.ForDevelopersApi;
import org.sagebionetworks.bridge.rest.api.ForResearchersApi;
import org.sagebionetworks.bridge.rest.api.InternalApi;
import org.sagebionetworks.bridge.rest.api.ParticipantsApi;
import org.sagebionetworks.bridge.rest.api.SchedulesV2Api;
import org.sagebionetworks.bridge.rest.api.StudyParticipantsApi;
import org.sagebionetworks.bridge.rest.api.SubpopulationsApi;
import org.sagebionetworks.bridge.rest.exceptions.ConsentRequiredException;
import org.sagebionetworks.bridge.rest.exceptions.EntityAlreadyExistsException;
import org.sagebionetworks.bridge.rest.exceptions.EntityNotFoundException;
import org.sagebionetworks.bridge.rest.exceptions.InvalidEntityException;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.Assessment;
import org.sagebionetworks.bridge.rest.model.AssessmentReference2;
import org.sagebionetworks.bridge.rest.model.ConsentSignature;
import org.sagebionetworks.bridge.rest.model.ConsentStatus;
import org.sagebionetworks.bridge.rest.model.Enrollment;
import org.sagebionetworks.bridge.rest.model.EnrollmentInfo;
import org.sagebionetworks.bridge.rest.model.GuidVersionHolder;
import org.sagebionetworks.bridge.rest.model.HealthDataRecord;
import org.sagebionetworks.bridge.rest.model.Message;
import org.sagebionetworks.bridge.rest.model.Schedule2;
import org.sagebionetworks.bridge.rest.model.Session;
import org.sagebionetworks.bridge.rest.model.SignIn;
import org.sagebionetworks.bridge.rest.model.SignUp;
import org.sagebionetworks.bridge.rest.model.SmsMessage;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.StudyActivityEvent;
import org.sagebionetworks.bridge.rest.model.StudyActivityEventList;
import org.sagebionetworks.bridge.rest.model.StudyBurst;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.rest.model.Subpopulation;
import org.sagebionetworks.bridge.rest.model.TimeWindow;
import org.sagebionetworks.bridge.rest.model.UserConsentHistory;
import org.sagebionetworks.bridge.rest.model.UserSessionInfo;
import org.sagebionetworks.bridge.rest.model.Withdrawal;
import org.sagebionetworks.bridge.user.TestUser;
import org.sagebionetworks.bridge.user.TestUserHelper;
import org.sagebionetworks.bridge.util.IntegTestUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

@Category(IntegrationSmokeTest.class)
@SuppressWarnings({ "ConstantConditions", "unchecked" })
public class ConsentTest {
    private static final Withdrawal WITHDRAWAL = new Withdrawal().reason("Reasons");
    private static final String FAKE_IMAGE_DATA = "VGVzdCBzdHJpbmc=";

    private static TestUser adminUser;
    private static TestUser phoneOnlyTestUser;
    private static TestUser researchUser;
    
    private TestUser user;
    private Schedule2 schedule;
    private Assessment assessmentA;
    private String externalId;

    @BeforeClass
    public static void before() throws Exception {
        // Get admin API.
        adminUser = TestUserHelper.getSignedInAdmin();

        // Make researcher.
        researchUser = TestUserHelper.createAndSignInUser(ConsentTest.class, true, RESEARCHER);

        // Make phone user.
        IntegTestUtils.deletePhoneUser();
        SignUp phoneOnlyUser = new SignUp().appId(TEST_APP_ID).consent(true).phone(PHONE);
        phoneOnlyTestUser = new TestUserHelper.Builder(ConsentTest.class).withConsentUser(true)
                .withSignUp(phoneOnlyUser).createAndSignInUser();

        // Verify necessary flags (health code export) are enabled
        ForAdminsApi adminApi = adminUser.getClient(ForAdminsApi.class);
        App app = adminApi.getUsersApp().execute().body();
        app.setHealthCodeExportEnabled(true);
        adminApi.updateUsersApp(app).execute();
    }

    @AfterClass
    public static void deleteResearcher() throws Exception {
        if (researchUser != null) {
            researchUser.signOutAndDeleteUser();
        }
    }

    @AfterClass
    public static void deletePhoneUser() throws Exception {
        if (phoneOnlyTestUser != null) {
            phoneOnlyTestUser.signOutAndDeleteUser();
        }
    }
    
    @After
    public void deleteUser() throws Exception {
        TestUser admin = TestUserHelper.getSignedInAdmin();
        if (user != null) {
            user.signOutAndDeleteUser();
        }
        if (externalId != null) {
            StudyParticipantsApi participantApi = admin.getClient(StudyParticipantsApi.class);
            StudyParticipant participant = participantApi.getStudyParticipantById(
                    STUDY_ID_1, "externalid:"+externalId, false).execute().body();
            participantApi.deleteStudyParticipant(STUDY_ID_1, participant.getId()).execute();
        }
        if (schedule != null) {
            admin.getClient(ForAdminsApi.class).deleteSchedule(schedule.getGuid()).execute();
        }
        if (assessmentA != null) {
            admin.getClient(ForAdminsApi.class).deleteAssessment(assessmentA.getGuid(), true).execute();   
        }
    }

    @Test
    public void canToggleDataSharing() throws Exception {
        TestUser testUser = TestUserHelper.createAndSignInUser(ConsentTest.class, true);
        ForConsentedUsersApi userApi = testUser.getClient(ForConsentedUsersApi.class);
        try {
            // starts out with no sharing
            UserSessionInfo session = testUser.getSession();
            assertEquals(NO_SHARING, session.getSharingScope());

            // Change, verify in-memory session changed, verify after signing in again that server state has changed
            StudyParticipant participant = new StudyParticipant();

            participant.sharingScope(SPONSORS_AND_PARTNERS);
            userApi.updateUsersParticipantRecord(participant).execute();
            
            participant = userApi.getUsersParticipantRecord(false).execute().body();
            assertEquals(SPONSORS_AND_PARTNERS, participant.getSharingScope());

            // Do the same thing in reverse, setting to no sharing
            participant = new StudyParticipant();
            participant.sharingScope(NO_SHARING);

            userApi.updateUsersParticipantRecord(participant).execute();

            participant = userApi.getUsersParticipantRecord(true).execute().body();
            assertEquals(NO_SHARING, participant.getSharingScope());
            
            Map<String,List<UserConsentHistory>> map = participant.getConsentHistories();
            UserConsentHistory history = map.get(TEST_APP_ID).get(0);
            
            assertEquals(TEST_APP_ID, history.getSubpopulationGuid());
            assertNotNull(history.getConsentCreatedOn());
            assertNotNull(history.getName());
            assertNotNull(history.getBirthdate());
            assertTrue(history.getSignedOn().isAfter(DateTime.now().minusHours(1)));
            assertTrue(history.isHasSignedActiveConsent());
            
            AuthenticationApi authApi = testUser.getClient(AuthenticationApi.class);
            authApi.signOut().execute();
        } finally {
            testUser.signOutAndDeleteUser();
        }
    }

    // BRIDGE-1594
    @Test
    public void giveConsentAndWithdrawTwice() throws Exception {
        TestUser developer = TestUserHelper.createAndSignInUser(ConsentTest.class, true, DEVELOPER);
        TestUser user = TestUserHelper.createAndSignInUser(ConsentTest.class, false);
        SubpopulationsApi subpopsApi = developer.getClientManager().getClient(SubpopulationsApi.class);
        GuidVersionHolder keys = null;
        try {

            Subpopulation subpop = new Subpopulation();
            subpop.setName("Optional additional consent");
            subpop.setRequired(false);
            subpop.addStudyIdsAssignedOnConsentItem(STUDY_ID_2);
            keys = subpopsApi.createSubpopulation(subpop).execute().body();

            ConsentSignature signature = new ConsentSignature();
            signature.setName("Test User");
            signature.setScope(NO_SHARING);
            signature.setBirthdate(LocalDate.parse("1970-04-04"));

            Withdrawal withdrawal = new Withdrawal();
            withdrawal.setReason("A reason.");

            // Now, this user will consent to both consents, then withdraw from the required consent,
            // then withdraw from the optional consent, and this should work where it didn't before.
            ForConsentedUsersApi usersApi = user.getClientManager().getClient(ForConsentedUsersApi.class);

            usersApi.createConsentSignature(user.getAppId(), signature).execute();
            usersApi.createConsentSignature(keys.getGuid(), signature).execute();
            // Withdrawing the optional consent first, you should then be able to get the second consent
            usersApi.withdrawConsentFromSubpopulation(keys.getGuid(), withdrawal).execute();

            user.signOut();
            user.signInAgain();

            usersApi.withdrawConsentFromSubpopulation(user.getAppId(), withdrawal).execute();

            user.signOut();
            try {
                user.signInAgain();
                fail("Should have thrown an exception.");
            } catch (ConsentRequiredException e) {
                UserSessionInfo session = e.getSession();
                for (ConsentStatus status : session.getConsentStatuses().values()) {
                    assertFalse(status.isConsented());
                }
                assertFalse(RestUtils.isUserConsented(session));
            }
        } finally {
            adminUser.getClient(SubpopulationsApi.class).deleteSubpopulation(keys.getGuid(), true).execute();
            user.signOutAndDeleteUser();
            developer.signOutAndDeleteUser();
        }
    }

    @Test
    public void giveAndGetConsent() throws Exception {
        giveAndGetConsentHelper("Eggplant McTester", new LocalDate(1970, 1, 1), null, null);
    }

    @Test
    public void giveAndGetConsentWithSignatureImage() throws Exception {
        giveAndGetConsentHelper("Eggplant McTester", new LocalDate(1970, 1, 1), FAKE_IMAGE_DATA, "image/fake");
    }

    @SuppressWarnings("deprecation")
    @Test
    public void signedInUserMustGiveConsent() throws Exception {
        TestUser user = TestUserHelper.createAndSignInUser(ConsentTest.class, false);
        try {
            ForConsentedUsersApi userApi = user.getClient(ForConsentedUsersApi.class);
            assertFalse("User has not consented", user.getSession().isConsented());
            try {
                userApi.getSchedulesV1().execute();
                fail("Should have required consent.");
            } catch (ConsentRequiredException e) {
                assertEquals("Exception is a 412 Precondition Failed", 412, e.getStatusCode());
            }

            LocalDate date = new LocalDate(1970, 10, 10);
            ConsentSignature signature = new ConsentSignature().name(user.getEmail()).birthdate(date)
                    .scope(SPONSORS_AND_PARTNERS);
            userApi.createConsentSignature(user.getDefaultSubpopulation(), signature).execute();

            UserSessionInfo session = user.signInAgain();

            assertTrue("User has consented", session.isConsented());
            // This should succeed
            userApi.getSchedulesV1().execute();
        } finally {
            user.signOutAndDeleteUser();
        }
    }

    @SuppressWarnings("deprecation")
    @Test(expected = InvalidEntityException.class)
    public void userMustMeetMinAgeRequirements() throws Exception {
        TestUser user = null;
        try {
            user = TestUserHelper.createAndSignInUser(ConsentTest.class, false);
        } catch (ConsentRequiredException e) {
            // this is expected when you sign in.
        }
        try {
            ForConsentedUsersApi userApi = user.getClient(ForConsentedUsersApi.class);
            try {
                userApi.getSchedulesV1();
            } catch (ConsentRequiredException e) {
                // this is what we're expecting now
            }
            LocalDate date = LocalDate.now();
            ConsentSignature signature = new ConsentSignature().name(user.getEmail()).birthdate(date)
                    .scope(SPONSORS_AND_PARTNERS);
            userApi.createConsentSignature(user.getDefaultSubpopulation(), signature).execute();
        } finally {
            user.signOutAndDeleteUser();
        }
    }

    @Test
    public void jsonSerialization() throws Exception {
        // setup
        String sigJson = "{\"name\":\"Jason McSerializer\"," + 
                "\"birthdate\":\"1985-12-31\"," +
                "\"imageData\":\"" + FAKE_IMAGE_DATA + "\"," + 
                "\"imageMimeType\":\"image/fake\"}";

        // de-serialize and validate
        ConsentSignature sig = RestUtils.GSON.fromJson(sigJson, ConsentSignature.class);

        assertEquals("(ConsentSignature instance) name matches", "Jason McSerializer", sig.getName());
        assertEquals("(ConsentSignature instance) birthdate matches", "1985-12-31",
                sig.getBirthdate().toString(ISODateTimeFormat.date()));
        assertEquals("(ConsentSignature instance) imageData matches", FAKE_IMAGE_DATA, sig.getImageData());
        assertEquals("(ConsentSignature instance) imageMimeType matches", "image/fake", sig.getImageMimeType());

        // re-serialize, then parse as a raw map to validate the JSON
        String reserializedJson = RestUtils.GSON.toJson(sig);
        Map<String, String> jsonAsMap = RestUtils.GSON.fromJson(reserializedJson, Map.class);
        assertEquals("JSON map has exactly 4 elements", 4, jsonAsMap.size());
        assertEquals("(JSON map) name matches", "Jason McSerializer", jsonAsMap.get("name"));
        assertEquals("(JSON map) birthdate matches", "1985-12-31", jsonAsMap.get("birthdate"));
        assertEquals("(JSON map) imageData matches", FAKE_IMAGE_DATA, jsonAsMap.get("imageData"));
        assertEquals("(JSON map) imageMimeType matches", "image/fake", jsonAsMap.get("imageMimeType"));
    }

    // helper method to test consent with and without images
    @SuppressWarnings("deprecation")
    private static void giveAndGetConsentHelper(String name, LocalDate birthdate, String imageData,
            String imageMimeType) throws Exception {
        TestUser testUser = TestUserHelper.createAndSignInUser(ConsentTest.class, false);

        ConsentSignature sig = new ConsentSignature().name(name).birthdate(birthdate).imageData(imageData)
                .imageMimeType(imageMimeType).scope(ALL_QUALIFIED_RESEARCHERS);
        try {
            ForConsentedUsersApi userApi = testUser.getClient(ForConsentedUsersApi.class);

            assertFalse("User has not consented", testUser.getSession().isConsented());
            assertFalse(RestUtils.isUserConsented(testUser.getSession()));

            // get consent should fail if the user hasn't given consent
            try {
                userApi.getConsentSignature(testUser.getDefaultSubpopulation()).execute();
                fail("ConsentRequiredException not thrown");
            } catch (ConsentRequiredException ex) {
                // expected
            }
            
            // Verify this does not change for older versions of the SDK that were not mapped
            // to intercept and update the session from this call.
            String existingSessionToken = testUser.getSession().getSessionToken();
            
            // give consent
            UserSessionInfo session = userApi.createConsentSignature(testUser.getDefaultSubpopulation(), sig).execute()
                    .body();
            
            assertEquals(existingSessionToken, session.getSessionToken());

            // Session should be updated to reflect this consent.
            ConsentStatus status = session.getConsentStatuses().get(testUser.getDefaultSubpopulation());
            assertTrue(status.isConsented());
            assertTrue(status.isSignedMostRecentConsent());

            // Participant record includes the sharing scope that was set
            StudyParticipant participant = userApi.getUsersParticipantRecord(false).execute().body();
            assertEquals(ALL_QUALIFIED_RESEARCHERS, participant.getSharingScope());

            // Session now shows consent...
            session = testUser.signInAgain();
            assertTrue(RestUtils.isUserConsented(session));

            // get consent and validate that it's the same consent
            ConsentSignature sigFromServer = userApi.getConsentSignature(testUser.getDefaultSubpopulation()).execute()
                    .body();
            assertEquals("name matches", name, sigFromServer.getName());
            assertEquals("birthdate matches", birthdate, sigFromServer.getBirthdate());
            assertEquals("imageData matches", imageData, sigFromServer.getImageData());
            assertEquals("imageMimeType matches", imageMimeType, sigFromServer.getImageMimeType());
            assertNotNull(sigFromServer.getSignedOn());
            
            // giving consent again will throw
            try {
                // See BRIDGE-1568
                sig = new ConsentSignature().name(sig.getName()).birthdate(sig.getBirthdate())
                        .scope(ALL_QUALIFIED_RESEARCHERS).imageData(sig.getImageData())
                        .imageMimeType(sig.getImageMimeType());
                userApi.createConsentSignature(testUser.getDefaultSubpopulation(), sig).execute();
                fail("EntityAlreadyExistsException not thrown");
            } catch (EntityAlreadyExistsException ex) {
                // expected
            }

            // The remote session should also reflect the sharing scope
            AuthenticationApi authApi = testUser.getClient(AuthenticationApi.class);
            authApi.signOut().execute();

            session = testUser.signInAgain();
            existingSessionToken = session.getSessionToken();
            
            assertEquals(ALL_QUALIFIED_RESEARCHERS, session.getSharingScope());
            assertTrue(RestUtils.isUserConsented(session));

            // withdraw consent
            Withdrawal withdrawal = new Withdrawal().reason("Withdrawing test user from study");
            userApi = testUser.getClient(ForConsentedUsersApi.class);
            session = userApi.withdrawConsentFromSubpopulation(testUser.getDefaultSubpopulation(), withdrawal).execute()
                    .body();
            
            // Again, the session token should not change as a result of withdrawing.
            assertEquals(existingSessionToken, session.getSessionToken());

            // Session should reflect the withdrawal of consent
            status = session.getConsentStatuses().get(testUser.getDefaultSubpopulation());
            assertFalse(status.isConsented());
            assertFalse(status.isSignedMostRecentConsent());
            assertNull(status.getSignedOn());
            
            // Get the consent signature and verify it is withdrawn. You can't get it as the test 
            // user... the user is withdrawn! 
            ParticipantsApi participantsApi = researchUser.getClient(ParticipantsApi.class);
            StudyParticipant retrieved = participantsApi.getParticipantById(testUser.getUserId(), true).execute().body();
            
            List<UserConsentHistory> history = retrieved.getConsentHistories().get(testUser.getDefaultSubpopulation());
            assertTrue( history.get(0).getWithdrewOn().isAfter(DateTime.now().minusHours(1)) );
            
            // This method should now (immediately) throw a ConsentRequiredException
            try {
                userApi.getSchedulesV1().execute();
                fail("Should have thrown exception");
            } catch (ConsentRequiredException e) {
                // what we want
            }
        } finally {
            testUser.signOutAndDeleteUser();
        }
    }

    @Test
    public void canResendConsentAgreement() throws Exception {
        TestUser testUser = TestUserHelper.createAndSignInUser(ConsentTest.class, true);
        try {
            ForConsentedUsersApi userApi = testUser.getClient(ForConsentedUsersApi.class);
            userApi.resendConsentAgreement(testUser.getDefaultSubpopulation()).execute();
        } finally {
            testUser.signOutAndDeleteUser();
        }
    }

    @Test
    public void canResendConsentAgreementForPhone() throws Exception {
        // Request phone consent.
        Response<Message> response = phoneOnlyTestUser.getClient(ForConsentedUsersApi.class)
                .resendConsentAgreement(phoneOnlyTestUser.getDefaultSubpopulation()).execute();
        assertEquals(202, response.code());

        // Verify message logs contains the expected message.
        SmsMessage message = adminUser.getClient(InternalApi.class)
                .getMostRecentSmsMessage(phoneOnlyTestUser.getUserId()).execute().body();
        assertEquals(phoneOnlyTestUser.getPhone().getNumber(), message.getPhoneNumber());
        assertNotNull(message.getMessageId());
        assertEquals(TRANSACTIONAL, message.getSmsType());
        assertEquals(phoneOnlyTestUser.getAppId(), message.getAppId());

        // Message body isn't constrained by the test, so just check that it exists.
        assertNotNull(message.getMessageBody());

        // Clock skew on Jenkins can be known to go as high as 10 minutes. For a robust test, simply check that the
        // message was sent within the last hour.
        assertTrue(message.getSentOn().isAfter(DateTime.now().minusHours(1)));

        // Verify the health code matches.
        StudyParticipant participant = researchUser.getClient(ForResearchersApi.class)
                .getParticipantById(phoneOnlyTestUser.getUserId(), false).execute().body();
        assertEquals(participant.getHealthCode(), message.getHealthCode());

        // Verify the SMS message log was written to health data.
        DateTime messageSentOn = message.getSentOn();
        Optional<HealthDataRecord> smsMessageRecordOpt = Tests.retryHelper(() -> phoneOnlyTestUser
                        .getClient(InternalApi.class).getHealthDataByCreatedOn(messageSentOn, messageSentOn).execute()
                        .body().getItems().stream()
                        .filter(r -> r.getSchemaId().equals("sms-messages-sent-from-bridge")).findAny(),
                Optional::isPresent);
        HealthDataRecord smsMessageRecord = smsMessageRecordOpt.get();
        assertEquals("sms-messages-sent-from-bridge", smsMessageRecord.getSchemaId());
        assertEquals(1, smsMessageRecord.getSchemaRevision().intValue());

        // SMS Message log saves the date as epoch milliseconds.
        assertEquals(messageSentOn.getMillis(), smsMessageRecord.getCreatedOn().getMillis());

        // Verify data.
        Map<String, String> recordDataMap = RestUtils.toType(smsMessageRecord.getData(), Map.class);
        assertEquals("Transactional", recordDataMap.get("smsType"));
        assertNotNull(recordDataMap.get("messageBody"));
        assertEquals(messageSentOn.getMillis(), DateTime.parse(recordDataMap.get("sentOn")).getMillis());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void canWithdrawFromApp() throws Exception {
        TestUser testUser = TestUserHelper.createAndSignInUser(ConsentTest.class, true);
        try {
            UserSessionInfo session = testUser.getSession();

            // Can get activities without an error... user is indeed consented.
            ForConsentedUsersApi userApi = testUser.getClient(ForConsentedUsersApi.class);
            userApi.getScheduledActivities("+00:00", 1, null).execute();

            assertTrue(RestUtils.isUserConsented(session));

            Withdrawal withdrawal = new Withdrawal().reason("I'm just a test user.");
            testUser.getClient(ForConsentedUsersApi.class).withdrawFromApp(withdrawal).execute();

            try {
                testUser.signInAgain();
                fail("Should have thrown exception");
            } catch (EntityNotFoundException e) {
            }
        } finally {
            testUser.signOutAndDeleteUser();
        }
    }

    @Test
    public void canWithdrawParticipantFromApp() throws Exception {
        String externalId = Tests.randomIdentifier(getClass());
        SignUp signUp = new SignUp().externalIds(ImmutableMap.of(STUDY_ID_2, externalId));
        TestUser testUser = TestUserHelper.createAndSignInUser(ConsentTest.class, true, signUp);
        String userId = testUser.getSession().getId();
        try {
            ParticipantsApi participantsApi = researchUser.getClient(ParticipantsApi.class);

            Withdrawal withdrawal = new Withdrawal().reason("Reason for withdrawal.");
            Message message = participantsApi.withdrawParticipantFromApp(userId, withdrawal).execute().body();
            assertEquals("User has been withdrawn from one or more studies in the app.", message.getMessage());

            // Retrieve the account and verify it has been processed correctly.
            StudyParticipant theUser = participantsApi.getParticipantById(userId, true).execute().body();
            assertEquals(NO_SHARING, theUser.getSharingScope());
            assertFalse(theUser.isNotifyByEmail());
            assertNull(theUser.getEmail());
            assertFalse(theUser.isEmailVerified());
            assertNull(theUser.getPhone());
            assertFalse(theUser.isPhoneVerified());
            assertTrue(theUser.getExternalIds().isEmpty());
            for (List<UserConsentHistory> histories : theUser.getConsentHistories().values()) {
                for (UserConsentHistory oneHistory : histories) {
                    assertNotNull(oneHistory.getWithdrewOn());
                }
            }
        } finally {
            testUser.signOutAndDeleteUser();
        }
    }

    @FunctionalInterface
    public abstract interface WithdrawMethod {
        public void withdraw(TestUser user, List<String> studyIds, Subpopulation subpop) throws Exception;
    }

    @Test
    public void consentAndWithdrawFromSubpopUpdatesDataGroupsAndStudies() throws Exception {
        withdrawalTest((user, studyIds, subpop) -> {
            ForConsentedUsersApi usersApi = user.getClient(ForConsentedUsersApi.class);
            UserSessionInfo updatedSession = usersApi.withdrawConsentFromSubpopulation(
                    subpop.getGuid(), WITHDRAWAL).execute().body();
            
            // verify that the session contains all the correct information
            assertFalse(updatedSession.getStudyIds().contains(STUDY_ID_2));
            assertEquals(updatedSession.getDataGroups(), ImmutableList.of("test_user"));
            assertNotNull(updatedSession.getEnrollments().get(STUDY_ID_2).getWithdrawnOn());
            assertTrue(updatedSession.getEnrollments().get(STUDY_ID_2).isWithdrawnBySelf());
        });
    }
    
    @Test
    public void consentAndWithdrawFromAppUpdatesDataGroupsAndStudies() throws Exception {
        withdrawalTest((user, studyIds, subpop) -> {
            ForConsentedUsersApi usersApi = user.getClient(ForConsentedUsersApi.class);
            usersApi.withdrawFromApp(WITHDRAWAL).execute();
            
            ParticipantsApi participantsApi = researchUser.getClient(ParticipantsApi.class);

            // verify the participant is now not enrolled in any study
            StudyParticipant participant = participantsApi.getParticipantById(user.getUserId(), false).execute().body();
            assertTrue(participant.getStudyIds().isEmpty());
            assertEquals(participant.getDataGroups(), ImmutableList.of("test_user"));
            assertTrue(participant.getEnrollments().get(STUDY_ID_2).isWithdrawnBySelf());
        });
    }
    
    @Test
    public void canWithdrawOnParticipantsBehalf() throws Exception {
        withdrawalTest((user, studyIds, subpop) -> {
            ForResearchersApi researchersApi = researchUser.getClient(ForResearchersApi.class);
            Enrollment en = researchersApi.withdrawParticipant(STUDY_ID_2, user.getUserId(), "Withdrawn").execute().body();
            
            assertEquals(researchUser.getUserId(), en.getWithdrawnBy());
            
            ParticipantsApi participantsApi = researchUser.getClient(ParticipantsApi.class);

            // verify the participant is now not enrolled in any study
            StudyParticipant participant = participantsApi.getParticipantById(user.getUserId(), false).execute().body();
            assertNull(participant.getEnrollments().get(STUDY_ID_2).isWithdrawnBySelf());
            assertNotNull(participant.getEnrollments().get(STUDY_ID_2).getWithdrawnOn());
        });
    }
    
    @Test
    public void canWithdrawFromAppOnParticipantsBehalf() throws Exception {
        withdrawalTest((user, studyIds, subpop) -> {
            ForResearchersApi researchersApi = researchUser.getClient(ForResearchersApi.class);
            researchersApi.withdrawParticipantFromApp(user.getUserId(), 
                    new Withdrawal().reason("Withdrawn")).execute().body();
            
            ParticipantsApi participantsApi = researchUser.getClient(ParticipantsApi.class);

            // verify the participant is now not enrolled in any study
            StudyParticipant participant = participantsApi.getParticipantById(user.getUserId(), false).execute().body();
            assertNull(participant.getEmail());
            assertNull(participant.getEnrollments().get(STUDY_ID_2).isWithdrawnBySelf());
            assertNotNull(participant.getEnrollments().get(STUDY_ID_2).getWithdrawnOn());
        });
    }
    
    @Test
    public void canConsentToSupplementalConsent() throws Exception {
        SubpopulationsApi subpopsApi = adminUser.getClient(SubpopulationsApi.class);
        TestUser user = null;
        Subpopulation subpop = new Subpopulation();
        try {
            subpop.setName("Supplemental Consent");
            subpop.setAutoSendConsentSuppressed(true);

            GuidVersionHolder keys = subpopsApi.createSubpopulation(subpop).execute().body();
            subpop.setGuid(keys.getGuid());
            subpop.setVersion(keys.getVersion());
            
            user = TestUserHelper.createAndSignInUser(ConsentTest.class, true);
            
            ConsentSignature sig = new ConsentSignature()
                    .name(user.getSession().getFirstName() + " " + user.getSession().getLastName())
                    .birthdate(LocalDate.parse("2000-01-01"))
                    .scope(ALL_QUALIFIED_RESEARCHERS);
            
            ConsentsApi consentsApi = user.getClient(ConsentsApi.class);
            UserSessionInfo session = consentsApi.createConsentSignature(subpop.getGuid(), sig).execute().body();
            
            ConsentStatus status = session.getConsentStatuses().get(subpop.getGuid());
            assertTrue(status.isConsented());
            
        } finally {
            if (user != null) {
                user.signOutAndDeleteUser();
            }
            if (subpop.getGuid() != null) {
                subpopsApi.deleteSubpopulation(subpop.getGuid(), true).execute();    
            }
        }
    }
    
    @Test
    public void consentToResearchCorrectlyTriggersScheduleEvents() throws Exception {
        ForDevelopersApi developersApi = adminUser.getClient(ForDevelopersApi.class);
        AssessmentsApi asmtsApi = adminUser.getClient(AssessmentsApi.class);
        
        Study study = developersApi.getStudy(Tests.STUDY_ID_1).execute().body();
        
        // If there's a schedule associated to study 1, we need to delete it.
        if (study.getScheduleGuid() != null) {
            adminUser.getClient(SchedulesV2Api.class).deleteSchedule(study.getScheduleGuid()).execute();
        }
        
        assessmentA = asmtsApi.createAssessment(new Assessment()
                .phase(Assessment.PhaseEnum.DRAFT)
                .identifier(randomIdentifier(ConsentTest.class))
                .osName("Universal")
                .ownerId(adminUser.getSession().getOrgMembership())
                .title("Assessment A")).execute().body();
        StudyBurst burst = new StudyBurst().identifier("foo").interval("P1W")
                .updateType(IMMUTABLE).occurrences(2).originEventId("enrollment");
        Session s1 = new Session()
                .name("Session #1")
                .addStartEventIdsItem(FAKE_ENROLLMENT)
                .addStudyBurstIdsItem("foo")
                .delay("P2D")
                .interval("P3D")
                .performanceOrder(SEQUENTIAL)
                .addAssessmentsItem(asmtToReference(assessmentA))
                .addTimeWindowsItem(new TimeWindow()
                        .startTime("08:00").expiration("PT6H"))
                .addTimeWindowsItem(new TimeWindow()
                        .startTime("16:00").expiration("PT6H").persistent(true));
        schedule = developersApi.saveScheduleForStudy(STUDY_ID_1, new Schedule2()
                .name("ConsentTest Schedule")
                .duration("P22D")
                .addStudyBurstsItem(burst)
                .addSessionsItem(s1)).execute().body();
        
        user = TestUserHelper.createAndSignInUser(ConsentTest.class, false);
        
        ForConsentedUsersApi userApi = user.getClient(ForConsentedUsersApi.class);
        ConsentSignature sig = new ConsentSignature()
                .name("Test User [ConsentTest]")
                .scope(NO_SHARING)
                .birthdate(LocalDate.parse("2000-01-01"));
        
        // This fails without server code to ensure the caller has access to the study they have
        // just enrolled into.
        userApi.createConsentSignature("api", sig).execute().body();
        
        StudyActivityEventList list = userApi.getStudyActivityEvents(STUDY_ID_1).execute().body();
        
        Set<String> events = list.getItems().stream().map(StudyActivityEvent::getEventId).collect(toSet());
        assertTrue(events.containsAll(ImmutableSet.of("enrollment", "study_burst:foo:01", "study_burst:foo:02")));
        
        // Verify that sign up also works. Same issue, different code path. The set up for this is expensive,
        // so we’re doing it here and not in a separate test (since we've set up the right study/schedule to 
        // trigger the issue).
        
        externalId = Tests.randomIdentifier(getClass());
        SignUp signUp = new SignUp().appId(TEST_APP_ID)
                .dataGroups(ImmutableList.of("test_user"))
                .externalIds(ImmutableMap.of(STUDY_ID_1, externalId)).password(PASSWORD);

        TestUser admin = TestUserHelper.getSignedInAdmin();
        ApiClientProvider provider = Tests.getUnauthenticatedClientProvider(admin.getClientManager(), TEST_APP_ID);
        AuthenticationApi authApi = provider.getClient(AuthenticationApi.class);
        authApi.signUp(signUp).execute();

        try {
            authApi.signIn(new SignIn().appId(TEST_APP_ID)
                    .externalId(externalId).password(PASSWORD)).execute().body();
            fail("Should have thrown exception");
        } catch(ConsentRequiredException e) {
            UserSessionInfo session = e.getSession();
            EnrollmentInfo en = session.getEnrollments().get(STUDY_ID_1);
            assertEquals(externalId, en.getExternalId());
        }
    }
    
    private AssessmentReference2 asmtToReference(Assessment asmt) {
        return new AssessmentReference2()
                .appId(TEST_APP_ID)
                .identifier(asmt.getIdentifier())
                .guid(asmt.getGuid());
    }
    
    private void withdrawalTest(WithdrawMethod withdrawMethod) throws Exception {
        TestUser user = null;
        Subpopulation subpop = null;
        TestUser devUser = TestUserHelper.createAndSignInUser(ConsentTest.class, true, DEVELOPER);
        SubpopulationsApi subpopApi = devUser.getClient(SubpopulationsApi.class);
        try {
            AppsApi appsApi = devUser.getClient(AppsApi.class);
            App app = appsApi.getUsersApp().execute().body();

            String dataGroup = Iterables.getFirst(app.getDataGroups(), null);
            List<String> dataGroupList = ImmutableList.of(dataGroup);
            List<String> studyIds = ImmutableList.of(STUDY_ID_2);

            // create an (optional) subpopulation that associates studies to a user
            subpop = new Subpopulation().name("Test Subpopulation").required(false);
            subpop.setStudyIdsAssignedOnConsent(studyIds);
            subpop.setDataGroupsAssignedWhileConsented(dataGroupList);
            subpop.setRequired(true);
            GuidVersionHolder subpopKeys = subpopApi.createSubpopulation(subpop).execute().body();
            subpop.setGuid(subpopKeys.getGuid());
            
            // The user is signed in to all required consents, so no need to consent again 
            // create a user and consent to that subpopulation. Verify that the session 
            // contains all the correct information
            user = TestUserHelper.createAndSignInUser(ConsentTest.class, true);
            UserSessionInfo session = user.getSession();
            assertTrue(session.getStudyIds().contains(STUDY_ID_2));
            assertTrue(session.getDataGroups().containsAll(dataGroupList));

            // withdraw and verify
            withdrawMethod.withdraw(user, studyIds, subpop);
        } finally {
            // delete the user
            if (user != null) {
                user.signOutAndDeleteUser();
            }
            // delete the subpopulation
            if (subpop != null && subpop.getGuid() != null) {
                adminUser.getClient(SubpopulationsApi.class).deleteSubpopulation(subpop.getGuid(), true).execute();
            }
            devUser.signOutAndDeleteUser();
        }
    }
}
