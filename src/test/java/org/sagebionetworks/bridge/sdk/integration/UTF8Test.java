package org.sagebionetworks.bridge.sdk.integration;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.sagebionetworks.bridge.rest.api.AuthenticationApi;
import org.sagebionetworks.bridge.rest.api.ForConsentedUsersApi;
import org.sagebionetworks.bridge.rest.api.ForSuperadminsApi;
import org.sagebionetworks.bridge.rest.model.SignIn;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.rest.model.UserSessionInfo;
import org.sagebionetworks.bridge.user.TestUserHelper;
import org.sagebionetworks.bridge.user.TestUserHelper.TestUser;

import static org.junit.Assert.assertEquals;
import static org.sagebionetworks.bridge.sdk.integration.Tests.API_SIGNIN;

@Category(IntegrationSmokeTest.class)
public class UTF8Test {
    @Test
    public void canSaveAndRetrieveDataStoredInDynamo() throws Exception {
        String studyId = Tests.randomIdentifier(UTF8Test.class);
        String studyName = "☃지구상의　３대　극지라　불리는";
        ForSuperadminsApi superadminApi = TestUserHelper.getSignedInAdmin().getClient(ForSuperadminsApi.class);

        // make minimal study
        Study study = new Study();
        study.setIdentifier(studyId);
        study.setName(studyName);
        study.setSponsorName(studyName);
        study.setTechnicalEmail("bridge-testing+technical@sagebase.org");
        study.setSupportEmail("bridge-testing+support@sagebase.org");
        study.setConsentNotificationEmail("bridge-testing+consent@sagebase.org");
        study.setEmailVerificationEnabled(true);

        // create study
        superadminApi.createStudy(study).execute();

        try {
            superadminApi.adminChangeStudy(new SignIn().study(studyId)).execute();
            
            // get study back and verify fields
            Study returnedStudy = superadminApi.getStudy(studyId).execute().body();
            assertEquals(studyId, returnedStudy.getIdentifier());
            assertEquals(studyName, returnedStudy.getName());
        } finally {
            superadminApi.adminChangeStudy(API_SIGNIN).execute();
            // clean-up: delete study
            superadminApi.deleteStudy(studyId, true).execute();
        }
    }

    @Test
    public void canSaveAndRetrieveDataStoredInRedis() throws Exception {
        TestUser testUser = TestUserHelper.createAndSignInUser(UTF8Test.class, true);
        try {
            ForConsentedUsersApi usersApi = testUser.getClient(ForConsentedUsersApi.class);

            StudyParticipant participant = new StudyParticipant();
            participant.setFirstName("☃");
            participant.setLastName("지구상의　３대　극지라　불리는");

            usersApi.updateUsersParticipantRecord(participant).execute();

            // Force a refresh of the Redis session cache.
            AuthenticationApi authApi = testUser.getClient(AuthenticationApi.class);
            authApi.signOut().execute();
            UserSessionInfo session = authApi.signInV4(testUser.getSignIn()).execute().body();

            assertEquals("☃", session.getFirstName());
            assertEquals("지구상의　３대　극지라　불리는", session.getLastName());
        } finally {
            testUser.signOutAndDeleteUser();
        }
    }
}
