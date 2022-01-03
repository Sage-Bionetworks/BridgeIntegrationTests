package org.sagebionetworks.bridge.sdk.integration;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.sagebionetworks.bridge.rest.api.ForConsentedUsersApi;
import org.sagebionetworks.bridge.rest.api.ParticipantsApi;
import org.sagebionetworks.bridge.rest.exceptions.ConsentRequiredException;
import org.sagebionetworks.bridge.user.TestUser;
import org.sagebionetworks.bridge.user.TestUserHelper;

import static org.junit.Assert.fail;
import static org.sagebionetworks.bridge.rest.model.Role.DEVELOPER;
import static org.sagebionetworks.bridge.util.IntegTestUtils.TEST_APP_2_ID;

import org.joda.time.DateTime;

public class SessionRefreshTest {
    private static TestUser user;
    private static TestUser app2Developer;

    @BeforeClass
    public static void createUser() throws Exception {
        user = TestUserHelper.createAndSignInUser(SessionRefreshTest.class, false);
        app2Developer = TestUserHelper.createAndSignInUser(SessionRefreshTest.class, TEST_APP_2_ID, DEVELOPER);
    }

    @AfterClass
    public static void deleteUser() throws Exception {
        if (user != null) {
            user.signOutAndDeleteUser();
        }
        if (app2Developer != null) {
            app2Developer.signOutAndDeleteUser();
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testReauthenticationThrowsConsentException() throws Exception {
        // User starts out signed in. Initial call succeeds.
        user.getClient(ParticipantsApi.class).getUsersParticipantRecord(false).execute();

        // Sign user out.
        user.signOut();

        // Call should succeed again. Sign-in happens again behind the scenes
        try {
            user.getClient(ForConsentedUsersApi.class)
                    .getScheduledActivitiesByDateRange(DateTime.now().minusDays(2), DateTime.now()).execute();
            fail("ConsentRequiredException expected");
        } catch (ConsentRequiredException e) {
            // this is expected
        }
    }

    @Test
    public void testReauthenticationAcrossStudies() throws Exception {
        // Use developer from the Shared app to test across studies. Initial call succeeds.
        app2Developer.getClient(ParticipantsApi.class).getUsersParticipantRecord(false).execute();

        // Sign user out.
        app2Developer.signOut();

        // Call should succeed again. Sign-in happens again behind the scenes
        app2Developer.getClient(ParticipantsApi.class).getUsersParticipantRecord(false).execute();
    }
}
