package org.sagebionetworks.bridge.sdk.integration;

import org.junit.Test;
import org.sagebionetworks.bridge.rest.api.AuthenticationApi;
import org.sagebionetworks.bridge.rest.api.ParticipantsApi;
import org.sagebionetworks.bridge.rest.exceptions.BadRequestException;
import org.sagebionetworks.bridge.rest.exceptions.EntityNotFoundException;
import org.sagebionetworks.bridge.rest.exceptions.InvalidEntityException;
import org.sagebionetworks.bridge.rest.model.SharingScope;
import org.sagebionetworks.bridge.rest.model.SignIn;
import org.sagebionetworks.bridge.rest.model.SignUp;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.rest.model.UserSessionInfo;
import org.sagebionetworks.bridge.user.TestUserHelper;
import org.sagebionetworks.bridge.user.TestUserHelper.TestUser;

import java.io.IOException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.sagebionetworks.bridge.rest.model.SharingScope.NO_SHARING;
import static org.sagebionetworks.bridge.sdk.integration.Tests.PASSWORD;
import static org.sagebionetworks.bridge.util.IntegTestUtils.TEST_APP_ID;

public class SignUpTest {

    @Test
    public void defaultValuesExist() throws Exception {
        TestUser testUser = TestUserHelper.createAndSignInUser(SignUpTest.class, true);
        try {
            ParticipantsApi participantsApi = testUser.getClientManager().getClient(ParticipantsApi.class);

            StudyParticipant participant = participantsApi.getUsersParticipantRecord(false).execute().body();
            assertTrue(participant.isNotifyByEmail());
            assertEquals(SharingScope.NO_SHARING, participant.getSharingScope());
        } finally {
            testUser.signOutAndDeleteUser();
        }
    }
    
    @Test
    public void canAuthenticateAndCreateClientAndSignOut() throws IOException {
        TestUser testUser = TestUserHelper.createAndSignInUser(SignUpTest.class, true);
        try {
            AuthenticationApi authApi = testUser.getClient(AuthenticationApi.class);
            
            authApi.signOut().execute();
            
            SignIn signIn = testUser.getSignIn();
            UserSessionInfo session = authApi.signInV4(signIn).execute().body();
            
            assertTrue(session.isAuthenticated());

            authApi.signOut().execute();
        } finally {
            testUser.signOutAndDeleteUser();
        }
    }
    
    @Test(expected = EntityNotFoundException.class)
    public void badAppIdReturns404() throws IOException {
        TestUser testUser = TestUserHelper.createAndSignInUser(SignUpTest.class, true);
        try {
            AuthenticationApi authApi = testUser.getClient(AuthenticationApi.class);
            
            SignIn email = new SignIn().appId("junk").email("bridge-testing@sagebase.org");
            authApi.requestResetPassword(email).execute();
        } finally {
            testUser.signOutAndDeleteUser();
        }
    }
    
    @Test(expected = BadRequestException.class)
    public void badEmailCredentialsReturnsException() throws IOException {
        TestUser testUser = TestUserHelper.createAndSignInUser(SignUpTest.class, true);
        try {
            AuthenticationApi authApi = testUser.getClient(AuthenticationApi.class);
            
            SignIn email = new SignIn().email("bridge-testing@sagebase.org");
            authApi.requestResetPassword(email).execute();
        } finally {
            testUser.signOutAndDeleteUser();
        }
    }
    
    @Test
    public void signUpWithExternalIdAndNoAccountFails() throws Exception {
        TestUser admin = TestUserHelper.getSignedInAdmin();
        AuthenticationApi authApi = admin.getClient(AuthenticationApi.class);

        String extId = Tests.randomIdentifier(SignUpTest.class);

        // In the API app we have two studies created by the initializer for integration tests,
        // and neither is "test", so one will be chosen at random. This is one of two ways we used
        // to accept this kind of sign up, both of which are now rejected.
        SignUp signUp = new SignUp().appId(TEST_APP_ID).dataGroups(ImmutableList.of("test_user"))
                .password(PASSWORD).externalId(extId).sharingScope(NO_SHARING);
        try {
            authApi.signUp(signUp).execute();
            fail("Should have thrown exception");
        } catch(InvalidEntityException e) {
        }

        signUp = new SignUp().appId(TEST_APP_ID).dataGroups(ImmutableList.of("test_user"))
                .password(PASSWORD).externalIds(ImmutableMap.of(Tests.STUDY_ID_2, extId))
                .sharingScope(NO_SHARING);
        try {
            authApi.signUp(signUp).execute();
            fail("Should have thrown exception");
        } catch(InvalidEntityException e) {
        }
    }
}
