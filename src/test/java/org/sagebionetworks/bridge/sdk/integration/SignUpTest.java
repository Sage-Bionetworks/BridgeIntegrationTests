package org.sagebionetworks.bridge.sdk.integration;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.sagebionetworks.bridge.rest.api.AppsApi;
import org.sagebionetworks.bridge.rest.api.AuthenticationApi;
import org.sagebionetworks.bridge.rest.api.ParticipantsApi;
import org.sagebionetworks.bridge.rest.exceptions.BadRequestException;
import org.sagebionetworks.bridge.rest.exceptions.EntityNotFoundException;
import org.sagebionetworks.bridge.rest.exceptions.InvalidEntityException;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.SharingScope;
import org.sagebionetworks.bridge.rest.model.SignIn;
import org.sagebionetworks.bridge.rest.model.SignUp;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.rest.model.UserSessionInfo;
import org.sagebionetworks.bridge.user.TestUser;
import org.sagebionetworks.bridge.user.TestUserHelper;
import org.sagebionetworks.bridge.util.IntegTestUtils;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(IntegrationSmokeTest.class)
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
            
            SignIn email = new SignIn().appId("junk").email("testing@bridgedigital.health");
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
            
            SignIn email = new SignIn().email("testing@bridgedigital.health");
            authApi.requestResetPassword(email).execute();
        } finally {
            testUser.signOutAndDeleteUser();
        }
    }
    
    @Test
    public void signUpForAppWithExternalIdValidation() throws Exception {
        TestUser admin = TestUserHelper.getSignedInAdmin();
        App app = admin.getClient(AppsApi.class).getUsersApp().execute().body();
        app.setExternalIdRequiredOnSignup(true);
        admin.getClient(AppsApi.class).updateUsersApp(app).execute();
        
        try {
            SignUp signUp = new SignUp()
                    .appId(app.getIdentifier())
                    .email(IntegTestUtils.makeEmail(SignUpTest.class))
                    .password("P@ssword`1");
            AuthenticationApi authApi = admin.getClient(AuthenticationApi.class);
            try {
                authApi.signUp(signUp).execute();
                fail("Should have thrown exception");
            } catch(InvalidEntityException e) {
                assertEquals("StudyParticipant is invalid: externalId is required", e.getMessage());
            }
        } finally {
            app = admin.getClient(AppsApi.class).getUsersApp().execute().body();
            app.setExternalIdRequiredOnSignup(false);
            admin.getClient(AppsApi.class).updateUsersApp(app).execute();
        }
    }
}
