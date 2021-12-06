package org.sagebionetworks.bridge.sdk.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.sagebionetworks.bridge.rest.model.Role.RESEARCHER;
import static org.sagebionetworks.bridge.sdk.integration.Tests.API_SIGNIN;
import static org.sagebionetworks.bridge.sdk.integration.Tests.SHARED_SIGNIN;
import static org.sagebionetworks.bridge.util.IntegTestUtils.SHARED_APP_ID;
import static org.sagebionetworks.bridge.util.IntegTestUtils.TEST_APP_ID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.api.AppsApi;
import org.sagebionetworks.bridge.rest.api.AuthenticationApi;
import org.sagebionetworks.bridge.rest.exceptions.EntityAlreadyExistsException;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.SignIn;
import org.sagebionetworks.bridge.rest.api.ForAdminsApi;
import org.sagebionetworks.bridge.rest.api.ForSuperadminsApi;
import org.sagebionetworks.bridge.rest.api.ParticipantsApi;
import org.sagebionetworks.bridge.rest.exceptions.EntityNotFoundException;
import org.sagebionetworks.bridge.rest.model.SignUp;
import org.sagebionetworks.bridge.rest.model.UserSessionInfo;
import org.sagebionetworks.bridge.user.TestUserHelper;
import org.sagebionetworks.bridge.user.TestUserHelper.TestUser;
import org.sagebionetworks.bridge.util.IntegTestUtils;

public class UserManagementTest {
    
    private TestUser admin;
    private TestUser researcher;

    @Before
    public void before() throws Exception {
        admin = TestUserHelper.getSignedInAdmin();

        researcher = TestUserHelper.createAndSignInUser(UserManagementTest.class, true, RESEARCHER);
    }

    @After
    public void after() throws Exception {
        if (researcher != null) {
            researcher.signOutAndDeleteUser(); //must do before admin session signout    
        }
        admin.signOut();
    }

    @Test
    public void canCreateAndSignOutAndDeleteUser() throws Exception {
        String email = IntegTestUtils.makeEmail(UserManagementTest.class);
        String password = "P4ssword";

        SignUp signUp = new SignUp();
        signUp.setAppId(admin.getAppId());
        signUp.setEmail(email);
        signUp.setConsent(true);
        signUp.setPassword(password);
        
        ForAdminsApi adminApi = admin.getClient(ForAdminsApi.class);
        
        UserSessionInfo userSession = adminApi.createUser(signUp).execute().body();
        assertNotNull(userSession.getId());

        // Can sign in with this user.
        SignIn signIn = new SignIn().appId(admin.getAppId()).email(email).password(password);
        ClientManager newUserClientManager = new ClientManager.Builder().withSignIn(signIn).build();
        AuthenticationApi newUserAuthApi = newUserClientManager.getClient(AuthenticationApi.class);
        newUserAuthApi.signInV4(signIn).execute();

        // Creating the user again throws an EntityAlreadyExists.
        try {
            adminApi.createUser(signUp).execute();
            fail("expected exception");
        } catch (EntityAlreadyExistsException ex) {
            // expected exception
        }

        adminApi.deleteUser(userSession.getId()).execute();

        try {
            ParticipantsApi participantsApi = researcher.getClient(ParticipantsApi.class);
            participantsApi.getParticipantById(userSession.getId(), false).execute();
            fail("Should have thrown an exception");
        } catch(EntityNotFoundException e) {
            // expected exception
        }
    }
    
    @Test
    public void canSignInAsAdminAndChangeStudy() throws Exception {
        // Unfortunately right now, we only have one study, but try all the APIs to make sure you
        // don't get any mistakes
        admin.signOut();
        
        ForSuperadminsApi superadminApi = admin.getClient(ForSuperadminsApi.class);
        AuthenticationApi authApi = admin.getClient(AuthenticationApi.class);
        SignIn signIn = new SignIn().appId(admin.getAppId())
                .email(admin.getEmail()).password((admin.getPassword()));
        
        superadminApi.adminSignIn(signIn).execute().body();
        App currentApp = admin.getClient(AppsApi.class).getUsersApp().execute().body();
        assertEquals(TEST_APP_ID, currentApp.getIdentifier());
        
        superadminApi = admin.getClient(ForSuperadminsApi.class);
        authApi.changeApp(SHARED_SIGNIN).execute().body();
        
        currentApp = admin.getClient(AppsApi.class).getUsersApp().execute().body();
        assertEquals(SHARED_APP_ID, currentApp.getIdentifier());
        
        // now reverse the order
        admin.signOut(); // so it's not necessary to switch back to the API study for future tests
        
        superadminApi = admin.getClient(ForSuperadminsApi.class);
        signIn = new SignIn().appId(SHARED_APP_ID)
                .email(admin.getEmail()).password((admin.getPassword()));
        
        superadminApi.adminSignIn(signIn).execute().body();
        currentApp = admin.getClient(AppsApi.class).getUsersApp().execute().body();
        assertEquals(SHARED_APP_ID, currentApp.getIdentifier());
        
        superadminApi = admin.getClient(ForSuperadminsApi.class);
        authApi.changeApp(API_SIGNIN).execute().body();
        
        currentApp = admin.getClient(AppsApi.class).getUsersApp().execute().body();
        assertEquals(TEST_APP_ID, currentApp.getIdentifier());
    }
}
