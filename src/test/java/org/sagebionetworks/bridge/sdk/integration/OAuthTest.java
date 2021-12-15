package org.sagebionetworks.bridge.sdk.integration;

import static java.lang.String.format;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.*;
import static org.sagebionetworks.bridge.rest.model.Role.DEVELOPER;
import static org.sagebionetworks.bridge.sdk.integration.Tests.API_SIGNIN;
import static org.sagebionetworks.bridge.sdk.integration.Tests.SHARED_SIGNIN;
import static org.sagebionetworks.bridge.sdk.integration.Tests.escapeJSON;
import static org.sagebionetworks.bridge.util.IntegTestUtils.CONFIG;
import static org.sagebionetworks.bridge.util.IntegTestUtils.SHARED_APP_ID;
import static org.sagebionetworks.bridge.util.IntegTestUtils.TEST_APP_ID;

import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Request;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.RestUtils;
import org.sagebionetworks.bridge.rest.api.AppsApi;
import org.sagebionetworks.bridge.rest.api.AuthenticationApi;
import org.sagebionetworks.bridge.rest.api.ForConsentedUsersApi;
import org.sagebionetworks.bridge.rest.exceptions.EntityNotFoundException;
import org.sagebionetworks.bridge.rest.exceptions.UnauthorizedException;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.AppList;
import org.sagebionetworks.bridge.rest.model.Environment;
import org.sagebionetworks.bridge.rest.model.OAuthAuthorizationToken;
import org.sagebionetworks.bridge.rest.model.SignIn;
import org.sagebionetworks.bridge.rest.model.UserSessionInfo;
import org.sagebionetworks.bridge.user.TestUserHelper;
import org.sagebionetworks.bridge.user.TestUserHelper.TestUser;

@Category(IntegrationSmokeTest.class)
public class OAuthTest {
    private static final String SYNAPSE_LOGIN_URL = "auth/v1/login";
    private static final String SYNAPSE_OAUTH_CONSENT = "auth/v1/oauth2/consent";

    private TestUser admin;
    private TestUser user;
    
    private String adminUserId;
    
    @Before
    public void before() throws Exception {
        admin = TestUserHelper.getSignedInAdmin(true);
        adminUserId = admin.getUserId();
    }
    
    @After
    public void after() throws Exception {
        if (user != null) {
            user.signOutAndDeleteUser();
        }
    }
    
    @Test(expected = EntityNotFoundException.class)
    public void requestOAuthAccessTokenExists() throws Exception {
        user = TestUserHelper.createAndSignInUser(OAuthTest.class, true);
        ForConsentedUsersApi usersApi = user.getClient(ForConsentedUsersApi.class);
        
        OAuthAuthorizationToken token = new OAuthAuthorizationToken().authToken("authToken");
        usersApi.requestOAuthAccessToken("vendorId", token).execute().body();
    }
    
    @Test
    public void signInWithSynapseAccount() throws Exception {
        String oauthClientId = CONFIG.get("synapse.oauth.client.id");
        String synapseEndpoint = CONFIG.get("synapse.endpoint");
        if (CONFIG.getEnvironment() == Environment.PRODUCTION) {
            oauthClientId = CONFIG.get("prod.synapse.oauth.client.id");
            synapseEndpoint = CONFIG.get("prod.synapse.endpoint");
        }
        String userEmail = CONFIG.get("synapse.test.user");
        String userPassword = CONFIG.get("synapse.test.user.password");
        String synapseUserId = CONFIG.get("synapse.test.user.id");
        
        // this is the admin user, so the account does not need to be created
        // Sign in to Synapse
        String payload = escapeJSON(format("{'username':'%s','password':'%s'}", userEmail, userPassword));
        HttpResponse response = Request.Post(synapseEndpoint + SYNAPSE_LOGIN_URL)
                .setHeader("content-type", "application/json")
                .body(new StringEntity(payload))
                .execute().returnResponse();
        
        String sessionToken = getValue(response, "sessionToken");

        // Consent to return OAuth authorization token
        payload = escapeJSON("{'clientId':'" + oauthClientId + "','scope':'openid','claims':{'id_token':{'userid':null}},"+
                "'responseType':'code','redirectUri':'https://research.sagebridge.org'}");
        response = Request.Post(synapseEndpoint + SYNAPSE_OAUTH_CONSENT)
                .setHeader("content-type", "application/json")
                .setHeader("sessiontoken", sessionToken)
                .body(new StringEntity(payload))
                .execute().returnResponse();
        String authToken = getValue(response, "access_code");
        
        // Call bridge to get a session
        OAuthAuthorizationToken token = new OAuthAuthorizationToken()
                .appId(TEST_APP_ID)
                .vendorId("synapse")
                .authToken(authToken)
                .callbackUrl("https://research.sagebridge.org");
        
        // These don't have to be valid credentials, as we're only going to use this client to sign in
        // via Synapse.
        ClientManager cm = new ClientManager.Builder().withSignIn(new SignIn().appId(TEST_APP_ID)
                .email(userEmail).password(userPassword)).build();
        AuthenticationApi authApi = cm.getClient(AuthenticationApi.class);

        UserSessionInfo session = authApi.signInWithOauthToken(token).execute().body();
        
        assertEquals(session.getId(), adminUserId);
        assertEquals(session.getSynapseUserId(), synapseUserId);
    }
    
    @Test
    public void signInWithSynapseAccountUsingRestUtils() throws Exception {
        String userEmail = CONFIG.get("synapse.test.user");
        String userPassword = CONFIG.get("synapse.test.user.password");
        String synapseUserId = CONFIG.get("synapse.test.user.id");
        SignIn signIn = new SignIn().appId(TEST_APP_ID).email(userEmail).password(userPassword);
        
        // These don't have to be valid credentials, as we're only going to use this client to sign in
        // via Synapse.
        ClientManager cm = new ClientManager.Builder().withSignIn(new SignIn().appId(TEST_APP_ID)
                .email(userEmail).password(userPassword)).build();
        AuthenticationApi authApi = cm.getClient(AuthenticationApi.class);
        
        UserSessionInfo session;
        if (CONFIG.getEnvironment() == Environment.PRODUCTION) {
            session = RestUtils.signInWithSynapse(authApi, signIn);
        } else {
            session = RestUtils.signInWithSynapseDev(authApi, signIn);
        }
        assertEquals(session.getId(), adminUserId);
        assertEquals(session.getSynapseUserId(), synapseUserId);
    }
    
    @Test
    public void synapseUserCanSwitchBetweenStudies() throws Exception {
        // Use the admin we know to be in two studies.
        admin.getClient(AuthenticationApi.class).changeApp(SHARED_SIGNIN).execute();
        App app = admin.getClient(AppsApi.class).getUsersApp().execute().body();
        assertEquals(app.getIdentifier(), SHARED_APP_ID);
        
        admin.getClient(AuthenticationApi.class).changeApp(API_SIGNIN).execute();
        app = admin.getClient(AppsApi.class).getUsersApp().execute().body();
        assertEquals(app.getIdentifier(), TEST_APP_ID);
    }
    
    @Test
    public void nonSynapseSignInCannotSwitchBetweenStudies() throws Exception {
        try {
            user = TestUserHelper.createAndSignInUser(OAuthTest.class, true);
            user.getClient(AuthenticationApi.class).changeApp(SHARED_SIGNIN).execute();
            fail("Should have throw exception");
        } catch(UnauthorizedException e) {
            assertEquals(e.getMessage(), "Account has not authenticated through Synapse.");
        }
    }
    
    private String getValue(HttpResponse response, String property) throws Exception {
        String responseBody = EntityUtils.toString(response.getEntity());
        JsonNode node = new ObjectMapper().readTree(responseBody);
        return node.get(property).textValue();
    }
}
