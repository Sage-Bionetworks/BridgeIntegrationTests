package org.sagebionetworks.bridge.sdk.integration;

import static java.lang.String.format;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.sagebionetworks.bridge.rest.model.Role.DEVELOPER;
import static org.sagebionetworks.bridge.rest.model.Role.WORKER;
import static org.sagebionetworks.bridge.sdk.integration.Tests.API_SIGNIN;
import static org.sagebionetworks.bridge.sdk.integration.Tests.SHARED_SIGNIN;
import static org.sagebionetworks.bridge.sdk.integration.Tests.escapeJSON;
import static org.sagebionetworks.bridge.util.IntegTestUtils.SHARED_STUDY_ID;
import static org.sagebionetworks.bridge.util.IntegTestUtils.STUDY_ID;

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
import org.junit.Test;

import org.sagebionetworks.bridge.rest.Config;
import org.sagebionetworks.bridge.rest.RestUtils;
import org.sagebionetworks.bridge.rest.api.AuthenticationApi;
import org.sagebionetworks.bridge.rest.api.ForConsentedUsersApi;
import org.sagebionetworks.bridge.rest.api.ForSuperadminsApi;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.api.StudiesApi;
import org.sagebionetworks.bridge.rest.exceptions.EntityNotFoundException;
import org.sagebionetworks.bridge.rest.model.ForwardCursorStringList;
import org.sagebionetworks.bridge.rest.model.OAuthAuthorizationToken;
import org.sagebionetworks.bridge.rest.model.OAuthProvider;
import org.sagebionetworks.bridge.rest.model.SignIn;
import org.sagebionetworks.bridge.rest.model.SignUp;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.StudyList;
import org.sagebionetworks.bridge.rest.model.UserSessionInfo;
import org.sagebionetworks.bridge.rest.model.VersionHolder;
import org.sagebionetworks.bridge.user.TestUserHelper;
import org.sagebionetworks.bridge.user.TestUserHelper.TestUser;

public class OAuthTest {
    private static final String SYNAPSE_LOGIN_URL = "https://repo-prod.prod.sagebase.org/auth/v1/login";
    private static final String SYNAPSE_OAUTH_CONSENT = "https://repo-prod.prod.sagebase.org/auth/v1/oauth2/consent";

    private TestUser admin;
    private TestUser user;
    private TestUser user2;
    private TestUser worker;
    
    @Before
    public void before() throws Exception {
        admin = TestUserHelper.getSignedInAdmin();
    }
    
    @After
    public void after() throws Exception {
        // Force admin back to the API test
        admin.signOut();
        if (user != null) {
            user.signOutAndDeleteUser();
        }
        if (worker != null) {
            worker.signOutAndDeleteUser();
        }
        if (user2 != null) {
            user2.signOutAndDeleteUser();
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
    public void test() throws Exception {
        String synapseUserId = admin.getConfig().get("synapse.test.user.id");
        worker = TestUserHelper.createAndSignInUser(OAuthTest.class, true, 
                new SignUp().roles(ImmutableList.of(WORKER)).synapseUserId(synapseUserId));
        
        ForWorkersApi workersApi = worker.getClient(ForWorkersApi.class);
        
        try {
            workersApi.getHealthCodesGrantingOAuthAccess(worker.getStudyId(), "unused-vendor-id", null, null).execute().body();
            fail("Should have thrown exception.");
        } catch(EntityNotFoundException e) {
            assertEquals("OAuthProvider not found.", e.getMessage());
        }
        try {
            workersApi.getOAuthAccessToken(worker.getStudyId(), "unused-vendor-id", "ABC-DEF-GHI").execute().body();
            fail("Should have thrown exception.");
        } catch(EntityNotFoundException e) {
            assertEquals("OAuthProvider not found.", e.getMessage());
        }
        ForSuperadminsApi superadminApi = admin.getClient(ForSuperadminsApi.class);
        Study study = superadminApi.getStudy(worker.getStudyId()).execute().body();
        try {
            OAuthProvider provider = new OAuthProvider().clientId("foo").endpoint("https://webservices.sagebridge.org/")
                    .callbackUrl("https://webservices.sagebridge.org/").secret("secret");
            
            study.getOAuthProviders().put("bridge", provider);
            VersionHolder version = superadminApi.updateStudy(study.getIdentifier(), study).execute().body();
            study.setVersion(version.getVersion()); 
            
            ForwardCursorStringList list = workersApi.getHealthCodesGrantingOAuthAccess(worker.getStudyId(), "bridge", null, null).execute().body();
            assertTrue(list.getItems().isEmpty());
            try {
                workersApi.getOAuthAccessToken(worker.getStudyId(), "bridge", "ABC-DEF-GHI").execute();
                fail("Should have thrown an exception");
            } catch(EntityNotFoundException e) {
                
            }
        } finally {
            study.getOAuthProviders().remove("bridge");
            superadminApi.updateStudy(study.getIdentifier(), study).execute();
        }
    }
    
    @Test
    public void signInWithSynapseAccount() throws Exception {
        String synapseUserId = admin.getConfig().get("synapse.test.user.id");
        worker = TestUserHelper.createAndSignInUser(OAuthTest.class, true, 
                new SignUp().roles(ImmutableList.of(WORKER)).synapseUserId(synapseUserId));
        
        Config config = worker.getConfig();
        String userEmail = config.get("synapse.test.user");
        String userPassword = config.get("synapse.test.user.password");
        worker.signOut();

        // Sign in to Synapse
        String payload = escapeJSON(format("{'username':'%s','password':'%s'}", userEmail, userPassword));
        HttpResponse response = Request.Post(SYNAPSE_LOGIN_URL)
                .setHeader("content-type", "application/json")
                .body(new StringEntity(payload))
                .execute().returnResponse();
        
        String sessionToken = getValue(response, "sessionToken");

        // Consent to return OAuth authorization token
        payload = escapeJSON("{'clientId':'100020','scope':'openid','claims':'{\\\"id_token\\\":{\\\"userid\\\":null}}',"+
                "'responseType':'code','redirectUri':'https://research-staging.sagebridge.org'}");
        response = Request.Post(SYNAPSE_OAUTH_CONSENT)
                .setHeader("content-type", "application/json")
                .setHeader("sessiontoken", sessionToken)
                .body(new StringEntity(payload))
                .execute().returnResponse();
        String authToken = getValue(response, "access_code");
        
        // Call bridge to get a session
        OAuthAuthorizationToken token = new OAuthAuthorizationToken()
                .study(STUDY_ID)
                .vendorId("synapse")
                .authToken(authToken)
                .callbackUrl("https://research-staging.sagebridge.org");
        
        AuthenticationApi authApi = worker.getClient(AuthenticationApi.class);
        UserSessionInfo session = authApi.signInWithOauthToken(token).execute().body();
        
        assertEquals(session.getId(), worker.getSession().getId());
        assertEquals(session.getSynapseUserId(), worker.getSession().getSynapseUserId());
    }
    
    @Test
    public void signInWithSynapseAccountUsingRestUtils() throws Exception {
        String synapseUserId = admin.getConfig().get("synapse.test.user.id");
        worker = TestUserHelper.createAndSignInUser(OAuthTest.class, true, 
                new SignUp().roles(ImmutableList.of(WORKER)).synapseUserId(synapseUserId));
        
        Config config = worker.getConfig();
        String userEmail = config.get("synapse.test.user");
        String userPassword = config.get("synapse.test.user.password");
        worker.signOut();

        SignIn signIn = new SignIn().study(STUDY_ID).email(userEmail).password(userPassword);
        AuthenticationApi authApi = worker.getClient(AuthenticationApi.class);
        
        UserSessionInfo session = RestUtils.signInWithSynapse(authApi, signIn);
        
        assertEquals(session.getId(), worker.getSession().getId());
        assertEquals(session.getSynapseUserId(), worker.getSession().getSynapseUserId());
    }
    
    @Test
    public void synapseUserCanSwitchBetweenStudies() throws Exception {
        // Going to use the shared study as well as the API study for this test.
        String synapseUserId = admin.getConfig().get("synapse.test.user.id");
        
        user = new TestUserHelper.Builder(OAuthTest.class).withStudyId(STUDY_ID)
                .withSignUp(new SignUp().study(STUDY_ID)
                .roles(ImmutableList.of(DEVELOPER)).synapseUserId(synapseUserId))
                .createAndSignInUser();
        String userId = user.getUserId();

        admin.getClient(ForSuperadminsApi.class).adminChangeStudy(SHARED_SIGNIN).execute();
        
        user2 = new TestUserHelper.Builder(OAuthTest.class).withStudyId(SHARED_STUDY_ID)
                .withSignUp(new SignUp().study(SHARED_STUDY_ID)
                .roles(ImmutableList.of(DEVELOPER)).synapseUserId(synapseUserId))
                .createAndSignInUser();
        String user2Id = user2.getUserId();

        StudiesApi studiesApi = user2.getClient(StudiesApi.class);
        StudyList list = studiesApi.getStudyMemberships().execute().body();
        assertEquals(2, list.getItems().size());
        
        Set<String> studyIds = list.getItems().stream().map(el -> el.getIdentifier()).collect(toSet());
        assertEquals(ImmutableSet.of(STUDY_ID, SHARED_STUDY_ID), studyIds);
        
        UserSessionInfo info = studiesApi.changeStudy(SHARED_SIGNIN).execute().body();
        assertEquals(user2Id, info.getId());
        
        info = studiesApi.changeStudy(API_SIGNIN).execute().body();
        assertEquals(userId, info.getId());
        
        // Before the admin switches back to the API study, delete this user in the shared study
        user2.signOutAndDeleteUser();
        
        // Verify this has an immediate effect on the other user
        list = user.getClient(StudiesApi.class).getStudyMemberships().execute().body();
        assertEquals(list.getItems().size(), 1);
    }
    
    private String getValue(HttpResponse response, String property) throws Exception {
        String responseBody = EntityUtils.toString(response.getEntity());
        JsonNode node = new ObjectMapper().readTree(responseBody);
        return node.get(property).textValue();
    }
}
