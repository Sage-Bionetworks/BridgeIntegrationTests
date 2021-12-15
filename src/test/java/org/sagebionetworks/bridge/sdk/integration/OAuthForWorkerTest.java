package org.sagebionetworks.bridge.sdk.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.sagebionetworks.bridge.rest.model.Role.WORKER;
import static org.sagebionetworks.bridge.util.IntegTestUtils.CONFIG;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.sagebionetworks.bridge.rest.api.ForSuperadminsApi;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.exceptions.EntityNotFoundException;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.ForwardCursorStringList;
import org.sagebionetworks.bridge.rest.model.OAuthProvider;
import org.sagebionetworks.bridge.rest.model.SignUp;
import org.sagebionetworks.bridge.rest.model.VersionHolder;
import org.sagebionetworks.bridge.user.TestUserHelper;
import org.sagebionetworks.bridge.user.TestUserHelper.TestUser;

import com.google.common.collect.ImmutableList;

@Ignore // For this test to pass, we'll need a second Synapse ID that can be assigned to the worker. 
public class OAuthForWorkerTest {
    
    private TestUser admin;
    private TestUser worker;

    @Before
    public void before() throws Exception {
        admin = TestUserHelper.getSignedInAdmin();
    }
    
    @After
    public void after() throws Exception {
        // Force admin back to the API test
        admin.signOut();
        if (worker != null) {
            worker.signOutAndDeleteUser();
        }
    }

    @Test
    public void test() throws Exception {
        String synapseUserId = CONFIG.get("synapse.test.user.id");
        worker = TestUserHelper.createAndSignInUser(OAuthTest.class, true, 
                new SignUp().roles(ImmutableList.of(WORKER)).synapseUserId(synapseUserId));
        
        ForWorkersApi workersApi = worker.getClient(ForWorkersApi.class);
        
        try {
            workersApi.getHealthCodesGrantingOAuthAccess(worker.getAppId(), "unused-vendor-id", null, null).execute().body();
            fail("Should have thrown exception.");
        } catch(EntityNotFoundException e) {
            assertEquals("OAuthProvider not found.", e.getMessage());
        }
        try {
            workersApi.getOAuthAccessToken(worker.getAppId(), "unused-vendor-id", "ABC-DEF-GHI").execute().body();
            fail("Should have thrown exception.");
        } catch(EntityNotFoundException e) {
            assertEquals("OAuthProvider not found.", e.getMessage());
        }
        ForSuperadminsApi superadminApi = admin.getClient(ForSuperadminsApi.class);
        App app = superadminApi.getApp(worker.getAppId()).execute().body();
        try {
            OAuthProvider provider = new OAuthProvider().clientId("foo").endpoint("https://webservices.sagebridge.org/")
                    .callbackUrl("https://webservices.sagebridge.org/").secret("secret");
            
            app.getOAuthProviders().put("bridge", provider);
            VersionHolder version = superadminApi.updateApp(app.getIdentifier(), app).execute().body();
            app.setVersion(version.getVersion()); 
            
            ForwardCursorStringList list = workersApi.getHealthCodesGrantingOAuthAccess(worker.getAppId(), "bridge", null, null).execute().body();
            assertTrue(list.getItems().isEmpty());
            try {
                workersApi.getOAuthAccessToken(worker.getAppId(), "bridge", "ABC-DEF-GHI").execute();
                fail("Should have thrown an exception");
            } catch(EntityNotFoundException e) {
                
            }
        } finally {
            app.getOAuthProviders().remove("bridge");
            superadminApi.updateApp(app.getIdentifier(), app).execute();
        }
    }

}
