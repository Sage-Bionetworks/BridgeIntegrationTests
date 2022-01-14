package org.sagebionetworks.bridge.sdk.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.sagebionetworks.bridge.rest.model.SharingScope.NO_SHARING;
import static org.sagebionetworks.bridge.sdk.integration.Tests.API_2_SIGNIN;
import static org.sagebionetworks.bridge.sdk.integration.Tests.API_SIGNIN;
import static org.sagebionetworks.bridge.sdk.integration.Tests.PASSWORD;
import static org.sagebionetworks.bridge.util.IntegTestUtils.SAGE_ID;
import static org.sagebionetworks.bridge.util.IntegTestUtils.TEST_APP_2_ID;
import static org.sagebionetworks.bridge.util.IntegTestUtils.TEST_APP_ID;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Test;
import org.sagebionetworks.bridge.rest.api.AuthenticationApi;
import org.sagebionetworks.bridge.rest.api.ForAdminsApi;
import org.sagebionetworks.bridge.rest.api.ForSuperadminsApi;
import org.sagebionetworks.bridge.rest.api.OrganizationsApi;
import org.sagebionetworks.bridge.rest.api.ParticipantsApi;
import org.sagebionetworks.bridge.rest.api.StudiesApi;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.Organization;
import org.sagebionetworks.bridge.rest.model.SignIn;
import org.sagebionetworks.bridge.rest.model.SignUp;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.StudyList;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.user.TestUser;
import org.sagebionetworks.bridge.user.TestUserHelper;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public class SignUpForWorkerTest {
    
    @Test
    public void signUpWithExternalIdAndNoAccountSucceeds() throws Exception {
        TestUser admin = TestUserHelper.getSignedInAdmin(true);
        ForAdminsApi adminsApi = admin.getClient(ForAdminsApi.class);
        StudiesApi studiesApi = admin.getClient(StudiesApi.class);
        OrganizationsApi orgsApi = admin.getClient(OrganizationsApi.class);
        ParticipantsApi participantsApi = admin.getClient(ParticipantsApi.class);
        ForSuperadminsApi superadminApi = admin.getClient(ForSuperadminsApi.class);
        AuthenticationApi authApi = admin.getClient(AuthenticationApi.class);

        String extId = Tests.randomIdentifier(getClass());

        // In the API app we have two studies created by the initializer for integration tests,
        // and neither is "test", so one will be chosen at random.
        
        SignUp signUp = new SignUp().appId(TEST_APP_ID).dataGroups(ImmutableList.of("test_user"))
                .password(PASSWORD).externalId(extId).sharingScope(NO_SHARING);
        StudyParticipant participant = null;
        
        try {
            authApi.signUp(signUp).execute();
            participant = participantsApi.getParticipantByExternalId(extId, false).execute().body();
            assertEquals(participant.getExternalIds().size(), 1);
            Map.Entry<String, String> reg = Iterables.getFirst(participant.getExternalIds().entrySet(), null);
            assertEquals(extId, reg.getValue());

            // Retrieving all Studies related to the API app.
            StudyList testApiStudies = adminsApi.getStudies(0, 50, false).execute().body();
            Set<String> possibleStudyIds = testApiStudies.getItems().stream()
                    .map(Study::getIdentifier)
                    .collect(Collectors.toSet());

            assertTrue(possibleStudyIds.contains(reg.getKey()));
        } finally {
            if (participant != null) {
                adminsApi.deleteUser(participant.getId()).execute();  
            }
        }
        
        // In api 2 there is only one study, and so that is the one that is used.
        signUp = new SignUp().appId(TEST_APP_2_ID).dataGroups(ImmutableList.of("test_user"))
                .password(PASSWORD).externalId(extId).sharingScope(NO_SHARING);
        try {
            authApi.signUp(signUp).execute();
            
            authApi.changeApp(API_2_SIGNIN).execute();
            participant = participantsApi.getParticipantByExternalId(extId, false).execute().body();
            assertEquals(1, participant.getExternalIds().size());
            // ... however that study is named differently in different environments.
            String extIdValue = participant.getExternalIds().get("api-2-study");
            if (extIdValue == null) {
                extIdValue = participant.getExternalIds().get("api-2");
            }
            assertEquals(extId, extIdValue);
            adminsApi.deleteUser(participant.getId()).execute();
        } finally {
            if (participant != null) {
                authApi.changeApp(API_SIGNIN).execute();    
            }
        }
        
        // Create an app with a test study only, and it will select that.
        App app = Tests.getApp(Tests.randomIdentifier(getClass()), null);
        superadminApi.createApp(app).execute();
        
        authApi.changeApp(new SignIn().appId(app.getIdentifier())).execute();
        
        // The superadmin is in the organization Sage Bionetworks, so that needs to exist in
        // this new app when we create a study.
        Organization org = new Organization().identifier(SAGE_ID).name("Sage Bionetworks");
        orgsApi.createOrganization(org).execute();
        
        Study study1 = new Study().identifier("test").name("Test");
        studiesApi.createStudy(study1).execute();
        
        // we need to remove the default study that is created with the app.
        studiesApi.deleteStudy(app.getIdentifier() + "-study", true).execute();

        signUp = new SignUp().appId(app.getIdentifier()).dataGroups(ImmutableList.of("test_user"))
                .password(PASSWORD).externalId(extId).sharingScope(NO_SHARING);
        try {
            authApi.signUp(signUp).execute();
            participant = participantsApi.getParticipantByExternalId(extId, false).execute().body();
            assertEquals(1, participant.getExternalIds().size());
            assertEquals(extId, participant.getExternalIds().get("test"));
        } finally {
            adminsApi.deleteUser(participant.getId()).execute();
        }
        
        // One last scenario: add in another study, and now test should not be chosen
        String study2Id = Tests.randomIdentifier(getClass());
        Study study2 = new Study().identifier(study2Id).name("Second Study");
        studiesApi.createStudy(study2).execute();
        
        signUp = new SignUp().appId(app.getIdentifier()).dataGroups(ImmutableList.of("test_user"))
                .password(PASSWORD).externalId(extId).sharingScope(NO_SHARING);
        try {
            authApi.signUp(signUp).execute();
            participant = participantsApi.getParticipantByExternalId(extId, false).execute().body();
            assertEquals(1, participant.getExternalIds().size());
            assertEquals(extId, participant.getExternalIds().get(study2Id));
        } finally {
            adminsApi.deleteUser(participant.getId()).execute();
            studiesApi.deleteStudy(study2Id, true).execute();
            studiesApi.deleteStudy("test", true).execute();
            authApi.changeApp(API_SIGNIN).execute();
            superadminApi.deleteApp(app.getIdentifier(), true).execute();
        }
    }
}
