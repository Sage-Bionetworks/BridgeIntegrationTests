package org.sagebionetworks.bridge.sdk.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.sagebionetworks.bridge.sdk.integration.Tests.STUDY_ID_1;
import static org.sagebionetworks.bridge.util.IntegTestUtils.TEST_APP_ID;

import java.io.IOException;
import java.util.List;

import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.sagebionetworks.bridge.rest.api.ForConsentedUsersApi;
import org.sagebionetworks.bridge.rest.api.ParticipantsApi;
import org.sagebionetworks.bridge.rest.model.AccountStatus;
import org.sagebionetworks.bridge.rest.model.ConsentStatus;
import org.sagebionetworks.bridge.rest.model.EnrollmentInfo;
import org.sagebionetworks.bridge.rest.model.Role;
import org.sagebionetworks.bridge.rest.model.SharingScope;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.rest.model.UserSessionInfo;
import org.sagebionetworks.bridge.rest.model.Withdrawal;
import org.sagebionetworks.bridge.user.TestUser;
import org.sagebionetworks.bridge.user.TestUserHelper;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class SessionTest {
    
    private TestUser user;
    private TestUser researcher;
  
    @Before
    public void beforeMethod() throws IOException {
        user = TestUserHelper.createAndSignInUser(getClass(), true);
    }
    
    @After
    public void afterMethod() {
        if (user != null) {
            user.signOutAndDeleteUser();
        }
        if (researcher != null) {
            researcher.signOutAndDeleteUser();
        }
    }
    
    @Test
    public void verifySession() throws Exception {
        DateTime startOfTest = DateTime.now();

        UserSessionInfo session = user.getSession();
        assertNotNull(session.getId());
        assertEquals(SharingScope.NO_SHARING, session.getSharingScope());
        assertTrue(session.getCreatedOn().isAfter(startOfTest.minusHours(1)));
        assertEquals(AccountStatus.ENABLED, session.getStatus());
        assertEquals("en", session.getLanguages().get(0));
        assertEquals(1, session.getLanguages().size());
        assertTrue(session.isAuthenticated());
        assertNotNull(session.getSessionToken());
        assertNotNull(session.getEmail());
        assertEquals(user.getEmail(), session.getEmail());
        assertTrue(session.isConsented());

        ConsentStatus status = session.getConsentStatuses().get(TEST_APP_ID);
        assertEquals("Default Consent Group", status.getName());
        assertEquals(TEST_APP_ID, status.getSubpopulationGuid());
        assertTrue(status.isRequired());
        assertTrue(status.isConsented());
        assertTrue(status.isSignedMostRecentConsent());
        assertTrue(status.getSignedOn().isAfter(startOfTest.minusHours(1)));

        ForConsentedUsersApi usersApi = user.getClient(ForConsentedUsersApi.class);

        Withdrawal withdrawal = new Withdrawal().reason("No longer want to be a test subject");
        UserSessionInfo session2 = usersApi.withdrawConsentFromSubpopulation(TEST_APP_ID, withdrawal).execute().body();

        ConsentStatus status2 = session2.getConsentStatuses().get(TEST_APP_ID);
        assertEquals("Default Consent Group", status2.getName());
        assertEquals(TEST_APP_ID, status2.getSubpopulationGuid());
        assertTrue(status2.isRequired());
        assertFalse(status2.isConsented());
        assertFalse(status2.isSignedMostRecentConsent());
        assertNull(status2.getSignedOn());
        
        EnrollmentInfo info = session2.getEnrollments().get(STUDY_ID_1);
        assertFalse(info.isConsentRequired());
        assertTrue(info.getEnrolledOn() != null && info.getWithdrawnOn() != null && 
                info.getEnrolledOn().isBefore(info.getWithdrawnOn()));
        assertNull(info.isEnrolledBySelf());
        assertTrue(info.isWithdrawnBySelf()); // see above
    }
    
    @Test
    public void canGetStudyParticipantWithAllData() throws Exception {
        List<String> dataGroups = ImmutableList.of("group1");
        List<String> languages = ImmutableList.of("de");
        
        ForConsentedUsersApi usersApi = user.getClient(ForConsentedUsersApi.class);

        StudyParticipant updated = usersApi.getUsersParticipantRecord(false).execute().body();
        updated.setFirstName("TestFirstName");
        updated.setDataGroups(dataGroups);
        updated.setNotifyByEmail(false);
        updated.setStatus(AccountStatus.DISABLED);
        updated.setSharingScope(SharingScope.SPONSORS_AND_PARTNERS);
        updated.setLastName("TestLastName");
        updated.setLanguages(languages);
        
        usersApi.updateUsersParticipantRecord(updated).execute();
        
        StudyParticipant asUpdated = usersApi.getUsersParticipantRecord(false).execute().body();
        
        assertEquals("TestFirstName", asUpdated.getFirstName());
        assertEquals("TestLastName", asUpdated.getLastName());
        assertEquals(languages, asUpdated.getLanguages());
        assertEquals(ImmutableList.of("test_user", "group1"), asUpdated.getDataGroups());
        assertFalse(asUpdated.isNotifyByEmail());
        assertEquals(SharingScope.SPONSORS_AND_PARTNERS, asUpdated.getSharingScope());
        assertEquals(AccountStatus.ENABLED, asUpdated.getStatus());
    }

    @Test
    public void sessionUpdatedWhenResearcherUpdatesOwnAccount() throws Exception {
        researcher = TestUserHelper.createAndSignInUser(getClass(), false, Role.RESEARCHER);

        List<String> dataGroups = Lists.newArrayList("group1");
        List<String> languages = Lists.newArrayList("de", "fr");

        ParticipantsApi participantsApi = researcher.getClient(ParticipantsApi.class);

        StudyParticipant participant = participantsApi.getParticipantById(user.getUserId(), false).execute().body();
        participant.setFirstName("TestFirstName");
        participant.setLastName("TestLastName");
        participant.setLanguages(languages);
        participant.setDataGroups(dataGroups);
        participant.setNotifyByEmail(false);
        participant.setSharingScope(SharingScope.SPONSORS_AND_PARTNERS);

        participantsApi.updateParticipant(participant.getId(), participant).execute();
        
        user.signOut();
        user.signInAgain();
        UserSessionInfo session = user.getSession();
        
        // Session should be updated with this information. 
        assertTrue(session.isAuthenticated());
        assertEquals("TestFirstName", session.getFirstName());
        assertEquals("TestLastName", session.getLastName());
        assertEquals(languages, session.getLanguages());
        assertTrue(session.getDataGroups().containsAll(dataGroups));
        assertFalse(session.isNotifyByEmail());
        assertEquals(SharingScope.SPONSORS_AND_PARTNERS, session.getSharingScope());
        assertEquals(AccountStatus.ENABLED, session.getStatus());
    }
}
