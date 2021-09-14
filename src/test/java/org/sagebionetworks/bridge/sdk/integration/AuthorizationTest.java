package org.sagebionetworks.bridge.sdk.integration;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.sagebionetworks.bridge.rest.model.Role.DEVELOPER;
import static org.sagebionetworks.bridge.rest.model.Role.RESEARCHER;
import static org.sagebionetworks.bridge.rest.model.Role.STUDY_COORDINATOR;
import static org.sagebionetworks.bridge.rest.model.Role.STUDY_DESIGNER;
import static org.sagebionetworks.bridge.sdk.integration.Tests.STUDY_ID_1;
import static org.sagebionetworks.bridge.util.IntegTestUtils.SAGE_ID;

import java.util.concurrent.Callable;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.sagebionetworks.bridge.rest.api.ForOrgAdminsApi;
import org.sagebionetworks.bridge.rest.api.ParticipantsApi;
import org.sagebionetworks.bridge.rest.api.StudiesApi;
import org.sagebionetworks.bridge.rest.api.StudyParticipantsApi;
import org.sagebionetworks.bridge.rest.model.AccountSummaryList;
import org.sagebionetworks.bridge.rest.model.AccountSummarySearch;
import org.sagebionetworks.bridge.rest.model.Enrollment;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.user.TestUserHelper;
import org.sagebionetworks.bridge.user.TestUserHelper.TestUser;

import retrofit2.Call;

public class AuthorizationTest {

    private static TestUser developer;
    private static TestUser researcher;
    private static TestUser studyDesigner;
    private static TestUser studyCoordinator;
    
    private TestUser prodUser;
    private TestUser testUser;
    
    private String prodUserId;
    private String prodUserEmail;
    private String testUserId;
    private String testUserEmail;
    
    private StudyParticipant testParticipant;
    private StudyParticipant prodParticipant;

    @BeforeClass
    public static void beforeTests() throws Exception {
        developer = TestUserHelper.createAndSignInUser(AuthorizationTest.class, false, DEVELOPER);
        researcher = TestUserHelper.createAndSignInUser(AuthorizationTest.class, false, RESEARCHER);
        studyDesigner = TestUserHelper.createAndSignInUser(AuthorizationTest.class, false, STUDY_DESIGNER);
        studyCoordinator = TestUserHelper.createAndSignInUser(AuthorizationTest.class, false, STUDY_COORDINATOR);

        // remove the app-scoped accounts from an organization (at first) to verify they work
        // without organizational associations
        TestUser admin = TestUserHelper.getSignedInAdmin();
        ForOrgAdminsApi orgApi = admin.getClient(ForOrgAdminsApi.class);
        orgApi.removeMember(SAGE_ID, developer.getUserId()).execute();
        orgApi.removeMember(SAGE_ID, researcher.getUserId()).execute();
    }
    
    @Before
    public void before() throws Exception {
        prodUser = TestUserHelper.createAndSignInUser(AuthorizationTest.class, false);
        testUser = new TestUserHelper.Builder(AuthorizationTest.class).withTestDataGroup().createAndSignInUser();
        prodUserId = prodUser.getUserId();
        prodUserEmail = prodUser.getEmail();
        testUserId = testUser.getUserId();
        testUserEmail = testUser.getEmail();
    }
    
    @AfterClass
    public static void afterTests() throws Exception {
        if (developer != null) {
            developer.signOutAndDeleteUser();    
        }
        if (researcher != null) {
            researcher.signOutAndDeleteUser();    
        }
        if (studyDesigner != null) {
            studyDesigner.signOutAndDeleteUser();            
        }
        if (studyCoordinator != null) {
            studyCoordinator.signOutAndDeleteUser();    
        }
    }
    
    @After
    public void after( ) throws Exception {
        if (prodUser != null) {
            prodUser.signOutAndDeleteUser();    
        }
        if (testUser != null) {
            testUser.signOutAndDeleteUser();
        }
    }
    
    @Test
    public void getById() throws Exception {
        ParticipantsApi devPartApi = developer.getClient(ParticipantsApi.class);
        ParticipantsApi resPartApi = researcher.getClient(ParticipantsApi.class);
        StudyParticipantsApi desPartApi = studyDesigner.getClient(StudyParticipantsApi.class);
        StudyParticipantsApi coordPartApi = studyCoordinator.getClient(StudyParticipantsApi.class);
        TestUser admin = TestUserHelper.getSignedInAdmin();
        ForOrgAdminsApi orgApi = admin.getClient(ForOrgAdminsApi.class);
        StudiesApi coordStudiesApi = studyCoordinator.getClient(StudiesApi.class);

        // Developer can access test user, but not a production user
        shouldFail(() -> devPartApi.getParticipantById(prodUserId, false));
        devPartApi.getParticipantById(testUserId, false).execute();
        
        // Researchers can access both users
        resPartApi.getParticipantById(prodUserId, false).execute();
        resPartApi.getParticipantById(testUserId, false).execute();
        
        // Designers cannot access either of these users because they are not in a study
        shouldFail(() -> desPartApi.getStudyParticipantById(STUDY_ID_1, prodUserId, false));
        shouldFail(() -> desPartApi.getStudyParticipantById(STUDY_ID_1, testUserId, false));
        
        // Coordinators also cannot access these users because they are not in a study
        shouldFail(() -> coordPartApi.getStudyParticipantById(STUDY_ID_1, prodUserId, false));
        shouldFail(() -> coordPartApi.getStudyParticipantById(STUDY_ID_1, testUserId, false));
        
        // PUT STUDY-SCOPED ADMINS INTO ORG
        orgApi.addMember(SAGE_ID, developer.getUserId()).execute();
        orgApi.addMember(SAGE_ID, researcher.getUserId()).execute();
        
        // these don't change
        shouldFail(() -> devPartApi.getParticipantById(prodUserId, false));
        devPartApi.getParticipantById(testUserId, false).execute();
        resPartApi.getParticipantById(prodUserId, false).execute();
        resPartApi.getParticipantById(testUserId, false).execute();
        
        // ENROLL USERS IN STUDY 1
        coordStudiesApi.enrollParticipant(STUDY_ID_1, new Enrollment().userId(prodUserId)).execute();
        coordStudiesApi.enrollParticipant(STUDY_ID_1, new Enrollment().userId(testUserId)).execute();
        
        // developer is the same
        shouldFail(() -> devPartApi.getParticipantById(prodUserId, false));
        devPartApi.getParticipantById(testUserId, false).execute();

        // researcher is the same
        resPartApi.getParticipantById(prodUserId, false).execute();
        resPartApi.getParticipantById(testUserId, false).execute();
        
        // designer can see the test account now that it is enrolled in accessible study
        shouldFail(() -> desPartApi.getStudyParticipantById(STUDY_ID_1, prodUserId, false));
        desPartApi.getStudyParticipantById(STUDY_ID_1, testUserId, false).execute();
        
        // coordinator can see both now that they are enrolled in an accessible study
        coordPartApi.getStudyParticipantById(STUDY_ID_1, prodUserId, false).execute();
        coordPartApi.getStudyParticipantById(STUDY_ID_1, testUserId, false).execute();
    }
    
    @Test
    public void getAccountSummaries() throws Exception {
        // Note that in these tests, we do not have to explicitly set the test_user flag in search.
        ParticipantsApi devPartApi = developer.getClient(ParticipantsApi.class);
        ParticipantsApi resPartApi = researcher.getClient(ParticipantsApi.class);
        StudyParticipantsApi desPartApi = studyDesigner.getClient(StudyParticipantsApi.class);
        StudyParticipantsApi coordPartApi = studyCoordinator.getClient(StudyParticipantsApi.class);
        StudiesApi coordStudiesApi = studyCoordinator.getClient(StudiesApi.class);

        // Developer can access test user, but not a production user
        shouldInclude(() -> devPartApi.searchAccountSummaries(search(testUserEmail)) );
        shouldExclude(() -> devPartApi.searchAccountSummaries(search(prodUserEmail)) );

        // Researcher can access both, but not a production user
        shouldInclude(() -> resPartApi.searchAccountSummaries(search(testUserEmail)) );
        shouldInclude(() -> resPartApi.searchAccountSummaries(search(prodUserEmail)) );
        
        // Designers cannot access either of these users because they are not in a study
        shouldExclude(() -> desPartApi.getStudyParticipants(STUDY_ID_1, search(prodUserEmail)));
        shouldExclude(() -> desPartApi.getStudyParticipants(STUDY_ID_1, search(testUserEmail)));
        
        // Coordinators also cannot access these users because they are not in a study
        shouldExclude(() -> desPartApi.getStudyParticipants(STUDY_ID_1, search(prodUserEmail)));
        shouldExclude(() -> desPartApi.getStudyParticipants(STUDY_ID_1, search(testUserEmail)));

        // ENROLL USERS IN STUDY 1
        coordStudiesApi.enrollParticipant(STUDY_ID_1, new Enrollment().userId(prodUserId)).execute();
        coordStudiesApi.enrollParticipant(STUDY_ID_1, new Enrollment().userId(testUserId)).execute();
        
        // Developer is the same
        shouldInclude(() -> devPartApi.searchAccountSummaries(search(testUserEmail)) );
        shouldExclude(() -> devPartApi.searchAccountSummaries(search(prodUserEmail)) );

        // Researcher is the same
        shouldInclude(() -> resPartApi.searchAccountSummaries(search(testUserEmail)) );
        shouldInclude(() -> resPartApi.searchAccountSummaries(search(prodUserEmail)) );

        // Study designers can now access test users
        shouldExclude(() -> desPartApi.getStudyParticipants(STUDY_ID_1, search(prodUserEmail)));
        shouldInclude(() -> desPartApi.getStudyParticipants(STUDY_ID_1, search(testUserEmail)));
        
        // Study coordinators can now access all study users
        shouldInclude(() -> coordPartApi.getStudyParticipants(STUDY_ID_1, search(prodUserEmail)));
        shouldInclude(() -> coordPartApi.getStudyParticipants(STUDY_ID_1, search(testUserEmail)));
    }
    
    @Test
    public void updateStudyParticipant() throws Exception {
        ParticipantsApi devPartApi = developer.getClient(ParticipantsApi.class);
        ParticipantsApi resPartApi = researcher.getClient(ParticipantsApi.class);
        StudyParticipantsApi desPartApi = studyDesigner.getClient(StudyParticipantsApi.class);
        StudyParticipantsApi coordPartApi = studyCoordinator.getClient(StudyParticipantsApi.class);
        StudiesApi coordStudiesApi = studyCoordinator.getClient(StudiesApi.class);

        StudyParticipant testParticipant = resPartApi.getParticipantById(testUserId, false).execute().body();
        StudyParticipant prodParticipant = resPartApi.getParticipantById(prodUserId, false).execute().body();
     
        // Developers can only update test accounts.
        shouldFail(() -> devPartApi.updateParticipant(prodUserId, prodParticipant));
        devPartApi.updateParticipant(testUserId, testParticipant).execute();

        // Researchers can update both kinds of accounts
        resPartApi.updateParticipant(prodUserId, prodParticipant);
        resPartApi.updateParticipant(testUserId, testParticipant);
        
        // As other tests, study coordinators and designers can't do anything with these
        // accounts because they are not in an accessible study
        shouldFail(() -> desPartApi.updateStudyParticipant(STUDY_ID_1, prodUserId, prodParticipant));
        shouldFail(() -> desPartApi.updateStudyParticipant(STUDY_ID_1, testUserId, testParticipant));

        shouldFail(() -> coordPartApi.updateStudyParticipant(STUDY_ID_1, prodUserId, prodParticipant));
        shouldFail(() -> coordPartApi.updateStudyParticipant(STUDY_ID_1, testUserId, testParticipant));
        
        // ENROLL USERS IN STUDY 1
        coordStudiesApi.enrollParticipant(STUDY_ID_1, new Enrollment().userId(prodUserId)).execute();
        coordStudiesApi.enrollParticipant(STUDY_ID_1, new Enrollment().userId(testUserId)).execute();

        // Devs and researchers don't change
        shouldFail(() -> devPartApi.updateParticipant(prodUserId, prodParticipant));
        devPartApi.updateParticipant(testUserId, testParticipant).execute();
        resPartApi.updateParticipant(prodUserId, prodParticipant);
        resPartApi.updateParticipant(testUserId, testParticipant);
        
        // Study designers can work with test accounts
        shouldFail(() -> desPartApi.updateStudyParticipant(STUDY_ID_1, prodUserId, prodParticipant));
        desPartApi.updateStudyParticipant(STUDY_ID_1, testUserId, testParticipant).execute();
        
        // Study coordinators can work with both
        coordPartApi.updateStudyParticipant(STUDY_ID_1, prodUserId, prodParticipant).execute();
        coordPartApi.updateStudyParticipant(STUDY_ID_1, testUserId, testParticipant).execute();
    }
    
//    private void updateStudyParticipantRecords() {
//        ParticipantsApi resPartApi = researcher.getClient(ParticipantsApi.class);
//        testParticipant = resPartApi.getParticipantById(testUserId, false).execute().body();
//        prodParticipant = resPartApi.getParticipantById(prodUserId, false).execute().body();
//    }
    
    private void shouldFail(Callable<Call<?>> callable) {
        try {
            callable.call().execute();
            fail("Should have thrown exception");
        } catch(Exception e) {
        }
    }
    
    private void shouldInclude(Callable<Call<AccountSummaryList>> callable) {
        try {
            AccountSummaryList list = callable.call().execute().body();
            assertFalse(list.getItems().isEmpty());
        } catch(Exception e) {
            fail("Threw exception");
        }
    }
    
    private void shouldExclude(Callable<Call<AccountSummaryList>> callable) {
        try {
            AccountSummaryList list = callable.call().execute().body();
            assertTrue(list.getItems().isEmpty());
        } catch(Exception e) {
            fail("Threw exception");
        }
    }
    
    private AccountSummarySearch search(String email) {
        return new AccountSummarySearch().emailFilter(email);
    }
}
