package org.sagebionetworks.bridge.sdk.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.sagebionetworks.bridge.rest.model.AccountStatus.ENABLED;
import static org.sagebionetworks.bridge.rest.model.Role.DEVELOPER;
import static org.sagebionetworks.bridge.rest.model.Role.ORG_ADMIN;
import static org.sagebionetworks.bridge.rest.model.Role.STUDY_COORDINATOR;
import static org.sagebionetworks.bridge.sdk.integration.Tests.PASSWORD;
import static org.sagebionetworks.bridge.sdk.integration.Tests.PHONE;
import static org.sagebionetworks.bridge.sdk.integration.Tests.STUDY_ID_1;
import static org.sagebionetworks.bridge.sdk.integration.Tests.SYNAPSE_USER_ID;
import static org.sagebionetworks.bridge.util.IntegTestUtils.SAGE_ID;
import static org.sagebionetworks.bridge.util.IntegTestUtils.TEST_APP_ID;

import java.io.IOException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.sagebionetworks.bridge.rest.RestUtils;
import org.sagebionetworks.bridge.rest.api.AccountsApi;
import org.sagebionetworks.bridge.rest.api.ForAdminsApi;
import org.sagebionetworks.bridge.rest.api.ForOrgAdminsApi;
import org.sagebionetworks.bridge.rest.api.ForStudyCoordinatorsApi;
import org.sagebionetworks.bridge.rest.api.ForSuperadminsApi;
import org.sagebionetworks.bridge.rest.api.OrganizationsApi;
import org.sagebionetworks.bridge.rest.api.ParticipantsApi;
import org.sagebionetworks.bridge.rest.api.StudiesApi;
import org.sagebionetworks.bridge.rest.api.StudyParticipantsApi;
import org.sagebionetworks.bridge.rest.api.ForConsentedUsersApi;
import org.sagebionetworks.bridge.rest.exceptions.ConsentRequiredException;
import org.sagebionetworks.bridge.rest.exceptions.EntityNotFoundException;
import org.sagebionetworks.bridge.rest.exceptions.UnauthorizedException;
import org.sagebionetworks.bridge.rest.model.Account;
import org.sagebionetworks.bridge.rest.model.AccountSummary;
import org.sagebionetworks.bridge.rest.model.AccountSummaryList;
import org.sagebionetworks.bridge.rest.model.AccountSummarySearch;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.IdentifierHolder;
import org.sagebionetworks.bridge.rest.model.IdentifierUpdate;
import org.sagebionetworks.bridge.rest.model.Phone;
import org.sagebionetworks.bridge.rest.model.RequestInfo;
import org.sagebionetworks.bridge.rest.model.SignIn;
import org.sagebionetworks.bridge.rest.model.SignUp;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.rest.model.UserSessionInfo;
import org.sagebionetworks.bridge.rest.model.VersionHolder;
import org.sagebionetworks.bridge.user.TestUser;
import org.sagebionetworks.bridge.user.TestUserHelper;
import org.sagebionetworks.bridge.util.IntegTestUtils;

public class AccountsTest {
    private static final ImmutableList<String> USER_DATA_GROUPS = ImmutableList.of("test_user", "sdk-int-1");
    private TestUser admin;
    private TestUser developer;
    private TestUser studyCoordinator;
    private TestUser orgAdmin;
    private String orgId;
    private String phoneUserId;
    private String emailUserId;
    private String studyId;
    private ForOrgAdminsApi orgAdminApi;

    @Before
    public void before() throws Exception {
        admin = TestUserHelper.getSignedInAdmin();
        developer = TestUserHelper.createAndSignInUser(AccountsTest.class, false, DEVELOPER);
        studyCoordinator = TestUserHelper.createAndSignInUser(AccountsTest.class, false, STUDY_COORDINATOR);
        orgAdmin = TestUserHelper.createAndSignInUser(AccountsTest.class, true, ORG_ADMIN);
        orgAdminApi = orgAdmin.getClient(ForOrgAdminsApi.class);
        orgId = orgAdmin.getSession().getOrgMembership();

        IntegTestUtils.deletePhoneUser();

        ForSuperadminsApi superadminApi = admin.getClient(ForSuperadminsApi.class);
        App app = superadminApi.getApp(TEST_APP_ID).execute().body();
        if (!app.isPhoneSignInEnabled() || !app.isEmailSignInEnabled()) {
            app.setPhoneSignInEnabled(true);
            app.setEmailSignInEnabled(true);
            VersionHolder keys = superadminApi.updateApp(app.getIdentifier(), app).execute().body();
            app.version(keys.getVersion());
        }
    }

    @After
    public void after() throws Exception {
        if (developer != null) {
            developer.signOutAndDeleteUser();
        }
        if (studyCoordinator != null) {
            studyCoordinator.signOutAndDeleteUser();
        }
        if (orgAdmin != null) {
            orgAdmin.signOutAndDeleteUser();
        }
        if (phoneUserId != null) {
            admin.getClient(ForAdminsApi.class).deleteUser(phoneUserId).execute();
        }
        if (emailUserId != null) {
            admin.getClient(ForAdminsApi.class).deleteUser(emailUserId).execute();
        }
        if (studyId != null) {
            admin.getClient(ForAdminsApi.class).deleteStudy(studyId, true).execute();
        }
    }
    
    @Test
    public void crudAccount() throws Exception {
        String email = IntegTestUtils.makeEmail(AccountsTest.class);
        Account account = new Account()
                .firstName("firstName")
                .lastName("lastName")
                .synapseUserId(SYNAPSE_USER_ID)
                .email(email)
                .phone(PHONE)
                .attributes(ImmutableMap.of("can_be_recontacted", "true"))
                .roles(ImmutableList.of(STUDY_COORDINATOR))
                .dataGroups(ImmutableList.of("test_user", "sdk-int-1"))
                .clientData("Test")
                .languages(ImmutableList.of("en", "fr"))
                .password(PASSWORD)
                .note("test note 1")
                .clientTimeZone("America/Los_Angeles");
        
        emailUserId = orgAdminApi.createAccount(account).execute().body().getIdentifier();
        
        Account retrieved = orgAdminApi.getAccount(emailUserId).execute().body();
        assertEquals("firstName", retrieved.getFirstName());
        assertEquals("lastName", retrieved.getLastName());
        assertEquals(SYNAPSE_USER_ID, retrieved.getSynapseUserId());
        assertEquals(email, retrieved.getEmail());
        assertEquals(PHONE.getNumber(), retrieved.getPhone().getNumber());
        assertEquals(PHONE.getRegionCode(), retrieved.getPhone().getRegionCode());
        assertEquals("true", retrieved.getAttributes().get("can_be_recontacted"));
        assertEquals(ENABLED, retrieved.getStatus());
        assertEquals(ImmutableList.of(STUDY_COORDINATOR), retrieved.getRoles());
        assertEquals(USER_DATA_GROUPS, retrieved.getDataGroups());
        assertEquals("Test", RestUtils.toType(retrieved.getClientData(), String.class));
        assertEquals(ImmutableList.of("en", "fr"), retrieved.getLanguages());
        assertEquals(orgId, retrieved.getOrgMembership());
        assertEquals("test note 1", retrieved.getNote());
        assertEquals("America/Los_Angeles", retrieved.getClientTimeZone());
        assertNull(retrieved.getPassword());
        
        AccountSummarySearch search = new AccountSummarySearch().emailFilter(email);
        AccountSummaryList list = orgAdmin.getClient(OrganizationsApi.class).getMembers(orgId, search).execute().body();
        assertEquals(emailUserId, list.getItems().get(0).getId());
        
        // update the account
        retrieved.setFirstName("Alternate first name");
        retrieved.setNote("test note 2");
        retrieved.setClientTimeZone("Africa/Sao_Tome");
        orgAdminApi.updateAccount(emailUserId, retrieved).execute();
        
        Account retrieved2 = orgAdminApi.getAccount(emailUserId).execute().body();
        assertEquals("Alternate first name", retrieved2.getFirstName());
        assertEquals("test note 2", retrieved2.getNote());
        assertEquals("Africa/Sao_Tome", retrieved2.getClientTimeZone());
        
        // request info. There's not a lot in it since the user cannot sign in (not verified).
        RequestInfo info = orgAdminApi.getAccountRequestInfo(emailUserId).execute().body();
        assertNotNull(info);
        
        // Verify these calls don’t fail
        int code = orgAdminApi.resendAccountEmailVerification(emailUserId).execute().code();
        assertEquals(202, code);
        
        code = orgAdminApi.resendAccountPhoneVerification(emailUserId).execute().code();
        assertEquals(202, code);
        
        code = orgAdminApi.requestAccountResetPassword(emailUserId).execute().code();
        assertEquals(202, code);
        
        code = orgAdminApi.signOutAccount(emailUserId, true).execute().code();
        assertEquals(200, code);
        
        // delete the account
        orgAdminApi.deleteAccount(emailUserId).execute();
        
        try {
            orgAdminApi.getAccount(emailUserId).execute().body();
            fail("Should have thrown an exception");
        } catch(EntityNotFoundException e) {
            assertEquals("Account not found.", e.getMessage());
        }
    }

    @Test
    public void addEmailToPhoneUser() throws Exception {
        SignUp signUp = new SignUp().appId(TEST_APP_ID).phone(PHONE).password(PASSWORD)
                .orgMembership(SAGE_ID).roles(ImmutableList.of(DEVELOPER));
        phoneUserId = admin.getClient(ForAdminsApi.class).createUser(signUp).execute().body().getId();
        
        SignIn signIn = new SignIn().appId(TEST_APP_ID).phone(signUp.getPhone()).password(signUp.getPassword());
        TestUser phoneUser = TestUserHelper.getSignedInUser(signIn);
        
        String email = IntegTestUtils.makeEmail(AccountsTest.class);
        IdentifierUpdate identifierUpdate = new IdentifierUpdate().signIn(signIn).emailUpdate(email);
        
        AccountsApi accountsApi = phoneUser.getClient(AccountsApi.class);
        UserSessionInfo info = accountsApi.updateIdentifiersForSelf(identifierUpdate).execute().body();
        assertEquals(email, info.getEmail());

        Account retrieved = orgAdminApi.getAccount(phoneUserId).execute().body();
        assertEquals(email, retrieved.getEmail());

        // But if you do it again, it should not work
        String newEmail = IntegTestUtils.makeEmail(AccountsTest.class);
        identifierUpdate = new IdentifierUpdate().signIn(signIn).emailUpdate(newEmail);

        info = accountsApi.updateIdentifiersForSelf(identifierUpdate).execute().body();
        assertEquals(email, info.getEmail()); // unchanged
    }

    @Test
    public void addPhoneToEmailUser() throws Exception {
        ForOrgAdminsApi orgAdminApi = orgAdmin.getClient(ForOrgAdminsApi.class);
        
        String email = IntegTestUtils.makeEmail(AccountsTest.class);
        SignUp signUp = new SignUp().appId(TEST_APP_ID).email(email).password(PASSWORD)
                .orgMembership(SAGE_ID).roles(ImmutableList.of(DEVELOPER));
        
        emailUserId = admin.getClient(ForAdminsApi.class).createUser(signUp).execute().body().getId();
        
        SignIn signIn = new SignIn().appId(TEST_APP_ID).email(signUp.getEmail()).password(signUp.getPassword());
        TestUser emailUser = TestUserHelper.getSignedInUser(signIn);
        
        IdentifierUpdate identifierUpdate = new IdentifierUpdate().signIn(signIn).phoneUpdate(PHONE);

        AccountsApi accountsApi = emailUser.getClient(AccountsApi.class);
        UserSessionInfo info = accountsApi.updateIdentifiersForSelf(identifierUpdate).execute().body();
        assertEquals(PHONE.getNumber(), info.getPhone().getNumber());

        Account retrieved = orgAdminApi.getAccount(emailUserId).execute().body();
        assertEquals(PHONE.getNumber(), retrieved.getPhone().getNumber());

        // But if you do it again (even using a new number), it should not work
        identifierUpdate.phoneUpdate(new Phone().number("4082588569").regionCode("US"));
        info = accountsApi.updateIdentifiersForSelf(identifierUpdate).execute().body();
        assertEquals(PHONE.getNumber(), info.getPhone().getNumber()); // unchanged
    }

    @Test
    public void nonAdminCannotViewNote() throws Exception {
        String email = IntegTestUtils.makeEmail(AccountsTest.class);
        SignUp signUp = new SignUp().appId(TEST_APP_ID).email(email).password(PASSWORD).consent(true);

        emailUserId = admin.getClient(ForAdminsApi.class).createUser(signUp).execute().body().getId();

        SignIn signIn = new SignIn().appId(TEST_APP_ID).email(signUp.getEmail()).password(signUp.getPassword());
        TestUser emailUser = TestUserHelper.getSignedInUser(signIn);
        
        ForStudyCoordinatorsApi coordApi = studyCoordinator.getClient(ForStudyCoordinatorsApi.class);
        
        StudyParticipant participant = coordApi.getStudyParticipantById(STUDY_ID_1, emailUserId, false).execute().body();
        participant.setNote("setting a test note");
        coordApi.updateStudyParticipant(STUDY_ID_1, emailUserId, participant).execute();

        // Verifying that the non-Admin user cannot see the note field
        ForConsentedUsersApi testUserConsentedUsersApi = emailUser.getClient(ForConsentedUsersApi.class);
        StudyParticipant nonAdminAccessTestUserAccount = testUserConsentedUsersApi.getUsersParticipantRecord(false).execute().body();
        assertNull(nonAdminAccessTestUserAccount.getNote());
    }

    @Test
    public void nonAdminCannotUpdateNote() throws Exception {
        String email = IntegTestUtils.makeEmail(AccountsTest.class);
        SignUp signUp = new SignUp().appId(TEST_APP_ID).email(email).password(PASSWORD).consent(true);

        emailUserId = admin.getClient(ForAdminsApi.class).createUser(signUp).execute().body().getId();

        SignIn signIn = new SignIn().appId(TEST_APP_ID).email(signUp.getEmail()).password(signUp.getPassword());
        TestUser emailUser = TestUserHelper.getSignedInUser(signIn);
        
        ForStudyCoordinatorsApi coordApi = studyCoordinator.getClient(ForStudyCoordinatorsApi.class);

        StudyParticipant participant = coordApi.getStudyParticipantById(STUDY_ID_1, emailUserId, false).execute().body(); 
        participant.setNote("original test note");
        coordApi.updateStudyParticipant(STUDY_ID_1, participant.getId(), participant).execute();

        // Attempting to update note through non-Admin account
        ForConsentedUsersApi testUserConsentedUsersApi = emailUser.getClient(ForConsentedUsersApi.class);
        StudyParticipant nonAdminAccessTestUserAccount = testUserConsentedUsersApi.getUsersParticipantRecord(false).execute().body();
        nonAdminAccessTestUserAccount.setNote("updated test note");
        testUserConsentedUsersApi.updateUsersParticipantRecord(nonAdminAccessTestUserAccount).execute();

        // Verifying that original note was retained
        StudyParticipant updatedParticipant = coordApi.getStudyParticipantById(STUDY_ID_1, emailUserId, false).execute().body();
        assertEquals("original test note", updatedParticipant.getNote());
    }

    @Test
    public void deleteUnusedAccount() throws Exception {
        String email = IntegTestUtils.makeEmail(AccountsTest.class);
        SignUp signUp = new SignUp().email(email).password(PASSWORD);
        
        StudyParticipantsApi coordParticipantApi = studyCoordinator.getClient(StudyParticipantsApi.class);
        IdentifierHolder idHolder = coordParticipantApi.createStudyParticipant(STUDY_ID_1, signUp).execute().body();
        
        coordParticipantApi.deleteStudyParticipant(STUDY_ID_1, idHolder.getIdentifier()).execute();
        
        try {
            coordParticipantApi.getStudyParticipantById(STUDY_ID_1, idHolder.getIdentifier(), false).execute();    
            fail("Should have thrown exception");
        } catch(EntityNotFoundException e) {
            
        }
    }

    @Test
    public void cannotDeleteUsedAccount() throws Exception {
        // Create a study that is not in design so this test doesn't fail on the enforced 
        // "test_user" flag.
        StudiesApi studiesApi = admin.getClient(StudiesApi.class);
        
        studyId = Tests.randomIdentifier(getClass());
        Study study = new Study().identifier(studyId).name("Study " + studyId);
        studiesApi.createStudy(study).execute().body();
        studiesApi.transitionStudyToRecruitment(studyId).execute();
        
        String externalId = IntegTestUtils.makeEmail(AccountsTest.class).split("@")[0];
        SignUp signUp = new SignUp().password(PASSWORD).externalIds(ImmutableMap.of(studyId, externalId));
        
        StudyParticipantsApi coordParticipantApi = studyCoordinator.getClient(StudyParticipantsApi.class);
        IdentifierHolder idHolder = coordParticipantApi.createStudyParticipant(studyId, signUp).execute().body();
        
        try {
            SignIn signIn = new SignIn().appId(TEST_APP_ID).externalId(externalId).password(PASSWORD);
            TestUserHelper.getSignedInUser(signIn); // this is enough to prevent a physical delete
        } catch(ConsentRequiredException e) {
            
        }
        try {
            coordParticipantApi.deleteStudyParticipant(studyId, idHolder.getIdentifier()).execute();
            fail("Should have thrown exception");
        } catch(UnauthorizedException e) {
            assertEquals("Account is not a test account or it is already in use.", e.getMessage());
        }
        TestUser admin = TestUserHelper.getSignedInAdmin();
        admin.getClient(AccountsApi.class).deleteAccount(idHolder.getIdentifier()).execute();
    }
    
    @Test
    public void adminIsTainted() throws IOException {
        AccountsApi accountsApi = admin.getClient(AccountsApi.class);
        OrganizationsApi orgApi = admin.getClient(OrganizationsApi.class);
        
        Account devAccount = accountsApi.getAccount(developer.getUserId()).execute().body();
        assertTrue(devAccount.isAdmin());
        
        // Removing the orgMembership and roles, and it is still marked as an admin.
        devAccount.setRoles(ImmutableList.of());
        accountsApi.updateAccount(devAccount.getId(), devAccount).execute();
        orgApi.removeMember(devAccount.getOrgMembership(), devAccount.getId()).execute();

        devAccount = accountsApi.getAccount(developer.getUserId()).execute().body();
        assertTrue(devAccount.isAdmin());
        assertNull(devAccount.getOrgMembership());
        assertTrue(devAccount.getRoles().isEmpty());
        
        // This record will be returned from several APIs that exist for admin accounts
        AccountSummaryList list = orgApi.getUnassignedAdminAccounts(new AccountSummarySearch()).execute().body();
        assertTrue(findAccount(list, devAccount.getId()));
        
        // this will pretty shortly not be allowed, but for now it's true that the account is still found this way
        list = admin.getClient(ParticipantsApi.class).searchAccountSummaries(
                new AccountSummarySearch().adminOnly(true)).execute().body();
        assertTrue(findAccount(list, devAccount.getId()));
        
        list = admin.getClient(ParticipantsApi.class).searchAccountSummaries(
                new AccountSummarySearch().adminOnly(false)).execute().body();
        assertFalse(findAccount(list, devAccount.getId()));
    }
    
    private boolean findAccount(AccountSummaryList list, String userId) {
        for (AccountSummary summary : list.getItems()) {
            if (summary.getId().equals(userId)) {
                return true;
            }
        }
        return false;
    }
}