package org.sagebionetworks.bridge.sdk.integration;

import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.Demographic;
import org.sagebionetworks.bridge.rest.model.DemographicUser;
import org.sagebionetworks.bridge.rest.model.DemographicUserAssessment;
import org.sagebionetworks.bridge.rest.model.DemographicUserAssessmentStep;
import org.sagebionetworks.bridge.rest.model.DemographicUserAssessmentStepAnswerType;
import org.sagebionetworks.bridge.rest.model.DemographicUserList;
import org.sagebionetworks.bridge.rest.model.Enrollment;
import org.sagebionetworks.bridge.rest.model.Organization;
import org.sagebionetworks.bridge.user.TestUser;
import org.sagebionetworks.bridge.user.TestUserHelper;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sagebionetworks.bridge.rest.api.AuthenticationApi;
import org.sagebionetworks.bridge.rest.api.ForAdminsApi;
import org.sagebionetworks.bridge.rest.api.ForConsentedUsersApi;
import org.sagebionetworks.bridge.rest.api.ForResearchersApi;
import org.sagebionetworks.bridge.rest.api.ForSuperadminsApi;
import org.sagebionetworks.bridge.rest.api.OrganizationsApi;
import org.sagebionetworks.bridge.rest.api.StudiesApi;
import org.sagebionetworks.bridge.rest.api.StudyParticipantsApi;
import org.sagebionetworks.bridge.rest.exceptions.ConsentRequiredException;
import org.sagebionetworks.bridge.rest.exceptions.EntityAlreadyExistsException;
import org.sagebionetworks.bridge.rest.exceptions.EntityNotFoundException;
import org.sagebionetworks.bridge.rest.model.Role;
import org.sagebionetworks.bridge.rest.model.SignIn;
import org.sagebionetworks.bridge.rest.model.SignUp;
import org.sagebionetworks.bridge.rest.model.Study;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.sagebionetworks.bridge.util.IntegTestUtils.SAGE_ID;

public class DemographicsTest {
    private static final String TEST_STUDY_ID = "study-id";
    private static final String TEST_CATEGORY1 = "category1";
    private static final String TEST_CATEGORY2 = "category2";
    private static final String TEST_CATEGORY3 = "category3";
    private static final String TEST_VALUE1 = "value1";
    private static final String TEST_VALUE2 = "value2";
    private static final String TEST_VALUE3 = "value3";
    private static final boolean TEST_VALUE_BOOL = true;
    private static final int TEST_VALUE_NUMBER = -5;
    private static final double TEST_VALUE_FLOAT = 6.7;
    private static final Object[] TEST_VALUE_ARRAY = { TEST_VALUE1, TEST_VALUE_BOOL, TEST_VALUE_NUMBER,
            TEST_VALUE_FLOAT };

    TestUser admin;
    TestUser researcherStudyCoordinator;
    TestUser consentedUserInStudy;
    TestUser secondConsentedUserInStudy;
    TestUser consentedUserNotInStudy;
    TestUser unconsentedUser;

    ForAdminsApi adminsApi;
    ForConsentedUsersApi consentedUsersApi;
    ForConsentedUsersApi secondConsentedUsersApi;
    ForConsentedUsersApi consentedUsersNotInStudyApi;
    ForConsentedUsersApi unconsentedConsentedUsersApi;
    ForResearchersApi researchersApi;
    StudiesApi studiesApi;
    ForSuperadminsApi superadminApi;
    AuthenticationApi authenticationApi;
    OrganizationsApi organizationsApi;

    @Before
    public void before() throws Exception {
        admin = TestUserHelper.getSignedInAdmin();

        adminsApi = admin.getClient(ForAdminsApi.class);
        studiesApi = admin.getClient(StudiesApi.class);
        superadminApi = admin.getClient(ForSuperadminsApi.class);
        authenticationApi = admin.getClient(AuthenticationApi.class);
        organizationsApi = admin.getClient(OrganizationsApi.class);

        // System.out.println(admin.getAppId());

        researcherStudyCoordinator = TestUserHelper.createAndSignInUser(DemographicsTest.class, true, Role.RESEARCHER,
                Role.STUDY_COORDINATOR);
        consentedUserInStudy = TestUserHelper.createAndSignInUser(DemographicsTest.class, true);
        secondConsentedUserInStudy = TestUserHelper.createAndSignInUser(DemographicsTest.class, true);
        consentedUserNotInStudy = TestUserHelper.createAndSignInUser(DemographicsTest.class, true);
        unconsentedUser = TestUserHelper.createAndSignInUser(DemographicsTest.class, false);

        researchersApi = researcherStudyCoordinator.getClient(ForResearchersApi.class);
        consentedUsersApi = consentedUserInStudy.getClient(ForConsentedUsersApi.class);
        secondConsentedUsersApi = secondConsentedUserInStudy.getClient(ForConsentedUsersApi.class);
        consentedUsersNotInStudyApi = consentedUserNotInStudy.getClient(ForConsentedUsersApi.class);
        unconsentedConsentedUsersApi = unconsentedUser.getClient(ForConsentedUsersApi.class);

        Organization org = new Organization().identifier(SAGE_ID).name("Sage Bionetworks");
        try {
            organizationsApi.createOrganization(org).execute();
        } catch (EntityAlreadyExistsException e) {

        }

        Study study = new Study().identifier(TEST_STUDY_ID).name("test study");
        try {
            studiesApi.createStudy(study).execute();
        } catch (EntityAlreadyExistsException e) {

        }
        studiesApi.enrollParticipant(TEST_STUDY_ID, new Enrollment().userId(consentedUserInStudy.getUserId()))
                .execute();
        studiesApi.enrollParticipant(TEST_STUDY_ID, new Enrollment().userId(secondConsentedUserInStudy.getUserId()))
                .execute();
        consentedUserInStudy.signInAgain();
        secondConsentedUserInStudy.signInAgain();
    }

    @After
    public void after() {
        researcherStudyCoordinator.signOutAndDeleteUser();
        consentedUserInStudy.signOutAndDeleteUser();
        consentedUserNotInStudy.signOutAndDeleteUser();
        unconsentedUser.signOutAndDeleteUser();
        studiesApi.deleteStudy(TEST_STUDY_ID, true);
        organizationsApi.deleteOrganization(SAGE_ID);
    }

    // save, save self, delete, get, save assessment, save self assessment, delete
    // user, get multiple
    // save app, save self app, delete app, get app, save app assessment, save self
    // app assessment, delete user app, get multiple app

    @Test
    public void getDemographicUser() throws IOException {
        // save
        DemographicUser demographicUserToSave = new DemographicUser().demographics(ImmutableMap.of(TEST_CATEGORY1,
                new Demographic().multipleSelect(true).values(ImmutableList.of(TEST_VALUE1, TEST_VALUE2)),
                TEST_CATEGORY2,
                new Demographic().multipleSelect(false).values(ImmutableList.of(TEST_VALUE3)).units("units")));
        try {
            researchersApi
                    .saveDemographicUser(TEST_STUDY_ID, consentedUserNotInStudy.getUserId(), demographicUserToSave)
                    .execute()
                    .body();
            fail("should have thrown an exception (user not in study)");
        } catch (EntityNotFoundException e) {

        }
        DemographicUser saveResult = researchersApi
                .saveDemographicUser(TEST_STUDY_ID, consentedUserInStudy.getUserId(), demographicUserToSave).execute()
                .body();

        assertEquals(consentedUserInStudy.getUserId(), saveResult.getUserId());
        assertEquals(2, saveResult.getDemographics().size());
        assertNotNull(saveResult.getDemographics().get(TEST_CATEGORY1).getId());
        assertNotNull(saveResult.getDemographics().get(TEST_CATEGORY2).getId());
        assertTrue(saveResult.getDemographics().get(TEST_CATEGORY1).isMultipleSelect());
        assertTrue(saveResult.getDemographics().get(TEST_CATEGORY1).isMultipleSelect());
        assertFalse(saveResult.getDemographics().get(TEST_CATEGORY2).isMultipleSelect());
        assertEquals(demographicUserToSave.getDemographics().get(TEST_CATEGORY1).getValues(),
                saveResult.getDemographics().get(TEST_CATEGORY1).getValues());
        assertEquals(demographicUserToSave.getDemographics().get(TEST_CATEGORY2).getValues(),
                saveResult.getDemographics().get(TEST_CATEGORY2).getValues());
        assertNull(saveResult.getDemographics().get(TEST_CATEGORY1).getUnits());
        assertEquals(demographicUserToSave.getDemographics().get(TEST_CATEGORY2).getUnits(),
                saveResult.getDemographics().get(TEST_CATEGORY2).getUnits());

        // save self
        DemographicUser demographicUserToSaveSelf = new DemographicUser().demographics(ImmutableMap.of(TEST_CATEGORY1,
                new Demographic().multipleSelect(true).values(ImmutableList.of(TEST_VALUE1, TEST_VALUE2)),
                TEST_CATEGORY3,
                new Demographic().multipleSelect(false).values(ImmutableList.of(TEST_VALUE3)).units("units")));
        try {
            consentedUsersNotInStudyApi.saveDemographicUserSelf(TEST_STUDY_ID, demographicUserToSaveSelf).execute()
                    .body();
            fail("should have thrown an exception (user is not in study)");
        } catch (EntityNotFoundException e) {

        }
        try {
            unconsentedConsentedUsersApi.saveDemographicUserSelf(TEST_STUDY_ID, demographicUserToSaveSelf).execute()
                    .body();
            fail("should have thrown an exception (user is not consented)");
        } catch (ConsentRequiredException e) {

        }
        DemographicUser saveSelfResult = consentedUsersApi
                .saveDemographicUserSelf(TEST_STUDY_ID, demographicUserToSaveSelf).execute().body();

        assertEquals(consentedUserInStudy.getUserId(), saveSelfResult.getUserId());
        assertEquals(2, saveSelfResult.getDemographics().size());
        assertNotNull(saveSelfResult.getDemographics().get(TEST_CATEGORY1).getId());
        assertNotNull(saveSelfResult.getDemographics().get(TEST_CATEGORY3).getId());
        assertTrue(saveSelfResult.getDemographics().get(TEST_CATEGORY1).isMultipleSelect());
        assertTrue(saveSelfResult.getDemographics().get(TEST_CATEGORY1).isMultipleSelect());
        assertFalse(saveSelfResult.getDemographics().get(TEST_CATEGORY3).isMultipleSelect());
        assertEquals(demographicUserToSaveSelf.getDemographics().get(TEST_CATEGORY1).getValues(),
                saveSelfResult.getDemographics().get(TEST_CATEGORY1).getValues());
        assertEquals(demographicUserToSaveSelf.getDemographics().get(TEST_CATEGORY3).getValues(),
                saveSelfResult.getDemographics().get(TEST_CATEGORY3).getValues());
        assertNull(saveSelfResult.getDemographics().get(TEST_CATEGORY1).getUnits());
        assertEquals(demographicUserToSaveSelf.getDemographics().get(TEST_CATEGORY3).getUnits(),
                saveSelfResult.getDemographics().get(TEST_CATEGORY3).getUnits());

        // delete, get
        try {
            researchersApi.deleteDemographic(TEST_STUDY_ID, consentedUserNotInStudy.getUserId(),
                    saveSelfResult.getDemographics().get(TEST_CATEGORY1).getId()).execute();
            fail("should have thrown an exception (user not in study)");
        } catch (EntityNotFoundException e) {

        }
        try {
            researchersApi.deleteDemographic(TEST_STUDY_ID, secondConsentedUserInStudy.getUserId(),
                    saveSelfResult.getDemographics().get(TEST_CATEGORY1).getId()).execute();
            fail("should have thrown an exception (user does not own this demographic)");
        } catch (EntityNotFoundException e) {

        }
        researchersApi.deleteDemographic(TEST_STUDY_ID, consentedUserInStudy.getUserId(),
                saveSelfResult.getDemographics().get(TEST_CATEGORY1).getId()).execute();
        DemographicUser getResult = researchersApi
                .getDemographicUser(TEST_STUDY_ID, consentedUserInStudy.getUserId()).execute().body();

        assertEquals(consentedUserInStudy.getUserId(), getResult.getUserId());
        assertEquals(1, getResult.getDemographics().size());
        assertFalse(getResult.getDemographics().containsKey(TEST_CATEGORY1));
        assertNotNull(getResult.getDemographics().get(TEST_CATEGORY3).getId());
        assertFalse(getResult.getDemographics().get(TEST_CATEGORY3).isMultipleSelect());
        assertEquals(demographicUserToSaveSelf.getDemographics().get(TEST_CATEGORY3).getValues(),
                getResult.getDemographics().get(TEST_CATEGORY3).getValues());
        assertEquals(demographicUserToSaveSelf.getDemographics().get(TEST_CATEGORY3).getUnits(),
                getResult.getDemographics().get(TEST_CATEGORY3).getUnits());

        // save assessment
        DemographicUserAssessment demographicUserAssessmentToSave = new DemographicUserAssessment()
                .stepHistory(ImmutableList.of(
                        new DemographicUserAssessmentStep().identifier(TEST_CATEGORY1)
                                .answerType(new DemographicUserAssessmentStepAnswerType().type("array"))
                                .value(ImmutableList.of("value1", "value2")),
                        new DemographicUserAssessmentStep().identifier(TEST_CATEGORY2)
                                .answerType(new DemographicUserAssessmentStepAnswerType().type("string"))
                                .value("value3")));
        try {
            researchersApi.saveDemographicUserAssessment(TEST_STUDY_ID, consentedUserNotInStudy.getUserId(),
                    demographicUserAssessmentToSave).execute().body();
            fail("should have thrown an exception (user not in study)");
        } catch (EntityNotFoundException e) {

        }
        System.out.println(researchersApi.getDemographicUsers(TEST_STUDY_ID).execute().body().getItems().size());
        DemographicUser saveAssessmentResult = researchersApi
                .saveDemographicUserAssessment(TEST_STUDY_ID, secondConsentedUserInStudy.getUserId(),
                        demographicUserAssessmentToSave)
                .execute().body();

        assertEquals(secondConsentedUserInStudy.getUserId(), saveAssessmentResult.getUserId());
        assertEquals(2, saveAssessmentResult.getDemographics().size());
        assertNotNull(saveAssessmentResult.getDemographics().get(TEST_CATEGORY1).getId());
        assertNotNull(saveAssessmentResult.getDemographics().get(TEST_CATEGORY2).getId());
        assertTrue(saveAssessmentResult.getDemographics().get(TEST_CATEGORY1).isMultipleSelect());
        assertTrue(saveAssessmentResult.getDemographics().get(TEST_CATEGORY1).isMultipleSelect());
        assertFalse(saveAssessmentResult.getDemographics().get(TEST_CATEGORY2).isMultipleSelect());
        assertEquals(demographicUserAssessmentToSave.getStepHistory().get(0).getValue(),
                saveAssessmentResult.getDemographics().get(TEST_CATEGORY1).getValues());
        assertEquals(demographicUserAssessmentToSave.getStepHistory().get(1).getValue(),
                saveAssessmentResult.getDemographics().get(TEST_CATEGORY2).getValues().get(0));
        assertNull(saveAssessmentResult.getDemographics().get(TEST_CATEGORY1).getUnits());
        assertNull(saveAssessmentResult.getDemographics().get(TEST_CATEGORY2).getUnits());

        // save self assessment
        DemographicUserAssessment demographicUserAssessmentToSaveSelf = new DemographicUserAssessment()
                .stepHistory(ImmutableList.of(
                        new DemographicUserAssessmentStep().identifier(TEST_CATEGORY1)
                                .answerType(new DemographicUserAssessmentStepAnswerType().type("array"))
                                .value(ImmutableList.of("value1", "value2")),
                        new DemographicUserAssessmentStep().identifier(TEST_CATEGORY3)
                                .answerType(new DemographicUserAssessmentStepAnswerType().type("string"))
                                .value("value3")));
        try {
            consentedUsersNotInStudyApi
                    .saveDemographicUserAssessmentSelf(TEST_STUDY_ID, demographicUserAssessmentToSaveSelf)
                    .execute().body();
            fail("should have thrown an exception (user not in study)");
        } catch (EntityNotFoundException e) {

        }
        System.out.println(researchersApi.getDemographicUsers(TEST_STUDY_ID).execute().body().getItems().size());
        DemographicUser saveAssessmentSelfResult = secondConsentedUsersApi
                .saveDemographicUserAssessmentSelf(TEST_STUDY_ID, demographicUserAssessmentToSaveSelf)
                .execute().body();

        assertEquals(secondConsentedUserInStudy.getUserId(), saveAssessmentSelfResult.getUserId());
        assertEquals(2, saveAssessmentSelfResult.getDemographics().size());
        assertNotNull(saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY1).getId());
        assertNotNull(saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY3).getId());
        assertTrue(saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY1).isMultipleSelect());
        assertTrue(saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY1).isMultipleSelect());
        assertFalse(saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY3).isMultipleSelect());
        assertEquals(demographicUserAssessmentToSaveSelf.getStepHistory().get(0).getValue(),
                saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY1).getValues());
        assertEquals(demographicUserAssessmentToSaveSelf.getStepHistory().get(1).getValue(),
                saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY3).getValues().get(0));
        assertNull(saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY1).getUnits());
        assertNull(saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY3).getUnits());

        // delete user, get multiple
        try {
            researchersApi.deleteDemographicUser(TEST_STUDY_ID, consentedUserNotInStudy.getUserId()).execute();
            fail("should have thrown an exception (user not in study)");
        } catch (EntityNotFoundException e) {

        }
        System.out.println(researchersApi.getDemographicUsers(TEST_STUDY_ID).execute().body().getItems());
        researchersApi.deleteDemographicUser(TEST_STUDY_ID, secondConsentedUserInStudy.getUserId()).execute();
        DemographicUserList getDemographicUsersResult = researchersApi.getDemographicUsers(TEST_STUDY_ID).execute()
                .body();

        System.out.println(getDemographicUsersResult.getItems());
        // TODO there should only be 1 here
        assertEquals(1, getDemographicUsersResult.getItems().size());
        assertNotNull(getDemographicUsersResult.getItems().get(0).getDemographics().get(TEST_CATEGORY3).getId());
        assertFalse(
                getDemographicUsersResult.getItems().get(0).getDemographics().get(TEST_CATEGORY3).isMultipleSelect());
        assertEquals(demographicUserToSaveSelf.getDemographics().get(TEST_CATEGORY3).getValues(),
                getDemographicUsersResult.getItems().get(0).getDemographics().get(TEST_CATEGORY3).getValues());
        assertEquals(demographicUserToSaveSelf.getDemographics().get(TEST_CATEGORY3).getUnits(),
                getDemographicUsersResult.getItems().get(0).getDemographics().get(TEST_CATEGORY3).getUnits());
    }
}
