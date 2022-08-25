package org.sagebionetworks.bridge.sdk.integration;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sagebionetworks.bridge.rest.api.ForAdminsApi;
import org.sagebionetworks.bridge.rest.api.ForConsentedUsersApi;
import org.sagebionetworks.bridge.rest.api.ForResearchersApi;
import org.sagebionetworks.bridge.rest.api.OrganizationsApi;
import org.sagebionetworks.bridge.rest.api.StudiesApi;
import org.sagebionetworks.bridge.rest.exceptions.ConsentRequiredException;
import org.sagebionetworks.bridge.rest.exceptions.EntityAlreadyExistsException;
import org.sagebionetworks.bridge.rest.exceptions.EntityNotFoundException;
import org.sagebionetworks.bridge.rest.model.Demographic;
import org.sagebionetworks.bridge.rest.model.DemographicUser;
import org.sagebionetworks.bridge.rest.model.DemographicUserAssessment;
import org.sagebionetworks.bridge.rest.model.DemographicUserAssessmentStep;
import org.sagebionetworks.bridge.rest.model.DemographicUserAssessmentStepAnswerType;
import org.sagebionetworks.bridge.rest.model.DemographicUserList;
import org.sagebionetworks.bridge.rest.model.Enrollment;
import org.sagebionetworks.bridge.rest.model.Role;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.user.TestUser;
import org.sagebionetworks.bridge.user.TestUserHelper;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

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
    private static final Object[] TEST_VALUE_ARRAY_STRING = Arrays.stream(TEST_VALUE_ARRAY).map(String::valueOf)
            .toArray();

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
    OrganizationsApi organizationsApi;

    @Before
    public void before() throws Exception {
        admin = TestUserHelper.getSignedInAdmin();

        adminsApi = admin.getClient(ForAdminsApi.class);
        studiesApi = admin.getClient(StudiesApi.class);
        organizationsApi = admin.getClient(OrganizationsApi.class);

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
        secondConsentedUserInStudy.signOutAndDeleteUser();
        consentedUserNotInStudy.signOutAndDeleteUser();
        unconsentedUser.signOutAndDeleteUser();
        studiesApi.deleteStudy(TEST_STUDY_ID, true);
    }

    /**
     * Tests saving, saving self, saving assessment format, saving assessment format
     * self, get multiple, delete demographic, get one, delete user, get multiple.
     * Tested at the study level.
     */
    @Test
    public void studyLevel() throws IOException {
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
        assertFalse(saveSelfResult.getDemographics().get(TEST_CATEGORY3).isMultipleSelect());
        assertEquals(demographicUserToSaveSelf.getDemographics().get(TEST_CATEGORY1).getValues(),
                saveSelfResult.getDemographics().get(TEST_CATEGORY1).getValues());
        assertEquals(demographicUserToSaveSelf.getDemographics().get(TEST_CATEGORY3).getValues(),
                saveSelfResult.getDemographics().get(TEST_CATEGORY3).getValues());
        assertNull(saveSelfResult.getDemographics().get(TEST_CATEGORY1).getUnits());
        assertEquals(demographicUserToSaveSelf.getDemographics().get(TEST_CATEGORY3).getUnits(),
                saveSelfResult.getDemographics().get(TEST_CATEGORY3).getUnits());

        // save assessment
        DemographicUserAssessment demographicUserAssessmentToSave = new DemographicUserAssessment()
                .stepHistory(ImmutableList.of(
                        new DemographicUserAssessmentStep().identifier(TEST_CATEGORY1)
                                .answerType(new DemographicUserAssessmentStepAnswerType().type("array"))
                                .value(ImmutableList.copyOf(TEST_VALUE_ARRAY)),
                        new DemographicUserAssessmentStep().identifier(TEST_CATEGORY2)
                                .answerType(new DemographicUserAssessmentStepAnswerType().type("string"))
                                .value("value3")));
        try {
            researchersApi.saveDemographicUserAssessment(TEST_STUDY_ID, consentedUserNotInStudy.getUserId(),
                    demographicUserAssessmentToSave).execute().body();
            fail("should have thrown an exception (user not in study)");
        } catch (EntityNotFoundException e) {

        }
        DemographicUser saveAssessmentResult = researchersApi
                .saveDemographicUserAssessment(TEST_STUDY_ID, secondConsentedUserInStudy.getUserId(),
                        demographicUserAssessmentToSave)
                .execute().body();

        assertEquals(secondConsentedUserInStudy.getUserId(), saveAssessmentResult.getUserId());
        assertEquals(2, saveAssessmentResult.getDemographics().size());
        assertNotNull(saveAssessmentResult.getDemographics().get(TEST_CATEGORY1).getId());
        assertNotNull(saveAssessmentResult.getDemographics().get(TEST_CATEGORY2).getId());
        assertTrue(saveAssessmentResult.getDemographics().get(TEST_CATEGORY1).isMultipleSelect());
        assertFalse(saveAssessmentResult.getDemographics().get(TEST_CATEGORY2).isMultipleSelect());
        assertArrayEquals(TEST_VALUE_ARRAY_STRING,
                saveAssessmentResult.getDemographics().get(TEST_CATEGORY1).getValues().toArray());
        assertEquals(demographicUserAssessmentToSave.getStepHistory().get(1).getValue(),
                saveAssessmentResult.getDemographics().get(TEST_CATEGORY2).getValues().get(0));
        assertNull(saveAssessmentResult.getDemographics().get(TEST_CATEGORY1).getUnits());
        assertNull(saveAssessmentResult.getDemographics().get(TEST_CATEGORY2).getUnits());

        // save self assessment
        DemographicUserAssessment demographicUserAssessmentToSaveSelf = new DemographicUserAssessment()
                .stepHistory(ImmutableList.of(
                        new DemographicUserAssessmentStep().identifier(TEST_CATEGORY1)
                                .answerType(new DemographicUserAssessmentStepAnswerType().type("array"))
                                .value(ImmutableList.copyOf(TEST_VALUE_ARRAY)),
                        new DemographicUserAssessmentStep().identifier(TEST_CATEGORY3)
                                .answerType(new DemographicUserAssessmentStepAnswerType().type("string"))
                                .value("value3")));
        try {
            consentedUsersNotInStudyApi
                    .saveDemographicUserSelfAssessment(TEST_STUDY_ID, demographicUserAssessmentToSaveSelf)
                    .execute().body();
            fail("should have thrown an exception (user not in study)");
        } catch (EntityNotFoundException e) {

        }
        DemographicUser saveAssessmentSelfResult = secondConsentedUsersApi
                .saveDemographicUserSelfAssessment(TEST_STUDY_ID, demographicUserAssessmentToSaveSelf)
                .execute().body();

        assertEquals(secondConsentedUserInStudy.getUserId(), saveAssessmentSelfResult.getUserId());
        assertEquals(2, saveAssessmentSelfResult.getDemographics().size());
        assertNotNull(saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY1).getId());
        assertNotNull(saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY3).getId());
        assertTrue(saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY1).isMultipleSelect());
        assertFalse(saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY3).isMultipleSelect());
        assertArrayEquals(TEST_VALUE_ARRAY_STRING,
                saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY1).getValues().toArray());
        assertEquals(demographicUserAssessmentToSaveSelf.getStepHistory().get(1).getValue(),
                saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY3).getValues().get(0));
        assertNull(saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY1).getUnits());
        assertNull(saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY3).getUnits());

        // get multiple
        DemographicUserList getDemographicUsersResult = researchersApi.getDemographicUsers(TEST_STUDY_ID, 0, 10)
                .execute().body();

        assertEquals(2, getDemographicUsersResult.getItems().size());
        // first user
        Optional<DemographicUser> getDemographicUsersResult0Optional = getDemographicUsersResult.getItems().stream()
                .filter(user -> user.getUserId().equals(consentedUserInStudy.getUserId())).findFirst();
        assertTrue(getDemographicUsersResult0Optional.isPresent());
        DemographicUser getDemographicUsersResult0 = getDemographicUsersResult0Optional.get();
        assertEquals(2, getDemographicUsersResult0.getDemographics().size());
        assertNotNull(getDemographicUsersResult0.getDemographics().get(TEST_CATEGORY1).getId());
        assertNotNull(getDemographicUsersResult0.getDemographics().get(TEST_CATEGORY3).getId());
        assertTrue(getDemographicUsersResult0.getDemographics().get(TEST_CATEGORY1).isMultipleSelect());
        assertFalse(getDemographicUsersResult0.getDemographics().get(TEST_CATEGORY3).isMultipleSelect());
        assertEquals(demographicUserToSaveSelf.getDemographics().get(TEST_CATEGORY1).getValues(),
                getDemographicUsersResult0.getDemographics().get(TEST_CATEGORY1).getValues());
        assertEquals(demographicUserToSaveSelf.getDemographics().get(TEST_CATEGORY3).getValues(),
                getDemographicUsersResult0.getDemographics().get(TEST_CATEGORY3).getValues());
        assertNull(getDemographicUsersResult0.getDemographics().get(TEST_CATEGORY1).getUnits());
        assertEquals(demographicUserToSaveSelf.getDemographics().get(TEST_CATEGORY3).getUnits(),
                getDemographicUsersResult0.getDemographics().get(TEST_CATEGORY3).getUnits());
        // second user
        Optional<DemographicUser> getDemographicUsersResult1Optional = getDemographicUsersResult.getItems().stream()
                .filter(user -> user.getUserId().equals(secondConsentedUserInStudy.getUserId())).findFirst();
        assertTrue(getDemographicUsersResult1Optional.isPresent());
        DemographicUser getDemographicUsersResult1 = getDemographicUsersResult1Optional.get();
        assertEquals(2, getDemographicUsersResult1.getDemographics().size());
        assertNotNull(getDemographicUsersResult1.getDemographics().get(TEST_CATEGORY1).getId());
        assertNotNull(getDemographicUsersResult1.getDemographics().get(TEST_CATEGORY3).getId());
        assertTrue(getDemographicUsersResult1.getDemographics().get(TEST_CATEGORY1).isMultipleSelect());
        assertFalse(getDemographicUsersResult1.getDemographics().get(TEST_CATEGORY3).isMultipleSelect());
        assertArrayEquals(TEST_VALUE_ARRAY_STRING,
                getDemographicUsersResult1.getDemographics().get(TEST_CATEGORY1).getValues().toArray());
        assertEquals(demographicUserAssessmentToSaveSelf.getStepHistory().get(1).getValue(),
                getDemographicUsersResult1.getDemographics().get(TEST_CATEGORY3).getValues().get(0));
        assertNull(getDemographicUsersResult1.getDemographics().get(TEST_CATEGORY1).getUnits());
        assertNull(getDemographicUsersResult1.getDemographics().get(TEST_CATEGORY3).getUnits());

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

        // delete user, get multiple
        try {
            researchersApi.deleteDemographicUser(TEST_STUDY_ID, consentedUserNotInStudy.getUserId()).execute();
            fail("should have thrown an exception (user not in study)");
        } catch (EntityNotFoundException e) {

        }
        researchersApi.deleteDemographicUser(TEST_STUDY_ID, secondConsentedUserInStudy.getUserId()).execute();
        DemographicUserList getDemographicUsersAfterDeleteResult = researchersApi
                .getDemographicUsers(TEST_STUDY_ID, 0, 10).execute().body();

        assertEquals(1, getDemographicUsersAfterDeleteResult.getItems().size());
        DemographicUser getDemographicUsersAfterDeleteResult0 = getDemographicUsersAfterDeleteResult.getItems().get(0);
        assertNotNull(getDemographicUsersAfterDeleteResult0.getDemographics().get(TEST_CATEGORY3).getId());
        assertFalse(
                getDemographicUsersAfterDeleteResult0.getDemographics().get(TEST_CATEGORY3).isMultipleSelect());
        assertEquals(demographicUserToSaveSelf.getDemographics().get(TEST_CATEGORY3).getValues(),
                getDemographicUsersAfterDeleteResult0.getDemographics().get(TEST_CATEGORY3).getValues());
        assertEquals(demographicUserToSaveSelf.getDemographics().get(TEST_CATEGORY3).getUnits(),
                getDemographicUsersAfterDeleteResult0.getDemographics().get(TEST_CATEGORY3).getUnits());
    }

    /**
     * Tests saving, saving self, saving assessment format, saving assessment format
     * self, get multiple, delete demographic, get one, delete user, get multiple.
     * Tested at the app level.
     */
    @Test
    public void appLevel() throws IOException {
        // save
        DemographicUser demographicUserToSave = new DemographicUser().demographics(ImmutableMap.of(TEST_CATEGORY1,
                new Demographic().multipleSelect(true).values(ImmutableList.of(TEST_VALUE1, TEST_VALUE2)),
                TEST_CATEGORY2,
                new Demographic().multipleSelect(false).values(ImmutableList.of(TEST_VALUE3)).units("units")));
        DemographicUser saveResult = adminsApi
                .saveDemographicUserAppLevel(consentedUserInStudy.getUserId(), demographicUserToSave).execute()
                .body();

        assertEquals(consentedUserInStudy.getUserId(), saveResult.getUserId());
        assertEquals(2, saveResult.getDemographics().size());
        assertNotNull(saveResult.getDemographics().get(TEST_CATEGORY1).getId());
        assertNotNull(saveResult.getDemographics().get(TEST_CATEGORY2).getId());
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
            unconsentedConsentedUsersApi.saveDemographicUserSelfAppLevel(demographicUserToSaveSelf).execute()
                    .body();
            fail("should have thrown an exception (user is not consented)");
        } catch (ConsentRequiredException e) {

        }
        DemographicUser saveSelfResult = consentedUsersApi
                .saveDemographicUserSelfAppLevel(demographicUserToSaveSelf).execute().body();

        assertEquals(consentedUserInStudy.getUserId(), saveSelfResult.getUserId());
        assertEquals(2, saveSelfResult.getDemographics().size());
        assertNotNull(saveSelfResult.getDemographics().get(TEST_CATEGORY1).getId());
        assertNotNull(saveSelfResult.getDemographics().get(TEST_CATEGORY3).getId());
        assertTrue(saveSelfResult.getDemographics().get(TEST_CATEGORY1).isMultipleSelect());
        assertFalse(saveSelfResult.getDemographics().get(TEST_CATEGORY3).isMultipleSelect());
        assertEquals(demographicUserToSaveSelf.getDemographics().get(TEST_CATEGORY1).getValues(),
                saveSelfResult.getDemographics().get(TEST_CATEGORY1).getValues());
        assertEquals(demographicUserToSaveSelf.getDemographics().get(TEST_CATEGORY3).getValues(),
                saveSelfResult.getDemographics().get(TEST_CATEGORY3).getValues());
        assertNull(saveSelfResult.getDemographics().get(TEST_CATEGORY1).getUnits());
        assertEquals(demographicUserToSaveSelf.getDemographics().get(TEST_CATEGORY3).getUnits(),
                saveSelfResult.getDemographics().get(TEST_CATEGORY3).getUnits());

        // save assessment
        DemographicUserAssessment demographicUserAssessmentToSave = new DemographicUserAssessment()
                .stepHistory(ImmutableList.of(
                        new DemographicUserAssessmentStep().identifier(TEST_CATEGORY1)
                                .answerType(new DemographicUserAssessmentStepAnswerType().type("array"))
                                .value(ImmutableList.copyOf(TEST_VALUE_ARRAY)),
                        new DemographicUserAssessmentStep().identifier(TEST_CATEGORY2)
                                .answerType(new DemographicUserAssessmentStepAnswerType().type("string"))
                                .value("value3")));
        DemographicUser saveAssessmentResult = adminsApi
                .saveDemographicUserAssessmentAppLevel(secondConsentedUserInStudy.getUserId(),
                        demographicUserAssessmentToSave)
                .execute().body();

        assertEquals(secondConsentedUserInStudy.getUserId(), saveAssessmentResult.getUserId());
        assertEquals(2, saveAssessmentResult.getDemographics().size());
        assertNotNull(saveAssessmentResult.getDemographics().get(TEST_CATEGORY1).getId());
        assertNotNull(saveAssessmentResult.getDemographics().get(TEST_CATEGORY2).getId());
        assertTrue(saveAssessmentResult.getDemographics().get(TEST_CATEGORY1).isMultipleSelect());
        assertFalse(saveAssessmentResult.getDemographics().get(TEST_CATEGORY2).isMultipleSelect());
        assertArrayEquals(TEST_VALUE_ARRAY_STRING,
                saveAssessmentResult.getDemographics().get(TEST_CATEGORY1).getValues().toArray());
        assertEquals(demographicUserAssessmentToSave.getStepHistory().get(1).getValue(),
                saveAssessmentResult.getDemographics().get(TEST_CATEGORY2).getValues().get(0));
        assertNull(saveAssessmentResult.getDemographics().get(TEST_CATEGORY1).getUnits());
        assertNull(saveAssessmentResult.getDemographics().get(TEST_CATEGORY2).getUnits());

        // save self assessment
        DemographicUserAssessment demographicUserAssessmentToSaveSelf = new DemographicUserAssessment()
                .stepHistory(ImmutableList.of(
                        new DemographicUserAssessmentStep().identifier(TEST_CATEGORY1)
                                .answerType(new DemographicUserAssessmentStepAnswerType().type("array"))
                                .value(ImmutableList.copyOf(TEST_VALUE_ARRAY)),
                        new DemographicUserAssessmentStep().identifier(TEST_CATEGORY3)
                                .answerType(new DemographicUserAssessmentStepAnswerType().type("string"))
                                .value("value3")));
        DemographicUser saveAssessmentSelfResult = secondConsentedUsersApi
                .saveDemographicUserSelfAssessmentAppLevel(demographicUserAssessmentToSaveSelf)
                .execute().body();

        assertEquals(secondConsentedUserInStudy.getUserId(), saveAssessmentSelfResult.getUserId());
        assertEquals(2, saveAssessmentSelfResult.getDemographics().size());
        assertNotNull(saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY1).getId());
        assertNotNull(saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY3).getId());
        assertTrue(saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY1).isMultipleSelect());
        assertFalse(saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY3).isMultipleSelect());
        assertArrayEquals(TEST_VALUE_ARRAY_STRING,
                saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY1).getValues().toArray());
        assertEquals(demographicUserAssessmentToSaveSelf.getStepHistory().get(1).getValue(),
                saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY3).getValues().get(0));
        assertNull(saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY1).getUnits());
        assertNull(saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY3).getUnits());

        // get multiple
        DemographicUserList getDemographicUsersResult = adminsApi.getDemographicUsersAppLevel(0, 10).execute().body();

        assertEquals(2, getDemographicUsersResult.getItems().size());
        // first user
        Optional<DemographicUser> getDemographicUsersResult0Optional = getDemographicUsersResult.getItems().stream()
                .filter(user -> user.getUserId().equals(consentedUserInStudy.getUserId())).findFirst();
        assertTrue(getDemographicUsersResult0Optional.isPresent());
        DemographicUser getDemographicUsersResult0 = getDemographicUsersResult0Optional.get();
        assertEquals(2, getDemographicUsersResult0.getDemographics().size());
        assertNotNull(getDemographicUsersResult0.getDemographics().get(TEST_CATEGORY1).getId());
        assertNotNull(getDemographicUsersResult0.getDemographics().get(TEST_CATEGORY3).getId());
        assertTrue(getDemographicUsersResult0.getDemographics().get(TEST_CATEGORY1).isMultipleSelect());
        assertFalse(getDemographicUsersResult0.getDemographics().get(TEST_CATEGORY3).isMultipleSelect());
        assertEquals(demographicUserToSaveSelf.getDemographics().get(TEST_CATEGORY1).getValues(),
                getDemographicUsersResult0.getDemographics().get(TEST_CATEGORY1).getValues());
        assertEquals(demographicUserToSaveSelf.getDemographics().get(TEST_CATEGORY3).getValues(),
                getDemographicUsersResult0.getDemographics().get(TEST_CATEGORY3).getValues());
        assertNull(getDemographicUsersResult0.getDemographics().get(TEST_CATEGORY1).getUnits());
        assertEquals(demographicUserToSaveSelf.getDemographics().get(TEST_CATEGORY3).getUnits(),
                getDemographicUsersResult0.getDemographics().get(TEST_CATEGORY3).getUnits());
        // second user
        Optional<DemographicUser> getDemographicUsersResult1Optional = getDemographicUsersResult.getItems().stream()
                .filter(user -> user.getUserId().equals(secondConsentedUserInStudy.getUserId())).findFirst();
        assertTrue(getDemographicUsersResult1Optional.isPresent());
        DemographicUser getDemographicUsersResult1 = getDemographicUsersResult1Optional.get();
        assertEquals(2, getDemographicUsersResult1.getDemographics().size());
        assertNotNull(getDemographicUsersResult1.getDemographics().get(TEST_CATEGORY1).getId());
        assertNotNull(getDemographicUsersResult1.getDemographics().get(TEST_CATEGORY3).getId());
        assertTrue(getDemographicUsersResult1.getDemographics().get(TEST_CATEGORY1).isMultipleSelect());
        assertFalse(getDemographicUsersResult1.getDemographics().get(TEST_CATEGORY3).isMultipleSelect());
        assertArrayEquals(TEST_VALUE_ARRAY_STRING,
                getDemographicUsersResult1.getDemographics().get(TEST_CATEGORY1).getValues().toArray());
        assertEquals(demographicUserAssessmentToSaveSelf.getStepHistory().get(1).getValue(),
                getDemographicUsersResult1.getDemographics().get(TEST_CATEGORY3).getValues().get(0));
        assertNull(getDemographicUsersResult1.getDemographics().get(TEST_CATEGORY1).getUnits());
        assertNull(getDemographicUsersResult1.getDemographics().get(TEST_CATEGORY3).getUnits());

        // delete, get
        try {
            adminsApi.deleteDemographicAppLevel(secondConsentedUserInStudy.getUserId(),
                    saveSelfResult.getDemographics().get(TEST_CATEGORY1).getId()).execute();
            fail("should have thrown an exception (user does not own this demographic)");
        } catch (EntityNotFoundException e) {

        }
        adminsApi.deleteDemographicAppLevel(consentedUserInStudy.getUserId(),
                saveSelfResult.getDemographics().get(TEST_CATEGORY1).getId()).execute();
        DemographicUser getResult = adminsApi
                .getDemographicUserAppLevel(consentedUserInStudy.getUserId()).execute().body();

        assertEquals(consentedUserInStudy.getUserId(), getResult.getUserId());
        assertEquals(1, getResult.getDemographics().size());
        assertFalse(getResult.getDemographics().containsKey(TEST_CATEGORY1));
        assertNotNull(getResult.getDemographics().get(TEST_CATEGORY3).getId());
        assertFalse(getResult.getDemographics().get(TEST_CATEGORY3).isMultipleSelect());
        assertEquals(demographicUserToSaveSelf.getDemographics().get(TEST_CATEGORY3).getValues(),
                getResult.getDemographics().get(TEST_CATEGORY3).getValues());
        assertEquals(demographicUserToSaveSelf.getDemographics().get(TEST_CATEGORY3).getUnits(),
                getResult.getDemographics().get(TEST_CATEGORY3).getUnits());

        // delete user, get multiple
        adminsApi.deleteDemographicUserAppLevel(secondConsentedUserInStudy.getUserId()).execute();
        DemographicUserList getDemographicUsersAfterDeleteResult = adminsApi.getDemographicUsersAppLevel(0, 10)
                .execute().body();

        assertEquals(1, getDemographicUsersAfterDeleteResult.getItems().size());
        DemographicUser getDemographicUsersAfterDeleteResult0 = getDemographicUsersAfterDeleteResult.getItems().get(0);
        assertNotNull(getDemographicUsersAfterDeleteResult0.getDemographics().get(TEST_CATEGORY3).getId());
        assertFalse(
                getDemographicUsersAfterDeleteResult0.getDemographics().get(TEST_CATEGORY3).isMultipleSelect());
        assertEquals(demographicUserToSaveSelf.getDemographics().get(TEST_CATEGORY3).getValues(),
                getDemographicUsersAfterDeleteResult0.getDemographics().get(TEST_CATEGORY3).getValues());
        assertEquals(demographicUserToSaveSelf.getDemographics().get(TEST_CATEGORY3).getUnits(),
                getDemographicUsersAfterDeleteResult0.getDemographics().get(TEST_CATEGORY3).getUnits());
    }
}
