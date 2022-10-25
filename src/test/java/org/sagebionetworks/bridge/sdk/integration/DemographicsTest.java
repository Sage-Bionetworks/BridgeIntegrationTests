package org.sagebionetworks.bridge.sdk.integration;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sagebionetworks.bridge.rest.api.AppConfigsApi;
import org.sagebionetworks.bridge.rest.api.ForAdminsApi;
import org.sagebionetworks.bridge.rest.api.ForConsentedUsersApi;
import org.sagebionetworks.bridge.rest.api.ForResearchersApi;
import org.sagebionetworks.bridge.rest.api.OrganizationsApi;
import org.sagebionetworks.bridge.rest.api.StudiesApi;
import org.sagebionetworks.bridge.rest.exceptions.ConsentRequiredException;
import org.sagebionetworks.bridge.rest.exceptions.EntityAlreadyExistsException;
import org.sagebionetworks.bridge.rest.exceptions.EntityNotFoundException;
import org.sagebionetworks.bridge.rest.exceptions.InvalidEntityException;
import org.sagebionetworks.bridge.rest.model.AppConfigElement;
import org.sagebionetworks.bridge.rest.model.Demographic;
import org.sagebionetworks.bridge.rest.model.DemographicUser;
import org.sagebionetworks.bridge.rest.model.DemographicUserAssessment;
import org.sagebionetworks.bridge.rest.model.DemographicUserAssessmentAnswer;
import org.sagebionetworks.bridge.rest.model.DemographicUserAssessmentAnswerAnswerType;
import org.sagebionetworks.bridge.rest.model.DemographicUserAssessmentAnswerCollection;
import org.sagebionetworks.bridge.rest.model.DemographicUserResponse;
import org.sagebionetworks.bridge.rest.model.DemographicUserResponseList;
import org.sagebionetworks.bridge.rest.model.DemographicValuesEnumValidationRules;
import org.sagebionetworks.bridge.rest.model.DemographicValuesNumberRangeValidationRules;
import org.sagebionetworks.bridge.rest.model.DemographicValuesValidationConfiguration;
import org.sagebionetworks.bridge.rest.model.DemographicValuesValidationConfiguration.ValidationTypeEnum;
import org.sagebionetworks.bridge.rest.model.Enrollment;
import org.sagebionetworks.bridge.rest.model.Role;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.user.TestUser;
import org.sagebionetworks.bridge.user.TestUserHelper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
    private static final String TEST_UNITS = "units";
    private static final boolean TEST_VALUE_BOOL = true;
    private static final int TEST_VALUE_NUMBER = -5;
    private static final double TEST_VALUE_FLOAT = 6.7;
    private static final Object[] TEST_VALUE_ARRAY = { TEST_VALUE1, TEST_VALUE_BOOL, TEST_VALUE_NUMBER,
            TEST_VALUE_FLOAT };
    private static final String[] TEST_VALUE_ARRAY_STRING = Arrays.stream(TEST_VALUE_ARRAY).map(String::valueOf)
            .toArray(size -> new String[size]);
    private static final Map<String, Object> TEST_VALUE_MAP = new HashMap<>();
    static {
        TEST_VALUE_MAP.put("a", "foo");
        TEST_VALUE_MAP.put("b", 3);
        TEST_VALUE_MAP.put("c", -5.7);
    }
    private static final String APP_CONFIG_RANDOM_ID = "id-that-should-be-ignored";
    private static final String APP_CONFIG_CATEGORY1_ID = "bridge-validation-demographics-values-category1";
    private static final String APP_CONFIG_CATEGORY2_ID = "bridge-validation-demographics-values-category2";
    private static final String APP_CONFIG_CATEGORY3_ID = "bridge-validation-demographics-values-category3";
    private static final String APP_CONFIG_CATEGORY4_ID = "bridge-validation-demographics-values-category4";
    private static final String APP_CONFIG_NIH_CATEGORY_ID_YEAR_OF_BIRTH = "bridge-validation-demographics-values-year-of-birth";
    private static final String NIH_CATEGORY_YEAR_OF_BIRTH = "year-of-birth";
    private static final String APP_CONFIG_NIH_CATEGORY_ID_BIOLOGICAL_SEX = "bridge-validation-demographics-values-biological-sex";
    private static final String NIH_CATEGORY_BIOLOGICAL_SEX = "biological-sex";
    private static final String APP_CONFIG_NIH_CATEGORY_ID_ETHNICITY = "bridge-validation-demographics-values-ethnicity";
    private static final String NIH_CATEGORY_ETHNICITY = "ethnicity";
    private static final String APP_CONFIG_NIH_CATEGORY_ID_HIGHEST_EDUCATION = "bridge-validation-demographics-values-highest-education";
    private static final String NIH_CATEGORY_HIGHEST_EDUCATION = "highest-education";
    private static final String[] APP_CONFIG_CATEGORIES_TO_DELETE = { APP_CONFIG_RANDOM_ID, APP_CONFIG_CATEGORY1_ID,
            APP_CONFIG_CATEGORY2_ID, APP_CONFIG_CATEGORY3_ID, APP_CONFIG_CATEGORY4_ID,
            APP_CONFIG_NIH_CATEGORY_ID_YEAR_OF_BIRTH, APP_CONFIG_NIH_CATEGORY_ID_BIOLOGICAL_SEX,
            APP_CONFIG_NIH_CATEGORY_ID_ETHNICITY, APP_CONFIG_NIH_CATEGORY_ID_HIGHEST_EDUCATION };

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
    AppConfigsApi appConfigsApi;

    @Before
    public void before() throws Exception {
        admin = TestUserHelper.getSignedInAdmin();

        adminsApi = admin.getClient(ForAdminsApi.class);
        studiesApi = admin.getClient(StudiesApi.class);
        organizationsApi = admin.getClient(OrganizationsApi.class);
        appConfigsApi = admin.getClient(AppConfigsApi.class);

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
    public void after() throws IOException {
        for (String appConfigElementId : APP_CONFIG_CATEGORIES_TO_DELETE) {
            appConfigsApi.deleteAllAppConfigElementRevisions(appConfigElementId, true).execute();
        }
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
                new Demographic().multipleSelect(false).values(ImmutableList.of(TEST_VALUE3)).units(TEST_UNITS)));
        try {
            researchersApi
                    .saveDemographicUser(TEST_STUDY_ID, consentedUserNotInStudy.getUserId(), demographicUserToSave)
                    .execute()
                    .body();
            fail("should have thrown an exception (user not in study)");
        } catch (EntityNotFoundException e) {

        }
        DemographicUserResponse saveResult = researchersApi
                .saveDemographicUser(TEST_STUDY_ID, consentedUserInStudy.getUserId(), demographicUserToSave).execute()
                .body();

        assertNotNull(saveResult);
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
                new Demographic().multipleSelect(false).values(ImmutableList.of(TEST_VALUE3)).units(TEST_UNITS)));
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
        DemographicUserResponse saveSelfResult = consentedUsersApi
                .saveDemographicUserSelf(TEST_STUDY_ID, demographicUserToSaveSelf).execute().body();

        assertNotNull(saveSelfResult);
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
                        new DemographicUserAssessmentAnswerCollection().children(ImmutableList.of(
                                new DemographicUserAssessmentAnswer().identifier(TEST_CATEGORY1)
                                        .value(ImmutableList.copyOf(TEST_VALUE_ARRAY)),
                                new DemographicUserAssessmentAnswer().identifier(TEST_CATEGORY2)
                                        .answerType(new DemographicUserAssessmentAnswerAnswerType().unit(TEST_UNITS))
                                        .value(TEST_VALUE3)))));
        try {
            researchersApi.saveDemographicUserAssessment(TEST_STUDY_ID, consentedUserNotInStudy.getUserId(),
                    demographicUserAssessmentToSave).execute().body();
            fail("should have thrown an exception (user not in study)");
        } catch (EntityNotFoundException e) {

        }
        DemographicUserResponse saveAssessmentResult = researchersApi
                .saveDemographicUserAssessment(TEST_STUDY_ID, secondConsentedUserInStudy.getUserId(),
                        demographicUserAssessmentToSave)
                .execute().body();

        assertNotNull(saveAssessmentResult);
        assertEquals(secondConsentedUserInStudy.getUserId(), saveAssessmentResult.getUserId());
        assertEquals(2, saveAssessmentResult.getDemographics().size());
        assertNotNull(saveAssessmentResult.getDemographics().get(TEST_CATEGORY1).getId());
        assertNotNull(saveAssessmentResult.getDemographics().get(TEST_CATEGORY2).getId());
        assertTrue(saveAssessmentResult.getDemographics().get(TEST_CATEGORY1).isMultipleSelect());
        assertFalse(saveAssessmentResult.getDemographics().get(TEST_CATEGORY2).isMultipleSelect());
        assertArrayEquals(TEST_VALUE_ARRAY_STRING,
                saveAssessmentResult.getDemographics().get(TEST_CATEGORY1).getValues().toArray());
        assertEquals(TEST_VALUE3, saveAssessmentResult.getDemographics().get(TEST_CATEGORY2).getValues().get(0));
        assertNull(saveAssessmentResult.getDemographics().get(TEST_CATEGORY1).getUnits());
        assertEquals(TEST_UNITS, saveAssessmentResult.getDemographics().get(TEST_CATEGORY2).getUnits());

        // save self assessment
        DemographicUserAssessment demographicUserAssessmentToSaveSelf = new DemographicUserAssessment()
                .stepHistory(ImmutableList.of(new DemographicUserAssessmentAnswerCollection().children(ImmutableList.of(
                        new DemographicUserAssessmentAnswer().identifier(TEST_CATEGORY1)
                                .value(ImmutableList.copyOf(TEST_VALUE_ARRAY)),
                        new DemographicUserAssessmentAnswer().identifier(TEST_CATEGORY3)
                                .answerType(new DemographicUserAssessmentAnswerAnswerType().unit(TEST_UNITS))
                                .value(TEST_VALUE_MAP)))));
        try {
            consentedUsersNotInStudyApi
                    .saveDemographicUserSelfAssessment(TEST_STUDY_ID, demographicUserAssessmentToSaveSelf)
                    .execute().body();
            fail("should have thrown an exception (user not in study)");
        } catch (EntityNotFoundException e) {

        }
        DemographicUserResponse saveAssessmentSelfResult = secondConsentedUsersApi
                .saveDemographicUserSelfAssessment(TEST_STUDY_ID, demographicUserAssessmentToSaveSelf)
                .execute().body();

        assertNotNull(saveAssessmentSelfResult);
        assertEquals(secondConsentedUserInStudy.getUserId(), saveAssessmentSelfResult.getUserId());
        assertEquals(2, saveAssessmentSelfResult.getDemographics().size());
        assertNotNull(saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY1).getId());
        assertNotNull(saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY3).getId());
        assertTrue(saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY1).isMultipleSelect());
        assertTrue(saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY3).isMultipleSelect());
        assertArrayEquals(TEST_VALUE_ARRAY_STRING,
                saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY1).getValues().toArray());
        assertEquals(TEST_VALUE_MAP.size(),
                saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY3).getValues().size());
        for (Map.Entry<String, Object> entry : TEST_VALUE_MAP.entrySet()) {
            String entryString = makeStringEntry(entry);
            assertThat(saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY3).getValues(),
                    hasItem(entryString));
        }
        assertNull(saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY1).getUnits());
        assertEquals(TEST_UNITS, saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY3).getUnits());

        // get multiple
        DemographicUserResponseList getDemographicUsersResult = researchersApi.getDemographicUsers(TEST_STUDY_ID, 0, 10)
                .execute().body();

        assertNotNull(getDemographicUsersResult);
        assertEquals(2, getDemographicUsersResult.getItems().size());
        // first user
        Optional<DemographicUserResponse> getDemographicUsersResult0Optional = getDemographicUsersResult.getItems()
                .stream().filter(user -> user.getUserId().equals(consentedUserInStudy.getUserId())).findFirst();
        assertTrue(getDemographicUsersResult0Optional.isPresent());
        DemographicUserResponse getDemographicUsersResult0 = getDemographicUsersResult0Optional.get();
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
        Optional<DemographicUserResponse> getDemographicUsersResult1Optional = getDemographicUsersResult.getItems()
                .stream().filter(user -> user.getUserId().equals(secondConsentedUserInStudy.getUserId())).findFirst();
        assertTrue(getDemographicUsersResult1Optional.isPresent());
        DemographicUserResponse getDemographicUsersResult1 = getDemographicUsersResult1Optional.get();
        assertEquals(2, getDemographicUsersResult1.getDemographics().size());
        assertNotNull(getDemographicUsersResult1.getDemographics().get(TEST_CATEGORY1).getId());
        assertNotNull(getDemographicUsersResult1.getDemographics().get(TEST_CATEGORY3).getId());
        assertTrue(getDemographicUsersResult1.getDemographics().get(TEST_CATEGORY1).isMultipleSelect());
        assertTrue(getDemographicUsersResult1.getDemographics().get(TEST_CATEGORY3).isMultipleSelect());
        assertArrayEquals(TEST_VALUE_ARRAY_STRING,
                getDemographicUsersResult1.getDemographics().get(TEST_CATEGORY1).getValues().toArray());
        assertEquals(TEST_VALUE_MAP.size(),
                getDemographicUsersResult1.getDemographics().get(TEST_CATEGORY3).getValues().size());
        for (Map.Entry<String, Object> entry : TEST_VALUE_MAP.entrySet()) {
            String entryString = makeStringEntry(entry);
            assertThat(getDemographicUsersResult1.getDemographics().get(TEST_CATEGORY3).getValues(),
                    hasItem(entryString));
        }
        assertNull(getDemographicUsersResult1.getDemographics().get(TEST_CATEGORY1).getUnits());
        assertEquals(TEST_UNITS, getDemographicUsersResult1.getDemographics().get(TEST_CATEGORY3).getUnits());

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
        DemographicUserResponse getResult = researchersApi
                .getDemographicUser(TEST_STUDY_ID, consentedUserInStudy.getUserId()).execute().body();

        assertNotNull(getResult);
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
        DemographicUserResponseList getDemographicUsersAfterDeleteResult = researchersApi
                .getDemographicUsers(TEST_STUDY_ID, 0, 10).execute().body();

        assertNotNull(getDemographicUsersAfterDeleteResult);
        assertEquals(1, getDemographicUsersAfterDeleteResult.getItems().size());
        DemographicUserResponse getDemographicUsersAfterDeleteResult0 = getDemographicUsersAfterDeleteResult.getItems()
                .get(0);
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
                new Demographic().multipleSelect(false).values(ImmutableList.of(TEST_VALUE3)).units(TEST_UNITS)));
        DemographicUserResponse saveResult = adminsApi
                .saveDemographicUserAppLevel(consentedUserInStudy.getUserId(), demographicUserToSave).execute()
                .body();

        assertNotNull(saveResult);
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
                new Demographic().multipleSelect(false).values(ImmutableList.of(TEST_VALUE3)).units(TEST_UNITS)));
        try {
            unconsentedConsentedUsersApi.saveDemographicUserSelfAppLevel(demographicUserToSaveSelf).execute()
                    .body();
            fail("should have thrown an exception (user is not consented)");
        } catch (ConsentRequiredException e) {

        }
        DemographicUserResponse saveSelfResult = consentedUsersApi
                .saveDemographicUserSelfAppLevel(demographicUserToSaveSelf).execute().body();

        assertNotNull(saveSelfResult);
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
                .stepHistory(ImmutableList.of(new DemographicUserAssessmentAnswerCollection().children(ImmutableList.of(
                        new DemographicUserAssessmentAnswer().identifier(TEST_CATEGORY1)
                                .value(ImmutableList.copyOf(TEST_VALUE_ARRAY)),
                        new DemographicUserAssessmentAnswer().identifier(TEST_CATEGORY2)
                                .answerType(new DemographicUserAssessmentAnswerAnswerType().unit(TEST_UNITS))
                                .value(TEST_VALUE3)))));
        DemographicUserResponse saveAssessmentResult = adminsApi
                .saveDemographicUserAssessmentAppLevel(secondConsentedUserInStudy.getUserId(),
                        demographicUserAssessmentToSave)
                .execute().body();

        assertNotNull(saveAssessmentResult);
        assertEquals(secondConsentedUserInStudy.getUserId(), saveAssessmentResult.getUserId());
        assertEquals(2, saveAssessmentResult.getDemographics().size());
        assertNotNull(saveAssessmentResult.getDemographics().get(TEST_CATEGORY1).getId());
        assertNotNull(saveAssessmentResult.getDemographics().get(TEST_CATEGORY2).getId());
        assertTrue(saveAssessmentResult.getDemographics().get(TEST_CATEGORY1).isMultipleSelect());
        assertFalse(saveAssessmentResult.getDemographics().get(TEST_CATEGORY2).isMultipleSelect());
        assertArrayEquals(TEST_VALUE_ARRAY_STRING,
                saveAssessmentResult.getDemographics().get(TEST_CATEGORY1).getValues().toArray());
        assertEquals(TEST_VALUE3, saveAssessmentResult.getDemographics().get(TEST_CATEGORY2).getValues().get(0));
        assertNull(saveAssessmentResult.getDemographics().get(TEST_CATEGORY1).getUnits());
        assertEquals(TEST_UNITS, saveAssessmentResult.getDemographics().get(TEST_CATEGORY2).getUnits());

        // save self assessment
        DemographicUserAssessment demographicUserAssessmentToSaveSelf = new DemographicUserAssessment()
                .stepHistory(ImmutableList.of(new DemographicUserAssessmentAnswerCollection().children(ImmutableList.of(
                        new DemographicUserAssessmentAnswer().identifier(TEST_CATEGORY1)
                                .value(ImmutableList.copyOf(TEST_VALUE_ARRAY)),
                        new DemographicUserAssessmentAnswer().identifier(TEST_CATEGORY3)
                                .answerType(new DemographicUserAssessmentAnswerAnswerType().unit(TEST_UNITS))
                                .value(TEST_VALUE_MAP)))));
        DemographicUserResponse saveAssessmentSelfResult = secondConsentedUsersApi
                .saveDemographicUserSelfAssessmentAppLevel(demographicUserAssessmentToSaveSelf)
                .execute().body();

        assertNotNull(saveAssessmentSelfResult);
        assertEquals(secondConsentedUserInStudy.getUserId(), saveAssessmentSelfResult.getUserId());
        assertEquals(2, saveAssessmentSelfResult.getDemographics().size());
        assertNotNull(saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY1).getId());
        assertNotNull(saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY3).getId());
        assertTrue(saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY1).isMultipleSelect());
        assertTrue(saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY3).isMultipleSelect());
        assertArrayEquals(TEST_VALUE_ARRAY_STRING,
                saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY1).getValues().toArray());
        assertEquals(TEST_VALUE_MAP.size(),
                saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY3).getValues().size());
        for (Map.Entry<String, Object> entry : TEST_VALUE_MAP.entrySet()) {
            String entryString = makeStringEntry(entry);
            assertThat(saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY3).getValues(),
                    hasItem(entryString));
        }
        assertNull(saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY1).getUnits());
        assertEquals(TEST_UNITS, saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY3).getUnits());

        // get multiple
        DemographicUserResponseList getDemographicUsersResult = adminsApi.getDemographicUsersAppLevel(0, 10).execute()
                .body();

        assertNotNull(getDemographicUsersResult);
        assertEquals(2, getDemographicUsersResult.getItems().size());
        // first user
        Optional<DemographicUserResponse> getDemographicUsersResult0Optional = getDemographicUsersResult.getItems()
                .stream().filter(user -> user.getUserId().equals(consentedUserInStudy.getUserId())).findFirst();
        assertTrue(getDemographicUsersResult0Optional.isPresent());
        DemographicUserResponse getDemographicUsersResult0 = getDemographicUsersResult0Optional.get();
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
        Optional<DemographicUserResponse> getDemographicUsersResult1Optional = getDemographicUsersResult.getItems()
                .stream().filter(user -> user.getUserId().equals(secondConsentedUserInStudy.getUserId())).findFirst();
        assertTrue(getDemographicUsersResult1Optional.isPresent());
        DemographicUserResponse getDemographicUsersResult1 = getDemographicUsersResult1Optional.get();
        assertEquals(2, getDemographicUsersResult1.getDemographics().size());
        assertNotNull(getDemographicUsersResult1.getDemographics().get(TEST_CATEGORY1).getId());
        assertNotNull(getDemographicUsersResult1.getDemographics().get(TEST_CATEGORY3).getId());
        assertTrue(getDemographicUsersResult1.getDemographics().get(TEST_CATEGORY1).isMultipleSelect());
        assertTrue(getDemographicUsersResult1.getDemographics().get(TEST_CATEGORY3).isMultipleSelect());
        assertArrayEquals(TEST_VALUE_ARRAY_STRING,
                getDemographicUsersResult1.getDemographics().get(TEST_CATEGORY1).getValues().toArray());
        assertEquals(TEST_VALUE_MAP.size(),
                getDemographicUsersResult1.getDemographics().get(TEST_CATEGORY3).getValues().size());
        for (Map.Entry<String, Object> entry : TEST_VALUE_MAP.entrySet()) {
            String entryString = makeStringEntry(entry);
            assertThat(getDemographicUsersResult1.getDemographics().get(TEST_CATEGORY3).getValues(),
                    hasItem(entryString));
        }
        assertNull(getDemographicUsersResult1.getDemographics().get(TEST_CATEGORY1).getUnits());
        assertEquals(TEST_UNITS, getDemographicUsersResult1.getDemographics().get(TEST_CATEGORY3).getUnits());

        // delete, get
        try {
            adminsApi.deleteDemographicAppLevel(secondConsentedUserInStudy.getUserId(),
                    saveSelfResult.getDemographics().get(TEST_CATEGORY1).getId()).execute();
            fail("should have thrown an exception (user does not own this demographic)");
        } catch (EntityNotFoundException e) {

        }
        adminsApi.deleteDemographicAppLevel(consentedUserInStudy.getUserId(),
                saveSelfResult.getDemographics().get(TEST_CATEGORY1).getId()).execute();
        DemographicUserResponse getResult = adminsApi
                .getDemographicUserAppLevel(consentedUserInStudy.getUserId()).execute().body();

        assertNotNull(getResult);
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
        DemographicUserResponseList getDemographicUsersAfterDeleteResult = adminsApi.getDemographicUsersAppLevel(0, 10)
                .execute().body();

        assertNotNull(getDemographicUsersAfterDeleteResult);
        assertEquals(1, getDemographicUsersAfterDeleteResult.getItems().size());
        DemographicUserResponse getDemographicUsersAfterDeleteResult0 = getDemographicUsersAfterDeleteResult.getItems()
                .get(0);
        assertNotNull(getDemographicUsersAfterDeleteResult0.getDemographics().get(TEST_CATEGORY3).getId());
        assertFalse(
                getDemographicUsersAfterDeleteResult0.getDemographics().get(TEST_CATEGORY3).isMultipleSelect());
        assertEquals(demographicUserToSaveSelf.getDemographics().get(TEST_CATEGORY3).getValues(),
                getDemographicUsersAfterDeleteResult0.getDemographics().get(TEST_CATEGORY3).getValues());
        assertEquals(demographicUserToSaveSelf.getDemographics().get(TEST_CATEGORY3).getUnits(),
                getDemographicUsersAfterDeleteResult0.getDemographics().get(TEST_CATEGORY3).getUnits());
    }

    @Test
    public void valueValidationWithAppConfigElements() throws IOException {
        // add random id that should be ignored
        DemographicValuesEnumValidationRules randomRules = new DemographicValuesEnumValidationRules();
        randomRules.put("en", ImmutableList.of("a", "b"));
        appConfigsApi.createAppConfigElement(new AppConfigElement().id(APP_CONFIG_RANDOM_ID).revision(1L)
                .data(new DemographicValuesValidationConfiguration().validationType(ValidationTypeEnum.ENUM)
                        .validationRules(randomRules)))
                .execute();

        // add validation for category2
        DemographicValuesEnumValidationRules category2Rules = new DemographicValuesEnumValidationRules();
        category2Rules.put("en", ImmutableList.of("foo", "bar"));
        appConfigsApi.createAppConfigElement(new AppConfigElement().id(APP_CONFIG_CATEGORY2_ID).revision(1L)
                .data(new DemographicValuesValidationConfiguration().validationType(ValidationTypeEnum.ENUM)
                        .validationRules(category2Rules)))
                .execute();

        // validation is for category2 not category1, no validation exists for
        // category1, so should work
        DemographicUser demographicUserInvalid = new DemographicUser()
                .demographics(
                        ImmutableMap.of(
                                "category1",
                                new Demographic().values(ImmutableList.of("incorrect", "values"))));
        adminsApi.saveDemographicUserAppLevel(consentedUserInStudy.getUserId(), demographicUserInvalid).execute();

        // add validation for category1
        // spanish should be ignored
        DemographicValuesEnumValidationRules category1Rules = new DemographicValuesEnumValidationRules();
        category1Rules.put("en", ImmutableList.of("a", "bb", "7", "-6.3", "true"));
        category1Rules.put("es", ImmutableList.of("this", "should", "be", "ignored"));
        appConfigsApi.createAppConfigElement(new AppConfigElement().id(APP_CONFIG_CATEGORY1_ID).revision(1L)
                .data(new DemographicValuesValidationConfiguration().validationType(ValidationTypeEnum.ENUM)
                        .validationRules(category1Rules)))
                .execute();

        // retry invalid demographics with validation for category1 now added (should
        // not work)
        try {
            adminsApi.saveDemographicUserAppLevel(consentedUserInStudy.getUserId(), demographicUserInvalid).execute();
            fail("should have thrown an exception, demographics are invalid for category1");
        } catch (InvalidEntityException e) {

        }

        // retry with valid demographics
        // values are out of order, repeated, and in all types
        DemographicUser demographicUserValid = new DemographicUser()
                .demographics(
                        ImmutableMap.of(
                                "category1",
                                new Demographic()
                                        .values(ImmutableList.of(true, "bb", "a", -6.3, 7, "a", "a", "a", "a"))));
        adminsApi.saveDemographicUserAppLevel(consentedUserInStudy.getUserId(), demographicUserValid).execute();

        // add another revision
        DemographicValuesEnumValidationRules category1RulesRev2 = new DemographicValuesEnumValidationRules();
        category1RulesRev2.put("en", ImmutableList.of("xyz"));
        appConfigsApi.createAppConfigElement(new AppConfigElement().id(APP_CONFIG_CATEGORY1_ID).revision(2L)
                .data(new DemographicValuesValidationConfiguration().validationType(ValidationTypeEnum.ENUM)
                        .validationRules(category1RulesRev2)))
                .execute();

        // this SHOULD NOT work because validation should only use the most revision
        try {
            adminsApi.saveDemographicUserAppLevel(consentedUserInStudy.getUserId(), demographicUserValid).execute();
            fail("should have thrown an exception, demographics are now invalid after app config element revision");
        } catch (InvalidEntityException e) {

        }

        // this SHOULD work because validation should only use the most revision
        DemographicUser demographicUserValidRev2 = new DemographicUser()
                .demographics(
                        ImmutableMap.of(
                                "category1",
                                new Demographic()
                                        .values(ImmutableList.of("xyz", "xyz"))));
        adminsApi.saveDemographicUserAppLevel(consentedUserInStudy.getUserId(), demographicUserValidRev2).execute();

        // no english
        DemographicValuesEnumValidationRules category4Rules = new DemographicValuesEnumValidationRules();
        category4Rules.put("es", ImmutableList.of());
        appConfigsApi.createAppConfigElement(new AppConfigElement().id(APP_CONFIG_CATEGORY4_ID).revision(1L)
                .data(new DemographicValuesValidationConfiguration().validationType(ValidationTypeEnum.ENUM)
                        .validationRules(category4Rules)))
                .execute();

        // should not error even when english is not explicitly specified in
        // configuration
        DemographicUser demographicUserValidNoEnglish = new DemographicUser()
                .demographics(
                        ImmutableMap.of(
                                "category4",
                                new Demographic()
                                        .values(ImmutableList.of(true, "bb", "a", -6.3, 7, "a", "a", "a", "a"))));
        adminsApi.saveDemographicUserAppLevel(consentedUserInStudy.getUserId(), demographicUserValidNoEnglish)
                .execute();

        // number range validation
        appConfigsApi.createAppConfigElement(new AppConfigElement().id(APP_CONFIG_CATEGORY3_ID).revision(1L)
                .data(new DemographicValuesValidationConfiguration().validationType(ValidationTypeEnum.NUMBER_RANGE)
                        .validationRules(new DemographicValuesNumberRangeValidationRules().min(0d).max(100d))))
                .execute();

        // should work
        DemographicUser demographicUserValidNumber = new DemographicUser()
                .demographics(
                        ImmutableMap.of(
                                "category3",
                                new Demographic()
                                        .values(ImmutableList.of(99))));
        adminsApi
                .saveDemographicUserAppLevel(consentedUserInStudy.getUserId(), demographicUserValidNumber)
                .execute();

        // should not work because not a number
        DemographicUser demographicUserStringsInNumberValidation = new DemographicUser()
                .demographics(
                        ImmutableMap.of(
                                "category3",
                                new Demographic()
                                        .values(ImmutableList.of("xyz", "xyz"))));
        try {
            adminsApi
                    .saveDemographicUserAppLevel(consentedUserInStudy.getUserId(),
                            demographicUserStringsInNumberValidation)
                    .execute();
            fail("should have thrown an exception (specified number range validation but not a number)");
        } catch (InvalidEntityException e) {

        }

        // should not work
        DemographicUser demographicUserInvalidNumber = new DemographicUser()
                .demographics(
                        ImmutableMap.of(
                                "category3",
                                new Demographic()
                                        .values(ImmutableList.of(-12.7))));
        try {
            adminsApi.saveDemographicUserAppLevel(consentedUserInStudy.getUserId(), demographicUserInvalidNumber)
                    .execute();
            fail("should have thrown an exception (number is less than min)");
        } catch (InvalidEntityException e) {

        }

        // uploading for study should work even with invalid values because validation
        // only applies to app-level demographics
        researchersApi.saveDemographicUser(TEST_STUDY_ID, consentedUserInStudy.getUserId(), demographicUserInvalid)
                .execute();
    }

    /**
     * Real-world test for NIH minimum categories. Valid and invalid sample for each
     * category.
     */
    @Test
    public void validateNIHCategories() throws IOException {
        // add validation
        appConfigsApi
                .createAppConfigElement(
                        new AppConfigElement().id(APP_CONFIG_NIH_CATEGORY_ID_YEAR_OF_BIRTH).revision(1L)
                                .data(new DemographicValuesValidationConfiguration()
                                        .validationType(ValidationTypeEnum.NUMBER_RANGE)
                                        .validationRules(new DemographicValuesNumberRangeValidationRules().min(1900.0)
                                                .max(2050.0))))
                .execute();
        DemographicValuesEnumValidationRules biologicalSexEnumRules = new DemographicValuesEnumValidationRules();
        biologicalSexEnumRules.put("en",
                ImmutableList.of("Male", "Female", "Intersex", "None of these describe me", "Prefer not to answer"));
        appConfigsApi
                .createAppConfigElement(
                        new AppConfigElement().id(APP_CONFIG_NIH_CATEGORY_ID_BIOLOGICAL_SEX).revision(1L)
                                .data(new DemographicValuesValidationConfiguration()
                                        .validationType(ValidationTypeEnum.ENUM)
                                        .validationRules(biologicalSexEnumRules)))
                .execute();
        DemographicValuesEnumValidationRules ethnicityEnumRules = new DemographicValuesEnumValidationRules();
        ethnicityEnumRules.put("en",
                ImmutableList.of("American Indian or Alaska Native", "Asian", "Black, African American, or African",
                        "Hispanic, Latino, or Spanish", "Middle Eastern or North African",
                        "Native Hawaiian or other Pacific Islander", "White", "None of these fully describe me",
                        "Prefer not to answer"));
        appConfigsApi
                .createAppConfigElement(new AppConfigElement().id(APP_CONFIG_NIH_CATEGORY_ID_ETHNICITY).revision(1L)
                        .data(new DemographicValuesValidationConfiguration()
                                .validationType(ValidationTypeEnum.ENUM)
                                .validationRules(ethnicityEnumRules)))
                .execute();
        DemographicValuesEnumValidationRules highestEducationRules = new DemographicValuesEnumValidationRules();
        highestEducationRules.put("en",
                ImmutableList.of("Never attended school or only attended kindergarten", "Grades 1 through 4 (Primary)",
                        "Grades 5 through 8 (Middle school)", "Grades 9 through 11 (Some high school)",
                        "Grade 12 or GED (High school graduate)",
                        "1 to 3 years after high school (Some college, Associate’s degree, or technical school)",
                        "College 4 years or more (College graduate)", "Advanced degree (Master’s, Doctorate, etc.)",
                        "Prefer not to answer"));
        appConfigsApi.createAppConfigElement(new AppConfigElement().id(APP_CONFIG_NIH_CATEGORY_ID_HIGHEST_EDUCATION)
                .revision(1L)
                .data(new DemographicValuesValidationConfiguration().validationType(ValidationTypeEnum.ENUM)
                        .validationRules(highestEducationRules)))
                .execute();

        // test responses
        DemographicUser demographicUserValidBirthYear = new DemographicUser()
                .demographics(
                        ImmutableMap.of(
                                NIH_CATEGORY_YEAR_OF_BIRTH,
                                new Demographic().values(ImmutableList.of(2000))));
        adminsApi.saveDemographicUserAppLevel(consentedUserInStudy.getUserId(), demographicUserValidBirthYear)
                .execute();
        DemographicUser demographicUserInvalidBirthYear1 = new DemographicUser()
                .demographics(
                        ImmutableMap.of(
                                NIH_CATEGORY_YEAR_OF_BIRTH,
                                new Demographic().values(ImmutableList.of(1899))));
        try {
            adminsApi.saveDemographicUserAppLevel(consentedUserInStudy.getUserId(), demographicUserInvalidBirthYear1)
                    .execute();
            fail("should have thrown an exception, invalid value for year of birth");
        } catch (InvalidEntityException e) {

        }
        DemographicUser demographicUserInvalidBirthYear2 = new DemographicUser()
                .demographics(
                        ImmutableMap.of(
                                NIH_CATEGORY_YEAR_OF_BIRTH,
                                new Demographic().values(ImmutableList.of(2051))));
        try {
            adminsApi.saveDemographicUserAppLevel(consentedUserInStudy.getUserId(), demographicUserInvalidBirthYear2)
                    .execute();
            fail("should have thrown an exception, invalid value for year of birth");
        } catch (InvalidEntityException e) {

        }

        DemographicUser demographicUserValidSex = new DemographicUser()
                .demographics(
                        ImmutableMap.of(
                                NIH_CATEGORY_BIOLOGICAL_SEX,
                                new Demographic().values(ImmutableList.of("None of these describe me"))));
        adminsApi.saveDemographicUserAppLevel(consentedUserInStudy.getUserId(), demographicUserValidSex).execute();
        DemographicUser demographicUserValidInvalidSex = new DemographicUser()
                .demographics(
                        ImmutableMap.of(
                                NIH_CATEGORY_BIOLOGICAL_SEX,
                                new Demographic().values(ImmutableList.of("invalid value"))));
        try {
            adminsApi.saveDemographicUserAppLevel(consentedUserInStudy.getUserId(), demographicUserValidInvalidSex)
                    .execute();
            fail("should have thrown an exception, invalid value for biological sex");
        } catch (InvalidEntityException e) {

        }

        DemographicUser demographicUserValidEthnicity = new DemographicUser()
                .demographics(
                        ImmutableMap.of(
                                NIH_CATEGORY_ETHNICITY,
                                new Demographic().values(ImmutableList.of("Black, African American, or African",
                                        "Hispanic, Latino, or Spanish"))));
        adminsApi.saveDemographicUserAppLevel(consentedUserInStudy.getUserId(), demographicUserValidEthnicity)
                .execute();
        DemographicUser demographicUserValidInvalidEthnicity = new DemographicUser()
                .demographics(
                        ImmutableMap.of(
                                NIH_CATEGORY_ETHNICITY,
                                new Demographic().values(ImmutableList.of("invalid value"))));
        try {
            adminsApi
                    .saveDemographicUserAppLevel(consentedUserInStudy.getUserId(), demographicUserValidInvalidEthnicity)
                    .execute();
            fail("should have thrown an exception, invalid value for ethnicity");
        } catch (InvalidEntityException e) {

        }

        DemographicUser demographicUserValidEducation = new DemographicUser()
                .demographics(
                        ImmutableMap.of(
                                NIH_CATEGORY_HIGHEST_EDUCATION,
                                new Demographic().values(ImmutableList.of(
                                        "1 to 3 years after high school (Some college, Associate’s degree, or technical school)"))));
        adminsApi.saveDemographicUserAppLevel(consentedUserInStudy.getUserId(), demographicUserValidEducation)
                .execute();
        DemographicUser demographicUserValidInvalidEducation = new DemographicUser()
                .demographics(
                        ImmutableMap.of(
                                NIH_CATEGORY_HIGHEST_EDUCATION,
                                new Demographic().values(ImmutableList.of("invalid value"))));
        try {
            adminsApi
                    .saveDemographicUserAppLevel(consentedUserInStudy.getUserId(), demographicUserValidInvalidEducation)
                    .execute();
            fail("should have thrown an exception, invalid value for highest education");
        } catch (InvalidEntityException e) {

        }
    }

    private String makeStringEntry(Map.Entry<String, Object> entry) throws JsonProcessingException {
        String entryString = entry.getKey() + "=";
        if (entry.getValue() instanceof String) {
            entryString += (String) entry.getValue();
        } else {
            entryString += new ObjectMapper().writeValueAsString(entry.getValue());
        }
        return entryString;
    }
}
