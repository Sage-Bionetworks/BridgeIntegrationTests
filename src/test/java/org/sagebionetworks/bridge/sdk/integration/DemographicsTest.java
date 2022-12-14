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
import java.util.stream.Collectors;

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
import org.sagebionetworks.bridge.rest.model.DemographicResponse;
import org.sagebionetworks.bridge.rest.model.DemographicUser;
import org.sagebionetworks.bridge.rest.model.DemographicUserAssessment;
import org.sagebionetworks.bridge.rest.model.DemographicUserAssessmentAnswer;
import org.sagebionetworks.bridge.rest.model.DemographicUserAssessmentAnswerAnswerType;
import org.sagebionetworks.bridge.rest.model.DemographicUserAssessmentAnswerCollection;
import org.sagebionetworks.bridge.rest.model.DemographicUserResponse;
import org.sagebionetworks.bridge.rest.model.DemographicUserResponseList;
import org.sagebionetworks.bridge.rest.model.DemographicValueResponse;
import org.sagebionetworks.bridge.rest.model.DemographicValuesEnumValidationRules;
import org.sagebionetworks.bridge.rest.model.DemographicValuesNumberRangeValidationRules;
import org.sagebionetworks.bridge.rest.model.DemographicValuesValidationConfig;
import org.sagebionetworks.bridge.rest.model.DemographicValuesValidationConfig.ValidationTypeEnum;
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
    
    private static final String IGNORED_CATEGORY = "category-that-should-be-ignored";
    private static final String CATEGORY1 = "category1";
    private static final String CATEGORY2 = "category2";
    private static final String CATEGORY3 = "category3";
    private static final String CATEGORY4 = "category4";
    private static final String NIH_CATEGORY_YEAR_OF_BIRTH = "year-of-birth";
    private static final String NIH_CATEGORY_BIOLOGICAL_SEX = "biological-sex";
    private static final String NIH_CATEGORY_ETHNICITY = "ethnicity";
    private static final String NIH_CATEGORY_HIGHEST_EDUCATION = "highest-education";
    private static final String INVALID_ENUM_VALUE = "invalid enum value";
    private static final String INVALID_NUMBER_VALUE_NOT_A_NUMBER = "invalid number";
    private static final String INVALID_NUMBER_VALUE_LESS_THAN_MIN = "invalid number value (less than min)";
    private static final String INVALID_NUMBER_VALUE_GREATER_THAN_MAX = "invalid number (larger than max)";

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
    public void after() throws IOException {
        researcherStudyCoordinator.signOutAndDeleteUser();
        consentedUserInStudy.signOutAndDeleteUser();
        secondConsentedUserInStudy.signOutAndDeleteUser();
        consentedUserNotInStudy.signOutAndDeleteUser();
        unconsentedUser.signOutAndDeleteUser();
        studiesApi.deleteStudy(TEST_STUDY_ID, true);
    }

    private void assertUploadAndResponse(String userId, DemographicUser uploaded, DemographicUserResponse response) {
        assertNotNull(response);
        assertEquals(userId, response.getUserId());
        assertEquals(uploaded.getDemographics().size(), response.getDemographics().size());
        for (String category : uploaded.getDemographics().keySet()) {
            Demographic uploadedDemographic = uploaded.getDemographics().get(category);
            DemographicResponse responseDemographic = response.getDemographics().get(category);
            assertNotNull(responseDemographic);
            assertNotNull(responseDemographic.getId());
            assertEquals(uploadedDemographic.isMultipleSelect(), responseDemographic.isMultipleSelect());
            assertEquals(uploadedDemographic.getValues().size(), responseDemographic.getValues().size());
            for (int i = 0; i < uploadedDemographic.getValues().size(); i++) {
                assertEquals(uploadedDemographic.getValues().get(i), responseDemographic.getValues().get(i).getValue());
            }
            assertEquals(uploadedDemographic.getUnits(), responseDemographic.getUnits());
        }
    }

    // does not check values or whether it is multiple select because everything is
    // a generic Object
    private void assertAssessmentUploadAndResponseWithoutValuesOrMultipleSelect(String userId,
            DemographicUserAssessment uploaded, int answerCollectionIndex, DemographicUserResponse response) {
        DemographicUserAssessmentAnswerCollection answerCollection = (DemographicUserAssessmentAnswerCollection) uploaded
                .getStepHistory().get(answerCollectionIndex);
        assertNotNull(response);
        assertEquals(userId, response.getUserId());
        assertEquals(answerCollection.getChildren().size(), response.getDemographics().size());
        for (DemographicUserAssessmentAnswer answer : answerCollection.getChildren()) {
            DemographicResponse responseDemographic = response.getDemographics().get(answer.getIdentifier());
            assertNotNull(responseDemographic);
            assertNotNull(responseDemographic.getId());
        }
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

        assertUploadAndResponse(consentedUserInStudy.getUserId(), demographicUserToSave, saveResult);

        // save self
        DemographicUser demographicUserToSaveSelf = new DemographicUser().demographics(new HashMap<>());
        demographicUserToSaveSelf.getDemographics().putAll(ImmutableMap.of(TEST_CATEGORY1,
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

        assertUploadAndResponse(consentedUserInStudy.getUserId(), demographicUserToSaveSelf, saveSelfResult);

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

        assertAssessmentUploadAndResponseWithoutValuesOrMultipleSelect(secondConsentedUserInStudy.getUserId(),
                demographicUserAssessmentToSave, 0, saveAssessmentResult);
        assertTrue(saveAssessmentResult.getDemographics().get(TEST_CATEGORY1).isMultipleSelect());
        assertFalse(saveAssessmentResult.getDemographics().get(TEST_CATEGORY2).isMultipleSelect());
        assertArrayEquals(TEST_VALUE_ARRAY_STRING,
                saveAssessmentResult.getDemographics().get(TEST_CATEGORY1).getValues().stream()
                        .map(DemographicValueResponse::getValue).toArray());
        assertEquals(TEST_VALUE3,
                saveAssessmentResult.getDemographics().get(TEST_CATEGORY2).getValues().get(0).getValue());
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

        assertAssessmentUploadAndResponseWithoutValuesOrMultipleSelect(secondConsentedUserInStudy.getUserId(),
                demographicUserAssessmentToSaveSelf, 0, saveAssessmentSelfResult);
        assertTrue(saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY1).isMultipleSelect());
        assertTrue(saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY3).isMultipleSelect());

        assertArrayEquals(TEST_VALUE_ARRAY_STRING,
                saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY1).getValues().stream()
                        .map(DemographicValueResponse::getValue).toArray());
        assertEquals(TEST_VALUE_MAP.size(),
                saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY3).getValues().size());
        for (Map.Entry<String, Object> entry : TEST_VALUE_MAP.entrySet()) {
            String entryString = makeStringEntry(entry);
            assertThat(
                    saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY3).getValues().stream()
                            .map(DemographicValueResponse::getValue).collect(Collectors.toList()),
                    hasItem(entryString));
        }

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
        assertUploadAndResponse(consentedUserInStudy.getUserId(), demographicUserToSaveSelf,
                getDemographicUsersResult0);
        // second user
        Optional<DemographicUserResponse> getDemographicUsersResult1Optional = getDemographicUsersResult.getItems()
                .stream().filter(user -> user.getUserId().equals(secondConsentedUserInStudy.getUserId())).findFirst();
        assertTrue(getDemographicUsersResult1Optional.isPresent());
        DemographicUserResponse getDemographicUsersResult1 = getDemographicUsersResult1Optional.get();
        assertAssessmentUploadAndResponseWithoutValuesOrMultipleSelect(secondConsentedUserInStudy.getUserId(),
                demographicUserAssessmentToSaveSelf, 0, getDemographicUsersResult1);
        assertTrue(getDemographicUsersResult1.getDemographics().get(TEST_CATEGORY1).isMultipleSelect());
        assertTrue(getDemographicUsersResult1.getDemographics().get(TEST_CATEGORY3).isMultipleSelect());
        assertArrayEquals(TEST_VALUE_ARRAY_STRING,
                getDemographicUsersResult1.getDemographics().get(TEST_CATEGORY1).getValues().stream()
                        .map(DemographicValueResponse::getValue).toArray());
        assertEquals(TEST_VALUE_MAP.size(),
                getDemographicUsersResult1.getDemographics().get(TEST_CATEGORY3).getValues().size());
        for (Map.Entry<String, Object> entry : TEST_VALUE_MAP.entrySet()) {
            String entryString = makeStringEntry(entry);
            assertThat(
                    getDemographicUsersResult1.getDemographics().get(TEST_CATEGORY3).getValues().stream()
                            .map(DemographicValueResponse::getValue).collect(Collectors.toList()),
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

        demographicUserToSaveSelf.getDemographics().remove(TEST_CATEGORY1);
        assertUploadAndResponse(consentedUserInStudy.getUserId(), demographicUserToSaveSelf, getResult);

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
                getDemographicUsersAfterDeleteResult0.getDemographics().get(TEST_CATEGORY3).getValues().stream()
                        .map(DemographicValueResponse::getValue).collect(Collectors.toList()));
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

        assertUploadAndResponse(consentedUserInStudy.getUserId(), demographicUserToSave, saveResult);

        // save self
        DemographicUser demographicUserToSaveSelf = new DemographicUser().demographics(new HashMap<>());
        demographicUserToSaveSelf.getDemographics().putAll(ImmutableMap.of(TEST_CATEGORY1,
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

        assertUploadAndResponse(consentedUserInStudy.getUserId(), demographicUserToSaveSelf, saveSelfResult);

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

        assertAssessmentUploadAndResponseWithoutValuesOrMultipleSelect(secondConsentedUserInStudy.getUserId(),
                demographicUserAssessmentToSave, 0, saveAssessmentResult);
        assertTrue(saveAssessmentResult.getDemographics().get(TEST_CATEGORY1).isMultipleSelect());
        assertFalse(saveAssessmentResult.getDemographics().get(TEST_CATEGORY2).isMultipleSelect());
        assertArrayEquals(TEST_VALUE_ARRAY_STRING,
                saveAssessmentResult.getDemographics().get(TEST_CATEGORY1).getValues().stream()
                        .map(DemographicValueResponse::getValue).toArray());
        assertEquals(TEST_VALUE3,
                saveAssessmentResult.getDemographics().get(TEST_CATEGORY2).getValues().get(0).getValue());
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

        assertAssessmentUploadAndResponseWithoutValuesOrMultipleSelect(secondConsentedUserInStudy.getUserId(),
                demographicUserAssessmentToSaveSelf, 0, saveAssessmentSelfResult);
        assertTrue(saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY1).isMultipleSelect());
        assertTrue(saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY3).isMultipleSelect());
        assertArrayEquals(TEST_VALUE_ARRAY_STRING,
                saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY1).getValues().stream()
                        .map(DemographicValueResponse::getValue).toArray());
        assertEquals(TEST_VALUE_MAP.size(),
                saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY3).getValues().size());
        for (Map.Entry<String, Object> entry : TEST_VALUE_MAP.entrySet()) {
            String entryString = makeStringEntry(entry);
            assertThat(
                    saveAssessmentSelfResult.getDemographics().get(TEST_CATEGORY3).getValues().stream()
                            .map(DemographicValueResponse::getValue).collect(Collectors.toList()),
                    hasItem(entryString));
        }

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
        assertUploadAndResponse(consentedUserInStudy.getUserId(), demographicUserToSaveSelf,
                getDemographicUsersResult0);
        // second user
        Optional<DemographicUserResponse> getDemographicUsersResult1Optional = getDemographicUsersResult.getItems()
                .stream().filter(user -> user.getUserId().equals(secondConsentedUserInStudy.getUserId())).findFirst();
        assertTrue(getDemographicUsersResult1Optional.isPresent());
        DemographicUserResponse getDemographicUsersResult1 = getDemographicUsersResult1Optional.get();
        assertAssessmentUploadAndResponseWithoutValuesOrMultipleSelect(secondConsentedUserInStudy.getUserId(),
                demographicUserAssessmentToSaveSelf, 0, getDemographicUsersResult1);
        assertTrue(getDemographicUsersResult1.getDemographics().get(TEST_CATEGORY1).isMultipleSelect());
        assertTrue(getDemographicUsersResult1.getDemographics().get(TEST_CATEGORY3).isMultipleSelect());
        assertArrayEquals(TEST_VALUE_ARRAY_STRING,
                getDemographicUsersResult1.getDemographics().get(TEST_CATEGORY1).getValues().stream()
                        .map(DemographicValueResponse::getValue).toArray());
        assertEquals(TEST_VALUE_MAP.size(),
                getDemographicUsersResult1.getDemographics().get(TEST_CATEGORY3).getValues().size());
        for (Map.Entry<String, Object> entry : TEST_VALUE_MAP.entrySet()) {
            String entryString = makeStringEntry(entry);
            assertThat(
                    getDemographicUsersResult1.getDemographics().get(TEST_CATEGORY3).getValues().stream()
                            .map(DemographicValueResponse::getValue).collect(Collectors.toList()),
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

        demographicUserToSaveSelf.getDemographics().remove(TEST_CATEGORY1);
        assertUploadAndResponse(consentedUserInStudy.getUserId(), demographicUserToSaveSelf, getResult);

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
                getDemographicUsersAfterDeleteResult0.getDemographics().get(TEST_CATEGORY3).getValues().stream()
                        .map(DemographicValueResponse::getValue).collect(Collectors.toList()));
        assertEquals(demographicUserToSaveSelf.getDemographics().get(TEST_CATEGORY3).getUnits(),
                getDemographicUsersAfterDeleteResult0.getDemographics().get(TEST_CATEGORY3).getUnits());
    }

    private void assertValid(DemographicResponse response) {
        assertNotNull(response);
        for (DemographicValueResponse value : response.getValues()) {
            assertNull(value.getInvalidity());
        }
    }

    private void assertValidAtIndex(DemographicResponse response, int index) {
        assertNotNull(response);
        assertNull(response.getValues().get(index).getInvalidity());
    }

    private void assertInvalidAtIndex(DemographicResponse response, String expectedErrorMessage, int index) {
        assertNotNull(response);
        assertEquals(expectedErrorMessage, response.getValues().get(index).getInvalidity());
    }

    @Test
    public void valueValidation() throws IOException {
        // add random id that should be ignored
        DemographicValuesEnumValidationRules randomRules = new DemographicValuesEnumValidationRules();
        randomRules.put("en", ImmutableList.of("a", "b"));
        adminsApi.saveDemographicsValidationConfigAppLevel(IGNORED_CATEGORY, new DemographicValuesValidationConfig().validationType(ValidationTypeEnum.ENUM).validationRules(randomRules)).execute();

        // add validation for category2
        DemographicValuesEnumValidationRules category2Rules = new DemographicValuesEnumValidationRules();
        category2Rules.put("en", ImmutableList.of("foo", "bar"));
        adminsApi.saveDemographicsValidationConfigAppLevel(CATEGORY2, new DemographicValuesValidationConfig().validationType(ValidationTypeEnum.ENUM).validationRules(category2Rules)).execute();


        // validation is for category2 not category1, no validation exists for
        // category1, so should work
        DemographicUser demographicUserInvalid = new DemographicUser()
                .demographics(
                        ImmutableMap.of(
                                CATEGORY1,
                                new Demographic().values(ImmutableList.of("incorrect", "values"))));
        DemographicUserResponse response = adminsApi
                .saveDemographicUserAppLevel(consentedUserInStudy.getUserId(), demographicUserInvalid).execute().body();
        assertValid(response.getDemographics().get(CATEGORY1));

        // add validation for category1
        // spanish should be ignored
        DemographicValuesEnumValidationRules category1Rules = new DemographicValuesEnumValidationRules();
        category1Rules.put("en", ImmutableList.of("a", "bb", "7", "-6.3", "true", "xyz"));
        category1Rules.put("es", ImmutableList.of("this", "should", "be", "ignored"));
        adminsApi.saveDemographicsValidationConfigAppLevel(CATEGORY1, new DemographicValuesValidationConfig().validationType(ValidationTypeEnum.ENUM).validationRules(category1Rules)).execute();

        // retry invalid demographics with validation for category1 now added (should
        // not work)
        response = adminsApi.saveDemographicUserAppLevel(consentedUserInStudy.getUserId(), demographicUserInvalid)
                .execute().body();
        assertInvalidAtIndex(response.getDemographics().get(CATEGORY1), INVALID_ENUM_VALUE, 0);
        assertInvalidAtIndex(response.getDemographics().get(CATEGORY1), INVALID_ENUM_VALUE, 1);

        // retry with valid demographics
        // values are out of order, repeated, and in all types
        DemographicUser demographicUserValid = new DemographicUser()
                .demographics(
                        ImmutableMap.of(
                                CATEGORY1,
                                new Demographic()
                                        .values(ImmutableList.of(true, "bb", "xyz", "a", -6.3, 7, "a", "a", "a",
                                                "a"))));
        response = adminsApi.saveDemographicUserAppLevel(consentedUserInStudy.getUserId(), demographicUserValid)
                .execute().body();
        assertValid(response.getDemographics().get(CATEGORY1));

        // replace old version
        DemographicValuesEnumValidationRules category1RulesRev2 = new DemographicValuesEnumValidationRules();
        category1RulesRev2.put("en", ImmutableList.of("xyz"));
        adminsApi.saveDemographicsValidationConfigAppLevel(CATEGORY1, new DemographicValuesValidationConfig().validationType(ValidationTypeEnum.ENUM).validationRules(category1RulesRev2)).execute();

        // this SHOULD NOT work because validation should only use the most revision
        response = adminsApi.saveDemographicUserAppLevel(consentedUserInStudy.getUserId(), demographicUserValid)
                .execute().body();
        assertInvalidAtIndex(response.getDemographics().get(CATEGORY1), INVALID_ENUM_VALUE, 0);
        assertInvalidAtIndex(response.getDemographics().get(CATEGORY1), INVALID_ENUM_VALUE, 1);
        assertValidAtIndex(response.getDemographics().get(CATEGORY1), 2);
        assertInvalidAtIndex(response.getDemographics().get(CATEGORY1), INVALID_ENUM_VALUE, 3);
        assertInvalidAtIndex(response.getDemographics().get(CATEGORY1), INVALID_ENUM_VALUE, 4);
        assertInvalidAtIndex(response.getDemographics().get(CATEGORY1), INVALID_ENUM_VALUE, 5);
        assertInvalidAtIndex(response.getDemographics().get(CATEGORY1), INVALID_ENUM_VALUE, 6);
        assertInvalidAtIndex(response.getDemographics().get(CATEGORY1), INVALID_ENUM_VALUE, 7);
        assertInvalidAtIndex(response.getDemographics().get(CATEGORY1), INVALID_ENUM_VALUE, 8);
        assertInvalidAtIndex(response.getDemographics().get(CATEGORY1), INVALID_ENUM_VALUE, 9);

        // this SHOULD work because validation should only use the latest version
        DemographicUser demographicUserValidRev2 = new DemographicUser()
                .demographics(
                        ImmutableMap.of(
                                CATEGORY1,
                                new Demographic()
                                        .values(ImmutableList.of("xyz", "xyz"))));
        response = adminsApi.saveDemographicUserAppLevel(consentedUserInStudy.getUserId(), demographicUserValidRev2)
                .execute().body();
        assertValid(response.getDemographics().get(CATEGORY1));

        // no english
        DemographicValuesEnumValidationRules category4Rules = new DemographicValuesEnumValidationRules();
        category4Rules.put("es", ImmutableList.of());
        adminsApi.saveDemographicsValidationConfigAppLevel(CATEGORY4,new DemographicValuesValidationConfig().validationType(ValidationTypeEnum.ENUM).validationRules(category4Rules)).execute();


        // should not error even when english is not explicitly specified in
        // configuration
        DemographicUser demographicUserValidNoEnglish = new DemographicUser()
                .demographics(
                        ImmutableMap.of(
                                CATEGORY4,
                                new Demographic()
                                        .values(ImmutableList.of(true, "bb", "a", -6.3, 7, "a", "a", "a", "a"))));
        response = adminsApi
                .saveDemographicUserAppLevel(consentedUserInStudy.getUserId(), demographicUserValidNoEnglish)
                .execute().body();
        assertValid(response.getDemographics().get(CATEGORY4));

        // number range validation
        adminsApi.saveDemographicsValidationConfigAppLevel(CATEGORY3,new DemographicValuesValidationConfig().validationType(ValidationTypeEnum.NUMBER_RANGE).validationRules(new DemographicValuesNumberRangeValidationRules().min(0d).max(100d))).execute();

        // should work
        DemographicUser demographicUserValidNumber = new DemographicUser()
                .demographics(
                        ImmutableMap.of(
                                CATEGORY3,
                                new Demographic()
                                        .values(ImmutableList.of(99))));
        response = adminsApi
                .saveDemographicUserAppLevel(consentedUserInStudy.getUserId(), demographicUserValidNumber)
                .execute().body();
        assertValid(response.getDemographics().get(CATEGORY3));

        // should not work because not a number
        DemographicUser demographicUserStringsInNumberValidation = new DemographicUser()
                .demographics(
                        ImmutableMap.of(
                                CATEGORY3,
                                new Demographic()
                                        .values(ImmutableList.of("5", "xyz", "xyz"))));
        response = adminsApi
                .saveDemographicUserAppLevel(consentedUserInStudy.getUserId(),
                        demographicUserStringsInNumberValidation)
                .execute().body();
        assertValidAtIndex(response.getDemographics().get(CATEGORY3), 0);
        assertInvalidAtIndex(response.getDemographics().get(CATEGORY3), INVALID_NUMBER_VALUE_NOT_A_NUMBER, 1);
        assertInvalidAtIndex(response.getDemographics().get(CATEGORY3), INVALID_NUMBER_VALUE_NOT_A_NUMBER, 2);

        // should not work
        DemographicUser demographicUserInvalidNumber = new DemographicUser()
                .demographics(
                        ImmutableMap.of(
                                CATEGORY3,
                                new Demographic()
                                        .values(ImmutableList.of(5, -12.7, 100.6))));
        response = adminsApi.saveDemographicUserAppLevel(consentedUserInStudy.getUserId(), demographicUserInvalidNumber)
                .execute().body();
        assertValidAtIndex(response.getDemographics().get(CATEGORY3), 0);
        assertInvalidAtIndex(response.getDemographics().get(CATEGORY3), INVALID_NUMBER_VALUE_LESS_THAN_MIN, 1);
        assertInvalidAtIndex(response.getDemographics().get(CATEGORY3), INVALID_NUMBER_VALUE_GREATER_THAN_MAX, 2);

        // uploading for study should work even with invalid values because validation is for app-level
        response = researchersApi
                .saveDemographicUser(TEST_STUDY_ID, consentedUserInStudy.getUserId(), demographicUserInvalid)
                .execute().body();
        assertValid(response.getDemographics().get(CATEGORY1));
    }

    /**
     * Real-world test for NIH minimum categories. Valid and invalid sample for each
     * category.
     */
    @Test
    public void validateNIHCategories() throws IOException {
        // add validation
        adminsApi.saveDemographicsValidationConfigAppLevel(NIH_CATEGORY_YEAR_OF_BIRTH,
                new DemographicValuesValidationConfig()
                        .validationType(ValidationTypeEnum.NUMBER_RANGE)
                        .validationRules(new DemographicValuesNumberRangeValidationRules().min(1900.0)
                                .max(2050.0)))
                .execute();

        DemographicValuesEnumValidationRules biologicalSexEnumRules = new DemographicValuesEnumValidationRules();
        biologicalSexEnumRules.put("en",
                ImmutableList.of("Male", "Female", "Intersex", "None of these describe me", "Prefer not to answer"));
        adminsApi.saveDemographicsValidationConfigAppLevel(NIH_CATEGORY_BIOLOGICAL_SEX,
                new DemographicValuesValidationConfig()
                        .validationType(ValidationTypeEnum.ENUM)
                        .validationRules(biologicalSexEnumRules))
                .execute();

        DemographicValuesEnumValidationRules ethnicityEnumRules = new DemographicValuesEnumValidationRules();
        ethnicityEnumRules.put("en",
                ImmutableList.of("American Indian or Alaska Native", "Asian", "Black, African American, or African",
                        "Hispanic, Latino, or Spanish", "Middle Eastern or North African",
                        "Native Hawaiian or other Pacific Islander", "White", "None of these fully describe me",
                        "Prefer not to answer"));
        adminsApi.saveDemographicsValidationConfigAppLevel(NIH_CATEGORY_ETHNICITY,
        new DemographicValuesValidationConfig()
        .validationType(ValidationTypeEnum.ENUM)
        .validationRules(ethnicityEnumRules))
                .execute();

        DemographicValuesEnumValidationRules highestEducationRules = new DemographicValuesEnumValidationRules();
        highestEducationRules.put("en",
                ImmutableList.of("Never attended school or only attended kindergarten", "Grades 1 through 4 (Primary)",
                        "Grades 5 through 8 (Middle school)", "Grades 9 through 11 (Some high school)",
                        "Grade 12 or GED (High school graduate)",
                        "1 to 3 years after high school (Some college, Associate’s degree, or technical school)",
                        "College 4 years or more (College graduate)", "Advanced degree (Master’s, Doctorate, etc.)",
                        "Prefer not to answer"));
        adminsApi.saveDemographicsValidationConfigAppLevel(NIH_CATEGORY_HIGHEST_EDUCATION,
        new DemographicValuesValidationConfig().validationType(ValidationTypeEnum.ENUM)
                        .validationRules(highestEducationRules))
                .execute();

        // test responses
        // valid year of birth
        DemographicUser demographicUserValidBirthYear = new DemographicUser()
                .demographics(
                        ImmutableMap.of(
                                NIH_CATEGORY_YEAR_OF_BIRTH,
                                new Demographic().values(ImmutableList.of(2000))));
        DemographicUserResponse response = adminsApi
                .saveDemographicUserAppLevel(consentedUserInStudy.getUserId(), demographicUserValidBirthYear)
                .execute().body();
        assertValid(response.getDemographics().get(NIH_CATEGORY_YEAR_OF_BIRTH));
        // invalid year of birth
        DemographicUser demographicUserInvalidBirthYear1 = new DemographicUser()
                .demographics(
                        ImmutableMap.of(
                                NIH_CATEGORY_YEAR_OF_BIRTH,
                                new Demographic().values(ImmutableList.of(1899))));
        response = adminsApi
                .saveDemographicUserAppLevel(consentedUserInStudy.getUserId(), demographicUserInvalidBirthYear1)
                .execute().body();
        assertInvalidAtIndex(response.getDemographics().get(NIH_CATEGORY_YEAR_OF_BIRTH),
                INVALID_NUMBER_VALUE_LESS_THAN_MIN, 0);
        // invalid year of birth
        DemographicUser demographicUserInvalidBirthYear2 = new DemographicUser()
                .demographics(
                        ImmutableMap.of(
                                NIH_CATEGORY_YEAR_OF_BIRTH,
                                new Demographic().values(ImmutableList.of(2051))));
        response = adminsApi
                .saveDemographicUserAppLevel(consentedUserInStudy.getUserId(), demographicUserInvalidBirthYear2)
                .execute().body();
        assertInvalidAtIndex(response.getDemographics().get(NIH_CATEGORY_YEAR_OF_BIRTH),
                INVALID_NUMBER_VALUE_GREATER_THAN_MAX, 0);

        // valid sex
        DemographicUser demographicUserValidSex = new DemographicUser()
                .demographics(
                        ImmutableMap.of(
                                NIH_CATEGORY_BIOLOGICAL_SEX,
                                new Demographic().values(ImmutableList.of("None of these describe me"))));
        response = adminsApi.saveDemographicUserAppLevel(consentedUserInStudy.getUserId(), demographicUserValidSex)
                .execute().body();
        assertValid(response.getDemographics().get(NIH_CATEGORY_BIOLOGICAL_SEX));
        // invalid sex
        DemographicUser demographicUserValidInvalidSex = new DemographicUser()
                .demographics(
                        ImmutableMap.of(
                                NIH_CATEGORY_BIOLOGICAL_SEX,
                                new Demographic().values(ImmutableList.of("invalid value"))));
        response = adminsApi
                .saveDemographicUserAppLevel(consentedUserInStudy.getUserId(), demographicUserValidInvalidSex)
                .execute().body();
        assertInvalidAtIndex(response.getDemographics().get(NIH_CATEGORY_BIOLOGICAL_SEX), INVALID_ENUM_VALUE, 0);

        // valid ethnicity
        DemographicUser demographicUserValidEthnicity = new DemographicUser()
                .demographics(
                        ImmutableMap.of(
                                NIH_CATEGORY_ETHNICITY,
                                new Demographic().values(ImmutableList.of("Black, African American, or African",
                                        "Hispanic, Latino, or Spanish"))));
        response = adminsApi
                .saveDemographicUserAppLevel(consentedUserInStudy.getUserId(), demographicUserValidEthnicity)
                .execute().body();
        assertValid(response.getDemographics().get(NIH_CATEGORY_ETHNICITY));
        // invalid ethnicity
        DemographicUser demographicUserValidInvalidEthnicity = new DemographicUser()
                .demographics(
                        ImmutableMap.of(
                                NIH_CATEGORY_ETHNICITY,
                                new Demographic().values(ImmutableList.of("invalid value"))));
        response = adminsApi
                .saveDemographicUserAppLevel(consentedUserInStudy.getUserId(), demographicUserValidInvalidEthnicity)
                .execute().body();
        assertInvalidAtIndex(response.getDemographics().get(NIH_CATEGORY_ETHNICITY), INVALID_ENUM_VALUE, 0);

        // valid education
        DemographicUser demographicUserValidEducation = new DemographicUser()
                .demographics(
                        ImmutableMap.of(
                                NIH_CATEGORY_HIGHEST_EDUCATION,
                                new Demographic().values(ImmutableList.of(
                                        "1 to 3 years after high school (Some college, Associate’s degree, or technical school)"))));
        response = adminsApi
                .saveDemographicUserAppLevel(consentedUserInStudy.getUserId(), demographicUserValidEducation)
                .execute().body();
        assertValid(response.getDemographics().get(NIH_CATEGORY_HIGHEST_EDUCATION));
        // invalid education
        DemographicUser demographicUserValidInvalidEducation = new DemographicUser()
                .demographics(
                        ImmutableMap.of(
                                NIH_CATEGORY_HIGHEST_EDUCATION,
                                new Demographic().values(ImmutableList.of("invalid value"))));
        response = adminsApi
                .saveDemographicUserAppLevel(consentedUserInStudy.getUserId(), demographicUserValidInvalidEducation)
                .execute().body();
        assertInvalidAtIndex(response.getDemographics().get(NIH_CATEGORY_HIGHEST_EDUCATION), INVALID_ENUM_VALUE, 0);
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
