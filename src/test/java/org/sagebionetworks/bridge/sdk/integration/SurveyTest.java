package org.sagebionetworks.bridge.sdk.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.sagebionetworks.bridge.rest.model.CountryCode.US;
import static org.sagebionetworks.bridge.rest.model.DataType.BOOLEAN;
import static org.sagebionetworks.bridge.rest.model.DataType.DATE;
import static org.sagebionetworks.bridge.rest.model.DataType.DATETIME;
import static org.sagebionetworks.bridge.rest.model.DataType.DECIMAL;
import static org.sagebionetworks.bridge.rest.model.DataType.DURATION;
import static org.sagebionetworks.bridge.rest.model.DataType.INTEGER;
import static org.sagebionetworks.bridge.rest.model.DataType.POSTALCODE;
import static org.sagebionetworks.bridge.rest.model.DataType.STRING;
import static org.sagebionetworks.bridge.rest.model.DataType.TIME;
import static org.sagebionetworks.bridge.rest.model.DataType.YEAR;
import static org.sagebionetworks.bridge.rest.model.DataType.YEARMONTH;
import static org.sagebionetworks.bridge.rest.model.Role.DEVELOPER;
import static org.sagebionetworks.bridge.rest.model.Unit.GRAMS;
import static org.sagebionetworks.bridge.sdk.integration.TestSurvey.BLOODPRESSURE_ID;
import static org.sagebionetworks.bridge.sdk.integration.TestSurvey.BOOLEAN_ID;
import static org.sagebionetworks.bridge.sdk.integration.TestSurvey.DATETIME_EARLIEST_VALUE;
import static org.sagebionetworks.bridge.sdk.integration.TestSurvey.DATETIME_ID;
import static org.sagebionetworks.bridge.sdk.integration.TestSurvey.DATETIME_LATEST_VALUE;
import static org.sagebionetworks.bridge.sdk.integration.TestSurvey.DATE_ID;
import static org.sagebionetworks.bridge.sdk.integration.TestSurvey.DATE_QUESTION_EARLIEST_VALUE;
import static org.sagebionetworks.bridge.sdk.integration.TestSurvey.DATE_QUESTION_LATEST_VALUE;
import static org.sagebionetworks.bridge.sdk.integration.TestSurvey.DECIMAL_ID;
import static org.sagebionetworks.bridge.sdk.integration.TestSurvey.DECIMAL_QUESTION_MAX_VALUE;
import static org.sagebionetworks.bridge.sdk.integration.TestSurvey.DECIMAL_QUESTION_MIN_VALUE;
import static org.sagebionetworks.bridge.sdk.integration.TestSurvey.DECIMAL_QUESTION_STEP;
import static org.sagebionetworks.bridge.sdk.integration.TestSurvey.DURATION_ID;
import static org.sagebionetworks.bridge.sdk.integration.TestSurvey.DURATION_QUESTION_MAX_VALUE;
import static org.sagebionetworks.bridge.sdk.integration.TestSurvey.DURATION_QUESTION_MIN_VALUE;
import static org.sagebionetworks.bridge.sdk.integration.TestSurvey.DURATION_QUESTION_STEP;
import static org.sagebionetworks.bridge.sdk.integration.TestSurvey.DURATION_QUESTION_UNIT;
import static org.sagebionetworks.bridge.sdk.integration.TestSurvey.HEIGHT_ID;
import static org.sagebionetworks.bridge.sdk.integration.TestSurvey.IDENTIFIER_PREFIX;
import static org.sagebionetworks.bridge.sdk.integration.TestSurvey.INTEGER_ID;
import static org.sagebionetworks.bridge.sdk.integration.TestSurvey.INT_QUESTION_MAX_VALUE;
import static org.sagebionetworks.bridge.sdk.integration.TestSurvey.INT_QUESTION_MIN_VALUE;
import static org.sagebionetworks.bridge.sdk.integration.TestSurvey.INT_QUESTION_STEP;
import static org.sagebionetworks.bridge.sdk.integration.TestSurvey.INT_QUESTION_UNIT;
import static org.sagebionetworks.bridge.sdk.integration.TestSurvey.MULTIVALUE_ID;
import static org.sagebionetworks.bridge.sdk.integration.TestSurvey.STRING_ID;
import static org.sagebionetworks.bridge.sdk.integration.TestSurvey.STRING_QUESTION_ERROR_MESSAGE;
import static org.sagebionetworks.bridge.sdk.integration.TestSurvey.STRING_QUESTION_MAX_LENGTH;
import static org.sagebionetworks.bridge.sdk.integration.TestSurvey.STRING_QUESTION_MIN_LENGTH;
import static org.sagebionetworks.bridge.sdk.integration.TestSurvey.STRING_QUESTION_PATTERN;
import static org.sagebionetworks.bridge.sdk.integration.TestSurvey.STRING_QUESTION_PLACEHOLDER;
import static org.sagebionetworks.bridge.sdk.integration.TestSurvey.TIME_ID;
import static org.sagebionetworks.bridge.sdk.integration.TestSurvey.WEIGHT_ID;
import static org.sagebionetworks.bridge.sdk.integration.TestSurvey.YEARMONTH_ID;
import static org.sagebionetworks.bridge.sdk.integration.TestSurvey.YEARMONTH_QUESTION_EARLIEST_VALUE;
import static org.sagebionetworks.bridge.sdk.integration.TestSurvey.YEARMONTH_QUESTION_LATEST_VALUE;
import static org.sagebionetworks.bridge.sdk.integration.TestSurvey.YEAR_ID;
import static org.sagebionetworks.bridge.util.IntegTestUtils.SHARED_APP_ID;
import static org.sagebionetworks.bridge.util.IntegTestUtils.TEST_APP_ID;
import static org.sagebionetworks.bridge.sdk.integration.TestSurvey.POSTALCODE_ID;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import retrofit2.Call;

import org.apache.commons.lang3.RandomStringUtils;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.sagebionetworks.bridge.rest.api.ForAdminsApi;
import org.sagebionetworks.bridge.rest.api.ForConsentedUsersApi;
import org.sagebionetworks.bridge.rest.api.ForDevelopersApi;
import org.sagebionetworks.bridge.rest.api.ForSuperadminsApi;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.api.SchedulesV1Api;
import org.sagebionetworks.bridge.rest.api.SurveysApi;
import org.sagebionetworks.bridge.rest.exceptions.BadRequestException;
import org.sagebionetworks.bridge.rest.exceptions.ConstraintViolationException;
import org.sagebionetworks.bridge.rest.exceptions.EntityAlreadyExistsException;
import org.sagebionetworks.bridge.rest.exceptions.EntityNotFoundException;
import org.sagebionetworks.bridge.rest.exceptions.InvalidEntityException;
import org.sagebionetworks.bridge.rest.exceptions.PublishedSurveyException;
import org.sagebionetworks.bridge.rest.exceptions.UnauthorizedException;
import org.sagebionetworks.bridge.rest.model.Activity;
import org.sagebionetworks.bridge.rest.model.ActivityType;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.BloodPressureConstraints;
import org.sagebionetworks.bridge.rest.model.BooleanConstraints;
import org.sagebionetworks.bridge.rest.model.DataType;
import org.sagebionetworks.bridge.rest.model.DateConstraints;
import org.sagebionetworks.bridge.rest.model.DateTimeConstraints;
import org.sagebionetworks.bridge.rest.model.DecimalConstraints;
import org.sagebionetworks.bridge.rest.model.DurationConstraints;
import org.sagebionetworks.bridge.rest.model.GuidCreatedOnVersionHolder;
import org.sagebionetworks.bridge.rest.model.GuidVersionHolder;
import org.sagebionetworks.bridge.rest.model.HeightConstraints;
import org.sagebionetworks.bridge.rest.model.Image;
import org.sagebionetworks.bridge.rest.model.IntegerConstraints;
import org.sagebionetworks.bridge.rest.model.MultiValueConstraints;
import org.sagebionetworks.bridge.rest.model.Operator;
import org.sagebionetworks.bridge.rest.model.PostalCodeConstraints;
import org.sagebionetworks.bridge.rest.model.Role;
import org.sagebionetworks.bridge.rest.model.Schedule;
import org.sagebionetworks.bridge.rest.model.SchedulePlan;
import org.sagebionetworks.bridge.rest.model.ScheduleType;
import org.sagebionetworks.bridge.rest.model.SharedModuleMetadata;
import org.sagebionetworks.bridge.rest.model.SimpleScheduleStrategy;
import org.sagebionetworks.bridge.rest.model.StringConstraints;
import org.sagebionetworks.bridge.rest.model.Survey;
import org.sagebionetworks.bridge.rest.model.SurveyElement;
import org.sagebionetworks.bridge.rest.model.SurveyInfoScreen;
import org.sagebionetworks.bridge.rest.model.SurveyList;
import org.sagebionetworks.bridge.rest.model.SurveyQuestion;
import org.sagebionetworks.bridge.rest.model.SurveyQuestionOption;
import org.sagebionetworks.bridge.rest.model.SurveyReference;
import org.sagebionetworks.bridge.rest.model.SurveyRule;
import org.sagebionetworks.bridge.rest.model.TimeConstraints;
import org.sagebionetworks.bridge.rest.model.UIHint;
import org.sagebionetworks.bridge.rest.model.Unit;
import org.sagebionetworks.bridge.rest.model.WeightConstraints;
import org.sagebionetworks.bridge.rest.model.YearConstraints;
import org.sagebionetworks.bridge.rest.model.YearMonthConstraints;
import org.sagebionetworks.bridge.user.TestUserHelper;
import org.sagebionetworks.bridge.user.TestUserHelper.TestUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({ "ConstantConditions", "Guava" })
public class SurveyTest {
    private static final Logger LOG = LoggerFactory.getLogger(SurveyTest.class);
    
    private static final String SURVEY_NAME = "dummy-survey-name";

    private static TestUser developer;
    private static TestUser user;
    private static TestUser worker;
    private static TestUser sharedDeveloper;
    private static ForDevelopersApi sharedDeveloperModulesApi;
    private static SurveysApi sharedSurveysApi;
    private static ForAdminsApi adminsApi;
    private static ForSuperadminsApi superadminsApi;

    private String surveyId;

    // We use SimpleGuidCreatedOnVersionHolder, because we need to use an immutable version holder, to ensure we're
    // deleting the correct surveys.
    private Set<GuidCreatedOnVersionHolder> surveysToDelete;

    @BeforeClass
    public static void beforeClass() throws Exception {
        TestUser admin = TestUserHelper.getSignedInAdmin();
        adminsApi = admin.getClient(ForAdminsApi.class);
        superadminsApi = admin.getClient(ForSuperadminsApi.class);
        developer = TestUserHelper.createAndSignInUser(SurveyTest.class, false, Role.DEVELOPER);
        user = TestUserHelper.createAndSignInUser(SurveyTest.class, true);
        worker = TestUserHelper.createAndSignInUser(SurveyTest.class, false, Role.WORKER);

        sharedDeveloper = TestUserHelper.createAndSignInUser(SurveyTest.class, SHARED_APP_ID, DEVELOPER);        
        sharedDeveloperModulesApi = sharedDeveloper.getClient(ForDevelopersApi.class);
        sharedSurveysApi = sharedDeveloper.getClient(SurveysApi.class);
    }

    @Before
    public void before() {
        surveyId = Tests.randomIdentifier(getClass());
        surveysToDelete = new HashSet<>();
    }

    @SuppressWarnings("deprecation")
    @After
    public void after() throws Exception {
        // cleanup surveys
        TestUser admin = TestUserHelper.getSignedInAdmin();
        
        SurveysApi surveysApi = admin.getClient(SurveysApi.class);
        for (GuidCreatedOnVersionHolder oneSurvey : surveysToDelete) {
            try {
                surveysApi.deleteSurvey(oneSurvey.getGuid(), oneSurvey.getCreatedOn(), true).execute();    
            } catch (RuntimeException ex) {
                LOG.error("Error deleting survey=" + oneSurvey + ": " + ex.getMessage(), ex);
            }
        }
    }

    @AfterClass
    public static void deleteDeveloper() throws Exception {
        if (developer != null) {
            developer.signOutAndDeleteUser();
        }
    }

    @AfterClass
    public static void deleteUser() throws Exception {
        if (user != null) {
            user.signOutAndDeleteUser();
        }
    }

    @AfterClass
    public static void deleteWorker() throws Exception {
        if (worker != null) {
            worker.signOutAndDeleteUser();
        }
    }
    
    @AfterClass
    public static void deleteSharedDeveloper() throws Exception {
        if (sharedDeveloper != null) {
            sharedDeveloper.signOutAndDeleteUser();
        }
    }
    
    @Test
    public void allowPastTrueByDefaultInSDK() {
        DateTimeConstraints dtc = new DateTimeConstraints();
        assertTrue(dtc.isAllowPast());
        YearMonthConstraints ymc = new YearMonthConstraints();
        assertTrue(ymc.isAllowPast());
        DateConstraints dc = new DateConstraints();
        assertTrue(dc.isAllowPast());
        YearConstraints yc = new YearConstraints();
        assertTrue(yc.isAllowPast());
    }
    
    // One test to verify that all the fields of the test survey can be persisted and retrieved.
    // Info screen is still tested separately. 
    @SuppressWarnings("deprecation")
    @Test
    public void canRountripSurvey() throws Exception {
        SurveysApi surveysApi = developer.getClient(SurveysApi.class);
        
        GuidCreatedOnVersionHolder key = createSurvey(surveysApi, TestSurvey.getSurvey(SurveyTest.class));
        
        Survey survey = surveysApi.getSurvey(key.getGuid(), key.getCreatedOn()).execute().body();
        
        // Boolean question
        SurveyQuestion booleanQuestion = getQuestion(survey, BOOLEAN_ID);
        assertEquals("SurveyQuestion", booleanQuestion.getType());
        BooleanConstraints booleanConstraints = (BooleanConstraints)booleanQuestion.getConstraints();
        assertEquals("BooleanConstraints", booleanConstraints.getType());
        assertEquals(BOOLEAN, booleanConstraints.getDataType());

        // Date question
        SurveyQuestion dateQuestion = getQuestion(survey, DATE_ID);
        DateConstraints dateConstraints = (DateConstraints)dateQuestion.getConstraints();
        assertEquals(DATE, dateConstraints.getDataType());
        assertFalse(dateConstraints.isAllowPast());
        assertTrue(dateConstraints.isAllowFuture());
        assertEquals("DateConstraints", dateConstraints.getType());
        assertEquals(DATE_QUESTION_EARLIEST_VALUE, dateConstraints.getEarliestValue());
        assertEquals(DATE_QUESTION_LATEST_VALUE, dateConstraints.getLatestValue());
        
        // DateTime question
        SurveyQuestion dateTimeQuestion = getQuestion(survey, DATETIME_ID);
        DateTimeConstraints dateTimeConstraints = (DateTimeConstraints)dateTimeQuestion.getConstraints();
        assertEquals(DATETIME, dateTimeConstraints.getDataType());
        assertEquals("DateTimeConstraints", dateTimeConstraints.getType());
        assertFalse(dateTimeConstraints.isAllowPast());
        assertTrue(dateTimeConstraints.isAllowFuture());
        assertEquals(DATETIME_EARLIEST_VALUE.toString(), dateTimeConstraints.getEarliestValue().toString());
        assertEquals(DATETIME_LATEST_VALUE.toString(), dateTimeConstraints.getLatestValue().toString());
        
        // Decimal question
        SurveyQuestion decimalQuestion = getQuestion(survey, DECIMAL_ID);
        DecimalConstraints decimalConstraints = (DecimalConstraints)decimalQuestion.getConstraints();
        assertEquals(DECIMAL, decimalConstraints.getDataType());
        assertEquals("DecimalConstraints", decimalConstraints.getType());
        assertEquals(DECIMAL_QUESTION_MIN_VALUE, decimalConstraints.getMinValue());
        assertEquals(DECIMAL_QUESTION_MAX_VALUE, decimalConstraints.getMaxValue());
        assertEquals(DECIMAL_QUESTION_STEP, decimalConstraints.getStep());
        assertEquals(GRAMS, decimalConstraints.getUnit());
        
        // Integer question
        SurveyQuestion intQuestion = getQuestion(survey, INTEGER_ID);
        IntegerConstraints intConstraints = (IntegerConstraints)intQuestion.getConstraints();
        assertEquals(INTEGER, intConstraints.getDataType());
        assertEquals("IntegerConstraints", intConstraints.getType());
        assertEquals(INT_QUESTION_MIN_VALUE, intConstraints.getMinValue());
        assertEquals(INT_QUESTION_MAX_VALUE, intConstraints.getMaxValue());
        assertEquals(INT_QUESTION_STEP, intConstraints.getStep());
        assertEquals(INT_QUESTION_UNIT, intConstraints.getUnit());
        
        // Duration question
        SurveyQuestion durationQuestion = getQuestion(survey, DURATION_ID);
        DurationConstraints durationConstraints = (DurationConstraints)durationQuestion.getConstraints();
        assertEquals(DURATION, durationConstraints.getDataType());
        assertEquals("DurationConstraints", durationConstraints.getType());
        assertEquals(DURATION_QUESTION_MIN_VALUE, durationConstraints.getMinValue());
        assertEquals(DURATION_QUESTION_MAX_VALUE, durationConstraints.getMaxValue());
        assertEquals(DURATION_QUESTION_STEP, durationConstraints.getStep());
        assertEquals(DURATION_QUESTION_UNIT, durationConstraints.getUnit());
        
        // Time question
        SurveyQuestion timeQuestion = getQuestion(survey, TIME_ID);
        TimeConstraints timeConstraints = (TimeConstraints)timeQuestion.getConstraints();
        assertEquals(TIME, timeConstraints.getDataType());
        assertEquals("TimeConstraints", timeConstraints.getType());
        
        // MultiValue question
        SurveyQuestion multiValueQuestion = getQuestion(survey, MULTIVALUE_ID);
        MultiValueConstraints multiValueConstraints = (MultiValueConstraints)multiValueQuestion.getConstraints();
        assertEquals(STRING, multiValueConstraints.getDataType());
        assertTrue(multiValueConstraints.isRequired());        
        assertEquals("MultiValueConstraints", multiValueConstraints.getType());
        List<SurveyQuestionOption> options = multiValueConstraints.getEnumeration();
        assertEquals(6, options.size());

        SurveyQuestionOption option = options.get(5);
        assertEquals("None of the above", option.getLabel());
        assertEquals("nota", option.getDetail());
        assertEquals("0", option.getValue());
        assertEquals("http://terrible.svg", option.getImage().getSource());
        assertEquals(new Integer(600), option.getImage().getWidth());
        assertEquals(new Integer(300), option.getImage().getHeight());
        assertTrue(option.isExclusive());
        
        // String question
        SurveyQuestion stringQuestion = getQuestion(survey, STRING_ID);
        StringConstraints stringConstraints = (StringConstraints)stringQuestion.getConstraints();
        assertEquals(STRING, stringConstraints.getDataType());
        assertEquals("StringConstraints", stringConstraints.getType());
        assertEquals(STRING_QUESTION_MIN_LENGTH, stringConstraints.getMinLength());
        assertEquals(STRING_QUESTION_MAX_LENGTH, stringConstraints.getMaxLength());
        assertEquals(STRING_QUESTION_PATTERN, stringConstraints.getPattern());
        assertEquals(STRING_QUESTION_ERROR_MESSAGE, stringConstraints.getPatternErrorMessage());
        assertEquals(STRING_QUESTION_PLACEHOLDER, stringConstraints.getPatternPlaceholder());
        
        // Blood pressure question
        SurveyQuestion bloodPressureQuestion = getQuestion(survey, BLOODPRESSURE_ID);
        BloodPressureConstraints bloodPressureConstraints = (BloodPressureConstraints)bloodPressureQuestion.getConstraints();
        assertEquals(DataType.BLOODPRESSURE, bloodPressureConstraints.getDataType());
        assertEquals("BloodPressureConstraints", bloodPressureConstraints.getType());
        assertEquals(Unit.CUBIC_CENTIMETERS, bloodPressureConstraints.getUnit());
        
        // Height question
        SurveyQuestion heightQuestion = getQuestion(survey, HEIGHT_ID);
        HeightConstraints heightConstraints = (HeightConstraints)heightQuestion.getConstraints();
        assertEquals(DataType.HEIGHT, heightConstraints.getDataType());
        assertEquals("HeightConstraints", heightConstraints.getType());
        assertEquals(Unit.CENTIMETERS, heightConstraints.getUnit());
        assertEquals(true, heightConstraints.isForInfant());
        
        // Weight question
        SurveyQuestion weightQuestion = getQuestion(survey, WEIGHT_ID);
        WeightConstraints weightConstraints = (WeightConstraints)weightQuestion.getConstraints();
        assertEquals(DataType.WEIGHT, weightConstraints.getDataType());
        assertEquals("WeightConstraints", weightConstraints.getType());
        assertEquals(Unit.KILOGRAMS, weightConstraints.getUnit());
        assertEquals(true, weightConstraints.isForInfant());
        
        // YearMonth question
        SurveyQuestion yearMonthQuestion = getQuestion(survey, YEARMONTH_ID);
        YearMonthConstraints yearMonthConstraints = (YearMonthConstraints)yearMonthQuestion.getConstraints();
        assertEquals(YEARMONTH, yearMonthConstraints.getDataType());
        assertFalse(yearMonthConstraints.isAllowPast());
        assertTrue(yearMonthConstraints.isAllowFuture());
        assertEquals("YearMonthConstraints", yearMonthConstraints.getType());
        assertEquals(YEARMONTH_QUESTION_EARLIEST_VALUE, yearMonthConstraints.getEarliestValue());
        assertEquals(YEARMONTH_QUESTION_LATEST_VALUE, yearMonthConstraints.getLatestValue());
        
        // Postal Code question
        SurveyQuestion postalCodeQuestion = getQuestion(survey, POSTALCODE_ID);
        PostalCodeConstraints postalCodeConstraints = (PostalCodeConstraints)postalCodeQuestion.getConstraints();
        assertEquals(POSTALCODE, postalCodeConstraints.getDataType());
        assertEquals("PostalCodeConstraints", postalCodeConstraints.getType());
        assertEquals(US, postalCodeConstraints.getCountryCode());
        assertEquals(17, postalCodeConstraints.getSparseZipCodePrefixes().size());
        assertEquals("036", postalCodeConstraints.getSparseZipCodePrefixes().get(0));

        // Year question
        SurveyQuestion yearQuestion = getQuestion(survey, YEAR_ID);
        YearConstraints yearConstraints = (YearConstraints)yearQuestion.getConstraints();
        assertEquals(YEAR, yearConstraints.getDataType());
        assertEquals("YearConstraints", yearConstraints.getType());
        assertEquals(UIHint.YEAR, yearQuestion.getUiHint());
        assertFalse(yearConstraints.isAllowPast());
        assertTrue(yearConstraints.isAllowFuture());
        assertEquals("2000", yearConstraints.getEarliestValue());
        assertEquals("2020", yearConstraints.getLatestValue());
    }
    
    private SurveyQuestion getQuestion(Survey survey, String questionId) {
        for (SurveyElement element : survey.getElements()) {
            if (element.getIdentifier().equals(questionId)) {
                return (SurveyQuestion)element;
            }
        }
        return null; 
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testPermanentDeleteWithSharedModule() throws Exception {
        // create test survey and test shared module
        String moduleId = "integ-test-module-delete" + RandomStringUtils.randomAlphabetic(4);

        Survey survey = new Survey().name(SURVEY_NAME).identifier(surveyId);
        GuidCreatedOnVersionHolder retSurvey = sharedSurveysApi.createSurvey(survey).execute().body();

        SharedModuleMetadata metadataToCreate = new SharedModuleMetadata().id(moduleId).version(0)
                .name("Integ Test Schema").surveyCreatedOn(retSurvey.getCreatedOn().toString()).surveyGuid(retSurvey.getGuid());
        sharedDeveloperModulesApi.createMetadata(metadataToCreate).execute()
                .body();

        // execute delete
        Exception thrownEx = null;
        try {
            superadminsApi.adminChangeApp(Tests.SHARED_SIGNIN).execute();
            adminsApi.deleteSurvey(retSurvey.getGuid(), retSurvey.getCreatedOn(), true).execute();
            fail("expected exception");
        } catch (BadRequestException e) {
            thrownEx = e;
        } finally {
            // finally delete shared module and uploaded schema
            adminsApi.deleteMetadataByIdAllVersions(moduleId, true).execute();
            adminsApi.deleteSurvey(retSurvey.getGuid(), retSurvey.getCreatedOn(), true).execute();
            superadminsApi.adminChangeApp(Tests.API_SIGNIN).execute();
        }
        assertNotNull(thrownEx);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testPermanentDeleteWithSharedModuleWithIdentifier() throws Exception {
        // create test survey and test shared module
        String moduleId = "integ-test-module-delete" + RandomStringUtils.randomAlphabetic(4);

        Survey survey = new Survey().name(SURVEY_NAME).identifier(surveyId);
        GuidCreatedOnVersionHolder retSurvey = sharedSurveysApi.createSurvey(survey).execute().body();

        // Verify the index is up-to-date.
        Tests.retryHelper(() -> sharedSurveysApi.getSurvey(IDENTIFIER_PREFIX+survey.getIdentifier(),
                retSurvey.getCreatedOn()).execute().body(),
                Predicates.alwaysTrue());

        SharedModuleMetadata metadataToCreate = new SharedModuleMetadata().id(moduleId).version(0)
                .name("Integ Test Schema").surveyCreatedOn(retSurvey.getCreatedOn().toString()).surveyGuid(retSurvey.getGuid());
        sharedDeveloperModulesApi.createMetadata(metadataToCreate).execute()
                .body();

        // execute delete
        Exception thrownEx = null;
        try {
            superadminsApi.adminChangeApp(Tests.SHARED_SIGNIN).execute();
            adminsApi.deleteSurvey(IDENTIFIER_PREFIX+survey.getIdentifier(), retSurvey.getCreatedOn(), true).execute();
            fail("expected exception");
        } catch (BadRequestException e) {
            thrownEx = e;
        } finally {
            // finally delete shared module and uploaded schema
            adminsApi.deleteMetadataByIdAllVersions(moduleId, true).execute();
            adminsApi.deleteSurvey(IDENTIFIER_PREFIX+survey.getIdentifier(), retSurvey.getCreatedOn(), true).execute();
            superadminsApi.adminChangeApp(Tests.API_SIGNIN).execute();
        }
        assertNotNull(thrownEx);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testVirtualDeleteWithSharedModule() throws Exception {
        // create test survey and test shared module
        String moduleId = "integ-test-module-delete" + RandomStringUtils.randomAlphabetic(4);

        Survey survey = new Survey().name(SURVEY_NAME).identifier(surveyId);
        GuidCreatedOnVersionHolder retSurvey = sharedSurveysApi.createSurvey(survey).execute().body();

        SharedModuleMetadata metadataToCreate = new SharedModuleMetadata().id(moduleId).version(0)
                .name("Integ Test Schema").surveyCreatedOn(retSurvey.getCreatedOn().toString()).surveyGuid(retSurvey.getGuid());
        sharedDeveloperModulesApi.createMetadata(metadataToCreate).execute()
                .body();

        // execute delete
        Exception thrownEx = null;
        try {
            sharedSurveysApi.deleteSurvey(retSurvey.getGuid(), retSurvey.getCreatedOn(), false).execute();
            fail("expected exception");
        } catch (BadRequestException e) {
            thrownEx = e;
        } finally {
            // finally delete shared module and uploaded schema
            superadminsApi.adminChangeApp(Tests.SHARED_SIGNIN).execute();
            adminsApi.deleteMetadataByIdAllVersions(moduleId, true).execute();
            adminsApi.deleteSurvey(retSurvey.getGuid(), retSurvey.getCreatedOn(), true).execute();
            superadminsApi.adminChangeApp(Tests.API_SIGNIN).execute();
        }
        assertNotNull(thrownEx);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testVirtualDeleteWithSharedModuleWithIdentifer() throws Exception {
        // create test survey and test shared module
        String moduleId = "integ-test-module-delete" + RandomStringUtils.randomAlphabetic(4);

        Survey survey = new Survey().name(SURVEY_NAME).identifier(surveyId);
        GuidCreatedOnVersionHolder retSurvey = sharedSurveysApi.createSurvey(survey).execute().body();

        // Verify the index is up-to-date.
        Tests.retryHelper(() -> sharedSurveysApi.getSurvey(IDENTIFIER_PREFIX+survey.getIdentifier(),
                retSurvey.getCreatedOn()).execute().body(),
                Predicates.alwaysTrue());

        SharedModuleMetadata metadataToCreate = new SharedModuleMetadata().id(moduleId).version(0)
                .name("Integ Test Schema").surveyCreatedOn(retSurvey.getCreatedOn().toString()).surveyGuid(retSurvey.getGuid());
        sharedDeveloperModulesApi.createMetadata(metadataToCreate).execute()
                .body();

        // execute delete
        Exception thrownEx = null;
        try {
            sharedSurveysApi.deleteSurvey(IDENTIFIER_PREFIX+survey.getIdentifier(), retSurvey.getCreatedOn(), false).execute();
            fail("expected exception");
        } catch (BadRequestException e) {
            thrownEx = e;
        } finally {
            // finally delete shared module and uploaded schema
            superadminsApi.adminChangeApp(Tests.SHARED_SIGNIN).execute();
            adminsApi.deleteMetadataByIdAllVersions(moduleId, true).execute();
            adminsApi.deleteSurvey(IDENTIFIER_PREFIX+survey.getIdentifier(), retSurvey.getCreatedOn(), true).execute();
            superadminsApi.adminChangeApp(Tests.API_SIGNIN).execute();
        }
        assertNotNull(thrownEx);
    }

    @Test
    public void cannotCreateSurveyWithDuplicateId() throws Exception {
        SurveysApi surveysApi = developer.getClient(SurveysApi.class);

        // Create survey. This succeeds.
        Survey survey = new Survey().name(SURVEY_NAME).identifier(surveyId);
        createSurvey(surveysApi, survey);

        // Create another identical survey with the same ID. This fails.
        Survey survey2 = new Survey().name(SURVEY_NAME).identifier(surveyId);
        try {
            createSurvey(surveysApi, survey2);
            fail("expected exception");
        } catch (EntityAlreadyExistsException ex) {
            assertTrue(ex.getMessage().contains("Survey identifier " + surveyId + " is already used by survey"));
        }
    }

    @SuppressWarnings("deprecation")
    @Test(expected=UnauthorizedException.class)
    public void cannotSubmitAsNormalUser() throws Exception {
        user.getClient(SurveysApi.class).getMostRecentSurveys(false).execute().body();
    }

    @SuppressWarnings("deprecation")
    @Test
    public void cannotUpdateSurveyId() throws Exception {
        SurveysApi surveysApi = developer.getClient(SurveysApi.class);

        // Create survey.
        Survey survey = new Survey().name(SURVEY_NAME).identifier(surveyId);
        GuidCreatedOnVersionHolder keys = createSurvey(surveysApi, survey);

        // Attempt to update the survey ID.
        survey = surveysApi.getSurvey(keys.getGuid(), keys.getCreatedOn()).execute().body();
        survey.setIdentifier(surveyId + "-2");
        surveysApi.updateSurvey(keys.getGuid(), keys.getCreatedOn(), survey).execute();

        // Survey ID remains unchanged.
        survey = surveysApi.getSurvey(keys.getGuid(), keys.getCreatedOn()).execute().body();
        assertEquals(surveyId, survey.getIdentifier());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void cannotUpdateSurveyIdWithIdentifier() throws Exception {
        SurveysApi surveysApi = developer.getClient(SurveysApi.class);

        // Create survey.
        Survey survey = new Survey().name(SURVEY_NAME).identifier(surveyId);
        GuidCreatedOnVersionHolder keys = createSurveyWithIdentifier(surveysApi, survey);

        // Attempt to update the survey ID.
        survey = surveysApi.getSurvey(keys.getGuid(), keys.getCreatedOn()).execute().body();
        assertEquals(keys.getGuid(), IDENTIFIER_PREFIX + survey.getIdentifier());
        
        survey.setIdentifier(surveyId + "-2");
        surveysApi.updateSurvey(keys.getGuid(), keys.getCreatedOn(), survey).execute();

        // Survey ID remains unchanged.
        survey = surveysApi.getSurvey(keys.getGuid(), keys.getCreatedOn()).execute().body();
        assertEquals(surveyId, survey.getIdentifier());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void saveAndRetrieveSurvey() throws Exception {
        SurveysApi surveysApi = developer.getClient(SurveysApi.class);
        GuidCreatedOnVersionHolder key = createSurvey(surveysApi, TestSurvey.getSurvey(SurveyTest.class));
        Survey survey = surveysApi.getSurvey(key.getGuid(), key.getCreatedOn()).execute().body();

        List<SurveyElement> questions = survey.getElements();
        String prompt = ((SurveyQuestion)questions.get(1)).getPrompt();
        assertEquals("Prompt is correct.", "When did you last have a medical check-up?", prompt);
        surveysApi.publishSurvey(key.getGuid(), key.getCreatedOn(), false).execute();
        
        ForConsentedUsersApi usersApi = user.getClient(ForConsentedUsersApi.class);
        survey = usersApi.getPublishedSurveyVersion(key.getGuid()).execute().body();
        // And again, correct
        questions = survey.getElements();
        prompt = ((SurveyQuestion)questions.get(1)).getPrompt();
        assertEquals("Prompt is correct.", "When did you last have a medical check-up?", prompt);

        // Check optional parameters.
        assertEquals(TestSurvey.COPYRIGHT_NOTICE, survey.getCopyrightNotice());
        assertEquals(TestSurvey.MODULE_ID, survey.getModuleId());
        assertEquals(TestSurvey.MODULE_VERSION, survey.getModuleVersion().intValue());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void saveAndRetrieveSurveyWithIdentifier() throws Exception {
        SurveysApi surveysApi = developer.getClient(SurveysApi.class);
        GuidCreatedOnVersionHolder keys = createSurveyWithIdentifier(surveysApi, TestSurvey.getSurvey(SurveyTest.class));

        Survey survey = surveysApi.getSurvey(keys.getGuid(), keys.getCreatedOn()).execute().body();

        List<SurveyElement> questions = survey.getElements();
        String prompt = ((SurveyQuestion)questions.get(1)).getPrompt();
        assertEquals("Prompt is correct.", "When did you last have a medical check-up?", prompt);
        surveysApi.publishSurvey(keys.getGuid(), keys.getCreatedOn(), false).execute();

        ForConsentedUsersApi usersApi = user.getClient(ForConsentedUsersApi.class);
        survey = Tests.retryHelper(() -> usersApi.getPublishedSurveyVersion(keys.getGuid()).execute().body(),
                s -> keys.getGuid().equals(IDENTIFIER_PREFIX+s.getIdentifier()));

        // And again, correct
        questions = survey.getElements();
        prompt = ((SurveyQuestion)questions.get(1)).getPrompt();
        assertEquals("Prompt is correct.", "When did you last have a medical check-up?", prompt);

        // Check optional parameters.
        assertEquals(TestSurvey.COPYRIGHT_NOTICE, survey.getCopyrightNotice());
        assertEquals(TestSurvey.MODULE_ID, survey.getModuleId());
        assertEquals(TestSurvey.MODULE_VERSION, survey.getModuleVersion().intValue());
    }
    
    @SuppressWarnings("deprecation")
    @Test
    public void createVersionPublish() throws Exception {
        SurveysApi surveysApi = developer.getClient(SurveysApi.class);

        Survey survey = TestSurvey.getSurvey(SurveyTest.class);
        assertNull(survey.getGuid());
        assertNull(survey.getVersion());
        assertNull(survey.getCreatedOn());
        GuidCreatedOnVersionHolder key = createSurvey(surveysApi, survey);
        assertNotNull(key.getGuid());
        assertNotNull(key.getVersion());
        assertNotNull(key.getCreatedOn());
        
        GuidCreatedOnVersionHolder laterKey = versionSurvey(surveysApi, key);
        assertNotEquals("Version has been updated.", key.getCreatedOn(), laterKey.getCreatedOn());

        survey = surveysApi.getSurvey(laterKey.getGuid(), laterKey.getCreatedOn()).execute().body();
        assertFalse("survey is not published.", survey.isPublished());

        surveysApi.publishSurvey(survey.getGuid(), survey.getCreatedOn(), false).execute();
        survey = surveysApi.getSurvey(survey.getGuid(), survey.getCreatedOn()).execute().body();
        assertTrue("survey is now published.", survey.isPublished());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void createVersionPublishWithIdentifier() throws Exception {
        SurveysApi surveysApi = developer.getClient(SurveysApi.class);

        Survey survey = TestSurvey.getSurvey(SurveyTest.class);
        assertNull(survey.getGuid());
        assertNull(survey.getVersion());
        assertNull(survey.getCreatedOn());
        GuidCreatedOnVersionHolder key = createSurveyWithIdentifier(surveysApi, survey);
        assertNotNull(key.getGuid());
        assertNotNull(key.getVersion());
        assertNotNull(key.getCreatedOn());

        GuidCreatedOnVersionHolder laterKey = versionSurvey(surveysApi, key);
        assertNotEquals("Version has been updated.", key.getCreatedOn(), laterKey.getCreatedOn());

        survey = surveysApi.getSurvey(key.getGuid(), laterKey.getCreatedOn()).execute().body();
        assertFalse("survey is not published.", survey.isPublished());

        surveysApi.publishSurvey(key.getGuid(), survey.getCreatedOn(), false).execute();

        DateTime expectedCreatedOn = survey.getCreatedOn();
        survey = Tests.retryHelper(() -> surveysApi.getSurvey(key.getGuid(), expectedCreatedOn).execute().body(),
                Survey::isPublished);
        assertEquals(key.getGuid(), IDENTIFIER_PREFIX+survey.getIdentifier());
        assertTrue("survey is now published.", survey.isPublished());
    }

    @SuppressWarnings("deprecation")
    @Test(expected = InvalidEntityException.class)
    public void createInvalidSurveyReturns400() throws Exception {
        // This should seem obvious. However, there was a previous bug in BridgePF where the 400 invalid survey
        // exception would be masked by an obscure 500 NullPointerException. The only codepath where this happens is
        // is creating surveys with invalid survey elements, which goes through the SurveyElementFactory.
        //
        // The easiest way to repro this bug is to create a survey question with a constraint without a DataType.
        // The bug would cause a 500 NPE. When fixed, it will produce a 400 InvalidEntityException with all the
        // validation messages.

        Survey survey = new Survey().addElementsItem(new SurveyQuestion().constraints(new IntegerConstraints()));
        SurveysApi surveysApi = developer.getClient(SurveysApi.class);
        surveysApi.createSurvey(survey).execute();
    }

    @SuppressWarnings("deprecation")
    @Test
    public void getAllVersionsOfASurvey() throws Exception {
        SurveysApi surveysApi = developer.getClient(SurveysApi.class);

        GuidCreatedOnVersionHolder key = createSurvey(surveysApi, TestSurvey.getSurvey(SurveyTest.class));
        key = versionSurvey(surveysApi, key);
        
        SurveyList surveyList = surveysApi.getAllVersionsOfSurvey(key.getGuid(), false).execute().body();
        int count = surveyList.getItems().size();
        assertEquals("Two versions for this survey.", 2, count);
        
        // verify includeDeleted
        Survey oneVersion = surveyList.getItems().get(0); 
        surveysApi.deleteSurvey(oneVersion.getGuid(), oneVersion.getCreatedOn(), false).execute();
        
        anyDeleted(surveysApi.getAllVersionsOfSurvey(key.getGuid(), true));
        noneDeleted(surveysApi.getAllVersionsOfSurvey(key.getGuid(), false));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void getAllVersionsOfASurveyWithIdentifier() throws Exception {
        SurveysApi surveysApi = developer.getClient(SurveysApi.class);

        GuidCreatedOnVersionHolder key = createSurveyWithIdentifier(surveysApi, TestSurvey.getSurvey(SurveyTest.class));
        String prefix = key.getGuid();

        versionSurvey(surveysApi, key);

        SurveyList surveyList = surveysApi.getAllVersionsOfSurvey(prefix, false).execute().body();
        int count = surveyList.getItems().size();
        assertEquals("Two versions for this survey.", 2, count);
        
        // verify includeDeleted
        Survey oneVersion = surveyList.getItems().get(0);
        assertEquals(prefix, IDENTIFIER_PREFIX + oneVersion.getIdentifier());
        
        surveysApi.deleteSurvey(prefix, oneVersion.getCreatedOn(), false).execute();

        anyDeleted(surveysApi.getAllVersionsOfSurvey(prefix, true));
        noneDeleted(surveysApi.getAllVersionsOfSurvey(prefix, false));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void canGetMostRecentOrRecentlyPublishedSurvey() throws Exception {
        SurveysApi surveysApi = developer.getClient(SurveysApi.class);

        GuidCreatedOnVersionHolder key = createSurvey(surveysApi, TestSurvey.getSurvey(SurveyTest.class));
        key = versionSurvey(surveysApi, key);
        key = versionSurvey(surveysApi, key);

        GuidCreatedOnVersionHolder key1 = createSurvey(surveysApi, TestSurvey.getSurvey(SurveyTest.class));
        key1 = versionSurvey(surveysApi, key1);
        key1 = versionSurvey(surveysApi, key1);

        GuidCreatedOnVersionHolder key2 = createSurvey(surveysApi, TestSurvey.getSurvey(SurveyTest.class));
        key2 = versionSurvey(surveysApi, key2);
        key2 = versionSurvey(surveysApi, key2);

        containsAll(() -> surveysApi.getMostRecentSurveys(false).execute().body(), key, key1, key2);

        key = surveysApi.publishSurvey(key.getGuid(), key.getCreatedOn(), false).execute().body();
        key2 = surveysApi.publishSurvey(key2.getGuid(), key2.getCreatedOn(), false).execute().body();

        containsAll(() -> surveysApi.getPublishedSurveys(false).execute().body(), key, key2);
        
        // verify logical deletion
        surveysApi.deleteSurvey(key2.getGuid(), key2.getCreatedOn(), false).execute();

        anyDeleted(surveysApi.getMostRecentSurveys(true));
        noneDeleted(surveysApi.getMostRecentSurveys(false));
    }
    
    @SuppressWarnings("deprecation")
    @Test
    public void canGetMostRecentOrRecentlyPublishedSurveyWithIdentifier() throws Exception {
        SurveysApi surveysApi = developer.getClient(SurveysApi.class);

        GuidCreatedOnVersionHolder key = createSurveyWithIdentifier(surveysApi, TestSurvey.getSurvey(SurveyTest.class));
        String key_prefix = key.getGuid();
        key = versionSurvey(surveysApi, key);
        key = versionSurvey(surveysApi, key);

        GuidCreatedOnVersionHolder key1 = createSurvey(surveysApi, TestSurvey.getSurvey(SurveyTest.class));
        key1 = versionSurvey(surveysApi, key1);
        key1 = versionSurvey(surveysApi, key1);

        GuidCreatedOnVersionHolder key2 = createSurveyWithIdentifier(surveysApi, TestSurvey.getSurvey(SurveyTest.class));
        String key2_prefix = key2.getGuid();
        key2 = versionSurvey(surveysApi, key2);
        key2 = versionSurvey(surveysApi, key2);

        containsAll(() -> surveysApi.getMostRecentSurveys(false).execute().body(), key, key1, key2);

        key = surveysApi.publishSurvey(key_prefix, key.getCreatedOn(), false).execute().body();
        key2 = surveysApi.publishSurvey(key2_prefix, key2.getCreatedOn(), false).execute().body();

        containsAll(() -> surveysApi.getPublishedSurveys(false).execute().body(), key, key2);
        
        // verify logical deletion
        surveysApi.deleteSurvey(key2_prefix, key2.getCreatedOn(), false).execute();

        anyDeleted(surveysApi.getMostRecentSurveys(true));
        noneDeleted(surveysApi.getMostRecentSurveys(false));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void canUpdateASurvey() throws Exception {
        SurveysApi surveysApi = developer.getClient(SurveysApi.class);
        
        GuidCreatedOnVersionHolder key = createSurvey(surveysApi, TestSurvey.getSurvey(SurveyTest.class));
        Survey survey = surveysApi.getSurvey(key.getGuid(), key.getCreatedOn()).execute().body();
        assertEquals("Type is Survey.", survey.getClass(), Survey.class);

        survey.setName("New name");
        GuidCreatedOnVersionHolder holder = surveysApi.updateSurvey(survey.getGuid(), survey.getCreatedOn(), survey).execute().body();
        // Should be incremented.
        assertTrue(holder.getVersion() > survey.getVersion());
        
        survey = surveysApi.getSurvey(survey.getGuid(), survey.getCreatedOn()).execute().body();
        assertEquals("Name should have changed.", survey.getName(), "New name");
    }

    @SuppressWarnings("deprecation")
    @Test
    public void canUpdateASurveyWithIdentifier() throws Exception {
        SurveysApi surveysApi = developer.getClient(SurveysApi.class);
        
        GuidCreatedOnVersionHolder key = createSurveyWithIdentifier(surveysApi, TestSurvey.getSurvey(SurveyTest.class));

        Survey survey = surveysApi.getSurvey(key.getGuid(), key.getCreatedOn()).execute().body();
        assertEquals("Type is Survey.", survey.getClass(), Survey.class);
        assertEquals(key.getGuid(), IDENTIFIER_PREFIX + survey.getIdentifier());
        
        survey.setName("New name");
        GuidCreatedOnVersionHolder holder = surveysApi.updateSurvey(key.getGuid(), survey.getCreatedOn(), survey).execute().body();
        // Should be incremented.
        assertTrue(holder.getVersion() > survey.getVersion());

        DateTime expectedCreatedOn = survey.getCreatedOn();
        survey = Tests.retryHelper(() -> surveysApi.getSurvey(key.getGuid(), expectedCreatedOn).execute().body(),
                s -> holder.getVersion().equals(s.getVersion()));
        assertEquals("Name should have changed.", survey.getName(), "New name");
        assertEquals(key.getGuid(), IDENTIFIER_PREFIX + survey.getIdentifier());
    }
    
    @SuppressWarnings("deprecation")
    @Test
    public void researcherCannotUpdatePublishedSurvey() throws Exception {
        SurveysApi surveysApi = developer.getClient(SurveysApi.class);
        
        Survey survey = TestSurvey.getSurvey(SurveyTest.class);
        GuidCreatedOnVersionHolder keys = createSurvey(surveysApi, survey);
        keys = surveysApi.publishSurvey(keys.getGuid(), keys.getCreatedOn(), false).execute().body();
        
        survey.setName("This is a new name");
        survey.setVersion(keys.getVersion());
        try {
            surveysApi.updateSurvey(keys.getGuid(), keys.getCreatedOn(), survey).execute();
            fail("attempting to update a published survey should throw an exception.");
        } catch(PublishedSurveyException e) {
            // expected exception
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void canGetMostRecentlyPublishedSurveyWithoutTimestamp() throws Exception {
        SurveysApi surveysApi = developer.getClient(SurveysApi.class);
        
        Survey survey = TestSurvey.getSurvey(SurveyTest.class);

        GuidCreatedOnVersionHolder key = createSurvey(surveysApi, survey);

        GuidCreatedOnVersionHolder key1 = versionSurvey(surveysApi, key);
        GuidCreatedOnVersionHolder key2 = versionSurvey(surveysApi, key1);
        surveysApi.publishSurvey(key2.getGuid(), key2.getCreatedOn(), false).execute();
        versionSurvey(surveysApi, key2);

        Survey found = surveysApi.getPublishedSurveyVersion(key2.getGuid()).execute().body();
        assertEquals("This returns the right version", key2.getCreatedOn(), found.getCreatedOn());
        assertNotEquals("And these are really different versions", key.getCreatedOn(), found.getCreatedOn());
    }
    
    @SuppressWarnings("deprecation")
    @Test
    public void canGetMostRecentlyPublishedSurveyWithoutTimestampWithIdentifier() throws Exception {
        SurveysApi surveysApi = developer.getClient(SurveysApi.class);
        
        Survey survey = TestSurvey.getSurvey(SurveyTest.class);

        GuidCreatedOnVersionHolder key = createSurveyWithIdentifier(surveysApi, survey);
        GuidCreatedOnVersionHolder key1 = versionSurvey(surveysApi, key);
        GuidCreatedOnVersionHolder key2 = versionSurvey(surveysApi, key1);

        surveysApi.publishSurvey(key.getGuid(), key2.getCreatedOn(), false).execute();
        Tests.retryHelper(() -> surveysApi.getSurvey(key.getGuid(), key2.getCreatedOn()).execute().body(),
                Survey::isPublished);

        versionSurvey(surveysApi, key2);

        Survey found = surveysApi.getPublishedSurveyVersion(key.getGuid()).execute().body();
        assertEquals(key.getGuid(), IDENTIFIER_PREFIX+found.getIdentifier());
        assertEquals("This returns the right version", key2.getCreatedOn(), found.getCreatedOn());
        assertNotEquals("And these are really different versions", key.getCreatedOn(), found.getCreatedOn());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void canCallMultiOperationMethodToMakeSurveyUpdate() throws Exception {
        SurveysApi surveysApi = developer.getClient(SurveysApi.class);
        Survey survey = TestSurvey.getSurvey(SurveyTest.class);

        GuidCreatedOnVersionHolder keys = createSurvey(surveysApi, survey);

        Survey existingSurvey = surveysApi.getSurvey(keys.getGuid(), keys.getCreatedOn()).execute().body();
        existingSurvey.setName("This is an update test");

        keys = surveysApi.versionSurvey(existingSurvey.getGuid(), existingSurvey.getCreatedOn()).execute().body();
        
        existingSurvey.setVersion(keys.getVersion());
        keys = surveysApi.updateSurvey(keys.getGuid(), keys.getCreatedOn(), existingSurvey).execute().body();
        
        keys = surveysApi.publishSurvey(keys.getGuid(), keys.getCreatedOn(), false).execute().body();
        
        surveysToDelete.add(new MutableHolder(keys));

        SurveyList allRevisions = surveysApi.getAllVersionsOfSurvey(keys.getGuid(), false).execute().body();
        assertEquals("There are now two versions", 2, allRevisions.getItems().size());

        Survey mostRecent = surveysApi.getPublishedSurveyVersion(existingSurvey.getGuid()).execute().body();
        assertEquals(mostRecent.getGuid(), keys.getGuid());
        assertEquals(mostRecent.getCreatedOn(), keys.getCreatedOn());
        assertEquals(mostRecent.getVersion(), keys.getVersion());
        assertEquals("The latest has a new title", "This is an update test", allRevisions.getItems().get(0).getName());
    }
    
    @SuppressWarnings("deprecation")
    @Test
    public void canSaveAndRetrieveInfoScreen() throws Exception {
        SurveysApi surveysApi = developer.getClient(SurveysApi.class);
        
        Survey survey = new Survey();
        survey.setIdentifier(surveyId);
        survey.setName("Test survey");
        
        SurveyInfoScreen screen = new SurveyInfoScreen();
        screen.setIdentifier("foo");
        screen.setTitle("Title");
        screen.setPrompt("Prompt");
        screen.setPromptDetail("Prompt detail");
        Tests.setVariableValueInObject(survey, "type", "SurveyInfoScreen");
        
        Image image = new Image();
        image.setSource("https://pbs.twimg.com/profile_images/1642204340/ReferencePear_400x400.PNG");
        image.setHeight(400);
        image.setWidth(400);
        screen.setImage(image);
        survey.getElements().add(screen);
        
        // Add a question too just to verify that's okay
        SurveyQuestion question = new SurveyQuestion();
        question.setIdentifier("bar");
        question.setPrompt("Prompt");
        question.setUiHint(UIHint.TEXTFIELD);
        StringConstraints sc = new StringConstraints();
        sc.setDataType(DataType.STRING);
        question.setConstraints(sc);
        Tests.setVariableValueInObject(question, "type", "SurveyQuestion");
        survey.getElements().add(question);
        
        GuidCreatedOnVersionHolder keys = createSurvey(surveysApi, survey);
        
        Survey newSurvey = surveysApi.getSurvey(keys.getGuid(), keys.getCreatedOn()).execute().body();
        assertEquals(2, newSurvey.getElements().size());
        
        SurveyInfoScreen newScreen = (SurveyInfoScreen)newSurvey.getElements().get(0);
        
        assertEquals(SurveyInfoScreen.class, newScreen.getClass());
        assertNotNull(newScreen.getGuid());
        assertEquals("foo", newScreen.getIdentifier());
        assertEquals("Title", newScreen.getTitle());
        assertEquals("Prompt", newScreen.getPrompt());
        assertEquals("Prompt detail", newScreen.getPromptDetail());
        assertEquals("https://pbs.twimg.com/profile_images/1642204340/ReferencePear_400x400.PNG", newScreen.getImage().getSource());
        assertEquals((Integer)400, newScreen.getImage().getWidth());
        assertEquals((Integer)400, newScreen.getImage().getHeight());
        
        SurveyQuestion newQuestion = (SurveyQuestion)newSurvey.getElements().get(1);
        assertEquals(SurveyQuestion.class, newQuestion.getClass());
        assertNotNull(newQuestion.getGuid());
        assertEquals("bar", newQuestion.getIdentifier());
        assertEquals("Prompt", newQuestion.getPrompt());
        assertEquals(UIHint.TEXTFIELD, newQuestion.getUiHint());
    }
    
    @SuppressWarnings("deprecation")
    @Test
    public void canSaveAndRetrieveInfoScreenWithIdentifier() throws Exception {
        SurveysApi surveysApi = developer.getClient(SurveysApi.class);
        
        Survey survey = new Survey();
        survey.setIdentifier(surveyId);
        survey.setName("Test survey");
        
        SurveyInfoScreen screen = new SurveyInfoScreen();
        screen.setIdentifier("foo");
        screen.setTitle("Title");
        screen.setPrompt("Prompt");
        screen.setPromptDetail("Prompt detail");
        Tests.setVariableValueInObject(survey, "type", "SurveyInfoScreen");
        
        Image image = new Image();
        image.setSource("https://pbs.twimg.com/profile_images/1642204340/ReferencePear_400x400.PNG");
        image.setHeight(400);
        image.setWidth(400);
        screen.setImage(image);
        survey.getElements().add(screen);
        
        // Add a question too just to verify that's okay
        SurveyQuestion question = new SurveyQuestion();
        question.setIdentifier("bar");
        question.setPrompt("Prompt");
        question.setUiHint(UIHint.TEXTFIELD);
        StringConstraints sc = new StringConstraints();
        sc.setDataType(DataType.STRING);
        question.setConstraints(sc);
        Tests.setVariableValueInObject(question, "type", "SurveyQuestion");
        survey.getElements().add(question);
        
        GuidCreatedOnVersionHolder keys = createSurveyWithIdentifier(surveysApi, survey);

        Survey newSurvey = surveysApi.getSurvey(keys.getGuid(), keys.getCreatedOn()).execute().body();
        assertEquals(keys.getGuid(), IDENTIFIER_PREFIX + newSurvey.getIdentifier());
        assertEquals(2, newSurvey.getElements().size());
        
        SurveyInfoScreen newScreen = (SurveyInfoScreen)newSurvey.getElements().get(0);
        
        assertEquals(SurveyInfoScreen.class, newScreen.getClass());
        assertNotNull(newScreen.getGuid());
        assertEquals("foo", newScreen.getIdentifier());
        assertEquals("Title", newScreen.getTitle());
        assertEquals("Prompt", newScreen.getPrompt());
        assertEquals("Prompt detail", newScreen.getPromptDetail());
        assertEquals("https://pbs.twimg.com/profile_images/1642204340/ReferencePear_400x400.PNG", newScreen.getImage().getSource());
        assertEquals((Integer)400, newScreen.getImage().getWidth());
        assertEquals((Integer)400, newScreen.getImage().getHeight());
        
        SurveyQuestion newQuestion = (SurveyQuestion)newSurvey.getElements().get(1);
        assertEquals(SurveyQuestion.class, newQuestion.getClass());
        assertNotNull(newQuestion.getGuid());
        assertEquals("bar", newQuestion.getIdentifier());
        assertEquals("Prompt", newQuestion.getPrompt());
        assertEquals(UIHint.TEXTFIELD, newQuestion.getUiHint());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void workerCanGetSurveys() throws Exception {
        // One of the key functionalities of worker accounts is that they can get surveys across studies.
        // Unfortunately, integration tests are set up so it's difficult to test across studies. As such, we do all our
        // testing in the API app to test basic functionality.

        // Create two surveys with two published versions.
        SurveysApi surveysApi = developer.getClient(SurveysApi.class);

        GuidCreatedOnVersionHolder survey1aKeys = createSurvey(surveysApi, TestSurvey.getSurvey(SurveyTest.class));
        surveysApi.publishSurvey(survey1aKeys.getGuid(), survey1aKeys.getCreatedOn(), false).execute();
        GuidCreatedOnVersionHolder survey1bKeys = versionSurvey(surveysApi, survey1aKeys);
        surveysApi.publishSurvey(survey1bKeys.getGuid(), survey1bKeys.getCreatedOn(), false).execute();

        GuidCreatedOnVersionHolder survey2aKeys = createSurvey(surveysApi, TestSurvey.getSurvey(SurveyTest.class));
        surveysApi.publishSurvey(survey2aKeys.getGuid(), survey2aKeys.getCreatedOn(), false).execute();
        GuidCreatedOnVersionHolder survey2bKeys = versionSurvey(surveysApi, survey2aKeys);

        surveysApi.publishSurvey(survey2bKeys.getGuid(), survey2bKeys.getCreatedOn(), false).execute();
        Tests.retryHelper(() -> surveysApi.getSurvey(survey2bKeys.getGuid(), survey2bKeys.getCreatedOn()).execute()
                        .body(), Survey::isPublished);

        ForWorkersApi workerApi = worker.getClient(ForWorkersApi.class);

        // The surveys we created were just dummies. Just check that the surveys are not null and that the keys match.
        Survey survey1a = workerApi.getSurvey(survey1aKeys.getGuid(), survey1aKeys.getCreatedOn()).execute().body();
        Survey survey1b = workerApi.getSurvey(survey1bKeys.getGuid(), survey1bKeys.getCreatedOn()).execute().body();
        Survey survey2a = workerApi.getSurvey(survey2aKeys.getGuid(), survey2aKeys.getCreatedOn()).execute().body();
        Survey survey2b = workerApi.getSurvey(survey2bKeys.getGuid(), survey2bKeys.getCreatedOn()).execute().body();

        assertKeysEqual(survey1aKeys, survey1a);
        assertKeysEqual(survey1bKeys, survey1b);
        assertKeysEqual(survey2aKeys, survey2a);
        assertKeysEqual(survey2bKeys, survey2b);

        // Get using the guid+createdOn API for completeness.
        Survey survey1aAgain = workerApi.getSurvey(survey1aKeys.getGuid(), survey1aKeys.getCreatedOn()).execute().body();
        assertKeysEqual(survey1aKeys, survey1aAgain);

        // We only expect the most recently published versions, namely 1b and 2b.
        containsAll(() -> workerApi.getAllPublishedSurveys(TEST_APP_ID, false).execute().body(),
                new MutableHolder(survey1b), new MutableHolder(survey2b));
        
        // Delete 2b.
        developer.getClient(SurveysApi.class).deleteSurvey(survey2b.getGuid(), survey2b.getCreatedOn(), false).execute();

        // Verify includeDeleted works
        noneDeleted(workerApi.getAllPublishedSurveys(TEST_APP_ID, false));
        anyDeleted(workerApi.getAllPublishedSurveys(TEST_APP_ID, true));
    }
    
    @SuppressWarnings("deprecation")
    @Test
    public void verifyEndSurveyRule() throws Exception {
        SurveysApi surveysApi = developer.getClient(SurveysApi.class);
        
        Survey survey = new Survey();
        survey.setIdentifier(surveyId);
        survey.setName("Test survey");

        SurveyRule rule = new SurveyRule();
        rule.setOperator(Operator.EQ);
        rule.setValue("true");
        rule.setEndSurvey(Boolean.TRUE);
        
        StringConstraints constraints = new StringConstraints();
        constraints.setDataType(DataType.STRING);

        SurveyQuestion question = new SurveyQuestion();
        question.setIdentifier("bar");
        question.setPrompt("Prompt");
        question.setUiHint(UIHint.TEXTFIELD);
        question.setConstraints(constraints);
        Tests.setVariableValueInObject(question, "type", "SurveyQuestion");
        question.setAfterRules(ImmutableList.of(rule)); // end survey
        survey.getElements().add(question);
        
        GuidCreatedOnVersionHolder keys = createSurvey(surveysApi, survey);
        
        Survey retrieved = surveysApi.getSurvey(keys.getGuid(), keys.getCreatedOn()).execute().body();
        SurveyRule retrievedRule = getSurveyElement(retrieved, "bar").getAfterRules().get(0);
        
        assertEquals(Boolean.TRUE, retrievedRule.isEndSurvey());
        assertEquals("true", retrievedRule.getValue());
        assertEquals(Operator.EQ, retrievedRule.getOperator());
        assertNull(retrievedRule.getSkipTo());
    }
    
    @SuppressWarnings("deprecation")
    @Test
    public void canLogicallyDeletePublishedSurvey() throws Exception {
        SurveysApi surveysApi = developer.getClient(SurveysApi.class);

        GuidCreatedOnVersionHolder keys = createSurvey(surveysApi, TestSurvey.getSurvey(SurveyTest.class));
        
        surveysApi.publishSurvey(keys.getGuid(), keys.getCreatedOn(), false).execute();
        
        surveysApi.deleteSurvey(keys.getGuid(), keys.getCreatedOn(), false).execute();

        // no longer in the list
        Tests.retryHelper(() -> surveysApi.getPublishedSurveys(false).execute().body().getItems(),
                list -> list.stream().noneMatch(survey -> survey.getGuid().equals(keys.getGuid())));

        // you can still retrieve the logically deleted survey in the list
        Tests.retryHelper(() -> surveysApi.getPublishedSurveys(true).execute().body().getItems(),
                list -> list.stream().anyMatch(survey -> survey.getGuid().equals(keys.getGuid())));
    }
    
    @SuppressWarnings("deprecation")
    @Test
    public void cannotLogicallyDeleteSurveyTwice() throws Exception {
        SurveysApi surveysApi = developer.getClient(SurveysApi.class);

        GuidCreatedOnVersionHolder keys = createSurvey(surveysApi, TestSurvey.getSurvey(SurveyTest.class));
        
        surveysApi.deleteSurvey(keys.getGuid(), keys.getCreatedOn(), false).execute();
        try {
            surveysApi.deleteSurvey(keys.getGuid(), keys.getCreatedOn(), false).execute();
            fail("Should have thrown exception");
        } catch(EntityNotFoundException e) {
        }
        try {
            surveysApi.getAllVersionsOfSurvey(keys.getGuid(), false).execute().body();    
            fail("Should have thrown exception");
        } catch(EntityNotFoundException e) {
        }
    }
    
    @SuppressWarnings("deprecation")
    @Test
    public void cannotPhysicallyDeleteSurveyInSchedule() throws Exception {
        SurveysApi surveysApi = developer.getClient(SurveysApi.class);
        SchedulesV1Api schedulesApi = developer.getClient(SchedulesV1Api.class);

        GuidCreatedOnVersionHolder keys = createSurvey(surveysApi, TestSurvey.getSurvey(SurveyTest.class));
        
        keys = surveysApi.publishSurvey(keys.getGuid(), keys.getCreatedOn(), false).execute().body();
        
        SchedulePlan plan = createSchedulePlanTo(keys);
        GuidVersionHolder holder = schedulesApi.createSchedulePlan(plan).execute().body();
        
        // Should not be able to physically delete this survey
        try {
            adminsApi.deleteSurvey(keys.getGuid(), keys.getCreatedOn(), true).execute();
            fail("Should have thrown an exception.");
        } catch(ConstraintViolationException e) {
            
        } finally {
            adminsApi.deleteSchedulePlan(holder.getGuid(), true).execute();
        }
    }
    
    @SuppressWarnings("deprecation")
    @Test
    public void canPhysicallyDeleteLogicallyDeletedSurvey() throws Exception {
        SurveysApi surveysApi = developer.getClient(SurveysApi.class);

        GuidCreatedOnVersionHolder keys = createSurvey(surveysApi, TestSurvey.getSurvey(SurveyTest.class));
        
        surveysApi.deleteSurvey(keys.getGuid(), keys.getCreatedOn(), false).execute();
        
        Survey retrieved = surveysApi.getSurvey(keys.getGuid(), keys.getCreatedOn()).execute().body();
        assertTrue(retrieved.isDeleted());
        
        adminsApi.deleteSurvey(keys.getGuid(), keys.getCreatedOn(), true).execute();
        surveysToDelete.remove(keys);
        
        try {
            surveysApi.getSurvey(keys.getGuid(), keys.getCreatedOn()).execute().body();
            fail("Should have thrown an exception.");
        } catch(EntityNotFoundException e) {
            
        }
    }
    
    @SuppressWarnings("deprecation")
    @Test
    public void canCreateAndSaveVariousKindsOfBeforeRules() throws Exception {
        App app = adminsApi.getUsersApp().execute().body();
        String dataGroup = app.getDataGroups().get(0);

        Survey survey = TestSurvey.getSurvey(SurveyTest.class);
        SurveyElement element = survey.getElements().get(0);
        SurveyElement skipToTarget = survey.getElements().get(1);
        // Trim the survey to one element
        survey.setElements(Lists.newArrayList(element,skipToTarget));
        
        SurveyRule endSurvey = new SurveyRule().endSurvey(true).operator(Operator.ALWAYS);
        SurveyRule skipTo = new SurveyRule().skipTo(skipToTarget.getIdentifier()).operator(Operator.EQ).value("2010-10-10");
        SurveyRule assignGroup = new SurveyRule().assignDataGroup(dataGroup).operator(Operator.DE);
        SurveyRule displayIf = new SurveyRule().displayIf(true).operator(Operator.ANY).addDataGroupsItem(dataGroup);
        SurveyRule displayUnless = new SurveyRule().displayUnless(true).operator(Operator.ALL)
                .addDataGroupsItem(dataGroup);
        
        element.setBeforeRules(Lists.newArrayList(endSurvey, skipTo, assignGroup, displayIf, displayUnless));
        
        SurveysApi devSurveysApi = developer.getClient(SurveysApi.class);
        
        GuidCreatedOnVersionHolder keys = createSurvey(devSurveysApi, survey);
        Survey created = devSurveysApi.getSurvey(keys.getGuid(), keys.getCreatedOn()).execute().body();
        
        List<SurveyRule> createdRules = created.getElements().get(0).getBeforeRules();
        // These aren't set locally and will cause equality to fail. Set them.
        Tests.setVariableValueInObject(endSurvey, "type", "SurveyRule");
        Tests.setVariableValueInObject(skipTo, "type", "SurveyRule");
        Tests.setVariableValueInObject(assignGroup, "type", "SurveyRule");
        Tests.setVariableValueInObject(displayIf, "type", "SurveyRule");
        Tests.setVariableValueInObject(displayUnless, "type", "SurveyRule");
        
        assertEquals(endSurvey, createdRules.get(0));
        assertEquals(skipTo, createdRules.get(1));
        assertEquals(assignGroup, createdRules.get(2));
        assertEquals(displayIf, createdRules.get(3));
        assertEquals(displayUnless, createdRules.get(4));
        
        // Verify they can all be deleted as well.
        created.getElements().get(0).setBeforeRules(Lists.newArrayList());
        
        keys = devSurveysApi.updateSurvey(created.getGuid(), created.getCreatedOn(), created).execute().body();
        Survey updated = devSurveysApi.getSurvey(keys.getGuid(), keys.getCreatedOn()).execute().body();
        
        assertTrue(updated.getElements().get(0).getBeforeRules().isEmpty());
    }
    
    @SuppressWarnings("deprecation")
    @Test
    public void canCreateAndSaveVariousKindsOfAfterRules() throws Exception {
        App app = adminsApi.getUsersApp().execute().body();
        String dataGroup = app.getDataGroups().get(0);

        Survey survey = TestSurvey.getSurvey(SurveyTest.class);
        SurveyElement element = survey.getElements().get(0);
        SurveyElement skipToTarget = survey.getElements().get(1);
        // Trim the survey to one element
        survey.setElements(Lists.newArrayList(element,skipToTarget));
        
        SurveyRule endSurvey = new SurveyRule().endSurvey(true).operator(Operator.ALWAYS);
        SurveyRule skipTo = new SurveyRule().skipTo(skipToTarget.getIdentifier()).operator(Operator.EQ)
                .value("2010-10-10");
        SurveyRule assignGroup = new SurveyRule().assignDataGroup(dataGroup).operator(Operator.DE);
        
        element.setAfterRules(Lists.newArrayList(endSurvey, skipTo, assignGroup));
        
        SurveysApi devSurveysApi = developer.getClient(SurveysApi.class);
        
        GuidCreatedOnVersionHolder keys = createSurvey(devSurveysApi, survey);
        Survey created = devSurveysApi.getSurvey(keys.getGuid(), keys.getCreatedOn()).execute().body();
        
        List<SurveyRule> createdRules = created.getElements().get(0).getAfterRules();
        // These aren't set locally and will cause equality to fail. Set them.
        Tests.setVariableValueInObject(endSurvey, "type", "SurveyRule");
        Tests.setVariableValueInObject(skipTo, "type", "SurveyRule");
        Tests.setVariableValueInObject(assignGroup, "type", "SurveyRule");
        
        assertEquals(endSurvey, createdRules.get(0));
        assertEquals(skipTo, createdRules.get(1));
        assertEquals(assignGroup, createdRules.get(2));
        
        // Verify they can all be deleted as well.
        created.getElements().get(0).setAfterRules(Lists.newArrayList());
        
        keys = devSurveysApi.updateSurvey(created.getGuid(), created.getCreatedOn(), created).execute().body();
        Survey updated = devSurveysApi.getSurvey(keys.getGuid(), keys.getCreatedOn()).execute().body();
        
        assertTrue(updated.getElements().get(0).getAfterRules().isEmpty());
    }
    
    @SuppressWarnings("deprecation")
    @Test
    public void displayActionsInAfterRulesValidated() throws Exception {
        App app = adminsApi.getUsersApp().execute().body();
        String dataGroup = app.getDataGroups().get(0);

        Survey survey = TestSurvey.getSurvey(SurveyTest.class);
        SurveyElement element = survey.getElements().get(0);
        SurveyElement skipToTarget = survey.getElements().get(1);
        // Trim the survey to one element
        survey.setElements(Lists.newArrayList(element,skipToTarget));
        
        SurveyRule displayIf = new SurveyRule().displayIf(true).operator(Operator.ANY).addDataGroupsItem(dataGroup);
        SurveyRule displayUnless = new SurveyRule().displayUnless(true).operator(Operator.ALL)
                .addDataGroupsItem(dataGroup);
        Tests.setVariableValueInObject(displayIf, "type", "SurveyRule");
        Tests.setVariableValueInObject(displayUnless, "type", "SurveyRule");
        
        element.setAfterRules(Lists.newArrayList(displayIf, displayUnless));
        
        SurveysApi devSurveysApi = developer.getClient(SurveysApi.class);
        
        try {
            devSurveysApi.createSurvey(survey).execute().body();
            fail("Should have thrown exception");
        } catch(InvalidEntityException e) {
            assertEquals("elements[0].afterRules[0].displayIf specifies display after screen has been shown",
                    e.getErrors().get("elements[0].afterRules[0].displayIf").get(0));
            assertEquals("elements[0].afterRules[1].displayUnless specifies display after screen has been shown",
                    e.getErrors().get("elements[0].afterRules[1].displayUnless").get(0));
        }
    }
    
    @SuppressWarnings("deprecation")
    @Test
    public void canRetrieveDeletedSurveys() throws Exception {
        SurveysApi surveysApi = developer.getClient(SurveysApi.class);
        
        Survey survey = TestSurvey.getSurvey(SurveyTest.class);
        
        GuidCreatedOnVersionHolder keys1 = createSurvey(surveysApi, survey);
        keys1 = surveysApi.publishSurvey(keys1.getGuid(), keys1.getCreatedOn(), true).execute().body();
        
        GuidCreatedOnVersionHolder keys2 = versionSurvey(surveysApi, keys1);
        
        survey.setCopyrightNotice("This is a change");
        survey.setCreatedOn(keys2.getCreatedOn());
        survey.setVersion(keys2.getVersion());
        surveysApi.updateSurvey(keys2.getGuid(), keys2.getCreatedOn(), survey).execute().body();
        
        // These two are the same because there are no deleted surveys
        noneDeleted(surveysApi.getAllVersionsOfSurvey(keys1.getGuid(), false));
        noneDeleted(surveysApi.getAllVersionsOfSurvey(keys1.getGuid(), true));

        // Logically delete the second version of the survey
        surveysApi.deleteSurvey(keys2.getGuid(), keys2.getCreatedOn(), false).execute();
        
        // These now differ.
        noneDeleted(surveysApi.getAllVersionsOfSurvey(keys1.getGuid(), false));
        anyDeleted(surveysApi.getAllVersionsOfSurvey(keys1.getGuid(), true));
        
        // You can get the logically deleted survey
        Survey deletedSurvey = surveysApi.getSurvey(keys2.getGuid(), keys2.getCreatedOn()).execute().body();
        assertTrue(deletedSurvey.isDeleted());
        
        // Really delete it
        adminsApi.deleteSurvey(keys2.getGuid(), keys2.getCreatedOn(), true).execute();
        surveysToDelete.remove(keys2);
        
        try {
            surveysApi.getSurvey(keys2.getGuid(), keys2.getCreatedOn()).execute().body();
            fail("Should have thrown exception");
        } catch(EntityNotFoundException e) {
            
        }
        // End result is that it no longer appears in the list of versions
        noneDeleted(surveysApi.getAllVersionsOfSurvey(keys1.getGuid(), true));
    }
    
    @SuppressWarnings("deprecation")
    @Test
    public void getPublishedSurveyVersionAndDelete() throws Exception {
        // Test the interaction of publication and the two kinds of deletion
        SurveysApi surveysApi = developer.getClient(SurveysApi.class);
        
        Survey survey = TestSurvey.getSurvey(SurveyTest.class);
        GuidCreatedOnVersionHolder keys1 = createSurvey(surveysApi, survey);
        GuidCreatedOnVersionHolder keys2 = versionSurvey(surveysApi, keys1);
        
        // You cannot publish a (logically) deleted survey
        surveysApi.deleteSurvey(keys1.getGuid(), keys1.getCreatedOn(), false).execute();
        try {
            surveysApi.publishSurvey(keys1.getGuid(), keys1.getCreatedOn(), false).execute();
            fail("Should have thrown an exception");
        } catch(EntityNotFoundException e) {
            
        }
        surveysApi.publishSurvey(keys2.getGuid(), keys2.getCreatedOn(), false).execute();
        surveysApi.deleteSurvey(keys2.getGuid(), keys2.getCreatedOn(), false).execute();
        
        anyDeleted(surveysApi.getPublishedSurveys(true));
        noneDeleted(surveysApi.getPublishedSurveys(false));
    }
    
    @SuppressWarnings("deprecation")
    @Test
    public void getMostRecentSurveyVersionAndDelete() throws Exception {
        // Test the interaction of publication and the two kinds of deletion
        SurveysApi surveysApi = developer.getClient(SurveysApi.class);
        
        Survey survey = TestSurvey.getSurvey(SurveyTest.class);
        GuidCreatedOnVersionHolder keys1 = createSurvey(surveysApi, survey);
        GuidCreatedOnVersionHolder keys2 = versionSurvey(surveysApi, keys1);
        String guid = keys1.getGuid();
        
        // You cannot publish a (logically) deleted survey
        surveysApi.deleteSurvey(keys1.getGuid(), keys1.getCreatedOn(), false).execute();
        try {
            surveysApi.publishSurvey(keys1.getGuid(), keys1.getCreatedOn(), false).execute();
            fail("Should have thrown an exception");
        } catch(EntityNotFoundException e) {
            
        }
        surveysApi.publishSurvey(keys2.getGuid(), keys2.getCreatedOn(), false).execute();
        surveysApi.deleteSurvey(keys2.getGuid(), keys2.getCreatedOn(), false).execute();

        Tests.retryHelper(
                () -> {
                    try {
                        return surveysApi.getMostRecentSurveyVersion(guid).execute().body();
                    } catch (EntityNotFoundException e) {
                        return null;
                    }
                },
                Predicates.isNull());
    }
    
    @SuppressWarnings("deprecation")
    @Test
    public void getMostRecentSurveyVersionAndDeleteWithIdentifier() throws Exception {
        // Test the interaction of publication and the two kinds of deletion
        SurveysApi surveysApi = developer.getClient(SurveysApi.class);
        
        Survey survey = TestSurvey.getSurvey(SurveyTest.class);
        GuidCreatedOnVersionHolder keys1 = createSurveyWithIdentifier(surveysApi, survey);
        GuidCreatedOnVersionHolder keys2 = versionSurvey(surveysApi, keys1);
        String guid = keys1.getGuid();

        // You cannot publish a (logically) deleted survey
        surveysApi.deleteSurvey(keys1.getGuid(), keys1.getCreatedOn(), false).execute();
        Tests.retryHelper(
                () -> {
                    try {
                        return surveysApi.publishSurvey(keys1.getGuid(), keys1.getCreatedOn(), false).execute().body();
                    } catch (EntityNotFoundException e) {
                        return null;
                    }
                },
                Predicates.isNull());

        surveysApi.publishSurvey(keys2.getGuid(), keys2.getCreatedOn(), false).execute();
        Tests.retryHelper(() -> surveysApi.getSurvey(keys2.getGuid(), keys2.getCreatedOn()).execute().body(),
                Survey::isPublished);

        surveysApi.deleteSurvey(keys2.getGuid(), keys2.getCreatedOn(), false).execute();
        Tests.retryHelper(
                () -> {
                    try {
                        return surveysApi.getMostRecentSurveyVersion(guid).execute().body();
                    } catch (EntityNotFoundException e) {
                        return null;
                    }
                },
                Predicates.isNull());
    }
    
    private void anyDeleted(Call<SurveyList> call) {
        Tests.retryHelper(() -> call.execute().body().getItems().stream(),
                s -> s.anyMatch(Survey::isDeleted));
    }
    
    private void noneDeleted(Call<SurveyList> call) {
        Tests.retryHelper(() -> call.execute().body().getItems().stream(),
                s -> s.noneMatch(Survey::isDeleted));
    }
    
    private SchedulePlan createSchedulePlanTo(GuidCreatedOnVersionHolder keys) {
        SurveyReference surveyReference = new SurveyReference().guid(keys.getGuid()).createdOn(keys.getCreatedOn());
        
        Activity activity = new Activity().label("Do this survey").activityType(ActivityType.SURVEY)
                .survey(surveyReference);
        Schedule schedule = new Schedule().activities(Lists.newArrayList(activity));
        schedule.setScheduleType(ScheduleType.ONCE);
        
        SimpleScheduleStrategy strategy = new SimpleScheduleStrategy().schedule(schedule);
        
        return new SchedulePlan().label("Plan").strategy(strategy);
    }
    
    private SurveyElement getSurveyElement(Survey survey, String id) {
        for (SurveyElement element : survey.getElements()) {
            if (element.getIdentifier().equals(id)) {
                return element;
            }
        }
        return null;
    }
    
    private void containsAll(Callable<SurveyList> call, GuidCreatedOnVersionHolder... keys) {
        Tests.retryHelper(call,
                s -> {
                    List<Survey> surveys = s.getItems();

                    // The server may have more surveys than the ones we created, if more than one person is running tests
                    // (unit or integration), or if there are persistent tests unrelated to this test.
                    if (surveys.size() < keys.length) {
                        return false;
                    }

                    // Check that we can find all of our expected surveys. Use the MutableHolder class, so we can use a
                    // set and contains().
                    Set<GuidCreatedOnVersionHolder> surveyKeySet = new HashSet<>();
                    for (Survey survey : surveys) {
                        surveyKeySet.add(new MutableHolder(survey));
                    }
                    for (GuidCreatedOnVersionHolder key : keys) {
                        MutableHolder adjKey = new MutableHolder(key);
                        if (!surveyKeySet.contains(adjKey)) {
                            return false;
                        }
                    }

                    return true;
                });
    }

    private class MutableHolder extends GuidCreatedOnVersionHolder {
        private final String guid;
        private final DateTime createdOn;
        private final Long version;
        MutableHolder(GuidCreatedOnVersionHolder keys) {
            this.guid = keys.getGuid();
            this.createdOn = keys.getCreatedOn();
            this.version = keys.getVersion();
        }
        MutableHolder(Survey keys) {
            this.guid = keys.getGuid();
            this.createdOn = keys.getCreatedOn();
            this.version = keys.getVersion();
        }
        @Override
        public String getGuid() {
            return guid;
        }
        @Override
        public DateTime getCreatedOn() {
            return createdOn;
        }
        @Override
        public Long getVersion() {
            return version;
        }
        @Override
        public int hashCode() {
            return Objects.hash(guid, createdOn, version);
        }
        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (!super.equals(obj) || getClass() != obj.getClass())
                return false;
            MutableHolder other = (MutableHolder) obj;
            return Objects.equals(createdOn, other.createdOn) &&
                Objects.equals(guid, other.guid) &&
                Objects.equals(version, other.version);
        }

        @Override
        public String toString() {
            return "MutableHolder{" +
                    "guid='" + guid + '\'' +
                    ", createdOn=" + createdOn +
                    ", version=" + version +
                    '}';
        }
    }
    
    // Helper methods to ensure we always record these calls for cleanup

    @SuppressWarnings("deprecation")
    private GuidCreatedOnVersionHolder createSurvey(SurveysApi surveysApi, Survey survey) throws Exception {
        GuidCreatedOnVersionHolder keys = surveysApi.createSurvey(survey).execute().body();
        surveysToDelete.add(keys);

        // Verify the index is up-to-date.
        Tests.retryHelper(() -> surveysApi.getSurvey(IDENTIFIER_PREFIX+survey.getIdentifier(),
                keys.getCreatedOn()).execute().body(),
                Predicates.alwaysTrue());

        return keys;
    }

    @SuppressWarnings("deprecation")
    private GuidCreatedOnVersionHolder createSurveyWithIdentifier(SurveysApi surveysApi, Survey survey) throws Exception {
        GuidCreatedOnVersionHolder keys = surveysApi.createSurvey(survey).execute().body();
        surveysToDelete.add(keys);

        // Verify the index is up-to-date.
        Tests.retryHelper(() -> surveysApi.getSurvey(IDENTIFIER_PREFIX+survey.getIdentifier(),
                keys.getCreatedOn()).execute().body(),
                Predicates.alwaysTrue());

        survey.setGuid(IDENTIFIER_PREFIX + survey.getIdentifier());
        survey.setCreatedOn(keys.getCreatedOn());
        survey.setVersion(keys.getVersion());
        return new MutableHolder(survey);
    }
    
    @SuppressWarnings("deprecation")
    private GuidCreatedOnVersionHolder versionSurvey(SurveysApi surveysApi, GuidCreatedOnVersionHolder survey) throws Exception {
        GuidCreatedOnVersionHolder versionHolder = surveysApi
                .versionSurvey(survey.getGuid(), survey.getCreatedOn()).execute().body();
        surveysToDelete.add(versionHolder);

        // Verify the index is up-to-date.
        Tests.retryHelper(() -> surveysApi.getAllVersionsOfSurvey(versionHolder.getGuid(), false)
                        .execute().body().getItems(),
                l -> l.stream().anyMatch(s -> versionHolder.getCreatedOn().equals(versionHolder.getCreatedOn())));

        return versionHolder;
    }

    private static void assertKeysEqual(GuidCreatedOnVersionHolder keys, Survey survey) {
        assertEquals(keys.getGuid(), survey.getGuid());
        assertEquals(keys.getCreatedOn(), survey.getCreatedOn());
    }
}
