package org.sagebionetworks.bridge.sdk.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.sagebionetworks.bridge.rest.model.Role.DEVELOPER;
import static org.sagebionetworks.bridge.sdk.integration.Tests.API_SIGNIN;
import static org.sagebionetworks.bridge.sdk.integration.Tests.SHARED_SIGNIN;
import static org.sagebionetworks.bridge.sdk.integration.UploadSchemaTest.makeSimpleSchema;
import static org.sagebionetworks.bridge.util.IntegTestUtils.SHARED_APP_ID;
import static org.sagebionetworks.bridge.util.IntegTestUtils.TEST_APP_ID;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.RandomStringUtils;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sagebionetworks.bridge.rest.api.AuthenticationApi;
import org.sagebionetworks.bridge.rest.api.ForAdminsApi;
import org.sagebionetworks.bridge.rest.api.ForDevelopersApi;
import org.sagebionetworks.bridge.rest.api.SurveysApi;
import org.sagebionetworks.bridge.rest.api.UploadSchemasApi;
import org.sagebionetworks.bridge.rest.exceptions.BadRequestException;
import org.sagebionetworks.bridge.rest.exceptions.BridgeSDKException;
import org.sagebionetworks.bridge.rest.exceptions.ConcurrentModificationException;
import org.sagebionetworks.bridge.rest.exceptions.EntityNotFoundException;
import org.sagebionetworks.bridge.rest.exceptions.UnauthorizedException;
import org.sagebionetworks.bridge.rest.model.GuidCreatedOnVersionHolder;
import org.sagebionetworks.bridge.rest.model.SharedModuleMetadata;
import org.sagebionetworks.bridge.rest.model.SharedModuleMetadataList;
import org.sagebionetworks.bridge.rest.model.SharedModuleType;
import org.sagebionetworks.bridge.rest.model.Survey;
import org.sagebionetworks.bridge.rest.model.UploadSchema;
import org.sagebionetworks.bridge.user.TestUserHelper;
import org.sagebionetworks.bridge.user.TestUserHelper.TestUser;

public class SharedModuleMetadataTest {
    private static final Logger LOG = LoggerFactory.getLogger(SharedModuleMetadataTest.class);

    private static final String MODULE_NAME = "Integ Test Module";
    private static final String NOTES = "These are some notes about a module.";
    private static final String OS = "Unix";
    private static final int SCHEMA_REV = 1;
    private static final String SURVEY_NAME = "dummy-survey-name";
    private static final Long SCHEMA_VERSION = 0L;

    // Note that this is canonically a set. However, Swagger only supports a list, so some wonkiness happens.
    private static final Set<String> TAGS = ImmutableSet.of("foo", "bar", "baz");

    private static ForDevelopersApi apiDeveloperModulesApi;
    private static ForDevelopersApi sharedDeveloperModulesApi;
    private static ForDevelopersApi nonAuthSharedModulesApi;
    private static UploadSchemasApi devUploadSchemasApi;
    private static SurveysApi devSurveysApi;
    private static AuthenticationApi authApi;
    private static ForAdminsApi adminsApi;
    private static SurveysApi adminSurveysApi;
    
    private String moduleId;
    private String schemaId;
    private String surveyGuid;
    private DateTime surveyCreatedOn;
    
    private static TestUser apiDeveloper;
    private static TestUser sharedDeveloper; 

    @BeforeClass
    public static void beforeClass() throws Exception {
        TestUserHelper.TestUser admin = TestUserHelper.getSignedInAdmin();
        apiDeveloper = TestUserHelper.createAndSignInUser(SharedModuleMetadataTest.class, false, DEVELOPER);
        apiDeveloperModulesApi = apiDeveloper.getClient(ForDevelopersApi.class);
        sharedDeveloper = TestUserHelper.createAndSignInUser(SharedModuleMetadataTest.class, SHARED_APP_ID, DEVELOPER);
        sharedDeveloperModulesApi = sharedDeveloper.getClient(ForDevelopersApi.class);
        nonAuthSharedModulesApi = Tests.getUnauthenticatedClientProvider(admin.getClientManager(), TEST_APP_ID)
                .getClient(ForDevelopersApi.class);
        devUploadSchemasApi = sharedDeveloper.getClient(UploadSchemasApi.class);
        devSurveysApi = sharedDeveloper.getClient(SurveysApi.class);
        adminsApi = admin.getClient(ForAdminsApi.class);
        authApi = admin.getClient(AuthenticationApi.class);
        authApi.changeApp(Tests.SHARED_SIGNIN).execute();
        adminSurveysApi = admin.getClient(SurveysApi.class);
    }

    @SuppressWarnings("deprecation")
    @Before
    public void before() throws IOException {
        moduleId = "integ-test-module-" + RandomStringUtils.randomAlphabetic(4);
        schemaId = "dummy-schema-id-" + RandomStringUtils.randomAlphabetic(4);

        // create test upload schema
        UploadSchema uploadSchema = makeSimpleSchema(schemaId, (long)SCHEMA_REV, SCHEMA_VERSION);
        devUploadSchemasApi.createUploadSchema(uploadSchema).execute().body();

        Survey survey = new Survey().name(SURVEY_NAME).identifier(Tests.randomIdentifier(getClass()));
        GuidCreatedOnVersionHolder retSurvey = devSurveysApi.createSurvey(survey).execute().body();

        // modify member var to fit with real survey info
        surveyGuid = retSurvey.getGuid();
        surveyCreatedOn = retSurvey.getCreatedOn();

        // Ensure all tests are consistent by having the admin start in the API app.
        authApi.changeApp(API_SIGNIN).execute();
    }

    @SuppressWarnings("deprecation")
    @After
    public void after() throws Exception {
        authApi.changeApp(SHARED_SIGNIN).execute();
        try {
            adminsApi.deleteMetadataByIdAllVersions(moduleId, true).execute();
        } catch (EntityNotFoundException ex) {
            // Suppress the exception, as the test may have already deleted the module.
        }
        // also delete created upload schema
        try {
            adminsApi.deleteAllRevisionsOfUploadSchema(schemaId, true).execute();
            adminSurveysApi.deleteSurvey(surveyGuid, surveyCreatedOn, true).execute();
        } finally {
            authApi.changeApp(API_SIGNIN).execute();
        }
    }
    
    @AfterClass
    public static void afterClass() throws Exception {
        if (apiDeveloper != null) {
            apiDeveloper.signOutAndDeleteUser();
        }
        if (sharedDeveloper != null) {
            sharedDeveloper.signOutAndDeleteUser();
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testNonAuthUserGetAndQueryCalls() throws Exception {
        // first create a test metadata
        SharedModuleMetadata metadataToCreate = new SharedModuleMetadata().id(moduleId).version(1)
                .name(moduleId + "name").notes(moduleId + "note").schemaId(schemaId).schemaRevision(SCHEMA_REV);
        SharedModuleMetadata metadata = sharedDeveloperModulesApi.createMetadata(metadataToCreate).execute()
                .body();
        // execute query and get
        SharedModuleMetadata retMetadata = nonAuthSharedModulesApi
                .getMetadataByIdAndVersion(metadata.getId(), metadata.getVersion()).execute().body();
        assertEquals(metadata, retMetadata);

        retMetadata = nonAuthSharedModulesApi.getMetadataByIdLatestVersion(metadata.getId()).execute().body();
        assertEquals(metadata, retMetadata);

        SharedModuleMetadataList retMetadataList = nonAuthSharedModulesApi.queryAllMetadata(false,
                false, null, moduleId + "note", null,true).execute().body();
        assertEquals(1, retMetadataList.getItems().size());
        assertEquals(metadata, retMetadataList.getItems().get(0));

        retMetadataList = nonAuthSharedModulesApi.queryAllMetadata(false, false,
                moduleId + "name", null, null, true).execute().body();
        assertEquals(1, retMetadataList.getItems().size());
        assertEquals(metadata, retMetadataList.getItems().get(0));
        
        retMetadataList = nonAuthSharedModulesApi.queryMetadataById(metadata.getId(), true, false, null, null, null, true).execute().body();
        assertEquals(1, retMetadataList.getItems().size());
        assertEquals(metadata, retMetadataList.getItems().get(0));
    }

    @SuppressWarnings("deprecation")
    @Test(expected = BadRequestException.class)
    public void testCreateWithoutSchema() throws Exception {
        String failedSchemaId = "failed-schema-id-" + RandomStringUtils.randomAlphabetic(4);
        SharedModuleMetadata metadataToFail = new SharedModuleMetadata().id(moduleId).name(MODULE_NAME)
                .schemaId(failedSchemaId).schemaRevision(SCHEMA_REV);

        sharedDeveloperModulesApi.createMetadata(metadataToFail).execute();
    }

    @SuppressWarnings("deprecation")
    @Test(expected = BadRequestException.class)
    public void testCreateWithoutSurvey() throws Exception {
        String failedSurveyId = "failed-survey-id-" + RandomStringUtils.randomAlphabetic(4);
        SharedModuleMetadata metadataToFail = new SharedModuleMetadata().id(moduleId).name(MODULE_NAME)
                .surveyGuid(failedSurveyId).surveyCreatedOn(DateTime.now().toString());

        sharedDeveloperModulesApi.createMetadata(metadataToFail).execute();
    }

    @SuppressWarnings("deprecation")
    @Test(expected = BadRequestException.class)
    public void testUpdateWithoutExistSchema() throws Exception {
        SharedModuleMetadata metadataToCreate = new SharedModuleMetadata().id(moduleId).name(MODULE_NAME).version(1)
                .schemaId(schemaId).schemaRevision(SCHEMA_REV);
        sharedDeveloperModulesApi.createMetadata(metadataToCreate).execute();

        String failedSchemaId = "failed-schema-id-" + RandomStringUtils.randomAlphabetic(4);
        SharedModuleMetadata metadataToUpdate = new SharedModuleMetadata().id(moduleId).name(MODULE_NAME).version(1)
                .schemaId(failedSchemaId).schemaRevision(SCHEMA_REV);

        sharedDeveloperModulesApi.updateMetadata(moduleId, 1, metadataToUpdate).execute();
    }

    @SuppressWarnings("deprecation")
    @Test(expected = BadRequestException.class)
    public void testUpdateWithoutExistSurvey() throws Exception {
        SharedModuleMetadata metadataToCreate = new SharedModuleMetadata().id(moduleId).name(MODULE_NAME).version(1)
                .surveyCreatedOn(surveyCreatedOn.toString()).surveyGuid(surveyGuid);
        sharedDeveloperModulesApi.createMetadata(metadataToCreate).execute();

        String failedSurveyId = "failed-survey-id-" + RandomStringUtils.randomAlphabetic(4);
        SharedModuleMetadata metadataToUpdate = new SharedModuleMetadata().id(moduleId).name(MODULE_NAME).version(1)
                .surveyCreatedOn(surveyCreatedOn.toString()).surveyGuid(failedSurveyId);

        sharedDeveloperModulesApi.updateMetadata(moduleId, 1, metadataToUpdate).execute();
    }

    @SuppressWarnings("deprecation")
    @Test
    public void crud() throws Exception {
        // Create a bunch of versions. This test various cases of version auto-incrementing and explicitly setting
        // versions.
        SharedModuleMetadata metadataV1 = testCreateGet(1, null);
        SharedModuleMetadata metadataV2 = testCreateGet(2, null);
        SharedModuleMetadata metadataV4 = testCreateGet(4, 4);
        SharedModuleMetadata metadataV6 = testCreateGet(6, 6);

        // Attempt to create v6 again. Will throw.
        try {
            sharedDeveloperModulesApi.createMetadata(metadataV6).execute();
            fail("expected exception");
        } catch (ConcurrentModificationException ex) {
            // expected exception
        }

        // Update v6. Verify the changes.
        SharedModuleMetadata metadataToUpdateV6 = new SharedModuleMetadata().id(moduleId).version(6)
                .name("Updated Module").schemaId(null).schemaRevision(null).surveyCreatedOn(surveyCreatedOn.toString())
                .surveyGuid(surveyGuid);
        SharedModuleMetadata updatedMetadataV6 = sharedDeveloperModulesApi.updateMetadata(moduleId, 6,
                metadataToUpdateV6).execute().body();
        assertEquals(moduleId, updatedMetadataV6.getId());
        assertEquals(6, updatedMetadataV6.getVersion().intValue());
        assertEquals("Updated Module", updatedMetadataV6.getName());
        assertNull(updatedMetadataV6.getSchemaId());
        assertNull(updatedMetadataV6.getSchemaRevision());
        assertEquals(surveyCreatedOn.toString(), updatedMetadataV6.getSurveyCreatedOn());
        assertEquals(surveyGuid, updatedMetadataV6.getSurveyGuid());

        // Get latest. Verify it's same as the updated version.
        SharedModuleMetadata gettedUpdatedMetadataV6 = sharedDeveloperModulesApi.getMetadataByIdLatestVersion(moduleId)
                .execute().body();
        assertEquals(updatedMetadataV6, gettedUpdatedMetadataV6);

        // Get by ID and version each version. Verify it's the same as before.
        SharedModuleMetadata gettedByIdAndVersionV1 = sharedDeveloperModulesApi.getMetadataByIdAndVersion(moduleId, 1)
                .execute().body();
        assertEquals(metadataV1, gettedByIdAndVersionV1);

        SharedModuleMetadata gettedByIdAndVersionV2 = sharedDeveloperModulesApi.getMetadataByIdAndVersion(moduleId, 2)
                .execute().body();
        assertEquals(metadataV2, gettedByIdAndVersionV2);

        SharedModuleMetadata gettedByIdAndVersionV4 = sharedDeveloperModulesApi.getMetadataByIdAndVersion(moduleId, 4)
                .execute().body();
        assertEquals(metadataV4, gettedByIdAndVersionV4);

        SharedModuleMetadata gettedByIdAndVersionV6 = sharedDeveloperModulesApi.getMetadataByIdAndVersion(moduleId, 6)
                .execute().body();
        assertEquals(updatedMetadataV6, gettedByIdAndVersionV6);

        // Delete v2. Latest is still v6.
        authApi.changeApp(SHARED_SIGNIN).execute();
        adminsApi.deleteMetadataByIdAndVersion(moduleId, 2, true).execute();
        SharedModuleMetadata gettedLatestAfterDeleteV2 = sharedDeveloperModulesApi.getMetadataByIdLatestVersion(
                moduleId).execute().body();
        assertEquals(updatedMetadataV6, gettedLatestAfterDeleteV2);

        // Delete v6. Latest is now v4.
        adminsApi.deleteMetadataByIdAndVersion(moduleId, 6, true).execute();
        SharedModuleMetadata gettedLatestAfterDeleteV6 = sharedDeveloperModulesApi.getMetadataByIdLatestVersion(
                moduleId).execute().body();
        assertEquals(metadataV4, gettedLatestAfterDeleteV6);

        // Delete all. Query by ID now returns an empty list.
        adminsApi.deleteMetadataByIdAllVersions(moduleId, true).execute();
        List<SharedModuleMetadata> metadataListAfterDeleteAll = sharedDeveloperModulesApi
                .queryMetadataById(moduleId, false, false, null, null, null, true).execute().body().getItems();
        assertEquals(0, metadataListAfterDeleteAll.size());
    }

    // Test helper to test create API and verify the get APIs return the expected result. Returns the created module
    // metadata.
    @SuppressWarnings("deprecation")
    private SharedModuleMetadata testCreateGet(int expectedVersion, Integer inputVersion) throws Exception {
        // create
        SharedModuleMetadata metadataToCreate = new SharedModuleMetadata().id(moduleId).version(inputVersion)
                .name(MODULE_NAME).schemaId(schemaId).schemaRevision(SCHEMA_REV);
        SharedModuleMetadata createdMetadata = sharedDeveloperModulesApi.createMetadata(metadataToCreate).execute()
                .body();
        assertEquals(moduleId, createdMetadata.getId());
        assertEquals(expectedVersion, createdMetadata.getVersion().intValue());
        assertEquals(MODULE_NAME, createdMetadata.getName());
        assertEquals(schemaId, createdMetadata.getSchemaId());
        assertEquals(SCHEMA_REV, createdMetadata.getSchemaRevision().intValue());

        // get latest, make sure it matches
        SharedModuleMetadata gettedLatestMetadata = sharedDeveloperModulesApi.getMetadataByIdLatestVersion(moduleId)
                .execute().body();
        assertEquals(createdMetadata, gettedLatestMetadata);

        return createdMetadata;
    }

    @SuppressWarnings("deprecation")
    @Test
    public void schemaModule() throws Exception {
        SharedModuleMetadata metadataToCreate = new SharedModuleMetadata().id(moduleId).name(MODULE_NAME)
                .schemaId(schemaId).schemaRevision(SCHEMA_REV);
        SharedModuleMetadata createdMetadata = sharedDeveloperModulesApi.createMetadata(metadataToCreate).execute()
                .body();
        assertEquals(moduleId, createdMetadata.getId());
        assertNotNull(createdMetadata.getVersion());
        assertEquals(MODULE_NAME, createdMetadata.getName());
        assertEquals(SharedModuleType.SCHEMA, createdMetadata.getModuleType());
        assertEquals(schemaId, createdMetadata.getSchemaId());
        assertEquals(SCHEMA_REV, createdMetadata.getSchemaRevision().intValue());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void surveyModule() throws Exception {
        SharedModuleMetadata metadataToCreate = new SharedModuleMetadata().id(moduleId).name(MODULE_NAME)
                .surveyCreatedOn(surveyCreatedOn.toString()).surveyGuid(surveyGuid);
        SharedModuleMetadata createdMetadata = sharedDeveloperModulesApi.createMetadata(metadataToCreate).execute()
                .body();
        assertEquals(moduleId, createdMetadata.getId());
        assertNotNull(createdMetadata.getVersion());
        assertEquals(MODULE_NAME, createdMetadata.getName());
        assertEquals(SharedModuleType.SURVEY, createdMetadata.getModuleType());
        assertEquals(surveyCreatedOn.toString(), createdMetadata.getSurveyCreatedOn());
        assertEquals(surveyGuid, createdMetadata.getSurveyGuid());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void optionalParams() throws Exception {
        SharedModuleMetadata metadataToCreate = new SharedModuleMetadata().id(moduleId).version(2).name(MODULE_NAME)
                .licenseRestricted(true).notes(NOTES).os(OS).published(true).schemaId(schemaId)
                .schemaRevision(SCHEMA_REV).tags(ImmutableList.copyOf(TAGS));
        SharedModuleMetadata createdMetadata = sharedDeveloperModulesApi.createMetadata(metadataToCreate).execute()
                .body();
        assertEquals(moduleId, createdMetadata.getId());
        assertEquals(2, createdMetadata.getVersion().intValue());
        assertEquals(MODULE_NAME, createdMetadata.getName());
        assertTrue(createdMetadata.isLicenseRestricted());
        assertEquals(SharedModuleType.SCHEMA, createdMetadata.getModuleType());
        assertEquals(NOTES, createdMetadata.getNotes());
        assertEquals(OS, createdMetadata.getOs());
        assertTrue(createdMetadata.isPublished());
        assertEquals(schemaId, createdMetadata.getSchemaId());
        assertEquals(SCHEMA_REV, createdMetadata.getSchemaRevision().intValue());
        assertEquals(TAGS, ImmutableSet.copyOf(createdMetadata.getTags()));
        assertEquals("SharedModuleMetadata", createdMetadata.getType());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void queryAll() throws Exception {
        // Create a few modules for the test
        SharedModuleMetadata moduleAV1ToCreate = new SharedModuleMetadata().id(moduleId + "A").version(1)
                .name("Module A Version 1").notes("Module A Version 1").schemaId(schemaId).schemaRevision(SCHEMA_REV)
                .published(true).os("iOS").addTagsItem("foo");
        SharedModuleMetadata moduleAV1 = sharedDeveloperModulesApi.createMetadata(moduleAV1ToCreate).execute().body();

        SharedModuleMetadata moduleAV2ToCreate = new SharedModuleMetadata().id(moduleId + "A").version(2)
                .name("Module A Version 2").schemaId(schemaId).schemaRevision(SCHEMA_REV).published(false).os("iOS");
        SharedModuleMetadata moduleAV2 = sharedDeveloperModulesApi.createMetadata(moduleAV2ToCreate).execute().body();

        SharedModuleMetadata moduleBV1ToCreate = new SharedModuleMetadata().id(moduleId + "B").version(1)
                .name("Module B Version 1").schemaId(schemaId).schemaRevision(SCHEMA_REV).published(true)
                .os("Android").notes("Android").addTagsItem("bar");
        SharedModuleMetadata moduleBV1 = sharedDeveloperModulesApi.createMetadata(moduleBV1ToCreate).execute().body();

        SharedModuleMetadata moduleBV2ToCreate = new SharedModuleMetadata().id(moduleId + "B").version(2)
                .name("Module B Version 2").schemaId(schemaId).schemaRevision(SCHEMA_REV).published(true)
                .os("Android").notes("Android");
        SharedModuleMetadata moduleBV2 = sharedDeveloperModulesApi.createMetadata(moduleBV2ToCreate).execute().body();

        // These cases originally tested the ability to find by operating system and licensing restrictions. These are 
        // temporarily removed from the API (we can add them back when this feature is put into widespread use). 
        // Right now the only consuming client (the BSM) does a search of the notes and name fields of shared modules.
        try {
            // Case 1: query with both mostrecent=true and where throws exception
            try {
                sharedDeveloperModulesApi.queryAllMetadata(true, false, null, "will not match", null, true).execute();
                fail("expected exception");
            } catch (BadRequestException ex) {
                // expected exception
            }

            // Note that since there may be other modules in the shared app, we can't rely on result counts. Instead,
            // well need to test for the presence and absence of our test modules.

            // Case 2: most recent published (returns AV1 and BV2)
            List<SharedModuleMetadata> case3MetadataList = sharedDeveloperModulesApi
                    .queryAllMetadata(true, true, null, null, null, true).execute().body().getItems();
            assertTrue(case3MetadataList.contains(moduleAV1));
            assertFalse(case3MetadataList.contains(moduleAV2));
            assertFalse(case3MetadataList.contains(moduleBV1));
            assertTrue(case3MetadataList.contains(moduleBV2));

            // Case 3: most recent (returns AV2 and BV2)
            List<SharedModuleMetadata> case4MetadataList = sharedDeveloperModulesApi
                    .queryAllMetadata(true, false, null, null, null, true).execute().body().getItems();
            assertFalse(case4MetadataList.contains(moduleAV1));
            assertTrue(case4MetadataList.contains(moduleAV2));
            assertFalse(case4MetadataList.contains(moduleBV1));
            assertTrue(case4MetadataList.contains(moduleBV2));

            // Case 4: published, where notes are matched (returns AV1)
            List<SharedModuleMetadata> case5MetadataList = sharedDeveloperModulesApi
                    .queryAllMetadata(false, true, null, "Module A Version 1", null, true).execute().body().getItems();
            assertTrue(case5MetadataList.contains(moduleAV1));
            assertFalse(case5MetadataList.contains(moduleAV2));
            assertFalse(case5MetadataList.contains(moduleBV1));
            assertFalse(case5MetadataList.contains(moduleBV2));

            // Case 5: published, no where clause (returns AV1, BV1, BV2)
            List<SharedModuleMetadata> case6MetadataList = sharedDeveloperModulesApi
                    .queryAllMetadata(false, true, null, null, null, true).execute().body().getItems();
            assertTrue(case6MetadataList.contains(moduleAV1));
            assertFalse(case6MetadataList.contains(moduleAV2));
            assertTrue(case6MetadataList.contains(moduleBV1));
            assertTrue(case6MetadataList.contains(moduleBV2));

            // Case 6: where notes contain "Android" (returns BV1, BV2)
            List<SharedModuleMetadata> case7MetadataList = sharedDeveloperModulesApi
                    .queryAllMetadata(false, false, null, "Android", null, true).execute().body().getItems();
            assertFalse(case7MetadataList.contains(moduleAV1));
            assertFalse(case7MetadataList.contains(moduleAV2));
            assertTrue(case7MetadataList.contains(moduleBV1));
            assertTrue(case7MetadataList.contains(moduleBV2));

            // Case 7: where notes contain 'Android', tags=bar (returns BV1)
            List<SharedModuleMetadata> case8MetadataList = sharedDeveloperModulesApi
                    .queryAllMetadata(false, false, null, "Android", "bar", true).execute().body().getItems();
            assertFalse(case8MetadataList.contains(moduleAV1));
            assertFalse(case8MetadataList.contains(moduleAV2));
            assertTrue(case8MetadataList.contains(moduleBV1));
            assertFalse(case8MetadataList.contains(moduleBV2));

            // Case 8: multiple tags (returns AV1, BV1)
            List<SharedModuleMetadata> case9MetadataList = sharedDeveloperModulesApi
                    .queryAllMetadata(false, false, null, null, "foo,bar", true).execute().body().getItems();
            assertTrue(case9MetadataList.contains(moduleAV1));
            assertFalse(case9MetadataList.contains(moduleAV2));
            assertTrue(case9MetadataList.contains(moduleBV1));
            assertFalse(case9MetadataList.contains(moduleBV2));

            // Case 9: all results
            List<SharedModuleMetadata> case10MetadataList = sharedDeveloperModulesApi
                    .queryAllMetadata(false, false, null, null, null, true).execute().body().getItems();
            assertTrue(case10MetadataList.contains(moduleAV1));
            assertTrue(case10MetadataList.contains(moduleAV2));
            assertTrue(case10MetadataList.contains(moduleBV1));
            assertTrue(case10MetadataList.contains(moduleBV2));

            // Case 10: no results
            List<SharedModuleMetadata> case11MetadataList = sharedDeveloperModulesApi
                    .queryAllMetadata(false, false, "matches no name", null, null, true).execute().body().getItems();
            assertFalse(case11MetadataList.contains(moduleAV1));
            assertFalse(case11MetadataList.contains(moduleAV2));
            assertFalse(case11MetadataList.contains(moduleBV1));
            assertFalse(case11MetadataList.contains(moduleBV2));
            
            // Logically delete a couple of versions of a couple of modules
            sharedDeveloperModulesApi.deleteMetadataByIdAndVersion(moduleAV1.getId(), moduleAV1.getVersion(), false).execute();
            sharedDeveloperModulesApi.deleteMetadataByIdAndVersion(moduleBV2.getId(), moduleBV2.getVersion(), false).execute();
            
            // Retrieve with logically deleted included, all are included
            List<SharedModuleMetadata> case12MetadataList = sharedDeveloperModulesApi
                    .queryAllMetadata(false, false, null, null, null, true).execute().body().getItems();
            // contains does not work for this test because the returned items are not equal 
            assertTrue(moduleMetadataListContains(case12MetadataList, moduleAV1));
            assertTrue(moduleMetadataListContains(case12MetadataList, moduleAV2));
            assertTrue(moduleMetadataListContains(case12MetadataList, moduleBV1));
            assertTrue(moduleMetadataListContains(case12MetadataList, moduleBV2));

            assertTrue(findMetadata(case12MetadataList, moduleAV1).isDeleted());
            assertTrue(findMetadata(case12MetadataList, moduleBV2).isDeleted());

            List<SharedModuleMetadata> case13MetadataList = sharedDeveloperModulesApi
                    .queryAllMetadata(false, false, null, null, null, false).execute().body().getItems();
            assertFalse(moduleMetadataListContains(case13MetadataList, moduleAV1));
            assertTrue(moduleMetadataListContains(case13MetadataList, moduleAV2));
            assertTrue(moduleMetadataListContains(case13MetadataList, moduleBV1));
            assertFalse(moduleMetadataListContains(case13MetadataList, moduleBV2));
            
            // Verify physical delete
            authApi.changeApp(SHARED_SIGNIN).execute();
            adminsApi.deleteMetadataByIdAndVersion(moduleAV1.getId(), moduleAV1.getVersion(), true).execute();
            try {
                sharedDeveloperModulesApi.getMetadataByIdAndVersion(moduleAV1.getId(), moduleAV1.getVersion()).execute();
                fail("Should have thrown exception");
            } catch(EntityNotFoundException e) {
            }
        } finally {
            authApi.changeApp(SHARED_SIGNIN).execute();
            try {
                adminsApi.deleteMetadataByIdAllVersions(moduleId + "A", true).execute();
            } catch (BridgeSDKException ex) {
                LOG.error("Error deleting module " + moduleId + "A: " + ex.getMessage(), ex);
            }

            try {
                adminsApi.deleteMetadataByIdAllVersions(moduleId + "B", true).execute();
            } catch (BridgeSDKException ex) {
                LOG.error("Error deleting module " + moduleId + "B: " + ex.getMessage(), ex);
            }
        }
    }
    
    private boolean moduleMetadataListContains(List<SharedModuleMetadata> list, SharedModuleMetadata metadata) {
        return findMetadata(list, metadata) != null;
    }
    
    private SharedModuleMetadata findMetadata(List<SharedModuleMetadata> list, SharedModuleMetadata metadata) {
        for (SharedModuleMetadata oneMetadata : list) {
            if (oneMetadata.getId().equals(metadata.getId()) && oneMetadata.getVersion().equals(metadata.getVersion())) {
                return oneMetadata;
            }
        }
        return null;
    }

    @SuppressWarnings("deprecation")
    @Test
    public void queryById() throws Exception {
        // Note that full query logic is tested in queryAll(). This tests an abbreviated set of logic to make sure
        // everything is plumbed through correctly.

        // Create a few module versions for test. Create a module with a different ID to make sure it's not also being
        // returned.
        SharedModuleMetadata moduleV1ToCreate = new SharedModuleMetadata().id(moduleId).version(1)
                .name("Test Module Version 1").schemaId(schemaId).schemaRevision(SCHEMA_REV).published(true)
                .licenseRestricted(true);
        SharedModuleMetadata moduleV1 = sharedDeveloperModulesApi.createMetadata(moduleV1ToCreate).execute().body();

        SharedModuleMetadata moduleV2ToCreate = new SharedModuleMetadata().id(moduleId).version(2)
                .name("Test Module Version 2").schemaId(schemaId).schemaRevision(SCHEMA_REV).published(true);
        SharedModuleMetadata moduleV2 = sharedDeveloperModulesApi.createMetadata(moduleV2ToCreate).execute().body();

        SharedModuleMetadata moduleV3ToCreate = new SharedModuleMetadata().id(moduleId).version(3)
                .name("Test Module Version 3").schemaId(schemaId).schemaRevision(SCHEMA_REV).published(false)
                .addTagsItem("foo");
        SharedModuleMetadata moduleV3 = sharedDeveloperModulesApi.createMetadata(moduleV3ToCreate).execute().body();

        SharedModuleMetadata moduleV4ToCreate = new SharedModuleMetadata().id(moduleId).version(4)
                .name("Test Module Version 4").schemaId(schemaId).schemaRevision(SCHEMA_REV).published(false);
        SharedModuleMetadata moduleV4 = sharedDeveloperModulesApi.createMetadata(moduleV4ToCreate).execute().body();

        SharedModuleMetadata otherModuleToCreate = new SharedModuleMetadata().id(moduleId + "other").version(1)
                .name("Other Module").schemaId(schemaId).schemaRevision(SCHEMA_REV);
        sharedDeveloperModulesApi.createMetadata(otherModuleToCreate).execute().body();

        try {
            // Note that since we are query by module ID, and module ID is relatively unique to this test, we *can*
            // use counts to know exactly what results we expect.

            // Case 1: most recent (v4)
            List<SharedModuleMetadata> case1MetadataList = sharedDeveloperModulesApi
                    .queryMetadataById(moduleId, true, false, null, null, null, true).execute().body().getItems();
            assertEquals(1, case1MetadataList.size());
            assertTrue(case1MetadataList.contains(moduleV4));

            // Case 2: published (v1, v2)
            List<SharedModuleMetadata> case2MetadataList = sharedDeveloperModulesApi
                    .queryMetadataById(moduleId, false, true, null, null, null, true).execute().body().getItems();
            assertEquals(2, case2MetadataList.size());
            assertTrue(case2MetadataList.contains(moduleV1));
            assertTrue(case2MetadataList.contains(moduleV2));

            // Case 3: where licenseRestricted=true (v1)
            List<SharedModuleMetadata> case3MetadataList = sharedDeveloperModulesApi
                    .queryMetadataById(moduleId, false, false, "Test Module Version 1", null, null, true).execute().body()
                    .getItems();
            assertEquals(1, case3MetadataList.size());
            assertTrue(case3MetadataList.contains(moduleV1));

            // Case 4: tags=foo (v3)
            List<SharedModuleMetadata> case4MetadataList = sharedDeveloperModulesApi
                    .queryMetadataById(moduleId, false, false, null, null, "foo", true).execute().body().getItems();
            assertEquals(1, case4MetadataList.size());
            assertTrue(case4MetadataList.contains(moduleV3));
        } finally {
            try {
                authApi.changeApp(SHARED_SIGNIN).execute();
                adminsApi.deleteMetadataByIdAllVersions(moduleId + "other", true).execute();
            } catch (BridgeSDKException ex) {
                LOG.error("Error deleting module " + moduleId + "other: " + ex.getMessage(), ex);
            }
        }
    }

    @SuppressWarnings("deprecation")
    @Test(expected = EntityNotFoundException.class)
    public void deleteByIdAllVersions404() throws Exception {
        authApi.changeApp(SHARED_SIGNIN).execute();
        adminsApi.deleteMetadataByIdAllVersions(moduleId, true).execute();
    }

    @SuppressWarnings("deprecation")
    @Test(expected = EntityNotFoundException.class)
    public void deleteByIdAndVersion404() throws Exception {
        authApi.changeApp(SHARED_SIGNIN).execute();
        adminsApi.deleteMetadataByIdAndVersion(moduleId, 1, true).execute();
    }

    @SuppressWarnings("deprecation")
    @Test(expected = EntityNotFoundException.class)
    public void getByIdAndVersion404() throws Exception {
        sharedDeveloperModulesApi.getMetadataByIdAndVersion(moduleId, 1).execute();
    }

    @SuppressWarnings("deprecation")
    @Test(expected = EntityNotFoundException.class)
    public void getByIdLatest404() throws Exception {
        sharedDeveloperModulesApi.getMetadataByIdLatestVersion(moduleId).execute();
    }

    @SuppressWarnings("deprecation")
    @Test(expected = EntityNotFoundException.class)
    public void update404() throws Exception {
        SharedModuleMetadata metadata = new SharedModuleMetadata().id(moduleId).version(1).name(MODULE_NAME)
                .schemaId(schemaId).schemaRevision(SCHEMA_REV);
        sharedDeveloperModulesApi.updateMetadata(moduleId, 1, metadata).execute();
    }

    @SuppressWarnings("deprecation")
    @Test(expected = UnauthorizedException.class)
    public void nonSharedDeveloperCantCreate() throws Exception {
        SharedModuleMetadata metadata = new SharedModuleMetadata().id(moduleId).version(1).name(MODULE_NAME)
                .schemaId(schemaId).schemaRevision(SCHEMA_REV);
        apiDeveloperModulesApi.createMetadata(metadata).execute();
    }

    @SuppressWarnings("deprecation")
    @Test(expected = UnauthorizedException.class)
    public void nonSharedDeveloperCantDeleteByIdAllVersions() throws Exception {
        adminsApi.deleteMetadataByIdAllVersions(moduleId, true).execute();
    }

    @SuppressWarnings("deprecation")
    @Test(expected = UnauthorizedException.class)
    public void nonSharedDeveloperCantDeleteByIdAndVersion() throws Exception {
        adminsApi.deleteMetadataByIdAndVersion(moduleId, 1, true).execute();
    }

    @SuppressWarnings("deprecation")
    @Test(expected = UnauthorizedException.class)
    public void nonSharedDeveloperCantUpdate() throws Exception {
        SharedModuleMetadata metadata = new SharedModuleMetadata().id(moduleId).version(1).name(MODULE_NAME)
                .schemaId(schemaId).schemaRevision(SCHEMA_REV);
        apiDeveloperModulesApi.updateMetadata(moduleId, 1, metadata).execute();
    }
    
    @SuppressWarnings("deprecation")
    @Test
    public void logicalDelete() throws Exception {
        try {
            sharedDeveloperModulesApi.createMetadata(new SharedModuleMetadata().id(moduleId + "A").version(1)
                    .name("Module A Version 1").schemaId(schemaId).schemaRevision(SCHEMA_REV)).execute().body();
    
            sharedDeveloperModulesApi.createMetadata(new SharedModuleMetadata().id(moduleId + "A").version(2)
                    .name("Module A Version 2").schemaId(schemaId).schemaRevision(SCHEMA_REV)).execute().body();
    
            sharedDeveloperModulesApi.createMetadata(new SharedModuleMetadata().id(moduleId + "B").version(1)
                    .name("Module B Version 1").schemaId(schemaId).schemaRevision(SCHEMA_REV)).execute().body();
    
            sharedDeveloperModulesApi.createMetadata(new SharedModuleMetadata().id(moduleId + "B").version(2)
                    .name("Module B Version 2").schemaId(schemaId).schemaRevision(SCHEMA_REV)).execute().body();
            
            // delete one of the As and all of the Bs logically
            sharedDeveloperModulesApi.deleteMetadataByIdAndVersion(moduleId + "A", 1, false).execute();
            sharedDeveloperModulesApi.deleteMetadataByIdAllVersions(moduleId + "B", false).execute();
            
            SharedModuleMetadata metadataA = sharedDeveloperModulesApi.getMetadataByIdLatestVersion(moduleId + "A").execute().body();
            assertEquals(new Integer(2), metadataA.getVersion());
            
            SharedModuleMetadataList list = sharedDeveloperModulesApi.queryMetadataById(moduleId + "A", null, 
                    null, null, null, null, false).execute().body();
            assertEquals(1, list.getItems().size());
            
            list = sharedDeveloperModulesApi.queryMetadataById(moduleId + "A", false, false, null, null, null, true)
                    .execute().body();
            assertEquals(2, list.getItems().size());
            
            list = sharedDeveloperModulesApi.queryMetadataById(moduleId + "B", false, false, null, null, null, false)
                    .execute().body();
            assertEquals(0, list.getItems().size());
            
            list = sharedDeveloperModulesApi.queryMetadataById(moduleId + "B", false, false, null, null, null, true)
                    .execute().body();
            assertEquals(2, list.getItems().size());

        } finally {
            authApi.changeApp(SHARED_SIGNIN).execute();
            adminsApi.deleteMetadataByIdAllVersions(moduleId + "A", true).execute();
            adminsApi.deleteMetadataByIdAllVersions(moduleId + "B", true).execute();
        }
    }
}
