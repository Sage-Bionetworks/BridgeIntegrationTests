package org.sagebionetworks.bridge.sdk.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.sagebionetworks.bridge.rest.model.Role.DEVELOPER;
import static org.sagebionetworks.bridge.sdk.integration.Tests.API_SIGNIN;
import static org.sagebionetworks.bridge.sdk.integration.Tests.SHARED_SIGNIN;
import static org.sagebionetworks.bridge.util.IntegTestUtils.SHARED_APP_ID;

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
import org.sagebionetworks.bridge.rest.model.GuidCreatedOnVersionHolder;
import org.sagebionetworks.bridge.rest.model.SharedModuleImportStatus;
import org.sagebionetworks.bridge.rest.model.SharedModuleMetadata;
import org.sagebionetworks.bridge.rest.model.SharedModuleType;
import org.sagebionetworks.bridge.rest.model.Survey;
import org.sagebionetworks.bridge.rest.model.SurveyElement;
import org.sagebionetworks.bridge.rest.model.UploadFieldDefinition;
import org.sagebionetworks.bridge.rest.model.UploadFieldType;
import org.sagebionetworks.bridge.rest.model.UploadSchema;
import org.sagebionetworks.bridge.rest.model.UploadSchemaType;
import org.sagebionetworks.bridge.user.TestUser;
import org.sagebionetworks.bridge.user.TestUserHelper;

public class SharedModuleTest {
    private static final Logger LOG = LoggerFactory.getLogger(SharedModuleTest.class);

    private static TestUser admin;
    private static TestUser apiDeveloper;
    private static TestUser sharedDeveloper;

    private SharedModuleMetadata module;
    private UploadSchema sharedSchema;
    private UploadSchema localSchema;
    private Survey sharedSurvey;
    private Survey localSurvey;

    @BeforeClass
    public static void beforeClass() throws Exception {
        admin = TestUserHelper.getSignedInAdmin();
        apiDeveloper = TestUserHelper.createAndSignInUser(SharedModuleTest.class, false, DEVELOPER);
        sharedDeveloper = TestUserHelper.createAndSignInUser(SharedModuleTest.class, SHARED_APP_ID, DEVELOPER);
    }

    @Before
    public void clearVars() {
        // Sometimes, JUnit doesn't clear test vars, so we need to clear them explicitly.
        module = null;
        sharedSchema = null;
        localSchema = null;
        sharedSurvey = null;
        localSurvey = null;
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
    @After
    public void deleteTestObjects() throws Exception {
        // Delete schemas created by test. We do it in a single After method instead of multiple, in case there are any
        // referential integrity concerns.
        ForAdminsApi adminApi = admin.getClient(ForAdminsApi.class);
        AuthenticationApi authApi = admin.getClient(AuthenticationApi.class);

        if (module != null) {
            try {
                authApi.changeApp(SHARED_SIGNIN).execute();
                adminApi.deleteMetadataByIdAllVersions(module.getId(), true).execute();
            } catch (BridgeSDKException ex) {
                LOG.error("Error deleting module " + module.getId() + ": "  + ex.getMessage(), ex);
            } finally {
                authApi.changeApp(API_SIGNIN).execute();
            }
        }

        if (localSchema != null) {
            try {
                adminApi.deleteAllRevisionsOfUploadSchema(localSchema.getSchemaId(), true)
                        .execute();
            } catch (BridgeSDKException ex) {
                LOG.error("Error deleting schema " + localSchema.getSchemaId() + " in app " +
                        apiDeveloper.getAppId() + ": "  + ex.getMessage(), ex);
            }
        }

        if (sharedSchema != null) {
            try {
                authApi.changeApp(SHARED_SIGNIN).execute();
                adminApi.deleteAllRevisionsOfUploadSchema(sharedSchema.getSchemaId(), true).execute();
            } catch (BridgeSDKException ex) {
                LOG.error("Error deleting schema " + sharedSchema.getSchemaId() + " in app " +
                        sharedDeveloper.getAppId() + ": "  + ex.getMessage(), ex);
            } finally {
                authApi.changeApp(API_SIGNIN).execute();
            }
        }

        if (localSurvey != null) {
            try {
                adminApi.deleteSurvey(localSurvey.getGuid(), localSurvey.getCreatedOn(), true).execute();
            } catch (BridgeSDKException ex) {
                LOG.error("Error deleting local survey " + sharedSurvey.getGuid() + ": "  + ex.getMessage(), ex);
            }
        }

        if (sharedSurvey != null) {
            try {
                authApi.changeApp(SHARED_SIGNIN).execute();
                adminApi.deleteSurvey(sharedSurvey.getGuid(), sharedSurvey.getCreatedOn(), true).execute();
            } catch (BridgeSDKException ex) {
                LOG.error("Error deleting shared survey " + sharedSurvey.getGuid() + ": "  + ex.getMessage(), ex);
            } finally {
                authApi.changeApp(API_SIGNIN).execute();
            }
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void byIdAndVersionSchemaSuccess() throws Exception {
        // Create shared schema and module.
        sharedSchema = createSharedSchema();
        module = createModuleForSchema(sharedSchema);

        // Copy to local app.
        SharedModuleImportStatus importStatus = apiDeveloper.getClient(ForDevelopersApi.class)
                .importModuleByIdAndVersion(module.getId(), module.getVersion()).execute().body();

        // Get local schema and verify some fields.
        localSchema = apiDeveloper.getClient(UploadSchemasApi.class).getUploadSchema(importStatus.getSchemaId(),
                importStatus.getSchemaRevision().longValue()).execute().body();

        assertLocalSchema(sharedSchema, localSchema, module);

        // import status should say schema
        assertEquals(SharedModuleType.SCHEMA, importStatus.getModuleType());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void byIdAndVersionSurveySuccess() throws Exception {
        // Create shared survey and module.
        sharedSurvey = createAndPublishSharedSurvey();
        module = createModuleForSurvey(sharedSurvey);

        // Copy to local app.
        SharedModuleImportStatus importStatus = apiDeveloper.getClient(ForDevelopersApi.class)
                .importModuleByIdAndVersion(module.getId(), module.getVersion()).execute().body();

        // Get local survey and verify some fields.
        localSurvey = apiDeveloper.getClient(SurveysApi.class).getSurvey(importStatus.getSurveyGuid(),
                DateTime.parse(importStatus.getSurveyCreatedOn())).execute().body();
        assertLocalSurvey(sharedSurvey, localSurvey, module);

        // import status should say survey
        assertEquals(SharedModuleType.SURVEY, importStatus.getModuleType());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void latestPublishedVersionSuccess() throws Exception {
        // Create shared schema and module.
        sharedSchema = createSharedSchema();
        module = createModuleForSchema(sharedSchema);

        // Copy to local app.
        SharedModuleImportStatus importStatus = apiDeveloper.getClient(ForDevelopersApi.class)
                .importModuleByIdLatestPublishedVersion(module.getId()).execute().body();

        // Get local schema and verify some fields.
        localSchema = apiDeveloper.getClient(UploadSchemasApi.class).getUploadSchema(importStatus.getSchemaId(),
                importStatus.getSchemaRevision().longValue()).execute().body();

        assertLocalSchema(sharedSchema, localSchema, module);

        // import status should say schema
        assertEquals(SharedModuleType.SCHEMA, importStatus.getModuleType());
    }

    // Helper method to create a schema in the shared module library. Returns the created schema.
    @SuppressWarnings("deprecation")
    private static UploadSchema createSharedSchema() throws Exception {
        String schemaId = "shared-module-test-schema-" + RandomStringUtils.randomAlphabetic(4);
        UploadFieldDefinition fieldDef = new UploadFieldDefinition().name("foo").type(UploadFieldType.INT);
        UploadSchema schemaToCreate = new UploadSchema().schemaId(schemaId).name("Shared Module Test Schema")
                .schemaType(UploadSchemaType.IOS_DATA).addFieldDefinitionsItem(fieldDef);
        return sharedDeveloper.getClient(UploadSchemasApi.class).createUploadSchema(schemaToCreate).execute().body();
    }

    // Helper method to create a shared module referencing the given schema. Returns the created module.
    @SuppressWarnings("deprecation")
    private static SharedModuleMetadata createModuleForSchema(UploadSchema schema) throws Exception {
        String moduleId = "test-module-" + RandomStringUtils.randomAlphabetic(4);
        SharedModuleMetadata moduleToCreate = new SharedModuleMetadata().id(moduleId).name("Test Module With Schema")
                .published(true).schemaId(schema.getSchemaId()).schemaRevision(schema.getRevision().intValue());
        return sharedDeveloper.getClient(ForDevelopersApi.class).createMetadata(moduleToCreate).execute().body();
    }

    // Helper method to verify shared schema and local schema match. Because the schemas are in different studies, they
    // might not match completely, but the fields we created should match. (Be sure to test non-index-key fields.)
    @SuppressWarnings("deprecation")
    private static void assertLocalSchema(UploadSchema sharedSchema, UploadSchema localSchema,
            SharedModuleMetadata module) throws Exception {
        assertEquals(sharedSchema.getSchemaId(), localSchema.getSchemaId());
        assertEquals(sharedSchema.getRevision(), localSchema.getRevision());
        assertEquals(sharedSchema.getName(), localSchema.getName());
        assertEquals(sharedSchema.getSchemaType(), localSchema.getSchemaType());
        assertEquals(sharedSchema.getFieldDefinitions(), localSchema.getFieldDefinitions());

        assertEquals(module.getId(), localSchema.getModuleId());
        assertEquals(module.getVersion(), localSchema.getModuleVersion());

        // imported schemas can't be updated
        try {
            apiDeveloper.getClient(UploadSchemasApi.class).updateUploadSchema(localSchema.getSchemaId(),
                    localSchema.getRevision(), localSchema).execute();
            fail("expected exception");
        } catch (BadRequestException ex) {
            assertEquals("Schema " + localSchema.getSchemaId() + " was imported from a shared module and cannot be " +
                    "modified.", ex.getMessage());
        }
    }

    // Helper method to create a survey in the shared module library. Returns the created survey.
    @SuppressWarnings("deprecation")
    private static Survey createAndPublishSharedSurvey() throws Exception {
        SurveysApi sharedSurveyApi = sharedDeveloper.getClient(SurveysApi.class);
        Survey surveyToCreate = TestSurvey.getSurvey(SharedModuleTest.class);
        GuidCreatedOnVersionHolder createdSurveyKey = sharedSurveyApi.createSurvey(surveyToCreate).execute().body();
        sharedSurveyApi.publishSurvey(createdSurveyKey.getGuid(), createdSurveyKey.getCreatedOn(), null).execute();
        return sharedSurveyApi.getSurvey(createdSurveyKey.getGuid(), createdSurveyKey.getCreatedOn()).execute().body();
    }

    // Helper method to create a shared module referencing the given survey. Returns the created module.
    @SuppressWarnings("deprecation")
    private static SharedModuleMetadata createModuleForSurvey(Survey survey) throws Exception {
        String moduleId = "test-module-" + RandomStringUtils.randomAlphabetic(4);
        SharedModuleMetadata moduleToCreate = new SharedModuleMetadata().id(moduleId).name("Test Module With Survey")
                .published(true).surveyGuid(survey.getGuid()).surveyCreatedOn(survey.getCreatedOn().toString());
        return sharedDeveloper.getClient(ForDevelopersApi.class).createMetadata(moduleToCreate).execute().body();
    }

    // Helper method to verify shared survey and local survey match. Similar to assertLocalSchema().
    private static void assertLocalSurvey(Survey sharedSurvey, Survey localSurvey, SharedModuleMetadata module) {
        // Basic fields.
        assertEquals(sharedSurvey.getName(), localSurvey.getName());
        assertEquals(sharedSurvey.getIdentifier(), localSurvey.getIdentifier());

        // We can't use a simple equals() for elements, since guid is different. Instead, go through each element,
        // clear the guid, and *then* check equals.
        assertEquals(sharedSurvey.getElements().size(), localSurvey.getElements().size());
        int numElements = localSurvey.getElements().size();
        for (int i = 0; i < numElements; i++) {
            SurveyElement sharedElement = sharedSurvey.getElements().get(i);
            sharedElement.setGuid(null);

            SurveyElement localElement = localSurvey.getElements().get(i);
            localElement.setGuid(null);

            assertEquals(sharedElement, localElement);
        }

        // Check that we annotated the survey with module ID and version.
        assertEquals(module.getId(), localSurvey.getModuleId());
        assertEquals(module.getVersion(), localSurvey.getModuleVersion());

        // Also, local survey is published.
        assertTrue(localSurvey.isPublished());
    }
}
