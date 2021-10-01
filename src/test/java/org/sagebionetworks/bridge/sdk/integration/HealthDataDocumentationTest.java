package org.sagebionetworks.bridge.sdk.integration;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sagebionetworks.bridge.rest.api.ForAdminsApi;
import org.sagebionetworks.bridge.rest.api.ForDevelopersApi;
import org.sagebionetworks.bridge.rest.api.ForResearchersApi;
import org.sagebionetworks.bridge.rest.model.ForwardCursorPagedResourceList;
import org.sagebionetworks.bridge.rest.model.HealthDataDocumentation;
import org.sagebionetworks.bridge.rest.model.Role;
import org.sagebionetworks.bridge.user.TestUserHelper;
import org.sagebionetworks.bridge.user.TestUserHelper.TestUser;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class HealthDataDocumentationTest {
    private static TestUser researcher;
    private static TestUser developer;
    private static TestUser admin;
    private static DateTime modifiedOn;
    private String identifier1;
    private String identifier2;
    private String identifier3;

    private TestUser user;

    private String DOCUMENTATION = "test-documentation";
    private Integer PAGE_SIZE = 10;
    private String OFFSET_KEY = null;

    @Before
    public void before() throws IOException {
        admin = TestUserHelper.getSignedInAdmin();
        researcher = TestUserHelper.createAndSignInUser(HealthDataDocumentationTest.class, false, Role.RESEARCHER);
        developer = TestUserHelper.createAndSignInUser(HealthDataDocumentationTest.class, false, Role.DEVELOPER);
        user = TestUserHelper.createAndSignInUser(HealthDataDocumentationTest.class, true);
        modifiedOn = DateTime.now(DateTimeZone.UTC);

        identifier1 = Tests.randomIdentifier(HealthDataDocumentationTest.class);
        identifier2 = Tests.randomIdentifier(HealthDataDocumentationTest.class);
        identifier3 = Tests.randomIdentifier(HealthDataDocumentationTest.class);
    }

    @After
    public void after() throws IOException {
        ForAdminsApi adminsApi = admin.getClient(ForAdminsApi.class);
        adminsApi.deleteAllParticipantDataForAdmin(user.getAppId(), user.getUserId());

        if (user != null) {
            user.signOutAndDeleteUser();
        }
        if (developer != null) {
            developer.signOutAndDeleteUser();
        }
        if (researcher != null) {
            researcher.signOutAndDeleteUser();
        }
    }

    @Test
    public void researcherCanCrudHealthDataDocumentation() throws IOException {
        ForResearchersApi researchersApi = researcher.getClient(ForResearchersApi.class);

        HealthDataDocumentation doc1 = createHealthDataDocumentation("foo",identifier1, DOCUMENTATION);
        HealthDataDocumentation doc2 = createHealthDataDocumentation("bar", identifier2, DOCUMENTATION);
        HealthDataDocumentation doc3 = createHealthDataDocumentation("baz", identifier3, DOCUMENTATION);

        // researcher can create health data documentation
        researchersApi.createOrUpdateHealthDataDocumentation(doc1);
        researchersApi.createOrUpdateHealthDataDocumentation(doc2);
        researchersApi.createOrUpdateHealthDataDocumentation(doc3);

        // researcher can get a health data documentation
        HealthDataDocumentation retrievedDoc1 = researchersApi.getHealthDataDocumentationForId(identifier1).execute().body();
        HealthDataDocumentation retrievedDoc2 = researchersApi.getHealthDataDocumentationForId(identifier2).execute().body();
        HealthDataDocumentation retrievedDoc3 = researchersApi.getHealthDataDocumentationForId(identifier3).execute().body();
        assertEquals(retrievedDoc1, doc1);
        assertEquals(retrievedDoc2, doc2);
        assertEquals(retrievedDoc3, doc3);

        // researcher can delete health data documentation
        researchersApi.deleteHealthDataDocumentationForIdentifier(identifier1).execute();
        retrievedDoc1 = researchersApi.getHealthDataDocumentationForId(identifier1).execute().body();
        assertNull(retrievedDoc1);

        researchersApi.deleteHealthDataDocumentationForIdentifier(identifier2).execute();
        retrievedDoc2 = researchersApi.getHealthDataDocumentationForId(identifier2).execute().body();
        assertNull(retrievedDoc2);

        researchersApi.deleteHealthDataDocumentationForIdentifier(identifier3).execute();
        retrievedDoc3 = researchersApi.getHealthDataDocumentationForId(identifier3).execute().body();
        assertNull(retrievedDoc3);
    }

    @Test
    public void developerCanCrudHealthDataDocumentation() throws IOException {
        ForDevelopersApi devsApi = developer.getClient(ForDevelopersApi.class);

        HealthDataDocumentation doc1 = createHealthDataDocumentation("foo",identifier1, DOCUMENTATION);
        HealthDataDocumentation doc2 = createHealthDataDocumentation("bar", identifier2, DOCUMENTATION);
        HealthDataDocumentation doc3 = createHealthDataDocumentation("baz", identifier3, DOCUMENTATION);

        // researcher can create health data documentation
        devsApi.createOrUpdateHealthDataDocumentation(doc1);
        devsApi.createOrUpdateHealthDataDocumentation(doc2);
        devsApi.createOrUpdateHealthDataDocumentation(doc3);

        // researcher can get a health data documentation
        HealthDataDocumentation retrievedDoc1 = devsApi.getHealthDataDocumentationForId(identifier1).execute().body();
        HealthDataDocumentation retrievedDoc2 = devsApi.getHealthDataDocumentationForId(identifier2).execute().body();
        HealthDataDocumentation retrievedDoc3 = devsApi.getHealthDataDocumentationForId(identifier3).execute().body();
        assertEquals(retrievedDoc1, doc1);
        assertEquals(retrievedDoc2, doc2);
        assertEquals(retrievedDoc3, doc3);

        // researcher can delete health data documentation
        devsApi.deleteHealthDataDocumentationForIdentifier(identifier1).execute();
        retrievedDoc1 = devsApi.getHealthDataDocumentationForId(identifier1).execute().body();
        assertNull(retrievedDoc1);

        devsApi.deleteHealthDataDocumentationForIdentifier(identifier2).execute();
        retrievedDoc2 = devsApi.getHealthDataDocumentationForId(identifier2).execute().body();
        assertNull(retrievedDoc2);

        devsApi.deleteHealthDataDocumentationForIdentifier(identifier3).execute();
        retrievedDoc3 = devsApi.getHealthDataDocumentationForId(identifier3).execute().body();
        assertNull(retrievedDoc3);
    }

    private static HealthDataDocumentation createHealthDataDocumentation(String title, String identifier, String documentation) {
        HealthDataDocumentation doc = new HealthDataDocumentation();
        doc.setTitle(title);
        doc.setIdentifier(identifier);
        doc.setDocumentation(documentation);
        return doc;
    }
}
