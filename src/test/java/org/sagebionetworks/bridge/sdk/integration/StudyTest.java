package org.sagebionetworks.bridge.sdk.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.sagebionetworks.bridge.rest.model.ContactRole.PRINCIPAL_INVESTIGATOR;
import static org.sagebionetworks.bridge.rest.model.ContactRole.TECHNICAL_SUPPORT;
import static org.sagebionetworks.bridge.rest.model.IrbDecisionType.EXEMPT;
import static org.sagebionetworks.bridge.rest.model.Role.STUDY_COORDINATOR;
import static org.sagebionetworks.bridge.rest.model.SignInType.EMAIL_MESSAGE;
import static org.sagebionetworks.bridge.rest.model.SignInType.PHONE_PASSWORD;
import static org.sagebionetworks.bridge.rest.model.StudyPhase.DESIGN;
import static org.sagebionetworks.bridge.rest.model.StudyPhase.IN_FLIGHT;
import static org.sagebionetworks.bridge.sdk.integration.Tests.ORG_ID_1;
import static org.sagebionetworks.bridge.sdk.integration.Tests.ORG_ID_2;
import static org.sagebionetworks.bridge.sdk.integration.Tests.PASSWORD;
import static org.sagebionetworks.bridge.sdk.integration.Tests.STUDY_ID_1;
import static org.sagebionetworks.bridge.sdk.integration.Tests.STUDY_ID_2;
import static org.sagebionetworks.bridge.util.IntegTestUtils.SAGE_ID;
import static org.sagebionetworks.bridge.util.IntegTestUtils.TEST_APP_ID;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.sagebionetworks.bridge.rest.RestUtils;
import org.sagebionetworks.bridge.rest.api.FilesApi;
import org.sagebionetworks.bridge.rest.api.ForAdminsApi;
import org.sagebionetworks.bridge.rest.api.ForConsentedUsersApi;
import org.sagebionetworks.bridge.rest.api.OrganizationsApi;
import org.sagebionetworks.bridge.rest.api.ParticipantsApi;
import org.sagebionetworks.bridge.rest.api.StudiesApi;
import org.sagebionetworks.bridge.rest.exceptions.EntityNotFoundException;
import org.sagebionetworks.bridge.rest.exceptions.UnauthorizedException;
import org.sagebionetworks.bridge.rest.model.Contact;
import org.sagebionetworks.bridge.rest.model.OrganizationList;
import org.sagebionetworks.bridge.rest.model.Role;
import org.sagebionetworks.bridge.rest.model.SignInType;
import org.sagebionetworks.bridge.rest.model.SignUp;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.StudyInfo;
import org.sagebionetworks.bridge.rest.model.StudyList;
import org.sagebionetworks.bridge.rest.model.VersionHolder;
import org.sagebionetworks.bridge.user.TestUserHelper;
import org.sagebionetworks.bridge.user.TestUserHelper.TestUser;
import org.sagebionetworks.bridge.util.IntegTestUtils;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class StudyTest {
    
    private static final ImmutableList<SignInType> SIGN_IN_TYPES = ImmutableList.of(EMAIL_MESSAGE, PHONE_PASSWORD);
    private static final ImmutableList<String> DESIGN_TYPE_LIST = ImmutableList.of("observational", "cross-over");
    private static final ImmutableList<String> DISEASE_LIST = ImmutableList.of("kidney", "liver");
    private List<String> studyIdsToDelete = new ArrayList<>();
    private List<String> userIdsToDelete = new ArrayList<>();
    private TestUser testResearcher;
    private TestUser admin;
    
    @Before
    public void before() throws Exception {
        admin = TestUserHelper.getSignedInAdmin();
        testResearcher = TestUserHelper.createAndSignInUser(StudyTest.class, false, Role.RESEARCHER);
    }
    
    @After
    public void deleteResearcher() throws Exception {
        if (testResearcher != null) {
            testResearcher.signOutAndDeleteUser();
        }
    }
    
    @After
    public void after() throws Exception {
        ForAdminsApi adminsApi = admin.getClient(ForAdminsApi.class);
        for (String userId : userIdsToDelete) {
            try {
                adminsApi.deleteUser(userId).execute();
            } catch(EntityNotFoundException e) {
            }
        }
        for (String studyId : studyIdsToDelete) {
            try {
                adminsApi.deleteStudy(studyId, true).execute();    
            } catch(EntityNotFoundException e) {
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void test() throws IOException {
        StudiesApi studiesApi = admin.getClient(StudiesApi.class);
        
        int initialCount = studiesApi.getStudies(null, null, false).execute().body().getItems().size();
        
        Map<String,String> map = new HashMap<>();
        map.put("enrollmentType", "byExternalId");
        
        String id = Tests.randomIdentifier(StudyTest.class);
        Study study = new Study().identifier(id).clientData(map).name("Study " + id);
        
        // IRB information
        study.setIrbName("IRB Name");
        study.setIrbProtocolName("IRB Protocol Name");
        study.setIrbProtocolId("123");
        study.setIrbDecisionType(EXEMPT);
        study.setIrbDecisionOn(LocalDate.parse("2012-12-12"));
        study.setIrbExpiresOn(LocalDate.parse("2013-12-12"));
        
        study.setDiseases(DISEASE_LIST);
        study.setStudyDesignTypes(DESIGN_TYPE_LIST);
        study.setSignInTypes(SIGN_IN_TYPES);
        study.setKeywords("some keywords");
        
        // We had an issue where you could not store two contacts with the same
        // name; verify this restriction has been fixed.
        String contactEmail = IntegTestUtils.makeEmail(StudyTest.class);
        Contact contact1 = new Contact()
                .role(PRINCIPAL_INVESTIGATOR)
                .name("Tim Powers")
                .affiliation("Miskatonic University")
                .email(contactEmail);
        Contact contact2 = new Contact()
                .role(TECHNICAL_SUPPORT)
                .name("Tim Powers")
                .affiliation("Miskatonic University")
                .email(contactEmail);
        study.contacts(ImmutableList.of(contact1, contact2));
        
        VersionHolder holder = studiesApi.createStudy(study).execute().body();
        study.setVersion(holder.getVersion());
        studyIdsToDelete.add(id);
        
        Study retrieved = studiesApi.getStudy(id).execute().body();
        assertEquals(id, retrieved.getIdentifier());
        assertEquals("Study " + id, retrieved.getName());
        assertEquals(DESIGN, retrieved.getPhase());
        assertTrue(retrieved.getCreatedOn().isAfter(DateTime.now().minusHours(1)));
        assertTrue(retrieved.getModifiedOn().isAfter(DateTime.now().minusHours(1)));
        
        assertEquals("IRB Name", retrieved.getIrbName());
        assertEquals("IRB Protocol Name", retrieved.getIrbProtocolName());
        assertEquals("123", retrieved.getIrbProtocolId());
        assertEquals(EXEMPT, retrieved.getIrbDecisionType());
        assertEquals(LocalDate.parse("2012-12-12"), retrieved.getIrbDecisionOn());
        assertEquals(LocalDate.parse("2013-12-12"), retrieved.getIrbExpiresOn());
        
        assertEquals(DISEASE_LIST, study.getDiseases());
        assertEquals(DESIGN_TYPE_LIST, study.getStudyDesignTypes());
        assertEquals(SIGN_IN_TYPES, study.getSignInTypes());
        assertEquals("some keywords", study.getKeywords());
        
        Contact retrievedContact1 = retrieved.getContacts().get(0);
        assertEquals(PRINCIPAL_INVESTIGATOR, retrievedContact1.getRole());
        assertEquals("Tim Powers", retrievedContact1.getName());
        assertEquals("Miskatonic University", retrievedContact1.getAffiliation());
        assertEquals(contactEmail, retrievedContact1.getEmail());
        
        Contact retrievedContact2 = retrieved.getContacts().get(1);
        assertEquals(TECHNICAL_SUPPORT, retrievedContact2.getRole());
        assertEquals("Tim Powers", retrievedContact2.getName());
        assertEquals("Miskatonic University", retrievedContact2.getAffiliation());
        assertEquals(contactEmail, retrievedContact2.getEmail());
        
        Map<String,String> theMap = RestUtils.toType(study.getClientData(), Map.class);
        assertEquals("byExternalId", theMap.get("enrollmentType"));
        
        OrganizationList orgList = studiesApi.getSponsors(id, 0, 100).execute().body();
        assertTrue(orgList.getItems().stream().anyMatch(org -> org.getIdentifier().equals(SAGE_ID)));
        
        DateTime lastModified1 = retrieved.getModifiedOn();
        
        study.name("New test name " + id);
        study.phase(IN_FLIGHT); // this cannot be changed
        VersionHolder holder2 = studiesApi.updateStudy(id, study).execute().body();
        assertNotEquals(holder.getVersion(), holder2.getVersion());
        
        Study retrieved2 = studiesApi.getStudy(id).execute().body();
        assertEquals("New test name " + id, retrieved2.getName());
        assertEquals(DESIGN, retrieved2.getPhase());
        assertNotEquals(lastModified1, retrieved2.getModifiedOn());
        
        StudyList studyList = studiesApi.getStudies(null, null, false).execute().body();
        assertEquals(initialCount+1, studyList.getItems().size());
        assertFalse(studyList.getRequestParams().isIncludeDeleted());
        
        // upload a logo
        File file = new File("src/test/resources/file-test/test.png");
        String url = RestUtils.uploadStudyLogoToS3(studiesApi, id, file);

        retrieved = studiesApi.getStudy(id).execute().body();
        assertEquals(url, retrieved.getStudyLogoUrl());
        
        // Now use the admin to delete the logo it via the files API (cleanup)
        String logoGuid = url.substring(url.lastIndexOf("/")+1, url.lastIndexOf("."));
        admin.getClient(FilesApi.class).deleteFile(logoGuid, true).execute();
        
        // logically delete it
        studiesApi.deleteStudy(id, false).execute();
        
        studyList = studiesApi.getStudies(null, null, false).execute().body();
        assertEquals(initialCount, studyList.getItems().size());
        
        studyList = studiesApi.getStudies(null, null, true).execute().body();
        assertTrue(studyList.getItems().size() > initialCount);
        assertTrue(studyList.getRequestParams().isIncludeDeleted());
        
        // you can still retrieve it
        Study retrieved3 = studiesApi.getStudy(id).execute().body();
        assertNotNull(retrieved3);
        
        // physically delete it
        studiesApi.deleteStudy(id, true).execute();
        
        // Now it's really gone
        studyList = studiesApi.getStudies(null, null, true).execute().body();
        assertEquals(initialCount, studyList.getItems().size());
        
        try {
            studiesApi.getStudy(id).execute();
            fail("Should have thrown an exception");
        } catch(EntityNotFoundException e) {
        }
    }
    
    @Test
    public void usersAreTaintedByStudyAssociation() throws Exception {
        // Create a study for this test.
        
        String id1 = Tests.randomIdentifier(StudyTest.class);
        Study study1 = new Study().identifier(id1).name("Study " + id1);

        String id2 = Tests.randomIdentifier(StudyTest.class);
        Study study2 = new Study().identifier(id2).name("Study " + id2);
        
        StudiesApi studiesApi = admin.getClient(StudiesApi.class);
        studiesApi.createStudy(study1).execute();
        studyIdsToDelete.add(id1);
        studiesApi.createStudy(study2).execute();
        studyIdsToDelete.add(id2);
        admin.getClient(OrganizationsApi.class).addStudySponsorship(ORG_ID_1, id1).execute();
        admin.getClient(OrganizationsApi.class).addStudySponsorship(ORG_ID_2, id2).execute();
        
        TestUser studyCoordinator = TestUserHelper.createAndSignInUser(StudyTest.class, true, STUDY_COORDINATOR);
        userIdsToDelete.add(studyCoordinator.getUserId());
        
        admin.getClient(OrganizationsApi.class).addMember(ORG_ID_1, studyCoordinator.getUserId()).execute();
        ParticipantsApi participantApi = studyCoordinator.getClient(ParticipantsApi.class);
        
        // Cannot associate this user to a non-existent study
        try {
            OrganizationsApi orgsApi = admin.getClient(OrganizationsApi.class);
            orgsApi.addMember("bad-id", studyCoordinator.getUserId()).execute();
            fail("Should have thrown exception");
        } catch(EntityNotFoundException e) {
            assertEquals("Organization not found.", e.getMessage());
        }
        
        // Cannot sign this user up because the enrollment includes one the study coordinator does not possess.
        String email2 = IntegTestUtils.makeEmail(StudyTest.class);
        SignUp signUp2 = new SignUp().email(email2).password(PASSWORD).appId(TEST_APP_ID)
                .externalIds(ImmutableMap.of(STUDY_ID_1, Tests.randomIdentifier(StudyTest.class), 
                        STUDY_ID_2, "cannot-work"));
        try {
            participantApi.createParticipant(signUp2).execute().body();
            fail("Should have thrown exception");
        } catch(UnauthorizedException e) {
            assertEquals("Caller does not have permission to access this service.", e.getMessage());
        }
    }
    
    @Test
    public void testSponsorship() throws Exception {
        StudiesApi adminStudiesApi = admin.getClient(StudiesApi.class);
        
        String tempStudyId = Tests.randomIdentifier(StudyTest.class);
        Study tempStudy = new Study().identifier(tempStudyId).name(tempStudyId);
        adminStudiesApi.createStudy(tempStudy).execute();
        studyIdsToDelete.add(tempStudyId);
        
        adminStudiesApi.addStudySponsor(tempStudyId, ORG_ID_1).execute();
        
        OrganizationList list = adminStudiesApi.getSponsors(tempStudyId, null, null).execute().body();
        assertTrue(list.getItems().stream().anyMatch((org) -> org.getIdentifier().equals(ORG_ID_1)));

        // The organization should see this as a sponsored study
        StudyList studyList = admin.getClient(OrganizationsApi.class).getSponsoredStudies(ORG_ID_1, null, null).execute().body();
        assertTrue(studyList.getItems().stream().anyMatch((study) -> study.getIdentifier().equals(tempStudyId)));

        adminStudiesApi.deleteStudy(tempStudyId, false).execute();
        
        // Now if we ask, we should not see this as a sponsored study
        studyList = admin.getClient(OrganizationsApi.class).getSponsoredStudies(ORG_ID_1, null, null).execute().body();
        assertFalse(studyList.getItems().stream().anyMatch((study) -> study.getIdentifier().equals(tempStudyId)));
        
        adminStudiesApi.removeStudySponsor(tempStudyId, ORG_ID_1).execute();

        list = adminStudiesApi.getSponsors(tempStudyId, null, null).execute().body();
        assertFalse(list.getItems().stream().anyMatch((org) -> org.getIdentifier().equals(ORG_ID_1)));
            
        adminStudiesApi.deleteStudy(tempStudyId, true).execute();
    }    
    
    @Test
    public void testPublicStudies() throws IOException {
        String url = admin.getClientManager().getHostUrl() + "/v1/apps/api/studies/study1";
        
        OkHttpClient client = new OkHttpClient();
        Request request = new Request.Builder().url(url).build();
        Response response = client.newCall(request).execute();
        
        assertEquals(200, response.code());
        assertEquals("application/json;charset=UTF-8", response.header("Content-Type"));
        
        ResponseBody body = response.body();
        String output = new String(body.bytes());
        // enums need to be upper-cased for the vanilla ObjectMapper to deserialize properly
        output = output.replace("design", "DESIGN");
        output = output.replace("legacy", "LEGACY");
        output = output.replace("email_message", "EMAIL_MESSAGE");
        output = output.replace("phone_message", "PHONE_MESSAGE");
        
        StudyInfo deser = new ObjectMapper().readValue(output, StudyInfo.class);
        assertEquals(STUDY_ID_1, deser.getIdentifier());
        
        TestUser user = TestUserHelper.createAndSignInUser(StudyTest.class, true);
        userIdsToDelete.add(user.getUserId());
        
        // This person by default has been put into study 1.
        
        ForConsentedUsersApi usersApi = user.getClient(ForConsentedUsersApi.class);
        Study usersStudy = usersApi.getStudy(STUDY_ID_1).execute().body();
        assertEquals(STUDY_ID_1, usersStudy.getIdentifier());
        
        try {
            usersApi.getStudy("study4").execute();
            fail("Should have thrown exception");
        } catch(EntityNotFoundException e) {
            
        }
        try {
            usersApi.getStudy(STUDY_ID_2).execute().body();
            fail("Should have thrown exception");
        } catch(UnauthorizedException e) {
            
        }
    }
}
