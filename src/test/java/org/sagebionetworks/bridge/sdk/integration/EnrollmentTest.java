package org.sagebionetworks.bridge.sdk.integration;

import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.sagebionetworks.bridge.rest.model.Role.RESEARCHER;
import static org.sagebionetworks.bridge.rest.model.Role.STUDY_COORDINATOR;
import static org.sagebionetworks.bridge.sdk.integration.Tests.APP_ID;
import static org.sagebionetworks.bridge.sdk.integration.Tests.ORG_ID_1;
import static org.sagebionetworks.bridge.sdk.integration.Tests.ORG_ID_2;
import static org.sagebionetworks.bridge.sdk.integration.Tests.STUDY_ID_1;
import static org.sagebionetworks.bridge.sdk.integration.Tests.STUDY_ID_2;

import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.sagebionetworks.bridge.rest.api.ForAdminsApi;
import org.sagebionetworks.bridge.rest.api.ForStudyCoordinatorsApi;
import org.sagebionetworks.bridge.rest.api.OrganizationsApi;
import org.sagebionetworks.bridge.rest.api.ParticipantsApi;
import org.sagebionetworks.bridge.rest.api.StudiesApi;
import org.sagebionetworks.bridge.rest.api.StudyParticipantsApi;
import org.sagebionetworks.bridge.rest.exceptions.EntityNotFoundException;
import org.sagebionetworks.bridge.rest.model.Enrollment;
import org.sagebionetworks.bridge.rest.model.EnrollmentDetail;
import org.sagebionetworks.bridge.rest.model.EnrollmentDetailList;
import org.sagebionetworks.bridge.rest.model.IdentifierHolder;
import org.sagebionetworks.bridge.rest.model.Role;
import org.sagebionetworks.bridge.rest.model.SignUp;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.user.TestUserHelper;
import org.sagebionetworks.bridge.user.TestUserHelper.TestUser;

public class EnrollmentTest {
    
    TestUser admin;
    TestUser researcher;
    TestUser studyCoordinator;
    TestUser org1StudyCoordinator;
    TestUser org2StudyCoordinator;
    
    @After
    public void after() throws Exception {
        if (researcher != null) {
            researcher.signOutAndDeleteUser();
        }
        if (studyCoordinator != null) {
            studyCoordinator.signOutAndDeleteUser();
        }
        if (org1StudyCoordinator != null) {
            org1StudyCoordinator.signOutAndDeleteUser();
        }
        if (org2StudyCoordinator != null) {
            org2StudyCoordinator.signOutAndDeleteUser();   
        }
    }
    
    @Before
    public void before() throws Exception {
        researcher = TestUserHelper.createAndSignInUser(EnrollmentTest.class, false, RESEARCHER);
        
        admin = TestUserHelper.getSignedInAdmin();
        OrganizationsApi orgsApi = admin.getClient(OrganizationsApi.class);

        orgsApi.addMember(ORG_ID_1, researcher.getUserId()).execute();
    }
    
    @Test
    public void badStudyParameterReturnsCorrectErrorMessage() throws Exception {
        StudiesApi studiesApi = admin.getClient(StudiesApi.class);
        try {
            studiesApi.enrollParticipant("junk", new Enrollment()
                    .userId("should not matter")).execute().body();
            fail("Should have thrown exception");
        } catch(EntityNotFoundException e) {
            assertEquals("Study not found.", e.getMessage());
        }
    }
    
    @Test
    public void test() throws Exception {
        String externalId = Tests.randomIdentifier(EnrollmentTest.class);
        TestUser user = TestUserHelper.createAndSignInUser(EnrollmentTest.class, true);
        try {
            DateTime timestamp = DateTime.now();
            StudiesApi studiesApi = admin.getClient(StudiesApi.class);
            
            Enrollment enrollment = new Enrollment();
            enrollment.setEnrolledOn(timestamp);
            enrollment.setExternalId(externalId);
            enrollment.setUserId(user.getUserId());
            enrollment.setConsentRequired(true);
            Enrollment retValue = studiesApi.enrollParticipant(STUDY_ID_1, enrollment).execute().body();
            
            assertEquals(externalId, retValue.getExternalId());
            assertEquals(user.getUserId(), retValue.getUserId());
            assertTrue(retValue.isConsentRequired());
            assertEquals(timestamp.getMillis(), retValue.getEnrolledOn().getMillis());
            assertEquals(admin.getUserId(), retValue.getEnrolledBy());
            assertNull(enrollment.getWithdrawnOn());
            assertNull(enrollment.getWithdrawnBy());
            assertNull(enrollment.getWithdrawalNote());
            
            // Now shows up in paged api
            EnrollmentDetailList list = studiesApi.getEnrollments(STUDY_ID_1, "enrolled", false, null, null).execute().body();
            assertTrue(list.getItems().stream().anyMatch(e -> e.getParticipant().getIdentifier().equals(user.getUserId())));
            
            list = studiesApi.getEnrollments(STUDY_ID_1, null, false, null, null).execute().body();
            assertTrue(list.getItems().stream().anyMatch(e -> e.getParticipant().getIdentifier().equals(user.getUserId())));
            
            retValue = studiesApi.withdrawParticipant(STUDY_ID_1, user.getUserId(), 
                    "Testing enrollment and withdrawal.").execute().body();
            assertEquals(user.getUserId(), retValue.getUserId());
            assertTrue(retValue.isConsentRequired());
            assertEquals(timestamp.getMillis(), retValue.getEnrolledOn().getMillis());
            assertEquals(admin.getUserId(), retValue.getEnrolledBy());
            
            // Enrollment comes from the client in this test, but withdrawal timestamp comes from the 
            // server. Clock skew can throw this off by as much as an hour, so just verify the 
            // withdrawnOn value is there and within the hour either way.
            assertTrue(retValue.getWithdrawnOn().isAfter(DateTime.now().minusHours(1)));
            assertTrue(retValue.getWithdrawnOn().isBefore(DateTime.now().plusHours(1)));
            
            assertEquals(admin.getUserId(), retValue.getWithdrawnBy());
            assertEquals("Testing enrollment and withdrawal.", retValue.getWithdrawalNote());
            
            list = studiesApi.getEnrollments(STUDY_ID_1, "enrolled", false, null, null).execute().body();
            assertFalse(list.getItems().stream().anyMatch(e -> e.getParticipant().getIdentifier().equals(user.getUserId())));
            
            // This person is accessible via the external ID.
            StudyParticipant participant = admin.getClient(ParticipantsApi.class)
                    .getParticipantByExternalId(externalId, false).execute().body();
            assertEquals(user.getUserId(), participant.getId());
            
            // It is still in the paged API, despite being withdrawn.
            list = studiesApi.getEnrollments(STUDY_ID_1, "withdrawn", false, null, null).execute().body();
            assertTrue(list.getItems().stream().anyMatch(e -> e.getParticipant().getIdentifier().equals(user.getUserId())));
            
            list = studiesApi.getEnrollments(STUDY_ID_1, "all", false, null, null).execute().body();
            assertTrue(list.getItems().stream().anyMatch(e -> e.getParticipant().getIdentifier().equals(user.getUserId())));
            
            // test the filter for test accounts
            participant.addDataGroupsItem("test_user");
            admin.getClient(ParticipantsApi.class).updateParticipant(participant.getId(), participant).execute();
            
            list = studiesApi.getEnrollments(STUDY_ID_1, "all", false, null, null).execute().body();
            assertFalse(list.getItems().stream().anyMatch(e -> e.getParticipant().getIdentifier().equals(user.getUserId())));
            
            list = studiesApi.getEnrollments(STUDY_ID_1, "all", true, null, null).execute().body();
            assertTrue(list.getItems().stream().anyMatch(e -> e.getParticipant().getIdentifier().equals(user.getUserId())));
        } finally {
            user.signOutAndDeleteUser();
        }
    }
    
    @Test
    public void studyCoordinatorEnrollsWithExternalId() throws Exception {
        // Enroll through the study coordinator API *and* add an external ID. This doesn't
        // look interesting from the client side, but it verifies correct behavior on the
        // server where we call enrollment code twice.
        String externalId = Tests.randomIdentifier(EnrollmentTest.class);
        IdentifierHolder keys = null;
        
        studyCoordinator = TestUserHelper.createAndSignInUser(EnrollmentTest.class, false, STUDY_COORDINATOR);
        try {
            // study coordinator can enroll the user in study1. Include an external ID as well.
            ForStudyCoordinatorsApi coordApi = studyCoordinator.getClient(ForStudyCoordinatorsApi.class);
            
            SignUp signUp = new SignUp()
                    .externalIds(ImmutableMap.of(STUDY_ID_1, externalId));
            
            keys = coordApi.createStudyParticipant(STUDY_ID_1, signUp).execute().body();;
            
            StudyParticipant participant = coordApi.getStudyParticipantById(
                    STUDY_ID_1, "externalid:"+externalId, true).execute().body();
            
            assertEquals(ImmutableList.of(STUDY_ID_1), participant.getStudyIds());
            assertEquals(1, participant.getExternalIds().size());
            assertEquals(externalId, participant.getExternalIds().get(STUDY_ID_1));
            // there is no consent history (which is keyed by subpopulation; the API app has a 
            // subpopulation also named "api"
            assertNull(participant.getConsentHistories().get(APP_ID));
        } finally {
            if (keys != null) {
                admin.getClient(ForAdminsApi.class).deleteUser(keys.getIdentifier()).execute();
            }
        }
    }
    
    @Test
    public void enrollmentsAreFiltered() throws Exception {
        admin = TestUserHelper.getSignedInAdmin();
        OrganizationsApi orgsApi = admin.getClient(OrganizationsApi.class);
        StudiesApi studiesApi = admin.getClient(StudiesApi.class);

        // Can access only study 1
        org1StudyCoordinator = TestUserHelper.createAndSignInUser(EnrollmentTest.class, false, STUDY_COORDINATOR);
        orgsApi.addMember(ORG_ID_1, org1StudyCoordinator.getUserId()).execute();

        // Can access only study 2
        org2StudyCoordinator = TestUserHelper.createAndSignInUser(EnrollmentTest.class, false, STUDY_COORDINATOR);
        orgsApi.addMember(ORG_ID_2, org2StudyCoordinator.getUserId()).execute();
        
        // Enroll user in both studies
        TestUser user = TestUserHelper.createAndSignInUser(EnrollmentTest.class, true);
        studiesApi.enrollParticipant(STUDY_ID_1, new Enrollment().userId(user.getUserId())).execute();
        studiesApi.enrollParticipant(STUDY_ID_2, new Enrollment().userId(user.getUserId())).execute();
        
        // This person can only see study 1 enrollment
        EnrollmentDetailList enrollmentsFor1 = org1StudyCoordinator
                .getClient(StudyParticipantsApi.class)
                .getStudyParticipantEnrollments(STUDY_ID_1, user.getUserId()).execute().body();
        assertEquals(1, enrollmentsFor1.getItems().size());
        assertEquals(STUDY_ID_1, enrollmentsFor1.getItems().get(0).getStudyId());
        
        // This person can only see study 1 enrollment on participant record
        StudyParticipant participantFor1 = org1StudyCoordinator.getClient(StudyParticipantsApi.class)
            .getStudyParticipantById(STUDY_ID_1, user.getUserId(), false).execute().body();
        assertEquals(1, participantFor1.getEnrollments().size());
        assertNotNull(participantFor1.getEnrollments().get(STUDY_ID_1));
        
        // This person can only see study 2 enrollment
        EnrollmentDetailList enrollmentsFor2 = org2StudyCoordinator
                .getClient(StudyParticipantsApi.class)
                .getStudyParticipantEnrollments(STUDY_ID_2, user.getUserId()).execute().body();
        assertEquals(1, enrollmentsFor2.getItems().size());
        assertEquals(STUDY_ID_2, enrollmentsFor2.getItems().get(0).getStudyId());

        // This person can only see study 2 enrollment on participant record
        StudyParticipant participantFor2 = org2StudyCoordinator.getClient(StudyParticipantsApi.class)
                .getStudyParticipantById(STUDY_ID_2, user.getUserId(), false).execute().body();
        assertEquals(1, participantFor2.getEnrollments().size());
        assertNotNull(participantFor2.getEnrollments().get(STUDY_ID_2));

        // An admin can see both enrollments
        EnrollmentDetailList enrollmentsForAdmin = admin
                .getClient(StudyParticipantsApi.class)
                .getStudyParticipantEnrollments(STUDY_ID_2, user.getUserId()).execute().body();
        assertEquals(2, enrollmentsForAdmin.getItems().size());
        assertEquals(ImmutableSet.of(STUDY_ID_1, STUDY_ID_2), enrollmentsForAdmin.getItems().stream()
                .map(EnrollmentDetail::getStudyId).collect(toSet()));

        // An admin can see both enrollments on participant record.
        StudyParticipant participantForAdmin = admin.getClient(StudyParticipantsApi.class)
                .getStudyParticipantById(STUDY_ID_1, user.getUserId(), false).execute().body();
        assertEquals(2, participantForAdmin.getEnrollments().size());
        assertNotNull(participantForAdmin.getEnrollments().get(STUDY_ID_1));
        assertNotNull(participantForAdmin.getEnrollments().get(STUDY_ID_2));
    }
}
