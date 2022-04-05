package org.sagebionetworks.bridge.sdk.integration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sagebionetworks.bridge.rest.api.AssessmentsApi;
import org.sagebionetworks.bridge.rest.api.OrganizationsApi;
import org.sagebionetworks.bridge.rest.api.PermissionsApi;
import org.sagebionetworks.bridge.rest.api.StudiesApi;
import org.sagebionetworks.bridge.rest.exceptions.ConcurrentModificationException;
import org.sagebionetworks.bridge.rest.exceptions.ConstraintViolationException;
import org.sagebionetworks.bridge.rest.exceptions.EntityNotFoundException;
import org.sagebionetworks.bridge.rest.model.AccessLevel;
import org.sagebionetworks.bridge.rest.model.Assessment;
import org.sagebionetworks.bridge.rest.model.EntityType;
import org.sagebionetworks.bridge.rest.model.Organization;
import org.sagebionetworks.bridge.rest.model.Permission;
import org.sagebionetworks.bridge.rest.model.PermissionDetail;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.user.TestUser;
import org.sagebionetworks.bridge.user.TestUserHelper;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.sagebionetworks.bridge.rest.model.AccessLevel.ADMIN;
import static org.sagebionetworks.bridge.rest.model.AccessLevel.DELETE;
import static org.sagebionetworks.bridge.rest.model.AccessLevel.EDIT;
import static org.sagebionetworks.bridge.rest.model.AccessLevel.LIST;
import static org.sagebionetworks.bridge.rest.model.AccessLevel.READ;
import static org.sagebionetworks.bridge.rest.model.EntityType.ASSESSMENT;
import static org.sagebionetworks.bridge.rest.model.EntityType.ORGANIZATION;
import static org.sagebionetworks.bridge.rest.model.EntityType.PARTICIPANTS;
import static org.sagebionetworks.bridge.rest.model.EntityType.STUDY;
import static org.sagebionetworks.bridge.rest.model.EntityType.STUDY_PI;
import static org.sagebionetworks.bridge.sdk.integration.Tests.getElement;

public class PermissionsTest {
    
    private TestUser admin;
    private TestUser user1;
    private TestUser user2;
    
    private OrganizationsApi orgApi;
    private StudiesApi studiesApi;
    private AssessmentsApi assessmentsApi;
    private PermissionsApi permissionsApi;
    
    private String orgId;
    private Organization org;
    
    private String studyId;
    private Study study;
    
    private String assessmentId;
    private Assessment assessment;
    
    @Before
    public void before() throws Exception {
        admin = TestUserHelper.getSignedInAdmin();
        user1 = TestUserHelper.createAndSignInUser(PermissionsTest.class, false);
        user2 = TestUserHelper.createAndSignInUser(PermissionsTest.class, false);
        
        studiesApi = admin.getClient(StudiesApi.class);
        orgApi = admin.getClient(OrganizationsApi.class);
        assessmentsApi = admin.getClient(AssessmentsApi.class);
        permissionsApi = admin.getClient(PermissionsApi.class);
    }
    
    @After
    public void after() throws Exception {
        if (user1 != null) {
            user1.signOutAndDeleteUser();
        }
        if (user2 != null) {
            user2.signOutAndDeleteUser();
        }
        
        try {
            assessmentsApi.deleteAssessment(assessmentId, true).execute();
        } catch (EntityNotFoundException e) {
            
        }
        try {
            studiesApi.deleteStudy(studyId, true).execute();
        } catch (EntityNotFoundException e) {
            
        }
        try {
            orgApi.deleteOrganization(orgId).execute();
        } catch (EntityNotFoundException e) {
            
        }
    }
    
    @Test
    public void test() throws Exception {
        
        // Creating an Org
        orgId = Tests.randomIdentifier(getClass());
        org = new Organization();
        org.setIdentifier(orgId);
        org.setName("test permissions org");
        org.description("testing permissions");
        orgApi.createOrganization(org).execute();
        
        // Creating a study
        studyId = Tests.randomIdentifier(getClass());
        study = new Study().identifier(studyId).name("test-study-name");
        studiesApi.createStudy(study).execute();
        
        // Creating assessment
        assessment = new Assessment()
                .identifier(Tests.randomIdentifier(getClass()))
                .title("test-assessment-title")
                .summary("Summary")
                .validationStatus("Not validated")
                .normingStatus("Not normed")
                .osName("Both")
                .ownerId(orgId);
        assessment = assessmentsApi.createAssessment(assessment).execute().body();
        assessmentId = assessment.getGuid();
        
        // Creating a permission
        Permission permitUser1Assessment = createNewPermission(user1.getUserId(), LIST, ASSESSMENT, assessmentId);
        
        PermissionDetail permDetUser1Assessment = permissionsApi.createPermission(permitUser1Assessment).execute().body();
        
        assertNotNull(permDetUser1Assessment);
        assertNotNull(permDetUser1Assessment.getGuid());
        assertEquals(LIST, permDetUser1Assessment.getAccessLevel());
        assertNotNull(permDetUser1Assessment.getEntity());
        assertEquals(ASSESSMENT, permDetUser1Assessment.getEntity().getEntityType());
        assertEquals(assessmentId, permDetUser1Assessment.getEntity().getEntityId());
        assertEquals("test-assessment-title", permDetUser1Assessment.getEntity().getEntityName());
        assertNotNull(permDetUser1Assessment.getAccount());
        assertEquals(user1.getUserId(), permDetUser1Assessment.getAccount().getIdentifier());
        assertNotNull(permDetUser1Assessment.getCreatedOn());
        assertEquals(permDetUser1Assessment.getCreatedOn(), permDetUser1Assessment.getModifiedOn());
        assertNotNull(permDetUser1Assessment.getVersion());
        
        // Failing to create a permission due to existing exact permission
        try {
            permissionsApi.createPermission(permitUser1Assessment).execute();
            fail("Should not have allowed a duplicate permission to be created.");
        } catch (ConstraintViolationException exception) {
            assertEquals("Cannot update this permission because it has duplicate access level permission", exception.getMessage());
        }
        
        // Failing to create a permission due to foreign key failing
        // Non-existent userId:
        try {
            permissionsApi.createPermission(createNewPermission("fake-user-id", LIST, ASSESSMENT, assessmentId))
                    .execute();
            fail("Should not have allowed a permission to be created with a fake user ID");
        } catch (ConstraintViolationException exception) {
            assertEquals("This permission cannot be created or updated because the referenced user account does not exist.",
                    exception.getMessage());
        }
        
        // Non-existent orgId:
        try {
            permissionsApi.createPermission(createNewPermission(user1.getUserId(), LIST, ORGANIZATION, "fake-org-id"))
                    .execute();
            fail("Should not have allowed a permission to be created with a fake org ID");
        } catch (ConstraintViolationException exception) {
            assertEquals("This permission cannot be created or updated because the referenced organization does not exist.",
                    exception.getMessage());
        }
        
        // Non-existent studyId:
        try {
            permissionsApi.createPermission(createNewPermission(user1.getUserId(), LIST, STUDY, "fake-study-id"))
                    .execute();
            fail("Should not have allowed a permission to be created with a fake study ID");
        } catch (ConstraintViolationException exception) {
            assertEquals("This permission cannot be created or updated because the referenced study does not exist.",
                    exception.getMessage());
        }
        
        // Non-existent assessmentId:
        try {
            permissionsApi.createPermission(createNewPermission(user1.getUserId(), LIST, ASSESSMENT, "fake-assessment-id"))
                    .execute().body();
            fail("Should not have allowed a permission to be created with a fake assessment ID");
        } catch (ConstraintViolationException exception) {
            assertEquals("This permission cannot be created or updated because the referenced assessment does not exist.",
                    exception.getMessage());
        }
        
        // Updating a permission fails with incorrect version
        permitUser1Assessment.setAccessLevel(EDIT);
        permitUser1Assessment.setVersion(5L);
        
        try {
            permissionsApi.updatePermission(permDetUser1Assessment.getGuid(), permitUser1Assessment).execute();
            fail("Should have failed to update due to an incorrect version.");
        } catch (ConcurrentModificationException exception) {
            assertEquals("Permission has the wrong version number; it may have been saved in the background.",
                    exception.getMessage());
        }
        
        // Successfully updating a permission
        permitUser1Assessment.setVersion(permDetUser1Assessment.getVersion());
        
        PermissionDetail permDetUser1AssessmentUpdated = permissionsApi.updatePermission(
                permDetUser1Assessment.getGuid(), permitUser1Assessment).execute().body();
        
        assertNotNull(permDetUser1AssessmentUpdated);
        assertEquals(permDetUser1Assessment.getGuid(), permDetUser1AssessmentUpdated.getGuid());
        assertEquals(EDIT, permDetUser1AssessmentUpdated.getAccessLevel());
        assertEquals(ASSESSMENT, permDetUser1AssessmentUpdated.getEntity().getEntityType());
        assertEquals(assessmentId, permDetUser1AssessmentUpdated.getEntity().getEntityId());
        assertEquals("test-assessment-title", permDetUser1AssessmentUpdated.getEntity().getEntityName());
        assertNotNull(permDetUser1AssessmentUpdated.getAccount());
        assertEquals(user1.getUserId(), permDetUser1AssessmentUpdated.getAccount().getIdentifier());
        assertEquals(permDetUser1Assessment.getCreatedOn(), permDetUser1AssessmentUpdated.getCreatedOn());
        assertNotEquals(permDetUser1AssessmentUpdated.getCreatedOn(), permDetUser1AssessmentUpdated.getModifiedOn());
        assertNotEquals(permDetUser1Assessment.getVersion(), permDetUser1AssessmentUpdated.getVersion());
        
        // Updating ignores all fields other than guid, accessLevel, and version
        Permission permitUser1AssessmentFake = createNewPermission("fake-user-id",
                DELETE,
                STUDY_PI,
                "fake-entity-id");
        permitUser1AssessmentFake.setVersion(permDetUser1AssessmentUpdated.getVersion());
        
        permDetUser1AssessmentUpdated = permissionsApi.updatePermission(
                permDetUser1Assessment.getGuid(), permitUser1AssessmentFake).execute().body();
        
        assertNotNull(permDetUser1AssessmentUpdated);
        assertEquals(permDetUser1Assessment.getGuid(), permDetUser1AssessmentUpdated.getGuid());
        assertEquals(DELETE, permDetUser1AssessmentUpdated.getAccessLevel());
        assertEquals(ASSESSMENT, permDetUser1AssessmentUpdated.getEntity().getEntityType());
        assertEquals(assessmentId, permDetUser1AssessmentUpdated.getEntity().getEntityId());
        assertEquals("test-assessment-title", permDetUser1AssessmentUpdated.getEntity().getEntityName());
        assertNotNull(permDetUser1AssessmentUpdated.getAccount());
        assertEquals(user1.getUserId(), permDetUser1AssessmentUpdated.getAccount().getIdentifier());
        assertEquals(permDetUser1Assessment.getCreatedOn(), permDetUser1AssessmentUpdated.getCreatedOn());
        assertNotEquals(permDetUser1AssessmentUpdated.getCreatedOn(), permDetUser1AssessmentUpdated.getModifiedOn());
        assertNotEquals(permitUser1AssessmentFake.getVersion(), permDetUser1AssessmentUpdated.getVersion());
        
        // Failing to update due to existing exact permission
        Permission permitUser1AssessmentAdmin = createNewPermission(
                permitUser1Assessment.getUserId(),
                ADMIN,
                permitUser1Assessment.getEntityType(),
                permitUser1Assessment.getEntityId());
        permitUser1AssessmentAdmin.setVersion(permDetUser1AssessmentUpdated.getVersion());
        PermissionDetail permDetUser1AssessmentAdmin = permissionsApi.createPermission(permitUser1AssessmentAdmin)
                .execute().body();
        
        try {
            permissionsApi.updatePermission(permDetUser1Assessment.getGuid(), permitUser1AssessmentAdmin).execute();
            fail("Should have failed to update due to existing duplicate permission.");
        } catch (ConstraintViolationException exception) {
            assertEquals("Cannot update this permission because it has duplicate access level permission",
                    exception.getMessage());
        }
        
        // Failing to update due to incorrect permission guid
        try {
            permissionsApi.updatePermission("fake-guid", permitUser1AssessmentAdmin).execute();
            fail("Should have failed to update due to GUID not matching an existing permission.");
        } catch (EntityNotFoundException exception) {
            assertEquals("Permission not found.", exception.getMessage());
        }
        
        // Getting permissions for a user
        Permission permitUser1Org = createNewPermission(user1.getUserId(), DELETE, ORGANIZATION, orgId);
        Permission permitUser1Participants = createNewPermission(user1.getUserId(), ADMIN, PARTICIPANTS, studyId);
        Permission permitUser2Participants = createNewPermission(user2.getUserId(), LIST, PARTICIPANTS, studyId);
        Permission permitUser2Study = createNewPermission(user2.getUserId(), READ, STUDY, studyId);
        
        PermissionDetail permDetUser1Org = permissionsApi.createPermission(permitUser1Org).execute().body();
        PermissionDetail permDetUser1Participants = permissionsApi.createPermission(permitUser1Participants).execute().body();
        PermissionDetail permDetUser2Participants = permissionsApi.createPermission(permitUser2Participants).execute().body();
        PermissionDetail permDetUser2Study = permissionsApi.createPermission(permitUser2Study).execute().body();
        
        List<PermissionDetail> permissionsForUser1 = permissionsApi.getPermissionsForUser(user1.getUserId())
                .execute().body().getItems();
        assertNotNull(permissionsForUser1);
        assertEquals(4, permissionsForUser1.size());
        assertTrue(getElement(permissionsForUser1, PermissionDetail::getGuid, permDetUser1AssessmentUpdated.getGuid()).isPresent());
        assertTrue(getElement(permissionsForUser1, PermissionDetail::getGuid, permDetUser1AssessmentAdmin.getGuid()).isPresent());
        assertTrue(getElement(permissionsForUser1, PermissionDetail::getGuid, permDetUser1Org.getGuid()).isPresent());
        assertTrue(getElement(permissionsForUser1, PermissionDetail::getGuid, permDetUser1Participants.getGuid()).isPresent());
        
        List<PermissionDetail> permissionsForUser2 = permissionsApi.getPermissionsForUser(user2.getUserId())
                .execute().body().getItems();
        assertNotNull(permissionsForUser2);
        assertEquals(2, permissionsForUser2.size());
        
        // Getting permissions for an entity
        List<PermissionDetail> permissionsForEntity = permissionsApi.getPermissionsForEntity("PARTICIPANTS", studyId)
                .execute().body().getItems();
        assertNotNull(permissionsForEntity);
        assertEquals(2, permissionsForEntity.size());
        
        PermissionDetail permUser1Participants = getElement(permissionsForEntity, PermissionDetail::getAccessLevel, ADMIN)
                .orElse(null);
        assertNotNull(permUser1Participants);
        assertEquals(ADMIN, permUser1Participants.getAccessLevel());;
        assertEquals(PARTICIPANTS, permUser1Participants.getEntity().getEntityType());
        assertEquals(studyId, permUser1Participants.getEntity().getEntityId());
        assertEquals("test-study-name", permUser1Participants.getEntity().getEntityName());
        assertEquals(user1.getUserId(), permUser1Participants.getAccount().getIdentifier());
        
        PermissionDetail permUser2Participants = getElement(permissionsForEntity, PermissionDetail::getAccessLevel, LIST)
                .orElse(null);
        assertNotNull(permUser2Participants);
        assertEquals(LIST, permUser2Participants.getAccessLevel());
        assertEquals(PARTICIPANTS, permUser2Participants.getEntity().getEntityType());
        assertEquals(studyId, permUser2Participants.getEntity().getEntityId());
        assertEquals("test-study-name", permUser2Participants.getEntity().getEntityName());
        assertEquals(user2.getUserId(), permUser2Participants.getAccount().getIdentifier());
        
        // Deleting a permission
        permissionsApi.deletePermission(permDetUser2Study.getGuid()).execute();
        
        permissionsForUser2 = permissionsApi.getPermissionsForUser(user2.getUserId()).execute().body().getItems();
        assertNotNull(permissionsForUser2);
        assertEquals(1, permissionsForUser2.size());
        assertEquals(permDetUser2Participants.getGuid(), permissionsForUser2.get(0).getGuid());
        
        // Deleting a user causes user's permissions to be deleted
        user2.signOutAndDeleteUser();
        
        List<PermissionDetail> permissionsForParticipants = permissionsApi.getPermissionsForEntity(
                "PARTICIPANTS",
                permDetUser2Participants.getEntity().getEntityId())
                .execute().body().getItems();
        assertNotNull(permissionsForParticipants);
        assertEquals(1, permissionsForParticipants.size());
        assertNotEquals(permDetUser2Participants.getGuid(), permissionsForParticipants.get(0).getGuid());
        assertNotEquals(user2.getUserId(), permissionsForParticipants.get(0).getAccount().getIdentifier());
        
        // Getting permissions for a non-existent user throws an exception
        try {
            permissionsApi.getPermissionsForUser(user2.getUserId()).execute();
            fail("Should have thrown an EntityNotFoundException");
        } catch (EntityNotFoundException e) {
            assertEquals("Account not found.", e.getMessage());
        }
        
        // Deleting an assessment causes permissions for that assessment to be deleted
        assessmentsApi.deleteAssessment(assessmentId, true).execute();
        
        permissionsForUser1 = permissionsApi.getPermissionsForUser(user1.getUserId()).execute().body().getItems();
        assertNotNull(permissionsForUser1);
        assertEquals(2, permissionsForUser1.size());
        assertFalse(getElement(permissionsForUser1, PermissionDetail::getGuid, permDetUser1AssessmentAdmin.getGuid())
                .isPresent());
        assertFalse(getElement(permissionsForUser1, PermissionDetail::getGuid, permDetUser1AssessmentUpdated.getGuid())
                .isPresent());
        
        // Getting permissions for a non-existent assessment throws an exception
        
        try {
            permissionsApi.getPermissionsForEntity("ASSESSMENT", assessmentId).execute();
            fail("Should have thrown an EntityNotFoundException");
        } catch (EntityNotFoundException e) {
            assertEquals("Assessment not found.", e.getMessage());
        }
        
        // Deleting a study causes related permissions to be deleted
        studiesApi.deleteStudy(studyId, true).execute();
        
        permissionsForUser1 = permissionsApi.getPermissionsForUser(user1.getUserId()).execute().body().getItems();
        assertNotNull(permissionsForUser1);
        assertEquals(1, permissionsForUser1.size());
        assertFalse(getElement(permissionsForUser1, PermissionDetail::getGuid, permDetUser1Participants.getGuid())
                .isPresent());
        
        // Getting permissions for a non-existent study throws an exception
        
        try {
            permissionsApi.getPermissionsForEntity("STUDY", studyId).execute();
            fail("Should have thrown an EntityNotFoundException");
        } catch (EntityNotFoundException e) {
            assertEquals("Study not found.", e.getMessage());
        }
        
        // Deleting an org causes related permissions to be deleted
        orgApi.deleteOrganization(orgId).execute();
        
        permissionsForUser1 = permissionsApi.getPermissionsForUser(user1.getUserId()).execute().body().getItems();
        assertNotNull(permissionsForUser1);
        assertTrue(permissionsForUser1.isEmpty());
        
        // Getting permissions for a non-existent org throws an exception
        
        try {
            permissionsApi.getPermissionsForEntity("ORGANIZATION", orgId).execute();
            fail("Should have thrown an EntityNotFoundException");
        } catch (EntityNotFoundException e) {
            assertEquals("Organization not found.", e.getMessage());
        }
    }
    
    private Permission createNewPermission(String userId, AccessLevel accessLevel, EntityType entityType, String entityId) {
        Permission permission = new Permission();
        permission.setUserId(userId);
        permission.setAccessLevel(accessLevel);
        permission.setEntityType(entityType);
        permission.setEntityId(entityId);
        return permission;
    }
    
}
