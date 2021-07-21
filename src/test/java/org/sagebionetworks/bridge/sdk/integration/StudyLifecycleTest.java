package org.sagebionetworks.bridge.sdk.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.sagebionetworks.bridge.rest.model.Role.STUDY_COORDINATOR;
import static org.sagebionetworks.bridge.rest.model.Role.STUDY_DESIGNER;
import static org.sagebionetworks.bridge.rest.model.StudyPhase.ANALYSIS;
import static org.sagebionetworks.bridge.rest.model.StudyPhase.COMPLETED;
import static org.sagebionetworks.bridge.rest.model.StudyPhase.DESIGN;
import static org.sagebionetworks.bridge.rest.model.StudyPhase.IN_FLIGHT;
import static org.sagebionetworks.bridge.rest.model.StudyPhase.RECRUITMENT;
import static org.sagebionetworks.bridge.rest.model.StudyPhase.WITHDRAWN;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.sagebionetworks.bridge.rest.api.StudiesApi;
import org.sagebionetworks.bridge.rest.exceptions.BadRequestException;
import org.sagebionetworks.bridge.rest.exceptions.UnauthorizedException;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.StudyPhase;
import org.sagebionetworks.bridge.user.TestUserHelper;
import org.sagebionetworks.bridge.user.TestUserHelper.TestUser;

import retrofit2.Call;

public class StudyLifecycleTest {
    
    @FunctionalInterface
    public interface Supplier {
        Call<Study> run() throws IOException;
    }
    
    private TestUser studyDesigner;
    private TestUser studyCoordinator;
    private List<String> studiesToDelete;
    
    @Before
    public void before() throws IOException {
        studiesToDelete = new ArrayList<>();
        studyDesigner = TestUserHelper.createAndSignInUser(StudyLifecycleTest.class, false, STUDY_DESIGNER);
        studyCoordinator = TestUserHelper.createAndSignInUser(StudyLifecycleTest.class, false, STUDY_COORDINATOR);
    }
    
    @After
    public void after() throws IOException {
        for (String studyId : studiesToDelete) {
            TestUser admin = TestUserHelper.getSignedInAdmin();
            admin.getClient(StudiesApi.class).deleteStudy(studyId, true).execute();
        }
        if (studyDesigner != null) {
            studyDesigner.signOutAndDeleteUser();
        }
        if (studyCoordinator != null) {
            studyCoordinator.signOutAndDeleteUser();
        }
    }
    
    @Test
    public void testPhaseTransitions() throws IOException {
        StudiesApi designerApi = studyDesigner.getClient(StudiesApi.class);
        String studyId = createStudy(designerApi);
        
        Study study = designerApi.getStudy(studyId).execute().body();
        assertEquals(DESIGN, study.getPhase());
        
        // However a designer cannot change the lifecycle.
        shouldFail(() -> designerApi.transitionStudyToRecruitment(studyId));
        
        // A coordinator can move the study through the phases, but there are allowed and 
        // not allowed phase changes.
        StudiesApi coordinatorApi = studyCoordinator.getClient(StudiesApi.class);
        shouldFail(() -> coordinatorApi.transitionStudyToInFlight(studyId));
        shouldFail(() -> coordinatorApi.transitionStudyToAnalysis(studyId));
        shouldFail(() -> coordinatorApi.transitionStudyToCompleted(studyId));
        
        shouldSucceed(() -> coordinatorApi.transitionStudyToRecruitment(studyId), RECRUITMENT);
        shouldSucceed(() -> coordinatorApi.transitionStudyToInFlight(studyId), IN_FLIGHT);
        shouldFail(() -> coordinatorApi.transitionStudyToDesign(studyId));
        shouldFail(() -> coordinatorApi.transitionStudyToCompleted(studyId));
        
        shouldSucceed(() -> coordinatorApi.transitionStudyToAnalysis(studyId), ANALYSIS);
        shouldFail(() -> coordinatorApi.transitionStudyToDesign(studyId));

        shouldSucceed(() -> coordinatorApi.transitionStudyToCompleted(studyId), COMPLETED);
        shouldFail(() -> coordinatorApi.transitionStudyToDesign(studyId));
        
        // Verify we can shift to withdrawn
        String studyId2 = createStudy(coordinatorApi);
        
        // From design...
        shouldSucceed(() -> coordinatorApi.transitionStudyToWithdrawn(studyId2), WITHDRAWN);

        String studyId3 = createStudy(coordinatorApi);;
        
        // From analysis...
        shouldSucceed(() -> coordinatorApi.transitionStudyToRecruitment(studyId3), RECRUITMENT);
        shouldSucceed(() -> coordinatorApi.transitionStudyToInFlight(studyId3), IN_FLIGHT);
        shouldSucceed(() -> coordinatorApi.transitionStudyToAnalysis(studyId3), ANALYSIS);
        shouldSucceed(() -> coordinatorApi.transitionStudyToWithdrawn(studyId3), WITHDRAWN);
    }

    protected String createStudy(StudiesApi coordinatorApi) throws IOException {
        String id = Tests.randomIdentifier(StudyLifecycleTest.class);
        studiesToDelete.add(id);
        Study study = new Study().identifier(id).name("StudyLifecycleTest");
        coordinatorApi.createStudy(study).execute();
        return id;
    }

    private void shouldFail(Supplier supplier) throws IOException {
        try {
            supplier.run().execute().body();
            fail("Should have thrown an exception");
        } catch(BadRequestException | UnauthorizedException e) {
        }
    }
    
    private void shouldSucceed(Supplier supplier, StudyPhase phase) throws IOException {
        Study resp = supplier.run().execute().body();
        assertEquals(phase, resp.getPhase());
    }
}
