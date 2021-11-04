package org.sagebionetworks.bridge.sdk.integration;

import static java.util.stream.Collectors.toSet;
import static org.joda.time.DateTimeZone.UTC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.sagebionetworks.bridge.rest.model.Role.STUDY_COORDINATOR;
import static org.sagebionetworks.bridge.rest.model.Role.STUDY_DESIGNER;
import static org.sagebionetworks.bridge.sdk.integration.Tests.ORG_ID_2;
import static org.sagebionetworks.bridge.sdk.integration.Tests.STUDY_ID_1;
import static org.sagebionetworks.bridge.user.TestUserHelper.createAndSignInUser;
import static org.sagebionetworks.bridge.util.IntegTestUtils.SAGE_ID;

import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sagebionetworks.bridge.rest.api.ForConsentedUsersApi;
import org.sagebionetworks.bridge.rest.api.ForStudyCoordinatorsApi;
import org.sagebionetworks.bridge.rest.api.ForStudyDesignersApi;
import org.sagebionetworks.bridge.rest.api.OrganizationsApi;
import org.sagebionetworks.bridge.rest.exceptions.EntityNotFoundException;
import org.sagebionetworks.bridge.rest.exceptions.UnauthorizedException;
import org.sagebionetworks.bridge.rest.model.ForwardCursorReportDataList;
import org.sagebionetworks.bridge.rest.model.ReportData;
import org.sagebionetworks.bridge.rest.model.ReportIndex;
import org.sagebionetworks.bridge.user.TestUserHelper;
import org.sagebionetworks.bridge.user.TestUserHelper.TestUser;

import com.google.common.collect.ImmutableMap;

public class StudyParticipantReportTest {
    
    private static final Map<String,String> DATA = ImmutableMap.of("key", "value");
    private static final DateTime START_TIMESTAMP = DateTime.now(UTC).minusDays(7);
    private static final DateTime END_TIMESTAMP = DateTime.now(UTC).plusDays(7);
    private static final ReportData DATA_1 = new ReportData().dateTime(DateTime.now(UTC).minusDays(2)).data(DATA);
    private static final ReportData DATA_2 = new ReportData().dateTime(DateTime.now(UTC).minusDays(1)).data(DATA);

    private String reportId;
    private TestUser user;
    private TestUser studyDesigner;
    private TestUser studyCoordinator;
    
    private ForConsentedUsersApi userApi;
    private ForStudyCoordinatorsApi coordApi;
    private ForStudyDesignersApi designerApi;

    @Before
    public void before() throws Exception {
        reportId = Tests.randomIdentifier(getClass());
    }
    
    @After
    public void after() throws Exception {
        if (studyCoordinator == null) {
            studyCoordinator = createAndSignInUser(getClass(), false, STUDY_COORDINATOR);
            coordApi = studyCoordinator.getClient(ForStudyCoordinatorsApi.class);
        }
        try {
            coordApi.deleteStudyParticipantReport(STUDY_ID_1, user.getUserId(), reportId).execute();    
        } catch(EntityNotFoundException e) {
        }
        try {
            coordApi.deleteStudyParticipantReportIndex(STUDY_ID_1, reportId).execute().body();    
        } catch(EntityNotFoundException e) {
        }
        if (user != null) {
            user.signOutAndDeleteUser();
        }
        if (studyDesigner != null) {
            studyDesigner.signOutAndDeleteUser();
        }
        if (studyCoordinator != null) {
            studyCoordinator.signOutAndDeleteUser();
        }
    }
    
    @Test
    public void userCanWorkWithReports() throws Exception {
        user = createAndSignInUser(getClass(), true);
        userApi = user.getClient(ForConsentedUsersApi.class);
        
        userApi.saveUsersStudyParticipantReportRecord(STUDY_ID_1, reportId, DATA_1).execute();
        userApi.saveUsersStudyParticipantReportRecord(STUDY_ID_1, reportId, DATA_2).execute();
        
        ForwardCursorReportDataList list = userApi.getUsersStudyParticipantReport(STUDY_ID_1, reportId, 
                DateTime.now(UTC).minusDays(7), DateTime.now(UTC).plusDays(7), null, 10).execute().body();

        assertEquals(list.getItems().size(), 2);
        ReportData data1 = list.getItems().get(0);
        assertEquals(data1.getData(), DATA);
        assertEquals(data1.getDate(), DATA_1.getDateTime().toString());
        assertEquals(data1.getDateTime(), DATA_1.getDateTime());
        ReportData data2 = list.getItems().get(1);
        assertEquals(data2.getData(), DATA);
        assertEquals(data2.getDate(), DATA_2.getDateTime().toString());
        assertEquals(data2.getDateTime(), DATA_2.getDateTime());
    }

    @Test
    public void studyCoordinatorCanWorkWithProductionReports() throws Exception {
        user = createAndSignInUser(getClass(), true);
        studyCoordinator = createAndSignInUser(getClass(), false, STUDY_COORDINATOR);
        coordApi = studyCoordinator.getClient(ForStudyCoordinatorsApi.class);
        
        coordApi.saveStudyParticipantReportRecord(STUDY_ID_1, user.getUserId(), reportId, DATA_1).execute();
        coordApi.saveStudyParticipantReportRecord(STUDY_ID_1, user.getUserId(), reportId, DATA_2).execute();
        
        ForwardCursorReportDataList list = coordApi.getStudyParticipantReport(
                STUDY_ID_1, user.getUserId(), reportId, START_TIMESTAMP, END_TIMESTAMP, null, null).execute().body();
        assertEquals(list.getItems().size(), 2);
        
        coordApi.deleteStudyParticipantReport(STUDY_ID_1, user.getUserId(), reportId).execute();
        list = coordApi.getStudyParticipantReport(STUDY_ID_1, user.getUserId(), reportId, 
                START_TIMESTAMP, END_TIMESTAMP, null, null).execute().body();
        assertEquals(list.getItems().size(), 0);
        
        coordApi.deleteStudyParticipantReportIndex(STUDY_ID_1, reportId).execute().body();
        
        Set<String> indices = coordApi.getStudyParticipantReportIndices(STUDY_ID_1).execute()
                .body().getItems().stream().map(ReportIndex::getIdentifier).collect(toSet());
        assertFalse(indices.contains(reportId));
    }

    @Test
    public void studyDesignerCannotWorkWithProductionReports() throws Exception {
        user = createAndSignInUser(getClass(), true);
        studyDesigner = createAndSignInUser(getClass(), false, STUDY_DESIGNER);
        designerApi = studyDesigner.getClient(ForStudyDesignersApi.class);
        
        try {
            designerApi.saveStudyParticipantReportRecord(STUDY_ID_1, user.getUserId(), reportId, DATA_1).execute();
            fail("Should have thrown exception");
        } catch(EntityNotFoundException e) {
        }
        try {
            designerApi.getStudyParticipantReport(STUDY_ID_1, user.getUserId(), reportId, 
                    START_TIMESTAMP, END_TIMESTAMP, null, null).execute().body();
            fail("Should have thrown exception");
        } catch(EntityNotFoundException e) {
        }
        try {
            designerApi.deleteStudyParticipantReport(STUDY_ID_1, user.getUserId(), reportId).execute();
            fail("Should have thrown exception");
        } catch(EntityNotFoundException e) {
        }
        try {
            designerApi.deleteStudyParticipantReportIndex(STUDY_ID_1, reportId).execute().body();
            fail("Should have thrown exception");
        } catch(EntityNotFoundException e) {
        }
    }

    @Test
    public void studyCoordinatorCanWorkWithTestReports() throws Exception {
        user = new TestUserHelper.Builder(getClass()).withTestDataGroup().withConsentUser(true)
                .createAndSignInUser();
        studyDesigner = createAndSignInUser(getClass(), false, STUDY_DESIGNER);
        designerApi = studyDesigner.getClient(ForStudyDesignersApi.class);        
        
        designerApi.saveStudyParticipantReportRecord(STUDY_ID_1, user.getUserId(), reportId, DATA_1).execute();
        designerApi.saveStudyParticipantReportRecord(STUDY_ID_1, user.getUserId(), reportId, DATA_2).execute();
        
        ForwardCursorReportDataList list = designerApi.getStudyParticipantReport(
                STUDY_ID_1, user.getUserId(), reportId, START_TIMESTAMP, END_TIMESTAMP, null, null).execute().body();
        assertEquals(list.getItems().size(), 2);
        
        designerApi.deleteStudyParticipantReport(STUDY_ID_1, user.getUserId(), reportId).execute();
        list = designerApi.getStudyParticipantReport(STUDY_ID_1, user.getUserId(), reportId, 
                START_TIMESTAMP, END_TIMESTAMP, null, null).execute().body();
        assertEquals(list.getItems().size(), 0);
        
        designerApi.deleteStudyParticipantReportIndex(STUDY_ID_1, reportId).execute().body();
        
        Set<String> indices = designerApi.getStudyParticipantReportIndices(STUDY_ID_1).execute()
                .body().getItems().stream().map(ReportIndex::getIdentifier).collect(toSet());
        assertFalse(indices.contains(reportId));
    }
    
    @Test
    public void participantReportsAreDividedByStudy() throws Exception {
        user = createAndSignInUser(getClass(), true);
        studyCoordinator = createAndSignInUser(getClass(), false, STUDY_COORDINATOR);
        coordApi = studyCoordinator.getClient(ForStudyCoordinatorsApi.class);
        
        coordApi.saveStudyParticipantReportRecord(STUDY_ID_1, user.getUserId(), reportId, DATA_1).execute();
        coordApi.saveStudyParticipantReportRecord(STUDY_ID_1, user.getUserId(), reportId, DATA_2).execute();
        
        // This account cannot see these reports
        TestUser admin = TestUserHelper.getSignedInAdmin();
        studyDesigner = createAndSignInUser(getClass(), false, STUDY_COORDINATOR);
        admin.getClient(OrganizationsApi.class).removeMember(SAGE_ID, studyDesigner.getUserId()).execute();
        admin.getClient(OrganizationsApi.class).addMember(ORG_ID_2, studyDesigner.getUserId()).execute();
        designerApi = studyDesigner.getClient(ForStudyDesignersApi.class);       
        
        try {
            designerApi.getStudyParticipantReportIndices(STUDY_ID_1).execute();
            fail("Should have thrown exception");
        } catch(UnauthorizedException e) {
        }
        try {
            designerApi.getStudyParticipantReportIndex(STUDY_ID_1, reportId).execute();
            fail("Should have thrown exception");
        } catch(EntityNotFoundException e) {
        }
        try {
            designerApi.getStudyParticipantReport(
                    STUDY_ID_1, user.getUserId(), reportId, START_TIMESTAMP, END_TIMESTAMP, null, null).execute().body();
            fail("Should have thrown exception");
        } catch(EntityNotFoundException e) {
            
        }
        try {
            designerApi.getStudyParticipantReportIndex(STUDY_ID_1, reportId).execute().body();
            fail("Should have thrown exception");
        } catch(EntityNotFoundException e) {
        }
        try {
            designerApi.getStudyParticipantReportIndices(STUDY_ID_1).execute().body();
            fail("Should have thrown exception");
        } catch(UnauthorizedException e) {
        }
    }
}
