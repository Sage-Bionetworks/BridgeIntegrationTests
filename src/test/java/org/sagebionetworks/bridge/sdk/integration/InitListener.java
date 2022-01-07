package org.sagebionetworks.bridge.sdk.integration;

import static org.sagebionetworks.bridge.rest.model.ActivityEventUpdateType.FUTURE_ONLY;
import static org.sagebionetworks.bridge.rest.model.ActivityEventUpdateType.IMMUTABLE;
import static org.sagebionetworks.bridge.rest.model.ActivityEventUpdateType.MUTABLE;
import static org.sagebionetworks.bridge.sdk.integration.Tests.API_SIGNIN;
import static org.sagebionetworks.bridge.sdk.integration.Tests.ORG_ID_1;
import static org.sagebionetworks.bridge.sdk.integration.Tests.ORG_ID_2;
import static org.sagebionetworks.bridge.sdk.integration.Tests.SHARED_SIGNIN;
import static org.sagebionetworks.bridge.sdk.integration.Tests.STUDY_ID_1;
import static org.sagebionetworks.bridge.sdk.integration.Tests.STUDY_ID_2;
import static org.sagebionetworks.bridge.util.IntegTestUtils.SAGE_ID;
import static org.sagebionetworks.bridge.util.IntegTestUtils.SAGE_NAME;
import static org.sagebionetworks.bridge.util.IntegTestUtils.TEST_APP_ID;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.runner.Description;
import org.junit.runner.Result;
import org.junit.runner.notification.RunListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.sagebionetworks.bridge.rest.api.AuthenticationApi;
import org.sagebionetworks.bridge.rest.api.ForAdminsApi;
import org.sagebionetworks.bridge.rest.api.ForOrgAdminsApi;
import org.sagebionetworks.bridge.rest.api.OrganizationsApi;
import org.sagebionetworks.bridge.rest.api.StudiesApi;
import org.sagebionetworks.bridge.rest.api.SubpopulationsApi;
import org.sagebionetworks.bridge.rest.exceptions.ConstraintViolationException;
import org.sagebionetworks.bridge.rest.exceptions.EntityNotFoundException;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.CustomEvent;
import org.sagebionetworks.bridge.rest.model.Environment;
import org.sagebionetworks.bridge.rest.model.Organization;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.Subpopulation;
import org.sagebionetworks.bridge.user.TestUser;
import org.sagebionetworks.bridge.user.TestUserHelper;

/**
 * We have some frequently used model classes that need specific relationships for
 * our tests to work given our permissions model. This listener sets these up one
 * time before running all our integration tests. These objects are safe to leave 
 * between runs of our tests in the API study (there is no cleanup).
 */
public class InitListener extends RunListener {
    private static final Logger LOG = LoggerFactory.getLogger(InitListener.class);

    public static final String EVENT_KEY1 = "event1";
    public static final String EVENT_KEY2 = "event2";
    public static final String EVENT_KEY3 = "event3";
    public static final String FAKE_ENROLLMENT = "fake_enrollment";
    public static final String CLINIC_VISIT = "clinic_visit";

    private boolean testRunInitialized;

    @Override
    public void testRunStarted(Description description) throws Exception {
        if (testRunInitialized) {
            return;
        }
        // Must do this first to initialize the logger correctly
        TestUser admin = TestUserHelper.getSignedInAdmin();

        LOG.info("Initializing some frequently used test objects...");

        StudiesApi studiesApi = admin.getClient(StudiesApi.class);
        try {
            studiesApi.getStudy(STUDY_ID_1).execute();
        } catch(EntityNotFoundException e) {
            Study study = new Study().identifier(STUDY_ID_1).name(STUDY_ID_1);
            studiesApi.createStudy(study).execute();
            LOG.info("  Creating study “{}”", STUDY_ID_1);
        }
        
        Study study1 = studiesApi.getStudy(STUDY_ID_1).execute().body();
        List<CustomEvent> events = new ArrayList<>();
        events.add(new CustomEvent().eventId(EVENT_KEY1).updateType(MUTABLE));
        events.add(new CustomEvent().eventId(EVENT_KEY2).updateType(IMMUTABLE));
        events.add(new CustomEvent().eventId(EVENT_KEY3).updateType(FUTURE_ONLY));
        events.add(new CustomEvent().eventId(FAKE_ENROLLMENT).updateType(MUTABLE));
        events.add(new CustomEvent().eventId(CLINIC_VISIT).updateType(MUTABLE));
        study1.setCustomEvents(events);
        studiesApi.updateStudy(study1.getIdentifier(), study1).execute();
        
        try {
            studiesApi.getStudy(STUDY_ID_2).execute();
        } catch(EntityNotFoundException e) {
            Study study = new Study().identifier(STUDY_ID_2).name(STUDY_ID_2);
            studiesApi.createStudy(study).execute();
            LOG.info("  Creating study “{}”", STUDY_ID_2);
        }

        OrganizationsApi orgsApi = admin.getClient(OrganizationsApi.class);
        try {
            orgsApi.getOrganization(ORG_ID_1).execute();
        } catch(EntityNotFoundException e) {
            Organization org = new Organization().identifier(ORG_ID_1).name(ORG_ID_1)
                    .description("Org 1 sponsors study 1 only");
            orgsApi.createOrganization(org).execute();
            LOG.info("  Creating organization “{}”", ORG_ID_1);
        }
        try {
            orgsApi.getOrganization(ORG_ID_2).execute();
        } catch(EntityNotFoundException e) {
            Organization org = new Organization().identifier(ORG_ID_2).name(ORG_ID_2)
                    .description("Org 2 sponsors study 2 only");
            orgsApi.createOrganization(org).execute();
            LOG.info("  Creating organization “{}”", ORG_ID_2);
        }
        try {
            orgsApi.getOrganization(SAGE_ID).execute();
        } catch(EntityNotFoundException e) {
            Organization org = new Organization().identifier(SAGE_ID).name(SAGE_NAME)
                    .description("Sage sponsors study1 and study2");
            orgsApi.createOrganization(org).execute();
            LOG.info("  Creating organization “{}”", SAGE_ID);
        }
        try {
            orgsApi.addStudySponsorship(SAGE_ID, STUDY_ID_1).execute();
            LOG.info("  {} sponsoring study “{}”", SAGE_NAME, STUDY_ID_1);
        } catch(ConstraintViolationException e) {
        }
        try {
            orgsApi.addStudySponsorship(SAGE_ID, STUDY_ID_2).execute();
            LOG.info("  {} sponsoring study “{}”", SAGE_NAME, STUDY_ID_2);
        } catch(ConstraintViolationException e) {
        }
        try {
            orgsApi.addStudySponsorship(ORG_ID_1, STUDY_ID_1).execute();
            LOG.info("  “{}” sponsoring study “{}”", ORG_ID_1, STUDY_ID_1);
        } catch(ConstraintViolationException e) {
        }
        try {
            orgsApi.addStudySponsorship(ORG_ID_2, STUDY_ID_2).execute();
            LOG.info("  “{}” sponsoring study “{}”", ORG_ID_2, STUDY_ID_2);
        } catch(ConstraintViolationException e) {
        }

        SubpopulationsApi subpopApi = admin.getClient(SubpopulationsApi.class);
        Subpopulation subpop = subpopApi.getSubpopulation(TEST_APP_ID).execute().body();
        if (!subpop.getStudyIdsAssignedOnConsent().contains(STUDY_ID_1)) {
            // Note: Required subpopulations can only have 1 study ID.
            subpop.setStudyIdsAssignedOnConsent(ImmutableList.of(STUDY_ID_1));
            subpopApi.updateSubpopulation(subpop.getGuid(), subpop).execute();
            LOG.info("  “{}” consent now enrolls participants in study “{}”", subpop.getGuid(), STUDY_ID_1);
        }
        
        // The admin should be in Sage Bionetworks if it is not already.
        if (!SAGE_ID.equals(admin.getSession().getOrgMembership())) {
            admin.getClient(ForOrgAdminsApi.class).addMember(SAGE_ID, admin.getUserId()).execute();
        }
        
        // Add dummy install link.
        ForAdminsApi adminApi = admin.getClient(ForAdminsApi.class);
        App app = adminApi.getUsersApp().execute().body();
        app.setInstallLinks(ImmutableMap.of("Universal", "http://example.com/"));
        adminApi.updateUsersApp(app).execute();
        
        // The bootstrap user does not have access to the shared app, so skip this in production.
        if (admin.getSession().getEnvironment() != Environment.PRODUCTION) {
            admin.getClient(AuthenticationApi.class).changeApp(SHARED_SIGNIN).execute();
            try {
                orgsApi.getOrganization(SAGE_ID).execute();
            } catch(EntityNotFoundException e) {
                Organization org = new Organization().identifier(SAGE_ID).name(SAGE_NAME)
                        .description("Sage sponsors study1 and study2");
                orgsApi.createOrganization(org).execute();
                LOG.info("  Creating organization “{}” in shared study", SAGE_ID);
            } finally {
                admin.getClient(AuthenticationApi.class).changeApp(API_SIGNIN).execute();
            }
        }
        testRunInitialized = true;
    }
    @Override
    public void testRunFinished(Result result) throws Exception {
        // noop
    }
}