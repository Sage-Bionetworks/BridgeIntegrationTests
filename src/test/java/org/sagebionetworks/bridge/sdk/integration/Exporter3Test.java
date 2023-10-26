package org.sagebionetworks.bridge.sdk.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.sagebionetworks.bridge.rest.model.ActivityEventUpdateType.MUTABLE;
import static org.sagebionetworks.bridge.rest.model.Role.STUDY_DESIGNER;
import static org.sagebionetworks.bridge.rest.model.PerformanceOrder.SEQUENTIAL;
import static org.sagebionetworks.bridge.sdk.integration.Tests.PASSWORD;
import static org.sagebionetworks.bridge.sdk.integration.Tests.STUDY_ID_1;
import static org.sagebionetworks.bridge.util.IntegTestUtils.SAGE_ID;
import static org.sagebionetworks.bridge.util.IntegTestUtils.TEST_APP_ID;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.PurgeQueueRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;

import com.google.common.io.Files;
import org.apache.commons.lang3.RandomStringUtils;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.Folder;
import org.sagebionetworks.repo.model.ObjectType;
import org.sagebionetworks.repo.model.Project;
import org.sagebionetworks.repo.model.Team;
import org.sagebionetworks.repo.model.dao.WikiPageKey;
import org.sagebionetworks.repo.model.dao.WikiPageKeyHelper;
import org.sagebionetworks.repo.model.annotation.v2.Annotations;
import org.sagebionetworks.repo.model.annotation.v2.AnnotationsValueType;
import org.sagebionetworks.repo.model.project.StsStorageLocationSetting;
import org.sagebionetworks.repo.model.table.MaterializedView;
import org.sagebionetworks.repo.model.table.TableEntity;
import org.sagebionetworks.repo.model.v2.wiki.V2WikiPage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.ApiClientProvider;
import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.RestUtils;
import org.sagebionetworks.bridge.rest.api.AccountsApi;
import org.sagebionetworks.bridge.rest.api.AssessmentsApi;
import org.sagebionetworks.bridge.rest.api.AuthenticationApi;
import org.sagebionetworks.bridge.rest.api.ConsentsApi;
import org.sagebionetworks.bridge.rest.api.ForAdminsApi;
import org.sagebionetworks.bridge.rest.api.ForConsentedUsersApi;
import org.sagebionetworks.bridge.rest.api.ForStudyDesignersApi;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.api.InternalApi;
import org.sagebionetworks.bridge.rest.api.ParticipantsApi;
import org.sagebionetworks.bridge.rest.api.SchedulesV2Api;
import org.sagebionetworks.bridge.rest.api.StudiesApi;
import org.sagebionetworks.bridge.rest.api.StudyParticipantsApi;
import org.sagebionetworks.bridge.rest.exceptions.ConsentRequiredException;
import org.sagebionetworks.bridge.rest.exceptions.EntityNotFoundException;
import org.sagebionetworks.bridge.rest.model.ActivityEventUpdateType;
import org.sagebionetworks.bridge.rest.model.Account;
import org.sagebionetworks.bridge.rest.model.AdherenceRecord;
import org.sagebionetworks.bridge.rest.model.AdherenceRecordUpdates;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.Assessment;
import org.sagebionetworks.bridge.rest.model.AssessmentReference2;
import org.sagebionetworks.bridge.rest.model.ClientInfo;
import org.sagebionetworks.bridge.rest.model.ConsentSignature;
import org.sagebionetworks.bridge.rest.model.ExportedRecordInfo;
import org.sagebionetworks.bridge.rest.model.ExportToAppNotification;
import org.sagebionetworks.bridge.rest.model.Exporter3Configuration;
import org.sagebionetworks.bridge.rest.model.Label;
import org.sagebionetworks.bridge.rest.model.ExporterSubscriptionRequest;
import org.sagebionetworks.bridge.rest.model.ExporterSubscriptionResult;
import org.sagebionetworks.bridge.rest.model.HealthDataRecordEx3;
import org.sagebionetworks.bridge.rest.model.ParticipantVersion;
import org.sagebionetworks.bridge.rest.model.Role;
import org.sagebionetworks.bridge.rest.model.Schedule2;
import org.sagebionetworks.bridge.rest.model.ScheduledSession;
import org.sagebionetworks.bridge.rest.model.Session;
import org.sagebionetworks.bridge.rest.model.SharingScope;
import org.sagebionetworks.bridge.rest.model.SharingScopeForm;
import org.sagebionetworks.bridge.rest.model.SignIn;
import org.sagebionetworks.bridge.rest.model.SignUp;
import org.sagebionetworks.bridge.rest.model.StudyBurst;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.rest.model.TimeWindow;
import org.sagebionetworks.bridge.rest.model.Timeline;
import org.sagebionetworks.bridge.rest.model.TimelineMetadata;
import org.sagebionetworks.bridge.rest.model.UploadMetadata;
import org.sagebionetworks.bridge.rest.model.UploadRequest;
import org.sagebionetworks.bridge.rest.model.UploadSession;
import org.sagebionetworks.bridge.rest.model.UploadViewEx3;
import org.sagebionetworks.bridge.rest.model.Withdrawal;
import org.sagebionetworks.bridge.user.TestUser;
import org.sagebionetworks.bridge.user.TestUserHelper;

@SuppressWarnings({ "SameParameterValue", "UnstableApiUsage" })
public class Exporter3Test {
    private static final Logger LOG = LoggerFactory.getLogger(Exporter3Test.class);
    private static final String MUTABLE_EVENT = "custom:event1";

    private static final String CONTENT_TYPE_TEXT_PLAIN = "text/plain";
    private static final byte[] UPLOAD_CONTENT = "This is the upload content".getBytes(StandardCharsets.UTF_8);
    private static final String UNPARSEABLE_USER_AGENT = "appName; 1 (something/1/test)";
    private static final String UNPARSEABLE_USER_AGENT_2 = "appName; 2 (something/2/test)";

    private static final String APP_NAME_FOR_USER = "app-name-for-user";
    private static final ClientInfo CLIENT_INFO_FOR_USER = new ClientInfo().appName(APP_NAME_FOR_USER).appVersion(1);
    private static final ClientInfo CLIENT_INFO_FOR_USER_2 = new ClientInfo().appName(APP_NAME_FOR_USER).appVersion(2);
    private static final String USER_AGENT_FOR_USER = RestUtils.getUserAgent(CLIENT_INFO_FOR_USER);
    private static final String USER_AGENT_FOR_USER_2 = RestUtils.getUserAgent(CLIENT_INFO_FOR_USER_2);

    // Fake record info.
    private static final String RECORD_ID = "fake-record";

    private static final String APP_FILE_ENTITY_ID = "syn1111";
    private static final String APP_PARENT_PROJECT_ID = "syn1222";
    private static final String APP_RAW_FOLDER_ID = "syn1333";
    private static final String APP_S3_BUCKET = "app-bucket";
    private static final String APP_S3_KEY = "app-record-key";

    private static final String STUDY_FILE_ENTITY_ID = "syn2111";
    private static final String STUDY_PARENT_PROJECT_ID = "syn2222";
    private static final String STUDY_RAW_FOLDER_ID = "syn2333";
    private static final String STUDY_S3_BUCKET = "study-bucket";
    private static final String STUDY_S3_KEY = "study-record-key";

    private static TestUser admin;
    private static ForAdminsApi adminsApi;
    private static DateTime oneHourAgo;
    private static SynapseClient synapseClient;
    private static AmazonSNS snsClient;
    private static AmazonSQS sqsClient;
    private static String testQueueArn;
    private static String testQueueUrl;
    private static ForWorkersApi workersApi;

    private static ForStudyDesignersApi studyDesignersApi;
    private static TestUser studyDesigner;

    private String extId;
    private Schedule2 schedule;
    private List<String> subscriptionArnList;
    private Assessment assessment;
    private String userId;

    @BeforeClass
    public static void beforeClass() throws Exception {
        Config config = Tests.loadTestConfig();

        synapseClient = Tests.getSynapseClient();
        admin = TestUserHelper.getSignedInAdmin();
        adminsApi = admin.getClient(ForAdminsApi.class);
        oneHourAgo = DateTime.now().minusHours(1);
        testQueueArn = config.get("integ.test.queue.arn");
        testQueueUrl = config.get("integ.test.queue.url");
        workersApi = admin.getClient(ForWorkersApi.class);
        studyDesigner = TestUserHelper.createAndSignInUser(StudyBurstTest.class, false, STUDY_DESIGNER);
        studyDesignersApi = studyDesigner.getClient(ForStudyDesignersApi.class);

        // Set up AWS clients.
        AWSCredentials awsCredentials = new BasicAWSCredentials(config.get("aws.key"),
                config.get("aws.secret.key"));
        AWSCredentialsProvider awsCredentialsProvider = new AWSStaticCredentialsProvider(awsCredentials);

        snsClient = AmazonSNSClientBuilder.standard().withCredentials(awsCredentialsProvider).build();
        sqsClient = AmazonSQSClientBuilder.standard().withCredentials(awsCredentialsProvider).build();

        // Clean up stray Synapse resources before test.
        deleteEx3Resources();

        // Clear queue. Note that PurgeQueue can only be called at most once every 60 seconds, or it will throw an
        // exception.
        PurgeQueueRequest purgeQueueRequest = new PurgeQueueRequest(testQueueUrl);
        sqsClient.purgeQueue(purgeQueueRequest);

        // Wait one second to ensure the queue is cleared.
        Thread.sleep(1000);

        // Init Exporter 3.
        adminsApi.initExporter3().execute().body();
    }

    @Before
    public void before() {
        extId = Tests.randomIdentifier(Exporter3Test.class);
        subscriptionArnList = new ArrayList<>();
    }

    @After
    public void after() throws Exception {
        for (String subscriptionArn : subscriptionArnList) {
            snsClient.unsubscribe(subscriptionArn);
        }

        if (userId != null) {
            admin.getClient(InternalApi.class).deleteAllParticipantVersionsForUser(userId).execute();
            adminsApi.deleteUser(userId).execute();
        }

        if (schedule != null) {
            SchedulesV2Api schedulesApi = admin.getClient(SchedulesV2Api.class);
            schedulesApi.deleteSchedule(schedule.getGuid()).execute();
        }

        if (assessment != null) {
            AssessmentsApi assessmentsApi = admin.getClient(AssessmentsApi.class);
            assessmentsApi.deleteAssessment(assessment.getGuid(), true).execute();
        }

        // Delete ex3ConfigForStudy for study.
        StudiesApi studiesApi = admin.getClient(StudiesApi.class);
        Study study = studiesApi.getStudy(Tests.STUDY_ID_1).execute().body();
        Exporter3Configuration ex3ConfigForStudy = study.getExporter3Configuration();
        deleteEx3Resources(ex3ConfigForStudy);

        if (ex3ConfigForStudy != null) {
            study.setExporter3Configuration(null);
            study.setExporter3Enabled(false);
            studiesApi.updateStudy(Tests.STUDY_ID_1, study).execute();
        }
    }

    @AfterClass
    public static void afterClass() throws Exception {
        // Clean up Synapse resources.
        deleteEx3Resources();

        if (studyDesigner != null) {
            studyDesigner.signOutAndDeleteUser();
        }
    }

    private static void deleteEx3Resources() throws IOException {
        // Delete for app.
        App app = adminsApi.getUsersApp().execute().body();
        Exporter3Configuration ex3Config = app.getExporter3Configuration();
        deleteEx3Resources(ex3Config);

        app.setExporter3Configuration(null);
        app.setExporter3Enabled(false);
        adminsApi.updateUsersApp(app).execute();

        // Delete for study.
        StudiesApi studiesApi = admin.getClient(StudiesApi.class);
        Study study = studiesApi.getStudy(Tests.STUDY_ID_1).execute().body();
        Exporter3Configuration ex3ConfigForStudy = study.getExporter3Configuration();
        deleteEx3Resources(ex3ConfigForStudy);

        study.setExporter3Configuration(null);
        study.setExporter3Enabled(false);
        studiesApi.updateStudy(Tests.STUDY_ID_1, study).execute();
    }

    private static void deleteEx3Resources(Exporter3Configuration ex3Config) {
        if (ex3Config == null) {
            // Exporter 3 is not configured on this app. We can skip this step.
            return;
        }

        // Delete the project. This automatically deletes the folder too.
        String projectId = ex3Config.getProjectId();
        if (projectId != null) {
            try {
                synapseClient.deleteEntityById(projectId, true);
            } catch (SynapseException ex) {
                LOG.error("Error deleting project " + projectId, ex);
            }
        }

        // Delete the data access team.
        Long dataAccessTeamId = ex3Config.getDataAccessTeamId();
        if (dataAccessTeamId != null) {
            try {
                synapseClient.deleteTeam(String.valueOf(dataAccessTeamId));
            } catch (SynapseException ex) {
                LOG.error("Error deleting team " + dataAccessTeamId, ex);
            }
        }

        // Storage locations are idempotent, so no need to delete that.

        // Delete the Create Study SNS topic.
        String createStudyTopicArn = ex3Config.getCreateStudyNotificationTopicArn();
        if (createStudyTopicArn != null) {
            try {
                snsClient.deleteTopic(createStudyTopicArn);
            } catch (AmazonClientException ex) {
                LOG.error("Error deleting topic " + createStudyTopicArn);
            }
        }

        // Delete the Export Notification SNS topic.
        String exportNotificationTopicArn = ex3Config.getExportNotificationTopicArn();
        if (exportNotificationTopicArn != null) {
            try {
                snsClient.deleteTopic(exportNotificationTopicArn);
            } catch (AmazonClientException ex) {
                LOG.error("Error deleting topic " + exportNotificationTopicArn);
            }
        }
    }

    @Test
    public void verifyInitExporter3() throws Exception {
        App updatedApp = adminsApi.getUsersApp().execute().body();
        assertTrue(updatedApp.isExporter3Enabled());
        Exporter3Configuration ex3Config = updatedApp.getExporter3Configuration();
        verifySynapseResources(ex3Config);
        assertTrue(ex3Config.isUploadTableEnabled());

        // Verify that the project has the correct app id annotation
        String projectId = ex3Config.getProjectId();
        Annotations annotations = synapseClient.getAnnotationsV2(projectId);
        assertEquals(AnnotationsValueType.STRING, annotations.getAnnotations().get("appId").getType());
        assertEquals(ImmutableList.of(TEST_APP_ID), annotations.getAnnotations().get("appId").getValue());
        assertFalse(annotations.getAnnotations().containsKey("studyId"));
    }

    @Test
    public void initForStudy() throws Exception {
        // Subscribe to study creation.
        ExporterSubscriptionRequest exporterSubscriptionRequest = new ExporterSubscriptionRequest();
        exporterSubscriptionRequest.putAttributesItem("RawMessageDelivery", "true");
        exporterSubscriptionRequest.setEndpoint(testQueueArn);
        exporterSubscriptionRequest.setProtocol("sqs");
        ExporterSubscriptionResult exporterSubscriptionResult = adminsApi.subscribeToCreateStudyNotifications(
                exporterSubscriptionRequest).execute().body();
        subscriptionArnList.add(exporterSubscriptionResult.getSubscriptionArn());

        // Init Exporter 3 for study.
        adminsApi.initExporter3ForStudy(STUDY_ID_1).execute();

        // Verify that study has been updated.
        Study updatedStudy = adminsApi.getStudy(STUDY_ID_1).execute().body();
        assertTrue(updatedStudy.isExporter3Enabled());
        Exporter3Configuration ex3Config = updatedStudy.getExporter3Configuration();
        verifySynapseResources(ex3Config);
        assertTrue(ex3Config.isUploadTableEnabled());

        // Verify that the project has the correct study id annotation.
        String projectId = ex3Config.getProjectId();
        Annotations annotations = synapseClient.getAnnotationsV2(projectId);
        assertEquals(AnnotationsValueType.STRING, annotations.getAnnotations().get("appId").getType());
        assertEquals(ImmutableList.of(TEST_APP_ID), annotations.getAnnotations().get("appId").getValue());
        assertEquals(AnnotationsValueType.STRING, annotations.getAnnotations().get("studyId").getType());
        assertEquals(ImmutableList.of(STUDY_ID_1), annotations.getAnnotations().get("studyId").getValue());

        // Verify notification in queue.
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
        receiveMessageRequest.setQueueUrl(testQueueUrl);
        receiveMessageRequest.setWaitTimeSeconds(10);

        List<Message> resultList = sqsClient.receiveMessage(receiveMessageRequest).getMessages();
        assertTrue(resultList.size() > 0);
        Message notification = resultList.get(0);
        String notificationJsonText = notification.getBody();
        JsonNode notificationNode = DefaultObjectMapper.INSTANCE.readTree(notificationJsonText);
        assertEquals(notificationNode.get("appId").textValue(), TEST_APP_ID);
        assertEquals(notificationNode.get("parentProjectId").textValue(), ex3Config.getProjectId());
        assertEquals(notificationNode.get("rawFolderId").textValue(), ex3Config.getRawDataFolderId());
        assertEquals(notificationNode.get("studyId").textValue(), STUDY_ID_1);

        // Delete message.
        sqsClient.deleteMessage(testQueueUrl, notification.getReceiptHandle());

        // Subscribe to export notifications for app and study. (For ease of testing, just re-use the subscription
        // request object.)
        exporterSubscriptionResult = adminsApi.subscribeToExportNotificationsForApp(exporterSubscriptionRequest)
                .execute().body();
        subscriptionArnList.add(exporterSubscriptionResult.getSubscriptionArn());

        exporterSubscriptionResult = adminsApi.subscribeToExportNotificationsForStudy(STUDY_ID_1,
                exporterSubscriptionRequest).execute().body();
        subscriptionArnList.add(exporterSubscriptionResult.getSubscriptionArn());

        // Fake an export notification and send it. We need to specify all of them or the server will mark this as
        // invalid.
        ExportToAppNotification exportNotification = new ExportToAppNotification();
        exportNotification.setAppId(TEST_APP_ID);
        exportNotification.setRecordId(RECORD_ID);

        ExportedRecordInfo appRecordInfo = new ExportedRecordInfo();
        appRecordInfo.setFileEntityId(APP_FILE_ENTITY_ID);
        appRecordInfo.setParentProjectId(APP_PARENT_PROJECT_ID);
        appRecordInfo.setRawFolderId(APP_RAW_FOLDER_ID);
        appRecordInfo.setS3Bucket(APP_S3_BUCKET);
        appRecordInfo.setS3Key(APP_S3_KEY);
        exportNotification.setRecord(appRecordInfo);

        ExportedRecordInfo studyRecordInfo = new ExportedRecordInfo();
        studyRecordInfo.setFileEntityId(STUDY_FILE_ENTITY_ID);
        studyRecordInfo.setParentProjectId(STUDY_PARENT_PROJECT_ID);
        studyRecordInfo.setRawFolderId(STUDY_RAW_FOLDER_ID);
        studyRecordInfo.setS3Bucket(STUDY_S3_BUCKET);
        studyRecordInfo.setS3Key(STUDY_S3_KEY);
        exportNotification.putStudyRecordsItem(STUDY_ID_1, studyRecordInfo);

        workersApi.sendExportNotifications(exportNotification).execute();

        // Receive notifications. Because we use a Standard queue and not an SQS queue, the messages can arrive in any
        // order.
        boolean foundAppNotification = false;
        boolean foundStudyNotification = false;
        receiveMessageRequest.setMaxNumberOfMessages(2);
        // Even with setMaxNumberOfMessages(2), SQS may return only 1 result. Call this in a loop until we have 2 total
        // messages.
        List<Message> allResultsList = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            resultList = sqsClient.receiveMessage(receiveMessageRequest).getMessages();
            allResultsList.addAll(resultList);
            if (allResultsList.size() >= 2) {
                break;
            }
        }
        assertTrue(allResultsList.size() >= 2);
        for (Message exportNotificationResult : allResultsList) {
            String exportNotificationJsonText = exportNotificationResult.getBody();
            JsonNode exportNotificationNode = DefaultObjectMapper.INSTANCE.readTree(exportNotificationJsonText);
            if ("ExportToAppNotification".equals(exportNotificationNode.get("type").textValue())) {
                foundAppNotification = true;
                // There are a lot of attributes. Just check a sample of attributes.
                assertEquals(TEST_APP_ID, exportNotificationNode.get("appId").textValue());
                assertEquals(RECORD_ID, exportNotificationNode.get("recordId").textValue());
                assertEquals(APP_S3_BUCKET, exportNotificationNode.get("record").get("s3Bucket").textValue());
                assertEquals(APP_S3_KEY, exportNotificationNode.get("record").get("s3Key").textValue());
                assertEquals(STUDY_S3_BUCKET, exportNotificationNode.get("studyRecords").get(STUDY_ID_1)
                        .get("s3Bucket").textValue());
                assertEquals(STUDY_S3_KEY, exportNotificationNode.get("studyRecords").get(STUDY_ID_1)
                        .get("s3Key").textValue());
            } else if ("ExportToStudyNotification".equals(exportNotificationNode.get("type").textValue())) {
                foundStudyNotification = true;
                // There are a lot of attributes. Just check a sample of attributes.
                assertEquals(TEST_APP_ID, exportNotificationNode.get("appId").textValue());
                assertEquals(STUDY_ID_1, exportNotificationNode.get("studyId").textValue());
                assertEquals(RECORD_ID, exportNotificationNode.get("recordId").textValue());
                assertEquals(STUDY_S3_BUCKET, exportNotificationNode.get("s3Bucket").textValue());
                assertEquals(STUDY_S3_KEY, exportNotificationNode.get("s3Key").textValue());
            }
        }

        assertTrue("Found app notification", foundAppNotification);
        assertTrue("Found study notification", foundStudyNotification);
    }

    private static void verifySynapseResources(Exporter3Configuration ex3Config) throws Exception {
        // Verify Synapse.
        long dataAccessTeamId = ex3Config.getDataAccessTeamId();
        Team dataAccessTeam = synapseClient.getTeam(String.valueOf(dataAccessTeamId));
        assertNotNull(dataAccessTeam);

        String projectId = ex3Config.getProjectId();
        Project project = synapseClient.getEntity(projectId, Project.class);
        assertNotNull(project);

        String participantVersionTableId = ex3Config.getParticipantVersionTableId();
        TableEntity participantVersionTable = synapseClient.getEntity(participantVersionTableId, TableEntity.class);
        assertNotNull(participantVersionTable);
        assertEquals(projectId, participantVersionTable.getParentId());

        String participantVersionDemographicsTableId = ex3Config.getParticipantVersionDemographicsTableId();
        TableEntity participantVersionDemographicsTable = synapseClient.getEntity(participantVersionDemographicsTableId, TableEntity.class);
        assertNotNull(participantVersionDemographicsTable);
        assertEquals(projectId, participantVersionDemographicsTable.getParentId());

        String participantVersionDemographicsViewId = ex3Config.getParticipantVersionDemographicsViewId();
        MaterializedView participantVersionDemographicsView = synapseClient.getEntity(participantVersionDemographicsViewId, MaterializedView.class);
        assertNotNull(participantVersionDemographicsView);
        assertEquals(projectId, participantVersionDemographicsView.getParentId());

        String rawFolderId = ex3Config.getRawDataFolderId();
        Folder rawFolder = synapseClient.getEntity(rawFolderId, Folder.class);
        assertNotNull(rawFolder);
        assertEquals(projectId, rawFolder.getParentId());

        long storageLocationId = ex3Config.getStorageLocationId();
        StsStorageLocationSetting storageLocation = synapseClient.getMyStorageLocationSetting(storageLocationId);
        assertNotNull(storageLocation);
        assertTrue(storageLocation.getStsEnabled());
    }

    // PARTICIPANT VERSION TESTS
    // This is part of Exporter3Test because Participant Versions need Exporter 3.0 to be enabled. I figured it was
    // better for Exporter 3.0 to be enabled and torn down in just one place instead of setting it up and tearing it
    // down multiple times.

    @Test
    public void selfSignedUpUser() throws Exception {
        ApiClientProvider unauthenticatedProvider = Tests.getUnauthenticatedClientProvider(admin.getClientManager(), TEST_APP_ID);
        AuthenticationApi authApi = unauthenticatedProvider.getAuthenticationApi();

        // Create user via sign up. Use external ID so we can bypass email verification.
        SignUp signUp = new SignUp().appId(TEST_APP_ID).addDataGroupsItem("test_user")
                .putExternalIdsItem(STUDY_ID_1, extId).password(PASSWORD);
        authApi.signUp(signUp).execute();

        // Sign in. This throws because we're not consented yet.
        SignIn signIn = new SignIn().appId(TEST_APP_ID).externalId(extId).password(PASSWORD);
        try {
            authApi.signInV4(signIn).execute();
            fail("signIn should have thrown ConsentRequiredException");
        } catch (ConsentRequiredException ex) {
            userId = ex.getSession().getId();
        }
        assertNotNull(userId);

        // Because the user is not consented, there should be no participant versions.
        List<ParticipantVersion> participantVersionList = workersApi.getAllParticipantVersionsForUser(TEST_APP_ID,
                userId).execute().body().getItems();
        assertTrue(participantVersionList.isEmpty());

        // Consent w/ sponsors_and_partners.
        ApiClientProvider.AuthenticatedClientProvider authenticatedProvider = unauthenticatedProvider
                .getAuthenticatedClientProviderBuilder().withExternalId(extId).withPassword(PASSWORD).build();
        ConsentSignature signature = new ConsentSignature().name("Eggplant McTester")
                .birthdate(LocalDate.parse("1970-04-04")).scope(SharingScope.SPONSORS_AND_PARTNERS);
        authenticatedProvider.getClient(ConsentsApi.class).createConsentSignature(TEST_APP_ID, signature).execute();

        // There is now one participant version.
        participantVersionList = workersApi.getAllParticipantVersionsForUser(TEST_APP_ID,
                userId).execute().body().getItems();
        assertEquals(1, participantVersionList.size());
        ParticipantVersion participantVersion1 = participantVersionList.get(0);
        verifyCommonParams(participantVersion1);
        assertEquals(1, participantVersion1.getParticipantVersion().intValue());
        assertEquals(SharingScope.SPONSORS_AND_PARTNERS, participantVersion1.getSharingScope());
        assertNull(participantVersion1.getTimeZone());

        Map<String, String> studyMembershipMap = participantVersion1.getStudyMemberships();
        assertEquals(1, studyMembershipMap.size());
        assertEquals(extId, studyMembershipMap.get(STUDY_ID_1));

        // Update participant by updating time zone.
        ParticipantsApi participantsApi = admin.getClient(ParticipantsApi.class);
        StudyParticipant participant = participantsApi.getParticipantById(userId, false).execute().body();
        participant.setClientTimeZone("America/Los_Angeles");
        participantsApi.updateParticipant(userId, participant).execute();

        // There is now version 2.
        participantVersionList = workersApi.getAllParticipantVersionsForUser(TEST_APP_ID,
                userId).execute().body().getItems();
        assertEquals(2, participantVersionList.size());
        ParticipantVersion participantVersion2 = participantVersionList.get(1);
        verifyCommonParams(participantVersion2);
        assertEquals(2, participantVersion2.getParticipantVersion().intValue());
        assertEquals(participantVersion1.getCreatedOn().getMillis(), participantVersion2.getCreatedOn().getMillis());
        assertNotEquals(participantVersion1.getModifiedOn().getMillis(), participantVersion2.getModifiedOn()
                .getMillis());
        assertEquals(SharingScope.SPONSORS_AND_PARTNERS, participantVersion2.getSharingScope());
        assertEquals("America/Los_Angeles", participantVersion2.getTimeZone());
        assertEquals(studyMembershipMap, participantVersion2.getStudyMemberships());

        // Participant updates itself.
        TestUser user = TestUserHelper.getSignedInUser(signIn);
        ForConsentedUsersApi consentedUsersApi = user.getClient(ForConsentedUsersApi.class);
        participant = consentedUsersApi.getUsersParticipantRecord(false).execute().body();
        participant.setClientTimeZone("Asia/Tokyo");
        consentedUsersApi.updateUsersParticipantRecord(participant).execute();

        // There is now version 3.
        participantVersionList = workersApi.getAllParticipantVersionsForUser(TEST_APP_ID,
                userId).execute().body().getItems();
        assertEquals(3, participantVersionList.size());
        ParticipantVersion participantVersion3 = participantVersionList.get(2);
        verifyCommonParams(participantVersion3);
        assertEquals(3, participantVersion3.getParticipantVersion().intValue());
        assertEquals(participantVersion2.getCreatedOn().getMillis(), participantVersion3.getCreatedOn().getMillis());
        assertNotEquals(participantVersion2.getModifiedOn().getMillis(), participantVersion3.getModifiedOn()
                .getMillis());
        assertEquals(SharingScope.SPONSORS_AND_PARTNERS, participantVersion3.getSharingScope());
        assertEquals("Asia/Tokyo", participantVersion3.getTimeZone());
        assertEquals(studyMembershipMap, participantVersion3.getStudyMemberships());

        // Update first/last name.
        participant = consentedUsersApi.getUsersParticipantRecord(false).execute().body();
        participant.setFirstName("Eggplant");
        participant.setLastName("McTester");
        consentedUsersApi.updateUsersParticipantRecord(participant).execute();

        // This doesn't create a new version.
        participantVersionList = workersApi.getAllParticipantVersionsForUser(TEST_APP_ID,
                userId).execute().body().getItems();
        assertEquals(3, participantVersionList.size());

        // Participant updates sharing scope to all_qualified_researchers.
        consentedUsersApi.changeSharingScope(new SharingScopeForm().scope(SharingScope.ALL_QUALIFIED_RESEARCHERS))
                .execute();

        // There is now version 4.
        participantVersionList = workersApi.getAllParticipantVersionsForUser(TEST_APP_ID,
                userId).execute().body().getItems();
        assertEquals(4, participantVersionList.size());
        ParticipantVersion participantVersion4 = participantVersionList.get(3);
        verifyCommonParams(participantVersion4);
        assertEquals(4, participantVersion4.getParticipantVersion().intValue());
        assertEquals(participantVersion3.getCreatedOn().getMillis(), participantVersion4.getCreatedOn().getMillis());
        assertNotEquals(participantVersion3.getModifiedOn().getMillis(), participantVersion4.getModifiedOn()
                .getMillis());
        assertEquals(SharingScope.ALL_QUALIFIED_RESEARCHERS, participantVersion4.getSharingScope());
        assertEquals("Asia/Tokyo", participantVersion4.getTimeZone());
        assertEquals(studyMembershipMap, participantVersion4.getStudyMemberships());

        // Toggle to no_sharing and back.
        consentedUsersApi.changeSharingScope(new SharingScopeForm().scope(SharingScope.NO_SHARING))
                .execute();
        consentedUsersApi.changeSharingScope(new SharingScopeForm().scope(SharingScope.ALL_QUALIFIED_RESEARCHERS))
                .execute();

        // This doesn't create a new version.
        participantVersionList = workersApi.getAllParticipantVersionsForUser(TEST_APP_ID,
                userId).execute().body().getItems();
        assertEquals(4, participantVersionList.size());

        // Toggle to no_sharing and update the participant again.
        consentedUsersApi.changeSharingScope(new SharingScopeForm().scope(SharingScope.NO_SHARING))
                .execute();
        participant = consentedUsersApi.getUsersParticipantRecord(false).execute().body();
        participant.setClientTimeZone("America/New_York");
        consentedUsersApi.updateUsersParticipantRecord(participant).execute();

        // This doesn't create a new version... yet.
        participantVersionList = workersApi.getAllParticipantVersionsForUser(TEST_APP_ID,
                userId).execute().body().getItems();
        assertEquals(4, participantVersionList.size());

        // Toggle back to all_qualified_researchers.
        consentedUsersApi.changeSharingScope(new SharingScopeForm().scope(SharingScope.ALL_QUALIFIED_RESEARCHERS))
                .execute();

        // There is now version 5.
        participantVersionList = workersApi.getAllParticipantVersionsForUser(TEST_APP_ID,
                userId).execute().body().getItems();
        assertEquals(5, participantVersionList.size());
        ParticipantVersion participantVersion5 = participantVersionList.get(4);
        verifyCommonParams(participantVersion5);
        assertEquals(5, participantVersion5.getParticipantVersion().intValue());
        assertEquals(participantVersion4.getCreatedOn().getMillis(), participantVersion5.getCreatedOn().getMillis());
        assertNotEquals(participantVersion4.getModifiedOn().getMillis(), participantVersion5.getModifiedOn()
                .getMillis());
        assertEquals(SharingScope.ALL_QUALIFIED_RESEARCHERS, participantVersion5.getSharingScope());
        assertEquals("America/New_York", participantVersion5.getTimeZone());
        assertEquals(studyMembershipMap, participantVersion5.getStudyMemberships());

        // Withdraw consent.
        consentedUsersApi.withdrawFromApp(new Withdrawal().reason("Testing")).execute();

        // Worker updates the user again.
        participant = participantsApi.getParticipantById(userId, false).execute().body();
        participant.setClientTimeZone("Europe/London");
        participantsApi.updateParticipant(userId, participant).execute();

        // Participant is withdrawn. There is no new version.
        participantVersionList = workersApi.getAllParticipantVersionsForUser(TEST_APP_ID,
                userId).execute().body().getItems();
        assertEquals(5, participantVersionList.size());

        // Test get by userId and participant version.
        ParticipantVersion result = workersApi.getParticipantVersion(TEST_APP_ID, userId, 1).execute().body();
        assertEquals(participantVersion1, result);

        result = workersApi.getParticipantVersion(TEST_APP_ID, userId, 2).execute().body();
        assertEquals(participantVersion2, result);

        result = workersApi.getParticipantVersion(TEST_APP_ID, userId, 3).execute().body();
        assertEquals(participantVersion3, result);

        result = workersApi.getParticipantVersion(TEST_APP_ID, userId, 4).execute().body();
        assertEquals(participantVersion4, result);

        result = workersApi.getParticipantVersion(TEST_APP_ID, userId, 5).execute().body();
        assertEquals(participantVersion5, result);
    }

    @Test
    public void adminCreatedUser() throws Exception {
        // This is a simpler test than the previous test. Go ahead and create a user that's already consented. However,
        // createUser() API automatically initially sets user to no_sharing.
        TestUser user = TestUserHelper.createAndSignInUser(Exporter3Test.class, true);
        userId = user.getUserId();

        // Add test_user data group.
        ParticipantsApi participantsApi = admin.getClient(ParticipantsApi.class);
        StudyParticipant participant = participantsApi.getParticipantById(userId, false).execute().body();
        participant.addDataGroupsItem("test_user");
        participantsApi.updateParticipant(userId, participant).execute();

        // Because user has no sharing scope, there is no participant version.
        List<ParticipantVersion> participantVersionList = workersApi.getAllParticipantVersionsForUser(TEST_APP_ID,
                userId).execute().body().getItems();
        assertTrue(participantVersionList.isEmpty());

        // Set sharing scope to all_qualified_researchers.
        user.getClient(ForConsentedUsersApi.class).changeSharingScope(new SharingScopeForm()
                .scope(SharingScope.ALL_QUALIFIED_RESEARCHERS)).execute();

        // There is now one participant version.
        participantVersionList = workersApi.getAllParticipantVersionsForUser(TEST_APP_ID,
                userId).execute().body().getItems();
        assertEquals(1, participantVersionList.size());
        ParticipantVersion participantVersion1 = participantVersionList.get(0);
        verifyCommonParams(participantVersion1);
        assertEquals(1, participantVersion1.getParticipantVersion().intValue());
        assertEquals(SharingScope.ALL_QUALIFIED_RESEARCHERS, participantVersion1.getSharingScope());

        Map<String, String> studyMembershipMap = participantVersion1.getStudyMemberships();
        assertEquals(1, studyMembershipMap.size());
        assertEquals("<none>", studyMembershipMap.get(STUDY_ID_1));

        // Unenrolling the participant will also prevent the participant from creating participant versions. (And this
        // is different from withdrawing consent.)
        StudyParticipantsApi studyParticipantsApi = admin.getClient(StudyParticipantsApi.class);
        studyParticipantsApi.withdrawParticipant(STUDY_ID_1, userId, "Testing").execute();

        // Update the participant again.
        participant = participantsApi.getParticipantById(userId, false).execute().body();
        participant.setClientTimeZone("America/Los_Angeles");
        participantsApi.updateParticipant(userId, participant).execute();

        // No new version is created.
        participantVersionList = workersApi.getAllParticipantVersionsForUser(TEST_APP_ID,
                userId).execute().body().getItems();
        assertEquals(1, participantVersionList.size());
    }

    @Test
    public void accountWithRoleHasNoVersions() throws Exception {
        // Create a developer w/ consent. This should never happen in real life, but we'll test it in case it happens.
        TestUser developer = TestUserHelper.createAndSignInUser(Exporter3Test.class, true, Role.DEVELOPER);
        userId = developer.getUserId();

        // Add test_user data group.
        AccountsApi accountsApi = admin.getClient(AccountsApi.class);
        Account account = accountsApi.getAccount(userId).execute().body();
        account.addDataGroupsItem("test_user");
        accountsApi.updateAccount(userId, account).execute();

        // Give it a sharing scope.
        developer.getClient(ForConsentedUsersApi.class).changeSharingScope(new SharingScopeForm()
                .scope(SharingScope.ALL_QUALIFIED_RESEARCHERS)).execute();

        // Accounts with roles never have versions.
        List<ParticipantVersion> participantVersionList = workersApi.getAllParticipantVersionsForUser(TEST_APP_ID,
                userId).execute().body().getItems();
        assertTrue(participantVersionList.isEmpty());
    }

    @Test
    public void backfillParticipantVersion() throws Exception {
        // Temporarily disable Exporter 3 for app. This way, when we create the test user, it doesn't create a
        // participant version.
        App app = adminsApi.getUsersApp().execute().body();
        app.setExporter3Enabled(false);
        adminsApi.updateUsersApp(app).execute();

        // Create test user. We have to set the sharing scope, too.
        TestUser user = TestUserHelper.createAndSignInUser(Exporter3Test.class, true);
        userId = user.getUserId();
        user.getClient(ForConsentedUsersApi.class).changeSharingScope(new SharingScopeForm()
                .scope(SharingScope.ALL_QUALIFIED_RESEARCHERS)).execute();

        // Enable Exporter 3.
        app = adminsApi.getUsersApp().execute().body();
        app.setExporter3Enabled(true);
        adminsApi.updateUsersApp(app).execute();

        // There are no participant versions, since Exporter 3 is was disabled when the user was created.
        List<ParticipantVersion> participantVersionList = workersApi.getAllParticipantVersionsForUser(TEST_APP_ID,
                userId).execute().body().getItems();
        assertTrue(participantVersionList.isEmpty());

        // Backfill participant version. There is now 1 participant version.
        workersApi.backfillParticipantVersion(TEST_APP_ID, userId).execute();
        participantVersionList = workersApi.getAllParticipantVersionsForUser(TEST_APP_ID, userId).execute().body()
                .getItems();
        assertEquals(1, participantVersionList.size());
    }

    private void verifyCommonParams(ParticipantVersion participantVersion) {
        assertEquals(TEST_APP_ID, participantVersion.getAppId());
        // Health code exists (but we don't know what it is).
        assertNotNull(participantVersion.getHealthCode());

        // createdOn and modifiedOn exist and are recent.
        assertTrue(participantVersion.getCreatedOn().isAfter(oneHourAgo));
        assertTrue(participantVersion.getModifiedOn().isAfter(oneHourAgo));

        assertEquals(1, participantVersion.getDataGroups().size());
        assertEquals("test_user", participantVersion.getDataGroups().get(0));
    }

    @Test
    public void exportTimelineForStudy() throws Exception {
        // Set up assessment.
        assessment = new Assessment()
                .phase(Assessment.PhaseEnum.DRAFT)
                .title(StudyBurstTest.class.getSimpleName())
                .osName("Universal")
                .ownerId(SAGE_ID)
                .identifier(Tests.randomIdentifier(Exporter3Test.class));

        assessment = studyDesigner.getClient(ForStudyDesignersApi.class)
                .createAssessment(assessment).execute().body();

        setupSchedule(MUTABLE_EVENT, MUTABLE, "P2D");

        Exporter3Configuration exporter3Config = adminsApi.exportTimelineForStudy(Tests.STUDY_ID_1).execute().body();

        WikiPageKey key = WikiPageKeyHelper.createWikiPageKey(exporter3Config.getProjectId(), ObjectType.ENTITY, exporter3Config.getWikiPageId());
        V2WikiPage getWiki = synapseClient.getV2WikiPage(key);
        String markdownId = getWiki.getMarkdownFileHandleId();

        // First time creating wiki page:
        // 1. wiki page is created
        // 2. wiki page attributes are as expected: title; markdownFileHandleId created/not null
        assertEquals(getWiki.getTitle(), "Exported Timeline for " + STUDY_ID_1);
        assertNotNull(getWiki.getMarkdownFileHandleId());

        // wiki page already exists:
        // 1. wiki page markdown handle id changes.
        Exporter3Configuration exporter3Config_again = adminsApi.exportTimelineForStudy(STUDY_ID_1).execute().body();
        V2WikiPage updatedWiki = synapseClient.getV2WikiPage(key);
        String newMarkdownId = updatedWiki.getMarkdownFileHandleId();
        assertNotEquals(markdownId, newMarkdownId);
    }

    private void setupSchedule(String originEventId, ActivityEventUpdateType burstUpdateType, String delayPeriod)
            throws Exception {
        // clean up any schedule that is there
        try {
            schedule = admin.getClient(SchedulesV2Api.class).getScheduleForStudy(STUDY_ID_1).execute().body();
            admin.getClient(SchedulesV2Api.class).deleteSchedule(schedule.getGuid()).execute();
        } catch (EntityNotFoundException e) {
            LOG.info("No schedule found." );
        }

        schedule = new Schedule2();
        schedule.setName("Test Schedule [Exporter3test]");
        schedule.setDuration("P10D");

        StudyBurst burst = new StudyBurst()
                .identifier("burst1")
                .originEventId(originEventId)
                .delay(delayPeriod)
                .interval("P1D")
                .occurrences(4)
                .updateType(burstUpdateType);
        schedule.setStudyBursts(ImmutableList.of(burst));

        Session session = new Session();
        session.setName("Simple assessment");
        session.addLabelsItem(new Label().lang("en").value("Take the assessment"));
        session.addStartEventIdsItem("timeline_retrieved");
        session.addStudyBurstIdsItem("burst1");
        session.setPerformanceOrder(SEQUENTIAL);

        AssessmentReference2 ref = new AssessmentReference2()
                .guid(assessment.getGuid())
                .appId(TEST_APP_ID)
                .revision(5)
                .addLabelsItem(new Label().lang("en").value("Test Value"))
                .minutesToComplete(10)
                .title("A title")
                .identifier(assessment.getIdentifier());
        session.addAssessmentsItem(ref);
        session.addTimeWindowsItem(new TimeWindow().startTime("08:00").expiration("PT3H"));
        schedule.addSessionsItem(session);

        schedule = studyDesignersApi.saveScheduleForStudy(STUDY_ID_1, schedule).execute().body();
    }

    // UPLOAD TESTS
    // Basic tests for the Health Data that is generated by Exporter 3.0.

    @Test
    public void upload_completedByUploader() throws Exception {
        TestUser user = new TestUserHelper.Builder(Exporter3Test.class).withClientInfo(CLIENT_INFO_FOR_USER)
                .withConsentUser(true).createAndSignInUser();
        userId = user.getUserId();

        // Upload.
        File file = createUploadFile();
        UploadSession session = createUploadSession(user, file, null);
        String uploadId = session.getId();

        // Change User-Agent to make sure that we don't overwrite it.
        user = loginUserWithClientInfo(user, CLIENT_INFO_FOR_USER_2);

        // Complete upload with a different client info.
        RestUtils.uploadToS3(file, session.getUrl(), CONTENT_TYPE_TEXT_PLAIN);
        ForConsentedUsersApi usersApi = user.getClient(ForConsentedUsersApi.class);
        usersApi.completeUploadSession(uploadId, true, false).execute();

        // Get health data and verify client info.
        HealthDataRecordEx3 record = usersApi.getRecordEx3ById(uploadId, "false").execute().body();
        String clientInfoJsonText = record.getClientInfo();
        JsonNode deser = DefaultObjectMapper.INSTANCE.readTree(clientInfoJsonText);
        assertEquals(APP_NAME_FOR_USER, deser.get("appName").textValue());
        assertEquals(1, deser.get("appVersion").intValue());

        // Note that because JavaSDK includes default client info, we have have to check for a prefix.
        assertTrue(record.getUserAgent().startsWith(USER_AGENT_FOR_USER));
    }

    @Test
    public void upload_requestHasNoUserAgent() throws Exception {
        // Create user without User-Agent.
        TestUser user = new TestUserHelper.Builder(Exporter3Test.class).withIncludeUserAgent(false)
                .withConsentUser(true).createAndSignInUser();
        userId = user.getUserId();

        // Upload.
        File file = createUploadFile();
        UploadSession session = createUploadSession(user, file, null);
        String uploadId = session.getId();

        // Change User-Agent to make sure that we don't overwrite it.
        user = loginUserWithClientInfo(user, CLIENT_INFO_FOR_USER_2);

        // Complete upload with a different client info.
        RestUtils.uploadToS3(file, session.getUrl(), CONTENT_TYPE_TEXT_PLAIN);
        ForConsentedUsersApi usersApi = user.getClient(ForConsentedUsersApi.class);
        usersApi.completeUploadSession(uploadId, true, false).execute();

        // Get health data and verify client info.
        HealthDataRecordEx3 record = usersApi.getRecordEx3ById(uploadId, "false").execute().body();
        String clientInfoJsonText = record.getClientInfo();
        JsonNode deser = DefaultObjectMapper.INSTANCE.readTree(clientInfoJsonText);
        assertEquals(APP_NAME_FOR_USER, deser.get("appName").textValue());
        assertEquals(2, deser.get("appVersion").intValue());

        // Note that because JavaSDK includes default client info, we have have to check for a prefix.
        assertTrue(record.getUserAgent().startsWith(USER_AGENT_FOR_USER_2));
    }

    @Test
    public void upload_completedByWorker() throws Exception {
        // Create user without User-Agent.
        TestUser user = new TestUserHelper.Builder(Exporter3Test.class).withIncludeUserAgent(false)
                .withConsentUser(true).createAndSignInUser();
        userId = user.getUserId();

        // Upload.
        File file = createUploadFile();
        UploadSession session = createUploadSession(user, file, null);
        String uploadId = session.getId();

        // Change User-Agent to make sure that we don't overwrite it.
        user = loginUserWithClientInfo(user, CLIENT_INFO_FOR_USER_2);

        // Upload the file to S3.
        RestUtils.uploadToS3(file, session.getUrl(), CONTENT_TYPE_TEXT_PLAIN);

        // Complete upload with the worker.
        workersApi.completeUploadSession(uploadId, true, false).execute();

        // Get health data and verify client info.
        ForConsentedUsersApi usersApi = user.getClient(ForConsentedUsersApi.class);
        HealthDataRecordEx3 record = usersApi.getRecordEx3ById(uploadId, "false").execute().body();
        String clientInfoJsonText = record.getClientInfo();
        JsonNode deser = DefaultObjectMapper.INSTANCE.readTree(clientInfoJsonText);
        assertEquals(APP_NAME_FOR_USER, deser.get("appName").textValue());
        assertEquals(2, deser.get("appVersion").intValue());

        // Note that because JavaSDK includes default client info, we have have to check for a prefix.
        assertTrue(record.getUserAgent().startsWith(USER_AGENT_FOR_USER_2));
    }

    private static TestUser loginUserWithClientInfo(TestUser user, ClientInfo clientInfo) {
        SignIn signIn = user.getSignIn();
        ClientManager clientManager = new ClientManager.Builder().withClientInfo(clientInfo)
                .withSignIn(signIn).build();
        TestUser updatedUser = new TestUser(signIn, clientManager, user.getUserId());
        updatedUser.signInAgain();
        return updatedUser;
    }

    @Test
    public void unparseableUserAgentFromUploadRequest() throws Exception {
        TestUser user = new TestUserHelper.Builder(Exporter3Test.class).withUserAgentOverride(UNPARSEABLE_USER_AGENT)
                .withConsentUser(true).createAndSignInUser();
        userId = user.getUserId();

        // Upload.
        File file = createUploadFile();
        UploadSession session = createUploadSession(user, file, null);
        String uploadId = session.getId();

        // Change User-Agent to make sure that we don't overwrite it.
        user = loginUserWithUserAgent(user, UNPARSEABLE_USER_AGENT_2);

        // Complete upload with a different client info.
        RestUtils.uploadToS3(file, session.getUrl(), CONTENT_TYPE_TEXT_PLAIN);
        ForConsentedUsersApi usersApi = user.getClient(ForConsentedUsersApi.class);
        usersApi.completeUploadSession(uploadId, true, false).execute();

        // Get health data and verify user agent. (No need to check client info.)
        HealthDataRecordEx3 record = usersApi.getRecordEx3ById(uploadId, "false").execute().body();
        assertEquals(UNPARSEABLE_USER_AGENT, record.getUserAgent());
    }

    @Test
    public void unparseableUserAgentFromUploadComplete() throws Exception {
        // Create user without User-Agent.
        TestUser user = new TestUserHelper.Builder(Exporter3Test.class).withIncludeUserAgent(false)
                .withConsentUser(true).createAndSignInUser();
        userId = user.getUserId();

        // Upload.
        File file = createUploadFile();
        UploadSession session = createUploadSession(user, file, null);
        String uploadId = session.getId();

        // Change User-Agent to make sure that we don't overwrite it.
        user = loginUserWithUserAgent(user, UNPARSEABLE_USER_AGENT_2);

        // Complete upload with a different client info.
        RestUtils.uploadToS3(file, session.getUrl(), CONTENT_TYPE_TEXT_PLAIN);
        ForConsentedUsersApi usersApi = user.getClient(ForConsentedUsersApi.class);
        usersApi.completeUploadSession(uploadId, true, false).execute();

        // Get health data and verify user agent.
        HealthDataRecordEx3 record = usersApi.getRecordEx3ById(uploadId, "false").execute().body();
        assertEquals(UNPARSEABLE_USER_AGENT_2, record.getUserAgent());
    }

    @Test
    public void unparseableUserAgentFromRequestInfo() throws Exception {
        // Create user without User-Agent.
        TestUser user = new TestUserHelper.Builder(Exporter3Test.class).withIncludeUserAgent(false)
                .withConsentUser(true).createAndSignInUser();
        userId = user.getUserId();

        // Upload.
        File file = createUploadFile();
        UploadSession session = createUploadSession(user, file, null);
        String uploadId = session.getId();

        // Change User-Agent to make sure that we don't overwrite it.
        user = loginUserWithUserAgent(user, UNPARSEABLE_USER_AGENT_2);

        // Upload the file to S3.
        RestUtils.uploadToS3(file, session.getUrl(), CONTENT_TYPE_TEXT_PLAIN);

        // Complete upload with the worker.
        workersApi.completeUploadSession(uploadId, true, false).execute();

        // Get health data and verify client info.
        ForConsentedUsersApi usersApi = user.getClient(ForConsentedUsersApi.class);
        HealthDataRecordEx3 record = usersApi.getRecordEx3ById(uploadId, "false").execute().body();
        assertEquals(UNPARSEABLE_USER_AGENT_2, record.getUserAgent());
    }

    private static TestUser loginUserWithUserAgent(TestUser user, String userAgent) {
        SignIn signIn = user.getSignIn();
        ClientManager clientManager = new ClientManager.Builder().withUserAgentOverride(userAgent).withSignIn(signIn)
                .build();
        TestUser updatedUser = new TestUser(signIn, clientManager, user.getUserId());
        updatedUser.signInAgain();
        return updatedUser;
    }

    @Test
    public void getUploadEx3() throws Exception {
        // Set up a schedule so we can test the timeline in getUploadViewEx3.
        AssessmentsApi assessmentsApi = admin.getClient(AssessmentsApi.class);
        SchedulesV2Api schedulesApi = admin.getClient(SchedulesV2Api.class);

        String assessmentId = getClass().getSimpleName() + "-" + RandomStringUtils.randomAlphabetic(10);

        assessment = new Assessment()
                .phase(Assessment.PhaseEnum.DRAFT)
                .title(assessmentId)
                .osName("Universal")
                .ownerId("sage-bionetworks")
                .identifier(assessmentId);
        assessment = assessmentsApi.createAssessment(assessment).execute().body();

        schedule = new Schedule2();
        schedule.setName("Test Schedule [Exporter3Test]");
        schedule.setDuration("P1W");

        Session session = new Session();
        session.setName("One time task");
        session.addStartEventIdsItem("enrollment");
        session.setPerformanceOrder(SEQUENTIAL);

        AssessmentReference2 ref = new AssessmentReference2()
                .guid(assessment.getGuid()).appId(TEST_APP_ID)
                .title(assessmentId)
                .identifier(assessment.getIdentifier())
                .revision(assessment.getRevision().intValue());
        session.addAssessmentsItem(ref);
        session.addTimeWindowsItem(new TimeWindow().startTime("00:00").expiration("P1W"));
        schedule.addSessionsItem(session);

        schedule = schedulesApi.saveScheduleForStudy(STUDY_ID_1, schedule).execute().body();

        Timeline timeline = schedulesApi.getTimelineForStudy(STUDY_ID_1).execute().body();

        // Could also use the session instanceGuid, it doesn't matter.
        ScheduledSession scheduledSession = timeline.getSchedule().get(0);
        String sessionInstanceGuid = scheduledSession.getInstanceGuid();
        String instanceGuid = scheduledSession.getAssessments().get(0).getInstanceGuid();

        // Create test user.
        TestUser user = new TestUserHelper.Builder(Exporter3Test.class).withClientInfo(CLIENT_INFO_FOR_USER)
                .withConsentUser(true).createAndSignInUser();
        userId = user.getUserId();

        // Get the user's healthcode.
        StudyParticipant participant = admin.getClient(ParticipantsApi.class).getParticipantById(userId,
                false).execute().body();
        String healthCode = participant.getHealthCode();

        // Upload.
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("instanceGuid", instanceGuid);

        String uploadId = createUpload(user, metadata);
        ForConsentedUsersApi usersApi = user.getClient(ForConsentedUsersApi.class);
        usersApi.completeUploadSession(uploadId, true, false).execute();

        // Make some adherence records for our test. eventTimestamp and startedOn are required, but can be any
        // arbitrary timestamp.
        DateTime timestamp = DateTime.now();
        AdherenceRecord adherenceWithInstanceGuid = new AdherenceRecord().instanceGuid(instanceGuid)
                .eventTimestamp(timestamp).startedOn(timestamp);

        // Make a second adherence record with the wrong instanceGuid, just to test that we can query by upload ID.
        // The wrong instanceGuid has to be real, or else the record won't save.
        AdherenceRecord adherenceWithUploadId = new AdherenceRecord().instanceGuid(sessionInstanceGuid)
                .eventTimestamp(timestamp).startedOn(timestamp).addUploadIdsItem(uploadId);
        AdherenceRecordUpdates adherenceRecordUpdates = new AdherenceRecordUpdates()
                .addRecordsItem(adherenceWithInstanceGuid).addRecordsItem(adherenceWithUploadId);
        usersApi.updateAdherenceRecords(STUDY_ID_1, adherenceRecordUpdates).execute();

        // Call getUploadEx3 for self.
        UploadViewEx3 upload = usersApi.getUploadEx3(uploadId, true).execute().body();
        verifyUpload(uploadId, healthCode, userId, instanceGuid, upload, false);

        upload = usersApi.getUploadEx3ForStudy(STUDY_ID_1, uploadId, true, true).execute()
                .body();
        verifyUpload(uploadId, healthCode, userId, instanceGuid, upload, true);

        // Call getUploadEx3 for worker.
        upload = workersApi.getUploadEx3ForWorker(TEST_APP_ID, uploadId, true).execute().body();
        verifyUpload(uploadId, healthCode, userId, instanceGuid, upload, false);

        upload = workersApi.getUploadEx3ForStudyForWorker(TEST_APP_ID, STUDY_ID_1, uploadId, true,
                true).execute().body();
        verifyUpload(uploadId, healthCode, userId, instanceGuid, upload, true);
    }

    private File createUploadFile() throws IOException {
        // Create a temp file so that we can use RestUtils.
        File file = File.createTempFile("text", ".txt");
        Files.write(UPLOAD_CONTENT, file);
        return file;
    }

    private UploadSession createUploadSession(TestUser user, File file, Map<String, Object> metadata)
            throws IOException {
        // Create upload request. RestUtils defaults to application/zip. We want to overwrite this.
        UploadRequest uploadRequest = RestUtils.makeUploadRequestForFile(file);
        uploadRequest.setContentType(CONTENT_TYPE_TEXT_PLAIN);
        uploadRequest.setEncrypted(false);
        uploadRequest.setZipped(false);
        if (metadata != null) {
            uploadRequest.setMetadata(metadata);
        }

        // Upload.
        UploadSession session = user.getClient(ForConsentedUsersApi.class).requestUploadSession(uploadRequest)
                .execute().body();
        return session;
    }

    private String createUpload(TestUser user, Map<String, Object> metadata) throws IOException {
        File file = createUploadFile();
        UploadSession session = createUploadSession(user, file, metadata);
        RestUtils.uploadToS3(file, session.getUrl(), CONTENT_TYPE_TEXT_PLAIN);
        return session.getId();
    }

    private static void verifyUpload(String expectedUploadId, String expectedHealthCode, String expectedUserId,
            String expectedInstanceGuid, UploadViewEx3 upload, boolean verifyAdherence) {
        assertEquals(expectedUploadId, upload.getId());
        assertEquals(expectedHealthCode, upload.getHealthCode());
        assertEquals(expectedUserId, upload.getUserId());

        // Verify basics for record, timeline, and upload metadata.
        HealthDataRecordEx3 record = upload.getRecord();
        assertEquals(expectedUploadId, record.getId());
        assertEquals(expectedHealthCode, record.getHealthCode());

        TimelineMetadata timelineMetadata = upload.getTimelineMetadata();
        assertEquals(expectedInstanceGuid, timelineMetadata.getMetadata().get("assessmentInstanceGuid"));

        UploadMetadata uploadMetadata = upload.getUpload();
        assertEquals(expectedUploadId, uploadMetadata.getUploadId());
        assertEquals(expectedHealthCode, uploadMetadata.getHealthCode());

        // Adherence is only present for study APIs.
        if (verifyAdherence) {
            List<AdherenceRecord> adherenceRecordList = upload.getAdherenceRecordsForSchedule();
            assertTrue(adherenceRecordList.size() > 0);
            AdherenceRecord adherenceRecord = adherenceRecordList.get(0);
            assertEquals(expectedInstanceGuid, adherenceRecord.getInstanceGuid());
            assertEquals(expectedUserId, adherenceRecord.getUserId());

            adherenceRecordList = upload.getAdherenceRecordsForUpload();
            assertTrue(adherenceRecordList.size() > 0);
            adherenceRecord = adherenceRecordList.get(0);
            assertEquals(expectedUploadId, adherenceRecord.getUploadIds().get(0));
            assertEquals(expectedUserId, adherenceRecord.getUserId());
        }
    }
}
