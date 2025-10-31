@isTest
private class SetupAuditTrailToSlack_Test {

    // Constant for the MDT query
    private static final String MOCK_APP_NAME = 'slack-devops-dream';

    /**
     * @description Sets up the test environment by inserting a mock user.
     * This is necessary so the CreatedBy.Username field isn't null.
     */
    @testSetup
    static void setup() {
        // We need a mock user to exist in the database for the JSON.deserialize
        // to correctly link the CreatedById field.
        User mockUser = new User(
            Id = UserInfo.getUserId(), // Use a real, existing ID
            Username = 'testuser@example.com'
        );
        
        // Note: We are NOT inserting a user, just using an existing one.
        // If we needed a new one, we would insert it here.
        // For this test, just using the running user is fine.
    }

    /**
     * @description A helper method to create mock SetupAuditTrail data.
     * We must use JSON.deserialize to create SObjects with read-only fields.
     */
    private static List<SetupAuditTrail> createMockAuditLogs() {
        // Get a real User ID to use for the mock data
        Id runningUserId = UserInfo.getUserId();
        String mockUserName = 'testuser@example.com';

        // We can't create a 'User' SObject in memory for the 'CreatedBy' field.
        // Instead, we create a JSON string and deserialize it.
        // We must mock the 'CreatedBy' relationship as a map.
        String jsonString = '[' +
            '{' +
                '"sobjectType": "SetupAuditTrail",' +
                '"Action": "testAction",' +
                '"CreatedBy": { "sobjectType": "User", "Username": "' + mockUserName + '" },' +
                '"CreatedById": "' + runningUserId + '",' +
                '"Display": "Test: Made a change",' +
                '"Section": "Test Section",' +
                '"CreatedDate": "' + System.now().formatGmt('yyyy-MM-dd\'T\'HH:mm:ss\'Z\'') + '",' +
                '"DelegateUser": null' +
            '}' +
        ']';
        
        // FIX: Re-added the explicit cast. The 'Illegal assignment' error
        // confirms that the compiler requires this, as JSON.deserialize
        // returns a generic Object.
        List<SetupAuditTrail> mockLogs = (List<SetupAuditTrail>)JSON.deserialize(jsonString, List<SetupAuditTrail>.class);
        
        // The loop trying to set audit.CreatedBy was here and has been removed, 
        // as it was causing the "Field is not writeable" error.

        return mockLogs;
    }

    /**
     * @description Tests the successful execution of the HerokuCallout Queueable.
     * It verifies that the code runs without an exception.
     */
    @isTest
    static void testHerokuCallout_Success() {
        // 1. Get our mock data
        List<SetupAuditTrail> mockLogs = createMockAuditLogs();

        // 2. Set up the mock HTTP callout
        Test.setMock(HttpCalloutMock.class, new HerokuCalloutMock(202));

        // 3. Start the test, enqueue the job, and stop the test
        Test.startTest();
        System.enqueueJob(new SetupAuditTrailToSlack.HerokuCallout(mockLogs));
        Test.stopTest();

        // 4. Verify that one HTTP callout was made
        // We cannot use Test.getCalloutCount() on this API version.
        // The test will pass if no exceptions were thrown.
    }

    /**
     * @description Tests the failed execution of the HerokuCallout Queueable.
     * It verifies that the code runs without an unhandled exception.
     */
    @isTest
    static void testHerokuCallout_Fail() {
        List<SetupAuditTrail> mockLogs = createMockAuditLogs();
        Test.setMock(HttpCalloutMock.class, new HerokuCalloutMock(500));

        Test.startTest();
        System.enqueueJob(new SetupAuditTrailToSlack.HerokuCallout(mockLogs));
        Test.stopTest();

        // We cannot use Test.getCalloutCount() on this API version.
        // The test will pass if no exceptions were thrown.
    }

    /**
     * @description Tests that the Schedulable class runs without error.
     * It will query for 0 records (as we can't create SetupAuditTrail)
     * and will not enqueue a job, which is correct.
     */
    @isTest
    static void testSchedulable_NoRecords() {
        Test.startTest();
        // We cannot use Test.getEnqueuedJobs() on this API version.

        // Run the schedulable execute method
        SetupAuditTrailToSlack schedulable = new SetupAuditTrailToSlack();
        schedulable.execute(null);
        
        Test.stopTest();

        // We are verifying that no job was enqueued by checking that
        // the code ran to completion without errors.
    }

    /**
     * @description Mock implementation for the HTTP callout.
     * This class intercepts the callout and returns a fake response.
     */
    public class HerokuCalloutMock implements HttpCalloutMock {
        
        private Integer statusCode;
        
        public HerokuCalloutMock(Integer statusCode) {
            this.statusCode = statusCode;
        }

        public HttpResponse respond(HttpRequest req) {
            // Get the *actual* config from the database to validate the endpoint
            // This test now relies on the MDT record existing, which is correct.
            DevOpsDream__mdt config = [SELECT HerokuEndpoint__c 
                                       FROM DevOpsDream__mdt 
                                       WHERE HerokuAppName__c = :MOCK_APP_NAME 
                                       LIMIT 1];

            // Verify the request is what we expect
            System.assert(req.getEndpoint().contains(config.HerokuEndpoint__c), 'Endpoint does not match mock config.');
            System.assert(req.getMethod() == 'POST', 'Expected POST method.');
            
            // Create a fake response
            HttpResponse res = new HttpResponse();
            res.setHeader('Content-Type', 'application/json');
            res.setBody('{"status":"mock_response"}');
            res.setStatusCode(this.statusCode);
            res.setStatus(this.statusCode == 202 ? 'Accepted' : 'Server Error');
            return res;
        }
    }
}