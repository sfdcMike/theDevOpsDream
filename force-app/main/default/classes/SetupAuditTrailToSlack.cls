//This schedulable class will get your SetupAuditTrail records over to Heroku. 
//Heroku is cool because it'll protect you from running into some limits, because let me tell you, you are about to hit them.
//Slack apps like to only post 1 time a second and ya'll are pushing some BIG changes. Heroku will help us regulate the posting
//rate to only 1 per second. You may be thinking, but that means my change of 2,000 will take over 30 min to drip into my slack channel. 
//Yeah. It will. Make a better one and let me know. I believe in you. 

public class SetupAuditTrailToSlack implements Schedulable {

    // We're going to hold some details about the Heroku app in custom metadata. We will call this MDT slack-devops-dream. Call yours something more creative. 
    private static final String MDT_APP_NAME = 'slack-devops-dream';

    public void execute(SchedulableContext sc) {
        
        //This is where we set the 15 min delay. You could change this. I'll end up scheduling this to run every 15 min. 
        //If you wanted to do this every 5 min, you'd have to set this to .addMinutes(-5) and then scheule it 12 times instead of 4 times.  
        Datetime timeThreshold = System.now().addMinutes(-15);

        // This is all the audit logs in a happy little list. 
        List<SetupAuditTrail> recentAudits = [SELECT Action, CreatedBy.Username, Display, Section, CreatedDate, DelegateUser 
            FROM SetupAuditTrail WHERE CreatedDate > :timeThreshold ORDER BY CreatedDate DESC
            ];

        //This is a check to see if there were any changes. If there are none, we can just stop. 
        if (recentAudits.isEmpty()) {
            System.debug('No new SetupAuditTrail records found. Go make some changes and then try again.');
            return;
        }
        
        // Fun fact, scheduled APEX cannot make a callout and we totally need to call out (to Heroku). So we're going to enqueue this
        System.enqueueJob(new HerokuCallout(recentAudits));
    }

    //This is our callout class, it's encapsulated. 
    public class HerokuCallout implements Queueable, Database.AllowsCallouts {

        private List<SetupAuditTrail> logsToSend;
        
        public HerokuCallout(List<SetupAuditTrail> logs) {
            this.logsToSend = logs;
        }

        public void execute(QueueableContext context) {

            DevOpsDream__mdt config;
            try {
                config = [SELECT HerokuEndpoint__c, HerokuSecurityToken__c 
                    FROM DevOpsDream__mdt WHERE HerokuAppName__c = :MDT_APP_NAME LIMIT 1
                ];
            } catch (Exception e) {
                System.debug('Failed to query DevOpsDream__mdt Custom Metadata. Error: ' + e.getMessage());
                return; 
            }

            if (String.isBlank(config.HerokuEndpoint__c) || String.isBlank(config.HerokuSecurityToken__c)) {
                System.debug('Heroku configuration in MDT is incomplete. Check HerokuEndpoint__c and HerokuSecurityToken__c.');
                return;
            }

            List<LogWrapper> wrappedLogs = new List<LogWrapper>();
            for (SetupAuditTrail audit : logsToSend) {
                wrappedLogs.add(new LogWrapper(audit));
            }

            HerokuPayload payload = new HerokuPayload();
            payload.token = config.HerokuSecurityToken__c; 
            payload.logs = wrappedLogs;
            
            String jsonBody = JSON.serialize(payload);

            HttpRequest req = new HttpRequest();
            req.setEndpoint(config.HerokuEndpoint__c);
            req.setMethod('POST');
            req.setHeader('Content-Type', 'application/json;charset=UTF-8');
            req.setBody(jsonBody);

            Http http = new Http();
            try {
                HttpResponse res = http.send(req);
                if (res.getStatusCode() == 202) {
                    System.debug('Successfully sent ' + wrappedLogs.size() + ' logs to Heroku.');
                } else {
                    System.debug('Error sending logs to Heroku. Status: ' + res.getStatus() + ' Body: ' + res.getBody());
                }
            } catch (Exception e) {
                System.debug('HTTP Callout Failed: ' + e.getMessage());
            }
        }
    }

    public class HerokuPayload {
        public String token;
        public List<LogWrapper> logs;
    }

    public class LogWrapper {
        public String Action;
        public String CreatedByUser;
        public String Display;
        public String Section;
        public Datetime Audit_Created_Date;
        public String DelegateUser; 

        public LogWrapper(SetupAuditTrail audit) {
            this.Action = audit.Action;
            this.CreatedByUser = audit.CreatedBy.Username;
            this.Display = audit.Display;
            this.Section = audit.Section;
            this.Audit_Created_Date = audit.CreatedDate;
            this.DelegateUser = audit.DelegateUser; 
        }
    }
}