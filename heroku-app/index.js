/*
 * This is the Heroku "Governor" App.
 * It provides one endpoint '/sfdc-audit-log' that Salesforce will call.
 *
 * It validates a security token and then adds all logs from the payload
 * into a local queue.
 *
 * A separate "worker" process runs continuously, pulling one message
 * from the queue every 1.1 seconds and posting it to Slack.
 */

import express from 'express';
import fetch from 'node-fetch';

// --- Configuration ---
// These MUST be set in your Heroku Config Vars
const PORT = process.env.PORT || 3000;
const SLACK_WEBHOOK_URL = process.env.SLACK_WEBHOOK_URL;
const SALESFORCE_SECURITY_TOKEN = process.env.SALESFORCE_SECURITY_TOKEN;
const THROTTLE_DELAY_MS = 1100; // 1.1 seconds to stay under Slack's 1-msg/sec limit

// --- In-Memory Queue ---
// This will hold all the log messages waiting to be sent.
// For a production app, you'd replace this with Redis or a persistent queue.
// For this POC, an in-memory array is perfectly fine.
const messageQueue = [];

// --- Express App Setup ---
const app = express();
app.use(express.json());

// --- 1. The Salesforce Endpoint ---
// Salesforce Apex will send a POST request here.
app.post('/sfdc-audit-log', (req, res) => {
    const { token, logs } = req.body;

    // 1. Security Check
    if (!SALESFORCE_SECURITY_TOKEN) {
        console.error('CRITICAL: SALESFORCE_SECURITY_TOKEN is not set in Heroku.');
        return res.status(500).send('Server configuration error.');
    }
    if (!token || token !== SALESFORCE_SECURITY_TOKEN) {
        console.warn('Invalid token received. Rejecting request.');
        return res.status(401).send('Unauthorized: Invalid token.');
    }

    // 2. Validate Payload
    if (!logs || !Array.isArray(logs) || logs.length === 0) {
        console.info('Received valid token but no logs. Payload was empty.');
        return res.status(202).send('Accepted. No logs to process.');
    }

    // 3. Add to Queue
    // We reverse the array because Salesforce sends them DESC, and we want to post them FIFO (oldest first).
    const logsToQueue = logs.reverse();
    messageQueue.push(...logsToQueue);
    
    console.log(`Received ${logsToQueue.length} logs. New queue size: ${messageQueue.length}`);
    
    // 4. Respond to Salesforce immediately
    // Salesforce doesn't need to wait for us to post to Slack.
    // A 202 "Accepted" tells Salesforce "I got it, I'll handle it."
    res.status(202).send(`Accepted. Queued ${logsToQueue.length} new logs.`);
});

// --- 2. The Slack "Drip" Worker ---
// This function runs in the background.
async function processQueue() {
    if (messageQueue.length === 0) {
        // Queue is empty, go back to sleep.
        setTimeout(processQueue, THROTTLE_DELAY_MS);
        return;
    }

    // 1. Get the next message (oldest one)
    const log = messageQueue.shift(); // .shift() pulls from the front (FIFO)

    // 2. Post it to Slack
    try {
        await postToSlack(log);
        console.log(`Posted log to Slack: ${log.Action} by ${log.CreatedByUser}`);
    } catch (e) {
        console.error(`Failed to post log to Slack. Re-queueing. Error: ${e.message}`);
        // If it fails (e.g., Slack is down), put it back at the front of the queue.
        messageQueue.unshift(log);
    }
    
    // 3. Schedule the next run after our throttle delay
    setTimeout(processQueue, THROTTLE_DELAY_MS);
}

// --- 3. The Slack Posting Function ---
// This function formats and sends the actual message to Slack.
async function postToSlack(log) {
    if (!SLACK_WEBHOOK_URL) {
        console.error('CRITICAL: SLACK_WEBHOOK_URL is not set. Cannot post to Slack.');
        // We're dropping the message here. In a real app, you'd re-queue.
        return;
    }

    // Format the Slack message
    // This matches the format we had in the Flow's text template.
    const message = {
        text: `*Action:* ${log.Action || 'N/A'}\n` +
              `*Section:* ${log.Section || 'N/A'}\n` +
              `*Display:* ${log.Display || 'N/A'}\n` +
              `*Created By User:* ${log.CreatedByUser || 'N/A'}\n` +
              `*Created Date:* ${log.AuditCreatedDate || 'N/A'}\n` +
              `*Delegate User:* ${log.DelegateUser || 'N/A'}`
    };

    // Send the request
    const response = await fetch(SLACK_WEBHOOK_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(message)
    });

    if (!response.ok) {
        // Slack returned an error (e.g., 400, 500)
        throw new Error(`Slack API error: ${response.status} ${await response.text()}`);
    }
}

// --- 4. Start the App ---
app.listen(PORT, () => {
    console.log(`Salesforce Audit Log Service listening on port ${PORT}`);
    
    // Start the queue worker
    processQueue();
});
