const { google } = require('googleapis');
const fs = require('fs');
const path = require('path');
const FormData = require('form-data');
const axios = require('axios');

// Configuration
const GMAIL_CREDENTIALS_PATH = './gmail_credentials.json';
const TOKEN_PATH = './gmail_token.json';
const NORMALIZER_URL = 'https://normalizer-bshc2hcqya-uc.a.run.app';
const CHECK_INTERVAL = 5 * 60 * 1000; // 5 minutes

// Gmail API setup
const SCOPES = ['https://www.googleapis.com/auth/gmail.modify'];

/**
 * Get OAuth2 client
 */
async function getGmailClient() {
  const credentials = JSON.parse(fs.readFileSync(GMAIL_CREDENTIALS_PATH));
  const { client_secret, client_id, redirect_uris } = credentials.installed;
  const oAuth2Client = new google.auth.OAuth2(client_id, client_secret, redirect_uris[0]);

  // Check if we have a token
  if (fs.existsSync(TOKEN_PATH)) {
    const token = JSON.parse(fs.readFileSync(TOKEN_PATH));
    oAuth2Client.setCredentials(token);
  } else {
    // Need to authorize
    await authorize(oAuth2Client);
  }

  return oAuth2Client;
}

/**
 * Authorize and save token
 */
async function authorize(oAuth2Client) {
  const authUrl = oAuth2Client.generateAuthUrl({
    access_type: 'offline',
    scope: SCOPES,
  });

  console.log('Authorize this app by visiting this url:', authUrl);
  console.log('Enter the code from that page here:');

  const readline = require('readline');
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  });

  return new Promise((resolve) => {
    rl.question('Code: ', async (code) => {
      rl.close();
      const { tokens } = await oAuth2Client.getToken(code);
      oAuth2Client.setCredentials(tokens);
      fs.writeFileSync(TOKEN_PATH, JSON.stringify(tokens));
      console.log('Token stored to', TOKEN_PATH);
      resolve();
    });
  });
}

/**
 * Extract orgId from email subject or body
 */
function extractOrgId(subject, snippet) {
  // Look for patterns like:
  // Subject: "Roster for acme-corp"
  // Subject: "Employee data - company-123"
  // Or in body: "Organization: acme-corp"
  
  const patterns = [
    /roster\s+for\s+([a-z0-9-]+)/i,
    /org(?:anization)?:\s*([a-z0-9-]+)/i,
    /company:\s*([a-z0-9-]+)/i,
    /\[([a-z0-9-]+)\]/,
  ];

  const text = `${subject} ${snippet}`;
  
  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (match) return match[1];
  }

  // Default to sender domain
  return 'default-org';
}

/**
 * Determine kind (upserts or deltas) from subject
 */
function extractKind(subject) {
  const lowerSubject = subject.toLowerCase();
  if (lowerSubject.includes('delta') || lowerSubject.includes('change') || lowerSubject.includes('update')) {
    return 'deltas';
  }
  return 'upserts';
}

/**
 * Process unread emails with attachments
 */
async function processEmails() {
  try {
    const auth = await getGmailClient();
    const gmail = google.gmail({ version: 'v1', auth });

    // Search for unread emails with attachments
    const res = await gmail.users.messages.list({
      userId: 'me',
      q: 'is:unread has:attachment',
    });

    const messages = res.data.messages || [];
    console.log(`Found ${messages.length} unread emails with attachments`);

    for (const message of messages) {
      await processMessage(gmail, message.id);
    }
  } catch (error) {
    console.error('Error processing emails:', error);
  }
}

/**
 * Process a single email message
 */
async function processMessage(gmail, messageId) {
  try {
    // Get full message
    const msg = await gmail.users.messages.get({
      userId: 'me',
      id: messageId,
    });

    const headers = msg.data.payload.headers;
    const subject = headers.find(h => h.name === 'Subject')?.value || '';
    const from = headers.find(h => h.name === 'From')?.value || '';
    const snippet = msg.data.snippet || '';

    console.log(`Processing email: ${subject} from ${from}`);

    // Extract orgId and kind
    const orgId = extractOrgId(subject, snippet);
    const kind = extractKind(subject);

    console.log(`  OrgId: ${orgId}, Kind: ${kind}`);

    // Find attachments
    const parts = getAllParts(msg.data.payload);
    const attachments = parts.filter(part => part.filename && part.body.attachmentId);

    if (attachments.length === 0) {
      console.log('  No attachments found, skipping');
      await markAsRead(gmail, messageId);
      return;
    }

    // Process each attachment
    for (const attachment of attachments) {
      await processAttachment(gmail, messageId, attachment, orgId, kind);
    }

    // Mark as read
    await markAsRead(gmail, messageId);
    console.log(`  ✅ Email processed successfully`);

  } catch (error) {
    console.error(`Error processing message ${messageId}:`, error);
  }
}

/**
 * Get all parts from message payload (recursive)
 */
function getAllParts(payload) {
  let parts = [];
  
  if (payload.parts) {
    for (const part of payload.parts) {
      parts.push(part);
      if (part.parts) {
        parts = parts.concat(getAllParts(part));
      }
    }
  } else {
    parts.push(payload);
  }
  
  return parts;
}

/**
 * Process attachment
 */
async function processAttachment(gmail, messageId, attachment, orgId, kind) {
  try {
    const filename = attachment.filename;
    console.log(`  Processing attachment: ${filename}`);

    // Check file type
    const ext = path.extname(filename).toLowerCase();
    if (!['.csv', '.xlsx', '.xls', '.json'].includes(ext)) {
      console.log(`  Skipping unsupported file type: ${ext}`);
      return;
    }

    // Download attachment
    const attachmentData = await gmail.users.messages.attachments.get({
      userId: 'me',
      messageId: messageId,
      id: attachment.body.attachmentId,
    });

    // Decode base64
    const buffer = Buffer.from(attachmentData.data.data, 'base64');

    // Send to normalizer
    const form = new FormData();
    form.append('orgId', orgId);
    form.append('kind', kind);
    form.append('file', buffer, { filename });

    const response = await axios.post(`${NORMALIZER_URL}/ingest/email`, form, {
      headers: form.getHeaders(),
    });

    console.log(`  ✅ Sent to normalizer:`, response.data);

  } catch (error) {
    console.error(`  Error processing attachment:`, error.message);
  }
}

/**
 * Mark message as read
 */
async function markAsRead(gmail, messageId) {
  await gmail.users.messages.modify({
    userId: 'me',
    id: messageId,
    requestBody: {
      removeLabelIds: ['UNREAD'],
    },
  });
}

/**
 * Main loop
 */
async function main() {
  console.log('🚀 Gmail Roster Processor Started');
  console.log(`Checking inbox every ${CHECK_INTERVAL / 1000 / 60} minutes`);
  console.log('');

  // Initial check
  await processEmails();

  // Set up interval
  setInterval(async () => {
    console.log(`\n[${new Date().toISOString()}] Checking for new emails...`);
    await processEmails();
  }, CHECK_INTERVAL);
}

// Start the processor
main().catch(console.error);
