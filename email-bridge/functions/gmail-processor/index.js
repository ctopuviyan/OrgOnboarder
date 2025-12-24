const { google } = require('googleapis');
const FormData = require('form-data');
const axios = require('axios');

const NORMALIZER_URL = 'https://normalizer-bshc2hcqya-uc.a.run.app';

/**
 * Extract orgId from email subject or body
 */
function extractOrgId(subject, snippet) {
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

  return 'default-org';
}

/**
 * Determine kind from subject
 */
function extractKind(subject) {
  const lowerSubject = subject.toLowerCase();
  if (lowerSubject.includes('delta') || lowerSubject.includes('change') || lowerSubject.includes('update')) {
    return 'deltas';
  }
  return 'upserts';
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
 * Process email message
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
    throw error;
  }
}

/**
 * Process attachment
 */
async function processAttachment(gmail, messageId, attachment, orgId, kind) {
  try {
    const filename = attachment.filename;
    console.log(`  Processing attachment: ${filename}`);

    // Check file type
    const ext = require('path').extname(filename).toLowerCase();
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
    throw error;
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
 * Cloud Function entry point
 * Triggered by Pub/Sub when Gmail receives a message
 */
exports.processGmailNotification = async (message, context) => {
  try {
    // Decode Pub/Sub message
    const data = message.data
      ? JSON.parse(Buffer.from(message.data, 'base64').toString())
      : {};

    console.log('Gmail notification received:', data);

    // Load OAuth credentials and token from environment variables
    const credentials = JSON.parse(process.env.GMAIL_CREDENTIALS || '{}');
    const token = JSON.parse(process.env.GMAIL_TOKEN || '{}');

    if (!credentials.installed || !token.access_token) {
      console.error('❌ Missing Gmail credentials or token in environment variables');
      console.error('Set GMAIL_CREDENTIALS and GMAIL_TOKEN environment variables');
      return;
    }

    const { client_secret, client_id, redirect_uris } = credentials.installed;
    const oAuth2Client = new google.auth.OAuth2(client_id, client_secret, redirect_uris[0]);
    oAuth2Client.setCredentials(token);

    const gmail = google.gmail({ version: 'v1', auth: oAuth2Client });

    // Get the message ID from notification
    const { emailAddress, historyId } = data;
    
    // List recent messages (unread with attachments)
    const res = await gmail.users.messages.list({
      userId: 'me',
      q: 'is:unread has:attachment',
      maxResults: 10,
    });

    const messages = res.data.messages || [];
    console.log(`Found ${messages.length} unread emails with attachments`);

    // Process each message
    for (const msg of messages) {
      await processMessage(gmail, msg.id);
    }

    console.log('✅ Notification processed successfully');

  } catch (error) {
    console.error('Error processing Gmail notification:', error);
    throw error;
  }
};
