import { google } from 'googleapis';
import fs from 'fs';

const TOKEN_PATH = './gmail_token.json';
const CREDENTIALS_PATH = './gmail_credentials.json';
const PROJECT_ID = 'gogreen-d6100';
const TOPIC_NAME = 'gmail-roster-notifications';

async function setupGmailWatch() {
  try {
    console.log('');
    console.log('╔════════════════════════════════════════════════════════════╗');
    console.log('║         Setting Up Gmail Watch (Push Notifications)       ║');
    console.log('╚════════════════════════════════════════════════════════════╝');
    console.log('');

    // Load credentials
    if (!fs.existsSync(CREDENTIALS_PATH)) {
      console.error('❌ gmail_credentials.json not found!');
      process.exit(1);
    }

    if (!fs.existsSync(TOKEN_PATH)) {
      console.error('❌ gmail_token.json not found!');
      console.log('Please run: node authorize.js first');
      process.exit(1);
    }

    console.log('✅ Credentials found');
    console.log('✅ Token found');
    console.log('');

    const credentials = JSON.parse(fs.readFileSync(CREDENTIALS_PATH));
    const { client_secret, client_id, redirect_uris } = credentials.installed;
    const oAuth2Client = new google.auth.OAuth2(client_id, client_secret, redirect_uris[0]);

    // Load token
    const token = JSON.parse(fs.readFileSync(TOKEN_PATH));
    oAuth2Client.setCredentials(token);

    const gmail = google.gmail({ version: 'v1', auth: oAuth2Client });

    console.log('📋 Setting up Gmail watch...');
    console.log('');

    // Set up watch
    const res = await gmail.users.watch({
      userId: 'me',
      requestBody: {
        topicName: `projects/${PROJECT_ID}/topics/${TOPIC_NAME}`,
        labelIds: ['INBOX'],
        labelFilterAction: 'include',
      },
    });

    console.log('✅ Gmail watch set up successfully!');
    console.log('');
    console.log('📊 Watch Details:');
    console.log('═══════════════════════════════════════════════════════════');
    console.log(`  Topic: projects/${PROJECT_ID}/topics/${TOPIC_NAME}`);
    console.log(`  History ID: ${res.data.historyId}`);
    console.log(`  Expiration: ${new Date(parseInt(res.data.expiration))}`);
    console.log('');
    console.log('⚠️  Important: Watch expires in 7 days');
    console.log('   Renew by running: node setup_watch_v2.js');
    console.log('');
    console.log('╔════════════════════════════════════════════════════════════╗');
    console.log('║              GMAIL WATCH SETUP COMPLETE!                  ║');
    console.log('╚════════════════════════════════════════════════════════════╝');
    console.log('');
    console.log('🎉 Your Gmail is now configured for push notifications!');
    console.log('');
    console.log('📋 Next Step: Add HMAC Secrets');
    console.log('================================');
    console.log('');
    console.log('Run the following command:');
    console.log('  ./add_hmac_secret.sh test-org');
    console.log('');

  } catch (error) {
    console.error('');
    console.error('❌ Error setting up Gmail watch:', error.message);
    console.error('');
    
    if (error.message.includes('Precondition check failed')) {
      console.log('💡 This might mean:');
      console.log('   1. Pub/Sub topic does not exist');
      console.log('   2. Gmail does not have permission to publish to topic');
      console.log('');
      console.log('Verify with:');
      console.log('  gcloud pubsub topics describe gmail-roster-notifications --project=gogreen-d6100');
      console.log('');
    } else if (error.message.includes('invalid_grant')) {
      console.log('💡 Token expired or invalid');
      console.log('   Run: node authorize.js');
      console.log('');
    }
    
    process.exit(1);
  }
}

setupGmailWatch();
