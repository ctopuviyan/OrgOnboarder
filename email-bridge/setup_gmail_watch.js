import { google } from 'googleapis';
import fs from 'fs';

async function setupGmailWatch() {
  try {
    // Load credentials
    if (!fs.existsSync('./gmail_credentials.json')) {
      console.error('❌ gmail_credentials.json not found!');
      console.log('Please download OAuth 2.0 credentials from Google Cloud Console');
      process.exit(1);
    }

    if (!fs.existsSync('./gmail_token.json')) {
      console.error('❌ gmail_token.json not found!');
      console.log('Please run: node gmail_processor.js first to authorize');
      process.exit(1);
    }

    const credentials = JSON.parse(fs.readFileSync('./gmail_credentials.json'));
    const { client_secret, client_id, redirect_uris } = credentials.installed;
    const oAuth2Client = new google.auth.OAuth2(client_id, client_secret, redirect_uris[0]);

    // Load token
    const token = JSON.parse(fs.readFileSync('./gmail_token.json'));
    oAuth2Client.setCredentials(token);

    const gmail = google.gmail({ version: 'v1', auth: oAuth2Client });

    console.log('Setting up Gmail watch...');

    // Set up watch
    const res = await gmail.users.watch({
      userId: 'me',
      requestBody: {
        topicName: 'projects/gogreen-d6100/topics/gmail-roster-notifications',
        labelIds: ['INBOX'],
        labelFilterAction: 'include',
      },
    });

    console.log('✅ Gmail watch set up successfully!');
    console.log('');
    console.log('Details:');
    console.log('  Expiration:', new Date(parseInt(res.data.expiration)));
    console.log('  History ID:', res.data.historyId);
    console.log('');
    console.log('⚠️  Note: Watch expires in 7 days. You need to renew it.');
    console.log('   Run this script again to renew, or set up auto-renewal.');
    console.log('');
    console.log('🎉 Your Gmail is now configured for push notifications!');
    console.log('   Send a test email to see instant processing.');

  } catch (error) {
    console.error('❌ Error setting up Gmail watch:', error.message);
    
    if (error.message.includes('Precondition check failed')) {
      console.log('');
      console.log('💡 This might mean:');
      console.log('   1. Pub/Sub topic does not exist');
      console.log('   2. Gmail does not have permission to publish to topic');
      console.log('');
      console.log('Run these commands:');
      console.log('');
      console.log('  # Create topic');
      console.log('  gcloud pubsub topics create gmail-roster-notifications --project=gogreen-d6100');
      console.log('');
      console.log('  # Grant permission');
      console.log('  gcloud pubsub topics add-iam-policy-binding gmail-roster-notifications \\');
      console.log('    --member=serviceAccount:gmail-api-push@system.gserviceaccount.com \\');
      console.log('    --role=roles/pubsub.publisher --project=gogreen-d6100');
    }
    
    process.exit(1);
  }
}

setupGmailWatch().catch(console.error);
