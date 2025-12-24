import { google } from 'googleapis';
import fs from 'fs';
import readline from 'readline';

const SCOPES = ['https://www.googleapis.com/auth/gmail.modify'];
const TOKEN_PATH = './gmail_token.json';
const CREDENTIALS_PATH = './gmail_credentials.json';

async function authorize() {
  try {
    // Load credentials
    if (!fs.existsSync(CREDENTIALS_PATH)) {
      console.error('❌ gmail_credentials.json not found!');
      console.log('Please download OAuth 2.0 credentials from Google Cloud Console');
      process.exit(1);
    }

    const credentials = JSON.parse(fs.readFileSync(CREDENTIALS_PATH));
    const { client_secret, client_id, redirect_uris } = credentials.installed;
    const oAuth2Client = new google.auth.OAuth2(client_id, client_secret, redirect_uris[0]);

    // Check if we already have a token
    if (fs.existsSync(TOKEN_PATH)) {
      console.log('✅ Token already exists!');
      const token = JSON.parse(fs.readFileSync(TOKEN_PATH));
      oAuth2Client.setCredentials(token);
      console.log('');
      console.log('Authorization is already complete.');
      console.log('Token file: gmail_token.json');
      console.log('');
      console.log('Next step: Run ./setup_watch.sh');
      return;
    }

    // Generate auth URL
    const authUrl = oAuth2Client.generateAuthUrl({
      access_type: 'offline',
      scope: SCOPES,
    });

    console.log('');
    console.log('╔════════════════════════════════════════════════════════════╗');
    console.log('║              Gmail Authorization Required                 ║');
    console.log('╚════════════════════════════════════════════════════════════╝');
    console.log('');
    console.log('📋 Steps:');
    console.log('1. Copy the URL below');
    console.log('2. Open it in your browser');
    console.log('3. Sign in with your Gmail account');
    console.log('4. Click "Allow" to grant permissions');
    console.log('5. Copy the authorization code');
    console.log('6. Paste it below');
    console.log('');
    console.log('🔗 Authorization URL:');
    console.log('');
    console.log(authUrl);
    console.log('');
    console.log('═══════════════════════════════════════════════════════════');
    console.log('');

    const rl = readline.createInterface({
      input: process.stdin,
      output: process.stdout,
    });

    rl.question('Enter the authorization code: ', async (code) => {
      rl.close();
      
      try {
        const { tokens } = await oAuth2Client.getToken(code);
        oAuth2Client.setCredentials(tokens);

        // Save token
        fs.writeFileSync(TOKEN_PATH, JSON.stringify(tokens));
        
        console.log('');
        console.log('✅ Authorization successful!');
        console.log('Token saved to: gmail_token.json');
        console.log('');
        console.log('╔════════════════════════════════════════════════════════════╗');
        console.log('║              AUTHORIZATION COMPLETE!                       ║');
        console.log('╚════════════════════════════════════════════════════════════╝');
        console.log('');
        console.log('📋 Next Step: Setup Gmail Watch');
        console.log('================================');
        console.log('');
        console.log('Run the following command:');
        console.log('  ./setup_watch.sh');
        console.log('');
      } catch (error) {
        console.error('');
        console.error('❌ Error retrieving access token:', error.message);
        console.error('');
        console.error('Please try again or check:');
        console.error('  1. Authorization code is correct');
        console.error('  2. Code has not expired (use within 10 minutes)');
        console.error('  3. OAuth credentials are valid');
        console.error('');
        process.exit(1);
      }
    });

  } catch (error) {
    console.error('❌ Error:', error.message);
    process.exit(1);
  }
}

authorize();
