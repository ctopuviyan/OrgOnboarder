/**
 * HTTP client with keep-alive, retry, and exponential backoff
 */

import { request } from 'undici';
import { config } from './env';
import { logger } from './logger';
import type { HttpResponse } from './types';

const keepAliveAgent = {
  keepAlive: true,
  keepAliveMsecs: 30000,
  maxSockets: 10,
  maxFreeSockets: 5,
};

function isRetryable(status: number): boolean {
  // Retry on 5xx, 429 (rate limit), and network errors
  // 409 (duplicate/conflict) is NOT retryable - treat as success for idempotency
  return status >= 500 || status === 429;
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function calculateBackoff(attempt: number): number {
  const backoff = Math.min(
    config.retryBaseMs * Math.pow(2, attempt),
    config.retryMaxMs
  );
  // Add jitter (±20%)
  const jitter = backoff * 0.2 * (Math.random() - 0.5);
  return Math.floor(backoff + jitter);
}

/**
 * POST JSON to Normalizer with retry logic
 * @param path - API path (e.g., '/ingest/kafka/upserts')
 * @param queryParams - Query parameters as object
 * @param body - Request body (will be JSON stringified)
 * @returns HttpResponse with status and text
 */
export async function postJson(
  path: string,
  queryParams: Record<string, string>,
  body: unknown
): Promise<HttpResponse> {
  const queryString = new URLSearchParams(queryParams).toString();
  const url = `${config.normalizerBaseUrl}${path}?${queryString}`;
  const bodyStr = JSON.stringify(body);
  const bodySize = Buffer.byteLength(bodyStr, 'utf8');

  let lastError: Error | null = null;

  for (let attempt = 0; attempt <= config.maxRetries; attempt++) {
    try {
      const response = await request(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Auth': config.ingestionToken,
        },
        body: bodyStr,
        bodyTimeout: config.httpTimeoutMs,
        headersTimeout: config.httpTimeoutMs,
        ...keepAliveAgent,
      });

      const text = await response.body.text();
      const status = response.statusCode;

      // Treat 409 (duplicate) as success for idempotency
      if (status === 409) {
        logger.info('Duplicate event (409), treating as success', {
          path,
          ...queryParams,
          attempt,
        });
        return { status: 200, text }; // Return as success
      }

      // Success
      if (status >= 200 && status < 300) {
        if (attempt > 0) {
          logger.info('Request succeeded after retries', {
            path,
            ...queryParams,
            attempt,
            status,
          });
        }
        return { status, text };
      }

      // Check if retryable
      if (!isRetryable(status)) {
        logger.error('Non-retryable error', {
          path,
          ...queryParams,
          status,
          bodySize,
          responsePreview: text.substring(0, 200),
        });
        return { status, text };
      }

      // Retryable error
      if (attempt < config.maxRetries) {
        const backoffMs = calculateBackoff(attempt);
        logger.warn('Retryable error, backing off', {
          path,
          ...queryParams,
          status,
          attempt,
          nextRetryMs: backoffMs,
        });
        await sleep(backoffMs);
        continue;
      }

      // Max retries exceeded
      logger.error('Max retries exceeded', {
        path,
        ...queryParams,
        status,
        attempts: attempt + 1,
        bodySize,
      });
      return { status, text };

    } catch (error) {
      lastError = error as Error;
      
      // Network/timeout errors are retryable
      if (attempt < config.maxRetries) {
        const backoffMs = calculateBackoff(attempt);
        logger.warn('Network error, backing off', {
          path,
          ...queryParams,
          error: lastError.message,
          attempt,
          nextRetryMs: backoffMs,
        });
        await sleep(backoffMs);
        continue;
      }

      // Max retries exceeded
      logger.error('Network error, max retries exceeded', {
        path,
        ...queryParams,
        error: lastError.message,
        attempts: attempt + 1,
        bodySize,
      });
      throw lastError;
    }
  }

  // Should never reach here, but TypeScript needs it
  throw lastError || new Error('Unexpected retry loop exit');
}
