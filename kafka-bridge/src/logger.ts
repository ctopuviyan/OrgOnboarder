/**
 * Minimal structured logger with context support
 */

type LogLevel = 'info' | 'warn' | 'error';

interface LogContext {
  component?: string;
  orgId?: string;
  eventId?: string;
  [key: string]: unknown;
}

class Logger {
  private formatMessage(level: LogLevel, message: string, context?: LogContext): string {
    const timestamp = new Date().toISOString();
    const contextStr = context ? ` ${JSON.stringify(context)}` : '';
    return `[${timestamp}] ${level.toUpperCase()}: ${message}${contextStr}`;
  }

  info(message: string, context?: LogContext): void {
    console.log(this.formatMessage('info', message, context));
  }

  warn(message: string, context?: LogContext): void {
    console.warn(this.formatMessage('warn', message, context));
  }

  error(message: string, context?: LogContext): void {
    console.error(this.formatMessage('error', message, context));
  }

  // Helper to create a logger with default context
  withContext(defaultContext: LogContext): Logger {
    const logger = new Logger();
    const originalInfo = logger.info.bind(logger);
    const originalWarn = logger.warn.bind(logger);
    const originalError = logger.error.bind(logger);

    logger.info = (msg: string, ctx?: LogContext) => 
      originalInfo(msg, { ...defaultContext, ...ctx });
    logger.warn = (msg: string, ctx?: LogContext) => 
      originalWarn(msg, { ...defaultContext, ...ctx });
    logger.error = (msg: string, ctx?: LogContext) => 
      originalError(msg, { ...defaultContext, ...ctx });

    return logger;
  }
}

export const logger = new Logger();
