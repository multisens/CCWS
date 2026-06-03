import logger from '../logger';

// Fail-fast: garante que envs obrigatorias estao setadas antes de qualquer
// inicializacao (MQTT, Redis, HTTPS server). Se faltar alguma, loga claro
// e termina com exit 1 — evita erros confusos tarde no boot (ex: Buffer.from
// estourando porque HTTPS_KEY eh undefined).
export function assertEnv(required: string[]): void {
    const missing = required.filter(k => !process.env[k] || String(process.env[k]).trim() === '');
    if (missing.length) {
        logger.error(`[boot] missing required env vars: ${missing.join(', ')}`);
        logger.error(`[boot] check docker-compose.yml or .env`);
        process.exit(1);
    }
}
