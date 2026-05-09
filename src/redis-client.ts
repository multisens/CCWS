import Redis from 'ioredis';
import * as dotenv from 'dotenv';
dotenv.config();

const redis = new Redis({
    host: process.env.REDIS_HOST || 'localhost',
    port: parseInt(process.env.REDIS_PORT || '6379'),
    lazyConnect: true,
});

redis.on('connect', () => console.log('[Redis] Connected'));
redis.on('error', (err: Error) => console.error('[Redis] Error:', err.message));

export default redis;
