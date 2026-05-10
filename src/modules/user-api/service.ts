import * as dotenv from 'dotenv';
import fs from 'fs';
import mqttClient, { TOPICS } from '../../mqtt-client';
import path from 'path';
import redis from '../../redis-client';
import { Expression, UserAttributes, UsersIdList } from './types';
dotenv.config();


// --- session state (Redis is the single source of truth) ---
// currentUser e currentService NAO sao mantidos em memoria. Toda leitura
// vai pro Redis em runtime, garantindo consistencia mesmo se a state for
// alterada externamente (outro CCWS, escrita direta, MQTT perdido etc).

const KEY_CURRENT_USER    = 'session:current-user';
const KEY_CURRENT_SERVICE = 'session:current-service-id';

async function readCurrentUser(): Promise<string> {
    return (await redis.get(KEY_CURRENT_USER)) ?? '';
}

async function readCurrentService(): Promise<string> {
    return (await redis.get(KEY_CURRENT_SERVICE)) ?? '';
}


// --- MQTT handlers ---

async function updateCurrentUser(m: string): Promise<void> {
    await redis.set(KEY_CURRENT_USER, m);
}

async function updateCurrentService(m: string): Promise<void> {
    await redis.set(KEY_CURRENT_SERVICE, m);
}

async function syncUsersFromFile(p: string): Promise<void> {
    if (!p || !fs.existsSync(p)) return;

    // AoP publica o diretorio em aop/users (USER_DATA_PATH); o initFromRedis passa o arquivo (USER_DATA_FILE).
    // Aceita ambos: se for diretorio, resolve para userData.json dentro dele.
    const filePath = fs.statSync(p).isDirectory() ? path.join(p, 'userData.json') : p;
    if (!fs.existsSync(filePath)) return;

    const raw = JSON.parse(fs.readFileSync(filePath, 'utf-8'));
    const users: any[] = raw.users ?? raw;

    const pipeline = redis.pipeline();

    for (const user of users) {
        const id = user.id;
        if (!id) continue;

        pipeline.sadd('users:index', id);

        const hashFields: Record<string, string> = {};
        for (const [key, val] of Object.entries(user)) {
            if (key === 'accessConsent' || key === 'consent') continue;
            if (val !== null && val !== undefined) {
                hashFields[key] = String(val);
            }
        }
        if (Object.keys(hashFields).length > 0) {
            pipeline.hset(`user:${id}`, hashFields);
        }

        // Merge (SADD sem DEL): consents concedidos fora do JSON sobrevivem aos syncs.
        // Consent eh incremental por natureza — nao deve ser sobrescrito a cada reload.
        const consent: string[] = (user.accessConsent ?? user.consent ?? []);
        if (consent.length > 0) {
            pipeline.sadd(`user:${id}:consent`, ...consent);
        }
    }

    await pipeline.exec();
    console.log(`[Redis] Synced ${users.length} users from ${filePath}`);
}

mqttClient.addTopicHandler(TOPICS.current_user, updateCurrentUser);
mqttClient.addTopicHandler(TOPICS.current_service, updateCurrentService);
mqttClient.addTopicHandler(TOPICS.users, syncUsersFromFile);


// --- startup: restore session and seed users from Redis ---

async function initFromRedis(): Promise<void> {
    // Sem cache em memoria — currentUser/currentService ficam apenas no Redis.
    // Apenas seed do users:index a partir do arquivo se Redis estiver vazio.
    const userCount = await redis.scard('users:index');
    if (userCount === 0 && process.env.USER_DATA_FILE) {
        await syncUsersFromFile(process.env.USER_DATA_FILE);
    }
}

initFromRedis().catch((err) => console.error('[Redis] Init failed:', err));


// --- Expression evaluator ---

function compareValues(userVal: string, op: string, filterVal: string): boolean {
    const a = isNaN(Number(userVal)) ? userVal : Number(userVal);
    const b = isNaN(Number(filterVal)) ? filterVal : Number(filterVal);
    switch (op) {
        case 'eq':  return a === b;
        case 'neq': return a !== b;
        case 'lt':  return a < b;
        case 'lte': return a <= b;
        case 'gt':  return a > b;
        case 'gte': return a >= b;
        default:    return false;
    }
}

function matchExpression(fields: Record<string, string>, expr: Expression): boolean {
    if ('attribute' in expr) {
        return compareValues(fields[expr.attribute] ?? '', expr.comparator, expr.value);
    } else if ('and' in expr) {
        return expr.and.every(e => matchExpression(fields, e));
    } else {
        return (expr as any).or.some((e: Expression) => matchExpression(fields, e));
    }
}


// --- API methods ---

async function getCurrentUser(): Promise<string> {
    return await readCurrentUser();
}

async function setCurrentUser(uuid: string): Promise<void> {
    await redis.set(KEY_CURRENT_USER, uuid);
    mqttClient.publish(TOPICS.current_user, uuid, true);
}

async function getUserList(body: Expression): Promise<UsersIdList> {
    const currentService = await readCurrentService();
    const userIds = await redis.smembers('users:index');

    // Sem service ativo (boot do AoP / profile-chooser): retorna todos os users.
    // Spec C.6.14.1 cobre o caso "com service ativo" — fora desse caso, lista
    // tudo pra permitir escolha de perfil antes de qualquer service ser selecionado.
    if (!currentService) {
        return { users: userIds.map(id => ({ id })) };
    }

    const consentPipeline = redis.pipeline();
    userIds.forEach(id => consentPipeline.smembers(`user:${id}:consent`));
    const consentResults = await consentPipeline.exec() as Array<[Error | null, string[]]>;

    const eligibleIds = userIds.filter((_, i) => {
        const consent = consentResults[i]?.[1] ?? [];
        return consent.includes(currentService);
    });

    if (!body || eligibleIds.length === 0) {
        return { users: eligibleIds.map(id => ({ id })) };
    }

    const fieldsPipeline = redis.pipeline();
    eligibleIds.forEach(id => fieldsPipeline.hgetall(`user:${id}`));
    const fieldsResults = await fieldsPipeline.exec() as Array<[Error | null, Record<string, string>]>;

    const matched = eligibleIds.filter((_, i) => {
        const fields = fieldsResults[i]?.[1] ?? {};
        return matchExpression(fields, body);
    });

    return { users: matched.map(id => ({ id })) };
}

async function getUserAttribute(uuid: string, atname?: string): Promise<UserAttributes> {
    const currentService = await readCurrentService();

    // Sem service ativo: profile-chooser pode pedir detalhes pra escolher perfil.
    // Com service ativo: aplica consent check (C.6.14.2).
    if (currentService) {
        const hasConsent = await redis.sismember(`user:${uuid}:consent`, currentService);
        if (!hasConsent) return null as any;
    }

    if (atname) {
        return await redis.hget(`user:${uuid}`, atname) as any;
    }
    return await redis.hgetall(`user:${uuid}`) as any;
}

async function checkConsent(avatarPath: string): Promise<boolean> {
    const currentService = await readCurrentService();
    const userIds = await redis.smembers('users:index');

    const pipeline = redis.pipeline();
    userIds.forEach(id => {
        pipeline.hget(`user:${id}`, 'avatar');
        pipeline.sismember(`user:${id}:consent`, currentService);
    });
    const results = await pipeline.exec() as Array<[Error | null, string | number]>;

    for (let i = 0; i < userIds.length; i++) {
        const avatar = results[i * 2]?.[1] as string;
        const hasConsent = results[i * 2 + 1]?.[1];
        if (avatar === avatarPath && hasConsent) {
            return true;
        }
    }
    return false;
}


// --- Broadcaster attributes ---

async function getBroadcasterAttrs(userId: string, serviceContextId: string): Promise<Record<string, string>> {
    return await redis.hgetall(`user:${userId}:broadcaster-attrs:${serviceContextId}`) ?? {};
}

async function setBroadcasterAttrs(userId: string, serviceContextId: string, attrs: Record<string, string>): Promise<void> {
    if (Object.keys(attrs).length === 0) return;
    const sanitized: Record<string, string> = {};
    for (const [k, v] of Object.entries(attrs)) {
        sanitized[k] = String(v);
    }
    await redis.hset(`user:${userId}:broadcaster-attrs:${serviceContextId}`, sanitized);
}


// --- File serving ---

type FileData = { size: number; mime: string; name: string; file: string };

function getFile(fpath: string): FileData {
    const file_path = path.join(process.env.USER_THUMBS as string, fpath);
    const file_name = path.parse(file_path).base;
    const stat = fs.statSync(file_path);
    const file = fs.readFileSync(file_path, 'binary');
    return {
        size: stat.size,
        mime: getMime(path.extname(file_name)),
        name: file_name,
        file,
    };
}

function getMime(ext: string): string {
    if (ext === '.jpeg' || ext === '.jpg') return 'image/jpeg';
    if (ext === '.png') return 'image/png';
    return 'application/octet-stream';
}


export default { getCurrentUser, setCurrentUser, getUserList, getUserAttribute, checkConsent, getFile, getBroadcasterAttrs, setBroadcasterAttrs };
