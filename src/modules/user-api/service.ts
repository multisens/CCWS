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

// Retorna o service ativo APENAS se for "conhecido" — i.e. pelo menos um user
// tem consent pra ele. Um service obsoleto/retido (ex: aop/currentService preso
// no broker apos um restart, cujo SID ninguem consentiu) eh tratado como "sem
// service ativo". Sem esse guard o filtro de consent esconde TODOS os perfis e
// o profile-chooser fica vazio, travando o workflow do AoP.
async function resolveActiveService(): Promise<string> {
    const currentService = await readCurrentService();
    if (!currentService) return '';

    const userIds = await redis.smembers('users:index');
    if (userIds.length === 0) return currentService;

    const pipeline = redis.pipeline();
    userIds.forEach(id => pipeline.sismember(`user:${id}:consent`, currentService));
    const results = await pipeline.exec() as Array<[Error | null, number]>;

    const known = results.some(r => r?.[1] === 1);
    return known ? currentService : '';
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
    const currentService = await resolveActiveService();
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
    const currentService = await resolveActiveService();

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


// --- Create user (ABNT NBR 25608) ---

const VALID_RATINGS = ['L', '10', '12', '14', '16', '18'];
const VALID_SIDES   = ['left', 'right'];

const DEFAULT_AVATAR_SVG =
    '<svg viewBox="0 0 100 100" preserveAspectRatio="xMidYMid meet" xmlns="http://www.w3.org/2000/svg">' +
        '<rect width="100" height="100" rx="30" fill="#4d7c5b"/>' +
        '<circle cx="50" cy="40" r="14" fill="none" stroke="#ffffff" stroke-width="5"/>' +
        '<path d="M 14 90 a 36 36 0 0 1 72 0" fill="none" stroke="#ffffff" stroke-width="5" stroke-linecap="round"/>' +
    '</svg>';

async function createUser(userData: any): Promise<any> {
    // ABNT NBR 25608 — Tabela 7 (atributos basicos do perfil do telespectador)
    const nickname = String(userData.nickname || userData.name || '').trim();
    if (!nickname) throw new Error('nickname é obrigatório');
    if (nickname.length > 20) throw new Error('nickname deve ter no máximo 20 caracteres');

    const parentalControl = !!userData.parentalControl;
    if (parentalControl && !userData.maxContentRating) {
        throw new Error('maxContentRating é obrigatório quando parentalControl = true');
    }
    if (userData.maxContentRating && !VALID_RATINGS.includes(userData.maxContentRating)) {
        throw new Error(`maxContentRating deve ser um de: ${VALID_RATINGS.join(', ')}`);
    }

    const closedSigning = !!userData.closedSigning;
    if (closedSigning) {
        if (userData.closedSigningWidth !== undefined) {
            const w = parseInt(userData.closedSigningWidth);
            if (isNaN(w) || w < 14 || w > 28) throw new Error('closedSigningWidth deve estar entre 14 e 28');
        }
        if (userData.closedSigningSide && !VALID_SIDES.includes(userData.closedSigningSide)) {
            throw new Error('closedSigningSide deve ser "left" ou "right"');
        }
    }

    const userId = `user_${Date.now()}`;
    const closedCaptioning = !!userData.closedCaptioning;

    // Quem cria perfil pelo profile-chooser concorda com o termo LGPD no form.
    // Esse aceite vira consent para o serviço ativo, garantindo que o novo perfil
    // apareça na proxima listagem (que filtra por consent do current-service).
    const currentService = await readCurrentService();
    const accessConsent: string[] = Array.isArray(userData.accessConsent) ? userData.accessConsent.slice() : [];
    if (currentService && !accessConsent.includes(currentService)) {
        accessConsent.push(currentService);
    }

    const newUser: any = {
        id:                       userId,
        nickname,
        avatar:                   userData.avatar || DEFAULT_AVATAR_SVG,
        parentalControl,
        maxContentRating:         parentalControl ? userData.maxContentRating : null,
        audioLanguage:            userData.audioLanguage || 'pt-BR',
        closedCaptioningLanguage: closedCaptioning ? (userData.closedCaptioningLanguage || 'pt-BR') : null,
        userInterfaceLanguage:    userData.userInterfaceLanguage || 'pt-BR',
        closedCaptioning,
        closedSigning,
        closedSigningSide:        closedSigning ? (userData.closedSigningSide || 'right') : null,
        closedSigningWidth:       closedSigning ? (userData.closedSigningWidth ? parseInt(userData.closedSigningWidth) : 28) : null,
        audioDescription:         !!userData.audioDescription,
        dialogEnhancement:        !!userData.dialogEnhancement,
        voiceGuidance:            !!userData.voiceGuidance,
        accessConsent
    };

    // Persiste no Redis (mesma estrutura do syncUsersFromFile)
    const hashFields: Record<string, string> = {};
    for (const [k, v] of Object.entries(newUser)) {
        if (k === 'accessConsent' || k === 'consent') continue;
        if (v !== null && v !== undefined) hashFields[k] = String(v);
    }
    await redis.sadd('users:index', userId);
    await redis.hset(`user:${userId}`, hashFields);
    if (newUser.accessConsent.length > 0) {
        await redis.sadd(`user:${userId}:consent`, ...newUser.accessConsent);
    }

    // Persiste no userData.json (mantem seed sincronizado pra rebuild da stack)
    const userDataFile = process.env.USER_DATA_FILE;
    if (userDataFile && fs.existsSync(userDataFile)) {
        try {
            const current = JSON.parse(fs.readFileSync(userDataFile, 'utf-8'));
            current.users = current.users || [];
            current.users.push(newUser);
            fs.writeFileSync(userDataFile, JSON.stringify(current, null, '\t'));
        } catch (err) {
            console.error(`[createUser] Falha persistindo em ${userDataFile}: ${(err as Error).message}`);
        }
    }

    // Notifica AoP via MQTT pra recarregar DATA.users
    mqttClient.publish('aop/users', process.env.USER_DATA_PATH || '/user-files', false);

    return newUser;
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


export default { getCurrentUser, setCurrentUser, getUserList, getUserAttribute, checkConsent, getFile, getBroadcasterAttrs, setBroadcasterAttrs, createUser };
