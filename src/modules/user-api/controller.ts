import { Request, Response } from 'express';
import service from './service';
import { UserId } from './types';


async function GETCurrentUser(req: Request, res: Response): Promise<void> {
	const id = await service.getCurrentUser();
	console.log(`pediu usuário e recebeu ${id}`);
    res.status(200).json({ id });
}

async function POSTCurrentUser(req: Request, res: Response): Promise<void> {
    const body: UserId = req.body;
    if (!body) {
        res.status(400).json({
			error : 106,
			description : 'API unavailable for this runtime environment'
		});
		return;
    }
    await service.setCurrentUser(body.id);
    res.sendStatus(200);
}

function POSTUserList(req: Request, res: Response): void {
	service.getUserList(req.body)
	.then((response) => { res.status(200).json(response) })
}

function GETUserAttribute(req: Request, res: Response): void {
	const uuid = req.params.userid;
	if (!uuid) {
		res.status(400).json({
			error : 305,
			description : 'No user defined'
		});
		return;
	}
	
	if (Object.keys(req.query).length > 0) {
		const atname = req.query.attribute as string;
		service.getUserAttribute(uuid, atname)
		.then((response) => { res.status(200).json(response) })
	}
	else {
		service.getUserAttribute(uuid)
		.then((response) => { res.status(200).json(response) })
	}
}

function GETUserFile(req: Request, res: Response): void {
    if (Object.keys(req.query).length == 0) {
		res.status(400).json({
            error : 105,
			description : 'Missing argument'
        });
		return;
	}

	const path = req.query.path as string;
	service.checkConsent(path)
	.then((result) => {
		if (!result) {
			res.status(400).json({
                error : 305,
                description : 'DTV resource not found'
            });
		}
		else {
			const file_data = service.getFile(path);
		
			res.setHeader('Content-Length', file_data.size);
			res.setHeader('Content-Type', file_data.mime);
			res.setHeader('Content-Disposition', `attachment; filename=${file_data.name}`);
			res.write(file_data.file, 'binary');
			res.end();
		}
	})
}


async function GETBroadcasterAttrs(req: Request, res: Response): Promise<void> {
    const { userid, serviceContextId } = req.params;
    if (!userid) {
        res.status(400).json({ error: 305, description: 'No user defined' });
        return;
    }
    const attrs = await service.getBroadcasterAttrs(userid, serviceContextId ?? 'current-service');
    res.status(200).json(attrs);
}

async function PUTBroadcasterAttrs(req: Request, res: Response): Promise<void> {
    const { userid, serviceContextId } = req.params;
    if (!userid) {
        res.status(400).json({ error: 305, description: 'No user defined' });
        return;
    }
    if (!req.body || typeof req.body !== 'object' || Array.isArray(req.body)) {
        res.status(400).json({ error: 106, description: 'Request body must be a JSON object' });
        return;
    }
    await service.setBroadcasterAttrs(userid, serviceContextId ?? 'current-service', req.body);
    res.sendStatus(200);
}


export default { GETCurrentUser, POSTCurrentUser, POSTUserList, GETUserAttribute, GETUserFile, GETBroadcasterAttrs, PUTBroadcasterAttrs }