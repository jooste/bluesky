import asyncio
import threading
import msgpack
import uvicorn
from numpy import ndarray
from typing import Annotated, Self
from fastapi import Body, FastAPI, Path, Request, WebSocket, WebSocketDisconnect, Depends, Query, HTTPException, status
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.encoders import jsonable_encoder

from starlette.middleware.sessions import SessionMiddleware

from contextlib import asynccontextmanager

from bluesky.network.common import genid, unpack_zmq_msgid, ws_msgid, zmq_msgid
from bluesky.network.npcodec import encode_json
from bluesky.network.server_async import Server
import bluesky as bs

bs.settings.set_variable_defaults(http_host='127.0.0.1', http_port=8081)


@asynccontextmanager
async def lifespan(app: FastAPI):
    server: WebServer = WebServer.instance()
    asyncio.create_task(server.loop())
    yield
    # We only have to call quit here if the server is running in the main thread
    if server.thread is None:
        server.quit()

async def get_client_id(session: dict, client_id: str=''):
    ''' Get Client ID from query/path or session. '''
    cookie_id = session.get('client_id', 'C')
    ret = genid(client_id or cookie_id)
    if cookie_id != ret:
        session.update(dict(client_id=ret))
    return ret


async def ensure_client_id(request: Request, client_id: str=''):
    ''' Dependency to ensure that the actual client id ends up as query in the url. '''
    s_client_id = await get_client_id(request.session, client_id)
    if s_client_id != client_id:
        url = request.url.include_query_params(client_id=s_client_id)
        raise HTTPException(
            status_code=status.HTTP_307_TEMPORARY_REDIRECT,
            headers={'Location': str(url)})
    return s_client_id

EnsureClientID = Annotated[str, Depends(ensure_client_id)]


app = FastAPI(lifespan=lifespan)
app.add_middleware(
    SessionMiddleware,
    same_site="strict",
    session_cookie='MY_SESSION_ID',
    secret_key="mysecret",
)
# app.mount('/static', StaticFiles(directory='static'), name='static')
templates = Jinja2Templates(directory='templates')


@app.get('/', response_class=HTMLResponse)
async def root(request: Request, client_id: EnsureClientID):#, act_id: EnsureActId):
    # TODO: general bluesky landing page, different client views as plugins
    return 'BlueSky server landing page under construction.'
    # response = templates.TemplateResponse(name='pmtiles.html', request=request, context={'client_id': client_id})
    # return response

@app.get('/api/nodes/', description='Retrieve the current list of simulation nodes and their status.')
async def get_nodes():
    manager = WebServer.instance()
    tasks = [asyncio.create_task(manager.request(node_id, 'SIMINFO', timeout=5.0), name=node_id) for node_id in manager.sim_nodes]
    await asyncio.gather(*tasks)
    return {
        task.get_name(): task.result() for task in tasks
    }

@app.put('/api/nodes/{node_id}/send/{topic}', description='Send information to a simulation node.')
async def send_data(node_id: Annotated[str, Path(description='Network ID of the node to send data to')],
                    topic: Annotated[str, Path(description='Topic of the data to send', pattern=r'^[A-Za-z]+$')],
                    body: Annotated[dict, Body()]):
    manager = WebServer.instance()
    if node_id not in manager.sim_nodes:
        raise HTTPException(422, f'Node with ID {node_id} does not exist, or is not connected to this server. Existing node ID\'s: {", ".join(manager.sim_nodes)}')
    print(body)


@app.get('/api/nodes/{node_id}/request/{topics}', description='Request the full state for one or more topics.')
async def request_data(node_id: Annotated[str, Path(description='Network ID of the node to get data from')],
                       topics: Annotated[str, Path(description='One or more topics to request data for, separated by commas', pattern=r'^[A-Za-z]+(?:,[A-Za-z]+)*$')],
                       timeout: Annotated[float, Query(description='Maximum number of seconds to wait for the simulation data request.', ge=0.0)] = 5.0):
    manager = WebServer.instance()
    if node_id not in manager.sim_nodes:
        raise HTTPException(422, f'Node with ID {node_id} does not exist, or is not connected to this server. Existing node ID\'s: {", ".join(manager.sim_nodes)}')
    return JSONResponse(content=
        jsonable_encoder(await manager.request(
            node_id, *topics.split(','), timeout=timeout
            ), custom_encoder={ndarray: encode_json}))


class WebServer(Server):
    __server: Self|None = None

    @classmethod
    def instance(cls) -> Self:
        return cls.__server or cls()

    def __init__(self, altconfig=None, startscn=None, workdir=None):
        if WebServer.__server:
            raise RuntimeError('ConnectionManager already initialized')
        super().__init__(altconfig, startscn, workdir)
        WebServer.__server = self

        self.connections: dict[str, Connection] = {}

        # The uvicorn server object
        self.uviserver = None
        self.thread = None
        self.is_quitting = False

    async def addConnection(self, ws: WebSocket, client_id: str) -> 'Connection':
        await ws.accept()
        conn = Connection(ws, client_id, self)
        self.connections[client_id] = conn
        # Send subscription for direct messages
        await super().subscribe(client_id.encode('charmap'))
        if self.sim_nodes:
            print('Sending list of current nodes to new WS client')
            await conn.announce_joined(*self.sim_nodes)
            await conn.set_actnode(self.sim_nodes[0])
        return conn

    async def removeConnection(self, client_id: str):
        conn = self.connections.pop(client_id, None)
        if conn is not None:
            # Unsubscribe for all topics that this connection was subscribed to
            await asyncio.gather(
                self.unsubscribe(client_id.encode('charmap')),
                *(self.unsubscribe(topic) for topic in conn.subscriptions)
            )

    async def register_node(self, node_id: str):
        ''' Register a simulation node with this server.

            Arguments:
            - node_id: The id of the node to register
        '''
        print('Node joined:', node_id)
        await asyncio.gather(
            super().register_node(node_id),
            *(conn.announce_joined(node_id) for conn in self.connections.values())
        )

    async def unregister_node(self, node_id: str):
        ''' Unregister a simulation node from this server.

            Arguments:
            - node_id: The id of the node to unregister
        '''
        print('Node left:', node_id)
        await asyncio.gather(
            super().unregister_node(node_id),
            *(conn.announce_leave(node_id) for conn in self.connections.values())
        )

    async def forward(self, msgid: bytes, data: bytes):
        await asyncio.gather(
            super().forward(msgid, data),
            *(conn.send(msgid, data) for conn in self.connections.values())
        )

    def run(self, threaded: bool = False) -> None:
        config = uvicorn.Config(app, host=bs.settings.http_host, port=bs.settings.http_port)
        self.uviserver = uvicorn.Server(config)
        if threaded:
            self.thread = threading.Thread(target=self.uviserver.run)
            self.thread.start()
        else:
            self.uviserver.run()

    def quit(self):
        if self.is_quitting:
            return
        self.is_quitting = True
        if self.uviserver and self.thread:
            self.uviserver.should_exit = True
        return super().quit()


class Connection:
    def __init__(self, connection: WebSocket, client_id: str, manager: WebServer):
        self.connection = connection
        self.manager = manager
        self.client_id = client_id
        self.bclient_id = client_id.encode('charmap')
        self.act_id = ''
        self.act_subscriptions: set[bytes] = set()
        self.subscriptions: set[bytes] = set()  # topics

    async def subscribe(self, topic: str, from_group: str='', to_group: str='', actonly: bool=False):
        if actonly:
            act_sub = zmq_msgid(topic, from_group)
            msgid = zmq_msgid(topic, from_group, self.act_id)
            self.act_subscriptions.add(act_sub)
        else:
            msgid = zmq_msgid(topic, from_group, to_group)
        self.subscriptions.add(msgid)
        await self.manager.subscribe(msgid)

    async def unsubscribe(self, topic: str, from_group: str='', to_group: str=''):
        act_sub = zmq_msgid(topic, from_group)
        if act_sub in self.act_subscriptions:
            self.act_subscriptions.remove(act_sub)
            msgid = zmq_msgid(topic, from_group, self.act_id)
        else:
            # This is not an actonly subscription. Use to_group to unsubscribe
            msgid = zmq_msgid(topic, from_group, to_group)
        if msgid in self.subscriptions:
            self.subscriptions.remove(msgid)
            await self.manager.unsubscribe(msgid)

    async def set_actnode(self, node_id: str, announce=True):
        async_tasks = []
        # First remove subscriptions from previous active node and replace with new subscriptions
        if self.act_id:
            for act_sub in self.act_subscriptions:
                prev_sub = act_sub + self.act_id.encode('charmap')
                new_sub = act_sub + node_id.encode('charmap')
                self.subscriptions.discard(prev_sub)
                self.subscriptions.add(new_sub)
                async_tasks.extend([self.manager.unsubscribe(prev_sub), self.manager.subscribe(new_sub)])
        else:
            for act_sub in self.act_subscriptions:
                new_sub = act_sub + node_id.encode('charmap')
                self.subscriptions.add(new_sub)
                async_tasks.append(self.manager.subscribe(new_sub))
        self.act_id = node_id
        if announce:
            # Communicate to gui
            data = msgpack.packb(node_id, use_bin_type=False)
            if data is not None:
                async_tasks.append(self.connection.send_bytes(ws_msgid('ACTNODE-CHANGED') + data))
        # Await all network I/O
        await asyncio.gather(*async_tasks)

    async def announce_joined(self, *node_ids: str):
        data = msgpack.packb(node_ids, use_bin_type=False)
        if data is not None:
            await self.connection.send_bytes(ws_msgid('NODE-ADDED') + data)

    async def announce_leave(self, *node_ids: str):
        data = msgpack.packb(node_ids, use_bin_type=False)
        if data is not None:
            await self.connection.send_bytes(ws_msgid('NODE-REMOVED') + data)

    async def send(self, msgid: bytes, data: bytes):
        if any(msgid.startswith(sub) for sub in self.subscriptions) or msgid.startswith(self.bclient_id):
            # Convert zmq_msgid to ws_msgid
            msgid = ws_msgid(*unpack_zmq_msgid(msgid))
            try:
                await self.connection.send_bytes(msgid + data)
            except WebSocketDisconnect:
                pass


@app.websocket('/ws')
async def websocket_client(websocket: WebSocket, client_id: str=''):
    manager = WebServer.instance()
    client_id = await get_client_id(websocket.session, client_id)
    try:
        conn = await manager.addConnection(websocket, client_id)
        while True:
            to_group, topic, data = msgpack.unpackb(await websocket.receive_bytes(), raw=False)
            if topic == 'SUBSCRIBE':
                await conn.subscribe(**data)
            elif topic == 'UNSUBSCRIBE':
                await conn.unsubscribe(**data)
            elif topic == 'ACTNODE-CHANGED':
                await conn.set_actnode(**data, announce=False)
            else:
                await manager.send(topic, data, to_group, client_id)

    except WebSocketDisconnect:
        await manager.removeConnection(client_id)


def start(threaded=False, **kwargs):
    server = WebServer(kwargs.get('altconfig'), kwargs.get('startscn'), kwargs.get('workdir'))
    server.run(threaded)


# To run: uvicorn server:app --reload
if __name__ == '__main__':
    start()
