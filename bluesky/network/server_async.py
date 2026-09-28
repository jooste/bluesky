from collections import defaultdict
from contextlib import suppress
import platform
import sys
import os
import signal
import threading
from multiprocessing import cpu_count
import asyncio
import subprocess
from typing import Any
import zmq
import zmq.asyncio
import msgpack

import bluesky as bs
from bluesky.network.npcodec import encode_ext, ext_hook
from bluesky.network.discovery import Discovery
from bluesky.network.common import ActionType, genid, unpack_zmq_msgid, zmq_msgid, MSG_SUBSCRIBE, MSG_UNSUBSCRIBE, GROUPID_SIM, IDLEN


# Register settings defaults
bs.settings.set_variable_defaults(max_nnodes=cpu_count(),
                                  recv_port=11000, send_port=11001,
                                  enable_discovery=False)

def split_scenarios(scentime, scencmd):
    ''' Split the contents of a batch file into individual scenarios. '''
    start = 0
    for i in range(1, len(scencmd) + 1):
        if i == len(scencmd) or scencmd[i][:4] == 'SCEN':
            scenname = scencmd[start].split()[1].strip()
            yield dict(name=scenname, scentime=scentime[start:i], scencmd=scencmd[start:i])
            start = i


class Server:
    def __init__(self, altconfig=None, startscn=None, workdir=None):
        self.spawned_processes = dict()
        self.running = True
        self.max_nnodes = min(cpu_count(), bs.settings.max_nnodes)
        self.scenarios = []
        self.server_id: str = genid(groupid=GROUPID_SIM, seqidx=0)
        self.bserver_id: bytes = self.server_id.encode('charmap')
        self.max_group_idx = 0
        self.sim_nodes = list()
        self.all_nodes = set()
        self.avail_nodes = set()

        self._requests: dict[tuple[str, str], Request] = defaultdict(Request)

        # Information to pass on to spawned nodes
        self.workdir = workdir
        self.altconfig = altconfig
        self.startscn = startscn

        if bs.settings.enable_discovery:
            self.discovery = Discovery(self.server_id, is_client=False)
        else:
            self.discovery = None

        # Get context and initialize sockets and poller
        ctx = zmq.asyncio.Context()
        self.sock_recv = ctx.socket(zmq.XSUB)
        self.sock_send = ctx.socket(zmq.XPUB)
        self.poller = zmq.asyncio.Poller()

    def quit(self):
        ''' Quit the server loop. '''
        print('Quit signal received')
        self.running = False

    async def _spawn_single(self, node_id=None, startscn=None):
        ''' Spawn a single node with given node_id. '''
        if node_id:
            newid = genid(node_id)
        else:
            self.max_group_idx += 1
            newid = genid(self.server_id, seqidx=self.max_group_idx)
        args = [sys.executable, '-m', 'bluesky', '--sim', '--groupid', newid]
        kwargs = dict()
        if self.altconfig:
            args.extend(['--configfile', self.altconfig])
        if self.workdir:
            args.extend(['--workdir', self.workdir])
        if startscn:
            args.extend(['--scenfile', startscn])
        if platform.system() == 'Windows':
                kwargs['creationflags'] = subprocess.CREATE_NEW_PROCESS_GROUP
        return newid, await asyncio.subprocess.create_subprocess_exec(*args, **kwargs)

    async def addnodes(self, count=1, node_ids=None, startscn=None):
        ''' Add [count] nodes to this server. '''
        self.spawned_processes.update({
            newid:proc for newid, proc in await asyncio.gather(
                *(self._spawn_single(node_id=(
                    node_ids[idx] if node_ids else None
                ), startscn=startscn) for idx in range(count))
            )
        })

    async def sendscenario(self, node_id):
        # Send a new scenario to the target sim process
        scen = self.scenarios.pop(0)
        await self.send('BATCH', scen, node_id)

    async def send(self, topic: str, data: Any='', to_group: str='', sender_id: str='') -> None:
        ''' Send data originating from the server to the specified destination.

            Arguments:
            - topic: The publication topic
            - data: The data to send
            - to_group: The destination mask. If empty, broadcast to all.
            - sender_id: The id of the sending node. If empty, use server id.
        '''
        # Pack identifier and data
        msgid = zmq_msgid(topic, sender_id or self.server_id, to_group)
        packed = msgpack.packb(data, default=encode_ext, use_bin_type=True)
        if packed is not None:
            await self.forward(msgid, packed)

    async def forward(self, msgid: bytes, data: bytes) -> None:
        await self.sock_send.send_multipart([msgid, data])

    def run(self, threaded: bool=False) -> None:
        ''' Run the server loop. '''
        if threaded:
            # create a new thread to execute the server main loop
            thread = threading.Thread(target=asyncio.run, args=(self.loop(),))
            thread.start()
        else:
            asyncio.run(self.loop())

    async def _request_response(self, node_id, topic):
        return await self._requests[(node_id, topic)].get()

    async def request(self, node_id, *topics, timeout=0.0) -> dict|str|None:
        ''' Send a data request to a node for one or more topics. 
            When timeout is larger than zero this method will block until all
            responses are in, or when the timeout duration is reached.
            When timeout=0 it returns None, otherwise a dictionary with 
            collecred responses.
        '''
        if node_id not in self.sim_nodes:
            return f'Node with ID {node_id} does not exist, or is not connected to this server.'
        utopics = [t.upper() for t in topics]
        snd = self.send('REQUEST', utopics, node_id)
        if timeout == 0:
            return await snd
        
        tasks = [asyncio.create_task(self._request_response(node_id, topic), name=topic) for topic in utopics]
        await asyncio.gather(snd, asyncio.wait(tasks, timeout=timeout))
        return {
                task.get_name(): task.result() if task.done() else f'Timeout or topic unknown. Waited {timeout} seconds' for task in tasks
            }

    async def subscribe(self, msgid: bytes) -> None:
        ''' Send subscribe request on the recv socket.

            Arguments:
            - msgid: The packed message identifier to subscribe to
              This is a zmq_msgid, so a concatenation of:
                - to_group (destination mask padded to IDLEN with '*')
                - the publication topic
                - from_group (sender mask for subscriptions, sender id for the actual publications)
        '''
        await self.sock_recv.send_multipart([b'\x01' + msgid])

    async def unsubscribe(self, msgid: bytes) -> None:
        ''' Send unsubscribe request on the recv socket.

            Arguments:
            - msgid: The message identifier to unsubscribe from
            This is a zmq_msgid, so a concatenation of:
                - to_group (destination mask padded to IDLEN with '*')
                - the publication topic
                - from_group (sender mask for subscriptions, sender id for the actual publications)
        '''
        await self.sock_recv.send_multipart([b'\x00' + msgid])

    async def register_node(self, node_id: str) -> None:
        ''' Register a simulation node with this server.

            Arguments:
            - node_id: The id of the node to register
        '''
        # This is an initial client, server, or node subscription
        if node_id[0] == GROUPID_SIM and node_id in self.spawned_processes:
            # This is a node owned by this server which has successfully started.
            if node_id not in self.sim_nodes:
                self.sim_nodes.append(node_id)
            await self.send('REQUEST', ['STATECHANGE'], node_id)
    
    async def unregister_node(self, node_id: str) -> None:
        ''' Unregister a simulation node from this server.

            Arguments:
            - node_id: The id of the node to unregister
        '''
        with suppress(ValueError):
            self.sim_nodes.remove(node_id)

    async def loop(self) -> None:
        ''' Main server loop. '''
        self.sock_recv.bind(f'tcp://*:{bs.settings.send_port}')
        self.sock_send.bind(f'tcp://*:{bs.settings.recv_port}')
        self.poller.register(self.sock_recv, zmq.POLLIN)
        self.poller.register(self.sock_send, zmq.POLLIN)

        print(f'BlueSky Simulation Server started with ID {self.server_id}')
        print(f'Listening for clients on ports {bs.settings.recv_port} (recv) and {bs.settings.send_port} (send)')

        if self.discovery:
            self.poller.register(self.discovery.handle, zmq.POLLIN)
        print(f'Discovery is {"en" if self.discovery else "dis"}abled')

        # Create subscription for messages targeted at this server
        await self.sock_recv.send_multipart([b'\x01' + self.server_id.encode('charmap')])

        # Start the first simulation node
        await self.addnodes(startscn=self.startscn)

        while self.running:
            events = dict(await self.poller.poll())
            # The socket with incoming data
            for sock, event in events.items():
                if event != zmq.POLLIN:
                    # The event does not refer to incoming data: skip for now
                    continue

                # First check if the poller was triggered by the discovery socket
                if self.discovery and sock == self.discovery.handle.fileno():
                    # This is a discovery message
                    dmsg = self.discovery.recv_reqreply()
                    if dmsg.conn_id != self.server_id and dmsg.is_request:
                        # This is a request from someone else: send a reply
                        self.discovery.send_reply(bs.settings.recv_port,
                            bs.settings.send_port)
                    continue

                msg = await sock.recv_multipart()
                if not msg:
                    # In the rare case that a message is empty, skip remaning processing
                    continue
                if sock == self.sock_send:
                    # This is an (un)subscribe message. If it's an id-only subscription
                    # this is also a registration message
                    if len(msg[0]) == IDLEN + 1:
                        node_id = msg[0][1:].decode()
                        if msg[0][0] == MSG_SUBSCRIBE:
                            await self.register_node(node_id)
                            
                        elif msg[0][0] == MSG_UNSUBSCRIBE:
                            await self.unregister_node(node_id)
                    # Always forward to zmq publishers
                    await self.sock_recv.send_multipart(msg)

                elif sock == self.sock_recv:
                    # First check if message is directed at this server
                    if msg[0].startswith(self.bserver_id):
                        # Unpack the message
                        topic, sender_id, to_group = unpack_zmq_msgid(msg[0])
                        data = msgpack.unpackb(msg[1], ext_hook=ext_hook, raw=False)
                        # Check if someone was waiting for this response.
                        # If so, notify with data
                        if request := self._requests.pop((sender_id, topic), None):
                            request.set(data)
                        elif topic == 'QUIT':
                            self.quit()
                        elif topic == 'ADDNODES':
                            if isinstance(data, int):
                                await self.addnodes(count=data)
                            elif isinstance(data, dict):
                                await self.addnodes(**data)
                        elif topic == 'STATECHANGE':
                            state = data[1]['simstate']
                            if state < bs.OP:
                                # If we have batch scenarios waiting, send
                                # the simulation node a new scenario, otherwise store it in
                                # the available simulation node list
                                if self.scenarios:
                                   await  self.sendscenario(sender_id)
                                else:
                                    self.avail_nodes.add(sender_id)
                            else:
                                self.avail_nodes.discard(sender_id)
                        elif topic == 'BATCH':
                            scentime, scencmd = data
                            self.scenarios = [scen for scen in split_scenarios(scentime, scencmd)]
                            # Check if the batch list contains scenarios
                            if not self.scenarios:
                                echomsg = 'No scenarios defined in batch file!'
                            else:
                                echomsg = f'Found {len(self.scenarios)} scenarios in batch'
                                # Send scenario to available nodes (nodes that are in init or hold mode):
                                while self.avail_nodes and self.scenarios:
                                    node_id = next(iter(self.avail_nodes))
                                    await self.sendscenario(node_id)
                                    self.avail_nodes.discard(node_id)

                                # If there are still scenarios left, determine and
                                # start the required number of local nodes
                                reqd_nnodes = min(len(self.scenarios), max(0, self.max_nnodes - len(self.sim_nodes)))
                                await self.addnodes(reqd_nnodes)
                            # ECHO the results to the calling client
                            await self.send('ECHO', dict(text=echomsg, flags=0), sender_id)

                    else:
                        await self.forward(*msg)
        print('Server quit. Stopping nodes:')

        # Terminate all spawned processes
        for proc in self.spawned_processes.values():
            if platform.system() == 'Windows':
                os.kill(proc.pid, getattr(signal, 'CTRL_BREAK_EVENT'))
            else:
                proc.terminate()

        await asyncio.gather(*(
            [proc.wait() for proc in self.spawned_processes.values()] +
            [self.sock_recv.send_multipart([b'\x00' + pid.encode('charmap')]) for pid in self.spawned_processes.keys()]
            ))
        print('Closing connections:', end=' ')
        self.poller.unregister(self.sock_recv)
        self.poller.unregister(self.sock_send)
        self.sock_recv.close()
        self.sock_send.close()
        zmq.Context.instance().destroy()
        print('done')


class Request:
    def __init__(self) -> None:
        self._event = asyncio.Event()
        self.data = None

    async def get(self):
        await self._event.wait()
        return self.data
    
    def set(self, data):
        self.data = data[1] if ActionType.isaction(data[0]) else data[0]
        self._event.set()


def start(threaded=False, **kwargs):
    server = Server(kwargs.get('altconfig'), kwargs.get('startscn'), kwargs.get('workdir'))

    # Connect to interrupt signal
    signal.signal(signal.SIGINT, lambda *args: server.quit())
    signal.signal(signal.SIGTERM, lambda *args: server.quit())
    if platform.system() == 'Windows':
        signal.signal(getattr(signal, 'SIGBREAK'), lambda *args: server.quit())

    # Run the server loop
    server.run(threaded)


if __name__ == '__main__':
    start()
