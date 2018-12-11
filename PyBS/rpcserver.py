import asyncio
import json


class RpcServer:
    def __init__(self, handler, port):
        self._handler = handler
        self._port = port
        self._server = None

    async def open(self):
        self._server = await asyncio.start_server(self.handle_request, '127.0.0.1', self._port,
                                                  loop=asyncio.get_event_loop())

    def close(self):
        self._server.close()

    async def wait_closed(self):
        await self._server.wait_closed()

    async def handle_request(self, reader, writer):
        # read data and decode it
        data = await reader.readline()
        message = data.decode()

        # parse json
        rpc = json.loads(message)

        # get method on handler
        if not hasattr(self._handler, rpc['method']):
            res = {'jsonrpc': '2.0', 'error': {'code': -32601, 'message': 'Method not found'}, 'id': rpc['id']}
            await self._send(writer, json.dumps(res))
            return
        method = getattr(self._handler, rpc['method'])

        # call method
        try:
            result = method(**rpc['params'])
        except ValueError as e:
            res = {'jsonrpc': '2.0', 'error': {'code': -32603, 'message': str(e)}, 'id': rpc['id']}
            await self._send(writer, json.dumps(res))
            return

        # send response
        res = {'jsonrpc': '2.0', 'result': result, 'id': rpc['id']}
        await self._send(writer, json.dumps(res))

    async def _send(self, writer, message):
        # send response
        writer.write((message + '\n').encode())
        await writer.drain()

        # close socket
        writer.close()
