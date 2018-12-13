import asyncio
import json


class RpcServer:
    """Server for remote procedure calls."""

    def __init__(self, handler, port: int):
        """Creates a new RPC server.

        Args:
            handler: Object that implements the methods that this server serves.
            port: Port for clients to connect to.
        """
        self._handler = handler
        self._port = port
        self._server = None

    async def open(self):
        """Open server."""
        self._server = await asyncio.start_server(self.handle_request, '127.0.0.1', self._port,
                                                  loop=asyncio.get_event_loop())

    def close(self):
        """Close server."""
        self._server.close()

    async def wait_closed(self):
        """Wait for server to be closed."""
        await self._server.wait_closed()

    async def handle_request(self, reader, writer):
        """Handle a request from a client.

        Args:
            reader: Stream to read from.
            writer: Stream to write to.
        """

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

    async def _send(self, writer, message: str):
        """Send a message to the client and close connection.

        Args:
            writer: Stream to write to.
            message: Message to send.
        """

        # send response
        writer.write((message + '\n').encode())
        await writer.drain()

        # close socket
        writer.close()


__all__ = ['RpcServer']
