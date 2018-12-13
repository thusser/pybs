import asyncio
import json


class RpcError(Exception):
    """Exception for all RPC errors."""
    pass


class RpcClient:
    """Client for remote procedure calls."""

    def __init__(self, host: str = 'localhost', port: int = 16219):
        """Create a new RPC client.

        Args:
            host: Hostname of server.
            port: Port on server to connect to.
        """
        self._cur_id = 1
        self._host = host
        self._port = port

    def __call__(self, command: str, **kwargs):
        """Calls a command on the server.

        Args:
            command: Name of command to run.
            **kwargs: Parameters for command

        Returns:
            Result of command.
        """

        # get event loop, run command and return result
        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(self._send_command(command, **kwargs))
        return result

    async def _send_command(self, command: str, **kwargs):
        """Actually send a command to the server and wait for results.

        Args:
            command: Name of command to run.
            **kwargs: Parameters for command

        Returns:
            Result of command.
        """

        # open connection
        reader, writer = await asyncio.open_connection(self._host, self._port)

        # build message
        message = {
            'jsonrpc': '2.0',
            'method': command,
            'params': kwargs,
            'id': self._cur_id
        }
        self._cur_id += 1

        # send command
        writer.write((json.dumps(message) + '\n').encode())

        # wait for reply
        data = await reader.readline()

        # close socket
        writer.close()

        # decode data
        rpc = json.loads(data.decode())

        # got an error?
        if 'error' in rpc:
            # with a message?
            if 'message' in rpc['error']:
                raise RpcError(rpc['error']['message'])
            raise RpcError('Unknown error')

        # return result
        return rpc['result']


__all__ = ['RpcClient', 'RpcError']
