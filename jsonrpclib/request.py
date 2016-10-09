import datetime
import logging
import inspect
import socket
from collections import defaultdict
from itertools import cycle
from copy import deepcopy

from jsonrpclib import Server
from jsonrpclib.jsonrpc import Transport


logger = logging.getLogger('jsonrpclib')


class ConnectionPool(object):
    def __init__(self, servers_dict=None, transport_method='django', user=None, reinitiate_delay=5):
        if servers_dict is None:
            raise ValueError('Server list shouldn\'t be empty')

        self.reinitiate_delay = datetime.timedelta(seconds=reinitiate_delay)

        self.original = deepcopy(servers_dict)
        self.user = user
        self.transport_method = transport_method

        self._create_server_list()

    def _create_server_list(self):
        self.initiate_time = datetime.datetime.now()
        self.black_list = defaultdict(list)
        self.servers = {}

        for server_name, connections in self.original.items():
            servers = []
            for connection in connections:
                servers.append(Connection(self.transport_method, self.user, *connection))

            self.servers[server_name] = cycle(servers)

    def __getattr__(self, name):
        """ needed for transport """
        if name in self.original:
            return self.get_available_server(name).connection
        else:
            raise InvalidServerName('Specified server name doesn\'t exists')

    def __dir__(self):
        return self.original.keys()

    def get_available_server(self, server_name):
        connection = self._get_server(server_name)

        is_alive = self.is_alive(server_name, connection)
        while connection in self.black_list[server_name] or (not is_alive):
            connection = self._get_server(server_name)
            is_alive = self.is_alive(server_name, connection)

        return connection

    def _get_server(self, server_name):
        try:
            return self.servers[server_name].next()
        except StopIteration:
            now = datetime.datetime.now()
            if self.initiate_time < (now - self.reinitiate_delay):
                self._create_server_list()

            raise NoServer('All servers are offline')

    def is_alive(self, server_name, connection):
        alive = connection.is_alive

        if not alive:
            self.black_list[server_name].append(connection)
            new_server_list = deepcopy(self.original[server_name])
            for server in self.black_list[server_name]:
                new_server_list.remove(connection.connection_info)

            self.servers[server_name] = cycle(new_server_list)

        return alive

    def add_server(self, name, connection_info):
        if name in self.original:
            self.original[name].append(connection_info)
        else:
            self.original[name] = [connection_info]

        self._create_server_list()


class Connection(object):
    def __init__(self, transport_method, user, host, port, auth_user=None, auth_password=None):
        self.transport_method = transport_method
        self.user = user
        self.host = host
        self.port = port
        self.auth_user = auth_user
        self.auth_password = auth_password

    @property
    def connection_info(self):
        return (self.host, self.port, self.auth_user, self.auth_password)

    @property
    def is_alive(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex((self.host, self.port))

        return result == 0

    @property
    def connection(self):
        address, user = self.get_transport_info(self.host, self.port)

        auth = ''
        if self.auth_user and self.auth_password:
            auth = '{0}:{1}@'.format(self.auth_user, self.auth_password)

        return Server(
            'http://{}{}:{}'.format(auth, self.host, self.port),
            transport=SpecialTransport(user=user, address=address)
        )

    def get_transport_info(self, host, port):
        if self.transport_method == 'django':
            return self.django_transport()
        elif self.transport_method == 'heisen':
            return self.heisen_transport(host, port)

    def heisen_transport(self, host, port):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect((host, port))
        address = s.getsockname()[0]

        user = self.user

        return address, user

    def django_transport(self):
        stack = inspect.stack()
        user = 'Unknown'
        address = '0.0.0.0'
        for func in stack:
            if 'request' in func[0].f_locals:
                request = func[0].f_locals['request']
                user = getattr(request, 'user', None)
                if user:
                    user = user.username
                else:
                    user = 'Unautenticated'

                address = self.get_client_ip(request)

                break

        return address, user

    def get_client_ip(self, request):
        x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
        if x_forwarded_for:
            ip = x_forwarded_for.split(',')[0]
        else:
            ip = request.META.get('REMOTE_ADDR')

        return ip


class SpecialTransport(Transport):
    def __init__(self, *args, **kwargs):
        self.user = kwargs.pop('user', None)
        self.address = kwargs.pop('address', None)
        super(SpecialTransport, self).__init__(*args, **kwargs)

    def send_content(self, connection, request_body):
        connection.putheader("X-User", self.user)
        connection.putheader("X-Address", self.address)
        connection.putheader("Content-Length", str(len(request_body)))
        connection.endheaders()
        if request_body:
            connection.send(request_body)


class NoServer(Exception):
    pass


class InvalidServerName(Exception):
    pass
