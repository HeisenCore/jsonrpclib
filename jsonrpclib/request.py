import datetime
import logging
import inspect
import socket
from collections import defaultdict
from itertools import cycle

from jsonrpclib import Server
from jsonrpclib.jsonrpc import Transport


logger = logging.getLogger(__name__)


class Connection(object):
    def __init__(self, servers_dict=None, transport_method='django', user=None, reinitiate_delay=20):
        if servers_dict is None:
            raise ValueError('Server list shouldn\'t be empty')

        self.reinitiate_delay = datetime.timedelta(seconds=reinitiate_delay)

        self.original = servers_dict
        self.user = user
        self.transport_method = transport_method

        self._create_server_list()

    def _create_server_list(self):
        self.initiate_time = datetime.datetime.now()
        self.servers = {
            key: cycle(value) for key, value in self.original.items()
        }

        self.black_list = defaultdict(list)

    def __getattr__(self, name):
        """ needed for transport """
        if name in self.original:
            return self.get_connection(name)

    def get_connection(self, server_name):
        server_info = self.get_available_server(server_name)
        return self.connect(*server_info)

    def get_available_server(self, server_name):
        server_info = self._get_server(server_name)

        while server_info in self.black_list[server_name] or (not self.is_alive(server_name, server_info)):
            server_info = self._get_server(server_name)

        return server_info

    def _get_server(self, server_name):
        try:
            return self.servers[server_name].next()
        except StopIteration:
            now = datetime.datetime.now()
            if self.initiate_time < (now - self.reinitiate_delay):
                self._create_server_list()

            raise NoServer('All servers are offline')

    def is_alive(self, server_name, server_info):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex((server_info[0], server_info[1]))

        alive = result == 0

        if not alive:
            self.black_list[server_name].append(server_info)
            self.original[server_name].remove(server_info)
            self.servers[server_name] = cycle(self.original[server_name])

        return alive

    def connect(self, host, port, auth_user=None, auth_password=None):
        address, user = self.get_transport_info(host, port)

        auth = ''
        if auth_user and auth_password:
            auth = '{0}:{1}@'.format(auth_user, auth_password)

        server = Server(
            'http://{}{}:{}'.format(auth, host, port),
            transport=SpecialTransport(user=user, address=address)
        )

        return server

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
