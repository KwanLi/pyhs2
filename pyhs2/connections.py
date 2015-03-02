import sys

from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport
import sasl
from cloudera.thrift_sasl import TSaslClientTransport

from TCLIService import TCLIService

from cursor import Cursor
from TCLIService.ttypes import TCloseSessionReq,TOpenSessionReq

class Connection(object):
    DEFAULT_KRB_SERVICE = 'hive'
    client = None
    session = None

    # def __init__(self, host, port, user):
    #     self.host = host
    #     self.port = port
    #     self.user = user
    #     self.session_handle = None
    #     self._client = None
 
    def connect(self):
        print 'connect'
        # transport = TSocket.TSocket(self.host, self.port)
        # transport = TTransport.TBufferedTransport(transport)
        # protocol = TBinaryProtocol.TBinaryProtocol(transport)
        # client = TCLIService.Client(protocol)
        # transport.open()
        # self._client = client

        authMechanisms = set(['NOSASL', 'PLAIN', 'KERBEROS', 'LDAP'])


        if self.authMechanism not in authMechanisms:
            raise NotImplementedError('authMechanism is either not supported or not implemented')
        #Must set a password for thrift, even if it doesn't need one
        #Open issue with python-sasl
        if self.authMechanism == 'PLAIN' and (self.password is None or len(self.password) == 0):
            password = 'password'
        socket = TSocket(self.host, self.port)
        socket.setTimeout(self.timeout)
        if self.authMechanism == 'NOSASL':
            transport = TBufferedTransport(socket)
        else:
            sasl_mech = 'PLAIN'
            saslc = sasl.Client()
            saslc.setAttr("username", user)
            saslc.setAttr("password", password)
            if authMechanism == 'KERBEROS':
                krb_host,krb_service = self._get_krb_settings(host, configuration)
                sasl_mech = 'GSSAPI'
                saslc.setAttr("host", krb_host)
                saslc.setAttr("service", krb_service)

            saslc.init()
            transport = TSaslClientTransport(saslc, sasl_mech, socket)

        self.client = TCLIService.Client(TBinaryProtocol(transport))
        transport.open()
        res = self.client.OpenSession(TOpenSessionReq(username=self.user, password=self.password, configuration=self.configuration))
        self.session = res.sessionHandle
        # if database is not None:
        #     with self.cursor() as cur:
        #         query = "USE {0}".format(database)
        #         cur.execute(query) 





    def __init__(self, host=None, port=10000, 
        authMechanism='NOSASL', user=None, password=None, database='default', 
        configuration=None, timeout=None):
        self.host = host
        self.port = port
        self.authMechanism=authMechanism
        self.user = user
        self.password=password
        self.database=database
        self.configuration=configuration
        self.timeout=timeout



    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback):
        self.close()

    def _get_krb_settings(self, default_host, config):
        host = default_host
        service = self.DEFAULT_KRB_SERVICE

        if config is not None:
            if 'krb_host' in config:
                host = config['krb_host']

            if 'krb_service' in config:
                service = config['krb_service']

        return host, service

    def cursor(self, secret=None, guid=None):
        if self.client is None:
            self.connect()
        return Cursor(self.client,self.session, secret=secret, guid=guid)

    def close(self):
        req = TCloseSessionReq(sessionHandle=self.session)
        self.client.CloseSession(req)