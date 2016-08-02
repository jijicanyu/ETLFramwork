#coding:utf8

"""
HTTP JSON RPC client lib

Author: wangxingang02@baidu.com

Example:

rpc = ServerProxy(("localhost", 9192))  #make a server proxy
result = rpc.echo(args)  #call the server's echo method

Both args and result is a Python dict object.

"""

import base64
import hashlib
import httplib
import itertools
import json


class _Method:
    """some magic to bind an RPC method to RPC server """

    def __init__(self, send_to, method_name):
        """constructor

        Args:
            send_to: A function to send local call to RPC Server
                This function must have a proto like:
                def send_to(method_name, args)
                    return dict(...)
                As method_name is a string of the RPC method name
                args is a python dict of the RPC arguments
                The return values of send_to must be a python dict
            method_name: A string of the PRC method name.
        """
        self._send_to = send_to
        self._method_name = method_name

    def __call__(self, args):
        """call the RPC method through send_to function """
        return self._send_to(self._method_name, args)

def hash_password(password, random_data=''):
    '''SHA1(password) XOR SHA1("20-bytes random data" <concat> SHA1(SHA1(password)))
    '''
    password_one_sha1 = hashlib.sha1(password).digest()
    password_two_sha1 = hashlib.sha1(password_one_sha1).digest()
    tmp = hashlib.sha1(random_data + password_two_sha1).digest()
    password_hash = ''.join((chr(ord(x) ^ ord(y))
        for (x, y) in itertools.izip(tmp, password_one_sha1)))
    return password_hash.encode('hex')


class ServerProxy:
    """A client proxy for a JSON RPC server"""
    def __init__(self, host, user="", password=""):
        self._host = host
        self._auth = None
        if user:
            if password:
                user_passwd = ':'.join((user, hash_password(password)))
                #user_passwd = ':'.join((user, hash_password(password).encode('hex')))
            else:
                user_passwd = user + ':'
            self._auth = base64.encodestring(user_passwd).replace('\n', '')

    def call_json(self, method_name, json_request):
        headers = {
            "Content-type": "application/json",
            "Accept": "application/json",
            "Content-length": len(json_request)
        }

        if self._auth:
            headers['Authorization'] = "Basic %s" % self._auth

        conn = httplib.HTTPConnection("%s:%d" % self._host)
        conn.request("POST", method_name, json_request, headers)
        response = conn.getresponse()
        json_response = response.read()
        conn.close()
        return json_response


    def _call_remote(self, method_name, args):
        """Call the remote RPC.

        @see _Method.__init__ for details.
        """
        json_request = json.dumps(args)
        json_response = self.call_json(method_name, json_request)
        return json.loads(json_response)

    def __getattr__(self, attr):
        return _Method(self._call_remote, attr)


