import pytest
import pdb
import threading
from multiprocessing import Process
import concurrent.futures
from utils import *

CONNECT_TIMEOUT = 12


class TestConnect:

    def local_ip(self, args):
        '''
        check if ip is localhost or not
        '''
        if not args["ip"] or args["ip"] == 'localhost' or args["ip"] == "127.0.0.1":
            return True
        else:
            return False

    @pytest.mark.tags(CaseLabel.L2)
    def test_close(self, connect):
        '''
        target: test disconnect
        method: disconnect a connected client
        expected: connect failed after disconnected
        '''
        connect.close()
        with pytest.raises(Exception) as e:
            connect.list_collections()

    @pytest.mark.tags(CaseLabel.L2)
    def test_close_repeatedly(self, dis_connect, args):
        '''
        target: test disconnect repeatedly
        method: disconnect a connected client, disconnect again
        expected: raise an error after disconnected
        '''
        with pytest.raises(Exception) as e:
            dis_connect.close()

    @pytest.mark.tags(CaseLabel.L2)
    def test_connect_correct_ip_port(self, args):
        '''
        target: test connect with correct ip and port value
        method: set correct ip and port
        expected: connected is True        
        '''
        milvus = get_milvus(args["ip"], args["port"], handler=args["handler"])

    # TODO: Currently we test with remote IP, localhost testing need to add
    @pytest.mark.tags(CaseLabel.L2)
    def _test_connect_ip_localhost(self, args):
        '''
        target: test connect with ip value: localhost
        method: set host localhost
        expected: connected is True
        '''
        milvus = get_milvus(args["ip"], args["port"], args["handler"])
        # milvus.connect(host='localhost', port=args["port"])
        # assert milvus.connected()

    @pytest.mark.timeout(CONNECT_TIMEOUT)
    @pytest.mark.tags(CaseLabel.L2)
    def test_connect_wrong_ip_null(self, args):
        '''
        target: test connect with wrong ip value
        method: set host null
        expected: not use default ip, connected is False
        '''
        ip = ""
        with pytest.raises(Exception) as e:
            get_milvus(ip, args["port"], args["handler"])

    @pytest.mark.tags(CaseLabel.L2)
    def test_connect_uri(self, args):
        '''
        target: test connect with correct uri
        method: uri format and value are both correct
        expected: connected is True        
        '''
        uri_value = "tcp://%s:%s" % (args["ip"], args["port"])
        milvus = get_milvus(args["ip"], args["port"], uri=uri_value, handler=args["handler"])

    @pytest.mark.tags(CaseLabel.L2)
    def test_connect_uri_null(self, args):
        '''
        target: test connect with null uri
        method: uri set null
        expected: connected is True        
        '''
        uri_value = ""
        if self.local_ip(args):
            milvus = get_milvus(None, None, uri=uri_value, handler=args["handler"])
        else:
            with pytest.raises(Exception) as e:
                milvus = get_milvus(None, None, uri=uri_value, handler=args["handler"])

    @pytest.mark.tags(CaseLabel.L2)
    def test_connect_with_multiprocess(self, args):
        '''
        target: test uri connect with multiprocess
        method: set correct uri, test with multiprocessing connecting
        expected: all connection is connected        
        '''
        def connect():
            milvus = get_milvus(args["ip"], args["port"], handler=args["handler"])
            assert milvus

        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            future_results = {executor.submit(
                connect): i for i in range(100)}
            for future in concurrent.futures.as_completed(future_results):
                future.result()

    @pytest.mark.tags(CaseLabel.L2)
    def test_connect_repeatedly(self, args):
        '''
        target: test connect repeatedly
        method: connect again
        expected: status.code is 0, and status.message shows have connected already
        '''
        uri_value = "tcp://%s:%s" % (args["ip"], args["port"])
        milvus = Milvus(uri=uri_value, handler=args["handler"])
        milvus = Milvus(uri=uri_value, handler=args["handler"])

    @pytest.mark.tags(CaseLabel.L2)
    def _test_add_vector_and_disconnect_concurrently(self):
        '''
        Target: test disconnect in the middle of add vectors
        Method:
            a. use coroutine or multi-processing, to simulate network crashing
            b. data_set not too large incase disconnection happens when data is underd-preparing
            c. data_set not too small incase disconnection happens when data has already been transferred
            d. make sure disconnection happens when data is in-transport
        Expected: Failure, count_entities == 0

        '''
        pass

    @pytest.mark.tags(CaseLabel.L2)
    def _test_search_vector_and_disconnect_concurrently(self):
        '''
        Target: Test disconnect in the middle of search vectors(with large nq and topk)multiple times, and search/add vectors still work
        Method:
            a. coroutine or multi-processing, to simulate network crashing
            b. connect, search and disconnect,  repeating many times
            c. connect and search, add vectors
        Expected: Successfully searched back, successfully added

        '''
        pass

    @pytest.mark.tags(CaseLabel.L2)
    def _test_thread_safe_with_one_connection_shared_in_multi_threads(self):
        '''
        Target: test 1 connection thread safe
        Method: 1 connection shared in multi-threads, all adding vectors, or other things
        Expected: Functional as one thread

       '''
        pass


class TestConnectIPInvalid(object):
    """
    Test connect server with invalid ip
    """

    @pytest.fixture(
        scope="function",
        params=gen_invalid_ips()
    )
    def get_invalid_ip(self, request):
        yield request.param

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.timeout(CONNECT_TIMEOUT)
    def test_connect_with_invalid_ip(self, args, get_invalid_ip):
        ip = get_invalid_ip
        with pytest.raises(Exception) as e:
            milvus = get_milvus(ip, args["port"], args["handler"])


class TestConnectPortInvalid(object):
    """
    Test connect server with invalid ip
    """

    @pytest.fixture(
        scope="function",
        params=gen_invalid_ints()
    )
    def get_invalid_port(self, request):
        yield request.param

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.timeout(CONNECT_TIMEOUT)
    def test_connect_with_invalid_port(self, args, get_invalid_port):
        '''
        target: test ip:port connect with invalid port value
        method: set port in gen_invalid_ports
        expected: connected is False        
        '''
        port = get_invalid_port
        with pytest.raises(Exception) as e:
            milvus = get_milvus(args["ip"], port, args["handler"])


class TestConnectURIInvalid(object):
    """
    Test connect server with invalid uri
    """

    @pytest.fixture(
        scope="function",
        params=gen_invalid_uris()
    )
    def get_invalid_uri(self, request):
        yield request.param

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.timeout(CONNECT_TIMEOUT)
    def test_connect_with_invalid_uri(self, get_invalid_uri, args):
        '''
        target: test uri connect with invalid uri value
        method: set port in gen_invalid_uris
        expected: connected is False        
        '''
        uri_value = get_invalid_uri
        with pytest.raises(Exception) as e:
            milvus = get_milvus(uri=uri_value, handler=args["handler"])
