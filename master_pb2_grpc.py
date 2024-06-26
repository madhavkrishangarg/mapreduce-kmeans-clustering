# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

import master_pb2 as master__pb2


class MasterStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.StartMapper = channel.unary_unary(
                '/Master/StartMapper',
                request_serializer=master__pb2.MapperRequest.SerializeToString,
                response_deserializer=master__pb2.MapperResponse.FromString,
                )
        self.StartReducer = channel.unary_unary(
                '/Master/StartReducer',
                request_serializer=master__pb2.ReducerRequest.SerializeToString,
                response_deserializer=master__pb2.ReducerResponse.FromString,
                )
        self.GetInput = channel.unary_unary(
                '/Master/GetInput',
                request_serializer=master__pb2.GetInputRequest.SerializeToString,
                response_deserializer=master__pb2.GetInputResponse.FromString,
                )


class MasterServicer(object):
    """Missing associated documentation comment in .proto file."""

    def StartMapper(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def StartReducer(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetInput(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_MasterServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'StartMapper': grpc.unary_unary_rpc_method_handler(
                    servicer.StartMapper,
                    request_deserializer=master__pb2.MapperRequest.FromString,
                    response_serializer=master__pb2.MapperResponse.SerializeToString,
            ),
            'StartReducer': grpc.unary_unary_rpc_method_handler(
                    servicer.StartReducer,
                    request_deserializer=master__pb2.ReducerRequest.FromString,
                    response_serializer=master__pb2.ReducerResponse.SerializeToString,
            ),
            'GetInput': grpc.unary_unary_rpc_method_handler(
                    servicer.GetInput,
                    request_deserializer=master__pb2.GetInputRequest.FromString,
                    response_serializer=master__pb2.GetInputResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'Master', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class Master(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def StartMapper(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/Master/StartMapper',
            master__pb2.MapperRequest.SerializeToString,
            master__pb2.MapperResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def StartReducer(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/Master/StartReducer',
            master__pb2.ReducerRequest.SerializeToString,
            master__pb2.ReducerResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def GetInput(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/Master/GetInput',
            master__pb2.GetInputRequest.SerializeToString,
            master__pb2.GetInputResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
