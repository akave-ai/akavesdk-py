import grpc
import logging
from ..config import SDKError


class GrpcClientBase:  
    def __init__(self, connection_timeout: int = None):
        self.connection_timeout = connection_timeout
        
    def _handle_grpc_error(self, method_name: str, error: grpc.RpcError) -> None:
        status_code = error.code()
        details = error.details() or "No details provided"

        if status_code == grpc.StatusCode.DEADLINE_EXCEEDED:
            # Deadline exceeded → request took longer than connection_timeout
            logging.warning(f"{method_name} timed out after {self.connection_timeout}s")
            raise SDKError(f"{method_name} request timed out after {self.connection_timeout}s") from error

        logging.error(
            f"gRPC call {method_name} failed: {status_code.name} ({status_code.value}) - {details}"
        )
        raise SDKError(
            f"gRPC call {method_name} failed: {status_code.name} ({status_code.value}) - {details}"
        ) from error
