import time
import logging
import binascii
import io
import math
import concurrent.futures
from hashlib import sha256
from typing import List, Optional, Callable, Dict, Any, Union, Tuple, cast, Sequence
from google.protobuf.timestamp_pb2 import Timestamp
from datetime import datetime
import grpc # Add grpc import for error handling
from multiformats.cid import CID as CIDType

from .common import MIN_BUCKET_NAME_LENGTH, SDKError, BLOCK_SIZE, ENCRYPTION_OVERHEAD
from .erasure_code import ErasureCode
from .dag import build_dag, extract_block_data, DAGRoot, ChunkDAG
from .connection import ConnectionPool
from .model import (
    IPCBucketCreateResult, IPCBucket, IPCFileMeta, IPCFileListItem,
    IPCFileMetaV2, IPCFileChunkUploadV2, AkaveBlockData, FileBlockUpload,
    FileBlockDownload, Chunk, IPCFileDownload, FileChunkDownload
)
from private.encryption import encrypt, derive_key, decrypt
from private.pb import ipcnodeapi_pb2, ipcnodeapi_pb2_grpc

from multiformats import cid as cidlib


BlockSize = BLOCK_SIZE
EncryptionOverhead = ENCRYPTION_OVERHEAD



def encryption_key(parent_key: bytes, *info_data: str) -> bytes:
    """
    Derives an encryption key based on the parent key and additional info data.
    The info_data is concatenated and used to derive a new key.
    """
    if len(parent_key) == 0:
        return b''
    
    info = "/".join(info_data)
    return derive_key(parent_key, info.encode())


def to_ipc_proto_chunk(
    chunk_cid: str,
    index: int,
    size: int,
    blocks: List[FileBlockUpload]
) -> ipcnodeapi_pb2.IPCChunk:
    pb_blocks: List[ipcnodeapi_pb2.IPCChunk.Block] = []
    for block in blocks:
        pb_blocks.append(
            ipcnodeapi_pb2.IPCChunk.Block(cid=block.cid, size=len(block.data))
        )
    return ipcnodeapi_pb2.IPCChunk(
        cid=chunk_cid,
        index=index,
        size=size,
        blocks=pb_blocks
    )

class IPC:
    def __init__(
        self,
        client: ipcnodeapi_pb2_grpc.IPCNodeAPIStub,
        conn: grpc.Channel,
        ipc_instance: Any,
        max_concurrency: int,
        block_part_size: int,
        use_connection_pool: bool,
        encryption_key: Optional[bytes] = None,
        max_blocks_in_chunk: int = 32,
        erasure_code: Optional[ErasureCode] = None
    ) -> None:
        if client is None:
            raise SDKError("IPC client has not been initialized.")
        self.client = client
        self.conn = conn
        self.ipc = ipc_instance
        self.max_concurrency = max_concurrency
        self.block_part_size = block_part_size
        self.use_connection_pool = use_connection_pool
        self.encryption_key = encryption_key or b''
        self.max_blocks_in_chunk = max_blocks_in_chunk
        self.erasure_code = erasure_code


    def create_bucket(self, ctx: Any, name: str) -> IPCBucketCreateResult:
        if len(name) < MIN_BUCKET_NAME_LENGTH:
            raise SDKError("invalid bucket name")
        try:
            tx = self.ipc.storage.create_bucket(
                bucket_name=name,
                from_address=self.ipc.auth.address,
                private_key=self.ipc.auth.key,
                gas_limit=500000
            )
            receipt = self.ipc.web3.eth.wait_for_transaction_receipt(tx)
            if receipt["status"] != 1:
                raise SDKError("bucket creation transaction failed")
            block = self.ipc.web3.eth.get_block(receipt["blockNumber"])
            created_at = block["timestamp"]
            return IPCBucketCreateResult(name=name, created_at=created_at)
        except Exception as e:
            raise SDKError(f"bucket creation failed: {e}")
        

    def view_bucket(self, ctx: Any, bucket_name: str) -> Optional[IPCBucket]:
        if not bucket_name:
            raise SDKError("empty bucket name")
        try:
            request = ipcnodeapi_pb2.IPCBucketViewRequest(
                name=bucket_name,
                address=self.ipc.auth.address.lower()
            )
            response = self.client.BucketView(request)
            if not response:
                return None
            created_at = int(response.created_at.seconds) if response.created_at else 0
            return IPCBucket(
                id=response.id,
                name=response.name,
                created_at=created_at
            )
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND: # type: ignore[union-attr]
                return None
            raise SDKError(f"failed to view bucket: {e.details()}") # type: ignore[return-value]


    def list_buckets(self, ctx: Any) -> List[IPCBucket]:
        try:
            request = ipcnodeapi_pb2.IPCBucketListRequest(
                address=self.ipc.auth.address.lower()
            )
            response = self.client.BucketList(request)
            buckets: List[IPCBucket] = []
            for b in response.buckets:
                buckets.append(IPCBucket(
                    id=b.id,
                    name=b.name,
                    created_at=int(b.created_at.seconds) if b.created_at else 0
                ))
            return buckets
        except grpc.RpcError as e:
            raise SDKError(f"failed to list buckets: {e.details()}") # type: ignore[return-value]


    def delete_bucket(self, ctx: Any, name: str) -> None:
        if not name:
            raise SDKError("empty bucket name")
        try:
            # ensure exists
            req = ipcnodeapi_pb2.IPCBucketViewRequest(
                name=name,
                address=self.ipc.auth.address.lower()
            )
            _ = self.client.BucketView(req)
            # delete on-chain
            self.ipc.storage.delete_bucket(
                bucket_name=name,
                from_address=self.ipc.auth.address,
                private_key=self.ipc.auth.key,
                bucket_id_hex=req.name  # assume req.name is ID
            )
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND: # type: ignore[union-attr]
                return
            raise SDKError(f"failed to delete bucket: {e.details()}") # type: ignore[return-value]


    def file_info(self, ctx: Any, bucket_name: str, file_name: str) -> Optional[IPCFileMeta]:
        if not bucket_name or not file_name:
            raise SDKError("empty bucket or file name")
        try:
            request = ipcnodeapi_pb2.IPCFileViewRequest(
                bucket_name=bucket_name,
                file_name=file_name,
                address=self.ipc.auth.address.lower()
            )
            resp = self.client.FileView(request)
            if not resp:
                return None
            created_at = int(resp.created_at.seconds) if resp.created_at else 0
            return IPCFileMeta(
                root_cid=resp.root_cid,
                name=resp.file_name,
                bucket_name=resp.bucket_name,
                encoded_size=resp.encoded_size,
                created_at=created_at
            )
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND: # type: ignore[union-attr]
                return None
            raise SDKError(f"failed to get file info: {e.details()}") # type: ignore[return]


    def list_files(self, ctx: Any, bucket_name: str) -> List[IPCFileListItem]:
        if not bucket_name:
            raise SDKError("empty bucket name")
        try:
            request = ipcnodeapi_pb2.IPCFileListRequest(
                bucket_name=bucket_name,
                address=self.ipc.auth.address.lower()
            )
            resp = self.client.FileList(request)
            items: List[IPCFileListItem] = []
            for f in resp.list:
                items.append(IPCFileListItem(
                    root_cid=f.root_cid,
                    name=f.name,
                    encoded_size=f.encoded_size,
                    created_at=int(f.created_at.seconds) if f.created_at else 0
                ))
            return items
        except grpc.RpcError as e:
            raise SDKError(f"failed to list files: {e.details()}") # type: ignore[return-value]


    def file_delete(self, ctx: Any, bucket_name: str, file_name: str) -> None:
        if not bucket_name.strip() or not file_name.strip():
            raise SDKError(f"empty bucket or file name. Bucket: '{bucket_name}', File: '{file_name}'")

        try:
            # Delete file using storage contract
            self.ipc.storage.delete_file(
                bucket_name,
                file_name,
                self.ipc.auth.address, 
                self.ipc.auth.key
            )
            logging.info(f"IPC file_delete transaction sent for '{file_name}' in bucket '{bucket_name}'")
            return None
        except Exception as err:
            logging.error(f"IPC file_delete failed: {err}")
            raise SDKError(f"failed to delete file: {err}")


    def create_file_upload(
        self, ctx: Any, bucket_name: str, file_name: str
    ) -> IPCFileMetaV2:
        if not bucket_name or not file_name:
            raise SDKError("empty bucket or file name")
        # derive file ID
        file_id = self.ipc.web3.keccak(text=f"{bucket_name}/{file_name}")
        self.ipc.storage.create_file(
            bucket_name,
            file_name,
            file_id,
            0,
            self.ipc.auth.address,
            self.ipc.auth.key
        )
        return IPCFileMetaV2(
            root_cid="",
            bucket_name=bucket_name,
            encoded_size=0
        )


    def upload(
        self, ctx: Any, bucket_name: str, file_name: str, reader: Union[io.BufferedIOBase, io.RawIOBase]
    ) -> IPCFileMetaV2:
        bucket = self.ipc.storage.get_bucket_by_name(
            {"from": self.ipc.auth.address},
            bucket_name
        )
        if not bucket:
            raise SDKError("bucket not found")
        # prepare
        file_enc_key: bytes = encryption_key(self.encryption_key, bucket_name, file_name)
        overhead = EncryptionOverhead if file_enc_key else 0
        is_empty = True
        buf_size: int = (self.erasure_code.data_blocks if self.erasure_code else self.max_blocks_in_chunk) * BlockSize
        buf_size -= overhead
        buf: bytearray = bytearray(buf_size)
        dag_root: DAGRoot = DAGRoot.new()
        idx = 0
        total_size = 0
        while True:
            n: int | Any = reader.readinto(buf)
            if n == 0:
                if is_empty:
                    raise SDKError("empty file")
                break
            is_empty = False
            data = bytes(buf[:n])
            if file_enc_key:
                data = encrypt(file_enc_key, data, str(idx).encode())

            if self.erasure_code:
                # type-narrow self.erasure_code from Optional[ErasureCode] to ErasureCode
                assert isinstance(self.erasure_code, ErasureCode)
                data = self.erasure_code.encode(data)
                block_size = len(data) // (self.erasure_code.data_blocks + self.erasure_code.parity_blocks)
            else:
                block_size = BLOCK_SIZE

            chunk: ChunkDAG = build_dag(ctx, io.BytesIO(data), block_size)
            dag_root.add_link(str(chunk.cid), chunk.raw_data_size, chunk.proto_node_size)
            # send chunk... (reuse streaming or gRPC upload logic)
            total_size += len(data)
            idx += 1
        root_cid = dag_root.build()
        # commit to chain
        self.ipc.storage.commit_file(
            bucket_name, file_name, total_size, bytes(root_cid), self.ipc.auth.address, self.ipc.auth.key
        )
        committed_at = time.time()
        info = self.file_info(ctx, bucket_name, file_name)
        if info:
            return IPCFileMetaV2(
                root_cid=info.root_cid,
                bucket_name=bucket_name,
                encoded_size=info.encoded_size,
                size=total_size,
                created_at=info.created_at,
                committed_at=committed_at
            )
        return IPCFileMetaV2(
            root_cid=str(root_cid),
            bucket_name=bucket_name,
            encoded_size=total_size,
            size=total_size,
            committed_at=committed_at
        )


    def create_chunk_upload(self, ctx: Any, index: int, file_encryption_key: bytes, data: bytes, bucket_id: bytes, file_name: str) -> IPCFileChunkUploadV2:
        if not self.client: raise SDKError("IPC client not initialized")
        try:
            if file_encryption_key:
                data = encrypt(file_encryption_key, data, str(index).encode())
            
            size = len(data)
            block_size = int(BlockSize)
            
            if self.erasure_code:
                erasure_code: ErasureCode = self.erasure_code
                data = erasure_code.encode(data)
                block_size: int = len(data) // (erasure_code.data_blocks + erasure_code.parity_blocks)
            else:
                block_size: int = BLOCK_SIZE
            
            chunk_dag: ChunkDAG = build_dag(ctx, io.BytesIO(data), block_size)
            
            proto_chunk = to_ipc_proto_chunk(str(chunk_dag.cid), index, size, chunk_dag.blocks)
            
            request = ipcnodeapi_pb2.IPCFileUploadChunkCreateRequest(chunk=proto_chunk, bucket_id=bucket_id, file_name=file_name)
            response = self.client.FileUploadChunkCreate(request)

            if len(response.blocks) != len(chunk_dag.blocks):
                raise SDKError(f"received unexpected amount of blocks {len(response.blocks)}, expected {len(chunk_dag.blocks)}")

            for i, upload_resp in enumerate(response.blocks):
                block = chunk_dag.blocks[i]
                if block.cid != upload_resp.cid:
                    raise SDKError(f"block CID mismatch at position {i}")

                block.node_address = upload_resp.node_address
                block.node_id = upload_resp.node_id
                block.permit = upload_resp.permit.encode() # Assuming permit is string in proto


            return IPCFileChunkUploadV2(
                index=index, chunk_cid=cast(CIDType, chunk_dag.cid),
                actual_size=size, raw_data_size=chunk_dag.raw_data_size,
                proto_node_size=chunk_dag.proto_node_size,
                blocks=chunk_dag.blocks, bucket_id=bucket_id, file_name=file_name
            )
        except Exception as err:
            raise SDKError(f"failed to create chunk upload: {err}")


    def upload_chunk(self, ctx: Any, file_chunk_upload: IPCFileChunkUploadV2) -> None:
        pool = ConnectionPool()
        try:
            # Build the single IPCChunk message (to_ipc_proto_chunk returns just that)
            proto_chunk = to_ipc_proto_chunk(
                str(file_chunk_upload.chunk_cid),
                file_chunk_upload.index,
                file_chunk_upload.actual_size,
                file_chunk_upload.blocks
            )

            # Upload each block in parallel
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_concurrency) as executor:
                futures = [
                    executor.submit(
                        self._upload_block,
                        ctx,
                        pool,
                        i,
                        block,
                        proto_chunk,
                        file_chunk_upload.bucket_id,
                        file_chunk_upload.file_name
                    )
                    for i, block in enumerate(file_chunk_upload.blocks)
                ]
                for future in concurrent.futures.as_completed(futures):
                    future.result()

        except Exception as err:
            raise SDKError(f"failed to upload chunk: {err}")
        finally:
            # Always clean up the pool
            pool.close()


    def _upload_block(self, ctx: Any, pool: ConnectionPool, block_index: int, block, proto_chunk, bucket_id, file_name: str) -> None:
        try:
            client, closer, err = pool.create_ipc_client(block["node_address"], self.use_connection_pool)
            if err:
                raise SDKError(f"failed to create client: {str(err)}")

            if not client:
                raise SDKError("IPC client is not available")
            
            try:
                block_data = ipcnodeapi_pb2.IPCFileBlockData(
                    data=block["data"],
                    cid=block["cid"],
                    index=block_index,
                    chunk=proto_chunk,
                    bucket_id=bucket_id,
                    file_name=file_name
                )
                
                response = client.FileUploadBlock(iter([block_data]))
                if not response:
                    raise SDKError("failed to upload block")
            finally:
                if closer:
                    closer()
        except Exception as err:
            raise SDKError(f"failed to upload block {block['cid']}: {str(err)}")


    def fetch_block_data(
        self,
        ctx: Any,
        pool: ConnectionPool,
        chunk_cid: str,
        bucket_name: str,
        file_name: str,
        address: str,
        chunk_index: int,
        block_index: int,
        block: FileBlockDownload
    ) -> bytes | None:
        if block.filecoin:
            data = self.ipc.sp_client.fetch_block(block.filecoin.base_url, block.cid)
            return data
        if not block.akave:
            raise SDKError("missing block metadata")
        client, closer, err = pool.create_ipc_client(block.akave.node_address, self.use_connection_pool)
        if err:
            raise SDKError(f"failed to create client: {str(err)}")
        
        try:
            req = ipcnodeapi_pb2.IPCFileDownloadBlockRequest(
                bucket_name=bucket_name,
                file_name=file_name,
                chunk_cid=chunk_cid,
                chunk_index=chunk_index,
                block_cid=block.cid,
                block_index=block_index,
                address=address
            )
            stream = client.FileDownloadBlock(req, timeout=30.0) # type: ignore[no-untyped-call]
            buffer = io.BytesIO()
            for msg in stream:
                buffer.write(msg.data)
            return buffer.getvalue()
        finally:
            if closer:
                closer()


    def create_file_download(self, ctx: Any, bucket_name: str, file_name: str) -> IPCFileDownload:
        try:
            if not bucket_name:
                raise SDKError("empty bucket name")
                
            if not file_name:
                raise SDKError("empty file name")
            
            if not self.client:
                raise SDKError("IPC client has not been initialized.")
                
            request = ipcnodeapi_pb2.IPCFileDownloadCreateRequest(
                bucket_name=bucket_name,
                file_name=file_name,
                address=self.ipc.auth.address
            )
            
            response = self.client.FileDownloadCreate(request)
            
            chunks = []
            for chunk in response.chunks:
                chunks.append(Chunk(
                    cid=chunk.cid,
                    encoded_size=chunk.encoded_size,
                    size=chunk.size,
                    index=chunk.index
                ))
            
            return IPCFileDownload(
                bucket_name=response.bucket_name,
                name=file_name,
                chunks=chunks
            )
        except Exception as err:
            raise SDKError(f"failed to create file download: {str(err)}")
            

    def download(
        self, ctx: Any, file_download: IPCFileDownload, writer: io.IOBase
    ) -> None:
        try:
            file_enc_key = encryption_key(self.encryption_key, file_download.bucket_name, file_download.name)
            for chunk in file_download.chunks:
                if hasattr(ctx, "done") and ctx.done():
                    raise SDKError("context cancelled")
                chunk_dl = self.create_chunk_download(ctx, file_download.bucket_name, file_download.name, chunk)
                self.download_chunk_blocks(
                    ctx,
                    file_download.bucket_name,
                    file_download.name,
                    self.ipc.auth.address,
                    chunk_dl,
                    file_enc_key,
                    writer
                )
        except Exception as err:
            raise SDKError(f"failed to download file: {str(err)}")



    def create_chunk_download(
        self, ctx: Any, bucket_name: str, file_name: str, chunk: Chunk
    ) -> FileChunkDownload:
        request = ipcnodeapi_pb2.IPCFileDownloadChunkCreateRequest(
            bucket_name=bucket_name,
            file_name=file_name,
            chunk_cid=chunk.cid,
            address=self.ipc.auth.address.lower()
        )
        response = self.client.FileDownloadChunkCreate(request)
        blocks: List[FileBlockDownload] = []
        for b in response.blocks:
            blocks.append(FileBlockDownload(
                cid=b.cid,
                data=b"" ,
                akave=AkaveBlockData(
                    permit=b.permit,
                    node_address=b.node_address,
                    node_id=b.node_id
                ) if b.node_id else None,
                filecoin=None
            ))
        return FileChunkDownload(
            cid=chunk.cid,
            index=chunk.index,
            encoded_size=chunk.encoded_size,
            size=chunk.size,
            blocks=blocks
        )
            

    def download_chunk_blocks(
        self,
        ctx: Any,
        bucket_name: str,
        file_name: str,
        address: str,
        chunk_download: FileChunkDownload,
        file_encryption_key: bytes,
        writer: io.IOBase
    ) -> None:
        pool = ConnectionPool()
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_concurrency) as executor:
                futures: Dict[concurrent.futures.Future, int] = {}
                for i, block in enumerate(chunk_download.blocks):
                    futures[executor.submit(
                        self.fetch_block_data,
                        ctx, pool,
                        chunk_download.cid,
                        bucket_name,
                        file_name,
                        address,
                        chunk_download.index,
                        i,
                        block
                    )] = i
                blocks: List[Optional[bytes]] = [None for _ in range(len(chunk_download.blocks))]
                for fut in concurrent.futures.as_completed(futures):
                    idx = futures[fut]
                    data = fut.result()
                    blocks[idx] = extract_block_data(chunk_download.blocks[idx].cid, data)
            # assemble
            if self.erasure_code:
                # Filter out None values and concatenate blocks for erasure code
                non_null_blocks = [b for b in blocks if b is not None]
                encoded_data = b"".join(non_null_blocks)
                data = self.erasure_code.extract_data(encoded_data, int(chunk_download.size))
            else:
                data = b"".join(b for b in blocks if b is not None)
            if file_encryption_key:
                data = decrypt(file_encryption_key, data, str(chunk_download.index).encode())
            writer.write(data)
        finally:
            pool.close()
    
    