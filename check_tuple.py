from web3 import Web3
from hexbytes import HexBytes

mock_abi = [{
    "inputs": [
        {
            "internalType": "string",
            "name": "name",
            "type": "string"
        },
        {
            "internalType": "bytes32",
            "name": "fileId",
            "type": "bytes32"
        }
    ],
    "name": "getFileIndexById",
    "outputs": [
        {
            "internalType": "uint256",
            "name": "index",
            "type": "uint256"
        }
    ],
    "stateMutability": "view",
    "type": "function"
}]
w3 = Web3()
contract = w3.eth.contract(address="0x0000000000000000000000000000000000000000", abi=mock_abi)
print("Contract outputs for single return:", contract.functions.getFileIndexById("test", b'0'*32).abi["outputs"])
