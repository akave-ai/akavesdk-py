from web3 import Web3

mock_abi = [{
    "inputs": [],
    "name": "getFileIndexById",
    "outputs": [{"internalType": "uint256", "name": "index", "type": "uint256"}],
    "stateMutability": "view",
    "type": "function"
}]
w3 = Web3()
try:
    contract = w3.eth.contract(address="0x0000000000000000000000000000000000000000", abi=mock_abi)
    print("Contract initialized")
except Exception as e:
    print(e)
