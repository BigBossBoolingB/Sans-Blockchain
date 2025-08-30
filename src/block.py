import hashlib
import json
import time
from typing import List
from src.transaction import Transaction

class Block:
    """
    Represents a block in the Sovereign Ledger Protocol.
    A block contains a set of transactions, a timestamp, and cryptographic hashes.
    """
    def __init__(self, transactions: List[Transaction], previous_hash: str, nonce: int = 0):
        self.timestamp = time.time()
        self.transactions = transactions
        self.previous_hash = previous_hash
        self.nonce = nonce
        self.hash = self.compute_hash()

    def __repr__(self) -> str:
        return (f"Block(timestamp={self.timestamp}, transactions={len(self.transactions)}, "
                f"previous_hash='{self.previous_hash}', hash='{self.hash}')")

    def compute_hash(self) -> str:
        """
        Computes the SHA-256 hash of the block.
        The block's data is first serialized to a JSON string to ensure consistency.
        """
        # We must use a representation of the transactions that is stable.
        # The to_dict() method in Transaction helps with this.
        block_dict = self.to_dict()
        block_string = json.dumps(block_dict, sort_keys=True)
        return hashlib.sha256(block_string.encode()).hexdigest()

    def to_dict(self) -> dict:
        """
        Serializes the block into a dictionary, excluding the hash itself.
        This dictionary is the basis for the block's hash.
        """
        return {
            'timestamp': self.timestamp,
            'transactions': [tx.to_dict() for tx in self.transactions],
            'previous_hash': self.previous_hash,
            'nonce': self.nonce,
        }
