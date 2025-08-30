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
    def __init__(self, transactions: List[Transaction], previous_hash: str, validator_address: str, nonce: int = 0):
        self.timestamp = time.time()
        self.transactions = transactions
        self.previous_hash = previous_hash
        self.validator_address = validator_address
        self.nonce = nonce
        self.hash = self.compute_hash()

    def __repr__(self) -> str:
        return (f"Block(validator='{self.validator_address}', transactions={len(self.transactions)}, "
                f"hash='{self.hash}')")

    def compute_hash(self) -> str:
        """
        Computes the SHA-256 hash of the block.
        The block's data is first serialized to a JSON string to ensure consistency.
        """
        block_dict = self.to_dict()
        # Using separators=(',', ':') removes whitespace for a more compact representation.
        block_string = json.dumps(block_dict, sort_keys=True, separators=(',', ':'))
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
            'validator_address': self.validator_address,
            'nonce': self.nonce,
        }

    @classmethod
    def from_dict(cls, data: dict):
        """
        Creates a Block object from a dictionary representation.
        """
        # Deserialize the transactions within the block
        transactions = [Transaction.from_dict(tx_data) for tx_data in data['transactions']]

        # Create the block instance
        block = cls(
            transactions=transactions,
            previous_hash=data['previous_hash'],
            validator_address=data['validator_address'],
            nonce=data['nonce']
        )
        # Manually set the timestamp and hash to match the original block's data
        block.timestamp = data['timestamp']
        block.hash = data['hash']
        return block
