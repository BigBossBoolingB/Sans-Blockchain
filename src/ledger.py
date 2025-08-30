from typing import List
from src.block import Block
from src.transaction import Transaction

class Ledger:
    """
    Manages the chain of blocks (the ledger) for the Sovereign Ledger Protocol.
    It is primarily a data structure, with validation logic handled by the Node.
    """
    def __init__(self):
        self.chain: List[Block] = []
        self.pending_transactions: List[Transaction] = []
        # Create the genesis block upon initialization
        self.create_genesis_block()

    def __repr__(self) -> str:
        return f"Ledger(chain_length={len(self.chain)}, pending_transactions={len(self.pending_transactions)})"

    def create_genesis_block(self):
        """
        Creates the very first block in the chain, the "genesis block".
        This block has no transactions and a 'previous_hash' of "0".
        """
        # A validator address is required, for genesis we can use a system address
        genesis_block = Block(
            transactions=[],
            previous_hash="0",
            validator_address="SYSTEM_GENESIS"
        )
        self.chain.append(genesis_block)

    @property
    def last_block(self) -> Block:
        """
        Returns the most recent block in the chain.
        """
        return self.chain[-1]

    def add_transaction(self, transaction: Transaction):
        """
        Adds a new transaction to the list of pending transactions.
        """
        self.pending_transactions.append(transaction)

    def add_block(self, block: Block):
        """
        Adds a block to the chain. Assumes the block has already been validated
        by the Node according to consensus rules.
        """
        self.chain.append(block)

    def is_chain_valid(self) -> bool:
        """
        Determines if the entire ledger is valid by checking the integrity of each block's hash
        and its link to the previous block.
        """
        for i in range(1, len(self.chain)):
            current_block = self.chain[i]
            previous_block = self.chain[i-1]

            # 1. Verify the hash of the current block is correct
            if current_block.hash != current_block.compute_hash():
                print(f"Data integrity compromised: The hash of block {i} is invalid.")
                return False

            # 2. Verify the link to the previous block
            if current_block.previous_hash != previous_block.hash:
                print(f"Chain link broken: Block {i}'s previous_hash does not match the hash of block {i-1}.")
                return False

        return True
