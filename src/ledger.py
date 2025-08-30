from typing import List
from src.block import Block
from src.transaction import Transaction

class Ledger:
    """
    Manages the chain of blocks (the ledger) for the Sovereign Ledger Protocol.
    It handles the creation of new blocks, storing transactions, and maintaining the chain's integrity.
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
        genesis_block = Block(transactions=[], previous_hash="0")
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
        These transactions will be included in the next block to be created.
        """
        self.pending_transactions.append(transaction)

    def mine_pending_transactions(self) -> Block:
        """
        Mines a new block, processes all pending transactions, and adds it to the chain.
        This method orchestrates the creation and validation of a new block.
        """
        new_block = Block(
            transactions=self.pending_transactions,
            previous_hash=self.last_block.hash
        )

        # This is the crucial step where the consensus mechanism comes into play.
        self.validate_and_add_block(new_block)

        self.pending_transactions = []  # Reset pending transactions
        return new_block

    def validate_and_add_block(self, block: Block):
        """
        Validates a new block and adds it to the chain.

        --- FOUNDATION FOR PROOF OF ARCHITECTURE (PoA) ---
        This method serves as the placeholder for the Sovereign Ledger Protocol's
        Proof of Architecture (PoA) consensus mechanism.

        In a future implementation, this function will contain logic to verify a block
        not based on computational work (like Proof of Work), but on the validating
        node's proven, useful contributions to the network's health and integrity.

        For now, it performs a simple validation of the previous hash.
        """
        if block.previous_hash != self.last_block.hash:
            raise ValueError("Block validation failed: Previous hash does not match.")

        # In a full PoA implementation, more complex validation would occur here,
        # e.g., checking the validator's credentials, contribution score, etc.

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
