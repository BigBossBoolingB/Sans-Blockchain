import time
from typing import Any

class Transaction:
    """
    Represents a transaction in the Sovereign Ledger Protocol.
    A transaction includes a sender, recipient, amount, and is timestamped.
    The signature is included as a placeholder for future cryptographic verification.
    """
    def __init__(self, sender: str, recipient: str, amount: float, signature: Any = None):
        self.sender = sender
        self.recipient = recipient
        self.amount = amount
        self.timestamp = time.time()
        self.signature = signature

    def __repr__(self) -> str:
        return (f"Transaction(sender='{self.sender}', recipient='{self.recipient}', "
                f"amount={self.amount}, timestamp={self.timestamp})")

    def to_dict(self) -> dict:
        """
        Serializes the transaction into a dictionary for hashing or transmission.
        """
        # The signature is omitted for now as it's a placeholder.
        # In a real implementation, the signed transaction data would be hashed.
        return {
            'sender': self.sender,
            'recipient': self.recipient,
            'amount': self.amount,
            'timestamp': self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: dict):
        """
        Creates a Transaction object from a dictionary representation.
        """
        # We create a new transaction object and then manually set the timestamp
        # because the __init__ method automatically sets it to the current time.
        tx = cls(
            sender=data['sender'],
            recipient=data['recipient'],
            amount=data['amount']
        )
        tx.timestamp = data['timestamp']
        # Signature handling would go here in a real implementation
        return tx
