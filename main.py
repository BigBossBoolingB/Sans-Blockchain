from src.ledger import Ledger
from src.transaction import Transaction

def run_demonstration():
    """
    Runs a demonstration of the Sovereign Ledger Protocol, showcasing its core functionalities.
    """
    # --- 1. Initialize the Ledger ---
    print("Initializing the Sovereign Ledger Protocol...")
    slp_ledger = Ledger()
    print(f"Ledger initialized. Genesis Block created: {slp_ledger.chain[0]}")
    print("-" * 30)

    # --- 2. Add Transactions ---
    print("Adding transactions to the pending pool...")
    tx1 = Transaction(sender="address-alice", recipient="address-bob", amount=10.5)
    slp_ledger.add_transaction(tx1)
    print(f"Added transaction: {tx1}")

    tx2 = Transaction(sender="address-bob", recipient="address-charlie", amount=5.0)
    slp_ledger.add_transaction(tx2)
    print(f"Added transaction: {tx2}")

    print(f"\nCurrent Ledger state: {slp_ledger}")
    print("-" * 30)

    # --- 3. Mine a New Block ---
    print("Mining a new block to process pending transactions...")
    mined_block = slp_ledger.mine_pending_transactions()
    print(f"New block mined successfully: {mined_block}")
    print(f"\nUpdated Ledger state: {slp_ledger}")
    print("Chain length:", len(slp_ledger.chain))
    print("-" * 30)

    # --- 4. Verify the Chain's Integrity ---
    print("Verifying the integrity of the ledger...")
    is_valid = slp_ledger.is_chain_valid()
    print(f"Is the ledger valid? {is_valid}")
    if is_valid:
        print("Ledger integrity confirmed.")
    else:
        print("Ledger integrity compromised!")
    print("-" * 30)

    # --- 5. Demonstrate Tamper-Proofing ---
    print("Attempting to tamper with the ledger...")
    try:
        # The tampered block is the second in the chain (index 1)
        tampered_block = slp_ledger.chain[1]
        print(f"Original first transaction in block 1: {tampered_block.transactions[0]}")

        # A malicious actor tries to change the transaction amount after mining
        tampered_block.transactions[0].amount = 1000.0
        print(f"Tampered first transaction in block 1: {tampered_block.transactions[0]}")

        print(f"Original block hash: {tampered_block.hash}")
        print(f"Hash after tampering: {tampered_block.compute_hash()}")

        print("\nRe-verifying the ledger after tampering...")
        is_valid_after_tamper = slp_ledger.is_chain_valid()
        print(f"Is the ledger still valid? {is_valid_after_tamper}")
        if not is_valid_after_tamper:
            print("Success! Tampering was detected, as expected.")
        else:
            print("Failure! Tampering was not detected.")

    except IndexError:
        print("Could not perform tamper test: chain is not long enough.")

    print("-" * 30)
    print("Demonstration complete.")

if __name__ == "__main__":
    run_demonstration()
