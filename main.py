import asyncio
import argparse
from src.node import Node

async def run_node_ui(node: Node, initial_peers: list):
    """A simple command-line interface to interact with the node."""
    # Give the server a moment to start up
    await asyncio.sleep(1)

    # Connect to initial seed peers provided via command line
    if initial_peers:
        print(f"Connecting to seed peers: {initial_peers}...")
        for peer in initial_peers:
            try:
                host, port_str = peer.split(':')
                port = int(port_str)
                await node.connect_to_peer(host, port)
            except ValueError:
                print(f"Invalid peer format: {peer}. Should be host:port.")
        print("-" * 30)

    while True:
        print("\n--- Sovereign Ledger Protocol Node CLI ---")
        print("1. Create and Broadcast Transaction")
        print("2. Propose a New Block")
        print("3. View Next Validator")
        print("4. Print Ledger")
        print("5. View Trust Ledger")
        print("6. List Peers")
        print("7. Exit")

        try:
            choice = await asyncio.to_thread(input, "Choose an option: ")
        except (EOFError, KeyboardInterrupt):
            choice = '7'

        if choice == '1':
            try:
                sender = await asyncio.to_thread(input, "  Sender address: ")
                recipient = await asyncio.to_thread(input, "  Recipient address: ")
                amount_str = await asyncio.to_thread(input, "  Amount: ")
                amount = float(amount_str)
                await node.create_and_broadcast_transaction(sender, recipient, amount)
            except ValueError:
                print("Error: Invalid amount. Please enter a number.")
            except Exception as e:
                print(f"An error occurred: {e}")

        elif choice == '2':
            new_block = await node.propose_and_broadcast_block()
            if new_block:
                print("Block proposal successful.")
            else:
                print("Block proposal failed (perhaps you are not the validator?).")

        elif choice == '3':
            validator = node.select_validator()
            print(f"\n>>> Validator for block height {len(node.ledger.chain)} is: {validator}")
            if validator == node.address:
                print(">>> That's this node! You can propose the next block.")

        elif choice == '4':
            print("\n" + "="*15 + " Current Ledger " + "="*15)
            for i, block in enumerate(node.ledger.chain):
                print(f"Block {i} | Validator: {block.validator_address} | Hash: {block.hash[:12]}...")
                for tx in block.transactions:
                    print(f"  -> {tx}")
            print("="*60)

        elif choice == '5':
            print("\n" + "="*15 + " Trust Ledger " + "="*15)
            # Sort the ledger by score for readability
            sorted_ledger = sorted(node.trust_ledger.items(), key=lambda item: item[1], reverse=True)
            for address, score in sorted_ledger:
                print(f"  - {address}: {score:.2f}")
            print("="*42)

        elif choice == '6':
            print("\n" + "="*15 + " Known Peers " + "="*15)
            print(node)
            print("="*41)

        elif choice == '7':
            print("Shutting down node...")
            break
        else:
            print("Invalid choice. Please try again.")

async def main():
    """Main function to set up and run the SLP node."""
    parser = argparse.ArgumentParser(description="Run a node for the Sovereign Ledger Protocol.")
    parser.add_argument('--host', type=str, default='127.0.0.1', help='Host to listen on.')
    parser.add_argument('--port', type=int, required=True, help='Port to listen on.')
    parser.add_argument('--peers', nargs='*', default=[], help='List of seed peers to connect to (e.g., 127.0.0.1:8889).')
    args = parser.parse_args()

    node = Node(host=args.host, port=args.port)

    server_task = asyncio.create_task(node.start_server())
    ui_task = asyncio.create_task(run_node_ui(node, args.peers))

    await ui_task

    server_task.cancel()
    try:
        await server_task
    except asyncio.CancelledError:
        print("Server task has been cancelled successfully.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nCaught KeyboardInterrupt, initiating shutdown.")
    finally:
        print("Node shutdown complete.")
