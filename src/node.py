import asyncio
import json
from dataclasses import dataclass
from typing import Dict

from src.ledger import Ledger
from src.transaction import Transaction
from src.block import Block

# A constant for the reward given to a validator for forging a block
VALIDATOR_REWARD = 100.0

@dataclass
class Peer:
    """Represents a connected peer in the network."""
    writer: asyncio.StreamWriter

    @property
    def address(self) -> str:
        """Returns the peer's address as a 'host:port' string."""
        peername = self.writer.get_extra_info('peername')
        return f"{peername[0]}:{peername[1]}" if peername else "unknown"

class Node:
    """
    Represents a node in the Sovereign Ledger Protocol network.
    A node manages its own copy of the ledger, communicates with peers,
    and participates in the consensus process.
    """
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.ledger = Ledger()
        # Peers are stored by their 'host:port' address string
        self.peers: Dict[str, Peer] = {}

    @property
    def address(self) -> str:
        """Returns the node's own address as a 'host:port' string."""
        return f"{self.host}:{self.port}"

    def __repr__(self) -> str:
        return f"Node(address='{self.address}', peers={list(self.peers.keys())})"

    def select_validator(self) -> str:
        """
        Selects the validator for the next block using a deterministic round-robin algorithm.

        This is the hook for the Proof of Architecture consensus. In a more advanced
        implementation, this method would weigh nodes based on their PoA score rather
        than using a simple round-robin.
        """
        # Get a sorted list of all known participants (self + peers) to ensure determinism.
        participant_addresses = sorted(list(self.peers.keys()) + [self.address])

        if not participant_addresses:
            # This should not happen in a running network, but as a safeguard:
            return self.address

        # The round is determined by the current length of the chain.
        chain_length = len(self.ledger.chain)

        # The validator is chosen using a deterministic round-robin algorithm.
        validator_index = chain_length % len(participant_addresses)

        return participant_addresses[validator_index]

    async def start_server(self):
        """
        Starts the node's server to listen for incoming connections from peers.
        """
        server = await asyncio.start_server(
            self.handle_connection, self.host, self.port
        )
        print(f"Node server listening on {self.address}")
        async with server:
            await server.serve_forever()

    async def broadcast(self, message: dict, exclude_peer_address: str = None):
        """
        Broadcasts a message to all connected peers, except the one specified by address.
        """
        message_json = json.dumps(message) + '\n'
        message_bytes = message_json.encode()

        disconnected_peers = []
        for peer_address, peer in self.peers.items():
            if peer_address == exclude_peer_address:
                continue

            if peer.writer.is_closing():
                disconnected_peers.append(peer_address)
                continue

            try:
                peer.writer.write(message_bytes)
                await peer.writer.drain()
            except ConnectionError as e:
                print(f"Failed to send message to peer {peer_address}: {e}. Marking for removal.")
                disconnected_peers.append(peer_address)

        for address in disconnected_peers:
            if address in self.peers:
                del self.peers[address]

    async def handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """
        Handles a connection from a peer, processing incoming messages and driving ledger operations.
        """
        peer = Peer(writer=writer)
        peer_address = peer.address

        if not peer_address or peer_address == "unknown":
            print("Could not determine peer address. Closing connection.")
            writer.close()
            await writer.wait_closed()
            return

        print(f"Managing connection with {peer_address}")
        self.peers[peer_address] = peer

        try:
            while True:
                data = await reader.readline()
                if not data:
                    break

                message_str = data.decode().strip()
                if not message_str:
                    continue

                try:
                    message = json.loads(message_str)
                    message_type = message.get('type')

                    if message_type == 'NEW_TRANSACTION':
                        tx_data = message['payload']
                        if all(k in tx_data for k in ['sender', 'recipient', 'amount']):
                            transaction = Transaction(sender=tx_data['sender'], recipient=tx_data['recipient'], amount=float(tx_data['amount']))
                            self.ledger.add_transaction(transaction)
                            print(f"Added transaction from network: {transaction.to_dict()}")
                            await self.broadcast(message, exclude_peer_address=peer_address)
                        else:
                            print("Received invalid transaction payload.")

                    elif message_type == 'NEW_BLOCK':
                        await self.handle_new_block(message['payload'], peer_address)

                    else:
                        print(f"Received unhandled message type: {message_type}")
                        await self.broadcast(message, exclude_peer_address=peer_address)

                except json.JSONDecodeError:
                    print(f"Received invalid JSON from {peer_address}: {message_str}")
                except Exception as e:
                    print(f"Error processing message from {peer_address}: {e}")

        except (ConnectionResetError, BrokenPipeError, asyncio.IncompleteReadError) as e:
            print(f"Connection with {peer_address} lost: {e}")
        finally:
            print(f"Closing connection with {peer_address}")
            if peer_address in self.peers:
                del self.peers[peer_address]
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()

    async def create_and_broadcast_transaction(self, sender: str, recipient: str, amount: float):
        """
        Creates a new transaction, adds it to the local ledger, and broadcasts it.
        """
        transaction = Transaction(sender=sender, recipient=recipient, amount=amount)
        self.ledger.add_transaction(transaction)

        message = {
            'type': 'NEW_TRANSACTION',
            'payload': transaction.to_dict()
        }
        print(f"Created and broadcasting new transaction: {transaction.to_dict()}")
        await self.broadcast(message)

    async def propose_and_broadcast_block(self):
        """
        Checks if this node is the current validator. If so, it forges a new block,
        including a reward transaction, and broadcasts it to the network.
        """
        validator_address = self.select_validator()

        if validator_address != self.address:
            print(f"I am not the validator for the next block. Validator is {validator_address}.")
            return None

        print(f"I am the validator. Proposing a new block...")

        # Create the reward transaction for the validator (this node)
        reward_transaction = Transaction(
            sender="NETWORK_REWARD",
            recipient=self.address,
            amount=VALIDATOR_REWARD
        )

        # The transactions for the new block include pending ones and the reward
        transactions_for_block = self.ledger.pending_transactions + [reward_transaction]

        # Create the new block
        new_block = Block(
            transactions=transactions_for_block,
            previous_hash=self.ledger.last_block.hash,
            validator_address=self.address
        )

        # Add the new block to our own ledger
        self.ledger.add_block(new_block)
        print(f"Successfully forged and added new block: {new_block.hash}")

        # If the block was added successfully, clear the pending transactions
        self.ledger.pending_transactions = []

        # Broadcast the new block to the network
        block_data = new_block.to_dict()
        block_data['hash'] = new_block.hash
        message = {
            'type': 'NEW_BLOCK',
            'payload': block_data
        }
        await self.broadcast(message)
        return new_block

    async def handle_new_block(self, block_data: dict, source_peer_address: str):
        """
        Handles a new block received from the network, performing full validation
        before adding it to the ledger and re-broadcasting.
        """
        print(f"Received new block for validation: {block_data.get('hash')[:12]}...")

        try:
            new_block = Block.from_dict(block_data)

            # 1. Authenticity Check: Does the hash match the content?
            if new_block.hash != new_block.compute_hash():
                print(f"Validation failed: Block hash is incorrect.")
                return

            # 2. Integrity Check: Does it connect to our chain?
            if new_block.previous_hash != self.ledger.last_block.hash:
                print(f"Validation failed: Previous hash does not match our chain.")
                return

            # 3. Authority Check: Was it created by the correct validator?
            expected_validator = self.select_validator()
            if new_block.validator_address != expected_validator:
                print(f"Validation failed: Incorrect validator. Expected {expected_validator}, got {new_block.validator_address}.")
                return

            # All checks passed. Add the block to our ledger.
            self.ledger.add_block(new_block)
            print(f"Successfully validated and added new block: {new_block.hash}")

            # Clear our pending transactions that are now confirmed in this block
            self.ledger.pending_transactions = [
                tx for tx in self.ledger.pending_transactions
                if tx not in new_block.transactions
            ]

            # Gossip the valid block to our peers.
            message = {'type': 'NEW_BLOCK', 'payload': block_data}
            await self.broadcast(message, exclude_peer_address=source_peer_address)

        except Exception as e:
            print(f"Error validating new block: {e}")

    async def connect_to_peer(self, host: str, port: int):
        """
        Establishes an outbound connection to another node.
        """
        try:
            print(f"Attempting to connect to peer {host}:{port}...")
            reader, writer = await asyncio.open_connection(host, port)
            # We successfully connected, now we handle the connection
            # just like we would an incoming one.
            # We run this in a separate task so it doesn't block.
            asyncio.create_task(self.handle_connection(reader, writer))
            print(f"Successfully connected to peer {host}:{port}")
        except ConnectionRefusedError:
            print(f"Connection to peer {host}:{port} refused. Is the remote node running?")
        except Exception as e:
            print(f"Failed to connect to peer {host}:{port}. Error: {e}")
