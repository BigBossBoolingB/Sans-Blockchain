import asyncio
import json
from typing import Set

from src.ledger import Ledger
from src.transaction import Transaction

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
        # The peers are stored as writer objects from asyncio streams
        self.peers: Set[asyncio.StreamWriter] = set()

    def __repr__(self) -> str:
        # Get peer addresses for a more informative representation
        peer_addresses = [f"{writer.get_extra_info('peername')[0]}:{writer.get_extra_info('peername')[1]}" for writer in self.peers]
        return f"Node(host='{self.host}', port={self.port}, peers={peer_addresses})"

    async def start_server(self):
        """
        Starts the node's server to listen for incoming connections from peers.
        """
        server = await asyncio.start_server(
            self.handle_connection, self.host, self.port
        )
        addr = server.sockets[0].getsockname()
        print(f"Node server listening on {addr}")
        async with server:
            await server.serve_forever()

    async def broadcast(self, message: dict, exclude_peer: asyncio.StreamWriter = None):
        """
        Broadcasts a message to all connected peers, except the one specified.
        """
        message_json = json.dumps(message) + '\n'
        message_bytes = message_json.encode()

        disconnected_peers = []
        for peer_writer in self.peers:
            if peer_writer == exclude_peer:
                continue

            if peer_writer.is_closing():
                disconnected_peers.append(peer_writer)
                continue

            try:
                peer_writer.write(message_bytes)
                await peer_writer.drain()
            except ConnectionError as e:
                print(f"Failed to send message to a peer: {e}. Marking for removal.")
                disconnected_peers.append(peer_writer)

        # Clean up peers that were found to be disconnected
        for peer in disconnected_peers:
            if peer in self.peers:
                self.peers.remove(peer)

    async def handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """
        Handles a connection from a peer, processing incoming messages and driving ledger operations.
        """
        addr = writer.get_extra_info('peername')
        print(f"Managing connection with {addr}")
        self.peers.add(writer)

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
                        # Basic validation to prevent malformed data
                        if all(k in tx_data for k in ['sender', 'recipient', 'amount']):
                            transaction = Transaction(sender=tx_data['sender'], recipient=tx_data['recipient'], amount=float(tx_data['amount']))
                            # TODO: Add more robust validation (e.g., check if tx already exists)
                            self.ledger.add_transaction(transaction)
                            print(f"Added transaction from network: {transaction.to_dict()}")
                            await self.broadcast(message, exclude_peer=writer)
                        else:
                            print("Received invalid transaction payload.")

                    elif message_type == 'NEW_BLOCK':
                        # In a real system, we'd deserialize, validate the block, and check PoA.
                        print("Received new block announcement from network.")
                        await self.broadcast(message, exclude_peer=writer)

                    else:
                        print(f"Received unhandled message type: {message_type}")
                        # Still gossip unknown message types for future compatibility
                        await self.broadcast(message, exclude_peer=writer)

                except json.JSONDecodeError:
                    print(f"Received invalid JSON from {addr}: {message_str}")
                except Exception as e:
                    print(f"Error processing message from {addr}: {e}")

        except (ConnectionResetError, BrokenPipeError, asyncio.IncompleteReadError) as e:
            print(f"Connection with {addr} lost: {e}")
        finally:
            print(f"Closing connection with {addr}")
            if writer in self.peers:
                self.peers.remove(writer)
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

    async def mine_and_broadcast_block(self):
        """
        Mines a new block from pending transactions and broadcasts the announcement.
        """
        if not self.ledger.pending_transactions:
            print("No pending transactions to mine.")
            return None

        new_block = self.ledger.mine_pending_transactions()
        print(f"Mined new block locally: {new_block.hash}")

        message = {
            'type': 'NEW_BLOCK',
            'payload': {
                'hash': new_block.hash,
                'timestamp': new_block.timestamp,
                'transaction_count': len(new_block.transactions)
            }
        }
        await self.broadcast(message)
        return new_block

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
