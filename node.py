import socket
import threading
import time
import json
import sys
import random
import os
from config import NODES, ELECTION_TIMEOUT, HEARTBEAT_INTERVAL, CLUSTER_A_NODES, CLUSTER_B_NODES

class Node:
    """
    Represents a node in the Raft consensus algorithm. Implements leader election, log replication, and consensus mechanisms. Also fulfills all the scenarios defined in the assignment.
    """
    def __init__(self, name):
        self.name = name # Unique identifier for the node
        self.ip = NODES[self.name]['ip'] # Node's IP address. This ise defined in the config file
        self.port = NODES[self.name]['port'] # Node's port number. This is defined in the config file
        # Raft state
        self.state = 'Follower' # Current state (Follower/Candidate/Leader)
        self.current_term = 0 # Current term number
        self.voted_for = None # Candidate voted for in current term. Initialized to None
        self.log = []  # [{'term': term, 'value': value, 'index': index}]
        # For log management
        self.commit_index = -1 # Index of highest log entry known to be committed
        self.last_applied = -1 # Index of highest log entry applied to state machine
        self.next_index = {} # For each server, index of next log entry to send
        self.match_index = {}  # For each server, index of highest log entry known to be replicated
        # Leadership attributes
        self.leader_id = None # Current leader's ID. Every node MUST know who's the current leader
        self.election_timer = None # Timer for election timeout
        self.heartbeat_timer = None 
        self.server_socket = None # Server socket for accepting connections
        self.running = True
        self.recovering = False # Flag indicating if node is in recovery
        self.last_recovery_attempt = 0
        self.recovery_timeout = 5  # seconds between recovery attempts
        # Thread safety
        self.lock = threading.Lock() # Lock for thread-safe operations
        self.simulating_crash_ongoing = False  # Flag to prevent multiple crash simulations or exceptions

        # Persistent storage
        self.log_filename = f"{self.name}_lab2Raft.txt"  # File for persistent log storage
        ## Initialize or load persistent log
        if not os.path.exists(self.log_filename):
            open(self.log_filename, 'w').close()   # Create empty log file if doesn't exist
        else:
            self.load_persistent_log()             # Load existing log entries

        self.cluster_name = self._determine_cluster()
        self.cluster_nodes = self._get_cluster_nodes()

    def _determine_cluster(self):
        """Determines which RAFT cluster this node belongs to."""
        if self.name.startswith('nodeA'):
            return 'A'
        elif self.name.startswith('nodeB'):
            return 'B'
        return None

    def _get_cluster_nodes(self):
        """Returns the nodes in this node's RAFT cluster."""
        if self.cluster_name == 'A':
            return CLUSTER_A_NODES
        elif self.cluster_name == 'B':
            return CLUSTER_B_NODES
        return {}    

    def start(self):
        """
        Initializes and starts the node's main operation loop.
        Handles the node's state machine and election timeout monitoring.
        """
        
        print(f"[{self.name}] Starting node...") # Announce node startup

        # Initialize and start server thread for handling incoming connections
        server_thread = threading.Thread(target=self.run_server)
        server_thread.daemon = True  # Thread will terminate when main program exits
        server_thread.start()
        
        # Initialize election timeout
        self.reset_election_timer()
        print(f"[{self.name}] Initial election timeout: {self.election_timer}")

        # Main operation loop
        while self.running:
            with self.lock:  # Thread-safe state access
                current_state = self.state
                current_timer = self.election_timer

            if current_state == 'Leader':
                # Leader responsibilities: send heartbeats
                self.send_heartbeats()
                time.sleep(HEARTBEAT_INTERVAL)  # Wait before next heartbeat
            else:
                # Follower/Candidate responsibilities: monitor election timeout
                if current_timer <= 0:
                    with self.lock:
                        if self.state != 'Leader':  # Double-check state hasn't changed
                            self.start_election()
                time.sleep(0.1)  # Small sleep to prevent CPU overuse
                with self.lock:
                    self.election_timer -= 0.1  # Decrement election timer

    def run_server(self):
        """
        Sets up and runs the TCP server that handles incoming RPC requests.
        Runs in a separate thread to handle concurrent connections.
        """
        
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # Initialize TCP socket
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # Enable address reuse to prevent "Address already in use" errors
        # Bind socket to node's IP and port and start listening
        self.server_socket.bind((self.ip, self.port)) 
        self.server_socket.listen(5)
        
        print(f"[{self.name}] Server listening at {self.ip}:{self.port}")

        # Main loop to keep the server running
        while self.running:
            try:
                # Accept incoming connection
                client_socket, _ = self.server_socket.accept()
                # Create new thread for each client connection
                client_thread = threading.Thread(target=self.handle_client_connection, args=(client_socket,))
                client_thread.daemon = True  # Thread will terminate when main program exits
                client_thread.start()
            except Exception as e:
                if self.running and self.simulating_crash_ongoing == False:
                    print(f"[{self.name}] Server error: {e}")

    def load_persistent_log(self):
        """
        Loads existing log entries from persistent storage.
        Reconstructs the log state from disk, maintaining persistence across restarts.
        """
        try:
            with open(self.log_filename, 'r') as f:
                lines = f.readlines()
                # Reconstruct log entries from stored values
                for idx, line in enumerate(lines):
                    entry = {
                        'term': self.current_term,  # Use current term as we don't store terms
                        'value': line.strip(),      # Remove any whitespace/newlines
                        'index': idx                # Maintain original entry ordering
                    }
                    self.log.append(entry)
                
                # If log exists, update indices to match loaded state
                if self.log:
                    self.commit_index = len(self.log) - 1
                    self.last_applied = self.commit_index
                    print(f"[{self.name}] Loaded {len(self.log)} entries from persistent storage")
        except FileNotFoundError:
            print(f"[{self.name}] No existing log found, starting fresh")

    def handle_client_connection(self, client_socket: socket.socket):
        """
        Mananges incoming client connections and RPC requests. It takes a client socket as input.
        Processes different types of RPCs and returns appropriate responses.
        """
        try:
            
            data = client_socket.recv(4096).decode() # Receive and decode client request
            if data:
                request = json.loads(data) # Parse JSON request
                rpc_type = request['rpc_type']
                response = {}

                # Manange different RPC types with thread safety
                with self.lock:
                    if rpc_type == 'RequestVote':
                        # Handle voting requests during leader election
                        response = self.handle_request_vote(request['data'])
                    elif rpc_type == 'AppendEntries':
                        # Handle log replication and heartbeat messages
                        response = self.handle_append_entries(request['data'])
                    elif rpc_type == 'SubmitValue':
                        # Handle client value submissions
                        response = self.handle_client_submit(request['data'])
                    elif rpc_type == 'TriggerLeaderChange':
                        # Handle manual leader step-down requests
                        response = self.trigger_leader_change()
                    elif rpc_type == 'SimulateCrash':
                        # Handle crash simulation requests
                        response = self.simulate_crash()
                    elif rpc_type == 'PrintLog':
                        # Handle print log request
                        response = self.print_node_log()
                    else:
                        response = {'error': 'Unknown RPC type'}

                # Send response back to client
                client_socket.sendall(json.dumps(response).encode())
        except Exception as e:
            print(f"[{self.name}] Error handling client connection: {e}")
        finally:
            client_socket.close()  # Ensure socket is closed even if an error occurs

    def reset_election_timer(self):
        """
        Resets the election timeout to a random value within the configured range.
        Randomization helps prevent election conflicts.
        """
        self.election_timer = random.uniform(*ELECTION_TIMEOUT)
        
    def print_node_log(self):
        """
        Prints the node's current log entries and state.
        """
        print(f"\n[{self.name}] Current Log State:")
        print(f"[{self.name}] Role: {self.state}")
        print(f"[{self.name}] Term: {self.current_term}")
        print(f"[{self.name}] Commit Index: {self.commit_index}")
        print(f"[{self.name}] Log Entries: {self.log}\n")
        return {'status': 'Log printed', 'log': self.log}

    def handle_request_vote(self, data: dict):
        """
        Handles incoming RequestVote RPCs from candidates during leader election.
        Implements Raft's voting rules to maintain consistency and leadership safety.
        
        Args:
            data (dict): Contains voting request information:
                - term: Candidate's term number
                - candidate_name: Name of the candidate requesting vote
                - last_log_index: Index of candidate's last log entry
                - last_log_term: Term of candidate's last log entry
                
        Returns:
            dict: Response containing:
                - term: Current term, for candidate to update itself
                - vote_granted: Boolean indicating if vote was granted
        """
        # Extract voting request data
        candidate_term = data['term']
        candidate_id = data['candidate_name']
        candidate_last_log_index = data['last_log_index']
        candidate_last_log_term = data['last_log_term']
        # Rule 1: If candidate's term is less than current term, reject vote
        if candidate_term < self.current_term:
            return {'term': self.current_term, 'vote_granted': False}
        # Rule 2: If candidate's term is greater, update current term and become follower
        if candidate_term > self.current_term:
            self.current_term = candidate_term
            self.state = 'Follower'
            self.voted_for = None
            self.leader_id = None

        # Rule 3: Check if we can vote for this candidate. We can only vote if we haven't voted in this term or already voted for this candidate
        can_vote = (self.voted_for is None or self.voted_for == candidate_id)
        
        # Rule 4: Check if candidate's log is at least as up-to-date as ours
        last_log_index = len(self.log) - 1
        last_log_term = self.log[last_log_index]['term'] if self.log else 0
        # Compare logs based on Raft rules:
        # - Last log term is higher, or
        # - Terms are equal but candidate's log is at least as long
        log_is_up_to_date = (
            candidate_last_log_term > last_log_term or
            (candidate_last_log_term == last_log_term and
             candidate_last_log_index >= last_log_index)
        )
        # Grant vote if both conditions are met
        if can_vote and log_is_up_to_date:
            self.voted_for = candidate_id
            self.reset_election_timer()
            print(f"[{self.name}] Voted for {candidate_id} in term {self.current_term}")
            return {'term': self.current_term, 'vote_granted': True}
        # Reject vote if any condition is not met
        return {'term': self.current_term, 'vote_granted': False}
    
    def handle_append_entries(self, data: dict):
        """
        Handles AppendEntries RPCs from the leader for log replication and heartbeats. Implements Raft's log consistency check and replication mechanisms.
        Args:
            data (dict): Contains:
                - term: Leader's term
                - leader_name: Leader's identifier
                - prev_log_index: Index of log entry before new ones
                - prev_log_term: Term of prev_log_index entry
                - entries: List of entries to append
                - leader_commit: Leader's commit index
        Returns:
            dict: Response containing term (Current term for leader to update itself) and success (Boolean indicating if append was successful)
        """
        # request data
        leader_term = data['term']
        leader_id = data['leader_name']
        prev_log_index = data['prev_log_index']
        prev_log_term = data['prev_log_term']
        entries = data['entries']
        leader_commit = data['leader_commit']

        # Rule 1: Reply false if leader's term < currentTerm
        if leader_term < self.current_term:
            return {'term': self.current_term, 'success': False}

        # Rule 2: Update term if leader's term is greater
        if leader_term > self.current_term:
            self.current_term = leader_term
            self.voted_for = None

        # Reset election timer and update leader state
        self.reset_election_timer()
        self.state = 'Follower'
        self.leader_id = leader_id

        # Check if recovery is needed for committed entries
        current_time = time.time()
        if (self.commit_index < leader_commit and 
            not self.recovering and 
            (current_time - self.last_recovery_attempt) > self.recovery_timeout):
            self.recovering = True
            self.last_recovery_attempt = current_time
            print(f"[{self.name}] Starting recovery of committed entries. "
                f"Local commit index: {self.commit_index}, Leader commit: {leader_commit}")

        # Rule 3: Check log consistency
        if prev_log_index >= 0 and (
            prev_log_index >= len(self.log) or
            self.log[prev_log_index]['term'] != prev_log_term
        ):
            return {'term': self.current_term, 'success': False}

        # Process new entries if any
        if entries:
            # Delete any conflicting entries and append new ones
            self.log = self.log[:prev_log_index + 1]
            old_len = len(self.log)
            self.log.extend(entries)
            
            # Print recovery information if in recovery mode
            if self.recovering:
                # Only consider entries up to leader's commit index
                committed_entries = [e for e in entries if e['index'] <= leader_commit]
                if committed_entries:
                    print(f"[{self.name}] Recovered {len(committed_entries)} committed entries")
                    print(f"[{self.name}] Committed entries indices: {[e['index'] for e in committed_entries]}")

        # Update commit index and apply newly committed entries
        old_commit_index = self.commit_index
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log) - 1)
            if self.recovering:
                newly_committed = self.commit_index - old_commit_index
                if newly_committed > 0:
                    print(f"[{self.name}] Applying {newly_committed} newly committed entries")
            self.apply_committed_entries()

        # Check if recovery is complete
        if self.recovering and self.commit_index >= leader_commit:
            self.recovering = False
            print(f"[{self.name}] Recovery of committed entries complete. "
                f"Commit index: {self.commit_index}")

        return {'term': self.current_term, 'success': True}

    def apply_committed_entries(self):
        """
        Applies all committed but not yet applied entries to the state machine. Guarantees that entries are applied in order and only once.
        This function is called whenever the commit index advances, ensuring that all committed entries are applied to the state machine in sequence.
        """
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied]
            self.apply_entry_to_state_machine(entry)

    def apply_entry_to_state_machine(self, entry):
        """
        Applies a single log entry to the state machine (persistent storage).
        """
        with open(self.log_filename, 'a') as f:
            f.write(f"{entry['value']}\n")
        print(f"[{self.name}] Applied entry to log: {entry['value']}")

    def start_election(self):
        """
        Initiates leader election process when election timeout occurs.
        Now only requests votes from nodes in the same cluster.
        """
        # Initialize election state
        self.state = 'Candidate'
        self.current_term += 1
        self.voted_for = self.name
        self.leader_id = None
        votes_received = 1  # Count self vote
        
        print(f"[{self.name}] Starting election for term {self.current_term}")
        self.reset_election_timer()

        # Prepare vote request arguments
        last_log_index = len(self.log) - 1
        last_log_term = self.log[last_log_index]['term'] if self.log else 0

        # Request votes only from nodes in the same cluster
        if not self.cluster_nodes:  # Skip if not part of a cluster
            return

        for node_name in self.cluster_nodes:
            if node_name != self.name:
                try:
                    response = self.send_rpc(
                        self.cluster_nodes[node_name]['ip'],
                        self.cluster_nodes[node_name]['port'],
                        'RequestVote',
                        {
                            'term': self.current_term,
                            'candidate_name': self.name,
                            'last_log_index': last_log_index,
                            'last_log_term': last_log_term
                        }
                    )

                    # Process vote response
                    if response and response.get('vote_granted'):
                        votes_received += 1

                    # Check if we have majority within our cluster
                    if (votes_received > len(self.cluster_nodes) // 2 
                        and self.state == 'Candidate'):
                        self.become_leader()
                        break

                    # Step down if we discover a higher term
                    elif response and response['term'] > self.current_term:
                        self.current_term = response['term']
                        self.state = 'Follower'
                        self.voted_for = None
                        break

                except Exception as e:
                    print(f"[{self.name}] Error requesting vote from {node_name}: {e}")

    def check_cluster_health(self):
        """Checks the health of the cluster by attempting to contact nodes in the same cluster."""
        if not self.cluster_nodes:
            return 0
            
        reachable_nodes = 1  # Count self
        for node_name in self.cluster_nodes:
            if node_name != self.name:
                try:
                    response = self.send_rpc(
                        self.cluster_nodes[node_name]['ip'],
                        self.cluster_nodes[node_name]['port'],
                        'AppendEntries',
                        {
                            'term': self.current_term,
                            'leader_name': self.name,
                            'prev_log_index': len(self.log) - 1,
                            'prev_log_term': self.log[-1]['term'] if self.log else 0,
                            'entries': [],
                            'leader_commit': self.commit_index
                        }
                    )
                    if response is not None:
                        reachable_nodes += 1
                except Exception:
                    continue
        return reachable_nodes

    def become_leader(self):
        """Transitions node to leader state if conditions are met."""
        # Check cluster health before becoming leader
        reachable_nodes = self.check_cluster_health()
        
        # Require majority of nodes in the CLUSTER to be reachable (not all NODES)
        cluster_size = len(self.cluster_nodes)
        if reachable_nodes <= cluster_size // 2:
            print(f"[{self.name}] Cannot become leader: only {reachable_nodes}/{cluster_size} nodes reachable")
            self.state = 'Follower'
            return

        print(f"[{self.name}] Becoming leader for term {self.current_term}")
        self.state = 'Leader'
        self.leader_id = self.name

        # Initialize leader state only for cluster nodes
        self.next_index = {node: len(self.log) for node in self.cluster_nodes if node != self.name}
        self.match_index = {node: -1 for node in self.cluster_nodes if node != self.name}

        # Send immediate heartbeat
        self.send_heartbeats()

    def send_heartbeats(self):
        """Sends heartbeats only to nodes in the same cluster."""
        if not self.cluster_nodes:
            return
            
        for node_name in self.cluster_nodes:
            if node_name != self.name:
                entries = []
                next_idx = self.next_index.get(node_name, len(self.log))
                if next_idx < len(self.log):
                    entries = self.log[next_idx:]
                self.send_append_entries(node_name, entries)

    def handle_client_submit(self, data):
        """
        Handles client requests to submit new values to the distributed log.
        Implements the leader's role in processing client requests and ensuring replication.
                
        Returns:
            dict: Response containing either success (if value was committed) or redirect (True and leader_name if not leader).
        """
        # If not leader, redirect client to current leader
        if self.state != 'Leader':
            return {
                'redirect': True,
                'leader_name': self.leader_id
            }

        # Create new log entry
        entry = {
            'term': self.current_term,
            'value': data['value'],
            'index': len(self.log)
        }
                
        self.log.append(entry) # Append entry to local log first
        print(f"[{self.name}] New entry added to log: {entry}")

        # Attempt to replicate to followers
        success_count = 1  # Count self as success
        
        # Try to replicate to all other nodes
        for node_name in NODES:
            if node_name != self.name:
                if self.replicate_log_to_follower(node_name):
                    success_count += 1

        # Check if we achieved majority consensus
        if success_count > len(NODES) // 2:
            # Majority successful, commit and apply
            self.commit_index = len(self.log) - 1
            self.apply_committed_entries()
            return {'success': True}
        else:
            # Failed to achieve majority, rollback entry
            self.log.pop()  # Remove the entry we just added
            return {'success': False}

    def replicate_log_to_follower(self, follower_name):
        """
        Only focus on replicating committed entries
        """
        next_idx = self.next_index[follower_name]
        
        # Only send entries up to commit_index
        entries = self.log[next_idx:self.commit_index + 1]
        if not entries:
            return True  # Nothing to replicate
            
        response = self.send_append_entries(follower_name, entries)
        
        if response and response.get('success'):
            self.next_index[follower_name] = self.commit_index + 1
            self.match_index[follower_name] = self.commit_index
            return True
        elif response:
            # If replication failed, decrease next_index and try with a smaller batch
            self.next_index[follower_name] = max(0, next_idx - 1)
            return self.replicate_log_to_follower(follower_name)
        return False

    def send_append_entries(self, follower_name, entries):
        """
        Sends AppendEntries RPC to a follower with new log entries or heartbeat.
        """
        # Calculate previous log information for consistency check
        prev_log_index = self.next_index[follower_name] - 1
        # Get term of previous log entry (0 if no previous entry)
        prev_log_term = (
            self.log[prev_log_index]['term'] 
            if prev_log_index >= 0 and self.log 
            else 0
        )

    # Send AppendEntries RPC to follower
        return self.send_rpc(
            NODES[follower_name]['ip'],
            NODES[follower_name]['port'],
            'AppendEntries',
            {
                'term': self.current_term,          # Leader's current term
                'leader_name': self.name,           # Leader's identifier
                'prev_log_index': prev_log_index,   # Index of log entry before new ones
                'prev_log_term': prev_log_term,     # Term of prev_log_index entry
                'entries': entries,                 # Log entries to store (empty for heartbeat)
                'leader_commit': self.commit_index  # Leader's commit index
            }
        )

    def trigger_leader_change(self):
        """
        Triggers a leader change if the current node is the leader. This is called by the client to initiate a leader change.
        """
        if self.state == 'Leader':
            print(f"[{self.name}] Triggering leader change")
            self.state = 'Follower'
            self.voted_for = None
            self.leader_id = None
            self.reset_election_timer()
            return {'status': 'Leader stepping down'}
        return {'status': 'Not a leader'}

    def simulate_crash(self):
        """
        Simulates a crash by:
        1. If leader: appends entries to log before sleeping
        2. Maintains volatile state to demonstrate log consistency
        3. Temporarily disconnects from network to allow new leader election
        """
        self.simulating_crash_ongoing = True
        
        print(f"\n[{self.name}] Simulating crash...")
        
        crash_entries = [
            {'term': self.current_term, 'value': 'crash_entry_1', 'index': len(self.log)},
            {'term': self.current_term, 'value': 'crash_entry_2', 'index': len(self.log) + 1}
        ]
        
        self.log.extend(crash_entries)
        print(f"[{self.name}] Leader appended entries before crash: {crash_entries}")
        print(f"[{self.name}] Current log: {self.log}")
        print(f"[{self.name}] Commit index: {self.commit_index}")
        
        # Print current state before sleep
        print(f"[{self.name}] Pre-sleep state:")
        print(f"[{self.name}] Current role: {self.state}")
        print(f"[{self.name}] Current term: {self.current_term}")
        print(f"[{self.name}] Log entries: {self.log}")
        
        # Close current socket
        if self.server_socket:
            self.server_socket.close()
            self.server_socket = None
        
        print(f"[{self.name}] Going to sleep for 10 seconds to allow new leader election...")
        time.sleep(10)
        
        # Create and bind new socket
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.ip, self.port))
        self.server_socket.listen(5)
        
        self.simulating_crash_ongoing = False
        print(f"[{self.name}] Node rejoining cluster with log: {self.log}")
        
        return {'status': 'Node crashed'}
        

    def send_rpc(self, ip, port, rpc_type, data, timeout=2.0):
        # Coming from Lab1 to handle RPC
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(timeout)
                s.connect((ip, port))
                message = json.dumps({'rpc_type': rpc_type, 'data': data})
                s.sendall(message.encode())
                response = s.recv(4096).decode()
                return json.loads(response)
        except socket.timeout:
            print(f"[{self.name}] RPC to {ip}:{port} timed out")
            return None
        except ConnectionRefusedError:
            print(f"[{self.name}] RPC to {ip}:{port} failed: Connection refused")
            return None
        except Exception as e:
            print(f"[{self.name}] RPC to {ip}:{port} failed: {e}")
            return None


if __name__ == '__main__':
    """
    Main entry point for our Raft node application.
    """
    if len(sys.argv) != 2:
        print("Usage: python node.py [node_name]")
        sys.exit(1)
    # Validate node name from config file
    node_name = sys.argv[1]
    if node_name not in NODES:
        print(f"Invalid node name. Available nodes: {list(NODES.keys())}")
        sys.exit(1)
    # Create and start the node
    node = Node(node_name)
    try:
        node.start()
    except KeyboardInterrupt:
        # Handle shutdown on Ctrl+C
        print(f"[{node.name}] Shutting down...")
        node.running = False
        if node.server_socket: # Clean up network resources
            node.server_socket.close()
