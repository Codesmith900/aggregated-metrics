import json
from datetime import datetime
from collections import defaultdict

def fetch_raw_events_from_db():
    """
    Simulating fetching raw video streaming events from a database.
    
    In a real-world scenario, this function would connect to a database
    (like PostgreSQL, MongoDB) and run a query to retrieve the raw event data
    for a specific time period.

    Returns:
        list: A list of dictionaries, where each dictionary represents a raw event.
    """
    # Sample raw event data. In a real-world scenario, this would be a query result.
    raw_event_data = [
        {'user_id': 'user_1', 'session_id': 'session_A', 'event_type': 'buffer_start', 'timestamp': '2023-10-27T10:00:15Z'},
        {'user_id': 'user_2', 'session_id': 'session_B', 'event_type': 'buffer_start', 'timestamp': '2023-10-27T10:01:05Z'},
        {'user_id': 'user_1', 'session_id': 'session_A', 'event_type': 'buffer_end', 'timestamp': '2023-10-27T10:00:18Z'},
        {'user_id': 'user_3', 'session_id': 'session_C', 'event_type': 'buffer_start', 'timestamp': '2023-10-27T10:02:30Z'},
        {'user_id': 'user_2', 'session_id': 'session_B', 'event_type': 'buffer_end', 'timestamp': '2023-10-27T10:01:10Z'},
        {'user_id': 'user_1', 'session_id': 'session_A', 'event_type': 'buffer_start', 'timestamp': '2023-10-27T10:05:00Z'},
        {'user_id': 'user_3', 'session_id': 'session_C', 'event_type': 'buffer_end', 'timestamp': '2023-10-27T10:02:35Z'},
        {'user_id': 'user_1', 'session_id': 'session_A', 'event_type': 'buffer_end', 'timestamp': '2023-10-27T10:05:02Z'},
    ]
    return raw_event_data

def aggregate_buffering_data(raw_events):
    """
    Aggregates raw video streaming events to calculate buffering metrics per session.

    Args:
        raw_events (list): A list of dictionaries, where each dictionary
                           represents a raw event with 'user_id', 'session_id',
                           'event_type', and 'timestamp'.

    Returns:
        dict: A dictionary where keys are session IDs and values are dictionaries
              containing 'total_buffer_duration_seconds' and 'buffer_events_count'.
    """
    # Sort events by timestamp to ensure consistent processing order
    sorted_events = sorted(raw_events, key=lambda x: x['timestamp'])

    # Use a dictionary to keep track of the start time for each active buffering event
    session_states = {}
    aggregated_data = defaultdict(lambda: {'total_buffer_duration_seconds': 0, 'buffer_events_count': 0})

    for event in sorted_events:
        session_id = event['session_id']
        event_type = event['event_type']
        
        try:
            # Strip the 'Z' from the end of the timestamp string before parsing
            timestamp_str = event['timestamp'].strip('Z')
            timestamp = datetime.fromisoformat(timestamp_str)
        except (ValueError, TypeError) as e:
            print(f"Skipping event due to invalid timestamp format: {event['timestamp']}. Error: {e}")
            continue

        if event_type == 'buffer_start':
            # Record the start time for the session
            session_states[session_id] = timestamp
            
        elif event_type == 'buffer_end':
            # Check if we have a corresponding start event
            if session_id in session_states:
                buffer_start_time = session_states[session_id]
                buffer_duration = (timestamp - buffer_start_time).total_seconds()
                
                # Add duration and increment count for this session
                aggregated_data[session_id]['total_buffer_duration_seconds'] += buffer_duration
                aggregated_data[session_id]['buffer_events_count'] += 1
                
                # Remove the start time from the state, as this buffer event is complete
                del session_states[session_id]
            else:
                # Handle cases where we have an 'end' event without a corresponding 'start'
                print(f"Warning: Found 'buffer_end' event for session {session_id} without a matching 'buffer_start'. Skipping this event.")

    return dict(aggregated_data)

def generate_summary_metrics(aggregated_data):
    """
    Generates high-level summary metrics from the aggregated data.

    Args:
        aggregated_data (dict): The output from aggregate_buffering_data.

    Returns:
        dict: A dictionary of summary metrics.
    """
    total_buffer_time_all_sessions = 0
    total_buffer_events_all_sessions = 0
    sessions_with_buffering = len(aggregated_data)
    
    for session_id, metrics in aggregated_data.items():
        total_buffer_time_all_sessions += metrics['total_buffer_duration_seconds']
        total_buffer_events_all_sessions += metrics['buffer_events_count']

    # Calculate averages
    avg_buffer_time_per_session = (
        total_buffer_time_all_sessions / sessions_with_buffering
        if sessions_with_buffering > 0 else 0
    )
    avg_buffer_time_per_event = (
        total_buffer_time_all_sessions / total_buffer_events_all_sessions
        if total_buffer_events_all_sessions > 0 else 0
    )

    return {
        'total_sessions_with_buffering': sessions_with_buffering,
        'total_buffer_time_seconds': total_buffer_time_all_sessions,
        'total_buffer_events': total_buffer_events_all_sessions,
        'average_buffer_time_per_session_seconds': avg_buffer_time_per_session,
        'average_buffer_time_per_event_seconds': avg_buffer_time_per_event
    }

if __name__ == "__main__":
    print("Starting data aggregation...")
    
    # Simulate fetching data from a database
    raw_event_data = fetch_raw_events_from_db()

    # 1. Aggregate raw events into session-level metrics
    session_metrics = aggregate_buffering_data(raw_event_data)
    print("\n--- Aggregated Metrics per Session ---")
    for session_id, metrics in session_metrics.items():
        print(f"Session ID: {session_id}")
        print(f"  Total Buffer Time: {metrics['total_buffer_duration_seconds']:.2f} seconds")
        print(f"  Number of Buffering Events: {metrics['buffer_events_count']}")
        print("-" * 30)

    
    # 2. Generate high-level summary metrics from the aggregated data
    summary = generate_summary_metrics(session_metrics)
    print("\n--- High-level Summary Metrics ---")
    print(f"Total Sessions with Buffering: {summary['total_sessions_with_buffering']}")
    print(f"Total Buffer Time (All Sessions): {summary['total_buffer_time_seconds']:.2f} seconds")
    print(f"Total Buffering Events (All Sessions): {summary['total_buffer_events']}")
    print(f"Average Buffer Time per Session: {summary['average_buffer_time_per_session_seconds']:.2f} seconds")
    print(f"Average Buffer Time per Event: {summary['average_buffer_time_per_event_seconds']:.2f} seconds")
