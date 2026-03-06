import subprocess
from typing import TypedDict, List
from pathlib import Path
from langgraph.graph import StateGraph, START, END
from datetime import datetime

# -------------------
# State Definition
# -------------------
class OBDState(TypedDict):
    """State for OBD base preparation workflow."""
    run_id: str  # Unique identifier for this run
    run_date: str  # When this run started
    
    # MSISDNs tracking
    previous_leftovers: List[str]  # From scrub_left.txt
    new_scrub_count: int  # Fresh MSISDNs from difference.py
    merged_count: int  # Total after merging
    batch_count: int  # Number of batches created
    new_leftovers: List[str]  # Remaining for next run
    
    # File paths
    base_file: str
    dnd_file: str
    scrub_file: str
    scrub_left_file: str
    
    # Status
    status: str  # 'running', 'completed', 'failed'
    error_message: str

# -------------------
# Utility Functions
# -------------------
def read_msisdns(filepath: str) -> List[str]:
    """Read MSISDNs from file, one per line."""
    try:
        path = Path(filepath)
        if not path.exists():
            return []
        with open(filepath, 'r') as f:
            # Remove duplicates and empty lines
            msisdns = [line.strip() for line in f if line.strip()]
            return list(set(msisdns))  # Remove duplicates
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return []

def write_msisdns(filepath: str, msisdns: List[str], mode='w'):
    """Write MSISDNs to file, one per line."""
    with open(filepath, mode) as f:
        f.write('\n'.join(msisdns))
        if msisdns:  # Add trailing newline if file not empty
            f.write('\n')

# -------------------
# Node Functions
# -------------------
def initialize_workflow(state: OBDState) -> OBDState:
    """Initialize the workflow with file paths and run metadata."""
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    print(f"\n{'='*60}")
    print(f"🚀 Starting OBD Base Workflow - Run ID: {run_id}")
    print(f"{'='*60}\n")
    
    return {
        **state,
        'run_id': run_id,
        'run_date': datetime.now().isoformat(),
        'base_file': state.get('base_file', 'base.txt'),
        'dnd_file': state.get('dnd_file', 'dnd.txt'),
        'scrub_file': state.get('scrub_file', 'scrub.txt'),
        'scrub_left_file': state.get('scrub_left_file', 'scrub_left.txt'),
        'status': 'running',
        'previous_leftovers': [],
        'new_scrub_count': 0,
        'merged_count': 0,
        'batch_count': 0,
        'new_leftovers': [],
        'error_message': ''
    }

def load_previous_leftovers(state: OBDState) -> OBDState:
    """Step 1: Load leftovers from previous run (scrub_left.txt)."""
    print("📂 Step 1: Loading previous leftovers from scrub_left.txt...")
    
    leftovers = read_msisdns(state['scrub_left_file'])
    
    print(f"   Found {len(leftovers)} leftover MSISDNs from previous run")
    
    return {
        **state,
        'previous_leftovers': leftovers
    }

def run_difference_script(state: OBDState) -> OBDState:
    """Step 2: Run difference.py to create scrub.txt (base.txt - dnd.txt)."""
    print("\n🔧 Step 2: Running difference.py script...")
    
    try:
        # Verify input files exist
        if not Path(state['base_file']).exists():
            raise FileNotFoundError(f"{state['base_file']} not found")
        if not Path(state['dnd_file']).exists():
            raise FileNotFoundError(f"{state['dnd_file']} not found")
        
        # Run the difference script
        result = subprocess.run(
            ['python', 'difference.py'],
            capture_output=True,
            text=True,
            check=True
        )
        
        print(f"   ✓ difference.py completed")
        print(f"   Output: {result.stdout.strip() if result.stdout else 'No output'}")
        
        # Count MSISDNs in newly created scrub.txt
        new_scrub = read_msisdns(state['scrub_file'])
        new_count = len(new_scrub)
        
        print(f"   ✓ Created scrub.txt with {new_count} MSISDNs")
        
        return {
            **state,
            'new_scrub_count': new_count
        }
        
    except subprocess.CalledProcessError as e:
        error_msg = f"difference.py failed: {e.stderr}"
        print(f"   ❌ {error_msg}")
        return {
            **state,
            'status': 'failed',
            'error_message': error_msg
        }
    except Exception as e:
        error_msg = f"Error in difference script: {str(e)}"
        print(f"   ❌ {error_msg}")
        return {
            **state,
            'status': 'failed',
            'error_message': error_msg
        }

def merge_leftovers_into_scrub(state: OBDState) -> OBDState:
    """Step 3: Merge previous leftovers into scrub.txt."""
    print("\n🔀 Step 3: Merging previous leftovers into scrub.txt...")
    
    if not state['previous_leftovers']:
        print("   No previous leftovers to merge")
        return {
            **state,
            'merged_count': state['new_scrub_count']
        }
    
    try:
        # Read current scrub.txt
        current_scrub = read_msisdns(state['scrub_file'])
        
        # Merge with leftovers (using set to remove duplicates)
        merged = list(dict.fromkeys(state['previous_leftovers'] + current_scrub))
        
        # Write merged list back to scrub.txt
        write_msisdns(state['scrub_file'], merged, mode='w')
        
        print(f"   ✓ Merged {len(state['previous_leftovers'])} leftovers")
        print(f"   ✓ New total in scrub.txt: {len(merged)} MSISDNs")
        print(f"   ({state['new_scrub_count']} new + {len(state['previous_leftovers'])} left = {len(merged)} total)")
        
        return {
            **state,
            'merged_count': len(merged)
        }
        
    except Exception as e:
        error_msg = f"Error merging leftovers: {str(e)}"
        print(f"   ❌ {error_msg}")
        return {
            **state,
            'status': 'failed',
            'error_message': error_msg
        }

def run_batch_split_script(state: OBDState) -> OBDState:
    """Step 4: Run batch_split.py on scrub.txt to create weekly batches."""
    print("\n📦 Step 4: Running batch_split.py to create batches...")
    
    try:
        # Run batch split script
        result = subprocess.run(
            ['python3', 'batch_split.py'],
            capture_output=True,
            text=True,
            check=True
        )
        
        print(f"   ✓ batch_split.py completed")
        
        # Parse output to count batches (adjust based on your script's output)
        output = result.stdout.strip()
        print(f"   Output: {output if output else 'No output'}")
        
        # Count batch files created (assuming they follow pattern: batch_1.txt, batch_2.txt, etc.)
        batch_files = list(Path('.').glob('batch_*.txt'))
        batch_count = len(batch_files)
        
        print(f"   ✓ Created {batch_count} batch files")
        
        return {
            **state,
            'batch_count': batch_count
        }
        
    except subprocess.CalledProcessError as e:
        error_msg = f"batch_split.py failed: {e.stderr}"
        print(f"   ❌ {error_msg}")
        return {
            **state,
            'status': 'failed',
            'error_message': error_msg
        }
    except Exception as e:
        error_msg = f"Error in batch split: {str(e)}"
        print(f"   ❌ {error_msg}")
        return {
            **state,
            'status': 'failed',
            'error_message': error_msg
        }

def extract_and_save_leftovers(state: OBDState) -> OBDState:
    """Step 5: Extract remaining MSISDNs from scrub.txt, save to scrub_left.txt, and clean scrub.txt."""
    print("\n💾 Step 5: Extracting leftover MSISDNs...")
    
    try:
        # Read remaining MSISDNs from scrub.txt
        remaining = read_msisdns(state['scrub_file'])
        
        if not remaining:
            print("   ✓ No leftovers - all MSISDNs processed into batches")
            # Clear scrub_left.txt
            write_msisdns(state['scrub_left_file'], [], mode='w')
            # Clear scrub.txt as well
            write_msisdns(state['scrub_file'], [], mode='w')
            return {
                **state,
                'new_leftovers': [],
                'status': 'completed'
            }
        
        # Save to scrub_left.txt
        write_msisdns(state['scrub_left_file'], remaining, mode='w')
        
        # Clean scrub.txt (remove all numbers that were moved to leftovers)
        write_msisdns(state['scrub_file'], [], mode='w')
        
        print(f"   ✓ Saved {len(remaining)} leftover MSISDNs to scrub_left.txt")
        print(f"   ✓ scrub.txt cleaned for next run")
        
        return {
            **state,
            'new_leftovers': remaining,
            'status': 'completed'
        }
        
    except Exception as e:
        error_msg = f"Error saving leftovers: {str(e)}"
        print(f"   ❌ {error_msg}")
        return {
            **state,
            'status': 'failed',
            'error_message': error_msg
        }


def print_summary(state: OBDState) -> OBDState:
    """Final step: Print execution summary."""
    print(f"\n{'='*60}")
    print(f"📊 WORKFLOW SUMMARY - Run ID: {state['run_id']}")
    print(f"{'='*60}")
    print(f"Status: {state['status'].upper()}")
    print(f"\nMSISDN Processing:")
    print(f"  • Previous leftovers brought forward: {len(state['previous_leftovers'])}")
    print(f"  • New MSISDNs after DND scrub: {state['new_scrub_count']}")
    print(f"  • Total merged: {state['merged_count']}")
    print(f"  • Batches created: {state['batch_count']}")
    print(f"  • New leftovers for next run: {len(state['new_leftovers'])}")
    
    if state['status'] == 'failed':
        print(f"\n❌ Error: {state['error_message']}")
    else:
        print(f"\n✅ Workflow completed successfully!")
    
    print(f"{'='*60}\n")
    
    return state


# -------------------
# Graph Construction
# -------------------
def build_obd_workflow():
    workflow = StateGraph(OBDState)
    workflow.add_node('initialize', initialize_workflow)
    workflow.add_node('load_leftovers', load_previous_leftovers)
    workflow.add_node('run_difference', run_difference_script)
    workflow.add_node('merge_leftovers', merge_leftovers_into_scrub)
    workflow.add_node('run_batch_split', run_batch_split_script)
    workflow.add_node('extract_leftovers', extract_and_save_leftovers)
    workflow.add_node('print_summary', print_summary)
    workflow.add_edge(START, 'initialize')
    workflow.add_edge('initialize', 'load_leftovers')
    workflow.add_edge('load_leftovers', 'run_difference')
    workflow.add_edge('run_difference', 'merge_leftovers')
    workflow.add_edge('merge_leftovers', 'run_batch_split')
    workflow.add_edge('run_batch_split', 'extract_leftovers')
    workflow.add_edge('extract_leftovers', 'print_summary')
    workflow.add_edge('print_summary', END)
    return workflow.compile()


# -------------------
# Execution
# -------------------
def run_obd_base_workflow():
    graph = build_obd_workflow()
    thread_id = 'obd_weekly_base_preparation'
    config = {'configurable': {'thread_id': thread_id}}
    try:
        final_state = graph.invoke({}, config=config)
        return final_state
    except Exception as e:
        print(f"\n❌ Fatal workflow error: {e}")
        raise


if __name__ == '__main__':
    print("="*60)
    print("OBD Base Automation Workflow")
    print("Powered by LangGraph")
    print("="*60)
    result = run_obd_base_workflow()
