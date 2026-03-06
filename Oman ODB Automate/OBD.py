# ssh -L 9090:10.72.144.50:8080 bizops@10.72.144.50
# bs#1940$

import re
import os
import sys
import requests
from datetime import datetime

# ============================================================================
# CONFIGURATION
# ============================================================================
BASE_PATH = "/Users/subhabratabarik/Desktop/BlackNGreen/Client/omantal_oman/Automate"
DATE = "03/11/2025"              # DD/MM/YYYY format
START_TIME = "10:00:00"
END_TIME = "21:00:00"
PRIORITY = "8"

SERVICES = [
    "CD_OBD(90010091)",
    "LE_OBD(90010007)",
    "Timeless_OBD(90010094)",
    "Heritage_OBD(90010093)"
]

CUSTOM_PREFIXES = {
    "Timeless_OBD": "TL",
    "Heritage_OBD": "HR"
}

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_file_path(base_path, date_str, service_prefix):
    """Generate file path: /path/2025Nov/X/XXNovember25CD.csv"""
    day, month, year = date_str.split('/')
    date_obj = datetime(int(year), int(month), int(day))
    
    year_month_folder = date_obj.strftime("%Y%b")
    day_folder = str(int(day))
    folder_path = os.path.join(base_path, year_month_folder, day_folder)
    
    file_date_str = date_obj.strftime("%d%B%y")
    file_name = f"{file_date_str}{service_prefix}.csv"
    
    return os.path.join(folder_path, file_name)

def generate_form_data(service_name, date, start_time, end_time, priority, base_path):
    service_base = service_name.split('(')[0]
    service_prefix = CUSTOM_PREFIXES.get(service_base, service_base.split('_')[0])
    
    cli_match = re.search(r'\((\d+)\)', service_name)
    cli_number = cli_match.group(1) if cli_match else ""
    
    date_without_slashes = date.replace('/', '')
    job_name = f"{service_prefix}O{date_without_slashes}"
    
    day, month, year = date.split('/')
    form_date = f"{month}/{day}/{year}"
    
    csv_file_path = get_file_path(base_path, date, service_prefix)
    
    return {
        'serviceName': service_name,
        'jobName': job_name,
        'queueName': 'original',        # NEW: Queue dropdown
        'jobType': 'normal',             # Radio button: Normal (checked)
        # 'starcopy': NOT included (unchecked)
        # 'recordDedication': NOT included (unchecked)
        # 'retrycheck': NOT included (unchecked checkbox)
        'startDate': form_date,
        'endDate': form_date,
        'startTime': start_time,
        'endTime': end_time,
        'cliNumber': cli_number,
        'priority': priority,
        'mscIp': '',
        'dndOption': '0',
        'check_job': '1'
    }, csv_file_path

# ============================================================================
# DISPLAY ALL JOBS AND CHECK FILES
# ============================================================================

print("="*80)
print("OBD JOB AUTOMATION")
print("="*80)
print()

all_jobs = []
missing_files = False

for service in SERVICES:
    form_data, csv_file = generate_form_data(
        service, DATE, START_TIME, END_TIME, PRIORITY, BASE_PATH
    )
    all_jobs.append((form_data, csv_file))
    
    print("Form data prepared (NOT submitted):")
    for key, value in form_data.items():
        print(f"  {key}: {value}")
    
    # Check file existence
    if os.path.exists(csv_file):
        with open(csv_file, 'r') as f:
            count = sum(1 for line in f if line.strip())
        print(f"\nStatus: ✅ File found ({count} numbers)")
        print(f"File: {csv_file}")
    else:
        print(f"\nStatus: ❌ File not found")
        print(f"Missing: {csv_file}")
        missing_files = True
    
    print()

# ============================================================================
# EXIT IF ANY FILE IS MISSING
# ============================================================================

if missing_files:
    print("="*80)
    print("❌ ERROR: One or more MSISDN files not found!")
    print("="*80)
    print("⚠️  Cannot proceed with job creation.")
    print("Please ensure all CSV files exist in the correct location.")
    print("\nExiting program...")
    sys.exit(1)

# ============================================================================
# USER CONFIRMATION
# ============================================================================

print("="*80)
print("✅ All files found! Ready to proceed.")
print("="*80)
print("\nForm Settings:")
print("  • QueueName: original (selected)")
print("  • JobType: Normal (checked ✓)")
print("  • Star Copy: (unchecked ✗)")
print("  • Record Dedication: (unchecked ✗)")
print("  • Retry: (unchecked ✗)")
print()

confirmation = input("⚠️  Do you want to ADD these jobs? (yes/no): ").lower().strip()

if confirmation == 'yes':
    print("\n✅ Proceeding with job submission...\n")
    
    # Login
    session = requests.Session()
    login_url = 'http://localhost:9090/IVRSecurity/j_spring_security_check'
    login_response = session.post(login_url, data={'j_username': 'admin', 'j_password': '123'})
    
    if login_response.status_code == 200:
        print("✅ Login successful\n")
        
        # Submit each job
        submit_url = 'http://localhost:9090/IVRSecurity/newjob.htm'
        success_count = 0
        
        for idx, (form_data, csv_file) in enumerate(all_jobs, 1):
            job_name = form_data['jobName']
            
            try:
                with open(csv_file, 'rb') as f:
                    files = {
                        'msisdn': (os.path.basename(csv_file), f, 'text/csv')
                    }
                    response = session.post(submit_url, data=form_data, files=files)
                
                if response.status_code == 200:
                    print(f"✅ [{idx}] {job_name}: Job added successfully")
                    success_count += 1
                else:
                    print(f"⚠️  [{idx}] {job_name}: Status {response.status_code}")
                    
            except Exception as e:
                print(f"❌ [{idx}] {job_name}: Error - {str(e)}")
        
        print(f"\n{'='*80}")
        print(f"✅ COMPLETED: {success_count}/{len(all_jobs)} jobs added successfully")
        print(f"{'='*80}")
    else:
        print("❌ Login failed!")
        sys.exit(1)
        
else:
    print("\n🚫 Job submission CANCELLED. No jobs were added.")
    print("Exiting...")
    sys.exit(0)