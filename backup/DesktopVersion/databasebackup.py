import subprocess
import datetime
import os

def export_bacpac():
    backup_dir = "C:\\Backups"
    os.makedirs(backup_dir, exist_ok=True)  # Ensure the folder exists

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(backup_dir, f"GSDM-Copy-IGS_{timestamp}.bacpac")

    command = [
        "C:\\customtools\\sqlpackage\\sqlpackage.exe",
        "/Action:Export",
        "/ssn:dev1epms.database.windows.net",
        "/sdn:GSDM-Copy-IGS",
        "/su:dev1user",
        "/sp:dev1@emps@db",
        f"/tf:{output_path}"
    ]

    result = subprocess.run(command, capture_output=True, text=True)
    
    if result.returncode == 0:
        print(f"[✔] Export successful: {output_path}")
    else:
        print("[✖] Export failed")
        print("STDOUT:", result.stdout)
        print("STDERR:", result.stderr)

export_bacpac()
