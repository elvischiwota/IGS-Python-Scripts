import subprocess
import datetime
import os

def export_bacpac(server, database, username, password):
    backup_dir = "C:\\Users\\tonde\\Desktop\\IGS Python Scripts\\DBBackups"
    os.makedirs(backup_dir, exist_ok=True)  # Ensure the folder exists
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(backup_dir, f"{database}{timestamp}.bacpac")

    connection_string = (
        f"Data Source={server};"
        f"Initial Catalog={database};"
        f"User ID={username};"
        f"Password={password};"
        "TrustServerCertificate=True"
    )

    command = [
        "C:\\customtools\\sqlpackage\\sqlpackage.exe",
        "/Action:Export",
        "/SourceConnectionString:" + connection_string,
        f"/tf:{output_path}"
    ]

    result = subprocess.run(command, capture_output=True, text=True)
    
    if result.returncode == 0:
        print(f"[✔] Export successful: {output_path}")
    else:
        print("[✖] Export failed")
        print("STDOUT:", result.stdout)
        print("STDERR:", result.stderr)

# Backup first database (your original)
export_bacpac(
    server="4.221.230.0",
    database="stlmprodb",
    username="stlmprodbuser",
    password="Stlm@epms#2025"
)

# Backup second database (new one you want to add)
export_bacpac(
    server="4.221.229.40",
    database="ndmprodb",  # <-- Replace this with your actual database name
    username="ndmvmuserpro",
    password="ndm@virual@987#"
)
