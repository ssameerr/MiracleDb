
import zipfile
import os

def zip_dir(path, zip_name):
    with zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(path):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, os.path.dirname(path))
                
                # Create ZipInfo to preserve permissions
                zinfo = zipfile.ZipInfo.from_file(file_path, arcname)
                
                # Make run.sh and executables executable
                if file.endswith('.sh') or 'miracledb' in file:
                     # 0o755 (rwxr-xr-x) -> shifted 16 bits for external_attr
                     zinfo.external_attr = 0o755 << 16
                
                with open(file_path, 'rb') as f:
                    zipf.writestr(zinfo, f.read())
    print(f"Created {zip_name}")

if __name__ == "__main__":
    zip_dir("miracle_pkg", "miracledb_release.zip")
