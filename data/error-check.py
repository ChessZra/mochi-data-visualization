import json
import glob
import os

files_skipped = 0
server_files = 0
client_files = 0
scanned_files = 0

empty_address_fields = 0
for filepath in glob.glob('scaled_test\\*.stats.json'):
    if os.path.getsize(filepath) == 0:
        files_skipped += 1
        continue
    scanned_files += 1
    with open(filepath, 'r') as f:
    
        data = json.load(f)
        str_data = json.dumps(data, indent=2)

        index_found = str_data.find('received from')
        if index_found != -1:
            field_substr = str_data[index_found:index_found+100] # Filter out received from <unknown> 
            if '<unknown>' not in field_substr: 
                server_files += 1

        if str_data.find('origin') != -1:
            client_files += 1
        
        if '\"address\": \"\"' in str_data:
            empty_address_fields += 1

print(f'Total files scanned: {scanned_files}')
print(f'Files skipped: {files_skipped}')
print(f'Server files: {server_files}')
print(f'Client files: {client_files}')
print(f'Files with empty addresses: {empty_address_fields}')