import json
import os
import sys
import time
import random
from pathlib import Path

def generate_scaled_files(num_files, output_dir="./scaled_test", base_address_id=None):
    # Auto-generate base ID if not provided
    if base_address_id is None:
        base_address_id = random.randint(100000, 999999)
    sample_dir = "./sample"
    # Find sample files
    if not os.path.exists(sample_dir):
        print(f"Error: Sample directory '{sample_dir}' not found!")
        return False
    sample_files = [f for f in os.listdir(sample_dir) if f.endswith('.stats.json')]
    if not sample_files:
        print(f"Error: No .stats.json files found in '{sample_dir}'!")
        return False
    os.makedirs(output_dir, exist_ok=True)
    print(f"Address ID range: {base_address_id:,} - {base_address_id + num_files - 1:,}")
    print()
    templates = []
    for sample_file in sample_files:
        file_path = os.path.join(sample_dir, sample_file)
        with open(file_path, 'r') as f:
            content = f.read()        
        with open(file_path, 'r') as f:
            data = json.load(f)
            original_address = data.get("address", "")
        template_name = Path(sample_file).stem.replace('.stats', '')
        templates.append((content, original_address, template_name))
    # Generate files
    start_time = time.time()
    for i in range(num_files):
        template_content, original_address, template_name = templates[i % len(templates)]
        address_id = base_address_id + i
        new_address = f"na+sm://{address_id}-0"
        new_content = template_content.replace(original_address, new_address)
        filename = f"gen.{template_name}.{address_id}.stats.json"
        filepath = os.path.join(output_dir, filename)
        with open(filepath, 'w') as f:
            f.write(new_content)
    print(f"   Files created: {num_files:,}")
    print(f"   • Output location: {os.path.abspath(output_dir)}")
    return True

def main():
    """Main function to handle command line arguments and run the generator."""
    # Default values
    num_files = 1000
    output_dir = "./scaled_test"
    # Parse command line arguments
    if len(sys.argv) > 1:
        try:
            num_files = int(sys.argv[1])
        except ValueError:
            print("Error: First argument must be a number (number of files to generate)")
            sys.exit(1)
    if len(sys.argv) > 2:
        output_dir = sys.argv[2]
    if num_files <= 0:
        print("Error: Number of files must be positive")
        sys.exit(1)
    if num_files > 50000:
        response = input(f"⚠️  You're about to generate {num_files:,} files. Continue? (y/N): ")
        if response.lower() != 'y':
            print("Operation cancelled")
            sys.exit(0)    
    success = generate_scaled_files(num_files, output_dir)
    if success:
        print(f"Next steps:")
        print(f"   - Test with your error-check.py script:")
        print(f"     python error-check.py")
        print(f"   - Remember to update the glob pattern in error-check.py to:")
        print(f"     glob.glob('./{os.path.basename(output_dir)}/*.stats.json')")
    else:
        print("Generation failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
