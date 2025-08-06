# Simple File Generator Usage

## You only need TWO commands:

### 1. Generate test files:
```cmd
python scale_generator.py 1000
```
This creates 1000 test files in a folder called "scaled_test"

### 2. Check the files:
```cmd
python error-check.py
```
This analyzes all the files you just created

## That's it!

### Examples:
- `python scale_generator.py 500` - makes 500 files
- `python scale_generator.py 5000` - makes 5000 files  
- `python scale_generator.py 10000` - makes 10000 files

The files are exactly like your sample files, just with different address fields.