"""Convert complete files to student versions by stripping solution code."""

def strip_solutions(content):
    lines = content.split('\n')
    result = []
    in_solution = False

    for line in lines:
        if '# START_SOLUTION' in line:
            in_solution = True
            # Get indentation from the previous line or current line
            if result:
                # Get indentation from the last added line
                last_line = result[-1]
                indent = len(last_line) - len(last_line.lstrip())
            else:
                # Fallback to current line's indentation
                indent = len(line) - len(line.lstrip())

            indent_str = ' ' * indent
            result.append(f'{indent_str}# Write your code here according to the instructions')
        elif '# END_SOLUTION' in line:
            in_solution = False
        elif not in_solution:
            result.append(line)

    return '\n'.join(result)

# Convert files
files = [
    ("main_complete.py", "main.py"),
    ("populate_complete.py", "populate.py"),
    ("delete_collection_complete.py", "delete_collection.py")
]

for source, target in files:
    with open(source, 'r') as f:
        content = f.read()

    student_content = strip_solutions(content)

    with open(target, 'w') as f:
        f.write(student_content)

    print(f"✅ {source} → {target}")
