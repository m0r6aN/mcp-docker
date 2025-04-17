import os


def load_gitignore(root_path):
    gitignore_path = os.path.join(root_path, '.gitignore')
    ignored_patterns = ['.git']  # Ignore .git directory and all its subdirectories
    if os.path.exists(gitignore_path):
        with open(gitignore_path, 'r') as file:
            for line in file:
                line = line.strip()
                if line and not line.startswith('#'):
                    ignored_patterns.append(line)
    #print(ignored_patterns)
    return ignored_patterns


def is_ignored(path, ignored_patterns):
    for pattern in ignored_patterns:
        # Ignore directories and subdirectories explicitly
        if pattern.endswith('/') or os.path.isdir(os.path.join(path)):
            if os.path.basename(path).startswith(pattern.strip('/')):
                return True
        elif pattern in path:
            return True
    return False


def get_dir_structure(start_path, indent='', ignored_patterns=[]):
    items = os.listdir(start_path)
    items.sort()  # Sorting for consistent output
    structure = indent + start_path + '/\n'
    #print(items)
    for index, item in enumerate(items):
        item_path = os.path.join(start_path, item)
        if is_ignored(item_path, ignored_patterns):
            continue
        is_last = (index == len(items) - 1)
        pointer = '└── ' if is_last else '├── '
        structure += indent + pointer + item + '\n'
        if os.path.isdir(item_path):
            next_indent = indent + ('    ' if is_last else '│   ')
            structure += get_dir_structure(item_path, next_indent, ignored_patterns)
    return structure


def save_structure_to_file(structure, file_name):
    with open(file_name, 'w', encoding='utf-8') as file:
        file.write(structure)


if __name__ == "__main__":
    start_path = r'D:\Repos\mcp-servers'
    ignored_patterns = load_gitignore(start_path)
    structure = get_dir_structure(start_path, ignored_patterns=ignored_patterns)
    save_structure_to_file(structure, 'Directory_Structure.md')

 #   start_path = r'D:\Repos\agents_as_a_service'


    