import os
from typing import Set, List, Iterable, Callable, Generator


def directory_file_generator(directory: str, yield_extensions: Set[str] = None) -> Generator[str, None, None]:
    """
    Generator to traverse the root directory recursively and yield files filtering on the given extensions, if any.
    :param directory: The directory to start in.
    :param yield_extensions: Optional set of extensions.
    """
    # Traverse root directory, and list directories as dirs and files as files
    if yield_extensions:
        # Filter in extension
        for root, directories, files in os.walk(directory):
            for file in files:
                file_name, file_extension = os.path.splitext(file)
                if file_extension in yield_extensions:
                    yield os.path.join(root, file)
    else:
        # No filtering
        for root, directories, files in os.walk(directory):
            for file in files:
                yield os.path.join(root, file)


def target_generator(
        target_path: str,
        yield_extensions: Set[str],
        base_path: str = None,
        source_targets: List[str] = None,
        skip_handler: Callable[[str, str], None] = None
) -> Generator[str, None, None]:
    """
    Generate a single target.

    A target is either:
     - a directory (all files with a yield extension will be yielded)
     - a yield file (a file with a yield extension, which will be yielded as the only thing)
     - a text file where each line is a new target (all targets files will be generated)

    Any other files will result in an error

    :param target_path: The path to the target.
    :param base_path: The base path for resolving relative paths.
    :param yield_extensions: File extensions to yield on.
    :param source_targets: Optional source target path to backtrack if another target produced this target.
    :param skip_handler: Function to handle when files are skipped for a given reason.
    """

    # Verify base_path
    if base_path:
        if not os.path.isabs(base_path):
            raise ValueError('The base path "{0}" is not absolute.'.format(base_path))
        if not os.path.exists(base_path):
            raise ValueError('The base path "{0}" does not exist.'.format(base_path))
        if not os.path.isdir(base_path):
            raise ValueError('The base path "{0}" is not a directory.'.format(base_path))

    joined_path = os.path.join(base_path, target_path.lstrip(os.path.sep)) if base_path else ''

    # Sanitize yield extensions
    yield_extensions_lowered = {ext.lower() for ext in yield_extensions}

    # Determine correct path and type
    is_file = False
    is_dir = False
    path = None

    if os.path.isfile(joined_path):
        is_file = True
        path = joined_path
    elif os.path.isdir(joined_path):
        is_dir = True
        path = joined_path
    elif os.path.isfile(target_path):
        is_file = True
        path = target_path
    elif os.path.isdir(target_path):
        is_dir = True
        path = target_path
    else:

        message_sub_target = '  is a sub target of: "{0}"'.format(
            '"\n  which is a sub target of: "'.join(source_targets)
        ) if source_targets else ''

        raise ValueError('The path could not be found.\n  The path: "{0}"\n{1}'.format(
            target_path,
            message_sub_target
        ))

    # Handle path
    if is_file:
        target_base_path, target_basename = os.path.split(path)
        file_name, file_extension = os.path.splitext(target_basename)

        if file_extension.lower() in yield_extensions_lowered:
            # File is one of the desired extensions
            yield path
        else:
            # File is expected to be a list of more files
            with open(path, mode='rt') as target_list_file:
                target_list = None
                try:
                    target_list = target_list_file.read().splitlines()
                except UnicodeDecodeError:
                    # File is not a list as expected
                    skip_handler(path, 'Unable to read file.')
                if target_list:
                    target_list = [item for item in target_list if item != '']  # Filter out empty lines
                    # Generate paths for the target list
                    for generated_path in multi_target_generator(
                            target_list,
                            yield_extensions=yield_extensions,
                            base_path=target_base_path,
                            source_targets=[path] + source_targets if source_targets else [path],
                            skip_handler=skip_handler
                    ):
                        yield generated_path
    elif is_dir:
        for generated_path in directory_file_generator(path, yield_extensions=yield_extensions):
            yield generated_path


def multi_target_generator(
        target_iterable: Iterable[str],
        yield_extensions: Set[str],
        base_path: str = None,
        source_targets: List[str] = None,
        skip_handler: Callable[[str, str], None] = None
) -> Generator[str, None, None]:
    """Generate all targets in an iterable, eliminating duplicate yields"""
    processed_items = set()
    for target_path in target_iterable:
        for file_path in target_generator(
                target_path,
                yield_extensions,
                source_targets=source_targets,
                base_path=base_path,
                skip_handler=skip_handler
        ):
            if file_path not in processed_items:
                processed_items.add(file_path)
                yield file_path
            elif skip_handler:
                skip_handler(file_path, 'Already processed')
