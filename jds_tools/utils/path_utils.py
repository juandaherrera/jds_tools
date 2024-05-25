import logging
import os
import sys

ROOT_FILES: set[str] = {
    '.git',
    'requirements.txt',
    'README.md',
    'environment.yml',
    '.env',
    '.venv',
    'LICENSE',
}


def add_project_root_to_sys_path(
    project_root: str = None, recursive_search: bool = True, max_depth: int = 5
) -> None:
    if project_root is None:
        project_root = os.getcwd()
        depth = 0
        while recursive_search:
            if any(
                file.lower() in [f.lower() for f in os.listdir(project_root)]
                for file in ROOT_FILES
            ):
                break
            project_root = os.path.dirname(project_root)
            depth += 1
            if max_depth is not None and depth >= max_depth:
                raise Exception(
                    f"No root files found within the specified maximum depth ({max_depth})."
                )
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
        logging.info(f"Added project root to sys.path: {project_root}")
