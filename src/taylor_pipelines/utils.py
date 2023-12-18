import os
import base64
import requests
from typing import Optional

def file_from_github(
    repo: str, 
    path_in_repo: str, 
    local_path: Optional[str] = None, # if None, will use persistent tempfile
    access_token: Optional[str] = None,
    force_overwrite: bool = False
):
    """
    Loads a file from GitHub and returns its local path as a string.
    
    :param repo: The GitHub repository in the format 'username/repo'.
    :param file_path: The path to the file within the repository.
    :param access_token: (Optional) Personal access token for private repositories.
    :return: The local path to the downloaded file.
    """
    base_url = f"https://api.github.com/repos/{repo}/contents/{path_in_repo}"

    headers = {}
    if access_token:
        headers['Authorization'] = f'token {access_token}'

    response = requests.get(base_url, headers=headers)

    if response.status_code == 200:
        b64_content = response.json()['content']
        content = base64.b64decode(b64_content)
        if local_path is None:
            import tempfile
            extension = os.path.splitext(path_in_repo)[1]
            if len(extension) == 0:
                extension = None
            local_path = tempfile.NamedTemporaryFile(
                delete=False, 
                suffix=extension
            ).name
        elif os.path.exists(local_path) and not force_overwrite:
            raise ValueError(f"File already exists at {local_path}. Use force_overwrite=True to overwrite.")

        with open(local_path, 'wb') as file:
            file.write(content)

        return local_path
    else:
        raise Exception(f"Failed to download file: {response.status_code} {response.reason}")

def normalize_path(path: str):
    """
    Normalizes a path by expanding user and relative paths.
    """
    expanded_path = os.path.expanduser(path)
    
    # Convert to absolute path and normalize
    absolute_path = os.path.abspath(expanded_path)
    normalized_path = os.path.normpath(absolute_path)

    return normalized_path