import sys, traceback
import posixpath

def error_msg(err, show_details=True):
    error_class = err.__class__.__name__
    if len(err.args) > 0: detail = err.args[0]
    else: detail = ''
    cl, exc, tb = sys.exc_info()
    details = '\n'.join([f"File \"{s[0]}\", line {s[1]} in {s[2]}" for s in traceback.extract_tb(tb)])
    errMsg = f"\n[{error_class}] {detail}"
    if show_details: print(details, errMsg)
    else: print (errMsg)
    
def clean_name(name):
    """
    Cleans the name so that Windows style paths work
    """
    # Normalize Windows style paths
    clean_name = posixpath.normpath(name).replace('\\', '/')

    # os.path.normpath() can strip trailing slashes so we implement
    # a workaround here.
    if name.endswith('/') and not clean_name.endswith('/'):
        # Add a trailing slash as it was stripped.
        clean_name = clean_name + '/'

    # Given an empty string, os.path.normpath() will return ., which we don't want
    if clean_name == '.':
        clean_name = ''

    return clean_name
    
def safe_join(base, *paths):
    """
    A version of django.utils._os.safe_join for S3 paths.
    Joins one or more path components to the base path component
    intelligently. Returns a normalized version of the final path.
    The final path must be located inside of the base path component
    (otherwise a ValueError is raised).
    Paths outside the base path indicate a possible security
    sensitive operation.
    """
    base_path = base
    if base_path:
        base_path = base_path.rstrip('/')
    else:
        base_path = ''
    paths = [p for p in paths]

    final_path = base_path + '/'
    for path in paths:
        _final_path = posixpath.normpath(posixpath.join(final_path, path))
        # posixpath.normpath() strips the trailing /. Add it back.
        if path.endswith('/') or _final_path + '/' == final_path:
            _final_path += '/'
        final_path = _final_path
    if final_path == base_path:
        final_path += '/'

    # Ensure final_path starts with base_path and that the next character after
    # the base path is /.
    base_path_len = len(base_path)
    if (not final_path.startswith(base_path) or final_path[base_path_len] != '/'):
        raise ValueError('the joined path is located outside of the base path'
                         ' component')

    return final_path.lstrip('/')