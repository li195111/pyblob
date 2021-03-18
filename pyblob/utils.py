import sys, traceback

def error_msg(err, show_details=True):
    error_class = err.__class__.__name__
    if len(err.args) > 0: detail = err.args[0]
    else: detail = ''
    cl, exc, tb = sys.exc_info()
    details = '\n'.join([f"File \"{s[0]}\", line {s[1]} in {s[2]}" for s in traceback.extract_tb(tb)])
    errMsg = f"\n[{error_class}] {detail}"
    if show_details: print(details, errMsg)
    else: print (errMsg)