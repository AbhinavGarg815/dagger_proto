"""Load the Python classes for proto/dagger/api.proto, compiled on the fly.

Shared by make_vectors.py and contract_test.py so neither depends on code
generated somewhere else.

Uses grpcio-tools' bundled protoc when it is installed (the PC), otherwise a
`protoc` on PATH (the Jetson: apt `protobuf-compiler`). protoc older than 3.15
needs --experimental_allow_proto3_optional for the schema's `optional` fields;
that flag is added when needed.
"""

import importlib
import os
import re
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
PROTO = os.path.join(HERE, "dagger", "api.proto")


def _compile_with_grpc_tools(out):
    from grpc_tools import protoc

    well_known = os.path.join(os.path.dirname(protoc.__file__), "_proto")
    return protoc.main(["protoc", f"-I{HERE}", f"-I{well_known}", f"--python_out={out}", PROTO])


def _compile_with_system_protoc(out):
    exe = shutil.which("protoc")
    if exe is None:
        sys.exit("needs grpcio-tools (pip) or protoc on PATH (apt protobuf-compiler)")
    version = subprocess.run([exe, "--version"], capture_output=True, text=True).stdout
    m = re.search(r"(\d+)\.(\d+)", version)
    args = [exe, f"-I{HERE}", f"--python_out={out}", PROTO]
    if m and (int(m.group(1)), int(m.group(2))) < (3, 15):
        args.insert(1, "--experimental_allow_proto3_optional")
    return subprocess.run(args).returncode


def load_api():
    out = tempfile.mkdtemp(prefix="dagger-proto-")
    try:
        rc = _compile_with_grpc_tools(out)
    except ImportError:
        rc = _compile_with_system_protoc(out)
    if rc != 0:
        sys.exit(f"protoc failed ({rc})")
    open(os.path.join(out, "dagger", "__init__.py"), "a").close()
    sys.path.insert(0, out)
    return importlib.import_module("dagger.api_pb2")
