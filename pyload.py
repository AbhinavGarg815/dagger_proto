"""Load the Python classes for proto/dagger/api.proto, compiled on the fly.

Shared by make_vectors.py and contract_test.py so neither depends on code
generated somewhere else. Needs grpcio-tools (for protoc) and protobuf.
"""

import importlib
import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))


def load_api():
    from grpc_tools import protoc

    out = tempfile.mkdtemp(prefix="dagger-proto-")
    well_known = os.path.join(os.path.dirname(protoc.__file__), "_proto")
    rc = protoc.main(["protoc", f"-I{HERE}", f"-I{well_known}", f"--python_out={out}",
                      os.path.join(HERE, "dagger", "api.proto")])
    if rc != 0:
        sys.exit(f"protoc failed ({rc})")
    open(os.path.join(out, "dagger", "__init__.py"), "a").close()
    sys.path.insert(0, out)
    return importlib.import_module("dagger.api_pb2")
