# DataPolish Lambda container image.
#
# Built with `sam build` from the project root using the SAM template's
# Image specification. Build context is the project root so we can COPY
# both src/datapolish/ and lambda/ into the image.
#
# Pinned to arm64 to match Apple Silicon dev machines and AWS Graviton
# Lambda. Native architecture means: no Docker emulation during build,
# pre-built wheels available for pandas/pyarrow/numpy, and ~20% cheaper
# Lambda invocations than x86_64.

FROM public.ecr.aws/lambda/python:3.11-arm64

# Install build tools as a safety net — most pip deps ship pre-built
# arm64 wheels, but if any dep falls back to a source build we want a
# C compiler available rather than failing with "cc not found".
RUN yum install -y gcc gcc-c++ make && yum clean all

# Install runtime Python dependencies first — keeps this layer cached
# across code changes.
COPY lambda/requirements-lambda.txt ${LAMBDA_TASK_ROOT}/
RUN pip install --no-cache-dir -r ${LAMBDA_TASK_ROOT}/requirements-lambda.txt

# Copy our package and the handler.
COPY src/datapolish/ ${LAMBDA_TASK_ROOT}/datapolish/
COPY lambda/lambda_function.py ${LAMBDA_TASK_ROOT}/

# Lambda runs whatever module.function CMD points to.
CMD ["lambda_function.lambda_handler"]
