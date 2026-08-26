# =============================================================================
# DICOMweb gateway
#
# QIDO-RS / WADO-RS / STOW-RS. Runs on localhost:8080 and is NOT an SPCS
# endpoint -- the nginx router is the only container on the public port.
#
# linux/amd64 is mandatory. SPCS compute pools are x86_64, and an arm64 image
# built on Apple silicon fails at service start with an exec format error that
# names the container, not the architecture.
# =============================================================================
FROM --platform=linux/amd64 python:3.11-slim-bookworm

# pydicom needs no compiler for header parsing, and we deliberately do NOT install
# pylibjpeg or openjpeg: this gateway never decodes pixel data. Frames are served
# as byte-range passthrough in their original transfer syntax and the browser
# decodes them. That keeps the image small, removes a CPU cost per frame, and
# means the bytes a client receives are the bytes that were ingested -- which
# matters for anything submission-adjacent.
#
# No apt-get layer at all. Nothing here needs a system package: the SPCS
# readinessProbe speaks HTTP directly to the port, so there is no need for curl
# in the image just to health-check it.

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY voxel/ ./voxel/

# Non-root. The gateway only ever reads: the stage volume is mounted read-only
# unless the owner role holds WRITE, and the memory volume is the only thing it
# writes to.
RUN useradd --create-home --shell /usr/sbin/nologin voxel \
 && chown -R voxel:voxel /app
USER voxel

EXPOSE 8080

# --workers 1 deliberately. The in-process series manifest cache and open file
# descriptors are per-process, so multiple workers would multiply stage-volume
# reads and file handles for no benefit. Horizontal scale is service INSTANCES,
# which is also what keeps autoscaling available -- block volumes would have
# forced MIN_INSTANCES = MAX_INSTANCES.
#
# --timeout-keep-alive 75 sits under the 90-second SPCS ingress idle timeout, so
# a stalled connection is closed by us with a clean error rather than by the proxy
# mid-response.
#
# ACCESS LOG IS ON. It was initially disabled with --no-access-log to keep frame
# scrolling from flooding the event table. That was the wrong trade: when the
# viewer failed, SYSTEM$GET_SERVICE_LOGS showed only startup lines, so there was
# no way to tell whether a request had even reached the gateway. Being unable to
# distinguish "no request arrived" from "request arrived and failed" costs more
# than the log volume.
CMD ["uvicorn", "voxel.app:app", \
     "--host", "0.0.0.0", \
     "--port", "8080", \
     "--workers", "1", \
     "--timeout-keep-alive", "75"]
