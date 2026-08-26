# =============================================================================
# Voxel router -- OHIF viewer + nginx
#
# Multi-stage: a Node builder produces the OHIF bundle, nginx serves it. The
# built image is pushed to the Snowflake image repository, so the ~154 MB of
# static assets and .wasm codecs travel INSIDE AN IMAGE LAYER.
#
# =============================================================================
# THIS IS WHY THE IMAGE IS BUILT HERE AND NOT IN SNOWFLAKE
#
# The incumbent was forced to deploy its apps imperatively via SDK -- abandoning the
# declarative bundle path entirely -- because OHIF's .wasm files exceed the incumbent's
# workspace-filesystem sync limits. Building the bundle into a container image and
# pushing to a registry sidesteps that completely: nothing large is ever synced,
# because the large thing is baked where it will run.
#
# It also means the router needs NO external access integration at runtime. npm
# runs at build time on a developer machine with normal internet, and the running
# service has zero outbound network capability. "No data egress" becomes a
# property provable with DESCRIBE SERVICE rather than a promise about code.
# =============================================================================

# ---------------------------------------------------------------------------
# Stage 1: build the viewer
# ---------------------------------------------------------------------------
FROM --platform=linux/amd64 node:20-bookworm-slim AS viewer

WORKDIR /build

# OHIF is pinned in package.json rather than vendored as prebuilt bytes, so a
# version bump is a dependency change reviewable in a diff.
COPY package.json .

# --ignore-scripts because OHIF's dependency tree runs postinstall hooks that
# expect a full monorepo checkout. Everything needed to serve the viewer is in
# the published dist.
RUN npm install --no-audit --no-fund --ignore-scripts

# The published @ohif/app package ships a built dist. Copy it out rather than
# running a webpack build, which needs the monorepo and adds minutes.
RUN set -eux; \
    if [ -d node_modules/@ohif/app/dist ]; then \
      cp -r node_modules/@ohif/app/dist /viewer; \
    elif [ -d node_modules/@ohif/viewer/dist ]; then \
      cp -r node_modules/@ohif/viewer/dist /viewer; \
    else \
      # Fail loudly with the actual layout rather than producing an image that
      # serves a blank page. A viewer that 200s with no content is harder to
      # diagnose than a build that stops here.
      echo "FATAL: no OHIF dist found. Layout was:" >&2; \
      ls -la node_modules/@ohif/ >&2 || true; \
      exit 1; \
    fi; \
    test -f /viewer/index.html

# ---------------------------------------------------------------------------
# Stage 2: serve
# ---------------------------------------------------------------------------
FROM --platform=linux/amd64 nginx:1.27-bookworm

# The rendered nginx.conf, which carries the Sf-Context-* header forwarding that
# is the authorization boundary. See dicomweb_nginx_conf.j2.
COPY nginx.conf /etc/nginx/conf.d/default.conf

COPY --from=viewer /viewer /usr/share/nginx/html

# Point OHIF at the same-origin DICOMweb path. Injected after the copy so it
# overwrites whatever default config the published bundle shipped with.
COPY ohif-app-config.js /usr/share/nginx/html/app-config.js

# nginx's stock /etc/nginx/mime.types (1.27) already maps BOTH
#     application/javascript  js;
#     application/wasm        wasm;
# so no MIME configuration is needed or wanted here. An earlier version added a
# `types { application/wasm wasm; }` block to the static-asset location; because
# `types` REPLACES the inherited map rather than extending it, that stripped the
# type from .js and .css and served them as application/octet-stream, which the
# browser refuses to execute -- producing a completely blank viewer.
#
# Validate the config at build time so a broken directive fails the build rather
# than the first page load.
RUN nginx -t

EXPOSE 8000

# nginx's default entrypoint drops privileges to the nginx user for workers while
# the master binds the port. Left as-is deliberately rather than forcing USER,
# which would break the port bind.
CMD ["nginx", "-g", "daemon off;"]
